//! Board `#76`: reconcile coherence at anchor retirement.
//!
//! A marker-delivery anchor and its outbox obligation are ONE fact kept in two
//! ledgers. The frontier owns the authority to ACCEPT a marker
//! acknowledgement; the outbox owns the obligation to PUSH the
//! `HistoryCompacted` record that acknowledgement answers. Retire one without
//! the other and the conversation re-offers a marker whose authority is gone:
//! the selection finds the live obligation, `record_publication_offer` finds
//! it too and records the offer, and the ack for that offer walks the
//! `offered = Some` arm of `apply_marker_ack_with_impact` into
//! `marker_delivery_progress` -- which correctly finds no authority and
//! refuses at `marker_progress.rs`'s invariant -- "stored `MarkerAck` has no
//! matching marker delivery authority". Fail closed, connection killed, and
//! because nothing about that state is traffic-dependent, killed again on the
//! next boot, and the next.
//!
//! ⛔ THE INVARIANT IS CORRECT AND IS NOT TOUCHED BY ANY OF THIS. It is the
//! right answer to a genuine divergence between a stored acknowledgement and
//! the authority that would have to have produced it. These pins are about
//! where the divergence came from, and the fix is entirely on the reconcile
//! side: `ops_session_replay::reconcile_orphaned_marker_obligations`, run at
//! the load-end anchor reconcile and after the mid-replay retry.
//!
//! ⚠ WHAT THE MANUFACTURE HERE IS, EXACTLY -- stated because the shape decides
//! what these pins may be cited for. The separation is driven by a MEMBER
//! LEAVING after its own compaction marker has drained and before the marker's
//! co-recipient has acknowledged it. `commit_leave` drops the departing
//! member's marker record from the frontier AND retires its stored anchor
//! together (measured: `marker_anchors` 1 -> 0 across the leave), while the
//! outbox keeps the surviving co-recipient's push obligation at that same
//! delivery sequence. So the ledgers separate at a retirement the anchor side
//! accounts for perfectly -- which is why the obligation reconcile runs
//! alongside the anchor reconcile rather than only when that reconcile
//! retired something.
//!
//! The literal orphan (`derived < stored`, the split
//! `reconcile_orphaned_marker_anchors` and
//! `retry_replay_after_orphan_reconcile` exist for) is NOT reachable from
//! client traffic at this tree: `commit_leave` retires the anchor together
//! with the record, and `search_capacity_floor` refuses any retention advance
//! that would strand one (`MarkerAnchorCapacity`). Every pin below therefore
//! MEASURES `replay_orphan_reconciles` instead of assuming it, so a reader can
//! see which reconcile site the store actually exercised.

use std::error::Error;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use haematite::{Database, DatabaseConfig, EventStore};
use liminal::durability::{DurableStore, HaematiteStore, open_ephemeral};
use liminal_protocol::wire::{
    AttachAttemptToken, AttachSecret, ClientRequest, ConnectionIncarnation,
    CredentialAttachRequest, EnrollBound, Generation, LeaveAttemptToken, LeaveRequest, MarkerAck,
    ParticipantAck, ParticipantId, ServerValue,
};

use super::ProductionParticipantHandler;
use super::tests::dispatch;
use super::tests_marker_ack_fixture::{
    marker_fixture_config, prepare_marker_fixture, prepare_marker_fixture_in,
};
use crate::server::participant::{ParticipantOfferedProgress, ParticipantSemanticHandler};

/// The exact refusal `marker_progress.rs` raises when a marker acknowledgement
/// arrives with no delivery authority behind it. Written out because these
/// pins' whole discriminator is that this STRING stops appearing.
const NO_MATCHING_AUTHORITY: &str = "stored MarkerAck has no matching marker delivery authority";

/// A durable store whose rows replay into the stranded shape, plus the exact
/// facts a caller needs to drive the surviving member.
struct StrandedStore {
    store: Arc<dyn DurableStore>,
    conversation_id: u64,
    marker_seq: u64,
    surviving: EnrollBound,
}

/// Drives the marker fixture and then retires the marker's OWNER, leaving the
/// surviving co-recipient holding a push obligation for a marker record the
/// frontier no longer carries.
///
/// Everything here is ordinary client traffic through the production dispatch
/// seam -- an enrolment fixture, a drain, a `Leave`. No durable row is forged
/// and no in-memory state is reached into, so the shape is reproduced by
/// REPLAY on every boot rather than installed once.
fn strand_marker_obligation(store: Arc<dyn DurableStore>) -> Result<StrandedStore, Box<dyn Error>> {
    let fixture = prepare_marker_fixture_in(store)?;
    let conversation_id = fixture.marker_delivery.conversation_id;
    let departing = fixture.target_participant;
    let surviving_id = if departing == fixture.record_participant {
        fixture.catchup_participant
    } else {
        fixture.record_participant
    };
    let receipt = |participant_id: ParticipantId| -> Result<EnrollBound, Box<dyn Error>> {
        fixture
            .enrolled
            .iter()
            .find(|bound| bound.participant_id() == participant_id)
            .cloned()
            .ok_or_else(|| {
                Box::<dyn Error>::from(format!(
                    "fixture carries no receipt for participant {participant_id}"
                ))
            })
    };
    let departing_receipt = receipt(departing)?;
    let surviving = receipt(surviving_id)?;

    let left = dispatch(
        &fixture.handler,
        fixture.target_connection,
        ClientRequest::Leave(LeaveRequest {
            conversation_id,
            participant_id: departing,
            capability_generation: Generation::ONE,
            attach_secret: departing_receipt.attach_secret(),
            leave_attempt_token: LeaveAttemptToken::new([0xC1; 16]),
        }),
    )?;
    if !matches!(left, ServerValue::LeaveCommitted(_)) {
        return Err(format!("the marker owner did not leave: {left:?}").into());
    }

    let stranded = StrandedStore {
        store: Arc::clone(&fixture.store),
        conversation_id,
        marker_seq: fixture.marker_delivery.delivery_seq,
        surviving,
    };
    drop(fixture);
    Ok(stranded)
}

/// The pair a member must present to attach: its live generation and the
/// attach secret currently issued to it.
#[derive(Clone, Copy)]
struct MemberCredential {
    generation: Generation,
    secret: AttachSecret,
}

impl MemberCredential {
    fn enrolled(receipt: &EnrollBound) -> Self {
        Self {
            generation: Generation::ONE,
            secret: receipt.attach_secret(),
        }
    }
}

/// One booted server plus the surviving member's live binding on it.
struct BootedMember {
    handler: ProductionParticipantHandler,
    connection: ConnectionIncarnation,
    conversation_id: u64,
    participant_id: ParticipantId,
    generation: Generation,
    next_credential: MemberCredential,
    marker_seq: u64,
    /// Committed ordinary admissions the moment the conversation reached
    /// load-ready, before this member attached. The quiet-estate pin's
    /// measurement of "no commits" is taken against this.
    admissions_at_load: usize,
}

impl BootedMember {
    /// Boots a fresh server over `stranded`'s durable bytes and attaches the
    /// surviving member on a fresh connection.
    fn boot(
        stranded: &StrandedStore,
        connection_ordinal: u64,
        credential: MemberCredential,
    ) -> Result<Self, Box<dyn Error>> {
        let handler =
            ProductionParticipantHandler::new(Arc::clone(&stranded.store), marker_fixture_config())?;
        let admissions_at_load = committed_admissions(&handler, stranded.conversation_id)?;
        let connection = ConnectionIncarnation::new(0xA7, connection_ordinal);
        let attach_token = u8::try_from(connection_ordinal & 0xFF)
            .map_err(|_| "connection ordinal did not fit the attempt-token byte")?;
        let participant_id = stranded.surviving.participant_id();
        let attached = dispatch(
            &handler,
            connection,
            ClientRequest::CredentialAttach(CredentialAttachRequest {
                conversation_id: stranded.conversation_id,
                participant_id,
                capability_generation: credential.generation,
                attach_secret: credential.secret,
                // DISTINCT PER BOOT. A repeated attempt token is answered
                // idempotently from the durable receipt index (a `Bound`
                // replay rather than a fresh `AttachBound`), which would make
                // the second boot's attach a different operation from the
                // first -- and the quiet-estate pin is about two boots that
                // differ in nothing but being two boots.
                attach_attempt_token: AttachAttemptToken::new([attach_token; 16]),
                accept_marker_delivery_seq: None,
            }),
        )?;
        let ServerValue::AttachBound(bound) = attached else {
            return Err(format!("the surviving member did not attach: {attached:?}").into());
        };
        Ok(Self {
            handler,
            connection,
            conversation_id: stranded.conversation_id,
            participant_id,
            generation: bound.origin_binding_epoch().capability_generation,
            // A credential attach ROTATES the secret and mints the next
            // generation. Carrying the issued pair forward is what lets a
            // second boot of the same store attach at all -- the enrolment
            // receipt is spent after the first one.
            next_credential: MemberCredential {
                generation: bound.origin_binding_epoch().capability_generation,
                secret: bound.attach_secret(),
            },
            marker_seq: stranded.marker_seq,
            admissions_at_load,
        })
    }

    /// Walks this member's live obligations exactly as the connection loop
    /// does, recording the offer for any marker it is handed -- which is the
    /// step that arms the ack's `offered = Some` arm.
    fn walk_offers(&self) -> Result<Vec<u64>, Box<dyn Error>> {
        let mut sequences = Vec::new();
        let mut offered = None;
        // Signed bound: the fixture's whole obligation index is far shorter
        // than this, so exhausting it means the walk ENDED, not that it was
        // cut off.
        for _ in 0..16 {
            let Some(publication) =
                self.handler
                    .next_publication(self.connection, self.conversation_id, offered)?
            else {
                return Ok(sequences);
            };
            sequences.push(publication.delivery_seq());
            offered = Some(ParticipantOfferedProgress {
                binding_epoch: publication.binding_epoch,
                through_seq: publication.delivery_seq(),
            });
            if publication.delivery_seq() == self.marker_seq {
                self.handler.record_publication_offer(&publication)?;
            }
        }
        Err("the surviving member's obligation walk did not end within its signed bound".into())
    }

    /// The marker acknowledgement itself, with the invariant refusal captured
    /// as text rather than propagated -- the pins discriminate on WHICH answer
    /// came back, so the fatal one has to be observable, not a `?`.
    fn marker_ack(&self) -> Result<ServerValue, String> {
        dispatch(
            &self.handler,
            self.connection,
            ClientRequest::MarkerAck(MarkerAck {
                conversation_id: self.conversation_id,
                participant_id: self.participant_id,
                capability_generation: self.generation,
                marker_delivery_seq: self.marker_seq,
            }),
        )
        .map_err(|error| error.to_string())
    }

    fn holds_marker_obligation(&self) -> Result<bool, Box<dyn Error>> {
        let cell = self.handler.cell(self.conversation_id)?;
        let owner = cell
            .lock()
            .map_err(|_| "booted conversation owner lock was poisoned")?;
        let authority = owner.as_ref().ok_or("booted conversation owner was absent")?;
        let outbox = authority.outbox.as_ref().ok_or("booted outbox was absent")?;
        let held = outbox.is_marker_obligation(self.participant_id, self.marker_seq);
        drop(owner);
        Ok(held)
    }

    fn replay_orphan_reconciles(&self) -> Result<u64, Box<dyn Error>> {
        let cell = self.handler.cell(self.conversation_id)?;
        let owner = cell
            .lock()
            .map_err(|_| "booted conversation owner lock was poisoned")?;
        let authority = owner.as_ref().ok_or("booted conversation owner was absent")?;
        let fired = authority.replay_orphan_reconciles.get();
        drop(owner);
        Ok(fired)
    }

    fn committed_admissions(&self) -> Result<usize, Box<dyn Error>> {
        committed_admissions(&self.handler, self.conversation_id)
    }
}

fn committed_admissions(
    handler: &ProductionParticipantHandler,
    conversation_id: u64,
) -> Result<usize, Box<dyn Error>> {
    let cell = handler.cell(conversation_id)?;
    let owner = cell
        .lock()
        .map_err(|_| "conversation owner lock was poisoned")?;
    let authority = owner.as_ref().ok_or("conversation owner was absent")?;
    let count = authority.committed_admissions.len();
    drop(owner);
    Ok(count)
}

/// The benign arms: a typed refusal the client receives on an open connection.
/// Anything else -- including a fatal `Err` -- fails the pin.
fn assert_benign_marker_answer(answer: &Result<ServerValue, String>, context: &str) {
    match answer {
        Ok(
            ServerValue::MarkerMismatch(_)
            | ServerValue::MarkerNotDelivered(_)
            | ServerValue::AckNoOp(_),
        ) => {}
        Ok(other) => panic!("{context}: marker ack answered {other:?}, not a typed refusal"),
        Err(error) => {
            assert!(
                !error.contains(NO_MATCHING_AUTHORITY),
                "{context}: the marker ack died at the coherence invariant -- {error}"
            );
            panic!("{context}: marker ack failed -- {error}");
        }
    }
}

/// PIN 1. The stranded obligation is retired at load, so the marker is never
/// re-offered and its acknowledgement lands on the benign arm -- and the
/// conversation is still serving afterwards.
///
/// RED against the unfixed tree: the marker IS re-offered, the offer is
/// recorded, and the ack dies at [`NO_MATCHING_AUTHORITY`].
#[test]
fn a_stranded_marker_obligation_is_retired_at_load_and_its_ack_answers_a_typed_refusal()
-> Result<(), Box<dyn Error>> {
    let stranded = strand_marker_obligation(Arc::new(open_ephemeral(1)?))?;
    let booted = BootedMember::boot(&stranded, 41, MemberCredential::enrolled(&stranded.surviving))?;

    // ORDER IS LOAD-BEARING. The walk runs first because it is what ARMS the
    // defect (it records the offer on the unfixed tree), and the ack runs
    // before every structural assertion because the ack IS the incident: on
    // the unfixed tree this pin has to die at `marker_progress`'s invariant,
    // not at a tidier statement about the outbox one line earlier.
    let offers = booted.walk_offers()?;
    let answer = booted.marker_ack();
    assert_benign_marker_answer(&answer, "stranded obligation, first boot");

    assert!(
        !booted.holds_marker_obligation()?,
        "the load-end reconcile left the marker obligation live, so nothing stops the re-offer"
    );
    assert!(
        !offers.contains(&stranded.marker_seq),
        "the marker at {} was re-offered after its authority was retired: offers {offers:?}",
        stranded.marker_seq
    );
    assert!(
        !offers.is_empty(),
        "POSITIVE CONTROL: the walk offered nothing at all, so `the marker was not offered` \
         would be vacuous -- the surviving member must still be owed its other deliveries"
    );

    // LIVENESS: the estate is UP, not merely un-crashed on one request. The
    // member's remaining obligations still acknowledge.
    let through_seq = offers
        .iter()
        .copied()
        .max()
        .ok_or("the surviving member was owed nothing to acknowledge")?;
    let acked = dispatch(
        &booted.handler,
        booted.connection,
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id: booted.conversation_id,
            participant_id: booted.participant_id,
            capability_generation: booted.generation,
            through_seq,
        }),
    )?;
    assert!(
        matches!(acked, ServerValue::AckCommitted(_)),
        "the conversation did not keep serving after the marker refusal: {acked:?}"
    );
    Ok(())
}

/// PIN 2, THE QUIET ESTATE. Not one ordinary commit happens between load-ready
/// and the marker acknowledgement, on either boot.
///
/// This is the pin that turns "not an accepted migration cost" into a
/// measurement. Pre-fix the answer is identical on boot 1 and boot 2 -- the
/// death is a property of the durable bytes, not of what traffic happens to
/// arrive, so a one-boot "self-heal" is an accident of a commit landing, not a
/// cure. Post-fix the FIRST boot is already clean, with `committed_admissions`
/// proving nothing was committed to make it so.
///
/// RED against the unfixed tree: both boots die at [`NO_MATCHING_AUTHORITY`].
#[test]
fn the_quiet_estate_answers_the_first_marker_ack_of_every_boot_with_no_commit_in_between()
-> Result<(), Box<dyn Error>> {
    let stranded = strand_marker_obligation(Arc::new(open_ephemeral(1)?))?;
    let mut credential = MemberCredential::enrolled(&stranded.surviving);
    for (boot_index, ordinal) in [(1_u32, 51_u64), (2, 52)] {
        let booted = BootedMember::boot(&stranded, ordinal, credential)?;
        credential = booted.next_credential;
        let context = format!("quiet estate, boot {boot_index}");
        let offers = booted.walk_offers()?;
        let answer = booted.marker_ack();
        assert_benign_marker_answer(&answer, &context);
        assert!(
            !offers.contains(&stranded.marker_seq),
            "{context}: the marker at {} was re-offered: offers {offers:?}",
            stranded.marker_seq
        );
        assert_eq!(
            booted.committed_admissions()?,
            booted.admissions_at_load,
            "{context}: an ordinary admission committed between load-ready and the marker ack, \
             so this boot is not the quiet one it claims to be"
        );
        assert_eq!(
            booted.replay_orphan_reconciles()?,
            0,
            "{context}: MEASUREMENT, not an assumption -- this store exercises the LOAD-END \
             reconcile site, and a nonzero count here means the pin is describing the wrong one"
        );
    }
    Ok(())
}

/// PIN 3, THE BACKED PATH IS UNTOUCHED. The same fixture with nobody leaving:
/// the marker record is still on the frontier, so BOTH recipients keep their
/// obligations across the load-end reconcile and the exact offered
/// acknowledgement still commits.
///
/// The co-recipient is the load-bearing half. It is a recipient of a marker it
/// does not own and can never itself acknowledge -- a coherence predicate
/// keyed on OWNERSHIP rather than on the record's existence would eat that
/// obligation and silently stop delivering the compaction record. It is
/// asserted here so that widening cannot pass.
#[test]
fn a_backed_marker_obligation_survives_the_load_end_reconcile_and_its_ack_still_commits()
-> Result<(), Box<dyn Error>> {
    let fixture = prepare_marker_fixture()?;
    let conversation_id = fixture.marker_delivery.conversation_id;
    let marker_seq = fixture.marker_delivery.delivery_seq;
    let owner_id = fixture.target_participant;
    let co_recipient_id = if owner_id == fixture.record_participant {
        fixture.catchup_participant
    } else {
        fixture.record_participant
    };
    let owner_receipt = fixture
        .enrolled
        .iter()
        .find(|bound| bound.participant_id() == owner_id)
        .cloned()
        .ok_or("fixture carries no receipt for the marker owner")?;
    let store = Arc::clone(&fixture.store);
    drop(fixture);

    let handler = ProductionParticipantHandler::new(store, marker_fixture_config())?;
    {
        let cell = handler.cell(conversation_id)?;
        let owner = cell
            .lock()
            .map_err(|_| "booted conversation owner lock was poisoned")?;
        let authority = owner.as_ref().ok_or("booted conversation owner was absent")?;
        let outbox = authority.outbox.as_ref().ok_or("booted outbox was absent")?;
        assert!(
            outbox.is_marker_obligation(owner_id, marker_seq),
            "the load-end reconcile retired the marker OWNER's live obligation"
        );
        assert!(
            outbox.is_marker_obligation(co_recipient_id, marker_seq),
            "the load-end reconcile retired the CO-RECIPIENT's live obligation -- a predicate \
             keyed on marker ownership rather than on the record's survival"
        );
        assert_eq!(
            authority.replay_orphan_reconciles.get(),
            0,
            "MEASUREMENT: the healthy store must not take the mid-replay retry at all"
        );
        drop(owner);
    }

    let connection = ConnectionIncarnation::new(0xA7, 61);
    let attached = dispatch(
        &handler,
        connection,
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id,
            participant_id: owner_id,
            capability_generation: Generation::ONE,
            attach_secret: owner_receipt.attach_secret(),
            attach_attempt_token: AttachAttemptToken::new([0xC3; 16]),
            accept_marker_delivery_seq: None,
        }),
    )?;
    let ServerValue::AttachBound(bound) = attached else {
        return Err(format!("the marker owner did not attach: {attached:?}").into());
    };
    let generation = bound.origin_binding_epoch().capability_generation;

    let mut offered = None;
    let mut reached = false;
    for _ in 0..16 {
        let Some(publication) = handler.next_publication(connection, conversation_id, offered)?
        else {
            break;
        };
        offered = Some(ParticipantOfferedProgress {
            binding_epoch: publication.binding_epoch,
            through_seq: publication.delivery_seq(),
        });
        if publication.delivery_seq() == marker_seq {
            handler.record_publication_offer(&publication)?;
            reached = true;
            break;
        }
    }
    assert!(
        reached,
        "the backed marker was never offered to its own owner after a boot"
    );

    let committed = dispatch(
        &handler,
        connection,
        ClientRequest::MarkerAck(MarkerAck {
            conversation_id,
            participant_id: owner_id,
            capability_generation: generation,
            marker_delivery_seq: marker_seq,
        }),
    )?;
    assert!(
        matches!(committed, ServerValue::MarkerAckCommitted(_)),
        "the backed marker acknowledgement stopped committing: {committed:?}"
    );
    Ok(())
}

/// Total bytes of every regular file under `path`, recursively. The disk arm's
/// instrument: a number that can only grow because something was WRITTEN.
fn tree_bytes(path: &Path) -> Result<u64, Box<dyn Error>> {
    let mut total = 0_u64;
    if !path.exists() {
        return Ok(0);
    }
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            total = total.saturating_add(tree_bytes(&entry.path())?);
        } else {
            total = total.saturating_add(metadata.len());
        }
    }
    Ok(total)
}

/// PIN 4, THE DISK ARM. Pin 2's quiet-estate measurement driven against a
/// haematite database this test owns on disk, with an instrument control
/// proving the store actually engaged.
///
/// The control is absent-before / grown-after on the store's own directory:
/// nothing exists at the path before the database is created, and the fixture
/// drive strictly GROWS the bytes under it. Without that control "the disk
/// store was used" is a claim about a constructor argument; with it, it is a
/// measurement of the file system.
#[test]
fn the_quiet_estate_answer_holds_over_a_disk_store_that_provably_engaged()
-> Result<(), Box<dyn Error>> {
    let root = tempfile::tempdir()?;
    let data_dir = root.path().join("durability");
    assert!(
        !data_dir.exists(),
        "INSTRUMENT CONTROL: the store directory already existed before the store was created"
    );

    let database = Database::create(DatabaseConfig {
        data_dir: data_dir.clone(),
        shard_count: 1,
        distributed: None,
        executor_threads: None,
    })?;
    let store: Arc<dyn DurableStore> =
        Arc::new(HaematiteStore::new(Arc::new(EventStore::new(database))));
    let empty_bytes = tree_bytes(&data_dir)?;

    let stranded = strand_marker_obligation(store)?;
    let driven_bytes = tree_bytes(&data_dir)?;
    assert!(
        driven_bytes > empty_bytes,
        "INSTRUMENT CONTROL: driving the fixture wrote nothing to the disk store at {} \
         ({empty_bytes} bytes before, {driven_bytes} after), so this arm never engaged it",
        data_dir.display()
    );

    let booted = BootedMember::boot(&stranded, 71, MemberCredential::enrolled(&stranded.surviving))?;
    let offers = booted.walk_offers()?;
    let answer = booted.marker_ack();
    assert_benign_marker_answer(&answer, "disk store, first boot");
    assert!(
        !offers.contains(&stranded.marker_seq),
        "disk store, first boot: the marker at {} was re-offered: offers {offers:?}",
        stranded.marker_seq
    );
    assert_eq!(
        booted.committed_admissions()?,
        booted.admissions_at_load,
        "disk store: an ordinary admission committed between load-ready and the marker ack"
    );
    Ok(())
}
