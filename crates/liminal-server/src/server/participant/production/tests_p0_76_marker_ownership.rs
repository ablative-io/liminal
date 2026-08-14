//! `#76` — MARKER OWNERSHIP. A `HistoryCompacted` record names exactly one
//! participant; only that participant holds a marker obligation for it.
//!
//! Lane brief: `docs/design/MARKER-OWNERSHIP-BUILD.md`.
//!
//! CONTRACT (R-C3 @`60530fd`, the brief's reading). Every member's entitled
//! subsequence is the full per-conversation sequence, so `HistoryCompacted` is
//! delivered to EVERY member as a record. But the `MarkerAck` route is
//! authorized only by "delivery of `HistoryCompacted { participant_id, ... }`"
//! naming the acker's OWN broken history: both authorized routes atomically
//! advance THAT participant's cursor to the marker sequence, and "every other
//! attempt spanning abandonment is refused". A non-owner has no abandonment to
//! span — its history over the marker's sequence is continuous, and is covered
//! by ordinary cumulative `ParticipantAck`. So a non-owner holds NO marker
//! obligation, and the record's delivery to it is an ORDINARY obligation.
//!
//! THE DEFECT these pins were authored red against (@`60530fd`):
//! `ConversationOutbox::is_marker_obligation` (`outbox/selection.rs:126-139`)
//! answered "yes" for every RECIPIENT of a `HistoryCompacted`, never comparing
//! the marker's own `affected_participant_id`. Its sole consumer,
//! `record_publication_offer` (`handler_semantic.rs:453-498`), therefore minted
//! an `offered_markers` entry for every survivor, and a survivor that then sent
//! a `MarkerAck` walked into `marker_progress.rs:76-78` —
//! "stored `MarkerAck` has no matching marker delivery authority" — the fatal
//! invariant that took the kernel down in the field boot-1 log
//! (`.manifold/kernel-boot-20260814-1547.log`, delivery 829, marker naming
//! participant 5, ack held by the registry at participant 0).
//!
//! THE INVARIANT IS NOT TOUCHED BY THIS LANE. It is correct. The fix removes
//! the unlawful OFFER that walked a non-owner into it.
//!
//! NON-VACUITY. Every pin below derives the non-owner from the fixture's own
//! `affected_participant_id` rather than naming a participant, and refuses
//! loudly if the fixture ever stops minting the two-role geometry (a marker
//! whose owner is one member and whose recipient set includes another). A pin
//! that silently stopped reaching the marker would be indistinguishable from a
//! pin whose defect is fixed.

use std::error::Error;
use std::path::Path;
use std::sync::Arc;

use liminal::durability::bridge::block_on;
use liminal::durability::{DurableStore, open_ephemeral};

use liminal_protocol::wire::{
    ClientRequest, ConnectionIncarnation, EnrollmentRequest, EnrollmentToken, LeaveAttemptToken,
    LeaveRequest, MarkerAck, ParticipantDelivery, ParticipantId, ParticipantRecord, RecordAdmission,
    RecordAdmissionAttemptToken, ServerPush, ServerValue,
};

use crate::server::participant::{
    ParticipantOfferedProgress, ParticipantPublication, ParticipantSemanticHandler,
};

use super::ProductionParticipantHandler;
use super::e2e_cold_all_shapes_fixture::{ColdMember, ack_through, expect_enrolled};
use super::e2e_tests::{SocketFixture, SocketPeer};
use super::outbox_log::{OutboxLog, OutboxRow};
use super::tests::{dispatch, open_disk_store_for_tests};
use super::tests_marker_ack_fixture::{
    MarkerFixture, marker_fixture_config, prepare_marker_fixture, prepare_marker_fixture_with_store,
};

/// The socket arm's own conversation, distinct from every other fixture's.
const OWNERSHIP_MARKER_CONVERSATION: u64 = 0x76_01;

/// The exact invariant text a non-owner's `MarkerAck` reached in the field.
/// Quoted from `marker_progress.rs:76-78` so a rename cannot silently retire
/// the assertion — if that string moves, this pin must be re-pointed
/// deliberately.
const DELIVERY_AUTHORITY_INVARIANT: &str = "stored MarkerAck has no matching marker delivery authority";

/// The two identities the marker geometry requires, derived from the fixture
/// rather than assumed: the participant the marker NAMES, and a second member
/// that merely RECEIVES it.
#[derive(Clone, Copy, Debug)]
struct OwnershipRoles {
    conversation_id: u64,
    marker_delivery_seq: u64,
    owner_participant: ParticipantId,
    non_owner_connection: ConnectionIncarnation,
    non_owner_participant: ParticipantId,
}

/// Names the roles, and REFUSES if the fixture no longer mints both.
fn ownership_roles(fixture: &MarkerFixture) -> Result<OwnershipRoles, Box<dyn Error>> {
    let owner_participant = fixture.target_participant;
    let ParticipantRecord::HistoryCompacted {
        affected_participant_id,
        ..
    } = fixture.marker_delivery.record
    else {
        return Err(format!(
            "the marker fixture's marker delivery is not a HistoryCompacted record: {:?}",
            fixture.marker_delivery.record
        )
        .into());
    };
    if affected_participant_id != owner_participant {
        return Err(format!(
            "the marker fixture's target {owner_participant} is not the participant its marker \
             NAMES ({affected_participant_id}); this file's whole subject is that distinction and \
             it cannot be measured from a fixture that has lost it"
        )
        .into());
    }
    let (non_owner_connection, non_owner_participant) =
        if owner_participant == fixture.record_participant {
            (fixture.catchup_connection, fixture.catchup_participant)
        } else if owner_participant == fixture.catchup_participant {
            (fixture.record_connection, fixture.record_participant)
        } else {
            return Err(format!(
                "the marker fixture targeted {owner_participant}, which is neither of its two \
                 members — the two-role geometry these pins measure is not built"
            )
            .into());
        };
    if non_owner_participant == owner_participant {
        return Err("the ownership question needs two distinct identities".into());
    }
    Ok(OwnershipRoles {
        conversation_id: fixture.marker_delivery.conversation_id,
        marker_delivery_seq: fixture.marker_delivery.delivery_seq,
        owner_participant,
        non_owner_connection,
        non_owner_participant,
    })
}

/// ARMING ASSERTION. The marker record must actually list the non-owner among
/// its recipients, or nothing below is reachable: `delivery_after` would never
/// hand the non-owner the marker at all and every pin would pass while
/// measuring an empty walk.
fn assert_non_owner_is_a_recipient(
    handler: &ProductionParticipantHandler,
    roles: &OwnershipRoles,
) -> Result<(), Box<dyn Error>> {
    let cell = handler.cell(roles.conversation_id)?;
    let owner = cell
        .lock()
        .map_err(|_| "ownership arming owner lock was poisoned")?;
    let authority = owner
        .as_ref()
        .ok_or("ownership arming conversation owner was absent")?;
    let outbox = authority
        .outbox
        .as_ref()
        .ok_or("ownership arming outbox was absent")?;
    let delivered = outbox
        .delivery_after(
            roles.non_owner_participant,
            roles.marker_delivery_seq.saturating_sub(1),
        )
        .map(|delivery| delivery.delivery_seq);
    drop(owner);
    if delivered != Some(roles.marker_delivery_seq) {
        return Err(format!(
            "NOT ARMED: the marker at {} is not the non-owner {}'s next obligation \
             (it sees {delivered:?}), so the marker is not delivered to a survivor at all and \
             these pins would witness NOTHING",
            roles.marker_delivery_seq, roles.non_owner_participant
        )
        .into());
    }
    Ok(())
}

/// Walks one connection's publications until the marker sequence is reached and
/// returns that publication, WITHOUT recording any offer.
///
/// Refuses rather than returning `None`: a walk that never reaches the marker
/// is a fixture failure, not a measurement.
fn publication_at_marker(
    handler: &ProductionParticipantHandler,
    connection: ConnectionIncarnation,
    roles: &OwnershipRoles,
) -> Result<ParticipantPublication, Box<dyn Error>> {
    let mut offered = None;
    for _ in 0..16 {
        let publication = handler
            .next_publication(connection, roles.conversation_id, offered)?
            .ok_or_else(|| {
                format!(
                    "publications for {connection:?} ended before the marker at {}",
                    roles.marker_delivery_seq
                )
            })?;
        offered = Some(ParticipantOfferedProgress {
            binding_epoch: publication.binding_epoch,
            through_seq: publication.delivery_seq(),
        });
        if publication.delivery_seq() == roles.marker_delivery_seq {
            return Ok(publication);
        }
    }
    Err(format!(
        "the walk for {connection:?} did not reach the marker at {} within its signed bound",
        roles.marker_delivery_seq
    )
    .into())
}

/// Whether an `offered_markers` entry exists for exactly this pair.
fn holds_marker_offer(
    handler: &ProductionParticipantHandler,
    conversation_id: u64,
    participant_id: ParticipantId,
    delivery_seq: u64,
) -> Result<bool, Box<dyn Error>> {
    let cell = handler.cell(conversation_id)?;
    let owner = cell
        .lock()
        .map_err(|_| "offered-marker inspection owner lock was poisoned")?;
    let authority = owner
        .as_ref()
        .ok_or("offered-marker inspection conversation owner was absent")?;
    let held = authority
        .offered_markers
        .contains_key(&(participant_id, delivery_seq));
    drop(owner);
    Ok(held)
}

/// PIN 1 (brief pin 1, offer half). A `HistoryCompacted` publication whose
/// named participant is NOT the recipient must mint no marker offer.
///
/// RED at `60530fd`: `is_marker_obligation` answered on recipiency alone, so
/// `record_publication_offer` inserted `(non_owner, marker_seq)` into
/// `offered_markers` and the survivor was handed a marker obligation the
/// contract never gave it.
#[test]
fn a_non_owner_history_compacted_delivery_mints_no_marker_offer() -> Result<(), Box<dyn Error>> {
    let fixture = prepare_marker_fixture()?;
    let roles = ownership_roles(&fixture)?;
    assert_non_owner_is_a_recipient(&fixture.handler, &roles)?;

    let publication = publication_at_marker(&fixture.handler, roles.non_owner_connection, &roles)?;
    if publication.participant_id != roles.non_owner_participant {
        return Err(format!(
            "the walked publication belongs to {} rather than the non-owner {}",
            publication.participant_id, roles.non_owner_participant
        )
        .into());
    }
    fixture.handler.record_publication_offer(&publication)?;

    if holds_marker_offer(
        &fixture.handler,
        roles.conversation_id,
        roles.non_owner_participant,
        roles.marker_delivery_seq,
    )? {
        return Err(format!(
            "#76: the offer path minted a MARKER obligation for participant {}, which merely \
             RECEIVES the compaction marker at {} — the record NAMES participant {}. A survivor's \
             history over that sequence is continuous, so it has no abandonment to span and the \
             contract gives it no marker route; this delivery is an ordinary obligation covered by \
             cumulative ParticipantAck.",
            roles.non_owner_participant, roles.marker_delivery_seq, roles.owner_participant
        )
        .into());
    }
    Ok(())
}

/// PIN 2 (brief pin 1, ack half + brief pin 3's consequence). The survivor's
/// `MarkerAck` must never reach the delivery-authority invariant.
///
/// RED at `60530fd` with the field's exact string. The point is not that the
/// ack is refused — it is that the refusal is a TYPED wire answer the client
/// can act on, never a `StateError::invariant` that fails the operation.
#[test]
fn a_non_owner_marker_ack_never_reaches_the_delivery_authority_invariant()
-> Result<(), Box<dyn Error>> {
    let fixture = prepare_marker_fixture()?;
    let roles = ownership_roles(&fixture)?;
    assert_non_owner_is_a_recipient(&fixture.handler, &roles)?;

    let publication = publication_at_marker(&fixture.handler, roles.non_owner_connection, &roles)?;
    let epoch = publication.binding_epoch;
    fixture.handler.record_publication_offer(&publication)?;

    // Addressed at the epoch the offer was actually made under, so the ack
    // cannot miss for a reason unrelated to ownership.
    let answered = dispatch(
        &fixture.handler,
        epoch.connection_incarnation,
        ClientRequest::MarkerAck(MarkerAck {
            conversation_id: roles.conversation_id,
            participant_id: roles.non_owner_participant,
            capability_generation: epoch.capability_generation,
            marker_delivery_seq: roles.marker_delivery_seq,
        }),
    );
    let answered = match answered {
        Ok(value) => value,
        Err(error) => {
            let text = error.to_string();
            if text.contains(DELIVERY_AUTHORITY_INVARIANT) {
                return Err(format!(
                    "#76 FIELD SHAPE REPRODUCED: participant {}'s MarkerAck for the marker at {} \
                     — a marker NAMING participant {} — died at the delivery-authority invariant \
                     `{DELIVERY_AUTHORITY_INVARIANT}`. This is boot-1's death: non-owner offered, \
                     non-owner acked, invariant fatal. Raw: {text}",
                    roles.non_owner_participant, roles.marker_delivery_seq, roles.owner_participant
                )
                .into());
            }
            return Err(format!(
                "the non-owner's MarkerAck failed for an unexpected reason (not the delivery \
                 authority invariant): {text}"
            )
            .into());
        }
    };
    if matches!(answered, ServerValue::MarkerAckCommitted(_)) {
        return Err(format!(
            "#76: participant {}'s MarkerAck COMMITTED against a marker naming participant {} — \
             an ack that can never advance the newer cursor of another binding: {answered:?}",
            roles.non_owner_participant, roles.owner_participant
        )
        .into());
    }
    Ok(())
}

/// PIN 3 — THE CONSUMER'S ARM (brief fix-shape item 2, the key-holder
/// amendment). Ordinary delivery of the record to a survivor must SURVIVE the
/// offer path.
///
/// `record_publication_offer` runs for EVERY `HistoryCompacted` publication and
/// treats `!current || !obligation` as an INTERNAL ERROR. Putting the ownership
/// condition only inside `is_marker_obligation` makes every lawful survivor
/// delivery hit `obligation == false` and ERROR — record delivery to survivors
/// breaks, which is worse than the defect. The ownership question therefore
/// decides marker-vs-ordinary BEFORE that guard, and the Internal error stays
/// reserved for OWNER-marker publications that genuinely lost binding or
/// obligation.
///
/// This pin was authored red against an intermediate tree carrying fix item 1
/// ALONE (`gate-logs/marker-ownership/03-consumer-arm-red-item1-only.log`), the
/// only tree at which the failure it names can exist. It is green at
/// `60530fd` and green at the lane tip, and the two greens mean opposite
/// things: "the defect is present" and "the fix kept the record flowing".
#[test]
fn a_non_owner_history_compacted_offer_is_ordinary_not_an_internal_error()
-> Result<(), Box<dyn Error>> {
    let fixture = prepare_marker_fixture()?;
    let roles = ownership_roles(&fixture)?;
    assert_non_owner_is_a_recipient(&fixture.handler, &roles)?;

    let publication = publication_at_marker(&fixture.handler, roles.non_owner_connection, &roles)?;
    fixture
        .handler
        .record_publication_offer(&publication)
        .map_err(|error| {
            format!(
                "#76 CONSUMER'S ARM: the offer path REFUSED an ordinary delivery of the \
                 compaction marker at {} to survivor {} — a record every member is entitled to \
                 receive. `not a marker for this recipient` is not `lost authority`, and the \
                 Internal error is reserved for the latter: {error:?}",
                roles.marker_delivery_seq, roles.non_owner_participant
            )
        })?;
    Ok(())
}

/// PIN 4 (brief pin 2). The OWNER's path is untouched: its marker still offers,
/// and its `MarkerAck` still commits, advancing its cursor to the marker
/// sequence.
///
/// GREEN AT BOTH TREES BY DESIGN, and declared as such: this is a regression
/// guard against an over-broad fix, not a red-first pin. Its evidence value is
/// that it is asserted at the same instant as the pins above, from the same
/// fixture, so an ownership condition that accidentally silenced the OWNER
/// could not hide behind them.
#[test]
fn the_owner_still_holds_its_marker_offer_and_its_ack_still_commits() -> Result<(), Box<dyn Error>>
{
    let fixture = prepare_marker_fixture()?;
    let roles = ownership_roles(&fixture)?;

    let publication = publication_at_marker(&fixture.handler, fixture.target_connection, &roles)?;
    let epoch = publication.binding_epoch;
    fixture.handler.record_publication_offer(&publication)?;

    if !holds_marker_offer(
        &fixture.handler,
        roles.conversation_id,
        roles.owner_participant,
        roles.marker_delivery_seq,
    )? {
        return Err(format!(
            "#76 OVER-FIX: the marker at {} names participant {} and was offered to it, yet no \
             marker obligation was minted. The owner's route is the one the contract authorizes.",
            roles.marker_delivery_seq, roles.owner_participant
        )
        .into());
    }
    let committed = dispatch(
        &fixture.handler,
        epoch.connection_incarnation,
        ClientRequest::MarkerAck(MarkerAck {
            conversation_id: roles.conversation_id,
            participant_id: roles.owner_participant,
            capability_generation: epoch.capability_generation,
            marker_delivery_seq: roles.marker_delivery_seq,
        }),
    )?;
    let ServerValue::MarkerAckCommitted(_) = committed else {
        return Err(format!(
            "#76 OVER-FIX: the marker owner {}'s own MarkerAck did not commit: {committed:?}",
            roles.owner_participant
        )
        .into());
    };
    let cell = fixture.handler.cell(roles.conversation_id)?;
    let owner = cell
        .lock()
        .map_err(|_| "owner-path cursor inspection lock was poisoned")?;
    let authority = owner
        .as_ref()
        .ok_or("owner-path cursor inspection conversation owner was absent")?;
    let cursor = authority
        .slots
        .get(&roles.owner_participant)
        .ok_or("the marker owner lost its slot")?
        .member
        .cursor();
    drop(owner);
    if cursor < roles.marker_delivery_seq {
        return Err(format!(
            "#76 OVER-FIX: the owner's committed MarkerAck left its cursor at {cursor}, below the \
             marker sequence {} it is required to advance to",
            roles.marker_delivery_seq
        )
        .into());
    }
    Ok(())
}

/// Reads the marker record's `affected_participant_id` out of a pushed
/// delivery.
fn named_participant(delivery: &ParticipantDelivery) -> Result<ParticipantId, Box<dyn Error>> {
    let ParticipantRecord::HistoryCompacted {
        affected_participant_id,
        ..
    } = delivery.record
    else {
        return Err(format!("pushed delivery was not a compaction marker: {delivery:?}").into());
    };
    Ok(affected_participant_id)
}

/// Drives the compaction-marker geometry over a REAL TCP socket server and
/// returns the marker delivery both members received.
///
/// The shape is `e2e_cold_all_shapes::fill_marker_history`'s, on its own
/// conversation: three members, two ack to a common prefix, the third leaves,
/// then four record admissions build and drain the marker debt while only the
/// second member keeps acking — so the first member's history is the one that
/// gets compacted.
fn drive_socket_marker(
    first: &mut SocketFixture,
    second: &mut SocketPeer,
    transient: &mut SocketPeer,
) -> Result<(ColdMember, ColdMember, ParticipantDelivery), Box<dyn Error>> {
    let member_a = ColdMember::enrolled(&expect_enrolled(
        first.request(ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: OWNERSHIP_MARKER_CONVERSATION,
            enrollment_token: EnrollmentToken::new([0x71; 16]),
        }))?,
        "ownership A",
    )?);
    let member_b = ColdMember::enrolled(&expect_enrolled(
        second.request(ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: OWNERSHIP_MARKER_CONVERSATION,
            enrollment_token: EnrollmentToken::new([0x72; 16]),
        }))?,
        "ownership B",
    )?);
    let member_c = ColdMember::enrolled(&expect_enrolled(
        transient.request(ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: OWNERSHIP_MARKER_CONVERSATION,
            enrollment_token: EnrollmentToken::new([0x73; 16]),
        }))?,
        "ownership C",
    )?);
    ack_through(first, OWNERSHIP_MARKER_CONVERSATION, member_a, 3)?;
    ack_through(second, OWNERSHIP_MARKER_CONVERSATION, member_b, 3)?;
    let left = transient.request(ClientRequest::Leave(LeaveRequest {
        conversation_id: OWNERSHIP_MARKER_CONVERSATION,
        participant_id: member_c.participant_id,
        capability_generation: member_c.generation,
        attach_secret: member_c.secret,
        leave_attempt_token: LeaveAttemptToken::new([0x74; 16]),
    }))?;
    let ServerValue::LeaveCommitted(left) = left else {
        return Err(format!("ownership C Leave did not commit: {left:?}").into());
    };
    ack_through(
        first,
        OWNERSHIP_MARKER_CONVERSATION,
        member_a,
        left.left_delivery_seq(),
    )?;
    ack_through(
        second,
        OWNERSHIP_MARKER_CONVERSATION,
        member_b,
        left.left_delivery_seq(),
    )?;
    let mut latest_record = 0_u64;
    for token in [0x75, 0x76, 0x77, 0x78] {
        if token == 0x78 {
            first.open_publication_replay()?;
        }
        let outcome = first.request(ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: OWNERSHIP_MARKER_CONVERSATION,
            participant_id: member_a.participant_id,
            capability_generation: member_a.generation,
            record_admission_attempt_token: RecordAdmissionAttemptToken::new([token; 16]),
            payload: vec![token],
        }))?;
        let ServerValue::RecordCommitted(committed) = outcome else {
            return Err(format!("marker-driving record {token:#x} failed: {outcome:?}").into());
        };
        latest_record = committed.delivery_seq();
        if token != 0x78 {
            ack_through(
                second,
                OWNERSHIP_MARKER_CONVERSATION,
                member_b,
                latest_record,
            )?;
        }
    }
    let ServerPush::ParticipantDelivery(marker_on_a) = first.read_push()? else {
        return Err("ownership A did not receive the generated marker".into());
    };
    let ServerPush::ParticipantDelivery(marker_on_b) = second.read_push()? else {
        return Err("ownership B did not receive the generated marker".into());
    };
    if marker_on_a != marker_on_b {
        return Err(format!(
            "the two survivors received different markers: {marker_on_a:?} vs {marker_on_b:?}"
        )
        .into());
    }
    let ServerPush::ParticipantDelivery(post_marker) = second.read_push()? else {
        return Err("ownership B did not receive the post-marker ordinary record".into());
    };
    if post_marker.delivery_seq != latest_record {
        return Err(format!(
            "the post-marker record arrived at {} rather than {latest_record}",
            post_marker.delivery_seq
        )
        .into());
    }
    Ok((member_a, member_b, marker_on_a))
}

/// PIN 5 (brief pin 4) — THE LEGACY-CLIENT ARM, ON A REAL TCP TRANSPORT.
///
/// A client built before this fix sends `MarkerAck` for any `HistoryCompacted`
/// it receives, its own or not. That client must be answered BENIGNLY — the
/// existing `NoMarkerExpected` re-sync — and never fatally.
///
/// The distinction is only visible at a real transport, which is why this pin
/// pays for a socket server: `ParticipantDispatch::Fatal` closes the connection
/// while "staying silent", so the client receives NO FRAME AT ALL. RED at
/// `60530fd`, where the survivor's delivery was marker-flagged on the way out
/// and its ack walked into the delivery-authority invariant; the red presents
/// as the request never being answered.
#[test]
fn a_legacy_non_owner_marker_ack_is_answered_benignly_over_a_real_socket()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let mut first = SocketFixture::start_replay_gated_with_config(
        &data_dir,
        marker_fixture_config(),
    )?;
    let mut second = first.spawn_peer()?;
    let mut transient = first.spawn_peer()?;

    let (member_a, member_b, marker) = drive_socket_marker(&mut first, &mut second, &mut transient)?;
    let named = named_participant(&marker)?;

    // The non-owner is whichever survivor the marker does NOT name. Derived,
    // never assumed.
    let (non_owner, answered) = if named == member_a.participant_id {
        (
            member_b,
            second.request(ClientRequest::MarkerAck(MarkerAck {
                conversation_id: OWNERSHIP_MARKER_CONVERSATION,
                participant_id: member_b.participant_id,
                capability_generation: member_b.generation,
                marker_delivery_seq: marker.delivery_seq,
            })),
        )
    } else if named == member_b.participant_id {
        (
            member_a,
            first.request(ClientRequest::MarkerAck(MarkerAck {
                conversation_id: OWNERSHIP_MARKER_CONVERSATION,
                participant_id: member_a.participant_id,
                capability_generation: member_a.generation,
                marker_delivery_seq: marker.delivery_seq,
            })),
        )
    } else {
        return Err(format!(
            "the generated marker named {named}, neither live member ({} / {})",
            member_a.participant_id, member_b.participant_id
        )
        .into());
    };

    let answered = answered.map_err(|error| {
        format!(
            "#76 FIELD SHAPE AT THE TRANSPORT: participant {}'s MarkerAck for the marker at {} — \
             a marker NAMING participant {named} — was never answered on the wire. A survivor's \
             legacy ack must be a typed re-sync, not a silent fatal close: {error}",
            non_owner.participant_id, marker.delivery_seq
        )
    })?;
    if matches!(answered, ServerValue::MarkerAckCommitted(_)) {
        return Err(format!(
            "#76: survivor {}'s MarkerAck COMMITTED against a marker naming {named}: {answered:?}",
            non_owner.participant_id
        )
        .into());
    }
    if !matches!(
        answered,
        ServerValue::MarkerMismatch(_) | ServerValue::MarkerNotDelivered(_) | ServerValue::AckNoOp(_)
    ) {
        return Err(format!(
            "survivor {}'s legacy MarkerAck was answered {answered:?}, which is not one of the \
             benign marker re-sync answers",
            non_owner.participant_id
        )
        .into());
    }

    // The record itself is still theirs to ack ordinarily: the whole reason a
    // survivor holds no marker obligation is that cumulative ack covers it.
    ack_through(
        &mut first,
        OWNERSHIP_MARKER_CONVERSATION,
        member_a,
        marker.delivery_seq,
    )?;
    first.stop();
    Ok(())
}

/// The restart-parity body, shared by the in-memory and on-disk arms so both
/// measure the SAME predicate rather than two hand-written approximations.
///
/// Mints the marker geometry over `store`, drops every live handle, reopens the
/// same store, and asks the reloaded authority the ownership question on the
/// REPLAY-DERIVED offer path.
fn assert_restart_never_marker_flags_a_non_owner(
    store: &Arc<dyn DurableStore>,
) -> Result<OwnershipRoles, Box<dyn Error>> {
    let roles = {
        let fixture = prepare_marker_fixture_with_store(Arc::clone(store))?;
        let roles = ownership_roles(&fixture)?;
        assert_non_owner_is_a_recipient(&fixture.handler, &roles)?;
        roles
    };

    // FIRST BOOT over a store whose history holds a marker for the owner while
    // the non-owner is live. It must load at all — the field's boot-1 did not.
    let reloaded = ProductionParticipantHandler::new(Arc::clone(store), marker_fixture_config())
        .map_err(|error| {
            format!(
                "#76 RESTART PARITY: a store holding a compaction marker for participant {} with \
                 survivor {} live did not load: {error}",
                roles.owner_participant, roles.non_owner_participant
            )
        })?;

    if holds_marker_offer(
        &reloaded,
        roles.conversation_id,
        roles.non_owner_participant,
        roles.marker_delivery_seq,
    )? {
        return Err(format!(
            "#76 RESTART PARITY: the reloaded authority came up already holding a marker offer \
             for survivor {} at {}",
            roles.non_owner_participant, roles.marker_delivery_seq
        )
        .into());
    }

    // The replay-derived offer path must be governed by the same predicate.
    assert_non_owner_is_a_recipient(&reloaded, &roles)?;
    let publication = publication_at_marker(&reloaded, roles.non_owner_connection, &roles)?;
    reloaded
        .record_publication_offer(&publication)
        .map_err(|error| {
            format!(
                "#76 RESTART PARITY, CONSUMER'S ARM: the reloaded offer path refused ordinary \
                 delivery of the marker at {} to survivor {}: {error:?}",
                roles.marker_delivery_seq, roles.non_owner_participant
            )
        })?;
    if holds_marker_offer(
        &reloaded,
        roles.conversation_id,
        roles.non_owner_participant,
        roles.marker_delivery_seq,
    )? {
        return Err(format!(
            "#76 RESTART PARITY: the REPLAY path minted a marker obligation for survivor {} at \
             {} — the ownership predicate must govern replay-derived offers identically to live \
             ones, or a restart re-poisons what the live path refused to mint. The marker names \
             participant {}.",
            roles.non_owner_participant, roles.marker_delivery_seq, roles.owner_participant
        )
        .into());
    }
    Ok(roles)
}

/// PIN 6 (brief pin 5) — RESTART PARITY. A store whose history holds a marker
/// for A with B live loads clean on first boot, and B is never marker-flagged
/// on the replay path either.
///
/// RED at `60530fd` on the replay half: `is_marker_obligation` is a pure
/// function of the restored outbox records, so a reloaded authority answered
/// exactly as wrongly as a live one and the first survivor publication after a
/// restart re-minted the offer the crash had cleared.
#[test]
fn a_restarted_store_never_marker_flags_a_non_owner_on_the_replay_path()
-> Result<(), Box<dyn Error>> {
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    assert_restart_never_marker_flags_a_non_owner(&store)?;
    Ok(())
}

/// The instrument control for the on-disk arm, read at the BYTES: does the
/// outbox stream persisted under `dir` carry a `HistoryCompacted` projection at
/// `delivery_seq`?
///
/// Deliberately NOT asked through a restored handler — a control that consults
/// the same restore path the arm exercises would be verifying itself.
fn disk_holds_marker(
    dir: &Path,
    conversation_id: u64,
    delivery_seq: u64,
) -> Result<bool, Box<dyn Error>> {
    let log = OutboxLog::new(open_disk_store_for_tests(dir)?, conversation_id);
    let rows = block_on(log.read_all())??;
    Ok(rows.iter().any(|(_, row)| match row {
        OutboxRow::Produced(batch) => batch.ordered_records().iter().any(|record| {
            record.delivery_seq() == delivery_seq
                && matches!(record.body(), ParticipantRecord::HistoryCompacted { .. })
        }),
        OutboxRow::AckAdvanced { .. } | OutboxRow::MarkerAckCommitted(_) => false,
    }))
}

/// PIN 7 (brief pin 6) — THE ON-DISK ARM of pin 6, WITH ITS INSTRUMENT CONTROL.
///
/// Pin 6 runs over an ephemeral store, where "reopen" is a handle the process
/// still owns. This arm mints the same geometry into a real haematite database
/// on disk and reloads from that directory — and then PROVES the disk store was
/// engaged, both ways, because an absence is a measurement of the instrument
/// until a known-present case has been detected through the same predicate:
///
/// * POSITIVE — a handler opened on the fixture's directory finds the marker
///   record. It can only have come off disk; nothing in that process wrote it.
/// * NEGATIVE — the identical predicate applied to a fresh empty directory
///   finds nothing. Without this, a predicate that always answered `true` would
///   satisfy the positive control while measuring nothing.
#[test]
fn the_on_disk_restart_arm_never_marker_flags_a_non_owner_with_the_store_engaged()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let roles = assert_restart_never_marker_flags_a_non_owner(&open_disk_store_for_tests(&data_dir)?)?;

    if !data_dir.join("config.json").exists() {
        return Err(format!(
            "INSTRUMENT: no haematite database was created at {}, so this arm never touched a \
             disk store and its restart proved nothing",
            data_dir.display()
        )
        .into());
    }
    if !disk_holds_marker(&data_dir, roles.conversation_id, roles.marker_delivery_seq)? {
        return Err(format!(
            "INSTRUMENT, POSITIVE CONTROL FAILED: a handler opened fresh on {} does not see the \
             marker record at {}, so the marker geometry never reached the disk and the reload \
             above restored nothing",
            data_dir.display(),
            roles.marker_delivery_seq
        )
        .into());
    }
    let empty = tempfile::tempdir()?;
    let empty_dir = empty.path().join("durability");
    if disk_holds_marker(&empty_dir, roles.conversation_id, roles.marker_delivery_seq)? {
        return Err(format!(
            "INSTRUMENT, NEGATIVE CONTROL FAILED: the same predicate reports the marker at {} \
             present in a FRESH EMPTY database, so it does not discriminate and the positive \
             control above is worthless",
            roles.marker_delivery_seq
        )
        .into());
    }
    Ok(())
}
