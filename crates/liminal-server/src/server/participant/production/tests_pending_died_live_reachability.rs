//! REPRODUCTION: is the live `PendingFinalization::Died` residence reachable
//! with NO crash and NO restart, and what does it do to attach and to the
//! next admitted record?
//!
//! Everything here goes through the production seam. Nothing hand-builds a
//! `BindingState`, nothing injects a residence, and no appender is cut short:
//! the fate transaction runs through
//! [`ProductionParticipantHandler::apply_connection_fate_with_impacts`] — the
//! exact function `handle_connection_fate` calls at `handler_semantic.rs:192`
//! — against the handler's own durable appender.
//!
//! The residence is minted by the two-part condition in
//! `liminal-protocol/src/lifecycle/operations/binding_terminal.rs`: retention
//! is at its cap (`has_capacity` false, `:188`/`:192`) AND a hard observer
//! still lags the terminal's delivery seq (`:212`).
//! `max_retained_record_rows = 4` plus enroll/attach/enroll/record puts
//! retention exactly at cap; the peer enrolls and never acks, so it lags.

use std::error::Error;
use std::sync::Arc;

use liminal::durability::{DurableStore, open_ephemeral};
use liminal_protocol::lifecycle::{BindingState, PendingFinalization};
use liminal_protocol::wire::{
    AttachAttemptToken, AttachSecret, BindingEpoch, ClientRequest, ConnectionIncarnation,
    CredentialAttachRequest, EnrollmentRequest, EnrollmentToken, Generation, ParticipantAck,
    RecordAdmission, RecordAdmissionAttemptToken, ServerValue,
};

use crate::server::participant::{
    ConnectionFateClass, ConnectionFateWorkItem, ParticipantConnectionConversations,
};

use super::ProductionParticipantHandler;
use super::tests::{dispatch_tracked, test_participant_config};

const CONVERSATION: u64 = 4_101;
const VICTIM_ENROLLMENT_TOKEN: [u8; 16] = [0xD1; 16];
const PEER_ENROLLMENT_TOKEN: [u8; 16] = [0xD2; 16];

/// A live conversation whose victim binding rests at
/// `PendingFinalization(Died(..))` — built only by running production code.
struct LiveResidence {
    handler: ProductionParticipantHandler,
    peer_conversations: ParticipantConnectionConversations,
    victim_connection: ConnectionIncarnation,
    peer_connection: ConnectionIncarnation,
    victim_id: u64,
    peer_id: u64,
    victim_epoch: BindingEpoch,
    victim_attach_secret: AttachSecret,
    victim_record_delivery_seq: u64,
}

fn small_retention_config() -> crate::config::types::ParticipantConfig {
    let mut config = test_participant_config();
    config.max_retained_record_rows = 4;
    config
}

/// Drives the production seam to the live pending-Died residence.
///
/// No `SourceOnlyAppender`, no restart, no replay: the connection-fate
/// transaction is completed in full against the handler's real appender and
/// the resulting IN-MEMORY authority is what the assertions read.
fn mint_live_pending_died_residence() -> Result<LiveResidence, Box<dyn Error>> {
    build_residence(true)
}

/// The CONTROL: the identical fixture with the connection-fate step skipped,
/// so the victim binding is still `Bound`. Any refusal that shows up here is
/// not caused by the pending residence.
fn bound_control_residence() -> Result<LiveResidence, Box<dyn Error>> {
    build_residence(false)
}

fn build_residence(kill_the_connection: bool) -> Result<LiveResidence, Box<dyn Error>> {
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let handler = ProductionParticipantHandler::new(store, small_retention_config())?;

    let victim_connection = ConnectionIncarnation::new(511, 1);
    let peer_connection = ConnectionIncarnation::new(511, 2);
    let mut victim_conversations = ParticipantConnectionConversations::default();
    let mut peer_conversations = ParticipantConnectionConversations::default();

    let enrolled = dispatch_tracked(
        &handler,
        victim_connection,
        &mut victim_conversations,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: CONVERSATION,
            enrollment_token: EnrollmentToken::new(VICTIM_ENROLLMENT_TOKEN),
        }),
    )?;
    let ServerValue::EnrollBound(victim) = enrolled else {
        return Err(format!("victim enrollment did not bind: {enrolled:?}").into());
    };

    let attached = dispatch_tracked(
        &handler,
        victim_connection,
        &mut victim_conversations,
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: CONVERSATION,
            participant_id: victim.participant_id(),
            capability_generation: Generation::ONE,
            attach_secret: victim.attach_secret(),
            attach_attempt_token: AttachAttemptToken::new([0xD3; 16]),
            accept_marker_delivery_seq: None,
        }),
    )?;
    let ServerValue::AttachBound(attached) = attached else {
        return Err(format!("victim attach did not bind: {attached:?}").into());
    };

    // The lagging hard observer: enrolled, never acked.
    let peer = dispatch_tracked(
        &handler,
        peer_connection,
        &mut peer_conversations,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: CONVERSATION,
            enrollment_token: EnrollmentToken::new(PEER_ENROLLMENT_TOKEN),
        }),
    )?;
    let ServerValue::EnrollBound(peer) = peer else {
        return Err(format!("peer enrollment did not bind: {peer:?}").into());
    };
    assert_ne!(peer.participant_id(), victim.participant_id());

    // Fourth retained row: retention now rests at its configured cap, so the
    // terminal that connection loss is about to charge has no capacity.
    let record = dispatch_tracked(
        &handler,
        victim_connection,
        &mut victim_conversations,
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: CONVERSATION,
            participant_id: victim.participant_id(),
            capability_generation: attached.origin_binding_epoch().capability_generation,
            record_admission_attempt_token: RecordAdmissionAttemptToken::new([0xD4; 16]),
            payload: vec![0xD5],
        }),
    )?;
    let ServerValue::RecordCommitted(record) = record else {
        return Err(format!("victim debt record did not commit: {record:?}").into());
    };
    let victim_record_delivery_seq = record.delivery_seq();

    // THE LIVE EVENT: the victim's transport dies. This is the production
    // entry point, with the production appender. Nothing is cut short.
    if kill_the_connection {
        handler
            .apply_connection_fate_with_impacts(&ConnectionFateWorkItem {
                open_sequence: 41,
                connection_incarnation: victim_connection,
                class: ConnectionFateClass::ConnectionLost,
                tracked_conversations: victim_conversations.tracked_conversations(),
            })
            .into_result()?;
    }

    Ok(LiveResidence {
        handler,
        peer_conversations,
        victim_connection,
        peer_connection,
        victim_id: victim.participant_id(),
        peer_id: peer.participant_id(),
        victim_epoch: attached.origin_binding_epoch(),
        victim_attach_secret: attached.attach_secret(),
        victim_record_delivery_seq,
    })
}

/// Reads the victim's binding state out of the LIVE in-memory authority.
fn live_binding_state(residence: &LiveResidence) -> Result<Option<BindingState>, Box<dyn Error>> {
    let cell = residence.handler.cell(CONVERSATION)?;
    let owner = cell
        .lock()
        .map_err(|_| "live residence conversation lock was poisoned")?;
    let authority = owner
        .as_ref()
        .ok_or("live residence conversation owner was discarded")?;
    let binding = authority
        .slots
        .get(&residence.victim_id)
        .map(|slot| slot.binding);
    drop(owner);
    Ok(binding)
}

/// Reports whether the live authority still maps `token` to any participant.
fn live_token_resolves(
    residence: &LiveResidence,
    token: [u8; 16],
) -> Result<Option<u64>, Box<dyn Error>> {
    let cell = residence.handler.cell(CONVERSATION)?;
    let owner = cell
        .lock()
        .map_err(|_| "live residence conversation lock was poisoned")?;
    let authority = owner
        .as_ref()
        .ok_or("live residence conversation owner was discarded")?;
    let resolved = authority
        .tokens
        .iter()
        .find(|(mapped_token, _)| **mapped_token == token)
        .map(|(_, participant_id)| *participant_id);
    drop(owner);
    Ok(resolved)
}

/// (a) A live connection loss — no crash, no restart — leaves the binding
/// resting at `PendingFinalization(Died(ConnectionLost))`.
#[test]
fn live_connection_loss_rests_the_binding_at_pending_finalization_died()
-> Result<(), Box<dyn Error>> {
    let residence = mint_live_pending_died_residence()?;
    let binding =
        live_binding_state(&residence)?.ok_or("victim slot vanished from the live authority")?;
    let BindingState::PendingFinalization(PendingFinalization::Died(pending)) = binding else {
        return Err(
            format!("live connection loss did not rest at pending Died: {binding:?}").into(),
        );
    };
    assert_eq!(pending.binding_epoch(), residence.victim_epoch);
    assert_eq!(
        pending.cause(),
        liminal_protocol::wire::DiedCause::ConnectionLost
    );
    Ok(())
}

/// Issues the victim's exact-secret re-attach on a fresh connection and
/// renders whatever the seam produced as a single string.
fn reattach_report(residence: &LiveResidence) -> String {
    let outcome = dispatch_tracked(
        &residence.handler,
        ConnectionIncarnation::new(511, 7),
        &mut ParticipantConnectionConversations::default(),
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: CONVERSATION,
            participant_id: residence.victim_id,
            capability_generation: residence.victim_epoch.capability_generation,
            attach_secret: residence.victim_attach_secret,
            attach_attempt_token: AttachAttemptToken::new([0xD6; 16]),
            accept_marker_delivery_seq: None,
        }),
    );
    match outcome {
        Ok(value) => format!("RESPONDED {value:?}"),
        Err(error) => format!("FATAL {error}"),
    }
}

const ATTACH_INVARIANT: &str =
    "pending finalization observed in a binding that commits detaches immediately";

/// (b) While the binding rests there, a fresh credential attach with the exact
/// valid secret hits the `PendingFinalization` refusal in
/// `ops_attach::allocate_attach_mode` — which the #14 funnel now PRESENTS as a
/// wire-visible `ObserverBackpressure` response (`StateError::PresentedRefusal`
/// raised at the arm, converted in `handler_semantic`) instead of a bare fatal
/// carrying the invariant text. The SAME request against the same fixture with
/// the binding still Bound takes neither exit.
#[test]
fn reattach_against_the_live_pending_died_residence_is_refused_on_the_wire()
-> Result<(), Box<dyn Error>> {
    let residence = mint_live_pending_died_residence()?;
    assert!(matches!(
        live_binding_state(&residence)?,
        Some(BindingState::PendingFinalization(
            PendingFinalization::Died(_)
        ))
    ));
    let pending_report = reattach_report(&residence);

    // Negative control: identical fixture, identical request, binding Bound.
    let control = bound_control_residence()?;
    assert!(
        matches!(live_binding_state(&control)?, Some(BindingState::Bound(_))),
        "the control fixture was not left Bound"
    );
    let control_report = reattach_report(&control);
    eprintln!("PENDING re-attach  => {pending_report}");
    eprintln!("BOUND CONTROL re-attach => {control_report}");

    assert!(
        pending_report.starts_with("RESPONDED") && pending_report.contains("ObserverBackpressure"),
        "re-attach against the live pending-Died residence was not refused on the wire as ObserverBackpressure; it produced: {pending_report}"
    );
    assert!(
        !pending_report.contains(ATTACH_INVARIANT),
        "the invariant text leaked to the client past the funnel: {pending_report}"
    );
    assert!(
        !control_report.contains("ObserverBackpressure")
            && !control_report.contains(ATTACH_INVARIANT),
        "the Bound control took the pending residence's exit too, so the discriminator is not the pending residence; control produced: {control_report}"
    );
    Ok(())
}

/// (c) The next record admitted by another participant drains the residence,
/// erasing the victim's identity: the slot is gone and its enrollment token no
/// longer resolves.
#[test]
fn peer_record_drains_the_live_residence_and_erases_the_victim_identity()
-> Result<(), Box<dyn Error>> {
    let mut residence = mint_live_pending_died_residence()?;
    assert!(matches!(
        live_binding_state(&residence)?,
        Some(BindingState::PendingFinalization(
            PendingFinalization::Died(_)
        ))
    ));
    assert_eq!(
        live_token_resolves(&residence, VICTIM_ENROLLMENT_TOKEN)?,
        Some(residence.victim_id),
        "the victim's enrollment token should still resolve before the drain"
    );

    let peer_connection = residence.peer_connection;
    let peer_id = residence.peer_id;
    let mut peer_conversations = std::mem::take(&mut residence.peer_conversations);
    let committed = dispatch_tracked(
        &residence.handler,
        peer_connection,
        &mut peer_conversations,
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: CONVERSATION,
            participant_id: peer_id,
            capability_generation: Generation::ONE,
            record_admission_attempt_token: RecordAdmissionAttemptToken::new([0xD7; 16]),
            payload: vec![0xD8, 0xD9],
        }),
    )?;
    if !matches!(committed, ServerValue::RecordCommitted(_)) {
        return Err(format!("the draining peer record did not commit: {committed:?}").into());
    }
    residence.peer_conversations = peer_conversations;

    assert_eq!(
        live_binding_state(&residence)?,
        None,
        "the drain did not remove the victim's binding slot"
    );
    assert_eq!(
        live_token_resolves(&residence, VICTIM_ENROLLMENT_TOKEN)?,
        None,
        "the drain left the victim's enrollment token still resolving"
    );

    // A later probe answers ParticipantUnknown.
    let probe = dispatch_tracked(
        &residence.handler,
        ConnectionIncarnation::new(511, 9),
        &mut ParticipantConnectionConversations::default(),
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: CONVERSATION,
            participant_id: residence.victim_id,
            capability_generation: residence.victim_epoch.capability_generation,
            attach_secret: residence.victim_attach_secret,
            attach_attempt_token: AttachAttemptToken::new([0xDA; 16]),
            accept_marker_delivery_seq: None,
        }),
    )?;
    assert!(
        matches!(probe, ServerValue::ParticipantUnknown(_)),
        "post-drain attach probe did not answer ParticipantUnknown: {probe:?}"
    );

    Ok(())
}

/// (b, unresolved) Is the `allocate_attach_mode` invariant an INDEPENDENT wall,
/// or is it masking a co-occurring `RetainedRecordLimit` refusal that would fire
/// anyway?
///
/// NOT SETTLED — and this test records exactly why. The residence exists only
/// because retention is at its cap (`binding_terminal.rs:192` requires
/// `!has_capacity`), and the Bound control shows attach faults on retention at
/// that same cap. To separate the two you need a state where the residence
/// exists but retention has room, and this test could not build one: acking is
/// the only reclaim lever available from the client surface, the lagging peer's
/// ack commits WITHOUT freeing retention, the residence survives it, and in the
/// Bound control the victim's own ack answers `AckGap { current_cursor: 0 }` so
/// even a fully-acked control still faults on `RetainedRecordLimit`.
///
/// What IS asserted is the observed inseparability plus the discrimination that
/// does hold: the pending residence decides the OUTCOME — a wire-visible
/// `ObserverBackpressure` refusal (the #14 funnel presenting the
/// `allocate_attach_mode` refusal) versus the retention fault.
#[test]
fn attach_invariant_and_retention_wall_are_inseparable_at_this_configuration()
-> Result<(), Box<dyn Error>> {
    let mut residence = mint_live_pending_died_residence()?;
    let peer_connection = residence.peer_connection;
    let peer_id = residence.peer_id;
    let through_seq = residence.victim_record_delivery_seq;
    let mut peer_conversations = std::mem::take(&mut residence.peer_conversations);
    let acked = dispatch_tracked(
        &residence.handler,
        peer_connection,
        &mut peer_conversations,
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id: CONVERSATION,
            participant_id: peer_id,
            capability_generation: Generation::ONE,
            through_seq,
        }),
    );
    residence.peer_conversations = peer_conversations;
    let ack_report = match acked {
        Ok(value) => format!("RESPONDED {value:?}"),
        Err(error) => format!("FATAL {error}"),
    };
    let binding_after_ack = live_binding_state(&residence)?;
    let attach_after_ack = reattach_report(&residence);

    // The CONTROL asks whether the retention wall is clearable AT ALL: the
    // same fixture with the victim still Bound, and BOTH participants acking
    // through the record. If the control's attach stops faulting on
    // RetainedRecordLimit, reclaim works and the pending case's surviving
    // retention wall is attributable to the dead victim's frozen cursor.
    let mut control = bound_control_residence()?;
    let control_peer_connection = control.peer_connection;
    let control_victim_connection = control.victim_connection;
    let control_peer_id = control.peer_id;
    let control_victim_id = control.victim_id;
    let control_generation = control.victim_epoch.capability_generation;
    let control_through_seq = control.victim_record_delivery_seq;
    let mut control_peer_conversations = std::mem::take(&mut control.peer_conversations);
    let control_peer_ack = dispatch_tracked(
        &control.handler,
        control_peer_connection,
        &mut control_peer_conversations,
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id: CONVERSATION,
            participant_id: control_peer_id,
            capability_generation: Generation::ONE,
            through_seq: control_through_seq,
        }),
    );
    control.peer_conversations = control_peer_conversations;
    let control_victim_ack = dispatch_tracked(
        &control.handler,
        control_victim_connection,
        &mut ParticipantConnectionConversations::default(),
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id: CONVERSATION,
            participant_id: control_victim_id,
            capability_generation: control_generation,
            through_seq: control_through_seq,
        }),
    );
    let control_ack_report = format!("peer={control_peer_ack:?} victim={control_victim_ack:?}");
    let control_attach_after_ack = reattach_report(&control);

    eprintln!("PENDING  peer ack          => {ack_report}");
    eprintln!("PENDING  binding after ack => {binding_after_ack:?}");
    eprintln!("PENDING  attach after ack  => {attach_after_ack}");
    eprintln!("CONTROL  both acks         => {control_ack_report}");
    eprintln!("CONTROL  attach after acks => {control_attach_after_ack}");

    // THE WALL, asserted rather than narrated: acking does not reclaim
    // retention, so the separating state is unbuildable from this surface.
    assert!(
        control_attach_after_ack.contains("RetainedRecordLimit"),
        "retention reclaim became reachable — the separating state may now be buildable and this test's premise is stale; control produced: {control_attach_after_ack}"
    );
    // The residence outlives the ack: it is not an ack-clearable condition.
    assert!(
        matches!(
            binding_after_ack,
            Some(BindingState::PendingFinalization(
                PendingFinalization::Died(_)
            ))
        ),
        "the peer ack dissolved the pending residence: {binding_after_ack:?}"
    );
    // And the discrimination that DOES hold: same request, same cap, and the
    // pending residence decides the outcome the client gets — the funnel's
    // wire-visible refusal versus the retention fault.
    assert!(
        attach_after_ack.starts_with("RESPONDED")
            && attach_after_ack.contains("ObserverBackpressure"),
        "re-attach against the pending residence was not refused on the wire as ObserverBackpressure; it produced: {attach_after_ack}"
    );
    assert!(
        !control_attach_after_ack.contains("ObserverBackpressure")
            && !control_attach_after_ack.contains(ATTACH_INVARIANT),
        "the Bound control took the pending residence's exit: {control_attach_after_ack}"
    );
    Ok(())
}

/// Issues one enrollment on a throwaway connection and renders the result.
fn enroll_report(residence: &LiveResidence, token: [u8; 16], incarnation: u64) -> String {
    let outcome = dispatch_tracked(
        &residence.handler,
        ConnectionIncarnation::new(511, incarnation),
        &mut ParticipantConnectionConversations::default(),
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: CONVERSATION,
            enrollment_token: EnrollmentToken::new(token),
        }),
    );
    match outcome {
        Ok(value) => format!("RESPONDED {value:?}"),
        Err(error) => format!("FATAL {error}"),
    }
}

/// (c, tail) The "re-enrollment with the same token mints a fresh identity"
/// half of the `release_drained_binding_slot` doc claim is NOT observable at
/// this configuration — and the control shows why: after the drain, retention
/// is still at its cap, so EVERY enrollment is refused, the erased token and a
/// never-seen token alike. The wall is `RetainedRecordLimit`, not identity.
#[test]
fn post_drain_reenrollment_is_walled_by_retention_not_by_identity() -> Result<(), Box<dyn Error>> {
    let mut residence = mint_live_pending_died_residence()?;
    let peer_connection = residence.peer_connection;
    let peer_id = residence.peer_id;
    let mut peer_conversations = std::mem::take(&mut residence.peer_conversations);
    let committed = dispatch_tracked(
        &residence.handler,
        peer_connection,
        &mut peer_conversations,
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: CONVERSATION,
            participant_id: peer_id,
            capability_generation: Generation::ONE,
            record_admission_attempt_token: RecordAdmissionAttemptToken::new([0xE1; 16]),
            payload: vec![0xE2],
        }),
    )?;
    if !matches!(committed, ServerValue::RecordCommitted(_)) {
        return Err(format!("the draining peer record did not commit: {committed:?}").into());
    }
    residence.peer_conversations = peer_conversations;
    assert_eq!(live_binding_state(&residence)?, None);

    let erased_token = enroll_report(&residence, VICTIM_ENROLLMENT_TOKEN, 10);
    let unrelated_token = enroll_report(&residence, [0xEE; 16], 11);
    eprintln!("ERASED token enroll    => {erased_token}");
    eprintln!("UNRELATED token enroll => {unrelated_token}");
    assert!(
        erased_token.contains("RetainedRecordLimit"),
        "post-drain re-enrollment with the erased token was expected to hit the retention wall; it produced: {erased_token}"
    );
    assert!(
        unrelated_token.contains("RetainedRecordLimit"),
        "a never-seen enrollment token was expected to hit the SAME retention wall (proving the refusal is not about the erased identity); it produced: {unrelated_token}"
    );
    Ok(())
}
