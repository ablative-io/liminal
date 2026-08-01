//! F8B `R-IDEMPOTENT-PREFIX` and `R-TYPED-REFUSAL`
//! (`docs/design/F8B-INTENT-DEADLOCK.md` §6.3, §6.4).
//!
//! One connection fate spans three tracked conversations. The first two commit
//! their terminal rows; the third is refused because its immutable-candidate
//! lane already holds a pending binding terminal. The durable `Open` is the
//! retry token, so the identical work item is re-run once the lane drains —
//! and that re-run must converge, not double-apply and not refuse.

use std::error::Error;
use std::sync::Arc;

use liminal::durability::{DurableStore, bridge::block_on, open_ephemeral};
use liminal_protocol::lifecycle::BindingTerminalAdmitError;
use liminal_protocol::wire::{
    ClientRequest, ConnectionIncarnation, EnrollmentRequest, EnrollmentToken, Generation,
    RecordAdmission, RecordAdmissionAttemptToken, ServerValue,
};

use crate::server::participant::{
    ConnectionFateClass, ConnectionFateWorkItem, ParticipantConnectionConversations,
    ParticipantSemanticError, ParticipantSemanticHandler,
};

use super::ProductionParticipantHandler;
use super::handler::state_error;
use super::log::{DecodedStoredOperation, OperationLog, STREAM_PREFIX, StoredOperation};
use super::state::StateError;
use super::tests::{dispatch_tracked, test_participant_config};
use super::tests_w1b_intent_recovery::FailOneStreamAppend;
use super::tests_w1b_pending_died_restart::{PendingRestartFixture, pending_restart_fixture};

/// The two conversations whose fate rows commit before the refusal. Both sort
/// ahead of the fixture's own conversation (67), so the canonical work-item
/// order puts the occupied lane last.
const PREFIX_CONVERSATIONS: [(u64, u8); 2] = [(11, 0xC1), (23, 0xC3)];

/// The durable `Open` sequence the re-run replays under.
const REFUSED_OPEN_SEQUENCE: u64 = 101;

fn operation_rows(
    store: &Arc<dyn DurableStore>,
    conversation_id: u64,
) -> Result<Vec<StoredOperation>, Box<dyn Error>> {
    let log = OperationLog::new(Arc::clone(store), conversation_id);
    let mut rows = Vec::new();
    let mut sequence = 0;
    while let Some(entry) = block_on(log.read_at(sequence))?? {
        let DecodedStoredOperation::V3(operation) = entry.operation else {
            return Err(format!("conversation {conversation_id} row {sequence} is not v3").into());
        };
        rows.push(operation);
        sequence = sequence
            .checked_add(1)
            .ok_or("durable log sequence overflowed")?;
    }
    Ok(rows)
}

struct PartlyAppliedFate {
    fixture: PendingRestartFixture,
    work_item: ConnectionFateWorkItem,
}

/// Binds the still-live peer connection into two further conversations, so one
/// connection fate spans a committable prefix and the occupied lane.
fn partly_applied_fate() -> Result<PartlyAppliedFate, Box<dyn Error>> {
    let fixture = pending_restart_fixture()?;
    let mut peer_conversations = ParticipantConnectionConversations::default();
    for (conversation_id, token_byte) in PREFIX_CONVERSATIONS {
        let enrolled = dispatch_tracked(
            &fixture.handler,
            fixture.peer_connection,
            &mut peer_conversations,
            ClientRequest::Enrollment(EnrollmentRequest {
                conversation_id,
                enrollment_token: EnrollmentToken::new([token_byte; 16]),
            }),
        )?;
        if !matches!(enrolled, ServerValue::EnrollBound(_)) {
            return Err(
                format!("prefix conversation {conversation_id} did not bind: {enrolled:?}").into(),
            );
        }
    }
    let mut tracked_conversations: Vec<u64> = PREFIX_CONVERSATIONS
        .iter()
        .map(|(conversation_id, _)| *conversation_id)
        .collect();
    tracked_conversations.push(fixture.conversation_id);
    Ok(PartlyAppliedFate {
        work_item: ConnectionFateWorkItem {
            open_sequence: REFUSED_OPEN_SEQUENCE,
            connection_incarnation: fixture.peer_connection,
            class: ConnectionFateClass::ConnectionLost,
            tracked_conversations,
        },
        fixture,
    })
}

/// Drains the occupied candidate lane exactly as production does: the next
/// successful publish on that conversation, no timer and no restart.
fn drain_the_lane(applied: &PartlyAppliedFate) -> Result<(), Box<dyn Error>> {
    let committed = dispatch_tracked(
        &applied.fixture.handler,
        applied.fixture.peer_connection,
        &mut ParticipantConnectionConversations::default(),
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: applied.fixture.conversation_id,
            participant_id: applied.fixture.peer_participant_id,
            capability_generation: Generation::ONE,
            record_admission_attempt_token: RecordAdmissionAttemptToken::new([0xD1; 16]),
            payload: vec![0xD2],
        }),
    )?;
    if !matches!(committed, ServerValue::RecordCommitted(_)) {
        return Err(format!("the draining publish did not commit: {committed:?}").into());
    }
    Ok(())
}

#[test]
fn partly_applied_connection_fate_reruns_its_committed_prefix_as_a_noop()
-> Result<(), Box<dyn Error>> {
    let applied = partly_applied_fate()?;
    let store = Arc::clone(&applied.fixture.handler.store);

    let refused = applied
        .fixture
        .handler
        .handle_connection_fate(applied.work_item.clone());
    if refused.is_ok() {
        return Err(
            "the occupied candidate lane admitted a second pending binding terminal — this \
             fixture no longer mints the partly-applied fate it exists to pin"
                .into(),
        );
    }

    let prefix_rows = PREFIX_CONVERSATIONS
        .iter()
        .map(|(conversation_id, _)| operation_rows(&store, *conversation_id))
        .collect::<Result<Vec<_>, _>>()?;
    for ((conversation_id, _), rows) in PREFIX_CONVERSATIONS.iter().zip(&prefix_rows) {
        let Some(StoredOperation::Died { row }) = rows.last() else {
            return Err(format!(
                "prefix conversation {conversation_id} did not commit its fate row before the \
                 refusal — the fixture is not partly applied"
            )
            .into());
        };
        if row.connection_intent_sequence != Some(REFUSED_OPEN_SEQUENCE) {
            return Err(format!(
                "prefix conversation {conversation_id} committed a row for another Open"
            )
            .into());
        }
    }

    drain_the_lane(&applied)?;

    let rerun = applied
        .fixture
        .handler
        .handle_connection_fate(applied.work_item.clone());
    // (c) the re-run returns Ok.
    rerun.map_err(|error| {
        format!("re-running the partly applied connection fate refused: {error}")
    })?;

    // (a) conversations 1 and 2 gain no additional durable rows.
    for ((conversation_id, _), before) in PREFIX_CONVERSATIONS.iter().zip(&prefix_rows) {
        let after = operation_rows(&store, *conversation_id)?;
        if &after != before {
            return Err(format!(
                "the re-run double-applied conversation {conversation_id}: {} rows before, {} after",
                before.len(),
                after.len()
            )
            .into());
        }
    }

    // (b) conversation 3 completes under the same durable Open.
    let refused_rows = operation_rows(&store, applied.fixture.conversation_id)?;
    let completed = refused_rows.iter().any(|operation| match operation {
        StoredOperation::Died { row } => {
            row.participant_id == applied.fixture.peer_participant_id
                && row.connection_intent_sequence == Some(REFUSED_OPEN_SEQUENCE)
        }
        _ => false,
    });
    if !completed {
        return Err(
            "the drained conversation never committed its fate row for the replayed Open".into(),
        );
    }
    Ok(())
}

/// R-TYPED-REFUSAL, positive pole. `ConnectionIncarnationAuthority::startup`
/// hands each unmatched durable `Open` to `&dyn ParticipantSemanticHandler`
/// (`connection/incarnation.rs:87-88`) and must decide park-vs-fatal on what
/// comes back. Lane occupancy has to arrive there BY TYPE — a decision taken
/// on a substring of the formatted message is the same defect wearing a
/// remedy's clothes.
#[test]
fn an_occupied_lane_refusal_reaches_the_recovery_consumer_typed() -> Result<(), Box<dyn Error>> {
    let applied = partly_applied_fate()?;
    let consumer: &dyn ParticipantSemanticHandler = &applied.fixture.handler;
    let refused = consumer.handle_connection_fate(applied.work_item.clone());
    let Err(ParticipantSemanticError::BindingTerminalAdmissionRefused {
        error: BindingTerminalAdmitError::Precedence,
    }) = refused
    else {
        return Err(format!(
            "lane occupancy did not reach the recovery consumer as a typed Precedence \
             refusal: {refused:?}"
        )
        .into());
    };
    Ok(())
}

/// R-TYPED-REFUSAL, negative pole ONE: a failure that is not an admission
/// refusal at all must not wear the carrier. One injected durable append
/// failure on the fate's own conversation stream, driven through the same
/// four-site chain the positive pole uses.
#[test]
fn a_durable_append_failure_stays_outside_the_refusal_carrier() -> Result<(), Box<dyn Error>> {
    const CONVERSATION: u64 = 211;
    let inner: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let gated = Arc::new(FailOneStreamAppend::new(inner));
    let store: Arc<dyn DurableStore> = gated.clone();
    let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
    let connection_incarnation = ConnectionIncarnation::new(211, 1);
    let enrolled = dispatch_tracked(
        &handler,
        connection_incarnation,
        &mut ParticipantConnectionConversations::default(),
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: CONVERSATION,
            enrollment_token: EnrollmentToken::new([0xE1; 16]),
        }),
    )?;
    if !matches!(enrolled, ServerValue::EnrollBound(_)) {
        return Err(format!("the append-gate fixture did not bind: {enrolled:?}").into());
    }
    gated.arm(format!("{STREAM_PREFIX}{CONVERSATION}"))?;

    let consumer: &dyn ParticipantSemanticHandler = &handler;
    let failed = consumer.handle_connection_fate(ConnectionFateWorkItem {
        open_sequence: 211,
        connection_incarnation,
        class: ConnectionFateClass::ConnectionLost,
        tracked_conversations: vec![CONVERSATION],
    });
    if !gated.fired() {
        return Err("the injected append gate never fired".into());
    }
    let Err(ParticipantSemanticError::Internal { .. }) = failed else {
        return Err(format!(
            "a durable append failure did not stay outside the refusal carrier: {failed:?}"
        )
        .into());
    };
    Ok(())
}

/// R-TYPED-REFUSAL, negative pole TWO: the carrier discriminates by the
/// protocol's own reason, not by the seat that raised it. Every non-occupancy
/// admission refusal travels the exact production mapping
/// (`handler.rs` `state_error`) and arrives with its own reason intact, so a
/// `Precedence` test at the consumer answers NO for all five.
#[test]
fn a_non_precedence_admission_refusal_keeps_its_own_reason() -> Result<(), Box<dyn Error>> {
    for reason in [
        BindingTerminalAdmitError::CandidateCharge,
        BindingTerminalAdmitError::RetainedRecordLimit,
        BindingTerminalAdmitError::Authority,
        BindingTerminalAdmitError::Frontier,
        BindingTerminalAdmitError::ClosureAccounting,
    ] {
        let carried = state_error(&StateError::BindingTerminalAdmissionRefused { error: reason });
        let ParticipantSemanticError::BindingTerminalAdmissionRefused { error } = carried else {
            return Err(format!("{reason:?} lost the typed carrier: {carried:?}").into());
        };
        if error != reason {
            return Err(format!("the carrier rewrote {reason:?} as {error:?}").into());
        }
        if error == BindingTerminalAdmitError::Precedence {
            return Err(format!("{reason:?} was misread as lane occupancy").into());
        }
    }
    Ok(())
}
