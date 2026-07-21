use std::cell::Cell;
use std::error::Error;
use std::sync::Arc;

use liminal::durability::bridge::block_on;
use liminal_protocol::lifecycle::{BindingState, PendingFinalization};
use liminal_protocol::wire::{
    AttachSecret, BindingEpoch, ConnectionIncarnation, LeaveAttemptToken, LeaveRequest,
};

use crate::server::participant::{ConnectionFateClass, ConnectionFateWorkItem};

use super::ProductionParticipantHandler;
use super::log::{
    DecodedStoredOperation, OperationLog, OperationLogError, StoredDetachedCause,
    StoredFinalizerPresentation, StoredLeaveV3, StoredOperation, StoredTerminalDisposition,
};
use super::state::DurableAppend;
use super::tests_w1b_pending_died_restart::{
    bound_debt_fixture, extend_leave_capacity, operation_facts,
};

struct SourceOnlyAppender<'a> {
    log: &'a OperationLog,
    source_flushed: Cell<bool>,
}

impl DurableAppend for SourceOnlyAppender<'_> {
    fn append(
        &self,
        operation: &StoredOperation,
        expected_sequence: u64,
    ) -> Result<(), OperationLogError> {
        if self.source_flushed.replace(true) {
            return Err(OperationLogError::FateReplayDrift {
                sequence: expected_sequence,
            });
        }
        block_on(self.log.append(operation, expected_sequence))?
    }
}

struct LogAppender<'a> {
    log: &'a OperationLog,
}

impl DurableAppend for LogAppender<'_> {
    fn append(
        &self,
        operation: &StoredOperation,
        expected_sequence: u64,
    ) -> Result<(), OperationLogError> {
        block_on(self.log.append(operation, expected_sequence))?
    }
}

struct PendingDetachedLeaveFixture {
    handler: ProductionParticipantHandler,
    log: OperationLog,
    connection: ConnectionIncarnation,
    conversation_id: u64,
    participant_id: u64,
    binding_epoch: BindingEpoch,
    attach_secret: AttachSecret,
    detached_source_sequence: u64,
    terminal_order: u64,
    terminal_delivery_seq: u64,
}

#[test]
pub(super) fn pending_detached_finalized_by_leave_presents_only_live_leave_commit()
-> Result<(), Box<dyn Error>> {
    let fixture = pending_detached_leave_fixture()?;
    let left_source_sequence = fixture
        .detached_source_sequence
        .checked_add(1)
        .ok_or("pending Detached Left source overflow")?;
    commit_pending_detached_leave(&fixture)?;

    let detached = block_on(fixture.log.read_at(fixture.detached_source_sequence))??
        .ok_or("pending Detached source row is absent")?;
    let DecodedStoredOperation::V3(StoredOperation::Detached { row: detached }) =
        detached.operation
    else {
        return Err("pending Detached fixture appended the wrong source".into());
    };
    assert_eq!(detached.cause, StoredDetachedCause::ServerShutdown);
    assert_eq!(detached.disposition, StoredTerminalDisposition::Pending);
    assert_eq!(detached.terminal_order, fixture.terminal_order);

    let left = block_on(fixture.log.read_at(left_source_sequence))??
        .ok_or("pending Detached Left row is absent")?;
    let DecodedStoredOperation::V3(StoredOperation::Left { row: left }) = left.operation else {
        return Err("pending Detached finalizer did not append Left".into());
    };
    assert_eq!(
        left.pending_source_sequence,
        Some(fixture.detached_source_sequence)
    );
    assert_eq!(
        left.finalizer_presentation,
        StoredFinalizerPresentation::PresentEnclosing
    );
    assert_eq!(
        left.prior_terminal_delivery_seq,
        Some(fixture.terminal_delivery_seq)
    );
    assert!(block_on(fixture.log.read_at(left_source_sequence + 1))??.is_none());
    Ok(())
}

#[test]
fn leave_finalizer_resolves_pending_source_without_claiming_stored_cause()
-> Result<(), Box<dyn Error>> {
    let fixture = pending_detached_leave_fixture()?;
    commit_pending_detached_leave(&fixture)?;
    let left_source_sequence = fixture
        .detached_source_sequence
        .checked_add(1)
        .ok_or("cause-honest Left source overflow")?;

    let detached = block_on(fixture.log.read_at(fixture.detached_source_sequence))??
        .ok_or("cause-honest pending source is absent")?;
    let DecodedStoredOperation::V3(StoredOperation::Detached { row: detached }) =
        detached.operation
    else {
        return Err("cause-honest fixture expected Detached source".into());
    };
    assert_eq!(detached.binding_epoch, fixture.binding_epoch.into());
    assert_eq!(detached.cause, StoredDetachedCause::ServerShutdown);
    assert_eq!(detached.terminal_order, fixture.terminal_order);
    assert_eq!(detached.disposition, StoredTerminalDisposition::Pending);

    let left = block_on(fixture.log.read_at(left_source_sequence))??
        .ok_or("cause-honest Left source is absent")?;
    let DecodedStoredOperation::V3(StoredOperation::Left { row: left }) = left.operation else {
        return Err("cause-honest fixture expected Left finalizer".into());
    };
    assert_eq!(left.ended_binding_epoch, None);
    assert_eq!(
        left.pending_source_sequence,
        Some(fixture.detached_source_sequence)
    );
    assert_eq!(
        left.prior_terminal_delivery_seq,
        Some(fixture.terminal_delivery_seq)
    );
    assert_eq!(
        left.finalizer_presentation,
        StoredFinalizerPresentation::PresentEnclosing
    );

    let StoredLeaveV3 {
        request: _,
        request_verifier: _,
        receiving_epoch: _,
        left_transaction_order: _,
        left_delivery_seq: _,
        ended_binding_epoch: _,
        prior_terminal_delivery_seq: _,
        pending_source_sequence: _,
        finalizer_presentation: _,
    } = left;
    Ok(())
}

fn pending_detached_leave_fixture() -> Result<PendingDetachedLeaveFixture, Box<dyn Error>> {
    let setup = bound_debt_fixture(
        71,
        ConnectionIncarnation::new(101, 3),
        ConnectionIncarnation::new(101, 4),
        None,
    )?;
    let log = OperationLog::new(Arc::clone(&setup.handler.store), setup.conversation_id);
    let cell = setup.handler.cell(setup.conversation_id)?;
    let (detached_source_sequence, terminal_order, terminal_delivery_seq) = {
        let mut owner = cell
            .lock()
            .map_err(|_| "pending Detached owner lock was poisoned")?;
        let authority = owner
            .as_mut()
            .ok_or("pending Detached owner was unavailable")?;
        let source_sequence = authority.next_log_sequence;
        let terminal_delivery_seq = authority.next_seq;
        authority
            .prepare_connection_fate_transaction(&ConnectionFateWorkItem {
                open_sequence: 43,
                connection_incarnation: setup.connection,
                class: ConnectionFateClass::ServerShutdown,
                tracked_conversations: setup.conversations.tracked_conversations(),
            })
            .complete(
                authority,
                &SourceOnlyAppender {
                    log: &log,
                    source_flushed: Cell::new(false),
                },
            )?;
        let pending = authority
            .slots
            .get(&setup.participant_id)
            .and_then(|slot| match slot.binding {
                BindingState::PendingFinalization(PendingFinalization::Detached(pending)) => {
                    Some(pending)
                }
                BindingState::PendingFinalization(PendingFinalization::Died(_))
                | BindingState::Bound(_)
                | BindingState::Detached => None,
            })
            .ok_or("ServerShutdown selector did not produce Pending Detached")?;
        let terminal_order = pending.admission_order().transaction_order();
        drop(owner);
        (source_sequence, terminal_order, terminal_delivery_seq)
    };
    Ok(PendingDetachedLeaveFixture {
        handler: setup.handler,
        log,
        connection: setup.connection,
        conversation_id: setup.conversation_id,
        participant_id: setup.participant_id,
        binding_epoch: setup.binding_epoch,
        attach_secret: setup.attach_secret,
        detached_source_sequence,
        terminal_order,
        terminal_delivery_seq,
    })
}

fn commit_pending_detached_leave(
    fixture: &PendingDetachedLeaveFixture,
) -> Result<(), Box<dyn Error>> {
    let cell = fixture.handler.cell(fixture.conversation_id)?;
    let mut owner = cell
        .lock()
        .map_err(|_| "pending Detached finalizer lock was poisoned")?;
    let authority = owner
        .as_mut()
        .ok_or("pending Detached finalizer owner was unavailable")?;
    let pending = authority
        .slots
        .get(&fixture.participant_id)
        .and_then(|slot| match slot.binding {
            BindingState::PendingFinalization(pending) => Some(pending),
            BindingState::Bound(_) | BindingState::Detached => None,
        })
        .ok_or("pending Detached finalizer lost its binding")?;
    extend_leave_capacity(authority, pending)?;
    drop(authority.take_observer_progress_witnesses());
    authority.apply_leave(
        &LeaveRequest {
            conversation_id: fixture.conversation_id,
            participant_id: fixture.participant_id,
            capability_generation: fixture.binding_epoch.capability_generation,
            attach_secret: fixture.attach_secret,
            leave_attempt_token: LeaveAttemptToken::new([95; 16]),
        },
        &operation_facts(fixture.connection)?,
        &LogAppender { log: &fixture.log },
    )?;
    assert!(authority.retired.contains_key(&fixture.participant_id));
    assert!(
        !authority
            .pending_specific_fates
            .contains_key(&fixture.participant_id)
    );
    let witnesses = authority.take_observer_progress_witnesses();
    let [leave_witness] = witnesses.as_slice() else {
        return Err(format!("pending Detached Leave witnesses: {witnesses:?}").into());
    };
    assert_eq!(leave_witness.progress(), authority.observer_progress);
    drop(owner);
    Ok(())
}
