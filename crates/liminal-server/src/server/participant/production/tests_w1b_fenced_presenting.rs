use std::error::Error;
use std::sync::Arc;

use liminal::durability::bridge::block_on;
use liminal_protocol::lifecycle::test_support_external::{
    ExecutablePendingFencedAttach, executable_pending_fenced_attach_after_ordinary_setup,
};

use super::barrier::CommitMode;
use super::log::{
    DecodedStoredOperation, StoredAttachModeV3, StoredDiedCause, StoredFinalizerPresentation,
    StoredOperation, StoredOrdinaryTerminalSource, StoredPendingDiedFinalizer,
};
use super::outbox_projection::project_attached_records;
use super::tests_w1b_fenced_finalizer::{
    FencedAppender, FencedInputs, extend_finalizer_capacity, fenced_inputs, marker_source,
};
use super::tests_w1b_pending_died_restart::{PendingRestartFixture, pending_restart_fixture_acked};

struct CommittedPresentingFenced {
    inputs: FencedInputs,
    attached_source_sequence: u64,
    ordinary_source_sequence: u64,
    terminal_delivery_seq: u64,
    attached_seq: u64,
}

#[test]
fn pending_terminal_composed_by_attach_presents_only_attached_source() -> Result<(), Box<dyn Error>>
{
    let fixture = executable_pending_fenced_attach_after_ordinary_setup()?;
    let setup = pending_restart_fixture_acked(fixture.marker_delivery_seq)?;
    assert_eq!(fixture.member.conversation_id(), setup.conversation_id);
    assert_eq!(fixture.member.participant_id(), setup.participant_id);
    assert_eq!(fixture.prior_binding_epoch, setup.binding_epoch);
    assert_eq!(fixture.terminal_order, setup.terminal_order);
    let committed = commit_presenting_fenced(&setup, fixture)?;
    assert_presenting_fenced_audit(&setup, &committed)
}

fn commit_presenting_fenced(
    setup: &PendingRestartFixture,
    fixture: ExecutablePendingFencedAttach,
) -> Result<CommittedPresentingFenced, Box<dyn Error>> {
    let marker_source_sequence = setup.specific_sequence;
    let attached_source_sequence = marker_source_sequence
        .checked_add(1)
        .ok_or("presenting fenced Attached source overflow")?;
    let ordinary_source_sequence = attached_source_sequence
        .checked_add(1)
        .ok_or("presenting fenced Ordinary source overflow")?;
    let inputs = fenced_inputs(
        setup.conversation_id,
        setup.participant_id,
        setup.binding_epoch,
        setup.died_source_sequence,
        StoredFinalizerPresentation::PresentEnclosing,
        &fixture,
        marker_source_sequence,
    )?;
    let (frontier, marker_row) = marker_source(fixture.owner, fixture.recovery)?;
    block_on(setup.log.append(
        &StoredOperation::MarkerDrained { row: marker_row },
        marker_source_sequence,
    ))??;

    let cell = setup.handler.cell(setup.conversation_id)?;
    {
        let mut owner = cell
            .lock()
            .map_err(|_| "presenting fenced finalizer owner lock was poisoned")?;
        let authority = owner
            .as_mut()
            .ok_or("presenting fenced finalizer owner was unavailable")?;
        authority.frontier = Some(frontier);
        authority.next_seq = fixture.terminal_delivery_seq;
        authority.next_order = fixture.terminal_order;
        authority.next_log_sequence = attached_source_sequence;
        extend_finalizer_capacity(
            authority,
            setup.conversation_id,
            setup.participant_id,
            setup.binding_epoch,
            fixture.terminal_order,
            fixture.terminal_delivery_seq,
            &inputs.allocation,
        )?;
        let slot = authority
            .slots
            .get_mut(&setup.participant_id)
            .ok_or("presenting fenced finalizer slot disappeared")?;
        slot.member = fixture.member;
        slot.binding = fixture.binding;
        slot.cell = fixture.detach_cell;
        slot.attach_secret = fixture.attach_secret;
        drop(authority.take_observer_progress_witnesses());

        authority.attach_commit(
            &inputs.request,
            &inputs.allocation,
            &inputs.mode,
            Arc::clone(&setup.handler.store),
            CommitMode::Live(&FencedAppender { log: &setup.log }),
        )?;
        assert!(
            !authority
                .pending_specific_fates
                .contains_key(&setup.participant_id)
        );
        assert!(
            !authority
                .prepared_ordinary_finalizers
                .contains_key(&setup.participant_id)
        );
        let witnesses = authority.take_observer_progress_witnesses();
        let [attached_witness] = witnesses.as_slice() else {
            return Err(format!("presenting fenced attach witnesses: {witnesses:?}").into());
        };
        assert_eq!(attached_witness.progress(), fixture.terminal_delivery_seq);
        drop(owner);
    }

    Ok(CommittedPresentingFenced {
        inputs,
        attached_source_sequence,
        ordinary_source_sequence,
        terminal_delivery_seq: fixture.terminal_delivery_seq,
        attached_seq: fixture.attached_seq,
    })
}

fn assert_presenting_fenced_audit(
    setup: &PendingRestartFixture,
    committed: &CommittedPresentingFenced,
) -> Result<(), Box<dyn Error>> {
    let attached = block_on(setup.log.read_at(committed.attached_source_sequence))??
        .ok_or("presenting fenced Attached row is absent")?;
    let DecodedStoredOperation::V3(StoredOperation::Attached { mode, .. }) = attached.operation
    else {
        return Err("presenting finalizer did not append fenced Attached".into());
    };
    assert_eq!(*mode, committed.inputs.mode);
    let StoredAttachModeV3::Fenced {
        composed_terminal: Some(terminal),
        ..
    } = mode.as_ref()
    else {
        return Err("presenting fenced Attached omitted its composed terminal".into());
    };
    assert_eq!(
        terminal.presentation,
        StoredFinalizerPresentation::PresentEnclosing
    );
    assert_eq!(terminal.pending_source_sequence, setup.died_source_sequence);

    let ordinary = block_on(setup.log.read_at(committed.ordinary_source_sequence))??
        .ok_or("presenting fenced Ordinary row is absent")?;
    let DecodedStoredOperation::V3(StoredOperation::Ordinary { row: ordinary, .. }) =
        ordinary.operation
    else {
        return Err("presenting fenced finalizer did not append Ordinary".into());
    };
    assert_eq!(
        ordinary.terminal_source,
        StoredOrdinaryTerminalSource::PendingDiedFinalized {
            died_source_sequence: setup.died_source_sequence,
            finalizer: StoredPendingDiedFinalizer::FencedAttached {
                source_sequence: committed.attached_source_sequence,
            },
        }
    );
    assert_eq!(
        ordinary.committed_terminal_audit.cause,
        StoredDiedCause::ConnectionLost
    );
    assert_eq!(
        ordinary.committed_terminal_audit.transaction_order,
        setup.terminal_order
    );
    assert_eq!(
        ordinary.committed_terminal_audit.terminal_seq,
        terminal.delivery_seq
    );
    assert_eq!(terminal.delivery_seq, committed.terminal_delivery_seq);

    let records = project_attached_records(
        setup.participant_id,
        &committed.inputs.allocation,
        &committed.inputs.mode,
        None,
    )?;
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].0, terminal.delivery_seq);
    assert_eq!(records[1].0, committed.attached_seq);
    Ok(())
}
