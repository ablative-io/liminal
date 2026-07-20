use alloc::{
    format,
    string::{String, ToString},
};

use crate::{
    algebra::{ResourceVector, WideResourceVector},
    lifecycle::{FrontierBinding, ImmutableSequenceCandidate, OrderHigh, SequenceDirectOwner},
};

use super::{BindingTerminalAdmission, BindingTerminalCauseClass};
use crate::lifecycle::operations::binding_fate_tests::{frontier_owner_with_limit, ordinary_token};

#[test]
fn terminal_disposition_selector_commits_or_pends_from_protocol_state() -> Result<(), String> {
    let (_token, active, cursor) = ordinary_token()?;
    let high_watermark = cursor
        .checked_add(1)
        .ok_or_else(|| "selector high watermark overflow".to_string())?;
    let candidate_sequence = high_watermark
        .checked_add(1)
        .ok_or_else(|| "selector candidate sequence overflow".to_string())?;

    assert_commit_transition(active, cursor, high_watermark, candidate_sequence)?;
    assert_pending_transition(active, cursor, high_watermark, candidate_sequence)?;
    assert_refusal_preserves_owner(active, cursor, high_watermark, candidate_sequence)
}

fn assert_pending_transition(
    active: crate::lifecycle::ActiveBinding,
    cursor: u64,
    high_watermark: u64,
    candidate_sequence: u64,
) -> Result<(), String> {
    let pending_owner = frontier_owner_with_limit(
        active.conversation_id,
        active.participant_id,
        active.binding_epoch,
        cursor,
        high_watermark,
        0,
    )?;
    let pending_accounting = pending_owner.closure_accounting();
    let pending = pending_owner
        .prepare_binding_terminal(
            active,
            BindingTerminalCauseClass::Detached,
            0,
            candidate_sequence,
            high_watermark,
        )
        .map_err(|refused| format!("selector pending prepare refused: {:?}", refused.error()))?;
    let pending_key = pending.candidate_key();
    let BindingTerminalAdmission::Pending(pending) =
        pending.admit(pending_key.bind_v3_charge(ResourceVector::new(1, 79)))
    else {
        return Err("selector observer-blocked state did not pend".to_string());
    };
    assert_eq!(pending.blocked_at_observer(), high_watermark);
    let (pending_owner, position) = pending.into_parts();
    assert_eq!(position.transaction_order(), 0);
    assert_eq!(
        pending_owner
            .frontiers()
            .sequence()
            .ledger()
            .high_watermark(),
        high_watermark
    );
    assert_eq!(
        pending_owner.frontiers().order().ledger().high(),
        OrderHigh::Allocated(0)
    );
    assert_eq!(
        pending_owner.frontiers().active_identities().participants()[0].binding(),
        FrontierBinding::Detached(active.binding_epoch)
    );
    let [
        ImmutableSequenceCandidate::BindingTerminal {
            delivery_seq,
            admission_order,
            owner,
        },
    ] = pending_owner.frontiers().sequence().immutable_candidates()
    else {
        return Err("pending owner did not retain one exact terminal candidate".to_string());
    };
    assert_eq!(*delivery_seq, candidate_sequence);
    assert_eq!(*admission_order, pending_key.admission_order());
    assert_eq!(owner.participant_index, active.participant_id);
    assert_eq!(owner.binding_epoch, active.binding_epoch);
    assert!(pending_owner.retained_charges().is_empty());
    assert_eq!(pending_owner.closure_accounting(), pending_accounting);
    assert!(
        pending_owner
            .frontiers()
            .sequence()
            .movable_claims()
            .iter()
            .all(|claim| !matches!(claim.owner, SequenceDirectOwner::BindingTerminal(_)))
    );
    Ok(())
}

fn assert_commit_transition(
    active: crate::lifecycle::ActiveBinding,
    cursor: u64,
    high_watermark: u64,
    candidate_sequence: u64,
) -> Result<(), String> {
    let committed_owner = frontier_owner_with_limit(
        active.conversation_id,
        active.participant_id,
        active.binding_epoch,
        cursor,
        high_watermark,
        1,
    )?;
    let committed = committed_owner
        .prepare_binding_terminal(
            active,
            BindingTerminalCauseClass::Died,
            0,
            candidate_sequence,
            high_watermark,
        )
        .map_err(|refused| format!("selector commit prepare refused: {:?}", refused.error()))?;
    let committed_key = committed.candidate_key();
    assert_eq!(committed_key.conversation_id(), active.conversation_id);
    assert_eq!(committed_key.participant_id(), active.participant_id);
    assert_eq!(committed_key.binding_epoch(), active.binding_epoch);
    assert_eq!(committed_key.cause_class(), BindingTerminalCauseClass::Died);
    let committed_charge = ResourceVector::new(1, 73);
    let BindingTerminalAdmission::Commit(commit) =
        committed.admit(committed_key.bind_v3_charge(committed_charge))
    else {
        return Err("selector capacity state did not commit".to_string());
    };
    let (committed_owner, position) = commit.into_parts();
    assert_eq!(position.transaction_order(), 0);
    assert_eq!(position.delivery_seq(), candidate_sequence);
    assert_eq!(
        committed_owner
            .frontiers()
            .sequence()
            .ledger()
            .high_watermark(),
        candidate_sequence
    );
    assert_eq!(
        committed_owner.frontiers().order().ledger().high(),
        OrderHigh::Allocated(0)
    );
    assert_eq!(
        committed_owner
            .frontiers()
            .active_identities()
            .participants()[0]
            .binding(),
        FrontierBinding::Detached(active.binding_epoch)
    );
    assert!(
        committed_owner
            .frontiers()
            .sequence()
            .immutable_candidates()
            .is_empty()
    );
    assert_eq!(committed_owner.retained_charges().len(), 1);
    assert_eq!(
        committed_owner.retained_charges()[0].encoded_charge(),
        committed_charge
    );
    assert_eq!(
        committed_owner.closure_accounting().baseline(),
        WideResourceVector::new(1, 73)
    );
    Ok(())
}

fn assert_refusal_preserves_owner(
    active: crate::lifecycle::ActiveBinding,
    cursor: u64,
    high_watermark: u64,
    candidate_sequence: u64,
) -> Result<(), String> {
    let refused_owner = frontier_owner_with_limit(
        active.conversation_id,
        active.participant_id,
        active.binding_epoch,
        cursor,
        high_watermark,
        1,
    )?;
    let retained_before = refused_owner.retained_charges().to_vec();
    let accounting_before = refused_owner.closure_accounting();
    let refused = refused_owner
        .prepare_binding_terminal(
            active,
            BindingTerminalCauseClass::Died,
            0,
            candidate_sequence,
            high_watermark,
        )
        .map_err(|value| format!("selector refusal prepare failed early: {:?}", value.error()))?;
    let refused_key = refused.candidate_key();
    let BindingTerminalAdmission::Refused(refusal) =
        refused.admit(refused_key.bind_v3_charge(ResourceVector::new(2, 83)))
    else {
        return Err("selector admitted a non-single-entry candidate".to_string());
    };
    let unchanged_owner = refusal.into_owner();
    assert_eq!(unchanged_owner.retained_charges(), retained_before);
    assert_eq!(unchanged_owner.closure_accounting(), accounting_before);
    assert_eq!(
        unchanged_owner
            .frontiers()
            .sequence()
            .ledger()
            .high_watermark(),
        high_watermark
    );
    assert_eq!(
        unchanged_owner.frontiers().order().ledger().high(),
        OrderHigh::Empty
    );
    assert_eq!(
        unchanged_owner
            .frontiers()
            .active_identities()
            .participants()[0]
            .binding(),
        FrontierBinding::Bound(active.binding_epoch)
    );
    Ok(())
}
