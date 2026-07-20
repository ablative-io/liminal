#![allow(clippy::panic)]

use crate::outcome::CandidatePhase;
use crate::wire::{
    BindingEpoch, CloseCause, ConnectionIncarnation, DetachedCause, DiedCause, Generation,
};

use super::binding::{
    ActiveBinding, AdmissionOrder, BindingState, BindingTerminalDisposition, BindingTerminalKind,
    CommittedBindingTerminal, CommittedBindingTerminalPosition, DetachedBindingTransition,
    DiedBindingTransition, PendingBindingTerminalPosition, PendingFinalization,
};

fn binding() -> ActiveBinding {
    ActiveBinding {
        participant_id: 3,
        conversation_id: 7,
        binding_epoch: BindingEpoch::new(ConnectionIncarnation::new(11, 13), Generation::ONE),
    }
}

fn committed() -> BindingTerminalDisposition {
    BindingTerminalDisposition::Committed(CommittedBindingTerminalPosition::new(17, 19))
}

fn pending() -> BindingTerminalDisposition {
    BindingTerminalDisposition::Pending(PendingBindingTerminalPosition::new(17))
}

#[test]
fn binding_terminal_order_fixes_phase_and_derives_index_from_id() {
    let order = AdmissionOrder::binding_terminal(17, binding().participant_id);
    assert_eq!(order.transaction_order(), 17);
    assert_eq!(order.candidate_phase(), CandidatePhase::BindingTerminal);
    assert_eq!(order.participant_index(), binding().participant_id);
}

#[test]
fn detached_fates_have_only_detached_causes() {
    let DetachedBindingTransition::Committed(clean) = binding().clean_disconnect(committed())
    else {
        panic!("committed placement must append the clean terminal");
    };
    assert_eq!(clean.cause(), DetachedCause::CleanDeregister);
    assert_eq!(clean.delivery_seq(), 19);
    assert_eq!(clean.admission_order().participant_index(), 3);

    let superseded = binding().superseded(CommittedBindingTerminalPosition::new(23, 29));
    assert_eq!(superseded.cause(), DetachedCause::Superseded);
    assert_eq!(superseded.delivery_seq(), 29);

    let DetachedBindingTransition::Pending(shutdown) = binding().server_shutdown(pending()) else {
        panic!("pending placement must preserve the shutdown terminal");
    };
    assert_eq!(shutdown.cause(), DetachedCause::ServerShutdown);
    let state = DetachedBindingTransition::Pending(shutdown).binding_state();
    assert_eq!(
        state,
        BindingState::PendingFinalization(PendingFinalization::Detached(shutdown))
    );
}

#[test]
fn died_fates_have_only_died_causes() {
    let DiedBindingTransition::Committed(connection_lost) = binding().connection_lost(committed())
    else {
        panic!("committed placement must append the Died terminal");
    };
    assert_eq!(connection_lost.cause(), DiedCause::ConnectionLost);

    let DiedBindingTransition::Committed(process_killed) = binding().process_killed(committed())
    else {
        panic!("committed placement must append the Died terminal");
    };
    assert_eq!(process_killed.cause(), DiedCause::ProcessKilled);

    let DiedBindingTransition::Pending(protocol_error) = binding().protocol_error(pending()) else {
        panic!("pending placement must preserve the Died terminal");
    };
    assert_eq!(protocol_error.cause(), DiedCause::ProtocolError);
}

#[test]
fn died_binding_transition_projects_terminal_sequence_only_when_committed() {
    let committed_transition = binding().connection_lost(committed());
    let committed_terminal_seq = match committed_transition {
        DiedBindingTransition::Committed(terminal) => terminal.delivery_seq(),
        DiedBindingTransition::Pending(_) => {
            panic!("committed disposition must produce a committed Died transition")
        }
    };
    let Some(committed_projection) = committed_transition.observer_progress_projection() else {
        panic!("the committed Died transition must present observer progress");
    };
    assert_eq!(
        committed_projection.new_observer_progress(),
        committed_terminal_seq
    );

    let pending_transition = binding().protocol_error(pending());
    assert!(pending_transition.observer_progress_projection().is_none());
}

#[test]
fn pending_commit_preserves_identity_order_and_cause() {
    let DiedBindingTransition::Pending(pending_terminal) = binding().connection_lost(pending())
    else {
        panic!("fixture selects pending finalization");
    };
    let pending = PendingFinalization::Died(pending_terminal);
    let committed = pending.commit(31);

    assert_eq!(committed.participant_id(), 3);
    assert_eq!(committed.conversation_id(), 7);
    assert_eq!(committed.binding_epoch(), binding().binding_epoch);
    assert_eq!(committed.admission_order(), pending.admission_order());
    assert_eq!(committed.delivery_seq(), 31);
    assert_eq!(committed.kind(), BindingTerminalKind::Died);
    assert_eq!(committed.close_cause(), CloseCause::ConnectionLost);
    assert_eq!(committed.died_cause(), Some(DiedCause::ConnectionLost));
    assert_eq!(committed.detached_cause(), None);
    assert!(matches!(committed, CommittedBindingTerminal::Died(_)));
}

#[test]
fn restart_cause_is_derived_from_old_binding_epoch() {
    let DiedBindingTransition::Pending(restart) = binding().unclean_server_restart(pending())
    else {
        panic!("fixture selects pending startup recovery");
    };
    assert_eq!(
        restart.cause(),
        DiedCause::UncleanServerRestart {
            prior_server_incarnation: 11,
        }
    );
    assert_eq!(
        PendingFinalization::Died(restart).close_cause(),
        CloseCause::UncleanServerRestart {
            prior_server_incarnation: 11,
        }
    );
}
