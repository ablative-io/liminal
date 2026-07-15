//! Regression for `docs/design/LP-EXTRACTION-GOAL.md` Fix 1.
//!
//! A three-variant `Empty | Pending | Committed` cell cannot compile against
//! the terminalized response path: [`VerifiedTerminalizedDetach::outcome`]
//! requires a borrow derived only from [`TerminalizedDetach`], and that state is
//! produced only by consuming [`CommittedDetach::terminalize`].

#![allow(clippy::expect_used)]

use crate::wire::{
    BindingEpoch, BindingStateView, ConnectionIncarnation, DetachAttemptToken, DetachRequest,
    Generation,
};

use super::{ActiveBinding, commit_detach};

fn generation(value: u64) -> Generation {
    Generation::new(value).expect("test generation is nonzero")
}

#[test]
fn committed_attach_terminalized_replay_retains_old_epoch() {
    let old_epoch = BindingEpoch::new(ConnectionIncarnation::new(7, 11), generation(4));
    let binding = ActiveBinding {
        participant_id: 3,
        conversation_id: 29,
        binding_epoch: old_epoch,
    };
    let token = DetachAttemptToken::new([0xD3; 16]);
    let verifier = [0xA5; 32];
    let request = DetachRequest {
        conversation_id: 29,
        participant_id: 3,
        capability_generation: generation(4),
        detach_attempt_token: token,
    };

    let verified_request = binding
        .verify_detach_request(request.clone(), verifier)
        .expect("request matches the active binding");
    let (_, committed, committed_outcome) = commit_detach(binding, verified_request, 44);
    assert_eq!(committed_outcome.committed_binding_epoch, old_epoch);
    assert_eq!(committed_outcome.detached_delivery_seq, 44);

    let terminalized = committed.terminalize();
    let verified_old = terminalized
        .verify_exact(&request, verifier)
        .expect("old request is byte-identical");
    let new_epoch = BindingEpoch::new(ConnectionIncarnation::new(7, 12), generation(5));
    let outcome = verified_old.outcome(
        29,
        generation(5),
        BindingStateView::Bound {
            current_binding_epoch: new_epoch,
        },
    );

    assert_eq!(outcome.committed_binding_epoch(), old_epoch);
    assert_eq!(outcome.current_generation(), generation(5));
    assert_eq!(
        outcome.binding_state(),
        BindingStateView::Bound {
            current_binding_epoch: new_epoch,
        }
    );
}

#[test]
fn exact_token_without_exact_verifier_cannot_produce_terminalized_result() {
    let old_epoch = BindingEpoch::new(ConnectionIncarnation::new(2, 3), generation(8));
    let binding = ActiveBinding {
        participant_id: 1,
        conversation_id: 5,
        binding_epoch: old_epoch,
    };
    let request = DetachRequest {
        conversation_id: 5,
        participant_id: 1,
        capability_generation: generation(8),
        detach_attempt_token: DetachAttemptToken::new([9; 16]),
    };
    let verifier = [4; 32];
    let verified_request = binding
        .verify_detach_request(request.clone(), verifier)
        .expect("request matches the active binding");
    let (_, committed, _) = commit_detach(binding, verified_request, 12);
    let terminalized = committed.terminalize();

    assert!(terminalized.verify_exact(&request, [5; 32]).is_err());
}
