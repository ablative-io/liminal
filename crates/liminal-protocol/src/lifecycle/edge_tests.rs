#![allow(clippy::unwrap_used)]

use crate::wire::{BindingEpoch, ConnectionIncarnation, Generation};

use super::{
    CursorFateSuccessor, CursorProgressContinuous, CursorProgressMarker, DebtCompletion,
    DetachedCredentialRecovery, DetachedCursorRelease, DetachedMarkerRelease,
    KClaimBackedDetachedLeave, LeaveOnlyEdge, MarkerDelivery, ParticipantCursorProgress,
};

fn epoch(generation: u64) -> BindingEpoch {
    BindingEpoch::new(
        ConnectionIncarnation::new(1, generation),
        Generation::new(generation).unwrap(),
    )
}

#[test]
fn cursor_typestate_selects_distinct_fate_edges() {
    let continuous = ParticipantCursorProgress::Continuous(CursorProgressContinuous {
        participant_id: 4,
        binding_epoch: epoch(2),
        through_seq: 8,
    });
    assert!(matches!(
        continuous.binding_fate(),
        CursorFateSuccessor::DetachedCursorRelease(DetachedCursorRelease { .. })
    ));

    let marker = ParticipantCursorProgress::Marker(CursorProgressMarker {
        participant_id: 4,
        binding_epoch: epoch(2),
        through_seq: 9,
        marker_delivery_seq: 9,
    });
    assert!(matches!(
        marker.binding_fate(),
        CursorFateSuccessor::DetachedCredentialRecovery(DetachedCredentialRecovery { .. })
    ));
}

#[test]
fn marker_delivery_fate_never_fabricates_recovery() {
    let delivery = MarkerDelivery {
        participant_id: 2,
        binding_epoch: epoch(3),
        marker_delivery_seq: 14,
    };
    let release = delivery.binding_fate();
    assert_eq!(release.marker_delivery_seq, 14);
}

#[test]
fn leave_only_edges_accept_only_matching_k_backed_leave() {
    let marker = DetachedMarkerRelease {
        participant_id: 7,
        marker_delivery_seq: 20,
        last_dead_binding_epoch: epoch(5),
    };
    assert!(
        marker
            .leave(
                KClaimBackedDetachedLeave::verified(7),
                DebtCompletion::Clear,
            )
            .is_ok()
    );

    let cursor = DetachedCursorRelease {
        participant_id: 8,
        last_dead_binding_epoch: epoch(6),
    };
    assert!(
        cursor
            .leave(
                KClaimBackedDetachedLeave::verified(9),
                DebtCompletion::Clear,
            )
            .is_err()
    );
}
