#![allow(clippy::expect_used, clippy::panic)]

use crate::algebra::{ResourceVector, WideResourceVector};
use crate::wire::{BindingEpoch, ConnectionIncarnation, Generation};

use super::edge::RecoveredBindingFateTransition;
use super::{
    ActiveBinding, BindingTerminalDisposition, ClosureDebt, ClosureState,
    CommittedBindingTerminalPosition, CursorFateSuccessor, DebtCompletion, DetachedAttachRefusal,
    DetachedCredentialRecovery, DetachedCursorRelease, DetachedMarkerRelease, Event, LeaveOnlyEdge,
    MarkerDelivery, ObserverProjection, OrdinaryBindingAuthority, ParticipantCursorProgress,
    PhysicalCompaction, StoredEdge,
};

fn epoch(generation: u64) -> BindingEpoch {
    BindingEpoch::new(
        ConnectionIncarnation::new(1, generation),
        Generation::new(generation).expect("test generations are nonzero"),
    )
}

fn debt(entries: u128, bytes: u128) -> ClosureDebt {
    ClosureDebt::new(WideResourceVector::new(entries, bytes)).expect("test closure debt is nonzero")
}

fn owed(debt: ClosureDebt, edge: StoredEdge) -> ClosureState {
    ClosureState::Owed { debt, edge }
}

fn compaction(from_floor: u64, through_seq: u64) -> PhysicalCompaction {
    PhysicalCompaction::new(from_floor, through_seq).expect("ordered test compaction range")
}

fn cursor_event(
    participant_id: u64,
    binding_epoch: BindingEpoch,
    previous_cursor: u64,
    through_seq: u64,
    resulting_floor: u64,
) -> Event {
    Event::cursor_progressed(
        participant_id,
        binding_epoch,
        previous_cursor,
        through_seq,
        resulting_floor,
    )
    .expect("test cursor event advances")
}

fn marker_progress(
    participant_id: u64,
    binding_epoch: BindingEpoch,
    marker_delivery_seq: u64,
    closure_debt: ClosureDebt,
) -> ParticipantCursorProgress {
    let delivery = MarkerDelivery::new(participant_id, binding_epoch, marker_delivery_seq);
    let event = Event::marker_delivered(participant_id, binding_epoch, marker_delivery_seq);
    match delivery
        .delivered(closure_debt, event)
        .expect("exact delivery commits")
    {
        ClosureState::Owed {
            edge: StoredEdge::ParticipantCursorProgress(progress),
            ..
        } => progress,
        other => panic!("delivery selected an unexpected state: {other:?}"),
    }
}

fn credential_recovery(
    participant_id: u64,
    binding_epoch: BindingEpoch,
    marker_delivery_seq: u64,
    closure_debt: ClosureDebt,
) -> DetachedCredentialRecovery {
    let progress = marker_progress(
        participant_id,
        binding_epoch,
        marker_delivery_seq,
        closure_debt,
    );
    let fate = Event::binding_fate_observed(participant_id, binding_epoch, marker_delivery_seq);
    match progress
        .binding_fate(closure_debt, fate)
        .expect("exact marker fate commits")
    {
        CursorFateSuccessor::DetachedCredentialRecovery(edge) => edge,
        other @ CursorFateSuccessor::DetachedCursorRelease(_) => {
            panic!("marker fate selected an unexpected edge: {other:?}")
        }
    }
}

fn marker_release(
    participant_id: u64,
    binding_epoch: BindingEpoch,
    marker_delivery_seq: u64,
    closure_debt: ClosureDebt,
) -> DetachedMarkerRelease {
    let delivery = MarkerDelivery::new(participant_id, binding_epoch, marker_delivery_seq);
    let fate = Event::binding_fate_observed(participant_id, binding_epoch, marker_delivery_seq);
    match delivery
        .binding_fate(closure_debt, fate)
        .expect("exact pre-delivery fate commits")
    {
        ClosureState::Owed {
            edge: StoredEdge::DetachedMarkerRelease(edge),
            ..
        } => edge,
        other => panic!("pre-delivery fate selected an unexpected state: {other:?}"),
    }
}

fn cursor_release(
    participant_id: u64,
    binding_epoch: BindingEpoch,
    through_seq: u64,
    closure_debt: ClosureDebt,
) -> DetachedCursorRelease {
    let binding = ActiveBinding {
        participant_id,
        conversation_id: 1,
        binding_epoch,
    };
    let terminal = match binding.connection_lost(BindingTerminalDisposition::Committed(
        CommittedBindingTerminalPosition::new(1, through_seq),
    )) {
        super::DiedBindingTransition::Committed(terminal) => terminal,
        super::DiedBindingTransition::Pending(_) => panic!("test terminal is committed"),
    };
    let fate = OrdinaryBindingAuthority::new(binding, through_seq)
        .binding_fate(terminal, through_seq)
        .expect("exact ordinary binding terminal derives cursor fate");
    let ClosureState::Owed {
        edge: StoredEdge::DetachedCursorRelease(edge),
        ..
    } = fate.into_direct_state(closure_debt)
    else {
        panic!("ordinary cursor fate did not select DCursor")
    };
    edge
}

#[test]
fn only_clear_closure_state_admits_an_ordinary_detached_attach() {
    let closure_debt = debt(1, 8);
    let binding_epoch = epoch(2);
    assert!(
        ClosureState::Clear
            .ordinary_detached_attach_admission()
            .is_ok(),
        "clear closure state must admit the ordinary detached-attach path"
    );

    let recovery_edges = [
        StoredEdge::DetachedCredentialRecovery(credential_recovery(
            4,
            binding_epoch,
            11,
            closure_debt,
        )),
        StoredEdge::DetachedMarkerRelease(marker_release(4, binding_epoch, 11, closure_debt)),
        StoredEdge::DetachedCursorRelease(cursor_release(4, binding_epoch, 11, closure_debt)),
    ];

    for edge in recovery_edges {
        let state = owed(closure_debt, edge);
        assert_eq!(
            state.ordinary_detached_attach_admission(),
            Err(state),
            "DCR, DMR, and DCursor must not mint ordinary attach admission: {edge:?}"
        );
    }
}

#[test]
fn observer_completion_binds_exact_event_and_strict_successor() {
    let original_debt = debt(2, 20);
    let next_debt = debt(1, 10);
    let projection = ObserverProjection::new(10);
    let exact = Event::projection_completed(10);
    let later = ObserverProjection::new(11);
    let successor = projection
        .strict_after_completion(&exact, next_debt, StoredEdge::ObserverProjection(later), 11)
        .expect("strict later projection is legal");

    assert_eq!(
        projection.complete(original_debt, exact, successor),
        Ok(owed(next_debt, StoredEdge::ObserverProjection(later)))
    );
    assert_eq!(
        projection.complete(original_debt, Event::projection_completed(9), successor),
        Err(owed(
            original_debt,
            StoredEdge::ObserverProjection(projection)
        ))
    );
    assert!(
        projection
            .strict_after_completion(
                &Event::projection_completed(11),
                next_debt,
                StoredEdge::ObserverProjection(later),
                11,
            )
            .is_none(),
        "a later projection occurrence cannot impersonate the exact witness"
    );
    assert!(
        projection
            .strict_after_completion(
                &exact,
                next_debt,
                StoredEdge::ObserverProjection(projection),
                10,
            )
            .is_none(),
        "the stored boundary is not a strict suffix"
    );

    let clear = projection
        .clear_after_completion(&exact)
        .expect("exact completion may clear debt");
    assert_eq!(
        projection.complete(original_debt, exact, clear),
        Ok(ClosureState::Clear)
    );
}

#[test]
fn strict_successor_set_contains_six_kinds_and_excludes_direct_recovery() {
    let closure_debt = debt(1, 8);
    let projection = ObserverProjection::new(10);
    let exact = Event::projection_completed(10);
    let binding = epoch(2);
    let dmr = marker_release(4, binding, 11, closure_debt);
    let dcursor = cursor_release(5, binding, 11, closure_debt);
    let allowed = [
        StoredEdge::ObserverProjection(ObserverProjection::new(11)),
        StoredEdge::PhysicalCompaction(compaction(11, 11)),
        StoredEdge::MarkerDelivery(MarkerDelivery::new(4, binding, 11)),
        StoredEdge::ParticipantCursorProgress(ParticipantCursorProgress::continuous(
            4, binding, 11,
        )),
        StoredEdge::DetachedMarkerRelease(dmr),
        StoredEdge::DetachedCursorRelease(dcursor),
    ];

    for edge in allowed {
        assert!(
            projection
                .strict_after_completion(&exact, closure_debt, edge, 11)
                .is_some(),
            "frozen strict successor {edge:?} must be constructible"
        );
    }

    let dcr = credential_recovery(4, binding, 11, closure_debt);
    assert!(
        projection
            .strict_after_completion(
                &exact,
                closure_debt,
                StoredEdge::DetachedCredentialRecovery(dcr),
                11,
            )
            .is_none(),
        "direct DCR is impossible without marker-backed PCP"
    );
}

#[test]
fn observer_marker_independent_and_binding_orderings_are_closed() {
    let original_debt = debt(2, 20);
    let next_debt = debt(1, 10);
    let projection = ObserverProjection::new(10);
    let marker = Event::marker_appended(12, 14);
    let later = ObserverProjection::new(14);
    let successor = projection
        .later_projection_after_marker(&marker, next_debt, later)
        .expect("marker extends the projected suffix");
    assert_eq!(
        projection.marker_appended(original_debt, marker, successor),
        Ok(owed(next_debt, StoredEdge::ObserverProjection(later)))
    );
    assert!(
        projection
            .later_projection_after_marker(&Event::marker_appended(10, 14), next_debt, later,)
            .is_none()
    );

    let leave = Event::live_leave_committed(2, epoch(2), 11);
    let leave_projection = ObserverProjection::new(15);
    let leave_successor = projection
        .later_projection_after_leave(&leave, next_debt, leave_projection)
        .expect("Leave appends a strict later projection boundary");
    assert_eq!(
        projection.leave_with_later_projection(original_debt, leave, leave_successor),
        Ok(owed(
            next_debt,
            StoredEdge::ObserverProjection(leave_projection)
        ))
    );
    assert!(
        projection
            .later_projection_after_leave(
                &Event::binding_fate_observed(2, epoch(2), 11),
                next_debt,
                leave_projection,
            )
            .is_none(),
        "binding fate cannot use the Leave-only successor path"
    );

    let binding = epoch(2);
    let independent = [
        cursor_event(2, binding, 0, 5, 1),
        Event::marker_acknowledged(2, binding, 5, 1),
        Event::binding_fate_observed(2, binding, 1),
        Event::live_leave_committed(2, binding, 1),
        Event::detached_leave_committed(2, 1),
    ];
    for event in independent {
        assert_eq!(
            projection.independent_event(original_debt, event, Some(next_debt)),
            Ok(owed(next_debt, StoredEdge::ObserverProjection(projection)))
        );
    }
    assert_eq!(
        projection.independent_event(
            original_debt,
            Event::projection_completed(10),
            Some(next_debt)
        ),
        Err(owed(
            original_debt,
            StoredEdge::ObserverProjection(projection)
        ))
    );
    assert_eq!(
        projection.charged_binding_change(original_debt, 3, 1, 4, None),
        Ok(ClosureState::Clear)
    );
    assert!(matches!(
        projection.charged_binding_change(original_debt, 3, 0, 4, Some(next_debt)),
        Err((_, DetachedAttachRefusal::EpisodeChurnLimit))
    ));
    assert!(matches!(
        projection.charged_binding_change(original_debt, 3, 2, 4, Some(next_debt)),
        Err((_, DetachedAttachRefusal::EpisodeChurnLimit))
    ));
}

#[test]
fn physical_completion_requires_its_exact_compaction_range() {
    let original_debt = debt(2, 20);
    let next_debt = debt(1, 10);
    let physical = compaction(5, 10);
    let exact = Event::compaction_completed(5, 10, 11).expect("valid completion event");
    let next = ObserverProjection::new(11);
    let successor = physical
        .strict_after_completion(&exact, next_debt, StoredEdge::ObserverProjection(next), 11)
        .expect("suffix starts at resulting floor");
    assert_eq!(
        physical.complete(original_debt, exact, successor),
        Ok(owed(next_debt, StoredEdge::ObserverProjection(next)))
    );

    let wrong_range =
        Event::compaction_completed(5, 11, 12).expect("locally valid different range");
    assert!(physical.clear_after_completion(&wrong_range).is_none());
    let dcr = credential_recovery(3, epoch(2), 12, original_debt);
    assert!(
        physical
            .strict_after_completion(
                &exact,
                next_debt,
                StoredEdge::DetachedCredentialRecovery(dcr),
                12,
            )
            .is_none()
    );
}

#[test]
fn physical_progress_classes_preserve_or_cover_and_refusals_do_neither() {
    let original_debt = debt(2, 20);
    let next_debt = debt(1, 10);
    let physical = compaction(5, 10);
    let binding = epoch(2);
    let preserving = [
        cursor_event(2, binding, 0, 5, 10),
        Event::marker_acknowledged(2, binding, 5, 10),
        Event::binding_fate_observed(2, binding, 10),
        Event::live_leave_committed(2, binding, 10),
        Event::detached_leave_committed(2, 10),
    ];
    for event in preserving {
        assert_eq!(
            physical.preserve_progress(original_debt, event, next_debt),
            Ok(owed(next_debt, StoredEdge::PhysicalCompaction(physical)))
        );
    }

    let covering = [
        cursor_event(2, binding, 0, 5, 11),
        Event::marker_acknowledged(2, binding, 5, 11),
        Event::binding_fate_observed(2, binding, 11),
        Event::live_leave_committed(2, binding, 11),
        Event::detached_leave_committed(2, 11),
    ];
    for event in covering {
        let successor = physical
            .clear_after_progress(&event)
            .expect("measured floor covers stored range");
        assert_eq!(
            physical.covered_by_progress(original_debt, event, successor),
            Ok(ClosureState::Clear)
        );
    }

    assert_eq!(
        physical.preserve_progress(original_debt, Event::projection_completed(10), next_debt),
        Err(owed(
            original_debt,
            StoredEdge::PhysicalCompaction(physical)
        ))
    );
    assert_eq!(
        physical.unchanged(original_debt),
        owed(original_debt, StoredEdge::PhysicalCompaction(physical))
    );

    assert_eq!(
        physical.charged_binding_change_preserving(original_debt, 1, 1, 3, 10, next_debt,),
        Ok(owed(next_debt, StoredEdge::PhysicalCompaction(physical)))
    );
    assert!(matches!(
        physical.charged_binding_change_preserving(original_debt, 1, 3, 3, 10, next_debt,),
        Err((_, DetachedAttachRefusal::EpisodeChurnLimit))
    ));
    assert_eq!(
        physical.charged_binding_change_covering(
            original_debt,
            1,
            1,
            3,
            11,
            next_debt,
            StoredEdge::ObserverProjection(ObserverProjection::new(11)),
            11,
        ),
        Ok(owed(
            next_debt,
            StoredEdge::ObserverProjection(ObserverProjection::new(11))
        ))
    );
    assert!(matches!(
        physical.charged_binding_change_covering(
            original_debt,
            1,
            1,
            3,
            11,
            next_debt,
            StoredEdge::ObserverProjection(ObserverProjection::new(10)),
            10,
        ),
        Err((_, DetachedAttachRefusal::StaleAuthority))
    ));
}

#[test]
fn marker_delivery_derives_progress_and_covers_every_invalidator() {
    let original_debt = debt(2, 20);
    let next_debt = debt(1, 10);
    let binding = epoch(3);
    let delivery = MarkerDelivery::new(4, binding, 14);
    let delivered = Event::marker_delivered(4, binding, 14);
    let state = delivery
        .delivered(original_debt, delivered)
        .expect("exact final-emitter delivery commits");
    let ClosureState::Owed {
        edge: StoredEdge::ParticipantCursorProgress(progress),
        ..
    } = state
    else {
        panic!("delivery did not derive PCP: {state:?}");
    };
    assert_eq!(progress.participant_id(), 4);
    assert_eq!(progress.binding_epoch(), binding);
    assert_eq!(progress.through_seq(), 14);
    assert_eq!(progress.marker_delivery_seq(), Some(14));
    assert!(
        delivery
            .delivered(original_debt, Event::marker_delivered(4, binding, 15))
            .is_err()
    );

    let lower = [
        cursor_event(9, binding, 0, 13, 1),
        Event::projection_completed(13),
        Event::compaction_completed(3, 13, 14).expect("lower compaction"),
    ];
    for event in lower {
        assert_eq!(
            delivery.lower_progress(original_debt, event, Some(next_debt)),
            Ok(owed(next_debt, StoredEdge::MarkerDelivery(delivery)))
        );
    }
    assert!(
        delivery
            .lower_progress(
                original_debt,
                Event::projection_completed(14),
                Some(next_debt)
            )
            .is_err()
    );

    let fate = Event::binding_fate_observed(4, binding, 3);
    let fate_state = delivery
        .binding_fate(original_debt, fate)
        .expect("pre-delivery fate commits");
    let ClosureState::Owed {
        edge: StoredEdge::DetachedMarkerRelease(release),
        ..
    } = fate_state
    else {
        panic!("pre-delivery fate fabricated the wrong edge: {fate_state:?}");
    };
    assert_eq!(release.participant_id(), 4);
    assert_eq!(release.marker_delivery_seq(), 14);
    assert_eq!(release.last_dead_binding_epoch(), binding);

    let retargeted = delivery
        .retarget(epoch(4), 1, 1, 3)
        .expect("positive in-limit next generation retargets");
    assert_eq!(retargeted.binding_epoch(), epoch(4));
    assert!(matches!(
        delivery.retarget(epoch(4), 1, 0, 3),
        Err((_, DetachedAttachRefusal::EpisodeChurnLimit))
    ));
    assert!(matches!(
        delivery.retarget(epoch(5), 1, 1, 3),
        Err((_, DetachedAttachRefusal::StaleAuthority))
    ));

    let leave = Event::live_leave_committed(4, binding, 15);
    let completion = DebtCompletion::observer_projection(next_debt, ObserverProjection::new(15));
    assert_eq!(
        delivery.leave(original_debt, leave, completion),
        Ok(owed(
            next_debt,
            StoredEdge::ObserverProjection(ObserverProjection::new(15))
        ))
    );
    assert!(
        delivery
            .leave(
                original_debt,
                Event::live_leave_committed(5, binding, 15),
                DebtCompletion::clear(),
            )
            .is_err()
    );
}

#[test]
fn cursor_ack_orderings_are_atomic_and_boundary_checked() {
    let original_debt = debt(2, 20);
    let next_debt = debt(1, 10);
    let binding = epoch(2);
    let progress = ParticipantCursorProgress::continuous(4, binding, 10);

    let lesser = cursor_event(4, binding, 2, 8, 3);
    assert_eq!(
        progress.lesser_ack(original_debt, lesser, next_debt),
        Ok(owed(
            next_debt,
            StoredEdge::ParticipantCursorProgress(progress)
        ))
    );
    let equal = cursor_event(4, binding, 8, 10, 4);
    assert_eq!(
        progress.complete_ack(original_debt, equal, DebtCompletion::clear()),
        Ok(ClosureState::Clear)
    );

    let greater = cursor_event(4, binding, 8, 12, 6);
    let strict = progress
        .strict_after_greater_ack(
            &greater,
            next_debt,
            StoredEdge::ObserverProjection(ObserverProjection::new(13)),
            13,
        )
        .expect("greater ack selects only its actual strict suffix");
    assert_eq!(
        progress.greater_ack(original_debt, greater, strict),
        Ok(owed(
            next_debt,
            StoredEdge::ObserverProjection(ObserverProjection::new(13))
        ))
    );
    assert!(
        progress
            .strict_after_greater_ack(
                &greater,
                next_debt,
                StoredEdge::ObserverProjection(ObserverProjection::new(12)),
                12,
            )
            .is_none(),
        "the greater-ack occurrence boundary is not its own strict suffix"
    );
    let already_covered = cursor_event(4, binding, 10, 12, 6);
    assert!(progress.clear_after_greater_ack(&already_covered).is_none());
    assert_eq!(
        progress.unchanged(original_debt),
        owed(
            original_debt,
            StoredEdge::ParticipantCursorProgress(progress)
        )
    );

    let marker = marker_progress(4, binding, 14, original_debt);
    let marker_ack = Event::marker_acknowledged(4, binding, 14, 5);
    assert_eq!(
        marker.complete_ack(original_debt, marker_ack, DebtCompletion::clear()),
        Ok(ClosureState::Clear)
    );
    assert!(
        progress
            .complete_ack(original_debt, marker_ack, DebtCompletion::clear())
            .is_err(),
        "continuous PCP cannot accept a marker acknowledgement"
    );
}

#[test]
fn cursor_storage_fate_supersession_and_leave_preserve_typestate() {
    let original_debt = debt(2, 20);
    let next_debt = debt(1, 10);
    let binding = epoch(2);
    let continuous = ParticipantCursorProgress::continuous(4, binding, 10);
    let marker = marker_progress(4, binding, 14, original_debt);

    assert_eq!(
        marker.storage_progress(
            original_debt,
            Event::projection_completed(8),
            Some(next_debt)
        ),
        Ok(owed(
            next_debt,
            StoredEdge::ParticipantCursorProgress(marker)
        ))
    );
    let below_marker = Event::compaction_completed(3, 13, 14).expect("below marker");
    assert!(
        marker
            .storage_progress(original_debt, below_marker, Some(next_debt))
            .is_ok()
    );
    let crosses_marker = Event::compaction_completed(3, 14, 15).expect("crosses marker");
    assert!(
        marker
            .storage_progress(original_debt, crosses_marker, Some(next_debt))
            .is_err()
    );

    let marker_fate = Event::binding_fate_observed(4, binding, 6);
    assert!(matches!(
        marker.binding_fate(original_debt, marker_fate),
        Ok(CursorFateSuccessor::DetachedCredentialRecovery(_))
    ));
    let continuous_fate = Event::binding_fate_observed(4, binding, 6);
    assert_eq!(
        continuous.binding_fate(original_debt, continuous_fate),
        Err(owed(
            original_debt,
            StoredEdge::ParticipantCursorProgress(continuous)
        )),
        "raw continuous PCP cannot derive DCursor without attach provenance"
    );

    let retargeted = continuous
        .retarget(epoch(3), 1, 1, 3)
        .expect("continuous PCP retargets under charged churn");
    assert_eq!(retargeted.binding_epoch(), epoch(3));
    assert!(matches!(
        marker.retarget(epoch(3), 1, 1, 3),
        Err((_, DetachedAttachRefusal::DeliveredMarkerAwaitingAck))
    ));
    assert!(matches!(
        continuous.retarget(epoch(4), 1, 1, 3),
        Err((_, DetachedAttachRefusal::StaleAuthority))
    ));
    assert!(matches!(
        continuous.retarget(epoch(3), 2, 2, 3),
        Err((_, DetachedAttachRefusal::EpisodeChurnLimit))
    ));

    assert_eq!(
        continuous.leave(
            original_debt,
            Event::live_leave_committed(4, binding, 11),
            DebtCompletion::physical_compaction(next_debt, compaction(11, 11)),
        ),
        Ok(owed(
            next_debt,
            StoredEdge::PhysicalCompaction(compaction(11, 11))
        ))
    );
}

#[test]
fn detached_recovery_fence_proof_binds_every_authority_field() {
    let original_debt = debt(2, 20);
    let next_debt = debt(1, 10);
    let binding = epoch(2);
    let recovery = credential_recovery(4, binding, 14, original_debt);
    assert_eq!(recovery.participant_id(), 4);
    assert_eq!(recovery.marker_delivery_seq(), 14);
    assert_eq!(recovery.prior_binding_epoch(), binding);
    assert_eq!(
        recovery.ordinary_attach_refusal(),
        DetachedAttachRefusal::RecoveryFence
    );
    assert_eq!(recovery.marker_attach_refusal(14), None);
    assert_eq!(
        recovery.marker_attach_refusal(15),
        Some(DetachedAttachRefusal::MarkerMismatch)
    );

    let fenced = Event::fenced_recovery_committed(4, 14, binding, epoch(3), 15);
    let successor = DebtCompletion::observer_projection(next_debt, ObserverProjection::new(15));
    let commit = recovery
        .fenced_attach(original_debt, fenced, successor)
        .expect("exact DCR creates fenced marker-acceptance proof");
    assert_eq!(commit.participant_id(), 4);
    assert_eq!(commit.marker_delivery_seq(), 14);
    assert_eq!(commit.prior_binding_epoch(), binding);
    assert_eq!(commit.new_binding_epoch(), epoch(3));
    assert_eq!(
        commit.next_state(),
        owed(
            next_debt,
            StoredEdge::ObserverProjection(ObserverProjection::new(15))
        )
    );
    assert!(
        recovery
            .fenced_attach(
                original_debt,
                Event::fenced_recovery_committed(4, 15, binding, epoch(3), 15),
                DebtCompletion::clear(),
            )
            .is_err()
    );
    assert_eq!(
        recovery.authority_superseded(),
        (recovery, DetachedAttachRefusal::StaleAuthority)
    );
}

#[test]
fn ordinary_binding_fate_requires_attach_and_exact_terminal_provenance() {
    let original_debt = debt(2, 20);
    let next_debt = debt(1, 10);
    let binding = ActiveBinding {
        participant_id: 4,
        conversation_id: 29,
        binding_epoch: epoch(2),
    };
    let authority = OrdinaryBindingAuthority::new(binding, 10);
    assert_eq!(
        authority.cursor_progressed(cursor_event(4, binding.binding_epoch, 9, 11, 9)),
        Err(authority),
        "cursor authority requires its exact previous boundary"
    );
    let authority = authority
        .cursor_progressed(cursor_event(4, binding.binding_epoch, 10, 12, 9))
        .expect("exact ordinary cursor progress preserves attach provenance");
    let wrong_binding = ActiveBinding {
        participant_id: 5,
        ..binding
    };
    let wrong_terminal = match wrong_binding.connection_lost(BindingTerminalDisposition::Committed(
        CommittedBindingTerminalPosition::new(4, 13),
    )) {
        super::DiedBindingTransition::Committed(terminal) => terminal,
        super::DiedBindingTransition::Pending(_) => panic!("test terminal is committed"),
    };
    assert_eq!(authority.binding_fate(wrong_terminal, 9), Err(authority));

    let terminal = match binding.connection_lost(BindingTerminalDisposition::Committed(
        CommittedBindingTerminalPosition::new(4, 13),
    )) {
        super::DiedBindingTransition::Committed(terminal) => terminal,
        super::DiedBindingTransition::Pending(_) => panic!("test terminal is committed"),
    };
    let fate = authority
        .binding_fate(terminal, 9)
        .expect("exact ordinary terminal derives no-marker fate");
    assert_eq!(fate.through_seq(), 12);
    assert_eq!(fate.resulting_floor(), 9);
    assert_eq!(
        fate.into_direct_state(original_debt),
        owed(
            original_debt,
            StoredEdge::DetachedCursorRelease(cursor_release(4, epoch(2), 10, original_debt))
        )
    );

    let projection = ObserverProjection::new(15);
    let pending = projection.apply_ordinary_binding_fate(next_debt, fate);
    assert_eq!(
        pending.current_state(),
        owed(next_debt, StoredEdge::ObserverProjection(projection))
    );
    let pending = projection
        .complete_after_binding_fate(Event::projection_completed(14), Some(next_debt), pending)
        .expect_err("another projection boundary preserves authority");
    let state = projection
        .complete_after_binding_fate(Event::projection_completed(15), Some(next_debt), pending)
        .expect("exact completion installs ordinary DCursor");
    let ClosureState::Owed {
        edge: StoredEdge::DetachedCursorRelease(release),
        ..
    } = state
    else {
        panic!("ordinary cursor fate did not survive projection")
    };
    assert_eq!(release.participant_id(), 4);
    assert_eq!(release.last_dead_binding_epoch(), epoch(2));
}

#[test]
fn fenced_recovery_fate_preserves_op_authority_until_exact_completion() {
    let recovery_debt = debt(2, 20);
    let attached_debt = debt(2, 18);
    let fate_debt = debt(2, 16);
    let completion_debt = debt(1, 8);
    let prior_epoch = epoch(2);
    let recovered_epoch = epoch(3);
    let recovery = credential_recovery(4, prior_epoch, 14, recovery_debt);
    let projection = ObserverProjection::new(15);
    let commit = recovery
        .fenced_attach(
            recovery_debt,
            Event::fenced_recovery_committed(4, 14, prior_epoch, recovered_epoch, 15),
            DebtCompletion::observer_projection(attached_debt, projection),
        )
        .expect("exact fenced attach installs nonzero-debt OP");

    assert_eq!(
        commit.recovered_binding_fate(Event::binding_fate_observed(5, recovered_epoch, 15,)),
        Err(commit.next_state()),
        "another participant cannot consume the recovered epoch's fate"
    );
    assert_eq!(
        commit.recovered_binding_fate(Event::binding_fate_observed(4, epoch(4), 15)),
        Err(commit.next_state()),
        "only the exact newly committed epoch can derive the cursor suffix"
    );

    let authority = commit
        .recovered_binding_fate(Event::binding_fate_observed(4, recovered_epoch, 15))
        .expect("exact recovered-epoch fate derives suffix authority");
    assert_eq!(authority.participant_id(), 4);
    assert_eq!(authority.last_dead_binding_epoch(), recovered_epoch);
    assert_eq!(authority.predecessor_state(), commit.next_state());

    let authority = projection
        .apply_recovered_binding_fate(recovery_debt, fate_debt, authority)
        .expect_err("authority is bound to the exact post-attach debt");
    let transition = projection
        .apply_recovered_binding_fate(attached_debt, fate_debt, authority)
        .expect("OP preserves fate's exact cursor suffix");
    let RecoveredBindingFateTransition::PendingStorage(pending) = transition else {
        panic!("OP fate must remain pending until projection completion")
    };
    assert_eq!(
        pending.current_state(),
        owed(fate_debt, StoredEdge::ObserverProjection(projection))
    );
    assert_eq!(pending.participant_id(), 4);
    assert_eq!(pending.last_dead_binding_epoch(), recovered_epoch);

    let pending = projection
        .complete_after_recovered_binding_fate(
            Event::projection_completed(14),
            Some(completion_debt),
            pending,
        )
        .expect_err("an inexact OP completion must reissue latent authority");
    let state = projection
        .complete_after_recovered_binding_fate(
            Event::projection_completed(15),
            Some(completion_debt),
            pending,
        )
        .expect("exact OP completion consumes latent cursor authority");
    let ClosureState::Owed {
        debt: actual_debt,
        edge: StoredEdge::DetachedCursorRelease(release),
    } = state
    else {
        panic!("OP completion did not select recovered DCursor: {state:?}")
    };
    assert_eq!(actual_debt, completion_debt);
    assert_eq!(release.participant_id(), 4);
    assert_eq!(release.last_dead_binding_epoch(), recovered_epoch);
}

#[test]
fn fenced_recovery_fate_preserves_or_covers_pc_with_exact_cursor_authority() {
    let recovery_debt = debt(2, 20);
    let attached_debt = debt(2, 18);
    let fate_debt = debt(2, 16);
    let completion_debt = debt(1, 8);
    let prior_epoch = epoch(2);
    let recovered_epoch = epoch(3);
    let recovery = credential_recovery(4, prior_epoch, 14, recovery_debt);
    let physical = compaction(10, 15);
    let commit = recovery
        .fenced_attach(
            recovery_debt,
            Event::fenced_recovery_committed(4, 14, prior_epoch, recovered_epoch, 10),
            DebtCompletion::physical_compaction(attached_debt, physical),
        )
        .expect("exact fenced attach installs nonzero-debt PC");

    let authority = commit
        .recovered_binding_fate(Event::binding_fate_observed(4, recovered_epoch, 15))
        .expect("fate at the PC boundary derives exact suffix authority");
    let authority = compaction(10, 14)
        .apply_recovered_binding_fate(attached_debt, fate_debt, authority)
        .expect_err("another PC range must return the authority unconsumed");
    let transition = physical
        .apply_recovered_binding_fate(attached_debt, fate_debt, authority)
        .expect("a non-covering fate preserves exact PC");
    let RecoveredBindingFateTransition::PendingStorage(pending) = transition else {
        panic!("a fate floor at through_seq must preserve PC")
    };
    assert_eq!(
        pending.current_state(),
        owed(fate_debt, StoredEdge::PhysicalCompaction(physical))
    );
    let completion = Event::compaction_completed(10, 15, 16).expect("exact PC completion");
    let state = physical
        .complete_after_recovered_binding_fate(completion, Some(completion_debt), pending)
        .expect("exact PC completion consumes latent cursor authority");
    let ClosureState::Owed {
        debt: actual_debt,
        edge: StoredEdge::DetachedCursorRelease(release),
    } = state
    else {
        panic!("PC completion did not select recovered DCursor: {state:?}")
    };
    assert_eq!(actual_debt, completion_debt);
    assert_eq!(release.participant_id(), 4);
    assert_eq!(release.last_dead_binding_epoch(), recovered_epoch);

    let covering_authority = commit
        .recovered_binding_fate(Event::binding_fate_observed(4, recovered_epoch, 16))
        .expect("covering fate derives a fresh replay-identical authority");
    let covering = physical
        .apply_recovered_binding_fate(attached_debt, fate_debt, covering_authority)
        .expect("a greater fate floor covers PC");
    let RecoveredBindingFateTransition::DetachedCursorRelease(released) = covering else {
        panic!("a fate floor beyond through_seq must cover PC")
    };
    assert_eq!(released.debt(), fate_debt);
    assert_eq!(released.edge().participant_id(), 4);
    assert_eq!(released.edge().last_dead_binding_epoch(), recovered_epoch);
    assert_eq!(
        released.into_state(),
        owed(fate_debt, StoredEdge::DetachedCursorRelease(release))
    );
}

#[test]
fn detached_recovery_leave_requires_exact_private_k_claim() {
    let original_debt = debt(2, 20);
    let next_debt = debt(1, 10);
    let binding = epoch(2);
    let recovery = credential_recovery(4, binding, 14, original_debt);
    let actual = ResourceVector::new(1, 8);
    let remaining = ResourceVector::new(2, 16);
    let evidence = recovery
        .validate_leave_claim(4, actual, remaining, 1)
        .expect("current target has a positive bounded K claim and exit claim");
    assert_eq!(evidence.participant_id(), 4);
    assert_eq!(evidence.actual_charge(), actual);
    assert!(
        recovery
            .validate_leave_claim(5, actual, remaining, 1)
            .is_none()
    );
    assert!(
        recovery
            .validate_leave_claim(4, ResourceVector::new(0, 8), remaining, 1)
            .is_none()
    );
    assert!(
        recovery
            .validate_leave_claim(4, ResourceVector::new(1, 0), remaining, 1)
            .is_none()
    );
    assert!(
        recovery
            .validate_leave_claim(4, ResourceVector::new(3, 8), remaining, 1)
            .is_none()
    );
    assert!(
        recovery
            .validate_leave_claim(4, ResourceVector::new(1, 17), remaining, 1)
            .is_none()
    );
    assert!(
        recovery
            .validate_leave_claim(4, actual, remaining, 0)
            .is_none()
    );
    assert_eq!(
        recovery.detached_leave(
            original_debt,
            Event::detached_leave_committed(4, 15),
            evidence,
            DebtCompletion::physical_compaction(next_debt, compaction(15, 15)),
        ),
        Ok(owed(
            next_debt,
            StoredEdge::PhysicalCompaction(compaction(15, 15))
        ))
    );

    let release = marker_release(4, binding, 14, original_debt);
    let wrong_target = release
        .validate_leave_claim(4, actual, remaining, 1)
        .expect("the different edge can validate only its own target");
    assert!(
        recovery
            .detached_leave(
                original_debt,
                Event::detached_leave_committed(4, 15),
                wrong_target,
                DebtCompletion::clear(),
            )
            .is_err(),
        "private evidence remains bound to the exact detached edge kind"
    );
}

#[test]
fn detached_marker_release_is_leave_only() {
    let original_debt = debt(2, 20);
    let next_debt = debt(1, 10);
    let binding = epoch(2);
    let release = marker_release(4, binding, 14, original_debt);
    assert_eq!(
        release.ordinary_attach_refusal(),
        DetachedAttachRefusal::RecoveryFence
    );
    assert_eq!(
        release.marker_attach_refusal(14),
        DetachedAttachRefusal::MarkerNotDelivered
    );
    assert_eq!(
        release.marker_attach_refusal(15),
        DetachedAttachRefusal::MarkerMismatch
    );
    assert_eq!(
        release.binding_required_refusal(),
        DetachedAttachRefusal::NoBinding
    );

    let evidence = release
        .validate_leave_claim(4, ResourceVector::new(1, 8), ResourceVector::new(1, 8), 1)
        .expect("exact DMR K claim validates");
    assert_eq!(
        release.leave(
            original_debt,
            Event::detached_leave_committed(4, 15),
            evidence,
            DebtCompletion::observer_projection(next_debt, ObserverProjection::new(15)),
        ),
        Ok(owed(
            next_debt,
            StoredEdge::ObserverProjection(ObserverProjection::new(15))
        ))
    );
    assert!(
        release
            .leave(
                original_debt,
                Event::live_leave_committed(4, binding, 15),
                evidence,
                DebtCompletion::clear(),
            )
            .is_err()
    );
    assert_eq!(
        release.repeat_fate(Event::binding_fate_observed(4, binding, 2)),
        Ok(release)
    );
    assert_eq!(
        release.repeat_fate(Event::binding_fate_observed(4, epoch(3), 2)),
        Err(release)
    );
    assert_eq!(
        release.authority_superseded(),
        (release, DetachedAttachRefusal::StaleAuthority)
    );
}

#[test]
fn detached_cursor_release_is_leave_only_and_unrelated_facts_are_safe() {
    let original_debt = debt(2, 20);
    let next_debt = debt(1, 10);
    let binding = epoch(2);
    let release = cursor_release(4, binding, 10, original_debt);
    assert_eq!(release.participant_id(), 4);
    assert_eq!(release.last_dead_binding_epoch(), binding);
    assert_eq!(
        release.ordinary_attach_refusal(),
        DetachedAttachRefusal::RecoveryFence
    );
    assert_eq!(
        release.marker_attach_refusal(),
        DetachedAttachRefusal::MarkerMismatch
    );
    assert_eq!(
        release.binding_required_refusal(),
        DetachedAttachRefusal::NoBinding
    );

    let evidence = release
        .validate_leave_claim(4, ResourceVector::new(1, 8), ResourceVector::new(1, 8), 1)
        .expect("exact DCursor K claim validates");
    assert_eq!(
        release.leave(
            original_debt,
            Event::detached_leave_committed(4, 11),
            evidence,
            DebtCompletion::clear(),
        ),
        Ok(ClosureState::Clear)
    );
    assert_eq!(
        release.repeat_fate(Event::binding_fate_observed(4, binding, 3)),
        Ok(release)
    );
    assert_eq!(
        release.authority_superseded(),
        (release, DetachedAttachRefusal::StaleAuthority)
    );

    let unrelated = cursor_event(9, binding, 0, 3, 1);
    assert_eq!(
        release.unrelated_event(original_debt, unrelated, Some(next_debt)),
        Ok(owed(next_debt, StoredEdge::DetachedCursorRelease(release)))
    );
    assert_eq!(
        release.unrelated_event(original_debt, cursor_event(9, binding, 0, 3, 1), None,),
        Ok(ClosureState::Clear)
    );
    assert!(
        release
            .unrelated_event(
                original_debt,
                cursor_event(4, binding, 0, 3, 1),
                Some(next_debt),
            )
            .is_err()
    );

    let recovery = credential_recovery(4, binding, 14, original_debt);
    assert!(
        recovery
            .unrelated_event(
                original_debt,
                Event::projection_completed(3),
                Some(next_debt),
            )
            .is_err(),
        "storage completion is not an unrelated participant occurrence"
    );
    assert!(
        recovery
            .unrelated_event(
                original_debt,
                Event::binding_fate_observed(4, binding, 3),
                Some(next_debt),
            )
            .is_err(),
        "the detached owner cannot use the cross-identity escape hatch"
    );
}
