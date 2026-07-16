#![allow(
    clippy::expect_used,
    clippy::large_types_passed_by_value,
    clippy::panic,
    clippy::too_many_lines
)]

use alloc::vec;

use crate::algebra::WideResourceVector;
use crate::wire::{
    AttachSecret, BindingEpoch, CloseCause, ConnectionIncarnation, DetachAttemptToken, Generation,
    LeaveAttemptToken,
};

use super::storage::{
    BindingFateTerminalRestore, BindingStateRestore, ClosureStateRestore,
    CommittedBindingTerminalRestore, CursorEpisodeRestore, DebtCompletionRestore,
    DetachCellRestore, DetachedCredentialRecoveryRestore, DetachedCursorReleaseProvenanceRestore,
    DetachedMarkerReleaseRestore, FencedAttachCommitRestore, LeaveCommittedRestore,
    LiveIdentityRestore, MarkerCursorProgressRestore, MarkerDeliveryRestore,
    OrdinaryBindingAuthorityRestore, OrdinaryBindingFateRestore, ParticipantLifecycleRestore,
    PendingFinalizationRestore, PendingRecoveredCursorReleaseRestore, RecoveredBindingFateRestore,
    RecoveredStorageCompletionRestore, RestoredParticipantLifecycle, RetiredIdentityRestore,
    StorageRestoreError, StoredEdgeRestore,
};
use super::{
    ActiveBinding, AdmissionOrder, BindingState, BoundParticipantCursor, CursorProgressFact,
    CursorProgressKey, DetachCell, EnrollmentFingerprint, LeaveFingerprint, StoredEdge,
};

type TestSnapshot = ParticipantLifecycleRestore<[u8; 4], [u8; 4], [u8; 4], [u8; 4]>;

const PARTICIPANT_ID: u64 = 7;
const CONVERSATION_ID: u64 = 41;

fn generation(value: u64) -> Generation {
    Generation::new(value).expect("test generations are nonzero")
}

fn epoch(generation: u64, ordinal: u64) -> BindingEpoch {
    BindingEpoch::new(
        ConnectionIncarnation::new(3, ordinal),
        self::generation(generation),
    )
}

fn binding(participant_id: u64, binding_epoch: BindingEpoch) -> ActiveBinding {
    ActiveBinding {
        participant_id,
        conversation_id: CONVERSATION_ID,
        binding_epoch,
    }
}

fn live_identity(
    participant_id: u64,
    generation: Generation,
    latest_terminal: Option<CommittedBindingTerminalRestore>,
) -> LiveIdentityRestore<[u8; 4]> {
    LiveIdentityRestore {
        participant_id,
        conversation_id: CONVERSATION_ID,
        generation,
        attach_secret: AttachSecret::new([0xA1; 32]),
        cursor: 2,
        enrollment_fingerprint: EnrollmentFingerprint::new([0xE1; 4]),
        latest_terminal,
    }
}

fn detach_token() -> DetachAttemptToken {
    DetachAttemptToken::new([0xD1; 16])
}

fn leave_token() -> LeaveAttemptToken {
    LeaveAttemptToken::new([0xB1; 16])
}

fn clean_terminal(
    participant_id: u64,
    binding_epoch: BindingEpoch,
    transaction_order: u64,
    delivery_seq: u64,
) -> CommittedBindingTerminalRestore {
    CommittedBindingTerminalRestore {
        binding: binding(participant_id, binding_epoch),
        cause: CloseCause::CleanDeregister,
        transaction_order,
        delivery_seq,
    }
}

fn died_terminal(
    participant_id: u64,
    binding_epoch: BindingEpoch,
    transaction_order: u64,
    delivery_seq: u64,
) -> CommittedBindingTerminalRestore {
    CommittedBindingTerminalRestore {
        binding: binding(participant_id, binding_epoch),
        cause: CloseCause::ConnectionLost,
        transaction_order,
        delivery_seq,
    }
}

#[test]
fn binding_terminals_restore_only_from_valid_cause_partitions() {
    let old_epoch = epoch(2, 8);
    let committed = clean_terminal(PARTICIPANT_ID, old_epoch, 11, 20)
        .restore()
        .expect("clean detach terminal is valid");
    assert_eq!(committed.participant_id(), PARTICIPANT_ID);
    assert_eq!(committed.binding_epoch(), old_epoch);
    assert_eq!(committed.delivery_seq(), 20);

    let pending = PendingFinalizationRestore {
        binding: binding(PARTICIPANT_ID, old_epoch),
        cause: CloseCause::ConnectionLost,
        transaction_order: 12,
    }
    .restore()
    .expect("connection loss can remain pending");
    assert_eq!(pending.participant_id(), PARTICIPANT_ID);
    assert_eq!(pending.binding_epoch(), old_epoch);

    assert_eq!(
        PendingFinalizationRestore {
            binding: binding(PARTICIPANT_ID, old_epoch),
            cause: CloseCause::Superseded,
            transaction_order: 12,
        }
        .restore(),
        Err(StorageRestoreError::PendingFinalization),
        "supersession is an indivisible committed handoff"
    );
    assert_eq!(
        CommittedBindingTerminalRestore {
            binding: binding(PARTICIPANT_ID, old_epoch),
            cause: CloseCause::UncleanServerRestart {
                prior_server_incarnation: 99,
            },
            transaction_order: 13,
            delivery_seq: 21,
        }
        .restore(),
        Err(StorageRestoreError::CommittedBindingTerminal),
        "unclean-restart suffix must name the epoch's prior server"
    );
}

#[test]
fn live_membership_rejects_terminal_identity_and_generation_drift() {
    let terminal_epoch = epoch(2, 8);
    let wrong_identity = TestSnapshot::Live {
        identity: live_identity(
            PARTICIPANT_ID,
            generation(2),
            Some(clean_terminal(PARTICIPANT_ID + 1, terminal_epoch, 11, 20)),
        ),
        binding: BindingStateRestore::Detached,
        detach_cell: DetachCellRestore::Empty,
    };
    assert_eq!(
        wrong_identity.restore(),
        Err(StorageRestoreError::MembershipInvariant)
    );

    let newer_terminal = TestSnapshot::Live {
        identity: live_identity(
            PARTICIPANT_ID,
            generation(2),
            Some(clean_terminal(PARTICIPANT_ID, epoch(3, 9), 11, 20)),
        ),
        binding: BindingStateRestore::Detached,
        detach_cell: DetachCellRestore::Empty,
    };
    assert_eq!(
        newer_terminal.restore(),
        Err(StorageRestoreError::MembershipInvariant)
    );
}

#[test]
fn retired_tombstone_validates_complete_permanent_leave_result() {
    let retired_generation = generation(4);
    let raw = RetiredIdentityRestore {
        participant_id: PARTICIPANT_ID,
        conversation_id: CONVERSATION_ID,
        retired_generation,
        enrollment_fingerprint: EnrollmentFingerprint::new([0xE2; 4]),
        leave_attempt_token: leave_token(),
        leave_request_verifier: [0xA2; 4],
        leave_fingerprint: LeaveFingerprint::new([0xF2; 4]),
        committed_result: LeaveCommittedRestore {
            conversation_id: CONVERSATION_ID,
            leave_attempt_token: leave_token(),
            participant_id: PARTICIPANT_ID,
            retired_generation,
            ended_binding_epoch: Some(epoch(4, 12)),
            prior_terminal_delivery_seq: Some(30),
            left_delivery_seq: 31,
        },
    };
    let restored: RestoredParticipantLifecycle<[u8; 4], [u8; 4], [u8; 4], [u8; 4]> =
        TestSnapshot::Retired(raw.clone())
            .restore()
            .expect("complete matching tombstone restores");
    let RestoredParticipantLifecycle::Retired(retired) = restored else {
        panic!("retired capsule must not produce live slots")
    };
    assert_eq!(retired.participant_id(), PARTICIPANT_ID);
    assert_eq!(retired.retired_generation(), retired_generation);

    let mut wrong_owner = raw.clone();
    wrong_owner.participant_id += 1;
    assert_eq!(
        TestSnapshot::Retired(wrong_owner).restore(),
        Err(StorageRestoreError::RetiredIdentity)
    );

    let mut invalid_result = raw;
    invalid_result.committed_result.left_delivery_seq = 30;
    assert_eq!(
        TestSnapshot::Retired(invalid_result).restore(),
        Err(StorageRestoreError::LeaveResult)
    );
}

#[test]
fn all_four_detach_cells_restore_with_atomic_binding_pair_checks() {
    let old_epoch = epoch(2, 8);
    let pending_order = AdmissionOrder::binding_terminal(11, PARTICIPANT_ID);
    let pending = TestSnapshot::Live {
        identity: live_identity(PARTICIPANT_ID, generation(2), None),
        binding: BindingStateRestore::PendingFinalization(PendingFinalizationRestore {
            binding: binding(PARTICIPANT_ID, old_epoch),
            cause: CloseCause::CleanDeregister,
            transaction_order: 11,
        }),
        detach_cell: DetachCellRestore::Pending {
            token: detach_token(),
            participant_id: PARTICIPANT_ID,
            request_generation: generation(2),
            request_verifier: [0xD2; 4],
            committed_binding_epoch: old_epoch,
            admission_order: pending_order,
            refused_epoch: 9,
        },
    }
    .restore()
    .expect("matching pending terminal and detach cell restore");
    assert!(matches!(
        pending,
        RestoredParticipantLifecycle::Live {
            binding: BindingState::PendingFinalization(_),
            detach_cell: DetachCell::Pending(_),
            ..
        }
    ));

    let committed = TestSnapshot::Live {
        identity: live_identity(
            PARTICIPANT_ID,
            generation(2),
            Some(clean_terminal(PARTICIPANT_ID, old_epoch, 11, 20)),
        ),
        binding: BindingStateRestore::Detached,
        detach_cell: DetachCellRestore::Committed {
            token: detach_token(),
            participant_id: PARTICIPANT_ID,
            request_generation: generation(2),
            request_verifier: [0xD2; 4],
            committed_binding_epoch: old_epoch,
            detached_delivery_seq: 20,
        },
    }
    .restore()
    .expect("committed cell matches retained clean terminal");
    assert!(matches!(
        committed,
        RestoredParticipantLifecycle::Live {
            binding: BindingState::Detached,
            detach_cell: DetachCell::Committed(_),
            ..
        }
    ));

    let terminalized = TestSnapshot::Live {
        identity: live_identity(
            PARTICIPANT_ID,
            generation(3),
            Some(clean_terminal(PARTICIPANT_ID, old_epoch, 11, 20)),
        ),
        binding: BindingStateRestore::Bound(binding(PARTICIPANT_ID, epoch(3, 9))),
        detach_cell: DetachCellRestore::Terminalized {
            token: detach_token(),
            participant_id: PARTICIPANT_ID,
            request_generation: generation(2),
            request_verifier: [0xD2; 4],
            committed_binding_epoch: old_epoch,
        },
    }
    .restore()
    .expect("post-attach terminalized cell retains old epoch");
    assert!(matches!(
        terminalized,
        RestoredParticipantLifecycle::Live {
            binding: BindingState::Bound(_),
            detach_cell: DetachCell::Terminalized(_),
            ..
        }
    ));

    let empty = TestSnapshot::Live {
        identity: live_identity(PARTICIPANT_ID, generation(3), None),
        binding: BindingStateRestore::Bound(binding(PARTICIPANT_ID, epoch(3, 9))),
        detach_cell: DetachCellRestore::Empty,
    }
    .restore()
    .expect("ordinary bound membership may have an empty detach cell");
    assert!(matches!(
        empty,
        RestoredParticipantLifecycle::Live {
            detach_cell: DetachCell::Empty(_),
            ..
        }
    ));

    let wrong_pair = TestSnapshot::Live {
        identity: live_identity(PARTICIPANT_ID, generation(2), None),
        binding: BindingStateRestore::PendingFinalization(PendingFinalizationRestore {
            binding: binding(PARTICIPANT_ID, old_epoch),
            cause: CloseCause::CleanDeregister,
            transaction_order: 12,
        }),
        detach_cell: DetachCellRestore::Pending {
            token: detach_token(),
            participant_id: PARTICIPANT_ID,
            request_generation: generation(2),
            request_verifier: [0xD2; 4],
            committed_binding_epoch: old_epoch,
            admission_order: pending_order,
            refused_epoch: 9,
        },
    };
    assert_eq!(
        wrong_pair.restore(),
        Err(StorageRestoreError::DetachBindingPair)
    );

    let stale_generation = TestSnapshot::Live {
        identity: live_identity(
            PARTICIPANT_ID,
            generation(2),
            Some(clean_terminal(PARTICIPANT_ID, old_epoch, 11, 20)),
        ),
        binding: BindingStateRestore::Detached,
        detach_cell: DetachCellRestore::Terminalized {
            token: detach_token(),
            participant_id: PARTICIPANT_ID,
            request_generation: generation(2),
            request_verifier: [0xD2; 4],
            committed_binding_epoch: old_epoch,
        },
    };
    assert_eq!(
        stale_generation.restore(),
        Err(StorageRestoreError::DetachBindingPair)
    );
}

#[test]
fn cursor_episode_restores_variable_participant_facts_and_rejects_bad_pairs() {
    let first_epoch = epoch(1, 1);
    let second_epoch = epoch(1, 2);
    let facts = vec![
        (
            CursorProgressKey {
                participant_index: 1,
                boundary: 1,
            },
            CursorProgressFact::Consumed,
        ),
        (
            CursorProgressKey {
                participant_index: 1,
                boundary: 2,
            },
            CursorProgressFact::Consumed,
        ),
        (
            CursorProgressKey {
                participant_index: 2,
                boundary: 1,
            },
            CursorProgressFact::Consumed,
        ),
        (
            CursorProgressKey {
                participant_index: 2,
                boundary: 2,
            },
            CursorProgressFact::Consumed,
        ),
    ];
    let raw = CursorEpisodeRestore {
        conversation_id: CONVERSATION_ID,
        debt: WideResourceVector::new(1, 8),
        observer_progress: 0,
        candidate_high_watermark: 2,
        current_floor: 1,
        cap_floor: 1,
        participants: vec![
            BoundParticipantCursor::new(1, first_epoch, 2),
            BoundParticipantCursor::new(2, second_epoch, 2),
        ],
        facts,
    };
    let restored = raw
        .clone()
        .restore()
        .expect("all four participant/boundary facts are durable");
    assert_eq!(restored.facts().len(), 4);
    assert_eq!(restored.retained_suffix_start(), Some(1));
    assert!(restored.retains(1));
    assert!(restored.retains(2));

    let mut unknown_participant = raw.clone();
    unknown_participant.facts[0].0.participant_index = 99;
    assert_eq!(
        unknown_participant.restore(),
        Err(StorageRestoreError::CursorEpisode)
    );

    let mut impossible_pending = raw.clone();
    impossible_pending.facts[0].1 = CursorProgressFact::Pending;
    assert_eq!(
        impossible_pending.restore(),
        Err(StorageRestoreError::CursorEpisode)
    );

    let mut duplicate = raw;
    duplicate.facts.push(duplicate.facts[0]);
    assert_eq!(duplicate.restore(), Err(StorageRestoreError::CursorEpisode));

    assert_eq!(
        CursorEpisodeRestore {
            conversation_id: CONVERSATION_ID,
            debt: WideResourceVector::new(0, 0),
            observer_progress: 0,
            candidate_high_watermark: 0,
            current_floor: 0,
            cap_floor: 0,
            participants: vec![],
            facts: vec![],
        }
        .restore(),
        Err(StorageRestoreError::ClosureDebt)
    );
}

fn marker_delivery_restore(
    participant_id: u64,
    binding_epoch: BindingEpoch,
    marker_delivery_seq: u64,
) -> MarkerDeliveryRestore {
    MarkerDeliveryRestore {
        participant_id,
        binding_epoch,
        marker_delivery_seq,
    }
}

fn marker_progress_restore(
    participant_id: u64,
    binding_epoch: BindingEpoch,
    marker_delivery_seq: u64,
) -> MarkerCursorProgressRestore {
    MarkerCursorProgressRestore {
        conversation_id: CONVERSATION_ID,
        participant_id,
        binding_epoch,
        through_seq: marker_delivery_seq,
        marker_delivery_seq,
        delivery: marker_delivery_restore(participant_id, binding_epoch, marker_delivery_seq),
    }
}

fn dcr_restore(
    participant_id: u64,
    binding_epoch: BindingEpoch,
    marker_delivery_seq: u64,
) -> DetachedCredentialRecoveryRestore {
    DetachedCredentialRecoveryRestore {
        participant_id,
        marker_delivery_seq,
        prior_binding_epoch: binding_epoch,
        resulting_floor: 1,
        terminal: BindingFateTerminalRestore::Committed(died_terminal(
            participant_id,
            binding_epoch,
            8,
            marker_delivery_seq + 1,
        )),
        progress: marker_progress_restore(participant_id, binding_epoch, marker_delivery_seq),
    }
}

fn dmr_restore(
    participant_id: u64,
    binding_epoch: BindingEpoch,
    marker_delivery_seq: u64,
) -> DetachedMarkerReleaseRestore {
    DetachedMarkerReleaseRestore {
        conversation_id: CONVERSATION_ID,
        participant_id,
        marker_delivery_seq,
        last_dead_binding_epoch: binding_epoch,
        resulting_floor: 1,
        terminal: BindingFateTerminalRestore::Committed(died_terminal(
            participant_id,
            binding_epoch,
            8,
            marker_delivery_seq + 1,
        )),
        delivery: marker_delivery_restore(participant_id, binding_epoch, marker_delivery_seq),
    }
}

fn owed(edge: StoredEdgeRestore) -> ClosureStateRestore {
    ClosureStateRestore::Owed {
        debt: WideResourceVector::new(2, 16),
        edge,
    }
}

#[test]
fn all_seven_stored_edges_restore_and_opaque_variants_require_exact_provenance() {
    let binding_epoch = epoch(2, 8);
    let ordinary = OrdinaryBindingAuthorityRestore {
        binding: binding(PARTICIPANT_ID, binding_epoch),
        through_seq: 12,
    };

    assert!(matches!(
        owed(StoredEdgeRestore::ObserverProjection { through_seq: 12 })
            .restore()
            .expect("OP restores"),
        super::ClosureState::Owed {
            edge: StoredEdge::ObserverProjection(_),
            ..
        }
    ));
    assert!(matches!(
        owed(StoredEdgeRestore::PhysicalCompaction {
            from_floor: 4,
            through_seq: 12,
        })
        .restore()
        .expect("PC restores"),
        super::ClosureState::Owed {
            edge: StoredEdge::PhysicalCompaction(_),
            ..
        }
    ));
    assert!(matches!(
        owed(StoredEdgeRestore::MarkerDelivery(marker_delivery_restore(
            PARTICIPANT_ID,
            binding_epoch,
            12,
        )))
        .restore()
        .expect("marker delivery restores"),
        super::ClosureState::Owed {
            edge: StoredEdge::MarkerDelivery(_),
            ..
        }
    ));
    assert!(matches!(
        owed(StoredEdgeRestore::ParticipantCursorProgressContinuous {
            participant_id: PARTICIPANT_ID,
            binding_epoch,
            through_seq: 12,
            authority: ordinary,
        })
        .restore()
        .expect("continuous PCP restores only from ordinary attach"),
        super::ClosureState::Owed {
            edge: StoredEdge::ParticipantCursorProgress(_),
            ..
        }
    ));
    assert!(matches!(
        owed(StoredEdgeRestore::ParticipantCursorProgressMarker(
            marker_progress_restore(PARTICIPANT_ID, binding_epoch, 12),
        ))
        .restore()
        .expect("marker PCP restores from exact delivery"),
        super::ClosureState::Owed {
            edge: StoredEdge::ParticipantCursorProgress(_),
            ..
        }
    ));
    assert!(matches!(
        owed(StoredEdgeRestore::DetachedCredentialRecovery(dcr_restore(
            PARTICIPANT_ID,
            binding_epoch,
            12,
        )))
        .restore()
        .expect("DCR restores from marker delivery plus ack plus fate"),
        super::ClosureState::Owed {
            edge: StoredEdge::DetachedCredentialRecovery(_),
            ..
        }
    ));
    assert!(matches!(
        owed(StoredEdgeRestore::DetachedMarkerRelease(dmr_restore(
            PARTICIPANT_ID,
            binding_epoch,
            12,
        )))
        .restore()
        .expect("DMR restores from undelivered marker plus fate"),
        super::ClosureState::Owed {
            edge: StoredEdge::DetachedMarkerRelease(_),
            ..
        }
    ));

    let fate = OrdinaryBindingFateRestore {
        authority: ordinary,
        terminal: died_terminal(PARTICIPANT_ID, binding_epoch, 9, 13),
        resulting_floor: 4,
    };
    assert!(matches!(
        owed(StoredEdgeRestore::DetachedCursorRelease {
            participant_id: PARTICIPANT_ID,
            last_dead_binding_epoch: binding_epoch,
            provenance: DetachedCursorReleaseProvenanceRestore::Ordinary(fate),
        })
        .restore()
        .expect("DCursor restores from ordinary attach and exact Died terminal"),
        super::ClosureState::Owed {
            edge: StoredEdge::DetachedCursorRelease(_),
            ..
        }
    ));

    assert_eq!(
        owed(StoredEdgeRestore::ParticipantCursorProgressContinuous {
            participant_id: PARTICIPANT_ID + 1,
            binding_epoch,
            through_seq: 12,
            authority: ordinary,
        })
        .restore(),
        Err(StorageRestoreError::StoredEdgeProvenance)
    );
    let mut wrong_marker = marker_progress_restore(PARTICIPANT_ID, binding_epoch, 12);
    wrong_marker.delivery.marker_delivery_seq = 13;
    assert_eq!(
        owed(StoredEdgeRestore::ParticipantCursorProgressMarker(
            wrong_marker
        ))
        .restore(),
        Err(StorageRestoreError::StoredEdgeProvenance)
    );
    let wrong_terminal = OrdinaryBindingFateRestore {
        authority: ordinary,
        terminal: died_terminal(PARTICIPANT_ID + 1, binding_epoch, 9, 13),
        resulting_floor: 4,
    };
    assert_eq!(
        owed(StoredEdgeRestore::DetachedCursorRelease {
            participant_id: PARTICIPANT_ID,
            last_dead_binding_epoch: binding_epoch,
            provenance: DetachedCursorReleaseProvenanceRestore::Ordinary(wrong_terminal),
        })
        .restore(),
        Err(StorageRestoreError::StoredEdgeProvenance)
    );
    assert_eq!(
        ClosureStateRestore::Owed {
            debt: WideResourceVector::new(0, 0),
            edge: StoredEdgeRestore::ObserverProjection { through_seq: 1 },
        }
        .restore(),
        Err(StorageRestoreError::ClosureDebt)
    );
}

fn fenced_attach_restore(successor: DebtCompletionRestore) -> FencedAttachCommitRestore {
    let prior_epoch = epoch(2, 8);
    FencedAttachCommitRestore {
        predecessor: dcr_restore(PARTICIPANT_ID, prior_epoch, 12),
        predecessor_debt: WideResourceVector::new(3, 24),
        participant_id: PARTICIPANT_ID,
        marker_delivery_seq: 12,
        prior_binding_epoch: prior_epoch,
        new_binding_epoch: epoch(3, 9),
        resulting_floor: 1,
        successor,
    }
}

#[test]
fn fenced_and_recovered_authorities_restore_only_from_exact_epoch_provenance() {
    let after_attach_debt = WideResourceVector::new(2, 16);
    let after_fate_debt = WideResourceVector::new(1, 8);
    let final_debt = WideResourceVector::new(1, 4);
    let fenced = fenced_attach_restore(DebtCompletionRestore::ObserverProjection {
        debt: after_attach_debt,
        through_seq: 20,
    });
    let commit = fenced.restore().expect("exact DCR fenced attach restores");
    assert_eq!(commit.participant_id(), PARTICIPANT_ID);
    assert_eq!(commit.prior_binding_epoch(), epoch(2, 8));
    assert_eq!(commit.new_binding_epoch(), epoch(3, 9));

    let mut wrong_participant = fenced;
    wrong_participant.participant_id += 1;
    assert_eq!(
        wrong_participant.restore(),
        Err(StorageRestoreError::StoredEdgeProvenance)
    );
    let mut wrong_epoch = fenced;
    wrong_epoch.new_binding_epoch = epoch(4, 9);
    assert_eq!(
        wrong_epoch.restore(),
        Err(StorageRestoreError::StoredEdgeProvenance)
    );

    let fate = RecoveredBindingFateRestore {
        fenced_attach: fenced,
        participant_id: PARTICIPANT_ID,
        binding_epoch: epoch(3, 9),
        resulting_floor: 1,
    };
    let restored_fate = fate
        .restore()
        .expect("fate must name the exact recovered epoch");
    assert_eq!(restored_fate.participant_id(), PARTICIPANT_ID);
    assert_eq!(restored_fate.last_dead_binding_epoch(), epoch(3, 9));

    let pending = PendingRecoveredCursorReleaseRestore {
        fate,
        resulting_debt: after_fate_debt,
    };
    let restored_pending = pending
        .restore()
        .expect("OP remains current with a latent recovered cursor release");
    assert_eq!(restored_pending.participant_id(), PARTICIPANT_ID);
    assert_eq!(restored_pending.last_dead_binding_epoch(), epoch(3, 9));

    let mut wrong_fate = fate;
    wrong_fate.binding_epoch = epoch(3, 10);
    assert_eq!(
        wrong_fate.restore(),
        Err(StorageRestoreError::StoredEdgeProvenance)
    );

    let after_storage = ClosureStateRestore::Owed {
        debt: final_debt,
        edge: StoredEdgeRestore::DetachedCursorRelease {
            participant_id: PARTICIPANT_ID,
            last_dead_binding_epoch: epoch(3, 9),
            provenance: DetachedCursorReleaseProvenanceRestore::RecoveredAfterStorage {
                pending,
                completion: RecoveredStorageCompletionRestore::ObserverProjection {
                    through_seq: 20,
                    resulting_debt: Some(final_debt),
                },
            },
        },
    }
    .restore()
    .expect("exact OP completion installs the latent DCursor suffix");
    assert!(matches!(
        after_storage,
        super::ClosureState::Owed {
            edge: StoredEdge::DetachedCursorRelease(_),
            ..
        }
    ));

    let direct_fenced = fenced_attach_restore(DebtCompletionRestore::PhysicalCompaction {
        debt: after_attach_debt,
        from_floor: 10,
        through_seq: 20,
    });
    let direct_fate = RecoveredBindingFateRestore {
        fenced_attach: direct_fenced,
        participant_id: PARTICIPANT_ID,
        binding_epoch: epoch(3, 9),
        resulting_floor: 21,
    };
    let direct = ClosureStateRestore::Owed {
        debt: final_debt,
        edge: StoredEdgeRestore::DetachedCursorRelease {
            participant_id: PARTICIPANT_ID,
            last_dead_binding_epoch: epoch(3, 9),
            provenance: DetachedCursorReleaseProvenanceRestore::RecoveredDirect {
                fate: direct_fate,
                resulting_debt: final_debt,
            },
        },
    }
    .restore()
    .expect("covering recovered fate installs DCursor directly");
    assert!(matches!(
        direct,
        super::ClosureState::Owed {
            edge: StoredEdge::DetachedCursorRelease(_),
            ..
        }
    ));
}
