#![allow(
    clippy::expect_used,
    clippy::large_types_passed_by_value,
    clippy::panic,
    clippy::too_many_lines
)]

use alloc::{vec, vec::Vec};

use crate::algebra::WideResourceVector;
use crate::{
    outcome::CandidatePhase,
    wire::{
        AttachAttemptToken, AttachSecret, BindingEpoch, CloseCause, ConnectionIncarnation,
        CredentialAttachRequest, DetachAttemptToken, EnrollmentRequest, EnrollmentToken,
        Generation, LeaveAttemptToken,
    },
};

use super::claim_frontier::ValidatedMarkerRecord;
use super::edge::validated_marker_record_for_test;
use super::storage::{
    BindingFateTerminalRestore, BindingStateRestore, ClosureStateRestore,
    CommittedBindingTerminalRestore, ConversationStateRestoreError, CursorEpisodeRestore,
    DebtCompletionRestore, DetachCellRestore, DetachedCredentialRecoveryRestore,
    DetachedCursorReleaseProvenanceRestore, DetachedMarkerReleaseRestore,
    FencedAttachCommitRestore, LeaveCommittedRestore, LiveIdentityRestore,
    MarkerCursorProgressRestore, MarkerDeliveryRestore, OrdinaryBindingAuthorityRestore,
    OrdinaryBindingFateRestore, ParticipantConversationRestore, ParticipantLifecycleRestore,
    PendingFinalizationRestore, PendingRecoveredCursorReleaseRestore, RecoveredBindingFateRestore,
    RecoveredStorageCompletionRestore, RestoredParticipantLifecycle, RetiredIdentityRestore,
    StorageRestoreError, StoredEdgeRestore, restore_conversation_state,
};
use super::{
    ActiveBinding, AdmissionOrder, AllocatedParticipantSlot, AttachCommitParameters,
    AttachSecretProof, AttachedRecordPosition, BindingOrigin, BindingState, BindingTerminalOwner,
    BoundParticipantCursor, ClaimFrontiersRestore, ClosureState, CursorProgressFact,
    CursorProgressKey, DetachCell, EnrollmentCommitParameters, EnrollmentFingerprint,
    FrontierBinding, FrontierParticipant, HistoricalCausalFactRestore,
    HistoricalMarkerDeliveryFactRestore, ImmutableOrderCandidateMajorRestore,
    ImmutableSequenceCandidate, LeaveFingerprint, LiveMember, LiveMemberRestore,
    MarkerCandidateAuthority, MarkerProvenance, MarkerSequenceOwner, MovableOrderClaim,
    MovableSequenceClaim, OrderClaimFrontierRestore, OrderClaims, OrderDirectOwner, OrderHigh,
    OrderLedger, ParticipantSlotAllocatorProof, RecoveryOrderActiveBindingRestore,
    RecoveryOrderBlockRestore, RecoverySequenceBlockRestore, RecoverySequenceReserve,
    RecoverySequenceTerminalRestore, ReplacementTerminalProductRangeRestore, RetainedCausalRecord,
    RetainedCausalRecordKind, SequenceClaimFrontierRestore, SequenceClaims, SequenceDirectOwner,
    SequenceLedger, SequenceProductRangesRestore, StoredEdge, TerminalProductRangeRestore,
    commit_attach, commit_enrollment,
};

type TestSnapshot = ParticipantLifecycleRestore<[u8; 4], [u8; 4], [u8; 4], [u8; 4]>;

const PARTICIPANT_ID: u64 = 7;
const CONVERSATION_ID: u64 = 41;

#[derive(Clone, Debug, PartialEq, Eq)]
struct StorageAllocationProof;

impl ParticipantSlotAllocatorProof for StorageAllocationProof {
    fn conversation_id(&self) -> u64 {
        CONVERSATION_ID
    }

    fn participant_index(&self) -> u64 {
        PARTICIPANT_ID
    }

    fn identity_limit(&self) -> u64 {
        PARTICIPANT_ID + 1
    }
}

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

fn unfenced_origin(binding_epoch: BindingEpoch) -> BindingOrigin {
    ordinary_origin(binding_epoch, AttachedRecordPosition::new(1, 1))
}

fn ordinary_origin(
    binding_epoch: BindingEpoch,
    attached_position: AttachedRecordPosition,
) -> BindingOrigin {
    let prior_generation = Generation::new(
        binding_epoch
            .capability_generation
            .get()
            .checked_sub(1)
            .expect("ordinary attach fixture starts after generation one"),
    )
    .expect("ordinary attach fixture prior generation is nonzero");
    let prior_secret = AttachSecret::new([0x91; 32]);
    let member = LiveMember::restore(LiveMemberRestore {
        participant_id: PARTICIPANT_ID,
        conversation_id: CONVERSATION_ID,
        generation: prior_generation,
        attach_secret: prior_secret,
        cursor: 12,
        enrollment_fingerprint: EnrollmentFingerprint::new([0xE1; 4]),
        latest_terminal: None,
    })
    .expect("ordinary attach replay member is valid");
    let request = CredentialAttachRequest {
        conversation_id: CONVERSATION_ID,
        participant_id: PARTICIPANT_ID,
        capability_generation: prior_generation,
        attach_secret: prior_secret,
        attach_attempt_token: AttachAttemptToken::new([0x91; 16]),
        accept_marker_delivery_seq: None,
    };
    let verified = member
        .verify_detached_attach(
            BindingState::Detached,
            ClosureState::Clear
                .ordinary_detached_attach_admission()
                .expect("clear replay state admits ordinary attach"),
            request,
            AttachSecretProof::Verified,
            AttachCommitParameters {
                binding: binding(PARTICIPANT_ID, binding_epoch),
                attach_secret: AttachSecret::new([0xA1; 32]),
                attached_position,
                receipt_expires_at: 100,
                provenance_expires_at: 200,
            },
        )
        .expect("ordinary attach replay verifies");
    commit_attach(verified, DetachCell::<[u8; 4]>::default())
        .expect("ordinary attach replay commits")
        .binding_origin()
}

fn enrollment_origin(attached_position: AttachedRecordPosition) -> BindingOrigin {
    commit_enrollment(
        &EnrollmentRequest {
            conversation_id: CONVERSATION_ID,
            enrollment_token: EnrollmentToken::new([0x71; 16]),
        },
        EnrollmentCommitParameters {
            allocated_slot: AllocatedParticipantSlot::from_allocator(StorageAllocationProof)
                .expect("enrollment replay slot is in range"),
            attach_secret: AttachSecret::new([0xA1; 32]),
            origin_binding_epoch: epoch(1, 7),
            attached_position,
            receipt_expires_at: 100,
            provenance_expires_at: 200,
            enrollment_fingerprint: EnrollmentFingerprint::new([0xE1; 4]),
        },
    )
    .expect("enrollment event replay commits")
    .binding_origin()
}

fn superseding_origin(
    binding_epoch: BindingEpoch,
    attached_position: AttachedRecordPosition,
) -> (BindingOrigin, CommittedBindingTerminalRestore) {
    let prior_generation = Generation::new(
        binding_epoch
            .capability_generation
            .get()
            .checked_sub(1)
            .expect("superseding replay starts after generation one"),
    )
    .expect("superseding replay prior generation is nonzero");
    let prior_epoch = BindingEpoch::new(ConnectionIncarnation::new(4, 77), prior_generation);
    let prior_secret = AttachSecret::new([0x93; 32]);
    let member = LiveMember::restore(LiveMemberRestore {
        participant_id: PARTICIPANT_ID,
        conversation_id: CONVERSATION_ID,
        generation: prior_generation,
        attach_secret: prior_secret,
        cursor: 12,
        enrollment_fingerprint: EnrollmentFingerprint::new([0xE1; 4]),
        latest_terminal: None,
    })
    .expect("superseding replay member is valid");
    let terminal_delivery_seq = attached_position
        .delivery_seq()
        .checked_sub(1)
        .expect("superseding Attached follows its terminal");
    let verified = member
        .verify_superseding_attach(
            binding(PARTICIPANT_ID, prior_epoch),
            CredentialAttachRequest {
                conversation_id: CONVERSATION_ID,
                participant_id: PARTICIPANT_ID,
                capability_generation: prior_generation,
                attach_secret: prior_secret,
                attach_attempt_token: AttachAttemptToken::new([0x93; 16]),
                accept_marker_delivery_seq: None,
            },
            AttachSecretProof::Verified,
            super::CommittedBindingTerminalPosition::new(
                attached_position.transaction_order(),
                terminal_delivery_seq,
            ),
            AttachCommitParameters {
                binding: binding(PARTICIPANT_ID, binding_epoch),
                attach_secret: AttachSecret::new([0xA1; 32]),
                attached_position,
                receipt_expires_at: 100,
                provenance_expires_at: 200,
            },
        )
        .expect("superseding event replay verifies");
    let committed = commit_attach(verified, DetachCell::<[u8; 4]>::default())
        .expect("superseding event replay commits");
    (
        committed.binding_origin(),
        CommittedBindingTerminalRestore {
            binding: binding(PARTICIPANT_ID, prior_epoch),
            cause: CloseCause::Superseded,
            transaction_order: attached_position.transaction_order(),
            delivery_seq: terminal_delivery_seq,
        },
    )
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
        binding_origin: None,
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
        binding_origin: None,
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
        left_transaction_order: 17,
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
    assert_eq!(retired.left_admission_order().transaction_order(), 17);

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
        binding_origin: Some(unfenced_origin(old_epoch)),
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
        binding_origin: Some(unfenced_origin(old_epoch)),
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
        binding_origin: Some(unfenced_origin(epoch(3, 9))),
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
        binding_origin: Some(unfenced_origin(epoch(3, 9))),
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
        binding_origin: Some(unfenced_origin(old_epoch)),
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
        binding_origin: Some(unfenced_origin(old_epoch)),
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

fn bound_marker_record(
    participant_id: u64,
    binding_epoch: BindingEpoch,
    marker_delivery_seq: u64,
    accepted: bool,
) -> ValidatedMarkerRecord {
    validated_marker_record_for_test(
        CONVERSATION_ID,
        participant_id,
        FrontierBinding::Bound(binding_epoch),
        marker_delivery_seq,
        if accepted {
            marker_delivery_seq
        } else {
            marker_delivery_seq.saturating_sub(1)
        },
    )
}

fn detached_marker_record(
    participant_id: u64,
    binding_epoch: BindingEpoch,
    marker_delivery_seq: u64,
    accepted: bool,
) -> ValidatedMarkerRecord {
    validated_marker_record_for_test(
        CONVERSATION_ID,
        participant_id,
        FrontierBinding::Detached(binding_epoch),
        marker_delivery_seq,
        if accepted {
            marker_delivery_seq
        } else {
            marker_delivery_seq.saturating_sub(1)
        },
    )
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

fn ordinary_bound_conversation(
    closure: ClosureStateRestore,
) -> ParticipantConversationRestore<[u8; 4], [u8; 4], [u8; 4], [u8; 4]> {
    let binding_epoch = epoch(2, 8);
    let terminal = BindingTerminalOwner {
        participant_index: PARTICIPANT_ID,
        binding_epoch,
    };
    let mut identity = live_identity(PARTICIPANT_ID, generation(2), None);
    identity.cursor = 12;
    ParticipantConversationRestore {
        participants: vec![TestSnapshot::Live {
            identity,
            binding: BindingStateRestore::Bound(binding(PARTICIPANT_ID, binding_epoch)),
            binding_origin: Some(ordinary_origin(
                binding_epoch,
                AttachedRecordPosition::new(0, 1),
            )),
            detach_cell: DetachCellRestore::Empty,
        }],
        frontiers: ClaimFrontiersRestore {
            conversation_id: CONVERSATION_ID,
            active_identities: vec![FrontierParticipant::new(
                PARTICIPANT_ID,
                12,
                FrontierBinding::Bound(binding_epoch),
            )],
            identity_slot_limit: PARTICIPANT_ID + 1,
            retained_floor: 13,
            retained_record_limit: 0,
            retained_records: vec![],
            active_marker_anchors: vec![],
            historical_marker_deliveries: vec![],
            historical_causal_facts: vec![],
            sequence: SequenceClaimFrontierRestore {
                movable_claims: vec![
                    MovableSequenceClaim {
                        delivery_seq: 13,
                        owner: SequenceDirectOwner::MembershipExit {
                            participant_index: PARTICIPANT_ID,
                        },
                    },
                    MovableSequenceClaim {
                        delivery_seq: 14,
                        owner: SequenceDirectOwner::BindingTerminal(terminal),
                    },
                ],
                immutable_candidates: vec![],
                products: SequenceProductRangesRestore {
                    live_times_terminal: vec![TerminalProductRangeRestore {
                        start: 15,
                        length: 1,
                        terminal,
                    }],
                    live_times_replacement_terminal: None,
                    other_live_times_exit: vec![],
                },
                recovery: None,
            },
            order: OrderClaimFrontierRestore {
                movable_claims: vec![
                    MovableOrderClaim {
                        transaction_order: 1,
                        owner: OrderDirectOwner::ActiveBindingTerminal(terminal),
                    },
                    MovableOrderClaim {
                        transaction_order: 2,
                        owner: OrderDirectOwner::MembershipExit {
                            participant_index: PARTICIPANT_ID,
                        },
                    },
                ],
                immutable_candidates: vec![],
                recovery: None,
            },
            recovery_marker_delivery_seq: None,
        },
        sequence_ledger: SequenceLedger::try_new(
            12,
            SequenceClaims::new(1, 1, 0, RecoverySequenceReserve::None),
        )
        .expect("ordinary bound claims are exact"),
        order_ledger: OrderLedger::try_new(
            OrderHigh::Allocated(0),
            OrderClaims::new(1, 1, false, false).expect("ordinary bound order claims are exact"),
        )
        .expect("ordinary bound order ledger is exact"),
        closure,
    }
}

fn ordinary_detached_conversation(
    closure: ClosureStateRestore,
) -> ParticipantConversationRestore<[u8; 4], [u8; 4], [u8; 4], [u8; 4]> {
    let binding_epoch = epoch(2, 8);
    let terminal = BindingTerminalOwner {
        participant_index: PARTICIPANT_ID,
        binding_epoch,
    };
    let mut identity = live_identity(
        PARTICIPANT_ID,
        generation(2),
        Some(died_terminal(PARTICIPANT_ID, binding_epoch, 9, 13)),
    );
    identity.cursor = 12;
    ParticipantConversationRestore {
        participants: vec![TestSnapshot::Live {
            identity,
            binding: BindingStateRestore::Detached,
            binding_origin: Some(ordinary_origin(
                binding_epoch,
                AttachedRecordPosition::new(0, 1),
            )),
            detach_cell: DetachCellRestore::Empty,
        }],
        frontiers: ClaimFrontiersRestore {
            conversation_id: CONVERSATION_ID,
            active_identities: vec![FrontierParticipant::new(
                PARTICIPANT_ID,
                12,
                FrontierBinding::Detached(binding_epoch),
            )],
            identity_slot_limit: PARTICIPANT_ID + 1,
            retained_floor: 13,
            retained_record_limit: 1,
            retained_records: vec![RetainedCausalRecord {
                delivery_seq: 13,
                admission_order: AdmissionOrder::binding_terminal(9, PARTICIPANT_ID),
                kind: RetainedCausalRecordKind::BindingTerminal(terminal),
            }],
            active_marker_anchors: vec![],
            historical_marker_deliveries: vec![],
            historical_causal_facts: vec![],
            sequence: SequenceClaimFrontierRestore {
                movable_claims: vec![MovableSequenceClaim {
                    delivery_seq: 14,
                    owner: SequenceDirectOwner::MembershipExit {
                        participant_index: PARTICIPANT_ID,
                    },
                }],
                immutable_candidates: vec![],
                products: SequenceProductRangesRestore::default(),
                recovery: None,
            },
            order: OrderClaimFrontierRestore {
                movable_claims: vec![MovableOrderClaim {
                    transaction_order: 10,
                    owner: OrderDirectOwner::MembershipExit {
                        participant_index: PARTICIPANT_ID,
                    },
                }],
                immutable_candidates: vec![],
                recovery: None,
            },
            recovery_marker_delivery_seq: None,
        },
        sequence_ledger: SequenceLedger::try_new(
            13,
            SequenceClaims::new(1, 0, 0, RecoverySequenceReserve::None),
        )
        .expect("ordinary detached claims are exact"),
        order_ledger: OrderLedger::try_new(
            OrderHigh::Allocated(9),
            OrderClaims::new(0, 1, false, false).expect("ordinary detached order claims are exact"),
        )
        .expect("ordinary detached order ledger is exact"),
        closure,
    }
}

#[test]
fn total_restore_seals_ordinary_cursor_and_fate_authority() {
    let binding_epoch = epoch(2, 8);
    assert_eq!(
        ordinary_origin(binding_epoch, AttachedRecordPosition::new(0, 1)),
        ordinary_origin(binding_epoch, AttachedRecordPosition::new(0, 1)),
        "replaying the ordinary attach event emits the identical opaque capsule"
    );
    let authority = OrdinaryBindingAuthorityRestore {
        binding: binding(PARTICIPANT_ID, binding_epoch),
        through_seq: 12,
    };
    let continuous = ordinary_bound_conversation(owed(
        StoredEdgeRestore::ParticipantCursorProgressContinuous {
            participant_id: PARTICIPANT_ID,
            binding_epoch,
            through_seq: 12,
            authority,
        },
    ))
    .restore()
    .expect("total restore seals the exact unfenced origin");
    assert!(matches!(
        continuous.closure(),
        ClosureState::Owed {
            edge: StoredEdge::ParticipantCursorProgress(_),
            ..
        }
    ));

    let fate = OrdinaryBindingFateRestore {
        authority,
        terminal: died_terminal(PARTICIPANT_ID, binding_epoch, 9, 13),
        resulting_floor: 14,
    };
    let detached = ordinary_detached_conversation(owed(StoredEdgeRestore::DetachedCursorRelease {
        participant_id: PARTICIPANT_ID,
        last_dead_binding_epoch: binding_epoch,
        provenance: DetachedCursorReleaseProvenanceRestore::Ordinary(fate),
    }))
    .restore()
    .expect("total restore seals ordinary fate from the exact origin and terminal");
    assert!(matches!(
        detached.closure(),
        ClosureState::Owed {
            edge: StoredEdge::DetachedCursorRelease(_),
            ..
        }
    ));

    let wrong_authority = OrdinaryBindingAuthorityRestore {
        binding: binding(PARTICIPANT_ID + 1, binding_epoch),
        through_seq: 12,
    };
    assert_eq!(
        ordinary_bound_conversation(owed(
            StoredEdgeRestore::ParticipantCursorProgressContinuous {
                participant_id: PARTICIPANT_ID,
                binding_epoch,
                through_seq: 12,
                authority: wrong_authority,
            },
        ))
        .restore(),
        Err(ConversationStateRestoreError::Storage(
            StorageRestoreError::StoredEdgeProvenance
        ))
    );
}

#[test]
fn producer_emitted_enrollment_and_superseding_origins_replay_through_total_restore() {
    let enrollment_epoch = epoch(1, 7);
    let enrollment_position = AttachedRecordPosition::new(0, 1);
    let enrollment = enrollment_origin(enrollment_position);
    assert_eq!(
        enrollment,
        enrollment_origin(enrollment_position),
        "replaying the same enrollment event emits the identical opaque capsule"
    );
    let enrollment_terminal = BindingTerminalOwner {
        participant_index: PARTICIPANT_ID,
        binding_epoch: enrollment_epoch,
    };
    let enrollment_authority = OrdinaryBindingAuthorityRestore {
        binding: binding(PARTICIPANT_ID, enrollment_epoch),
        through_seq: 12,
    };
    let mut enrollment_snapshot = ordinary_bound_conversation(owed(
        StoredEdgeRestore::ParticipantCursorProgressContinuous {
            participant_id: PARTICIPANT_ID,
            binding_epoch: enrollment_epoch,
            through_seq: 12,
            authority: enrollment_authority,
        },
    ));
    let mut enrollment_identity = live_identity(PARTICIPANT_ID, generation(1), None);
    enrollment_identity.cursor = 12;
    enrollment_snapshot.participants = vec![TestSnapshot::Live {
        identity: enrollment_identity,
        binding: BindingStateRestore::Bound(binding(PARTICIPANT_ID, enrollment_epoch)),
        binding_origin: Some(enrollment),
        detach_cell: DetachCellRestore::Empty,
    }];
    enrollment_snapshot.frontiers.active_identities[0] =
        FrontierParticipant::new(PARTICIPANT_ID, 12, FrontierBinding::Bound(enrollment_epoch));
    for claim in &mut enrollment_snapshot.frontiers.sequence.movable_claims {
        if let SequenceDirectOwner::BindingTerminal(owner) = &mut claim.owner {
            *owner = enrollment_terminal;
        }
    }
    enrollment_snapshot
        .frontiers
        .sequence
        .products
        .live_times_terminal[0]
        .terminal = enrollment_terminal;
    for claim in &mut enrollment_snapshot.frontiers.order.movable_claims {
        if let OrderDirectOwner::ActiveBindingTerminal(owner) = &mut claim.owner {
            *owner = enrollment_terminal;
        }
    }
    let enrollment_restored = enrollment_snapshot
        .restore()
        .expect("producer-emitted enrollment origin replays through total restore");
    assert!(matches!(
        enrollment_restored.closure(),
        ClosureState::Owed {
            edge: StoredEdge::ParticipantCursorProgress(_),
            ..
        }
    ));

    let current_epoch = epoch(2, 8);
    let superseding_position = AttachedRecordPosition::new(0, 1);
    let (superseding, prior_terminal) = superseding_origin(current_epoch, superseding_position);
    assert_eq!(
        superseding,
        superseding_origin(current_epoch, superseding_position).0,
        "replaying the same supersession emits the identical opaque capsule"
    );
    let authority = OrdinaryBindingAuthorityRestore {
        binding: binding(PARTICIPANT_ID, current_epoch),
        through_seq: 12,
    };
    let mut superseding_snapshot = ordinary_bound_conversation(owed(
        StoredEdgeRestore::ParticipantCursorProgressContinuous {
            participant_id: PARTICIPANT_ID,
            binding_epoch: current_epoch,
            through_seq: 12,
            authority,
        },
    ));
    let mut identity = live_identity(PARTICIPANT_ID, generation(2), Some(prior_terminal));
    identity.cursor = 12;
    superseding_snapshot.participants = vec![TestSnapshot::Live {
        identity,
        binding: BindingStateRestore::Bound(binding(PARTICIPANT_ID, current_epoch)),
        binding_origin: Some(superseding),
        detach_cell: DetachCellRestore::Empty,
    }];
    let superseding_restored = superseding_snapshot
        .restore()
        .expect("producer-emitted superseding origin replays through total restore");
    assert!(matches!(
        superseding_restored.closure(),
        ClosureState::Owed {
            edge: StoredEdge::ParticipantCursorProgress(_),
            ..
        }
    ));
}

#[test]
fn historical_causal_rows_require_exact_owned_lifecycle_history() {
    let prior_epoch = epoch(2, 8);
    let current_epoch = epoch(3, 9);
    let prior_terminal = died_terminal(PARTICIPANT_ID, prior_epoch, 0, 0);
    let current_terminal = BindingTerminalOwner {
        participant_index: PARTICIPANT_ID,
        binding_epoch: current_epoch,
    };
    let mut aggregate = ordinary_bound_conversation(ClosureStateRestore::Clear);
    let mut identity = live_identity(PARTICIPANT_ID, generation(3), Some(prior_terminal));
    identity.cursor = 12;
    aggregate.participants = vec![TestSnapshot::Live {
        identity,
        binding: BindingStateRestore::Bound(binding(PARTICIPANT_ID, current_epoch)),
        binding_origin: Some(ordinary_origin(
            current_epoch,
            AttachedRecordPosition::new(1, 1),
        )),
        detach_cell: DetachCellRestore::Empty,
    }];
    aggregate.frontiers.active_identities[0] =
        FrontierParticipant::new(PARTICIPANT_ID, 12, FrontierBinding::Bound(current_epoch));
    let terminal_claim = aggregate
        .frontiers
        .sequence
        .movable_claims
        .iter_mut()
        .find_map(|claim| match &mut claim.owner {
            SequenceDirectOwner::BindingTerminal(owner) => Some(owner),
            SequenceDirectOwner::MembershipExit { .. } => None,
        })
        .expect("bound fixture carries one current terminal claim");
    *terminal_claim = current_terminal;
    aggregate.frontiers.sequence.products.live_times_terminal[0].terminal = current_terminal;
    let order_terminal = aggregate
        .frontiers
        .order
        .movable_claims
        .iter_mut()
        .find_map(|claim| match &mut claim.owner {
            OrderDirectOwner::ActiveBindingTerminal(owner) => Some(owner),
            OrderDirectOwner::MembershipExit { .. } => None,
        })
        .expect("bound fixture carries one current order terminal claim");
    *order_terminal = current_terminal;
    aggregate.frontiers.historical_causal_facts =
        vec![HistoricalCausalFactRestore::BindingTerminal {
            conversation_id: CONVERSATION_ID,
            participant_index: PARTICIPANT_ID,
            binding_epoch: prior_epoch,
            admission_order: AdmissionOrder::binding_terminal(0, PARTICIPANT_ID),
        }];

    assert!(
        matches!(
            restore_conversation_state(
                aggregate.frontiers.clone(),
                aggregate.sequence_ledger,
                aggregate.order_ledger,
                &ClosureStateRestore::Clear,
            ),
            Err(ConversationStateRestoreError::ClaimFrontier(_))
        ),
        "the same raw row has no standalone executable authority"
    );

    let restored = aggregate
        .clone()
        .restore()
        .expect("the exact terminal retained by owned membership seals its raw row");
    assert_eq!(restored.participants().len(), 1);

    let HistoricalCausalFactRestore::BindingTerminal {
        participant_index, ..
    } = &mut aggregate.frontiers.historical_causal_facts[0]
    else {
        panic!("fixture row is a binding terminal")
    };
    *participant_index += 1;
    assert!(matches!(
        aggregate.restore(),
        Err(ConversationStateRestoreError::ClaimFrontier(_))
    ));
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
    let raw_marker = owed(StoredEdgeRestore::MarkerDelivery(marker_delivery_restore(
        PARTICIPANT_ID,
        binding_epoch,
        12,
    )));
    assert_eq!(
        raw_marker.restore(),
        Err(StorageRestoreError::StoredEdgeProvenance),
        "raw marker bytes cannot restore executable authority"
    );
    assert_eq!(
        raw_marker.restore_with_marker_record(
            CONVERSATION_ID,
            detached_marker_record(PARTICIPANT_ID, binding_epoch, 12, false),
        ),
        Err(StorageRestoreError::StoredEdgeProvenance),
        "a detached retained marker cannot resurrect live delivery"
    );
    assert_eq!(
        raw_marker.restore_with_marker_record(
            CONVERSATION_ID + 1,
            bound_marker_record(PARTICIPANT_ID, binding_epoch, 12, false),
        ),
        Err(StorageRestoreError::StoredEdgeProvenance),
        "marker authority is conversation-bound"
    );
    assert_eq!(
        raw_marker.restore_with_marker_record(
            CONVERSATION_ID,
            bound_marker_record(PARTICIPANT_ID + 1, binding_epoch, 12, false),
        ),
        Err(StorageRestoreError::StoredEdgeProvenance),
        "marker authority is participant-bound"
    );
    assert!(matches!(
        raw_marker
            .restore_with_marker_record(
                CONVERSATION_ID,
                bound_marker_record(PARTICIPANT_ID, binding_epoch, 12, false),
            )
            .expect("marker delivery restores"),
        super::ClosureState::Owed {
            edge: StoredEdge::MarkerDelivery(_),
            ..
        }
    ));
    assert_eq!(
        owed(StoredEdgeRestore::ParticipantCursorProgressContinuous {
            participant_id: PARTICIPANT_ID,
            binding_epoch,
            through_seq: 12,
            authority: ordinary,
        })
        .restore(),
        Err(StorageRestoreError::StoredEdgeProvenance),
        "raw ordinary authority cannot bypass total participant restore"
    );
    assert!(matches!(
        owed(StoredEdgeRestore::ParticipantCursorProgressMarker(
            marker_progress_restore(PARTICIPANT_ID, binding_epoch, 12),
        ))
        .restore_with_marker_record(
            CONVERSATION_ID,
            bound_marker_record(PARTICIPANT_ID, binding_epoch, 12, true),
        )
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
        .restore_with_marker_record(
            CONVERSATION_ID,
            detached_marker_record(PARTICIPANT_ID, binding_epoch, 12, true),
        )
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
        .restore_with_marker_record(
            CONVERSATION_ID,
            detached_marker_record(PARTICIPANT_ID, binding_epoch, 12, false),
        )
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
    assert_eq!(
        owed(StoredEdgeRestore::DetachedCursorRelease {
            participant_id: PARTICIPANT_ID,
            last_dead_binding_epoch: binding_epoch,
            provenance: DetachedCursorReleaseProvenanceRestore::Ordinary(fate),
        })
        .restore(),
        Err(StorageRestoreError::StoredEdgeProvenance),
        "raw ordinary fate cannot bypass total participant restore"
    );

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
        .restore_with_marker_record(
            CONVERSATION_ID,
            bound_marker_record(PARTICIPANT_ID, binding_epoch, 12, true),
        ),
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

fn recovered_origin() -> BindingOrigin {
    let prior_epoch = epoch(2, 8);
    let recovered_epoch = epoch(3, 9);
    let proof = fenced_attach_restore(DebtCompletionRestore::ObserverProjection {
        debt: WideResourceVector::new(2, 16),
        through_seq: 20,
    })
    .restore(detached_marker_record(
        PARTICIPANT_ID,
        prior_epoch,
        12,
        true,
    ))
    .expect("fenced attach replay restores its typed proof");
    let prior_secret = AttachSecret::new([0x92; 32]);
    let member = LiveMember::restore(LiveMemberRestore {
        participant_id: PARTICIPANT_ID,
        conversation_id: CONVERSATION_ID,
        generation: generation(2),
        attach_secret: prior_secret,
        cursor: 12,
        enrollment_fingerprint: EnrollmentFingerprint::new([0xE1; 4]),
        latest_terminal: Some(
            died_terminal(PARTICIPANT_ID, prior_epoch, 8, 13)
                .restore()
                .expect("fenced replay terminal is typed"),
        ),
    })
    .expect("fenced attach replay member is valid");
    let verified = member
        .verify_fenced_attach(
            BindingState::Detached,
            CredentialAttachRequest {
                conversation_id: CONVERSATION_ID,
                participant_id: PARTICIPANT_ID,
                capability_generation: generation(2),
                attach_secret: prior_secret,
                attach_attempt_token: AttachAttemptToken::new([0x92; 16]),
                accept_marker_delivery_seq: Some(12),
            },
            AttachSecretProof::Verified,
            &proof,
            None,
            AttachCommitParameters {
                binding: binding(PARTICIPANT_ID, recovered_epoch),
                attach_secret: AttachSecret::new([0xA1; 32]),
                attached_position: AttachedRecordPosition::new(9, 14),
                receipt_expires_at: 100,
                provenance_expires_at: 200,
            },
        )
        .expect("fenced attach replay verifies from its proof");
    commit_attach(verified, DetachCell::<[u8; 4]>::default())
        .expect("fenced attach replay commits")
        .binding_origin()
}

fn recovered_joint_frontiers(
    current_binding: FrontierBinding,
    delivered_epoch: Option<BindingEpoch>,
) -> (ClaimFrontiersRestore, SequenceLedger, OrderLedger) {
    let prior_epoch = epoch(2, 8);
    let terminal = BindingTerminalOwner {
        participant_index: PARTICIPANT_ID,
        binding_epoch: prior_epoch,
    };
    let (sequence_claims, sequence_movable, products, order_claims, order_movable) =
        match current_binding {
            FrontierBinding::Bound(current_epoch) => (
                SequenceClaims::new(1, 1, 0, RecoverySequenceReserve::None),
                vec![
                    MovableSequenceClaim {
                        delivery_seq: 15,
                        owner: SequenceDirectOwner::MembershipExit {
                            participant_index: PARTICIPANT_ID,
                        },
                    },
                    MovableSequenceClaim {
                        delivery_seq: 16,
                        owner: SequenceDirectOwner::BindingTerminal(BindingTerminalOwner {
                            participant_index: PARTICIPANT_ID,
                            binding_epoch: current_epoch,
                        }),
                    },
                ],
                SequenceProductRangesRestore {
                    live_times_terminal: vec![TerminalProductRangeRestore {
                        start: 17,
                        length: 1,
                        terminal: BindingTerminalOwner {
                            participant_index: PARTICIPANT_ID,
                            binding_epoch: current_epoch,
                        },
                    }],
                    live_times_replacement_terminal: None,
                    other_live_times_exit: vec![],
                },
                OrderClaims::new(1, 1, false, false)
                    .expect("bound joint fixture has no torn recovery pair"),
                vec![
                    MovableOrderClaim {
                        transaction_order: 10,
                        owner: OrderDirectOwner::ActiveBindingTerminal(BindingTerminalOwner {
                            participant_index: PARTICIPANT_ID,
                            binding_epoch: current_epoch,
                        }),
                    },
                    MovableOrderClaim {
                        transaction_order: 11,
                        owner: OrderDirectOwner::MembershipExit {
                            participant_index: PARTICIPANT_ID,
                        },
                    },
                ],
            ),
            FrontierBinding::Detached(_) => (
                SequenceClaims::new(1, 0, 0, RecoverySequenceReserve::None),
                vec![MovableSequenceClaim {
                    delivery_seq: 16,
                    owner: SequenceDirectOwner::MembershipExit {
                        participant_index: PARTICIPANT_ID,
                    },
                }],
                SequenceProductRangesRestore::default(),
                OrderClaims::new(0, 1, false, false)
                    .expect("detached joint fixture has no torn recovery pair"),
                vec![MovableOrderClaim {
                    transaction_order: 11,
                    owner: OrderDirectOwner::MembershipExit {
                        participant_index: PARTICIPANT_ID,
                    },
                }],
            ),
        };
    let historical_marker_deliveries = delivered_epoch.map_or_else(Vec::new, |binding_epoch| {
        vec![HistoricalMarkerDeliveryFactRestore {
            conversation_id: CONVERSATION_ID,
            participant_index: PARTICIPANT_ID,
            marker_delivery_seq: 12,
            delivered_binding_epoch: binding_epoch,
        }]
    });
    let restore = ClaimFrontiersRestore {
        conversation_id: CONVERSATION_ID,
        active_identities: vec![FrontierParticipant::new(
            PARTICIPANT_ID,
            12,
            current_binding,
        )],
        identity_slot_limit: PARTICIPANT_ID + 1,
        retained_floor: 12,
        retained_record_limit: if matches!(current_binding, FrontierBinding::Bound(_)) {
            3
        } else {
            4
        },
        retained_records: {
            let current_epoch = match current_binding {
                FrontierBinding::Bound(epoch) | FrontierBinding::Detached(epoch) => epoch,
            };
            let mut records = vec![
                RetainedCausalRecord {
                    delivery_seq: 12,
                    admission_order: AdmissionOrder::new(
                        6,
                        CandidatePhase::CompactionMarker,
                        PARTICIPANT_ID,
                    ),
                    kind: RetainedCausalRecordKind::CompactionMarker {
                        participant_index: PARTICIPANT_ID,
                        provenance: super::MarkerProvenance::NonProductM,
                    },
                },
                RetainedCausalRecord {
                    delivery_seq: 13,
                    admission_order: AdmissionOrder::new(
                        8,
                        CandidatePhase::BindingTerminal,
                        PARTICIPANT_ID,
                    ),
                    kind: RetainedCausalRecordKind::BindingTerminal(terminal),
                },
                RetainedCausalRecord {
                    delivery_seq: 14,
                    admission_order: AdmissionOrder::new(
                        9,
                        CandidatePhase::AttachLifecycle,
                        PARTICIPANT_ID,
                    ),
                    kind: RetainedCausalRecordKind::AttachLifecycle {
                        participant_index: PARTICIPANT_ID,
                        binding_epoch: current_epoch,
                    },
                },
            ];
            if matches!(current_binding, FrontierBinding::Detached(_)) {
                records.push(RetainedCausalRecord {
                    delivery_seq: 15,
                    admission_order: AdmissionOrder::new(
                        10,
                        CandidatePhase::BindingTerminal,
                        PARTICIPANT_ID,
                    ),
                    kind: RetainedCausalRecordKind::BindingTerminal(BindingTerminalOwner {
                        participant_index: PARTICIPANT_ID,
                        binding_epoch: current_epoch,
                    }),
                });
            }
            records
        },
        active_marker_anchors: vec![],
        historical_marker_deliveries,
        historical_causal_facts: vec![],
        sequence: SequenceClaimFrontierRestore {
            movable_claims: sequence_movable,
            immutable_candidates: vec![],
            products,
            recovery: None,
        },
        order: OrderClaimFrontierRestore {
            movable_claims: order_movable,
            immutable_candidates: vec![],
            recovery: None,
        },
        recovery_marker_delivery_seq: None,
    };
    (
        restore,
        SequenceLedger::try_new(
            if matches!(current_binding, FrontierBinding::Bound(_)) {
                14
            } else {
                15
            },
            sequence_claims,
        )
        .expect("joint sequence claims fit after retained lifecycle history"),
        OrderLedger::try_new(
            OrderHigh::Allocated(if matches!(current_binding, FrontierBinding::Bound(_)) {
                9
            } else {
                10
            }),
            order_claims,
        )
        .expect("joint order claims fit after retained lifecycle history"),
    )
}

fn recovered_participant(current_binding: FrontierBinding) -> TestSnapshot {
    let prior_epoch = epoch(2, 8);
    let recovered_epoch = match current_binding {
        FrontierBinding::Bound(epoch) | FrontierBinding::Detached(epoch) => epoch,
    };
    let latest_terminal = match current_binding {
        FrontierBinding::Bound(_) => died_terminal(PARTICIPANT_ID, prior_epoch, 8, 13),
        FrontierBinding::Detached(_) => died_terminal(PARTICIPANT_ID, recovered_epoch, 10, 15),
    };
    let binding_state = match current_binding {
        FrontierBinding::Bound(_) => {
            BindingStateRestore::Bound(binding(PARTICIPANT_ID, recovered_epoch))
        }
        FrontierBinding::Detached(_) => BindingStateRestore::Detached,
    };
    let mut identity = live_identity(
        PARTICIPANT_ID,
        recovered_epoch.capability_generation,
        Some(latest_terminal),
    );
    identity.cursor = 12;
    TestSnapshot::Live {
        identity,
        binding: binding_state,
        binding_origin: Some(recovered_origin()),
        detach_cell: DetachCellRestore::Empty,
    }
}

fn recovered_conversation(
    current_binding: FrontierBinding,
    delivered_epoch: Option<BindingEpoch>,
    closure: ClosureStateRestore,
) -> ParticipantConversationRestore<[u8; 4], [u8; 4], [u8; 4], [u8; 4]> {
    let (frontiers, sequence_ledger, order_ledger) =
        recovered_joint_frontiers(current_binding, delivered_epoch);
    ParticipantConversationRestore {
        participants: vec![recovered_participant(current_binding)],
        frontiers,
        sequence_ledger,
        order_ledger,
        closure,
    }
}

fn recovered_bound_with_active_quartet(
    delivered_epoch: Option<BindingEpoch>,
    closure: ClosureStateRestore,
) -> ParticipantConversationRestore<[u8; 4], [u8; 4], [u8; 4], [u8; 4]> {
    let recovered_epoch = epoch(3, 9);
    let terminal = BindingTerminalOwner {
        participant_index: PARTICIPANT_ID,
        binding_epoch: recovered_epoch,
    };
    let (mut frontiers, _, _) =
        recovered_joint_frontiers(FrontierBinding::Bound(recovered_epoch), delivered_epoch);
    frontiers.sequence = SequenceClaimFrontierRestore {
        movable_claims: vec![
            MovableSequenceClaim {
                delivery_seq: 15,
                owner: SequenceDirectOwner::BindingTerminal(terminal),
            },
            MovableSequenceClaim {
                delivery_seq: 20,
                owner: SequenceDirectOwner::MembershipExit {
                    participant_index: PARTICIPANT_ID,
                },
            },
        ],
        immutable_candidates: vec![],
        products: SequenceProductRangesRestore {
            live_times_terminal: vec![TerminalProductRangeRestore {
                start: 18,
                length: 1,
                terminal,
            }],
            live_times_replacement_terminal: Some(ReplacementTerminalProductRangeRestore {
                start: 19,
                length: 1,
            }),
            other_live_times_exit: vec![],
        },
        recovery: Some(RecoverySequenceBlockRestore {
            terminal: None,
            recovery_attach_seq: 16,
            replacement_terminal_seq: 17,
        }),
    };
    frontiers.order = OrderClaimFrontierRestore {
        movable_claims: vec![
            MovableOrderClaim {
                transaction_order: 10,
                owner: OrderDirectOwner::ActiveBindingTerminal(terminal),
            },
            MovableOrderClaim {
                transaction_order: 13,
                owner: OrderDirectOwner::MembershipExit {
                    participant_index: PARTICIPANT_ID,
                },
            },
        ],
        immutable_candidates: vec![],
        recovery: Some(RecoveryOrderBlockRestore {
            active_binding: None,
            recovery_operation_order: 11,
            replacement_terminal_order: 12,
        }),
    };
    frontiers.recovery_marker_delivery_seq = Some(12);
    ParticipantConversationRestore {
        participants: vec![recovered_participant(FrontierBinding::Bound(
            recovered_epoch,
        ))],
        frontiers,
        sequence_ledger: SequenceLedger::try_new(
            14,
            SequenceClaims::new(1, 1, 0, RecoverySequenceReserve::DetachedCredentialRecovery),
        )
        .expect("post-fenced active recovery quartet is exact"),
        order_ledger: OrderLedger::try_new(
            OrderHigh::Allocated(9),
            OrderClaims::new(1, 1, true, true).expect("post-fenced active order quartet is paired"),
        )
        .expect("post-fenced active order frontier is exact"),
        closure,
    }
}

fn prefate_candidate_joint_frontiers() -> (ClaimFrontiersRestore, SequenceLedger, OrderLedger) {
    let binding_epoch = epoch(2, 8);
    let terminal = BindingTerminalOwner {
        participant_index: PARTICIPANT_ID,
        binding_epoch,
    };
    let marker_order = AdmissionOrder::new(0, CandidatePhase::CompactionMarker, PARTICIPANT_ID);
    let restore = ClaimFrontiersRestore {
        conversation_id: CONVERSATION_ID,
        active_identities: vec![FrontierParticipant::new(
            PARTICIPANT_ID,
            0,
            FrontierBinding::Bound(binding_epoch),
        )],
        identity_slot_limit: PARTICIPANT_ID + 1,
        retained_floor: 1,
        retained_record_limit: 0,
        retained_records: vec![],
        active_marker_anchors: vec![],
        historical_marker_deliveries: vec![],
        historical_causal_facts: vec![],
        sequence: SequenceClaimFrontierRestore {
            movable_claims: vec![MovableSequenceClaim {
                delivery_seq: 5,
                owner: SequenceDirectOwner::MembershipExit {
                    participant_index: PARTICIPANT_ID,
                },
            }],
            immutable_candidates: vec![ImmutableSequenceCandidate::Marker(
                MarkerCandidateAuthority {
                    delivery_seq: 1,
                    admission_order: marker_order,
                    target_binding: FrontierBinding::Bound(binding_epoch),
                    provenance: MarkerProvenance::NonProductM,
                    current_owner: MarkerSequenceOwner::Marker,
                },
            )],
            products: SequenceProductRangesRestore {
                live_times_terminal: vec![TerminalProductRangeRestore {
                    start: 6,
                    length: 1,
                    terminal,
                }],
                live_times_replacement_terminal: Some(ReplacementTerminalProductRangeRestore {
                    start: 7,
                    length: 1,
                }),
                other_live_times_exit: vec![],
            },
            recovery: Some(RecoverySequenceBlockRestore {
                terminal: Some(RecoverySequenceTerminalRestore {
                    delivery_seq: 2,
                    owner: terminal,
                }),
                recovery_attach_seq: 3,
                replacement_terminal_seq: 4,
            }),
        },
        order: OrderClaimFrontierRestore {
            movable_claims: vec![MovableOrderClaim {
                transaction_order: 4,
                owner: OrderDirectOwner::MembershipExit {
                    participant_index: PARTICIPANT_ID,
                },
            }],
            immutable_candidates: vec![ImmutableOrderCandidateMajorRestore {
                transaction_order: 0,
                candidate_keys: vec![marker_order],
            }],
            recovery: Some(RecoveryOrderBlockRestore {
                active_binding: Some(RecoveryOrderActiveBindingRestore {
                    transaction_order: 1,
                    owner: terminal,
                }),
                recovery_operation_order: 2,
                replacement_terminal_order: 3,
            }),
        },
        recovery_marker_delivery_seq: Some(1),
    };
    (
        restore,
        SequenceLedger::try_new(
            0,
            SequenceClaims::new(1, 1, 1, RecoverySequenceReserve::DetachedCredentialRecovery),
        )
        .expect("pre-fate recovery sequence reserve is exact"),
        OrderLedger::try_new(
            OrderHigh::Allocated(0),
            OrderClaims::new(1, 1, true, true).expect("pre-fate recovery order pair is exact"),
        )
        .expect("pre-fate recovery order reserve is exact"),
    )
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
    let commit = fenced
        .restore(detached_marker_record(
            PARTICIPANT_ID,
            epoch(2, 8),
            12,
            true,
        ))
        .expect("exact DCR fenced attach restores");
    assert_eq!(commit.participant_id(), PARTICIPANT_ID);
    assert_eq!(commit.prior_binding_epoch(), epoch(2, 8));
    assert_eq!(commit.new_binding_epoch(), epoch(3, 9));

    let mut wrong_participant = fenced;
    wrong_participant.participant_id += 1;
    assert_eq!(
        wrong_participant.restore(detached_marker_record(
            PARTICIPANT_ID,
            epoch(2, 8),
            12,
            true,
        )),
        Err(StorageRestoreError::StoredEdgeProvenance)
    );
    let mut wrong_epoch = fenced;
    wrong_epoch.new_binding_epoch = epoch(4, 9);
    assert_eq!(
        wrong_epoch.restore(detached_marker_record(
            PARTICIPANT_ID,
            epoch(2, 8),
            12,
            true,
        )),
        Err(StorageRestoreError::StoredEdgeProvenance)
    );

    let fate = RecoveredBindingFateRestore {
        fenced_attach: fenced,
        participant_id: PARTICIPANT_ID,
        binding_epoch: epoch(3, 9),
        resulting_floor: 1,
    };
    let restored_fate = fate
        .restore(detached_marker_record(
            PARTICIPANT_ID,
            epoch(2, 8),
            12,
            true,
        ))
        .expect("fate must name the exact recovered epoch");
    assert_eq!(restored_fate.participant_id(), PARTICIPANT_ID);
    assert_eq!(restored_fate.last_dead_binding_epoch(), epoch(3, 9));

    let pending = PendingRecoveredCursorReleaseRestore {
        fate,
        resulting_debt: after_fate_debt,
    };
    let restored_pending = pending
        .restore(detached_marker_record(
            PARTICIPANT_ID,
            epoch(2, 8),
            12,
            true,
        ))
        .expect("OP remains current with a latent recovered cursor release");
    assert_eq!(restored_pending.participant_id(), PARTICIPANT_ID);
    assert_eq!(restored_pending.last_dead_binding_epoch(), epoch(3, 9));

    let mut wrong_fate = fate;
    wrong_fate.binding_epoch = epoch(3, 10);
    assert_eq!(
        wrong_fate.restore(detached_marker_record(
            PARTICIPANT_ID,
            epoch(2, 8),
            12,
            true,
        )),
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
    .restore_with_marker_record(
        CONVERSATION_ID,
        detached_marker_record(PARTICIPANT_ID, epoch(2, 8), 12, true),
    )
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
    .restore_with_marker_record(
        CONVERSATION_ID,
        detached_marker_record(PARTICIPANT_ID, epoch(2, 8), 12, true),
    )
    .expect("covering recovered fate installs DCursor directly");
    assert!(matches!(
        direct,
        super::ClosureState::Owed {
            edge: StoredEdge::DetachedCursorRelease(_),
            ..
        }
    ));
}

#[test]
fn joint_cold_restore_uses_historical_old_epoch_across_fenced_recovery() {
    let prior_epoch = epoch(2, 8);
    let recovered_epoch = epoch(3, 9);
    let after_attach_debt = WideResourceVector::new(2, 16);
    let after_fate_debt = WideResourceVector::new(1, 8);
    let final_debt = WideResourceVector::new(1, 4);
    let fenced_origin = recovered_origin();
    assert_eq!(
        fenced_origin,
        recovered_origin(),
        "replaying the fenced attach event emits the identical opaque capsule"
    );
    assert_eq!(fenced_origin.recovered_marker(), Some((12, prior_epoch)));

    let fenced = fenced_attach_restore(DebtCompletionRestore::ObserverProjection {
        debt: after_attach_debt,
        through_seq: 20,
    });
    let commit = fenced
        .restore(detached_marker_record(
            PARTICIPANT_ID,
            prior_epoch,
            12,
            true,
        ))
        .expect("old detached marker proves the fenced attach");
    assert_eq!(commit.new_binding_epoch(), recovered_epoch);

    let bound_snapshot = recovered_conversation(
        FrontierBinding::Bound(recovered_epoch),
        Some(prior_epoch),
        ClosureStateRestore::Owed {
            debt: after_attach_debt,
            edge: StoredEdgeRestore::ObserverProjection { through_seq: 14 },
        },
    )
    .restore()
    .expect("post-fenced Bound(new) is sealed by exact old marker delivery history");
    assert!(matches!(
        bound_snapshot.closure(),
        ClosureState::Owed {
            edge: StoredEdge::ObserverProjection(_),
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
        binding_epoch: recovered_epoch,
        resulting_floor: 21,
    };
    let direct_closure = ClosureStateRestore::Owed {
        debt: final_debt,
        edge: StoredEdgeRestore::DetachedCursorRelease {
            participant_id: PARTICIPANT_ID,
            last_dead_binding_epoch: recovered_epoch,
            provenance: DetachedCursorReleaseProvenanceRestore::RecoveredDirect {
                fate: direct_fate,
                resulting_debt: final_debt,
            },
        },
    };
    let restore_direct = || {
        recovered_conversation(
            FrontierBinding::Detached(recovered_epoch),
            Some(prior_epoch),
            direct_closure,
        )
        .restore()
    };
    let direct = restore_direct().expect("historical old epoch restores direct recovered DCursor");
    assert!(matches!(
        direct.closure(),
        ClosureState::Owed {
            edge: StoredEdge::DetachedCursorRelease(_),
            ..
        }
    ));
    assert_eq!(restore_direct(), Ok(direct), "cold replay is deterministic");

    let pending = PendingRecoveredCursorReleaseRestore {
        fate: RecoveredBindingFateRestore {
            fenced_attach: fenced,
            participant_id: PARTICIPANT_ID,
            binding_epoch: recovered_epoch,
            resulting_floor: 1,
        },
        resulting_debt: after_fate_debt,
    };
    let after_storage_closure = ClosureStateRestore::Owed {
        debt: final_debt,
        edge: StoredEdgeRestore::DetachedCursorRelease {
            participant_id: PARTICIPANT_ID,
            last_dead_binding_epoch: recovered_epoch,
            provenance: DetachedCursorReleaseProvenanceRestore::RecoveredAfterStorage {
                pending,
                completion: RecoveredStorageCompletionRestore::ObserverProjection {
                    through_seq: 20,
                    resulting_debt: Some(final_debt),
                },
            },
        },
    };
    let restore_after_storage = || {
        recovered_conversation(
            FrontierBinding::Detached(recovered_epoch),
            Some(prior_epoch),
            after_storage_closure,
        )
        .restore()
    };
    let after_storage = restore_after_storage()
        .expect("historical old epoch restores post-storage recovered DCursor");
    assert!(matches!(
        after_storage.closure(),
        ClosureState::Owed {
            edge: StoredEdge::DetachedCursorRelease(_),
            ..
        }
    ));
    assert_eq!(
        restore_after_storage(),
        Ok(after_storage),
        "post-storage cold replay is deterministic"
    );

    assert!(
        matches!(
            recovered_conversation(
                FrontierBinding::Detached(recovered_epoch),
                None,
                direct_closure,
            )
            .restore(),
            Err(ConversationStateRestoreError::ClaimFrontier(_))
        ),
        "current Detached(new) cannot stand in for old delivery authority"
    );

    assert!(matches!(
        recovered_conversation(
            FrontierBinding::Detached(recovered_epoch),
            Some(epoch(4, 10)),
            direct_closure,
        )
        .restore(),
        Err(ConversationStateRestoreError::ClaimFrontier(_))
    ));

    let bound_closure = ClosureStateRestore::Owed {
        debt: after_attach_debt,
        edge: StoredEdgeRestore::ObserverProjection { through_seq: 14 },
    };
    assert!(matches!(
        recovered_conversation(FrontierBinding::Bound(recovered_epoch), None, bound_closure,)
            .restore(),
        Err(ConversationStateRestoreError::ClaimFrontier(_))
    ));
    assert!(matches!(
        recovered_conversation(
            FrontierBinding::Bound(recovered_epoch),
            Some(epoch(4, 10)),
            bound_closure,
        )
        .restore(),
        Err(ConversationStateRestoreError::ClaimFrontier(_))
    ));
}

#[test]
fn recovered_origin_cannot_execute_ordinary_pcp_or_cursor_release() {
    let prior_epoch = epoch(2, 8);
    let recovered_epoch = epoch(3, 9);
    let authority = OrdinaryBindingAuthorityRestore {
        binding: binding(PARTICIPANT_ID, recovered_epoch),
        through_seq: 12,
    };
    let pcp = ClosureStateRestore::Owed {
        debt: WideResourceVector::new(2, 16),
        edge: StoredEdgeRestore::ParticipantCursorProgressContinuous {
            participant_id: PARTICIPANT_ID,
            binding_epoch: recovered_epoch,
            through_seq: 12,
            authority,
        },
    };
    assert_eq!(
        recovered_conversation(
            FrontierBinding::Bound(recovered_epoch),
            Some(prior_epoch),
            pcp,
        )
        .restore(),
        Err(ConversationStateRestoreError::Storage(
            StorageRestoreError::StoredEdgeProvenance
        )),
        "the fenced producer capsule cannot be treated as ordinary PCP authority"
    );
    assert!(
        matches!(
            recovered_conversation(FrontierBinding::Bound(recovered_epoch), None, pcp).restore(),
            Err(ConversationStateRestoreError::ClaimFrontier(_))
        ),
        "deleting old marker history cannot relabel the recovered capsule"
    );

    let ordinary_fate = OrdinaryBindingFateRestore {
        authority,
        terminal: died_terminal(PARTICIPANT_ID, recovered_epoch, 10, 15),
        resulting_floor: 16,
    };
    let cursor_release = ClosureStateRestore::Owed {
        debt: WideResourceVector::new(1, 4),
        edge: StoredEdgeRestore::DetachedCursorRelease {
            participant_id: PARTICIPANT_ID,
            last_dead_binding_epoch: recovered_epoch,
            provenance: DetachedCursorReleaseProvenanceRestore::Ordinary(ordinary_fate),
        },
    };
    assert_eq!(
        recovered_conversation(
            FrontierBinding::Detached(recovered_epoch),
            Some(prior_epoch),
            cursor_release,
        )
        .restore(),
        Err(ConversationStateRestoreError::Storage(
            StorageRestoreError::StoredEdgeProvenance
        )),
        "the fenced producer capsule cannot be treated as ordinary DCursor authority"
    );
    assert!(
        matches!(
            recovered_conversation(
                FrontierBinding::Detached(recovered_epoch),
                None,
                cursor_release,
            )
            .restore(),
            Err(ConversationStateRestoreError::ClaimFrontier(_))
        ),
        "history deletion cannot turn recovered DCursor provenance into ordinary fate"
    );
}

#[test]
fn post_fenced_active_quartet_resolves_from_old_history_under_current_storage() {
    let prior_epoch = epoch(2, 8);
    let recovered_epoch = epoch(3, 9);
    let closures = [
        ClosureStateRestore::Owed {
            debt: WideResourceVector::new(2, 16),
            edge: StoredEdgeRestore::ObserverProjection { through_seq: 14 },
        },
        ClosureStateRestore::Owed {
            debt: WideResourceVector::new(2, 16),
            edge: StoredEdgeRestore::PhysicalCompaction {
                from_floor: 12,
                through_seq: 14,
            },
        },
    ];
    for closure in closures {
        let restored = recovered_bound_with_active_quartet(Some(prior_epoch), closure)
            .restore()
            .expect("old marker history resolves RecoveredBound independently of OP or PC");
        let recovery = restored
            .frontiers()
            .sequence()
            .recovery()
            .expect("the active quartet remains present");
        assert_eq!(recovery.participant_index(), PARTICIPANT_ID);
        assert_eq!(recovery.marker_delivery_seq(), 12);
        assert_eq!(recovery.recovered_binding_epoch(), prior_epoch);
        assert_eq!(
            restored.frontiers().active_identities().participants()[0].binding(),
            FrontierBinding::Bound(recovered_epoch)
        );
    }

    let closure = ClosureStateRestore::Owed {
        debt: WideResourceVector::new(2, 16),
        edge: StoredEdgeRestore::ObserverProjection { through_seq: 14 },
    };
    assert!(matches!(
        recovered_bound_with_active_quartet(None, closure).restore(),
        Err(ConversationStateRestoreError::ClaimFrontier(_))
    ));
    assert!(matches!(
        recovered_bound_with_active_quartet(Some(epoch(4, 10)), closure).restore(),
        Err(ConversationStateRestoreError::ClaimFrontier(_))
    ));
}

#[test]
fn case_56_joint_restore_keeps_prefate_candidate_quartet_while_pc_is_current() {
    let (frontiers, sequence, order) = prefate_candidate_joint_frontiers();
    let closure = ClosureStateRestore::Owed {
        debt: WideResourceVector::new(1, 1),
        edge: StoredEdgeRestore::PhysicalCompaction {
            from_floor: 0,
            through_seq: 0,
        },
    };
    let restored = restore_conversation_state(frontiers, sequence, order, &closure)
        .expect("candidate recovery authority is independent of current PC");

    assert!(matches!(
        restored.closure(),
        ClosureState::Owed {
            edge: StoredEdge::PhysicalCompaction(_),
            ..
        }
    ));
    let recovery = restored
        .frontiers()
        .sequence()
        .recovery()
        .expect("pre-endowed recovery quartet survives joint cold restore");
    assert_eq!(recovery.participant_index(), PARTICIPANT_ID);
    assert_eq!(recovery.marker_delivery_seq(), 1);
    assert_eq!(recovery.recovered_binding_epoch(), epoch(2, 8));
}
