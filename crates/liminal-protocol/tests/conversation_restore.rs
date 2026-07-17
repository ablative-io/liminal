//! Public-surface whole-conversation cold-restore coverage.
//!
//! These tests exercise `ParticipantConversationRestore::restore` from outside
//! the crate, proving the composition surface is public while the
//! sealed-authority door stays shut: no test here can mint a binding-origin
//! capsule, a marker-record authority, or a validated conversation state from
//! raw values.

#![allow(clippy::expect_used, clippy::panic)]

use liminal_protocol::lifecycle::{
    ActiveBinding, BindingStateRestore, ClaimFrontiersRestore, ClosureState, ClosureStateRestore,
    ConversationStateRestoreError, DetachCellRestore, EnrollmentFingerprint, FrontierBinding,
    FrontierParticipant, IdentityState, LeaveCommittedRestore, LeaveFingerprint,
    LiveIdentityRestore, OrderClaimFrontierRestore, OrderClaims, OrderHigh, OrderLedger,
    ParticipantConversationRestore, ParticipantLifecycleRestore, RecoverySequenceReserve,
    RestoredParticipantLifecycle, RetiredIdentityRestore, SequenceClaimFrontierRestore,
    SequenceClaims, SequenceLedger, SequenceProductRangesRestore, StorageRestoreError,
};
use liminal_protocol::wire::{
    AttachSecret, BindingEpoch, ConnectionIncarnation, Generation, LeaveAttemptToken,
};

type Snapshot = ParticipantConversationRestore<[u8; 4], [u8; 4], [u8; 4], [u8; 4]>;
type SnapshotParticipant = ParticipantLifecycleRestore<[u8; 4], [u8; 4], [u8; 4], [u8; 4]>;

const CONVERSATION_ID: u64 = 57;
const PARTICIPANT_ID: u64 = 3;

const fn generation(value: u64) -> Generation {
    Generation::new(value).expect("test generations are nonzero")
}

const fn epoch(generation_value: u64, connection_ordinal: u64) -> BindingEpoch {
    BindingEpoch::new(
        ConnectionIncarnation::new(5, connection_ordinal),
        generation(generation_value),
    )
}

const fn retired_participant(participant_id: u64) -> SnapshotParticipant {
    ParticipantLifecycleRestore::Retired(RetiredIdentityRestore {
        participant_id,
        conversation_id: CONVERSATION_ID,
        retired_generation: generation(4),
        enrollment_fingerprint: EnrollmentFingerprint::new([0xE1; 4]),
        leave_attempt_token: LeaveAttemptToken::new([0xB1; 16]),
        leave_request_verifier: [0xA2; 4],
        leave_fingerprint: LeaveFingerprint::new([0xF2; 4]),
        left_transaction_order: 17,
        committed_result: LeaveCommittedRestore {
            conversation_id: CONVERSATION_ID,
            leave_attempt_token: LeaveAttemptToken::new([0xB1; 16]),
            participant_id,
            retired_generation: generation(4),
            ended_binding_epoch: Some(epoch(4, 12)),
            prior_terminal_delivery_seq: Some(30),
            left_delivery_seq: 31,
        },
    })
}

fn empty_frontiers(active_identities: Vec<FrontierParticipant>) -> ClaimFrontiersRestore {
    ClaimFrontiersRestore {
        conversation_id: CONVERSATION_ID,
        active_identities,
        identity_slot_limit: PARTICIPANT_ID + 1,
        retained_floor: 32,
        retained_record_limit: 0,
        retained_records: vec![],
        active_marker_anchors: vec![],
        historical_marker_deliveries: vec![],
        historical_causal_facts: vec![],
        sequence: SequenceClaimFrontierRestore {
            movable_claims: vec![],
            immutable_candidates: vec![],
            products: SequenceProductRangesRestore::default(),
            recovery: None,
        },
        order: OrderClaimFrontierRestore {
            movable_claims: vec![],
            immutable_candidates: vec![],
            recovery: None,
        },
        recovery_marker_delivery_seq: None,
    }
}

fn retired_only_snapshot(participants: Vec<SnapshotParticipant>) -> Snapshot {
    ParticipantConversationRestore {
        participants,
        frontiers: empty_frontiers(vec![]),
        sequence_ledger: SequenceLedger::try_new(
            31,
            SequenceClaims::new(0, 0, 0, RecoverySequenceReserve::None),
        )
        .expect("a settled conversation holds no sequence claims"),
        order_ledger: OrderLedger::try_new(
            OrderHigh::Allocated(17),
            OrderClaims::new(0, 0, false, false)
                .expect("a settled conversation holds no order claims"),
        )
        .expect("a settled conversation's order ledger is exact"),
        closure: ClosureStateRestore::Clear,
    }
}

#[test]
fn public_total_restore_validates_a_settled_conversation() {
    let restored = retired_only_snapshot(vec![retired_participant(PARTICIPANT_ID)])
        .restore()
        .expect("a settled tombstone-only conversation restores publicly");

    assert_eq!(restored.participants().len(), 1);
    let RestoredParticipantLifecycle::Retired(retired) = &restored.participants()[0] else {
        panic!("a tombstone capsule must restore as a retired identity");
    };
    assert_eq!(retired.participant_id(), PARTICIPANT_ID);
    assert_eq!(retired.retired_generation(), generation(4));
    assert_eq!(restored.frontiers().conversation_id(), CONVERSATION_ID);
    assert_eq!(restored.closure(), ClosureState::Clear);

    let (participants, frontiers, closure) =
        retired_only_snapshot(vec![retired_participant(PARTICIPANT_ID)])
            .restore()
            .expect("the identical snapshot restores deterministically")
            .into_parts();
    assert_eq!(frontiers.conversation_id(), CONVERSATION_ID);
    assert_eq!(closure, ClosureState::Clear);
    let mut participants = participants;
    let participant = participants.remove(0);
    let (identity, binding, detach_cell) = participant.into_parts();
    assert!(matches!(identity, IdentityState::Retired(_)));
    assert_eq!(binding, None);
    assert_eq!(detach_cell, None);
}

#[test]
fn public_total_restore_refuses_duplicate_participants() {
    assert_eq!(
        retired_only_snapshot(vec![
            retired_participant(PARTICIPANT_ID),
            retired_participant(PARTICIPANT_ID),
        ])
        .restore(),
        Err(ConversationStateRestoreError::Storage(
            StorageRestoreError::MembershipInvariant
        )),
        "one permanent identity must not appear twice in a snapshot"
    );
}

#[test]
fn public_total_restore_refuses_frontier_participants_without_capsules() {
    let mut snapshot = retired_only_snapshot(vec![retired_participant(PARTICIPANT_ID)]);
    snapshot.frontiers.active_identities = vec![FrontierParticipant::new(
        PARTICIPANT_ID,
        12,
        FrontierBinding::Bound(epoch(4, 12)),
    )];
    assert_eq!(
        snapshot.restore(),
        Err(ConversationStateRestoreError::Storage(
            StorageRestoreError::MembershipInvariant
        )),
        "a frontier row without its participant capsule must not combine"
    );
}

#[test]
fn public_total_restore_refuses_cross_conversation_tombstones() {
    let mut snapshot = retired_only_snapshot(vec![retired_participant(PARTICIPANT_ID)]);
    snapshot.frontiers.conversation_id = CONVERSATION_ID + 1;
    assert_eq!(
        snapshot.restore(),
        Err(ConversationStateRestoreError::Storage(
            StorageRestoreError::MembershipInvariant
        )),
        "tombstones persisted for another conversation must not combine"
    );
}

#[test]
fn public_restore_cannot_conjure_binding_authority_without_a_producer_origin() {
    // A bound participant requires the producer-emitted binding-origin
    // capsule. No public constructor mints one, so a caller-authored live
    // snapshot cannot become executable binding authority.
    let bound_epoch = epoch(2, 9);
    let mut snapshot = retired_only_snapshot(vec![ParticipantLifecycleRestore::Live {
        identity: LiveIdentityRestore {
            participant_id: PARTICIPANT_ID,
            conversation_id: CONVERSATION_ID,
            generation: generation(2),
            attach_secret: AttachSecret::new([0xA1; 32]),
            cursor: 12,
            enrollment_fingerprint: EnrollmentFingerprint::new([0xE1; 4]),
            latest_terminal: None,
        },
        binding: BindingStateRestore::Bound(ActiveBinding {
            participant_id: PARTICIPANT_ID,
            conversation_id: CONVERSATION_ID,
            binding_epoch: bound_epoch,
        }),
        binding_origin: None,
        detach_cell: DetachCellRestore::Empty,
    }]);
    snapshot.frontiers.active_identities = vec![FrontierParticipant::new(
        PARTICIPANT_ID,
        12,
        FrontierBinding::Bound(bound_epoch),
    )];
    assert_eq!(
        snapshot.restore(),
        Err(ConversationStateRestoreError::Storage(
            StorageRestoreError::BindingAuthority
        )),
        "the phase-A seal holds: no origin capsule, no executable binding"
    );
}
