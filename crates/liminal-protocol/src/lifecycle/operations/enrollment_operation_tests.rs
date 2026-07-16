#![allow(clippy::expect_used, clippy::panic)]

use alloc::{vec, vec::Vec};
use core::cell::Cell;

use crate::algebra::{ResourceVector, WideResourceVector};
use crate::wire::{
    AttachSecret, BindingEpoch, ConnectionIncarnation, EnrollmentRequest, EnrollmentToken,
    Generation, ServerValue,
};

use super::super::{
    AllocatedParticipantSlot, BindingSlotOccupancy, BindingState, CapacityCounter,
    ClosureAccounting, ClosureState, ConnectionConversationTracking, EnrollmentCapacityCounters,
    EnrollmentFingerprint, EnrollmentLiveReceipt, EnrollmentTokenPhase,
    FreshParticipantCapacityCounter, IdentityState, InitialEnrollmentClosureInput, OrderClaims,
    OrderHigh, OrderLedger, ParticipantSlotAllocationError, ParticipantSlotAllocatorProof,
    ResolvedIdentity, SequenceClaims, SequenceLedger,
};
use super::{
    InitialEnrollmentCommitValues, InitialEnrollmentOperationDecision,
    InitialEnrollmentOperationInput, ReceiptDeadlineError, ReceiptDeadlines,
    apply_initial_enrollment,
};

type TestInput<'a> = InitialEnrollmentOperationInput<'a, Vec<u8>, u64, Vec<u8>>;

#[derive(Clone, Debug, PartialEq, Eq)]
struct AllocationProof {
    conversation_id: u64,
    participant_index: u64,
    identity_limit: u64,
}

impl ParticipantSlotAllocatorProof for AllocationProof {
    fn conversation_id(&self) -> u64 {
        self.conversation_id
    }

    fn participant_index(&self) -> u64 {
        self.participant_index
    }

    fn identity_limit(&self) -> u64 {
        self.identity_limit
    }
}

fn request() -> EnrollmentRequest {
    EnrollmentRequest {
        conversation_id: 25,
        enrollment_token: EnrollmentToken::new([0x25; 16]),
    }
}

fn epoch() -> BindingEpoch {
    BindingEpoch::new(ConnectionIncarnation::new(7, 9), Generation::ONE)
}

fn counter(limit: u64, occupied: u64) -> CapacityCounter {
    CapacityCounter::try_new(limit, occupied).expect("test counter is bounded")
}

fn fresh_counter(limit: u64) -> FreshParticipantCapacityCounter {
    FreshParticipantCapacityCounter::try_new(limit, 0).expect("new participant owns no rows")
}

fn enrollment_capacity(identity_server: CapacityCounter) -> EnrollmentCapacityCounters {
    EnrollmentCapacityCounters::new(
        identity_server,
        counter(4, 0),
        counter(4, 0),
        fresh_counter(4),
        counter(4, 0),
        counter(4, 0),
        fresh_counter(4),
    )
}

fn closure_input() -> InitialEnrollmentClosureInput {
    let accounting = ClosureAccounting::try_new(
        ClosureState::Clear,
        0,
        0,
        0,
        0,
        ResourceVector::default(),
        WideResourceVector::new(1, 4),
        ResourceVector::new(5, 20),
        0,
        2,
    )
    .expect("Case 25 clear accounting is valid");
    InitialEnrollmentClosureInput::new(
        accounting,
        1,
        ResourceVector::new(2, 8),
        ResourceVector::new(2, 8),
        ResourceVector::new(1, 4),
        ResourceVector::new(1, 4),
        0,
        epoch(),
        OrderLedger::try_new(OrderHigh::Empty, OrderClaims::default())
            .expect("initial order ledger is empty"),
        SequenceLedger::try_new(0, SequenceClaims::default())
            .expect("initial sequence ledger is empty"),
        1,
        0,
    )
}

fn input<'a>(
    request: &'a EnrollmentRequest,
    token_phase: EnrollmentTokenPhase<'a, Vec<u8>, u64, Vec<u8>>,
    lookup_binding: &'a BindingState,
    tracking: ConnectionConversationTracking,
    connection_capacity: CapacityCounter,
    binding_occupancy: BindingSlotOccupancy,
    identity_server: CapacityCounter,
) -> TestInput<'a> {
    InitialEnrollmentOperationInput::new(
        request,
        token_phase,
        lookup_binding,
        tracking,
        connection_capacity,
        binding_occupancy,
        enrollment_capacity(identity_server),
        closure_input(),
    )
}

fn commit_values() -> InitialEnrollmentCommitValues<Vec<u8>> {
    InitialEnrollmentCommitValues::new(
        AttachSecret::new([0xA5; 32]),
        ReceiptDeadlines::try_from_ttls(0, 1_000, 2_000)
            .expect("test TTLs derive ordered deadlines"),
        EnrollmentFingerprint::new(vec![2, 5, 2, 5]),
    )
}

#[test]
fn receipt_deadlines_validate_precedence_and_widen_before_addition() {
    assert_eq!(
        ReceiptDeadlines::try_from_ttls(9, 0, 0),
        Err(ReceiptDeadlineError::ZeroAttachReceiptTtl)
    );
    assert_eq!(
        ReceiptDeadlines::try_from_ttls(9, 1, 0),
        Err(ReceiptDeadlineError::ZeroReceiptProvenanceTtl)
    );
    assert_eq!(
        ReceiptDeadlines::try_from_ttls(9, 2, 1),
        Err(ReceiptDeadlineError::ProvenanceTtlShorterThanReceipt {
            attach_receipt_ttl_ms: 2,
            receipt_provenance_ttl_ms: 1,
        })
    );

    let deadlines = ReceiptDeadlines::try_from_ttls(u64::MAX, u64::MAX, u64::MAX)
        .expect("widened u64 sums fit u128");
    assert_eq!(deadlines.receipt_expires_at(), u128::from(u64::MAX) * 2);
    assert_eq!(deadlines.provenance_expires_at(), u128::from(u64::MAX) * 2);
}

fn allocate() -> Result<AllocatedParticipantSlot<AllocationProof>, ParticipantSlotAllocationError> {
    AllocatedParticipantSlot::from_allocator(AllocationProof {
        conversation_id: 25,
        participant_index: 0,
        identity_limit: 1,
    })
}

#[test]
fn case_25_success_returns_one_atomic_commit() {
    let request = request();
    let binding = BindingState::Detached;
    let calls = Cell::new(0);
    let decision = apply_initial_enrollment(
        &input(
            &request,
            EnrollmentTokenPhase::Unmapped,
            &binding,
            ConnectionConversationTracking::Untracked,
            counter(4, 0),
            BindingSlotOccupancy::Empty,
            counter(4, 0),
        ),
        commit_values,
        || {
            calls.set(calls.get() + 1);
            allocate()
        },
    );
    let InitialEnrollmentOperationDecision::Commit(commit) = decision else {
        panic!("Case 25 must commit");
    };

    assert_eq!(calls.get(), 1);
    assert_eq!(commit.enrollment().member.participant_id(), 0);
    assert_eq!(commit.enrollment().member.cursor(), 0);
    assert_eq!(commit.enrollment().attached.delivery_seq(), 1);
    assert_eq!(commit.order().major(), 0);
    assert_eq!(commit.sequence().resulting().high_watermark(), 1);
    assert_eq!(commit.observer_floor().cap_floor(), 1);
    assert_eq!(
        commit.closure_projection().debt(),
        WideResourceVector::new(1, 4)
    );
    assert_eq!(commit.connection_capacity().resulting().occupied(), 1);
    assert_eq!(
        commit
            .enrollment_capacity()
            .resulting()
            .identity_server()
            .occupied(),
        1
    );
}

#[test]
fn token_replay_precedes_every_capacity_and_never_allocates() {
    let request = request();
    let first = apply_initial_enrollment(
        &input(
            &request,
            EnrollmentTokenPhase::Unmapped,
            &BindingState::Detached,
            ConnectionConversationTracking::Untracked,
            counter(4, 0),
            BindingSlotOccupancy::Empty,
            counter(4, 0),
        ),
        commit_values,
        allocate,
    );
    let InitialEnrollmentOperationDecision::Commit(committed) = first else {
        panic!("fixture enrollment must commit");
    };
    let identity: IdentityState<Vec<u8>, u64, Vec<u8>> =
        IdentityState::Live(committed.enrollment().member.clone());
    let receipt = EnrollmentLiveReceipt::from_commit(committed.enrollment().outcome.clone());
    let binding = committed.enrollment().binding_state;
    let calls = Cell::new(0);
    let value_calls = Cell::new(0);

    let replay = apply_initial_enrollment(
        &input(
            &request,
            EnrollmentTokenPhase::LiveReceipt {
                identity: ResolvedIdentity::from(&identity),
                receipt: &receipt,
            },
            &binding,
            ConnectionConversationTracking::Untracked,
            counter(1, 1),
            BindingSlotOccupancy::Occupied { participant_id: 99 },
            counter(1, 1),
        ),
        || {
            value_calls.set(value_calls.get() + 1);
            commit_values()
        },
        || {
            calls.set(calls.get() + 1);
            allocate()
        },
    );

    assert!(matches!(
        replay,
        InitialEnrollmentOperationDecision::Respond(ServerValue::Bound(_))
    ));
    assert_eq!(calls.get(), 0);
    assert_eq!(value_calls.get(), 0);
}

#[test]
fn stage_six_and_eight_refusals_are_ordered_and_nonmutating() {
    let request = request();
    let binding = BindingState::Detached;
    let calls = Cell::new(0);
    let value_calls = Cell::new(0);
    let semantic = apply_initial_enrollment(
        &input(
            &request,
            EnrollmentTokenPhase::Unmapped,
            &binding,
            ConnectionConversationTracking::Untracked,
            counter(1, 1),
            BindingSlotOccupancy::Occupied { participant_id: 9 },
            counter(1, 1),
        ),
        || {
            value_calls.set(value_calls.get() + 1);
            commit_values()
        },
        || {
            calls.set(calls.get() + 1);
            allocate()
        },
    );
    assert!(matches!(
        semantic,
        InitialEnrollmentOperationDecision::Respond(
            ServerValue::ConnectionConversationCapacityExceeded(_)
        )
    ));

    let binding_occupied = apply_initial_enrollment(
        &input(
            &request,
            EnrollmentTokenPhase::Unmapped,
            &binding,
            ConnectionConversationTracking::AlreadyTracked,
            counter(1, 1),
            BindingSlotOccupancy::Occupied { participant_id: 9 },
            counter(1, 1),
        ),
        || {
            value_calls.set(value_calls.get() + 1);
            commit_values()
        },
        || {
            calls.set(calls.get() + 1);
            allocate()
        },
    );
    assert!(matches!(
        binding_occupied,
        InitialEnrollmentOperationDecision::Respond(
            ServerValue::ConnectionConversationBindingOccupied(_)
        )
    ));

    let identity_capacity = apply_initial_enrollment(
        &input(
            &request,
            EnrollmentTokenPhase::Unmapped,
            &binding,
            ConnectionConversationTracking::AlreadyTracked,
            counter(1, 1),
            BindingSlotOccupancy::Empty,
            counter(1, 1),
        ),
        || {
            value_calls.set(value_calls.get() + 1);
            commit_values()
        },
        || {
            calls.set(calls.get() + 1);
            allocate()
        },
    );
    assert!(matches!(
        identity_capacity,
        InitialEnrollmentOperationDecision::Respond(ServerValue::IdentityCapacityExceeded(_))
    ));
    assert_eq!(calls.get(), 0);
    assert_eq!(value_calls.get(), 0);
}

#[test]
fn crash_replay_from_identical_prestate_is_identical() {
    let request = request();
    let binding = BindingState::Detached;
    let run = || {
        apply_initial_enrollment(
            &input(
                &request,
                EnrollmentTokenPhase::Unmapped,
                &binding,
                ConnectionConversationTracking::Untracked,
                counter(4, 0),
                BindingSlotOccupancy::Empty,
                counter(4, 0),
            ),
            commit_values,
            allocate,
        )
    };

    assert_eq!(run(), run());
}

#[test]
fn allocator_mismatch_does_not_mint_commit_values() {
    let request = request();
    let binding = BindingState::Detached;
    let value_calls = Cell::new(0);
    let decision = apply_initial_enrollment(
        &input(
            &request,
            EnrollmentTokenPhase::Unmapped,
            &binding,
            ConnectionConversationTracking::Untracked,
            counter(4, 0),
            BindingSlotOccupancy::Empty,
            counter(4, 0),
        ),
        || {
            value_calls.set(value_calls.get() + 1);
            commit_values()
        },
        || {
            AllocatedParticipantSlot::from_allocator(AllocationProof {
                conversation_id: 25,
                participant_index: 1,
                identity_limit: 2,
            })
        },
    );

    assert!(matches!(
        decision,
        InitialEnrollmentOperationDecision::Fault(
            super::InitialEnrollmentOperationFault::AllocatedParticipantMismatch {
                expected: 0,
                actual: 1,
            }
        )
    ));
    assert_eq!(value_calls.get(), 0);
}

#[test]
fn allocator_identity_domain_mismatch_does_not_mint_commit_values() {
    let request = request();
    let binding = BindingState::Detached;
    let value_calls = Cell::new(0);
    let decision = apply_initial_enrollment(
        &input(
            &request,
            EnrollmentTokenPhase::Unmapped,
            &binding,
            ConnectionConversationTracking::Untracked,
            counter(4, 0),
            BindingSlotOccupancy::Empty,
            counter(4, 0),
        ),
        || {
            value_calls.set(value_calls.get() + 1);
            commit_values()
        },
        || {
            AllocatedParticipantSlot::from_allocator(AllocationProof {
                conversation_id: 25,
                participant_index: 0,
                identity_limit: 2,
            })
        },
    );

    assert!(matches!(
        decision,
        InitialEnrollmentOperationDecision::Fault(
            super::InitialEnrollmentOperationFault::AllocatedIdentityLimitMismatch {
                expected: 1,
                actual: 2,
            }
        )
    ));
    assert_eq!(value_calls.get(), 0);
}
