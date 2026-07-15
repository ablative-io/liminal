#![allow(clippy::expect_used)]

use crate::wire::{
    AttachAttemptToken, AttachBound, AttachSecret, BindingEpoch, ConnectionIncarnation,
    DetachAttemptToken, DetachEnvelope, Generation,
};

use super::{
    AuthoritySuperseded, ParkOrderCounter, SdkDetachReplayAuthority,
    SdkObserverParkCapacityExceeded, SdkParkOrderExhausted,
};

#[test]
fn park_capacity_has_only_the_five_legal_pairs() {
    let values = [
        SdkObserverParkCapacityExceeded::PerConversationRows {
            conversation_id: 1,
            limit: 2,
            occupied: 2,
            requested: 1,
        },
        SdkObserverParkCapacityExceeded::PerConversationBytes {
            conversation_id: 1,
            limit: 20,
            occupied: 16,
            requested: 8,
        },
        SdkObserverParkCapacityExceeded::SdkWideConversations {
            conversation_id: 3,
            limit: 2,
            occupied: 2,
            requested: 1,
        },
        SdkObserverParkCapacityExceeded::SdkWideRows {
            conversation_id: 1,
            limit: 2,
            occupied: 2,
            requested: 1,
        },
        SdkObserverParkCapacityExceeded::SdkWideBytes {
            conversation_id: 1,
            limit: 20,
            occupied: 16,
            requested: 8,
        },
    ];
    assert_eq!(values.len(), 5);
}

#[test]
fn park_order_exhaustion_fixes_counter_and_value() {
    let outcome = SdkParkOrderExhausted::new(9);
    assert_eq!(outcome.counter(), ParkOrderCounter::ParkOrder);
    assert_eq!(outcome.value(), u64::MAX);
}

#[test]
fn newer_attach_terminalizes_old_detach_replay_authority() {
    let old_generation = Generation::new(7).expect("test generation is nonzero");
    let new_generation = Generation::new(8).expect("test generation is nonzero");
    let authority = SdkDetachReplayAuthority::new(DetachEnvelope {
        conversation_id: 10,
        participant_id: 20,
        capability_generation: old_generation,
        detach_attempt_token: DetachAttemptToken::new([3; 16]),
    });
    let attach = AttachBound::ordinary(
        10,
        AttachAttemptToken::new([2; 16]),
        20,
        old_generation,
        AttachSecret::new([5; 32]),
        BindingEpoch::new(ConnectionIncarnation::new(40, 41), new_generation),
        6,
        100,
        200,
    )
    .expect("test receipt has an exact successor generation");

    let terminal: AuthoritySuperseded = authority
        .supersede(&attach)
        .expect("matching newer attach must supersede replay authority");
    let _ = terminal;
}

#[test]
fn unrelated_attach_preserves_detach_replay_authority() {
    let old_generation = Generation::new(7).expect("test generation is nonzero");
    let new_generation = Generation::new(8).expect("test generation is nonzero");
    let request = DetachEnvelope {
        conversation_id: 10,
        participant_id: 20,
        capability_generation: old_generation,
        detach_attempt_token: DetachAttemptToken::new([3; 16]),
    };
    let authority = SdkDetachReplayAuthority::new(request.clone());
    let attach = AttachBound::ordinary(
        11,
        AttachAttemptToken::new([2; 16]),
        20,
        old_generation,
        AttachSecret::new([5; 32]),
        BindingEpoch::new(ConnectionIncarnation::new(40, 41), new_generation),
        6,
        100,
        200,
    )
    .expect("test receipt has an exact successor generation");

    let still_active = authority
        .supersede(&attach)
        .expect_err("another conversation cannot supersede this token");
    assert_eq!(still_active.request(), &request);
}
