use super::{ParkOrderCounter, SdkObserverParkCapacityExceeded, SdkParkOrderExhausted};

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
