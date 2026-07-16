#![allow(clippy::expect_used, clippy::panic)]

use crate::wire::{
    ConversationOrderExhausted, EnrollmentEnvelope, EnrollmentToken, OrderAllocatingEnvelope,
};

use super::{
    OrderAdmissionError, OrderClaims, OrderHigh, OrderLedger, OrderLedgerInvariantError,
    ResultingOrderClaims, allocate_order,
};

fn request() -> OrderAllocatingEnvelope {
    OrderAllocatingEnvelope::Enrollment(EnrollmentEnvelope {
        conversation_id: 43,
        enrollment_token: EnrollmentToken::new([0x43; 16]),
    })
}

#[test]
fn empty_ledger_owns_all_values_and_allocates_zero() {
    let ledger = OrderLedger::try_new(OrderHigh::Empty, OrderClaims::default())
        .expect("empty order ledger is valid");
    assert_eq!(ledger.remaining(), u128::from(u64::MAX) + 1);

    let allocation = allocate_order(
        request(),
        ledger,
        ResultingOrderClaims::from_claims(OrderClaims::new(1, 1, false, false)),
    )
    .expect("initial enrollment fits");
    assert_eq!(allocation.major(), 0);
    assert_eq!(allocation.resulting().high(), OrderHigh::Allocated(0));
    assert_eq!(allocation.resulting().claims().total(), 2);
}

#[test]
fn case_43_exact_claim_shortfall_selects_order_exhaustion() {
    let ledger = OrderLedger::try_new(
        OrderHigh::Allocated(u64::MAX - 2),
        OrderClaims::new(1, 1, false, false),
    )
    .expect("two claims own the final two values");
    let error = allocate_order(
        request(),
        ledger,
        ResultingOrderClaims::from_claims(OrderClaims::new(1, 1, false, false)),
    )
    .expect_err("one caller major leaves only one value for two claims");
    let OrderAdmissionError::Exhausted(exhausted) = error else {
        panic!("fixture must select canonical exhaustion");
    };
    assert_exhaustion(&exhausted, u64::MAX - 2, 2, 2, 1, 2);
}

#[test]
fn final_major_is_legal_only_when_no_claim_survives() {
    let ledger = OrderLedger::try_new(
        OrderHigh::Allocated(u64::MAX - 1),
        OrderClaims::new(1, 0, false, false),
    )
    .expect("one final value owns one claim");
    let allocation = allocate_order(
        request(),
        ledger,
        ResultingOrderClaims::from_claims(OrderClaims::default()),
    )
    .expect("MAX is legal with no post-state claims");
    assert_eq!(allocation.major(), u64::MAX);
    assert_eq!(allocation.resulting().remaining(), 0);

    let ledger = OrderLedger::try_new(
        OrderHigh::Allocated(u64::MAX - 1),
        OrderClaims::new(1, 0, false, false),
    )
    .expect("one final value owns one claim");
    let error = allocate_order(
        request(),
        ledger,
        ResultingOrderClaims::from_claims(OrderClaims::new(1, 0, false, false)),
    )
    .expect_err("a surviving claim cannot coexist with allocation of MAX");
    let OrderAdmissionError::Exhausted(exhausted) = error else {
        panic!("fixture must select canonical exhaustion");
    };
    assert_exhaustion(&exhausted, u64::MAX - 1, 1, 1, 0, 1);
}

#[test]
fn exhausted_maximum_has_no_next_value() {
    let ledger = OrderLedger::try_new(OrderHigh::Allocated(u64::MAX), OrderClaims::default())
        .expect("drained maximum ledger is valid");
    let error = allocate_order(
        request(),
        ledger,
        ResultingOrderClaims::from_claims(OrderClaims::default()),
    )
    .expect_err("no major exists after MAX");
    let OrderAdmissionError::Exhausted(exhausted) = error else {
        panic!("fixture must select canonical exhaustion");
    };
    assert_eq!(exhausted.high(), u64::MAX);
    assert_eq!(exhausted.next_value(), None);
    assert_eq!(exhausted.order_remaining(), 0);
    assert_eq!(exhausted.resulting_order_remaining(), 0);
    assert_eq!(exhausted.resulting_reserved_claims(), 0);
}

#[test]
fn restore_rejects_claims_beyond_the_counter_suffix() {
    let error = OrderLedger::try_new(
        OrderHigh::Allocated(u64::MAX),
        OrderClaims::new(1, 0, false, false),
    )
    .expect_err("no value remains to own the claim");
    assert_eq!(
        error,
        OrderLedgerInvariantError::ClaimsExceedRemaining {
            remaining: 0,
            claims: 1,
        }
    );
}

fn assert_exhaustion(
    exhausted: &ConversationOrderExhausted,
    high: u64,
    remaining: u128,
    claims: u128,
    resulting_remaining: u128,
    resulting_claims: u128,
) {
    assert_eq!(exhausted.high(), high);
    assert_eq!(exhausted.next_value(), high.checked_add(1));
    assert_eq!(exhausted.order_remaining(), remaining);
    assert_eq!(exhausted.reserved_claims(), claims);
    assert_eq!(exhausted.resulting_order_remaining(), resulting_remaining);
    assert_eq!(exhausted.resulting_reserved_claims(), resulting_claims);
}
