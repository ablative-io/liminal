#![allow(clippy::expect_used, clippy::panic)]

use crate::wire::{
    AttachAttemptToken, AttachEnvelope, ConversationOrderExhausted, EnrollmentEnvelope,
    EnrollmentToken, Generation, OrderAllocatingEnvelope, RecordAdmissionEnvelope,
};

use super::{
    OrderAdmissionError, OrderClaims, OrderHigh, OrderLedger, OrderLedgerInvariantError,
    allocate_order,
};

fn generation(value: u64) -> Generation {
    Generation::new(value).expect("test generation is nonzero")
}

fn enrollment_request() -> OrderAllocatingEnvelope {
    OrderAllocatingEnvelope::Enrollment(EnrollmentEnvelope {
        conversation_id: 43,
        enrollment_token: EnrollmentToken::new([0x43; 16]),
    })
}

fn attach_request() -> OrderAllocatingEnvelope {
    OrderAllocatingEnvelope::CredentialAttach(AttachEnvelope {
        conversation_id: 43,
        participant_id: 3,
        capability_generation: generation(7),
        attach_attempt_token: AttachAttemptToken::new([0xA3; 16]),
        accept_marker_delivery_seq: None,
    })
}

fn record_request() -> OrderAllocatingEnvelope {
    OrderAllocatingEnvelope::RecordAdmission(RecordAdmissionEnvelope {
        conversation_id: 43,
        participant_id: 3,
        capability_generation: generation(7),
    })
}

#[test]
fn empty_ledger_owns_all_values_and_enrollment_allocates_zero() {
    let ledger = OrderLedger::try_new(OrderHigh::Empty, OrderClaims::default())
        .expect("empty order ledger is valid");
    assert_eq!(ledger.remaining(), u128::from(u64::MAX) + 1);

    let planned = ledger
        .plan_enrollment()
        .expect("initial enrollment claim additions fit");
    let allocation =
        allocate_order(enrollment_request(), ledger, planned).expect("initial enrollment fits");
    assert_eq!(allocation.major(), 0);
    assert_eq!(allocation.resulting().high(), OrderHigh::Allocated(0));
    assert_eq!(allocation.resulting().claims().total(), 2);
}

#[test]
fn optional_planners_apply_exact_deltas_and_preserve_recovery_claims() {
    let current = OrderLedger::try_new(
        OrderHigh::Allocated(100),
        OrderClaims::new(4, 5, true, true),
    )
    .expect("fixture order claims fit");

    let enrollment = allocate_order(
        enrollment_request(),
        current,
        current.plan_enrollment().expect("enrollment additions fit"),
    )
    .expect("enrollment order allocation fits")
    .resulting();
    assert_claims(enrollment.claims(), 5, 6, true, true);

    let detached_attach = allocate_order(
        attach_request(),
        current,
        current
            .plan_detached_attach()
            .expect("detached attach addition fits"),
    )
    .expect("detached attach order allocation fits")
    .resulting();
    assert_claims(detached_attach.claims(), 5, 5, true, true);

    let supersession = allocate_order(attach_request(), current, current.plan_supersession())
        .expect("supersession order allocation fits")
        .resulting();
    assert_claims(supersession.claims(), 4, 5, true, true);

    let ordinary = allocate_order(record_request(), current, current.plan_ordinary_record())
        .expect("ordinary order allocation fits")
        .resulting();
    assert_claims(ordinary.claims(), 4, 5, true, true);

    for resulting in [enrollment, detached_attach, supersession, ordinary] {
        assert_eq!(resulting.high(), OrderHigh::Allocated(101));
    }
}

#[test]
fn checked_planners_reject_claim_counter_overflow() {
    let active_max = OrderLedger::try_new(
        OrderHigh::Empty,
        OrderClaims::new(u64::MAX, 0, false, false),
    )
    .expect("MAX active claims fit the empty counter suffix");
    assert_eq!(
        active_max.plan_enrollment(),
        Err(OrderAdmissionError::ActiveBindingClaimOverflow)
    );
    assert_eq!(
        active_max.plan_detached_attach(),
        Err(OrderAdmissionError::ActiveBindingClaimOverflow)
    );

    let exits_max = OrderLedger::try_new(
        OrderHigh::Empty,
        OrderClaims::new(0, u64::MAX, false, false),
    )
    .expect("MAX exit claims fit the empty counter suffix");
    assert_eq!(
        exits_max.plan_enrollment(),
        Err(OrderAdmissionError::MembershipExitClaimOverflow)
    );
}

#[test]
fn fenced_recovery_consumes_ro_and_transfers_ra_without_allocating() {
    let current =
        OrderLedger::try_new(OrderHigh::Allocated(42), OrderClaims::new(2, 3, true, true))
            .expect("fenced recovery fixture is valid");
    let resulting = current
        .apply_fenced_recovery()
        .expect("coupled recovery reserve transfers exactly");

    assert_eq!(resulting.high(), current.high());
    assert_eq!(resulting.remaining(), current.remaining());
    assert_claims(resulting.claims(), 3, 3, false, false);
    assert_eq!(current.claims().total() - resulting.claims().total(), 1);
}

#[test]
fn fenced_recovery_requires_the_coupled_ro_ra_pair() {
    let missing_replacement = OrderLedger::try_new(
        OrderHigh::Allocated(10),
        OrderClaims::new(0, 1, true, false),
    )
    .expect("partial recovery fixture is representable for corruption refusal");
    assert_eq!(
        missing_replacement.apply_fenced_recovery(),
        Err(OrderAdmissionError::RecoveryOrderReserveMissing {
            recovery_operation: true,
            recovery_replacement_terminal: false,
        })
    );

    let missing_operation = OrderLedger::try_new(
        OrderHigh::Allocated(10),
        OrderClaims::new(0, 1, false, true),
    )
    .expect("partial recovery fixture is representable for corruption refusal");
    assert_eq!(
        missing_operation.apply_fenced_recovery(),
        Err(OrderAdmissionError::RecoveryOrderReserveMissing {
            recovery_operation: false,
            recovery_replacement_terminal: true,
        })
    );
}

#[test]
fn case_43_exact_claim_shortfall_selects_order_exhaustion() {
    let ledger = OrderLedger::try_new(
        OrderHigh::Allocated(u64::MAX - 2),
        OrderClaims::new(1, 1, false, false),
    )
    .expect("two claims own the final two values");
    let error = allocate_order(record_request(), ledger, ledger.plan_ordinary_record())
        .expect_err("one caller major leaves only one value for two claims");
    let OrderAdmissionError::Exhausted(exhausted) = error else {
        panic!("fixture must select canonical exhaustion");
    };
    assert_exhaustion(&exhausted, u64::MAX - 2, 2, 2, 1, 2);
}

#[test]
fn final_major_is_legal_only_when_no_claim_survives() {
    let clear = OrderLedger::try_new(OrderHigh::Allocated(u64::MAX - 1), OrderClaims::default())
        .expect("one unreserved major remains");
    let allocation = allocate_order(record_request(), clear, clear.plan_ordinary_record())
        .expect("MAX is legal with no post-state claims");
    assert_eq!(allocation.major(), u64::MAX);
    assert_eq!(allocation.resulting().remaining(), 0);

    let claimed = OrderLedger::try_new(
        OrderHigh::Allocated(u64::MAX - 1),
        OrderClaims::new(1, 0, false, false),
    )
    .expect("one final value owns one claim");
    let error = allocate_order(record_request(), claimed, claimed.plan_ordinary_record())
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
    let error = allocate_order(record_request(), ledger, ledger.plan_ordinary_record())
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

fn assert_claims(claims: OrderClaims, active: u64, exits: u64, ro: bool, ra: bool) {
    assert_eq!(claims.active_binding_terminals(), active);
    assert_eq!(claims.membership_exits(), exits);
    assert_eq!(claims.recovery_operation(), ro);
    assert_eq!(claims.recovery_replacement_terminal(), ra);
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
