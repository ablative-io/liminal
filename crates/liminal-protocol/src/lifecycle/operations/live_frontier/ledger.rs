use crate::{
    lifecycle::{
        OrderClaims, OrderHigh, OrderLedger, RetainedCausalRecord, SequenceClaims, SequenceLedger,
    },
    wire::TransactionOrder,
};

pub(super) fn enrollment_sequence(current: SequenceLedger, high: u64) -> Option<SequenceLedger> {
    let claims = current.claims();
    SequenceLedger::try_new(
        high,
        SequenceClaims::new(
            claims.live_members().checked_add(1)?,
            claims.binding_terminals().checked_add(1)?,
            claims.markers(),
            claims.recovery(),
        ),
    )
    .ok()
}

pub(super) fn detached_attach_sequence(
    current: SequenceLedger,
    high: u64,
) -> Option<SequenceLedger> {
    let claims = current.claims();
    SequenceLedger::try_new(
        high,
        SequenceClaims::new(
            claims.live_members(),
            claims.binding_terminals().checked_add(1)?,
            claims.markers(),
            claims.recovery(),
        ),
    )
    .ok()
}

pub(super) fn superseding_attach_sequence(
    current: SequenceLedger,
    rows: &[RetainedCausalRecord],
) -> Option<SequenceLedger> {
    let high = rows.last()?.delivery_seq;
    let first = rows.first()?.delivery_seq;
    (first == current.high_watermark().checked_add(1)? && high == first.checked_add(1)?)
        .then(|| SequenceLedger::try_new(high, current.claims()).ok())
        .flatten()
}

pub(super) fn detach_sequence(current: SequenceLedger, high: u64) -> Option<SequenceLedger> {
    let claims = current.claims();
    SequenceLedger::try_new(
        high,
        SequenceClaims::new(
            claims.live_members(),
            claims.binding_terminals().checked_sub(1)?,
            claims.markers(),
            claims.recovery(),
        ),
    )
    .ok()
}

pub(super) fn enrollment_order(
    current: OrderLedger,
    major: TransactionOrder,
) -> Option<OrderLedger> {
    let claims = current.claims();
    next_order(
        current,
        major,
        OrderClaims::new(
            claims.active_binding_terminals().checked_add(1)?,
            claims.membership_exits().checked_add(1)?,
            claims.recovery_operation(),
            claims.recovery_replacement_terminal(),
        )
        .ok()?,
    )
}

pub(super) fn detached_attach_order(
    current: OrderLedger,
    major: TransactionOrder,
) -> Option<OrderLedger> {
    let claims = current.claims();
    next_order(
        current,
        major,
        OrderClaims::new(
            claims.active_binding_terminals().checked_add(1)?,
            claims.membership_exits(),
            claims.recovery_operation(),
            claims.recovery_replacement_terminal(),
        )
        .ok()?,
    )
}

pub(super) fn superseding_attach_order(
    current: OrderLedger,
    major: TransactionOrder,
) -> Option<OrderLedger> {
    next_order(current, major, current.claims())
}

pub(super) fn detach_order(current: OrderLedger, major: TransactionOrder) -> Option<OrderLedger> {
    let claims = current.claims();
    next_order(
        current,
        major,
        OrderClaims::new(
            claims.active_binding_terminals().checked_sub(1)?,
            claims.membership_exits(),
            claims.recovery_operation(),
            claims.recovery_replacement_terminal(),
        )
        .ok()?,
    )
}

fn next_order(
    current: OrderLedger,
    major: TransactionOrder,
    claims: OrderClaims,
) -> Option<OrderLedger> {
    let expected = match current.high() {
        OrderHigh::Empty => 0,
        OrderHigh::Allocated(high) => high.checked_add(1)?,
    };
    (major == expected)
        .then(|| OrderLedger::try_new(OrderHigh::Allocated(major), claims).ok())
        .flatten()
}
