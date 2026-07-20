use liminal_protocol::lifecycle::{
    ClosureDebt, DebtCompletion, DetachedCredentialRecovery, Event, LiveFrontierOwner,
};

fn mint_twice(
    owner: LiveFrontierOwner,
    marker_source_sequence: u64,
    recovery: DetachedCredentialRecovery,
    debt: ClosureDebt,
    event: Event,
    successor: DebtCompletion,
) {
    let first = owner.mint_fenced_attach(
        marker_source_sequence,
        recovery,
        debt,
        event,
        successor,
    );
    drop(first);
    let second = owner.mint_fenced_attach(
        marker_source_sequence,
        recovery,
        debt,
        event,
        successor,
    );
    drop(second);
}

fn main() {
    let _ = mint_twice;
}
