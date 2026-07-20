use liminal_protocol::lifecycle::{
    ClosureDebt, DebtCompletion, DetachedCredentialRecovery, Event,
};

fn raw_mint(
    recovery: DetachedCredentialRecovery,
    debt: ClosureDebt,
    event: Event,
    successor: DebtCompletion,
) {
    let _ = recovery.fenced_attach(debt, event, successor);
}

fn main() {
    let _ = raw_mint;
}
