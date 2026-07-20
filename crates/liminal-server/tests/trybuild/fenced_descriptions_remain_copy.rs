use liminal_protocol::lifecycle::{CursorFateSuccessor, DetachedCredentialRecovery};

fn copy_recovery(value: DetachedCredentialRecovery) {
    let retained = value;
    let copied = value;
    let _ = (retained, copied);
}

fn copy_successor(value: CursorFateSuccessor) {
    let retained = value;
    let copied = value;
    let _ = (retained, copied);
}

fn main() {
    let _ = copy_recovery;
    let _ = copy_successor;
}
