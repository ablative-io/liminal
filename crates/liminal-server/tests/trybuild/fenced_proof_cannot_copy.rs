use liminal_protocol::lifecycle::FencedAttachCommit;

fn copy_proof(proof: FencedAttachCommit) {
    let retained = proof;
    drop(retained);
    let copied = proof;
    drop(copied);
}

fn main() {
    let _ = copy_proof;
}
