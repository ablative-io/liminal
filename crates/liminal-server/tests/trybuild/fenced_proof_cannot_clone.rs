use liminal_protocol::lifecycle::FencedAttachCommit;

fn clone_proof(proof: FencedAttachCommit) {
    let cloned = proof.clone();
    drop(cloned);
}

fn main() {
    let _ = clone_proof;
}
