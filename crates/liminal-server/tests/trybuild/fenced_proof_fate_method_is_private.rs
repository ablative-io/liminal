use liminal_protocol::lifecycle::{Event, FencedAttachCommit};

fn bypass_split(proof: FencedAttachCommit, event: Event) {
    let fate = proof.recovered_binding_fate(event);
    drop(fate);
}

fn main() {
    let _ = bypass_split;
}
