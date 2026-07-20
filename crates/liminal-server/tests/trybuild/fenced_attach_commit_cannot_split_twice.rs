use liminal_protocol::lifecycle::AttachCommit;

fn split_twice(commit: AttachCommit<Vec<u8>, Vec<u8>>) {
    let first = commit.into_slot_and_fate();
    drop(first);
    let second = commit.into_slot_and_fate();
    drop(second);
}

fn main() {
    let _ = split_twice;
}
