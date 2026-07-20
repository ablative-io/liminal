use liminal_protocol::lifecycle::with_validated_marker_record_type;

fn require_copy<T: Copy>(value: T) {
    let first = value;
    let second = value;
    drop((first, second));
}

fn main() {
    with_validated_marker_record_type(require_copy);
}
