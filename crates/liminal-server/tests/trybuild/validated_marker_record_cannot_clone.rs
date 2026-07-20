use liminal_protocol::lifecycle::with_validated_marker_record_type;

fn main() {
    with_validated_marker_record_type(|record| {
        drop(record.clone());
    });
}
