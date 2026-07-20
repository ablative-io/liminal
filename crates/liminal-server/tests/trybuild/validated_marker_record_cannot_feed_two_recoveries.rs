use liminal_protocol::lifecycle::{
    DetachedCredentialRecovery, with_validated_marker_record_type,
};

fn feed<T>(record: T, recovery: DetachedCredentialRecovery) {
    drop((record, recovery));
}

fn cannot_fork(recovery: DetachedCredentialRecovery) {
    let copied_recovery = recovery;
    with_validated_marker_record_type(|record| {
        feed(record, recovery);
        feed(record, copied_recovery);
    });
}

fn main() {}
