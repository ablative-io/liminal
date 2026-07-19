use liminal_protocol::lifecycle::LeaveCommit;

fn project_plain_leave(commit: &LeaveCommit<(), (), ()>) {
    drop(commit.observer_progress_projection());
}

fn main() {}
