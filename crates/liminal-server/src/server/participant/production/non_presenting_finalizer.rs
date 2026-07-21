//! Typed projection-free result for a finalizer whose occurrence is already owned.

use liminal_protocol::lifecycle::LiveFrontierOwner;

/// Pending-Died finalizer result that deliberately exposes no projection accessor.
pub(super) struct NonPresentingFinalizerCommit<T> {
    commit: T,
    owner: LiveFrontierOwner,
}

impl<T> NonPresentingFinalizerCommit<T> {
    pub(super) const fn new(commit: T, owner: LiveFrontierOwner) -> Self {
        Self { commit, owner }
    }

    pub(super) fn into_parts(self) -> (T, LiveFrontierOwner) {
        (self.commit, self.owner)
    }
}
