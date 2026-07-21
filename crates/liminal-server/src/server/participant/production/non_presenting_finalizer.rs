//! Typed projection-free result for a finalizer whose occurrence is already owned.

use liminal_protocol::lifecycle::{IdentityState, LiveFrontierOwner};

use super::facts::Digest;

/// Pending-Died finalizer result that deliberately exposes no projection accessor.
pub(super) struct NonPresentingFinalizerCommit {
    identity: IdentityState<Digest, Digest, Digest>,
    owner: LiveFrontierOwner,
}

impl NonPresentingFinalizerCommit {
    pub(super) const fn new(
        identity: IdentityState<Digest, Digest, Digest>,
        owner: LiveFrontierOwner,
    ) -> Self {
        Self { identity, owner }
    }

    pub(super) fn into_parts(self) -> (IdentityState<Digest, Digest, Digest>, LiveFrontierOwner) {
        (self.identity, self.owner)
    }
}
