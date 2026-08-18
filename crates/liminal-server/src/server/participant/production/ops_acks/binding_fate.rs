//! Sealed binding-fate token progression for both ack arms.
//!
//! The seam is the `#26` pair itself: two functions that do the same thing for
//! two different commit types, whose refusal strings are DELIBERATELY different,
//! and whose only correctness argument is the contrast between them. Splitting
//! them across the commit and replay modules would put the sibling on the other
//! side of a file boundary from the comment that reasons about it, and the
//! reason the wordings differ would survive only as a cross-reference. They are
//! callers-of-one-idea, so they live together and both arms import them.

use liminal_protocol::lifecycle::{MarkerAckCommit, ParticipantAckCommit};

use super::super::state::{PendingBindingFate, Slot, StateError};

/// Moves the sealed binding-fate token in step with a MARKER acknowledgement.
///
/// The sibling of `progress_pending_binding_fate`, for the path that never had
/// one. A member with no pending fate token is the ordinary case and returns
/// `Ok` untouched — the token only exists after a `CredentialAttach` mints it.
///
/// # ⚠ WHY THIS REFUSAL IS WORDED DIFFERENTLY FROM ITS SIBLING
///
/// The sibling raises `ack cursor commit disagrees with sealed binding-fate
/// authority` — the string the `#26` defect surfaces, because under the defect
/// it is the ORDINARY ack that gets refused against a token this path failed to
/// move. If this function reused that wording, a future refusal from the marker
/// path would be indistinguishable from the original bug in every log and every
/// assertion, and "the fix regressed" would read exactly like "the fix was never
/// applied". The distinct prefix keeps the two separable at a glance.
///
/// ⛔ It does NOT make the string safe as a gate signal. A refusal is a refusal
/// in both a healthy and a broken tree; only STATE — whether the next ordinary
/// ack COMMITS — discriminates, and the guarding units are built on that.
pub(super) fn progress_pending_marker_binding_fate(
    slot: &mut Slot,
    commit: &MarkerAckCommit,
) -> Result<(), StateError> {
    let Some(pending) = slot.binding_fate.take() else {
        return Ok(());
    };
    let PendingBindingFate {
        attached_source_sequence,
        token,
    } = pending;
    match commit.progress_binding_fate_token(token) {
        Ok(token) => {
            slot.binding_fate = Some(PendingBindingFate {
                attached_source_sequence,
                token,
            });
            Ok(())
        }
        Err(token) => {
            slot.binding_fate = Some(PendingBindingFate {
                attached_source_sequence,
                token: *token,
            });
            Err(StateError::invariant(
                "marker ack cursor commit disagrees with sealed binding-fate authority",
            ))
        }
    }
}

pub(super) fn progress_pending_binding_fate(
    slot: &mut Slot,
    commit: &ParticipantAckCommit,
) -> Result<(), StateError> {
    let Some(pending) = slot.binding_fate.take() else {
        return Ok(());
    };
    let PendingBindingFate {
        attached_source_sequence,
        token,
    } = pending;
    match commit.progress_binding_fate_token(token) {
        Ok(token) => {
            slot.binding_fate = Some(PendingBindingFate {
                attached_source_sequence,
                token,
            });
            Ok(())
        }
        Err(token) => {
            slot.binding_fate = Some(PendingBindingFate {
                attached_source_sequence,
                token: *token,
            });
            Err(StateError::invariant(
                "ack cursor commit disagrees with sealed binding-fate authority",
            ))
        }
    }
}
