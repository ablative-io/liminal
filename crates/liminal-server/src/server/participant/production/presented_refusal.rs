//! Board #14's funnel: ONE exit for a refusal the client is entitled to hear.
//!
//! # Why a carrier exists at all
//!
//! `docs/design/ATTACH-SILENCE-14.md` measured 28 `StateError::invariant`
//! sites on the attach path and found they converge on ONE exit before they
//! reach the client — `ParticipantSemanticError::Internal`, whose own doc says
//! its text is "never placed on the participant wire", then
//! `ParticipantDispatch::Fatal`, then a bare `FrameAction::Close`. Its
//! conclusion: **the wire half of #14 is not 28 edits; it is one decision at
//! one place, plus a classification of which sites are entitled to reach it.**
//!
//! Two populations shared that exit. Class A is genuinely internal — state
//! corruption, broken invariants over durable bytes — and the contract says
//! those "fail the conversation closed, preserve durable bytes, and create no
//! wire retry/poll", so silence is compliant and inventing an answer for them
//! would contradict the register. Class B is an ordinary refusal wearing a
//! `StateError::invariant` coat: a well-formed, correctly-authorized request
//! the server declines for a reason the client could act on. Class B is
//! entitled to a frame and did not get one.
//!
//! This type is the Class B exit. It carries a refusal from wherever the
//! refusing transition lives out to the arm boundary, where
//! `handler_semantic`'s `conversation_operation_with_impact` turns it back
//! into an ordinary response — the same shape every already-correct refusal on
//! the same arm takes.
//!
//! # The invariant this type does NOT weaken
//!
//! `ParticipantSemanticError` is "deliberately not convertible to
//! `ServerValue`, preventing the server from inventing a lifecycle response
//! when the protocol-owned transition did not produce one." That property is
//! preserved here BY CONSTRUCTION, not by convention: the only ways to build a
//! `PresentedRefusal` take a `CredentialAttachResponse` or a `DetachResponse`,
//! the operation-bound response authorities in `liminal-protocol` whose own
//! doc reads *"Constructors exist only for the outcomes the frozen R-D1
//! register admits ...; every other pairing is a compile error by
//! construction."* There is no constructor from a bare `ServerValue`, so a
//! hand-built lifecycle response cannot enter this path.
//!
//! # ⛔ The precondition every raise site must satisfy
//!
//! A `PresentedRefusal` becomes an `Ok` at the arm boundary, so the
//! conversation owner is RETAINED rather than discarded. Every existing
//! `StateError` is an `Err` there, and `with_conversation_reconciliation`
//! answers an `Err` by dropping the possibly part-consumed owner and
//! cold-replaying durable truth on the next touch.
//!
//! ⛔ **A `PresentedRefusal` may therefore only be raised where the transition
//! has consumed NO authority**: no `take_frontier`, no `take_shell`, no
//! `slots.remove_entry`, no durable append. A refusal raised after any of
//! those would leave a part-consumed owner installed, and the frame would be
//! bought with corruption. Where a Class B refusal is only detectable after
//! authority has been consumed, the site needs either a pre-flight predicate
//! or an explicit restore — it does not get to use this carrier as-is.
//!
//! Stage-8 capacity reservations are the one deliberate exception, and they
//! are safe because they are already guards: `AttachStage8::Reserved` holds a
//! `CapacityReservation` that "rolls back unless confirmed", so an early
//! return through this carrier releases it on drop exactly as an early return
//! through any other refusal does.

use liminal_protocol::wire::{
    CredentialAttachResponse, DetachResponse, EnrollmentResponse, ServerValue,
};

/// A refusal the frozen R-D1 register admits on the participant wire, carried
/// out of a transition that cannot answer for itself.
///
/// The value is BOXED, and not for tidiness. `StateError` is the error half of
/// nearly every `Result` in this module, so its size is paid on every
/// successful call too; a `ServerValue` stored inline widens it enough that
/// `clippy::result_large_err` fires at roughly two hundred sites. One
/// indirection on the rarest path is the right trade.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct PresentedRefusal {
    value: Box<ServerValue>,
}

impl PresentedRefusal {
    /// Presents a credential-attach refusal selected by the operation's own
    /// register-bound response authority.
    pub(super) fn credential_attach(response: CredentialAttachResponse) -> Self {
        Self {
            value: Box::new(response.into_server_value()),
        }
    }

    /// Presents a detach refusal selected by the operation's own
    /// register-bound response authority.
    ///
    /// Dormant no longer. Participant contract §0.16 (amendment A5, RATIFIED
    /// 2026-08-13) is the contract surface this constructor's own
    /// `#[expect(dead_code)]` named as its unblocking condition, and its first
    /// sink is the detach twin of the attach flatten — `detach_commit`'s
    /// frontier transition, refused while a marker candidate awaits its drain.
    pub(super) fn detach(response: DetachResponse) -> Self {
        Self {
            value: Box::new(response.into_server_value()),
        }
    }

    /// Presents an enrollment refusal selected by the operation's own
    /// register-bound response authority.
    ///
    /// The third wrapper of `apply_live_transition` was found by the
    /// reviewer-of-record's census, which is why §0.16's law attaches to the
    /// SEAM rather than to a call-site list: a fourth wrapper cannot re-open
    /// the gap by falling outside an enumeration. Its condition-2 answer
    /// carries no epoch and no wake.
    pub(super) fn enrollment(response: EnrollmentResponse) -> Self {
        Self {
            value: Box::new(response.into_server_value()),
        }
    }

    /// Moves the presented value out for the arm's ordinary response path.
    pub(super) fn into_server_value(self) -> ServerValue {
        *self.value
    }
}
