//! The operator WRITE surface for authorized credential re-issue (R18
//! amendment A7, `PARTICIPANT-CONTRACT.md` §0.18).
//!
//! `OperatorCredentialReissue` is an operation of the operator surface — the
//! same trust plane that serves `GET /unloadable-conversations` — and never of
//! the participant wire. The participant protocol is byte-identical under A7:
//! nothing here adds a wire frame, a response variant, or a protocol-crate
//! delta.
//!
//! This module holds the operation's SHAPE and its plumbing. Like its
//! read-surface sibling it is PULL-ONLY in the scheduling sense: the operation
//! runs exactly when an operator calls it, and nothing here starts a thread,
//! arms a timer, samples a clock, or sweeps anything. The endpoint's
//! zero-idle-wake property (W4 leg 2, LAW-1) is untouched — a node nobody
//! calls does no work at all for this surface.
//!
//! # The secret transits exactly once
//!
//! [`OperatorCredentialReissued::attach_secret`] is the ONE delivery of the
//! minted credential (§0.18 item 4). There is deliberately no receipt replay
//! for operator issue: R-C0's receipt machinery is untouched by A7, so a lost
//! response is repaired by repeating the operation, and the
//! [`OperatorCredentialReissueRefusal::GenerationMismatch`] payload is what
//! tells an operator who lost the response what the post-rotation generation
//! is. That payload is NORMATIVE, not a courtesy.

use std::fmt::Write as _;
use std::sync::{Arc, Mutex, PoisonError};

use liminal_protocol::wire::{ConversationId, ParticipantId};

/// The complete input of one `OperatorCredentialReissue` (§0.18 item 1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OperatorCredentialReissueRequest {
    /// Conversation holding the identity to re-issue.
    pub conversation_id: ConversationId,
    /// Permanent participant index within that conversation.
    pub participant_id: ParticipantId,
    /// Generation the operator believes is current — the compare-and-set that
    /// makes concurrent operator repetitions serialize instead of
    /// double-rotating.
    pub expected_current_generation: u64,
}

/// The committed result of one re-issue, carrying its sole secret delivery.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct OperatorCredentialReissued {
    /// Conversation the re-issue committed in.
    pub conversation_id: ConversationId,
    /// Identity whose credential was re-issued.
    pub participant_id: ParticipantId,
    /// Generation the compare-and-set matched (G).
    pub presented_generation: u64,
    /// Generation this re-issue minted (G+1).
    pub issued_generation: u64,
    /// The minted attach secret, lowercase hex. Returned EXACTLY ONCE: nothing
    /// replays it, and no receipt row holds it.
    pub attach_secret: String,
}

/// Every typed refusal `OperatorCredentialReissue` can answer (§0.18 item 2).
///
/// Each variant commits no receipt, order, cursor, binding, lifecycle record,
/// or retention mutation, and each is a NAMED refusal rather than whatever the
/// code happens to do.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "refusal", rename_all = "snake_case")]
pub enum OperatorCredentialReissueRefusal {
    /// Pre-guard lookup miss: no durable conversation resolves this id.
    ConversationUnknown {
        /// The presented conversation id, and nothing beyond it.
        conversation_id: ConversationId,
    },
    /// Pre-guard lookup miss: the conversation exists, the identity does not.
    ParticipantUnknown {
        /// The presented conversation id.
        conversation_id: ConversationId,
        /// The presented participant id, and nothing beyond it.
        participant_id: ParticipantId,
    },
    /// Guard (a): a tombstoned identity. Re-issue never remints a retired one.
    Retired {
        /// The presented conversation id.
        conversation_id: ConversationId,
        /// The retired identity.
        participant_id: ParticipantId,
        /// Generation the identity held when it was retired.
        retired_generation: u64,
    },
    /// Guard (b): a live binding. A bound member is demonstrably operating
    /// under working authority, and re-issue against it would be seat
    /// revocation — v1 has no operator Leave and A7 does not create one.
    LiveBinding {
        /// The presented conversation id.
        conversation_id: ConversationId,
        /// The bound identity.
        participant_id: ParticipantId,
        /// Current credential generation.
        current_generation: u64,
        /// Which live binding state refused: `bound` or `pending_finalization`.
        binding_state: &'static str,
    },
    /// ⚠ NOT one of §0.18's four guards — a defect this build MEASURED, named
    /// rather than absorbed, and returned to the seat as a contract flag.
    ///
    /// The identity's last committed detach still holds its exact-replay cell
    /// open. `commit_attach` requires that cell's request generation to equal
    /// the member's current generation (`lifecycle::attach.rs`,
    /// `transition_detach_cell`'s `DetachCell::Committed` arm), and a re-issue
    /// moves the generation while the cell stays where it is. So a re-issue
    /// against this shape would mint a lawful-looking credential that the
    /// ORDINARY attach path of §0.18 item 5 then refuses with a bare
    /// `AttachCommitError::DetachCellAuthority` invariant — an unattachable
    /// credential, which is the silent trap this estate refuses to ship.
    ///
    /// Refusing is the only answer available inside this lane's authority:
    /// terminalizing the cell here would change what an exact detach-token
    /// replay is answered with, which is existing refusal/restoration
    /// semantics and not this lane's to move.
    DetachReplayOpen {
        /// The presented conversation id.
        conversation_id: ConversationId,
        /// The identity holding the open replay cell.
        participant_id: ParticipantId,
        /// Current credential generation.
        current_generation: u64,
    },
    /// Guard (c): a live attach or enrollment receipt. A live receipt means the
    /// R-C0 recovery window is still open and the ordinary recovery path must
    /// be exhausted first.
    LiveReceipt {
        /// The presented conversation id.
        conversation_id: ConversationId,
        /// The identity holding the live receipt.
        participant_id: ParticipantId,
        /// Current credential generation.
        current_generation: u64,
        /// Which receipt is still live: `attach` or `enrollment`.
        receipt: &'static str,
    },
    /// Guard (d): the compare-and-set failed.
    ///
    /// ⛔ This payload is NORMATIVE (§0.18 item 4). The presented/current pair
    /// is the ONLY way an operator who lost a re-issue response learns the
    /// post-rotation generation, so a future edit that minimizes it silently
    /// breaks lost-response recovery.
    GenerationMismatch {
        /// The presented conversation id.
        conversation_id: ConversationId,
        /// The identity.
        participant_id: ParticipantId,
        /// Generation the operator presented.
        presented_generation: u64,
        /// Generation the identity actually holds.
        current_generation: u64,
    },
}

/// The complete answer of one `OperatorCredentialReissue`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OperatorCredentialReissueOutcome {
    /// One atomic durable commit happened; the secret is inside, once.
    Issued(OperatorCredentialReissued),
    /// A typed, provably mutation-free refusal.
    Refused(OperatorCredentialReissueRefusal),
}

/// A re-issue that could not be decided at all (service fatal, unloadable
/// conversation, durable failure).
///
/// Deliberately distinct from [`OperatorCredentialReissueRefusal`]: a refusal
/// is a decided answer with a proven-empty state delta, while this is the
/// absence of an answer.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("operator credential re-issue could not be decided: {message}")]
pub struct OperatorCredentialReissueError {
    /// Diagnostic text for the operator.
    pub message: String,
}

/// The one serialized participant-state authority, as this surface sees it.
pub trait OperatorCredentialReissuer: std::fmt::Debug + Send + Sync {
    /// Runs one re-issue at the serialized participant-state point.
    ///
    /// # Errors
    ///
    /// Returns [`OperatorCredentialReissueError`] when the operation could not
    /// be decided. Every DECIDED refusal is an `Ok`
    /// [`OperatorCredentialReissueOutcome::Refused`].
    fn reissue(
        &self,
        request: OperatorCredentialReissueRequest,
    ) -> Result<OperatorCredentialReissueOutcome, OperatorCredentialReissueError>;
}

/// The endpoint's slot for the participant authority.
///
/// The health server binds BEFORE the participant handler exists, exactly as
/// it does for the refused-load record, so the authority is published into
/// this slot once built. Until then the route answers "no participant is
/// installed" rather than pretending an identity is unknown.
#[derive(Clone, Debug, Default)]
pub struct SharedOperatorCredentialReissue {
    reissuer: Arc<Mutex<Option<Arc<dyn OperatorCredentialReissuer>>>>,
}

impl SharedOperatorCredentialReissue {
    /// Publishes the participant authority into the surface.
    pub fn install(&self, reissuer: Arc<dyn OperatorCredentialReissuer>) {
        *self.reissuer.lock().unwrap_or_else(PoisonError::into_inner) = Some(reissuer);
    }

    /// Whether a participant authority is installed at all.
    #[must_use]
    pub fn participant_installed(&self) -> bool {
        self.reissuer
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
    }

    /// Runs one re-issue, or reports that no participant is installed.
    ///
    /// # Errors
    ///
    /// Returns [`OperatorCredentialReissueError`] when the operation could not
    /// be decided.
    pub fn reissue(
        &self,
        request: OperatorCredentialReissueRequest,
    ) -> Result<Option<OperatorCredentialReissueOutcome>, OperatorCredentialReissueError> {
        let reissuer = self
            .reissuer
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        reissuer
            .map(|reissuer| reissuer.reissue(request))
            .transpose()
    }
}

/// Lowercase hex of the minted secret.
///
/// Local rather than a dependency: the server takes no new crate for thirty-two
/// bytes, and this is the only place a secret is ever rendered.
#[must_use]
pub fn encode_hex(bytes: &[u8; 32]) -> String {
    let mut rendered = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        // Writing into a `String` is infallible; the result is discarded
        // deliberately rather than unwrapped.
        let _ = write!(rendered, "{byte:02x}");
    }
    rendered
}

#[cfg(test)]
mod tests {
    use super::{
        OperatorCredentialReissueError, OperatorCredentialReissueOutcome,
        OperatorCredentialReissueRefusal, OperatorCredentialReissueRequest,
        OperatorCredentialReissued, OperatorCredentialReissuer, SharedOperatorCredentialReissue,
        encode_hex,
    };
    use std::sync::Arc;

    #[derive(Debug)]
    struct FixedReissuer(OperatorCredentialReissueOutcome);

    impl OperatorCredentialReissuer for FixedReissuer {
        fn reissue(
            &self,
            _request: OperatorCredentialReissueRequest,
        ) -> Result<OperatorCredentialReissueOutcome, OperatorCredentialReissueError> {
            Ok(self.0.clone())
        }
    }

    fn request() -> OperatorCredentialReissueRequest {
        OperatorCredentialReissueRequest {
            conversation_id: 7,
            participant_id: 3,
            expected_current_generation: 14,
        }
    }

    /// An uninstalled surface must not answer like a node whose identity is
    /// unknown: it says it is looking at nothing.
    #[test]
    fn an_uninstalled_surface_reports_that_no_participant_is_installed() {
        let surface = SharedOperatorCredentialReissue::default();

        assert!(!surface.participant_installed());
        assert_eq!(surface.reissue(request()), Ok(None));
    }

    /// An installed authority answers, and the answer travels unchanged.
    #[test]
    fn an_installed_authority_answers_the_surface() {
        let issued = OperatorCredentialReissued {
            conversation_id: 7,
            participant_id: 3,
            presented_generation: 14,
            issued_generation: 15,
            attach_secret: encode_hex(&[0xAB; 32]),
        };
        let surface = SharedOperatorCredentialReissue::default();
        surface.install(Arc::new(FixedReissuer(
            OperatorCredentialReissueOutcome::Issued(issued.clone()),
        )));

        assert!(surface.participant_installed());
        assert_eq!(
            surface.reissue(request()),
            Ok(Some(OperatorCredentialReissueOutcome::Issued(issued)))
        );
    }

    /// The normative CAS payload survives serialization with BOTH generations.
    /// §0.18 item 4: an operator who lost the response learns the post-rotation
    /// generation from exactly this row and nowhere else.
    #[test]
    fn the_generation_mismatch_refusal_serializes_both_generations() -> Result<(), serde_json::Error>
    {
        let refusal = OperatorCredentialReissueRefusal::GenerationMismatch {
            conversation_id: 7,
            participant_id: 3,
            presented_generation: 14,
            current_generation: 15,
        };

        let rendered = serde_json::to_value(&refusal)?;

        assert_eq!(rendered["refusal"], "generation_mismatch");
        assert_eq!(rendered["presented_generation"], 14);
        assert_eq!(rendered["current_generation"], 15);
        Ok(())
    }

    /// Hex is lowercase, fixed width, and covers every byte.
    #[test]
    fn hex_rendering_is_lowercase_and_fixed_width() {
        let mut bytes = [0_u8; 32];
        bytes[0] = 0x00;
        bytes[1] = 0x0F;
        bytes[31] = 0xFF;

        let rendered = encode_hex(&bytes);

        assert_eq!(rendered.len(), 64);
        assert!(rendered.starts_with("000f"));
        assert!(rendered.ends_with("ff"));
    }
}
