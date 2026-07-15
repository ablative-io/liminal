use crate::wire::{
    AttachSecret, ConversationId, DeliverySeq, Generation, LeaveAttemptToken, LeaveCommitted,
    ParticipantId,
};

macro_rules! fingerprint_type {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub struct $name([u8; 32]);

        impl $name {
            /// Creates a fixed-size canonical fingerprint.
            #[must_use]
            pub const fn new(bytes: [u8; 32]) -> Self {
                Self(bytes)
            }

            /// Returns the canonical fingerprint bytes.
            #[must_use]
            pub const fn into_bytes(self) -> [u8; 32] {
                self.0
            }
        }
    };
}

fingerprint_type!(
    /// Permanent enrollment-token fingerprint retained by the tombstone.
    EnrollmentFingerprint
);
fingerprint_type!(
    /// Permanent canonical Leave-request fingerprint retained by the tombstone.
    LeaveFingerprint
);

/// Live participant membership, independent of current binding state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LiveMember {
    /// Permanent participant identity.
    pub participant_id: ParticipantId,
    /// Conversation membership.
    pub conversation_id: ConversationId,
    /// Current credential generation.
    pub generation: Generation,
    /// Current attach secret.
    pub attach_secret: AttachSecret,
    /// Durable cumulative participant cursor.
    pub cursor: DeliverySeq,
}

/// Permanent retired identity tombstone.
///
/// The tombstone deliberately retains no attach secret or request body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetiredIdentity<V> {
    participant_id: ParticipantId,
    conversation_id: ConversationId,
    retired_generation: Generation,
    enrollment_fingerprint: EnrollmentFingerprint,
    leave_attempt_token: LeaveAttemptToken,
    leave_request_verifier: V,
    leave_fingerprint: LeaveFingerprint,
    committed_result: LeaveCommitted,
}

impl<V> RetiredIdentity<V> {
    /// Permanent participant id.
    #[must_use]
    pub const fn participant_id(&self) -> ParticipantId {
        self.participant_id
    }

    /// Conversation containing the tombstone.
    #[must_use]
    pub const fn conversation_id(&self) -> ConversationId {
        self.conversation_id
    }

    /// Permanent retired generation.
    #[must_use]
    pub const fn retired_generation(&self) -> Generation {
        self.retired_generation
    }

    /// Permanent committed Leave token.
    #[must_use]
    pub const fn leave_attempt_token(&self) -> LeaveAttemptToken {
        self.leave_attempt_token
    }

    /// Stored complete Leave result for exact replay.
    #[must_use]
    pub const fn committed_result(&self) -> &LeaveCommitted {
        &self.committed_result
    }

    /// Stored non-reversible secret-proof verifier.
    #[must_use]
    pub const fn leave_request_verifier(&self) -> &V {
        &self.leave_request_verifier
    }

    /// Permanent enrollment mapping fingerprint.
    #[must_use]
    pub const fn enrollment_fingerprint(&self) -> EnrollmentFingerprint {
        self.enrollment_fingerprint
    }

    /// Permanent canonical Leave fingerprint.
    #[must_use]
    pub const fn leave_fingerprint(&self) -> LeaveFingerprint {
        self.leave_fingerprint
    }
}

/// Present participant identity state; absence is represented outside this enum.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IdentityState<V> {
    /// Live membership, whether bound or detached.
    Live(LiveMember),
    /// Permanent Leave tombstone.
    Retired(RetiredIdentity<V>),
}

/// Mismatch between a live member and proposed stored Leave result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetirementError {
    /// Result names another conversation.
    Conversation,
    /// Result names another participant.
    Participant,
    /// Result's presented generation differs from current generation.
    Generation,
    /// Result's Leave token differs from the committing token.
    Token,
}

impl LiveMember {
    /// Consumes live membership into a permanent tombstone.
    ///
    /// # Errors
    ///
    /// Returns [`RetirementError`] when the complete stored result does not
    /// describe this member and committing token.
    pub fn retire<V>(
        self,
        enrollment_fingerprint: EnrollmentFingerprint,
        leave_attempt_token: LeaveAttemptToken,
        leave_request_verifier: V,
        leave_fingerprint: LeaveFingerprint,
        committed_result: LeaveCommitted,
    ) -> Result<RetiredIdentity<V>, RetirementError> {
        if committed_result.conversation_id != self.conversation_id {
            return Err(RetirementError::Conversation);
        }
        if committed_result.participant_id != self.participant_id {
            return Err(RetirementError::Participant);
        }
        if committed_result.presented_generation != self.generation {
            return Err(RetirementError::Generation);
        }
        if committed_result.leave_attempt_token != leave_attempt_token {
            return Err(RetirementError::Token);
        }

        Ok(RetiredIdentity {
            participant_id: self.participant_id,
            conversation_id: self.conversation_id,
            retired_generation: committed_result.retired_generation,
            enrollment_fingerprint,
            leave_attempt_token,
            leave_request_verifier,
            leave_fingerprint,
            committed_result,
        })
    }
}
