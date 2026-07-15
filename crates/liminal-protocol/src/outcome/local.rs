use crate::wire::{AttachBound, ConversationId, DetachEnvelope, Generation, ParticipantId};

/// Complete SDK-local participant request exceeded `min(R, WF)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SdkParticipantRequestTooLarge {
    /// Conversation named by the unsent request.
    pub conversation_id: ConversationId,
    /// Complete encoded participant request bytes.
    pub encoded_bytes: u64,
    /// Exact signed effective request limit `R_send`.
    pub limit: u64,
}

/// Closed five-way SDK parking-capacity outcome.
///
/// Encoding the scope/dimension pair in the enum makes the forbidden
/// `PerConversation/Conversations` combination unconstructible.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SdkObserverParkCapacityExceeded {
    /// Per-conversation row count exceeded `N`.
    PerConversationRows {
        /// Conversation receiving the row.
        conversation_id: ConversationId,
        /// Signed per-conversation row limit.
        limit: u64,
        /// Current rows in the conversation.
        occupied: u64,
        /// Full requested row count.
        requested: u64,
    },
    /// Per-conversation charged bytes exceeded `C`.
    PerConversationBytes {
        /// Conversation receiving the row.
        conversation_id: ConversationId,
        /// Signed per-conversation byte limit.
        limit: u64,
        /// Current full-row bytes in the conversation.
        occupied: u64,
        /// Exact full-row bytes requested.
        requested: u64,
    },
    /// SDK-wide parked-conversation count exceeded `P`.
    SdkWideConversations {
        /// Conversation requiring its first parked slot.
        conversation_id: ConversationId,
        /// Signed SDK conversation limit.
        limit: u64,
        /// Current parked-conversation count.
        occupied: u64,
        /// Requested conversations; v1 requests one.
        requested: u64,
    },
    /// SDK-wide parked-row count exceeded `G`.
    SdkWideRows {
        /// Conversation receiving the row.
        conversation_id: ConversationId,
        /// Signed SDK row limit.
        limit: u64,
        /// Current SDK row count.
        occupied: u64,
        /// Full requested row count.
        requested: u64,
    },
    /// SDK-wide parked bytes exceeded `D`.
    SdkWideBytes {
        /// Conversation receiving the row.
        conversation_id: ConversationId,
        /// Signed SDK byte limit.
        limit: u64,
        /// Current SDK full-row bytes.
        occupied: u64,
        /// Exact full-row bytes requested.
        requested: u64,
    },
}

/// Singleton local counter selector.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParkOrderCounter {
    /// Per-conversation durable park order.
    ParkOrder,
}

/// Nonempty parked set exhausted its checked park-order counter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SdkParkOrderExhausted {
    conversation_id: ConversationId,
}

impl SdkParkOrderExhausted {
    /// Constructs the only legal exhausted-counter payload.
    #[must_use]
    pub const fn new(conversation_id: ConversationId) -> Self {
        Self { conversation_id }
    }

    /// Conversation whose nonempty set exhausted.
    #[must_use]
    pub const fn conversation_id(self) -> ConversationId {
        self.conversation_id
    }

    /// Returns the fixed counter selector.
    #[must_use]
    pub const fn counter(self) -> ParkOrderCounter {
        let _ = self;
        ParkOrderCounter::ParkOrder
    }

    /// Returns the terminal maximum counter value.
    #[must_use]
    pub const fn value(self) -> u64 {
        let _ = self;
        u64::MAX
    }
}

/// Singleton ambiguous local operation selector.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordAdmissionOperation {
    /// Untokenized ordinary record admission.
    OrdinaryRecordAdmission,
}

/// SDK-local terminal ambiguity after an ordinary record response is lost.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordAdmissionUnknown {
    /// Conversation from the lost request.
    pub conversation_id: ConversationId,
    /// Participant from the lost request.
    pub participant_id: ParticipantId,
    /// Presented generation from the lost request.
    pub capability_generation: Generation,
    /// Operation is exactly ordinary record admission.
    pub operation: RecordAdmissionOperation,
    /// Durable SDK row order deleted by this transition.
    pub park_order: u64,
}

/// SDK terminal state after a credential-rotation result becomes unrecoverable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CredentialRecoveryLost {
    /// Conversation whose credential continuity was lost.
    pub conversation_id: ConversationId,
    /// Preserved permanent participant identity.
    pub participant_id: ParticipantId,
    /// Last credential generation durably known by the SDK.
    pub last_known_generation: Generation,
}

/// SDK authority for replaying a detach whose response may have been lost.
///
/// Receiving a newer matching [`AttachBound`] consumes this authority through
/// [`Self::supersede`]; the old detach token must never be resent afterward.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SdkDetachReplayAuthority {
    request: DetachEnvelope,
}

impl SdkDetachReplayAuthority {
    /// Creates replay authority from the exact write-ahead detach request.
    #[must_use]
    pub const fn new(request: DetachEnvelope) -> Self {
        Self { request }
    }

    /// Exact detach request whose unknown result remains replayable.
    #[must_use]
    pub const fn request(&self) -> &DetachEnvelope {
        &self.request
    }

    /// Terminalizes this replay authority after a newer matching attach.
    ///
    /// The active authority is returned unchanged when the attach belongs to a
    /// different conversation or participant, or its result generation is not
    /// newer than the detach generation.
    ///
    /// # Errors
    ///
    /// Returns this unchanged authority when `attach` is not a matching newer
    /// result for the same conversation and participant.
    pub fn supersede(self, attach: &AttachBound) -> Result<AuthoritySuperseded, Self> {
        if attach.conversation_id() == self.request.conversation_id
            && attach.participant_id() == self.request.participant_id
            && attach.capability_generation() > self.request.capability_generation
        {
            Ok(AuthoritySuperseded { _sealed: () })
        } else {
            Err(self)
        }
    }
}

/// Terminal SDK state for an old detach token superseded by a newer attach.
///
/// This no-payload marker is constructible only by consuming
/// [`SdkDetachReplayAuthority`] with a matching newer [`AttachBound`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AuthoritySuperseded {
    _sealed: (),
}

/// Reconnect lifecycle state required by the no-permit result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReconnectState {
    /// Connection lifecycle is already reconnecting.
    Reconnecting,
}

/// Event required to mint one reconnect permit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReconnectRequiredEvent {
    /// A fresh transport fate must occur.
    TransportFate,
}

/// Exact SDK-local reconnect-delay result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReconnectDelayResult {
    /// A fresh single-use permit was consumed.
    ReconnectArmed {
        /// Pure bounded reconnect delay.
        delay_ms: u64,
    },
    /// No fresh transport-fate permit exists.
    ReconnectNotArmed {
        /// Current reconnect state.
        state: ReconnectState,
        /// Event required before another arm.
        required_event: ReconnectRequiredEvent,
    },
}
