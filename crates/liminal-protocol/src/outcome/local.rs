use crate::wire::{ConversationId, Generation, ParticipantId};

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

/// Reconnect lifecycle state reported without exposing permit identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReconnectState {
    /// No authorization or attempt is outstanding.
    Parked,
    /// One fresh-event permit is outstanding.
    PermitOutstanding,
    /// One real connection attempt is in progress.
    AttemptInProgress,
    /// The connection is proved online.
    Online,
}

/// Fresh event class required to mint one reconnect permit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReconnectRequiredEvent {
    /// Established-connection transport fate.
    TransportFate,
    /// Proved online transition.
    OnlineTransition,
    /// Explicit caller action.
    ExplicitCallerAction,
}

/// Exact SDK-local reconnect result, retained under its stable name.
///
/// The former delay field is deliberately absent: fresh events authorize one
/// real attempt and never a timer arm.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReconnectDelayResult {
    /// A fresh single-use event permit was minted.
    ReconnectArmed {
        /// Exact fresh event class that minted the permit.
        event: ReconnectRequiredEvent,
    },
    /// No fresh event permit was minted.
    ReconnectNotArmed {
        /// Current reconnect state.
        state: ReconnectState,
        /// Event class required for a future authorization.
        required_event: ReconnectRequiredEvent,
    },
}
