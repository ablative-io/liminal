use alloc::vec::Vec;

use super::{
    BindingEpoch, CloseCause, ConversationId, DeliverySeq, ObserverEpoch, ParticipantId,
    PushDiscriminant, RecordKind,
};

/// Causes valid only for a `Detached` lifecycle record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DetachedCause {
    /// Clean deregistration.
    CleanDeregister,
    /// Binding supersession.
    Superseded,
    /// Orderly server shutdown.
    ServerShutdown,
}

impl DetachedCause {
    /// Converts to the shared close-cause registry without permitting Died-only causes.
    #[must_use]
    pub const fn close_cause(self) -> CloseCause {
        match self {
            Self::CleanDeregister => CloseCause::CleanDeregister,
            Self::Superseded => CloseCause::Superseded,
            Self::ServerShutdown => CloseCause::ServerShutdown,
        }
    }
}

/// Causes valid only for a `Died` lifecycle record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiedCause {
    /// Transport connection was lost.
    ConnectionLost,
    /// Participant process was killed.
    ProcessKilled,
    /// Participant protocol error.
    ProtocolError,
    /// Binding was recovered after an unclean restart.
    UncleanServerRestart {
        /// Server incarnation that previously owned the binding.
        prior_server_incarnation: u64,
    },
}

impl DiedCause {
    /// Converts to the shared close-cause registry without permitting Detached-only causes.
    #[must_use]
    pub const fn close_cause(self) -> CloseCause {
        match self {
            Self::ConnectionLost => CloseCause::ConnectionLost,
            Self::ProcessKilled => CloseCause::ProcessKilled,
            Self::ProtocolError => CloseCause::ProtocolError,
            Self::UncleanServerRestart {
                prior_server_incarnation,
            } => CloseCause::UncleanServerRestart {
                prior_server_incarnation,
            },
        }
    }
}

/// Exact record-kind body carried by `ParticipantDelivery`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParticipantRecord {
    /// Ordinary application record.
    OrdinaryRecord {
        /// Verified sender participant.
        sender_participant_id: ParticipantId,
        /// Opaque application payload.
        payload: Vec<u8>,
    },
    /// Participant binding was attached.
    Attached {
        /// Affected participant.
        affected_participant_id: ParticipantId,
        /// New binding epoch.
        binding_epoch: BindingEpoch,
    },
    /// Binding ended with a Detached-class cause.
    Detached {
        /// Affected participant.
        affected_participant_id: ParticipantId,
        /// Ended binding epoch.
        binding_epoch: BindingEpoch,
        /// Type-restricted Detached cause.
        cause: DetachedCause,
    },
    /// Binding ended with a Died-class cause.
    Died {
        /// Affected participant.
        affected_participant_id: ParticipantId,
        /// Ended binding epoch.
        binding_epoch: BindingEpoch,
        /// Type-restricted Died cause.
        cause: DiedCause,
    },
    /// Participant permanently left.
    Left {
        /// Affected participant.
        affected_participant_id: ParticipantId,
        /// Binding ended by the same Leave commit, if any.
        ended_binding_epoch: Option<BindingEpoch>,
    },
    /// Retained history was explicitly abandoned and compacted.
    HistoryCompacted {
        /// Affected participant.
        affected_participant_id: ParticipantId,
        /// Last sequence known delivered before abandonment.
        abandoned_after: DeliverySeq,
        /// Last abandoned sequence.
        abandoned_through: DeliverySeq,
        /// Physical floor selected by the compaction decision.
        physical_floor_at_decision: DeliverySeq,
    },
}

impl ParticipantRecord {
    /// Returns the explicit record-kind selector.
    #[must_use]
    pub const fn record_kind(&self) -> RecordKind {
        match self {
            Self::OrdinaryRecord { .. } => RecordKind::OrdinaryRecord,
            Self::Attached { .. } => RecordKind::Attached,
            Self::Detached { .. } => RecordKind::Detached,
            Self::Died { .. } => RecordKind::Died,
            Self::Left { .. } => RecordKind::Left,
            Self::HistoryCompacted { .. } => RecordKind::HistoryCompacted,
        }
    }
}

/// Complete participant delivery push body (`0x0201`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParticipantDelivery {
    /// Conversation multiplexing key.
    pub conversation_id: ConversationId,
    /// Delivered record sequence.
    pub delivery_seq: DeliverySeq,
    /// Exact tagged record body.
    pub record: ParticipantRecord,
}

/// Exhaustive pushed participant control/value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServerPush {
    /// Observer progress wake (`0x0200`).
    ObserverProgressed {
        /// Conversation whose observer advanced.
        conversation_id: ConversationId,
        /// Refusal epoch the progress may wake.
        refused_epoch: ObserverEpoch,
        /// Current observer progress.
        observer_progress: DeliverySeq,
    },
    /// Participant record delivery (`0x0201`).
    ParticipantDelivery(ParticipantDelivery),
}

impl ServerPush {
    /// Returns the stable push discriminant.
    #[must_use]
    pub const fn discriminant(&self) -> PushDiscriminant {
        match self {
            Self::ObserverProgressed { .. } => PushDiscriminant::ObserverProgressed,
            Self::ParticipantDelivery(_) => PushDiscriminant::ParticipantDelivery,
        }
    }
}
