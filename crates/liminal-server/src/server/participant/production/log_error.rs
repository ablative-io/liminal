//! Typed semantic refusals produced while decoding participant log rows.

/// Exact typed reason a closed Attached-v3 mode or fenced proof is refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub(super) enum FencedAttachProofRefusal {
    #[error("Ordinary mode carries a request marker")]
    OrdinaryRequestMarker,
    #[error("Superseding mode carries a request marker")]
    SupersedingRequestMarker,
    #[error("Superseding terminal order differs from the Attached order")]
    SupersedingTerminalOrder,
    #[error("request marker differs from the fenced mode marker")]
    RequestMarkerMismatch,
    #[error("DetachedCredentialRecovery bytes are malformed")]
    DetachedCredentialRecoveryMalformed,
    #[error("DetachedCredentialRecovery bytes are not canonical")]
    DetachedCredentialRecoveryNonCanonical,
    #[error("predecessor debt bytes are malformed")]
    PredecessorDebtMalformed,
    #[error("predecessor debt bytes are not canonical")]
    PredecessorDebtNonCanonical,
    #[error("predecessor debt is zero")]
    PredecessorDebtZero,
    #[error("restricted successor bytes are malformed")]
    SuccessorMalformed,
    #[error("restricted successor bytes are not canonical")]
    SuccessorNonCanonical,
    #[error("restricted successor debt is zero")]
    SuccessorDebtZero,
    #[error("physical-compaction successor range is inverted")]
    SuccessorCompactionRange,
    #[error("recovery conversation differs from the Attached request")]
    RecoveryConversationMismatch,
    #[error("recovery participant differs from the Attached request")]
    RecoveryParticipantMismatch,
    #[error("recovery marker differs from the fenced mode")]
    RecoveryMarkerMismatch,
    #[error("recovery prior epoch differs from the fenced mode")]
    RecoveryPriorEpochMismatch,
    #[error("new binding generation is not immediately after the prior epoch")]
    NewBindingGenerationMismatch,
    #[error("cursor-progress conversation differs from recovery")]
    ProgressConversationMismatch,
    #[error("cursor-progress participant differs from recovery")]
    ProgressParticipantMismatch,
    #[error("cursor-progress epoch differs from recovery")]
    ProgressEpochMismatch,
    #[error("cursor-progress marker differs from recovery")]
    ProgressMarkerMismatch,
    #[error("cursor-progress through boundary differs from its marker")]
    ProgressThroughMismatch,
    #[error("marker-delivery participant differs from cursor progress")]
    DeliveryParticipantMismatch,
    #[error("marker-delivery epoch differs from cursor progress")]
    DeliveryEpochMismatch,
    #[error("marker-delivery sequence differs from cursor progress")]
    DeliveryMarkerMismatch,
    #[error("terminal conversation differs from recovery")]
    TerminalConversationMismatch,
    #[error("terminal participant differs from recovery")]
    TerminalParticipantMismatch,
    #[error("terminal epoch differs from recovery")]
    TerminalEpochMismatch,
    #[error("composed terminal kind and cause class disagree")]
    ComposedTerminalKindCause,
    #[error("composed terminal order differs from the enclosing Attached order")]
    ComposedTerminalOrder,
    #[error("composed terminal pending source does not precede Attached")]
    ComposedPendingSourceOrder,
    #[error("recovered reservation source does not precede Attached")]
    ComposedRecoveredSourceOrder,
    #[error("only Pending Died may consume a recovered finalizer reservation")]
    ComposedRecoveredReservationKind,
    #[error("composed terminal pending source row disagrees with its audit")]
    ComposedPendingSourceMismatch,
    #[error("composed terminal recovered reservation row disagrees with its audit")]
    ComposedRecoveredReservationMismatch,
    #[error("composed terminal disagrees with replay binding prestate")]
    ComposedReplayStateMismatch,
}
