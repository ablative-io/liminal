#![allow(clippy::unwrap_used)]

use core::fmt::Debug;

use crate::algebra::ResourceDimension;

use super::{
    AckGapReason, AckRegressionReason, AttemptConflict, AttemptOperation, BindingStateTag,
    ClientDiscriminant, CloseCause, CloseCauseTag, ClosureScope, Counter, DecodeClass,
    DetachAuthorityStateTag, IdentityCapacityScope, InvalidObserverEpochListReason,
    InvalidObserverEpochReason, LeaveAuthorityStateTag, MarkerMismatchReason,
    MarkerNotDeliveredReason, PushDiscriminant, ReceiptCapacityScope, ReceiptExpiryReason,
    RecordKind, RepaymentEdgeTag, ResourceDimensionTag, ServerDiscriminant, TagError,
    TransportReasonTag,
};

fn assert_tag<T>(tag: T, expected: u16)
where
    T: Copy + Debug + PartialEq + Into<u16> + TryFrom<u16, Error = TagError>,
{
    assert_eq!(tag.into(), expected);
    assert_eq!(T::try_from(expected), Ok(tag));
}

#[test]
fn client_registry_is_exactly_eight_contiguous_values() {
    let values = [
        ClientDiscriminant::EnrollmentRequest,
        ClientDiscriminant::CredentialAttachRequest,
        ClientDiscriminant::DetachRequest,
        ClientDiscriminant::ParticipantAck,
        ClientDiscriminant::LeaveRequest,
        ClientDiscriminant::MarkerAck,
        ClientDiscriminant::RecordAdmission,
        ClientDiscriminant::ObserverRecoveryHandshake,
    ];

    for (offset, value) in values.into_iter().enumerate() {
        assert_eq!(value.wire_value(), u16::try_from(offset + 1).unwrap());
    }
}

#[test]
fn server_registry_is_exactly_thirty_seven_contiguous_values() {
    for value in 0x0100..=0x0124 {
        let tag = ServerDiscriminant::try_from(value).unwrap();
        assert_eq!(tag.wire_value(), value);
    }
    assert!(ServerDiscriminant::try_from(0x00FF).is_err());
    assert!(ServerDiscriminant::try_from(0x0125).is_err());
}

#[test]
fn pushed_and_record_registries_preserve_their_explicit_bases() {
    assert_tag(PushDiscriminant::ObserverProgressed, 0x0200);
    assert_tag(PushDiscriminant::ParticipantDelivery, 0x0201);
    assert_tag(RecordKind::OrdinaryRecord, 0x0000);
    assert_tag(RecordKind::Attached, 0x0001);
    assert_tag(RecordKind::Detached, 0x0002);
    assert_tag(RecordKind::Died, 0x0003);
    assert_tag(RecordKind::Left, 0x0004);
    assert_tag(RecordKind::HistoryCompacted, 0x0005);
}

#[test]
fn close_cause_payload_does_not_change_its_tag() {
    assert_eq!(
        CloseCause::UncleanServerRestart {
            prior_server_incarnation: 41,
        }
        .tag(),
        CloseCauseTag::UncleanServerRestart
    );
}

#[test]
fn close_cause_registry_is_exact() {
    assert_tag(CloseCauseTag::CleanDeregister, 1);
    assert_tag(CloseCauseTag::ConnectionLost, 2);
    assert_tag(CloseCauseTag::ProcessKilled, 3);
    assert_tag(CloseCauseTag::ProtocolError, 4);
    assert_tag(CloseCauseTag::Superseded, 5);
    assert_tag(CloseCauseTag::ServerShutdown, 6);
    assert_tag(CloseCauseTag::UncleanServerRestart, 7);
}

#[test]
fn transport_and_decode_registries_are_exact() {
    assert_tag(TransportReasonTag::FrameTooLarge, 1);
    assert_tag(TransportReasonTag::DecodeFailed, 2);
    assert_tag(TransportReasonTag::UnsupportedVersion, 3);
    assert_tag(TransportReasonTag::AuthenticationFailed, 4);
    assert_tag(TransportReasonTag::ParticipantCapabilityRequired, 5);

    assert_tag(DecodeClass::Framing, 1);
    assert_tag(DecodeClass::UnknownDiscriminant, 2);
    assert_tag(DecodeClass::CanonicalEncoding, 3);
    assert_tag(DecodeClass::MissingRequiredField, 4);
    assert_tag(DecodeClass::InvalidField, 5);
}

#[test]
fn attempt_and_authority_registries_are_origin_specific() {
    assert_tag(AttemptOperation::CredentialAttachRequest, 1);
    assert_tag(AttemptOperation::LeaveRequest, 2);
    assert_tag(AttemptConflict::Generation, 1);
    assert_tag(AttemptConflict::MarkerDeliverySequence, 2);

    assert_tag(DetachAuthorityStateTag::Live, 1);
    assert_tag(DetachAuthorityStateTag::TerminalizedDetachCell, 2);
    assert_tag(LeaveAuthorityStateTag::Live, 1);
    assert_tag(LeaveAuthorityStateTag::CommittedLeaveTombstone, 2);
    assert_tag(BindingStateTag::Bound, 1);
    assert_tag(BindingStateTag::Detached, 2);
}

#[test]
fn capacity_and_edge_registries_are_exact() {
    assert_tag(ReceiptCapacityScope::LiveReceiptServer, 1);
    assert_tag(ReceiptCapacityScope::LiveReceiptParticipant, 2);
    assert_tag(ReceiptCapacityScope::ProvenanceServer, 3);
    assert_tag(ReceiptCapacityScope::ProvenanceConversation, 4);
    assert_tag(ReceiptCapacityScope::ProvenanceParticipant, 5);
    assert_tag(IdentityCapacityScope::Server, 1);
    assert_tag(IdentityCapacityScope::Conversation, 2);

    assert_tag(ClosureScope::Capacity, 1);
    assert_tag(ClosureScope::RecoveryFence, 2);
    assert_tag(ClosureScope::DeliveredMarkerAwaitingAck, 3);
    assert_tag(ClosureScope::EpisodeChurnLimit, 4);
    assert_tag(ResourceDimensionTag::Entries, 1);
    assert_tag(ResourceDimensionTag::Bytes, 2);
    assert_eq!(
        ResourceDimensionTag::from(ResourceDimension::Entries),
        ResourceDimensionTag::Entries
    );
    assert_eq!(
        ResourceDimension::from(ResourceDimensionTag::Bytes),
        ResourceDimension::Bytes
    );

    assert_tag(RepaymentEdgeTag::None, 1);
    assert_tag(RepaymentEdgeTag::ObserverProjection, 2);
    assert_tag(RepaymentEdgeTag::PhysicalCompaction, 3);
    assert_tag(RepaymentEdgeTag::MarkerDelivery, 4);
    assert_tag(RepaymentEdgeTag::ParticipantCursorProgress, 5);
    assert_tag(RepaymentEdgeTag::DetachedCredentialRecovery, 6);
    assert_tag(RepaymentEdgeTag::DetachedMarkerRelease, 7);
    assert_tag(RepaymentEdgeTag::DetachedCursorRelease, 8);
}

#[test]
fn receipt_marker_ack_and_recovery_reason_registries_are_exact() {
    assert_tag(ReceiptExpiryReason::Deadline, 1);
    assert_tag(ReceiptExpiryReason::Superseded, 2);
    assert_tag(MarkerNotDeliveredReason::NotDeliveredToProofEpoch, 1);
    assert_tag(MarkerMismatchReason::BelowCursor, 1);
    assert_tag(MarkerMismatchReason::NoMarkerExpected, 2);
    assert_tag(MarkerMismatchReason::ExpectedDifferentMarker, 3);
    assert_tag(AckGapReason::NotContiguouslyAvailable, 1);
    assert_tag(AckRegressionReason::BelowCursor, 1);
    assert_tag(InvalidObserverEpochReason::ConversationUnknown, 1);
    assert_tag(InvalidObserverEpochReason::EpochAhead, 2);
    assert_tag(InvalidObserverEpochListReason::TooManyEntries, 1);
    assert_tag(InvalidObserverEpochListReason::DuplicateConversation, 2);
    assert_tag(Counter::TransactionOrder, 1);
}
