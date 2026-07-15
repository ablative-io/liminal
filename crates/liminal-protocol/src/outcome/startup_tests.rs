use super::startup::{
    CapabilityLimitField, ConnectionIncarnationExhausted,
    ParticipantCapabilityConfigurationInvalid, ParticipantRetentionCapacityInvalid,
};

#[test]
fn all_nine_capability_nonzero_fields_are_named() {
    let fields = [
        CapabilityLimitField::AttachReceiptTtlMs,
        CapabilityLimitField::ReceiptProvenanceTtlMs,
        CapabilityLimitField::MaxLiveAttachReceiptsServer,
        CapabilityLimitField::MaxLiveAttachReceiptsPerParticipant,
        CapabilityLimitField::MaxReceiptProvenanceServer,
        CapabilityLimitField::MaxReceiptProvenancePerConversation,
        CapabilityLimitField::MaxReceiptProvenancePerParticipant,
        CapabilityLimitField::MaxRetiredIdentitySlotsServer,
        CapabilityLimitField::MaxRetiredIdentitySlotsPerConversation,
    ];

    assert_eq!(fields.len(), 9);
    for field in fields {
        let outcome = ParticipantCapabilityConfigurationInvalid::NonzeroLimit {
            field,
            actual: 0,
            required_minimum: 1,
        };
        assert!(matches!(
            outcome,
            ParticipantCapabilityConfigurationInvalid::NonzeroLimit { .. }
        ));
    }
}

#[test]
fn capability_deadline_order_is_a_flat_tagged_body() {
    let outcome = ParticipantCapabilityConfigurationInvalid::ReceiptDeadlineOrder {
        attach_receipt_ttl_ms: 1_000,
        receipt_provenance_ttl_ms: 999,
        required_minimum_provenance_ttl_ms: 1_000,
    };

    assert!(matches!(
        outcome,
        ParticipantCapabilityConfigurationInvalid::ReceiptDeadlineOrder {
            required_minimum_provenance_ttl_ms: 1_000,
            ..
        }
    ));
}

#[test]
fn retention_has_only_the_three_fix_two_dimensions() {
    let outcomes = [
        ParticipantRetentionCapacityInvalid::EntryCapacity {
            required: 5,
            configured: 4,
        },
        ParticipantRetentionCapacityInvalid::ByteCapacity {
            required: 100,
            configured: 99,
        },
        ParticipantRetentionCapacityInvalid::EpisodeChurnLimit {
            configured: 1,
            required_minimum: 2,
            required_maximum: u64::from(u32::MAX),
        },
    ];

    assert_eq!(outcomes.len(), 3);
}

#[test]
fn connection_exhaustion_encodes_only_the_two_valid_payload_shapes() {
    let server = ConnectionIncarnationExhausted::ServerIncarnation;
    assert_eq!(server.current_value(), u64::MAX);
    assert_eq!(server.attempted_server_incarnation(), None);

    let ordinal = ConnectionIncarnationExhausted::ConnectionOrdinal {
        attempted_server_incarnation: 7,
    };
    assert_eq!(ordinal.current_value(), u64::MAX);
    assert_eq!(ordinal.attempted_server_incarnation(), Some(7));
}
