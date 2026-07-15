use super::parking::{
    CheckedMultiplyOverflow, CheckedOperation, HandshakeSizeOperands, ParkingLimitField,
    ParkingShapeViolation, ParticipantParkingConfigurationInvalid,
    ParticipantRecoveryHandshakeTooLarge, RecoveryHandshakeDimension,
    SdkParkingCapacityIncompatible,
};

const HANDSHAKE: HandshakeSizeOperands = HandshakeSizeOperands {
    max_entries: 4,
    framing_bytes: 12,
    request_entry_bytes: 16,
    response_entry_bytes: 8,
    error_response_bytes: 24,
    request_encoded_bytes: 76,
    response_encoded_bytes: 44,
};

#[test]
fn all_nine_configuration_shape_dimensions_are_constructible() {
    let overflow = CheckedMultiplyOverflow {
        left: u64::MAX,
        right: 2,
    };
    let violations = [
        ParkingShapeViolation::NonzeroLimit {
            field: ParkingLimitField::N,
            actual: 0,
            required_minimum: 1,
        },
        ParkingShapeViolation::RecoveryEntrySchemaBytes {
            actual: 15,
            required: 16,
        },
        ParkingShapeViolation::WireSchemaBytes {
            actual: 31,
            required: 32,
        },
        ParkingShapeViolation::RequestSchemaBytes {
            configured_request_limit: 64,
            wire_frame_limit: 31,
            actual: 31,
            required: 32,
        },
        ParkingShapeViolation::RowSchemaBytes {
            request_limit: 64,
            row_metadata_bytes: 12,
            actual: 75,
            required: 76,
        },
        ParkingShapeViolation::CheckedProduct(overflow),
        ParkingShapeViolation::RowBytesBound {
            left: 2,
            right: 40,
            checked_product: 80,
            actual: 79,
        },
        ParkingShapeViolation::SdkBytesBound {
            left: 3,
            right: 40,
            checked_product: 120,
            actual: 119,
        },
        ParkingShapeViolation::RecoverableSlots {
            actual: 5,
            limit: 4,
        },
    ];

    assert_eq!(violations.len(), 9);
    assert_eq!(overflow.operation(), CheckedOperation::Multiply);
    assert_eq!(overflow.checked_result(), None);
    assert!(overflow.overflow());
    assert_eq!(
        ParticipantParkingConfigurationInvalid {
            violation: violations[5],
        }
        .violation,
        ParkingShapeViolation::CheckedProduct(overflow)
    );
}

#[test]
fn all_eighteen_parked_configuration_dimensions_are_constructible() {
    let dimensions = [
        SdkParkingCapacityIncompatible::NonzeroLimit {
            field: ParkingLimitField::RE,
            actual: 0,
            required_minimum: 1,
        },
        SdkParkingCapacityIncompatible::RecoveryEntrySchemaBytes {
            actual: 15,
            required: 16,
        },
        SdkParkingCapacityIncompatible::WireSchemaBytes {
            actual: 31,
            required: 32,
        },
        SdkParkingCapacityIncompatible::RequestSchemaBytes {
            configured_request_limit: 64,
            wire_frame_limit: 31,
            actual: 31,
            required: 32,
        },
        SdkParkingCapacityIncompatible::RowSchemaBytes {
            request_limit: 64,
            row_metadata_bytes: 12,
            actual: 75,
            required: 76,
        },
        SdkParkingCapacityIncompatible::CheckedProduct(CheckedMultiplyOverflow {
            left: u64::MAX,
            right: 2,
        }),
        SdkParkingCapacityIncompatible::RowBytesBound {
            left: 2,
            right: 40,
            checked_product: 80,
            actual: 79,
        },
        SdkParkingCapacityIncompatible::SdkBytesBound {
            left: 3,
            right: 40,
            checked_product: 120,
            actual: 119,
        },
        SdkParkingCapacityIncompatible::RecoverableSlots {
            actual: 5,
            limit: 4,
        },
        SdkParkingCapacityIncompatible::ConversationRows {
            conversation_id: 7,
            occupied: 3,
            limit: 2,
        },
        SdkParkingCapacityIncompatible::ConversationBytes {
            conversation_id: 7,
            occupied: 81,
            limit: 80,
        },
        SdkParkingCapacityIncompatible::SdkConversations {
            occupied: 5,
            limit: 4,
        },
        SdkParkingCapacityIncompatible::SdkRows {
            occupied: 4,
            limit: 3,
        },
        SdkParkingCapacityIncompatible::SdkBytes {
            occupied: 121,
            limit: 120,
        },
        SdkParkingCapacityIncompatible::RequestBytes {
            conversation_id: 7,
            park_order: 9,
            actual: 65,
            limit: 64,
        },
        SdkParkingCapacityIncompatible::RecoveryHandshakeRequestBytes {
            operands: HANDSHAKE,
            limit: 75,
        },
        SdkParkingCapacityIncompatible::RecoveryHandshakeRequestWireFrameBytes {
            operands: HANDSHAKE,
            limit: 75,
        },
        SdkParkingCapacityIncompatible::RecoveryHandshakeResponseWireFrameBytes {
            operands: HANDSHAKE,
            limit: 43,
        },
    ];

    assert_eq!(dimensions.len(), 18);
}

#[test]
fn handshake_failure_keeps_both_limits_and_all_widened_operands() {
    let outcome = ParticipantRecoveryHandshakeTooLarge {
        max_entries: HANDSHAKE.max_entries,
        framing_bytes: HANDSHAKE.framing_bytes,
        request_entry_bytes: HANDSHAKE.request_entry_bytes,
        response_entry_bytes: HANDSHAKE.response_entry_bytes,
        error_response_bytes: HANDSHAKE.error_response_bytes,
        request_encoded_bytes: HANDSHAKE.request_encoded_bytes,
        response_encoded_bytes: HANDSHAKE.response_encoded_bytes,
        request_limit: 75,
        wire_frame_limit: 100,
        dimension: RecoveryHandshakeDimension::RequestBytes,
    };

    assert_eq!(outcome.request_encoded_bytes, 76);
    assert_eq!(outcome.request_limit, 75);
    assert_eq!(outcome.wire_frame_limit, 100);
}
