use crate::wire::ConversationId;

/// Configuration-limit selector used by parking shape validation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParkingLimitField {
    /// Per-conversation parked-row limit.
    N,
    /// Per-conversation parked-byte limit.
    C,
    /// SDK-wide recoverable-conversation limit.
    P,
    /// SDK-wide parked-row limit.
    G,
    /// SDK-wide parked-byte limit.
    D,
    /// Participant request-byte limit.
    R,
    /// Charged parked-row byte bound.
    B,
    /// Recovery request-entry schema bytes.
    RE,
    /// Negotiated wire-frame byte limit.
    WF,
}

/// Exact operands for the only checked-product failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckedMultiplyOverflow {
    /// Left multiplication operand.
    pub left: u64,
    /// Right multiplication operand.
    pub right: u64,
}

impl CheckedMultiplyOverflow {
    /// The failed operation is always multiplication.
    #[must_use]
    pub const fn operation(self) -> CheckedOperation {
        let _ = self;
        CheckedOperation::Multiply
    }

    /// An overflow has no checked result.
    #[must_use]
    pub const fn checked_result(self) -> Option<u64> {
        let _ = self;
        None
    }

    /// The tagged body always denotes overflow.
    #[must_use]
    pub const fn overflow(self) -> bool {
        let _ = self;
        true
    }
}

/// Operation selector for a checked-product failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckedOperation {
    /// Checked multiplication.
    Multiply,
}

/// Exact nine-way parking configuration-shape violation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParkingShapeViolation {
    /// One of the signed limits is zero.
    NonzeroLimit {
        /// Offending signed field.
        field: ParkingLimitField,
        /// Actual value, which is zero for this variant.
        actual: u64,
        /// Required minimum, which is one for this variant.
        required_minimum: u64,
    },
    /// Recovery request-entry schema is not the fixed width.
    RecoveryEntrySchemaBytes {
        /// Configured request-entry width.
        actual: u64,
        /// Required request-entry width of sixteen bytes.
        required: u64,
    },
    /// Wire frame is too small for one participant frame.
    WireSchemaBytes {
        /// Configured wire-frame limit.
        actual: u64,
        /// Required participant-frame schema bytes.
        required: u64,
    },
    /// Effective request limit is too small for one request schema.
    RequestSchemaBytes {
        /// Configured participant request limit.
        configured_request_limit: u64,
        /// Configured wire-frame limit.
        wire_frame_limit: u64,
        /// Effective request limit `min(R, WF)`.
        actual: u64,
        /// Required participant-request schema bytes.
        required: u64,
    },
    /// Charged row bound is below request plus row metadata.
    RowSchemaBytes {
        /// Configured participant request limit.
        request_limit: u64,
        /// Fixed row-metadata bytes.
        row_metadata_bytes: u64,
        /// Configured charged row byte bound.
        actual: u64,
        /// Exact widened required bytes.
        required: u128,
    },
    /// A required limit product overflowed `u64`.
    CheckedProduct(CheckedMultiplyOverflow),
    /// Per-conversation bytes are below `N * B`.
    RowBytesBound {
        /// Per-conversation row limit.
        left: u64,
        /// Charged row byte bound.
        right: u64,
        /// Checked `N * B` product.
        checked_product: u64,
        /// Configured per-conversation byte limit.
        actual: u64,
    },
    /// SDK-wide bytes are below `G * B`.
    SdkBytesBound {
        /// SDK-wide row limit.
        left: u64,
        /// Charged row byte bound.
        right: u64,
        /// Checked `G * B` product.
        checked_product: u64,
        /// Configured SDK-wide byte limit.
        actual: u64,
    },
    /// Recoverable conversation slots exceed the connection capability.
    RecoverableSlots {
        /// Configured recoverable-conversation count.
        actual: u64,
        /// Negotiated participant-conversation slots per connection.
        limit: u64,
    },
}

/// Startup parking configuration is invalid before handshake sizing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParticipantParkingConfigurationInvalid {
    /// First failing shape dimension and its exact operands.
    pub violation: ParkingShapeViolation,
}

/// Exact widened operands shared by recovery-handshake size failures.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HandshakeSizeOperands {
    /// Maximum recovery entries `P`.
    pub max_entries: u64,
    /// Exact widened `u128(RF) + u128(RC(P))` bytes.
    pub framing_bytes: u128,
    /// Recovery request-entry schema bytes `RE`.
    pub request_entry_bytes: u64,
    /// Recovery response status-entry schema bytes `SE`.
    pub response_entry_bytes: u64,
    /// Recovery error-response schema bytes `EE`.
    pub error_response_bytes: u64,
    /// Exact widened request bytes `RH(P)`.
    pub request_encoded_bytes: u128,
    /// Exact widened response bytes `SH(P)`.
    pub response_encoded_bytes: u128,
}

/// Recovery-handshake dimension that exceeded a signed limit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryHandshakeDimension {
    /// Recovery request exceeded `R`.
    RequestBytes,
    /// Recovery request exceeded `WF`.
    RequestWireFrameBytes,
    /// Recovery response exceeded `WF`.
    ResponseWireFrameBytes,
}

/// Initial-phase recovery handshake cannot fit the signed limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParticipantRecoveryHandshakeTooLarge {
    /// Complete exact widened sizing operands.
    pub operands: HandshakeSizeOperands,
    /// Signed participant request limit `R`.
    pub request_limit: u64,
    /// Signed negotiated wire-frame limit `WF`.
    pub wire_frame_limit: u64,
    /// First failing handshake dimension.
    pub dimension: RecoveryHandshakeDimension,
}

/// Parked rows are incompatible with replacement SDK configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SdkParkingCapacityIncompatible {
    /// One of the replacement signed limits is zero.
    NonzeroLimit {
        /// Offending signed field.
        field: ParkingLimitField,
        /// Actual value, which is zero for this variant.
        actual: u64,
        /// Required minimum, which is one for this variant.
        required_minimum: u64,
    },
    /// Recovery request-entry schema is not the fixed width.
    RecoveryEntrySchemaBytes {
        /// Configured request-entry width.
        actual: u64,
        /// Required request-entry width.
        required: u64,
    },
    /// Wire frame is too small for one participant frame.
    WireSchemaBytes {
        /// Configured wire-frame limit.
        actual: u64,
        /// Required participant-frame schema bytes.
        required: u64,
    },
    /// Effective request limit is too small for one request schema.
    RequestSchemaBytes {
        /// Configured participant request limit.
        configured_request_limit: u64,
        /// Configured wire-frame limit.
        wire_frame_limit: u64,
        /// Effective request limit `min(R, WF)`.
        actual: u64,
        /// Required participant-request schema bytes.
        required: u64,
    },
    /// Charged row bound is below request plus row metadata.
    RowSchemaBytes {
        /// Configured participant request limit.
        request_limit: u64,
        /// Fixed row-metadata bytes.
        row_metadata_bytes: u64,
        /// Configured charged row byte bound.
        actual: u64,
        /// Exact widened required bytes.
        required: u128,
    },
    /// A required replacement limit product overflowed `u64`.
    CheckedProduct(CheckedMultiplyOverflow),
    /// Per-conversation bytes are below `N * B`.
    RowBytesBound {
        /// Per-conversation row limit.
        left: u64,
        /// Charged row byte bound.
        right: u64,
        /// Checked `N * B` product.
        checked_product: u64,
        /// Configured per-conversation byte limit.
        actual: u64,
    },
    /// SDK-wide bytes are below `G * B`.
    SdkBytesBound {
        /// SDK-wide row limit.
        left: u64,
        /// Charged row byte bound.
        right: u64,
        /// Checked `G * B` product.
        checked_product: u64,
        /// Configured SDK-wide byte limit.
        actual: u64,
    },
    /// Recoverable conversation slots exceed connection capability.
    RecoverableSlots {
        /// Configured recoverable-conversation count.
        actual: u64,
        /// Negotiated participant-conversation slots per connection.
        limit: u64,
    },
    /// Existing rows exceed replacement per-conversation row capacity.
    ConversationRows {
        /// Conversation with incompatible retained rows.
        conversation_id: ConversationId,
        /// Existing retained rows.
        occupied: u64,
        /// Replacement per-conversation row limit.
        limit: u64,
    },
    /// Existing bytes exceed replacement per-conversation byte capacity.
    ConversationBytes {
        /// Conversation with incompatible retained bytes.
        conversation_id: ConversationId,
        /// Existing charged retained bytes.
        occupied: u64,
        /// Replacement per-conversation byte limit.
        limit: u64,
    },
    /// Existing parked conversations exceed replacement SDK capacity.
    SdkConversations {
        /// Existing parked-conversation count.
        occupied: u64,
        /// Replacement SDK conversation limit.
        limit: u64,
    },
    /// Existing rows exceed replacement SDK row capacity.
    SdkRows {
        /// Existing SDK-wide retained rows.
        occupied: u64,
        /// Replacement SDK row limit.
        limit: u64,
    },
    /// Existing bytes exceed replacement SDK byte capacity.
    SdkBytes {
        /// Existing SDK-wide charged retained bytes.
        occupied: u64,
        /// Replacement SDK byte limit.
        limit: u64,
    },
    /// One retained request exceeds replacement effective request bytes.
    RequestBytes {
        /// Conversation holding the retained request.
        conversation_id: ConversationId,
        /// Durable order of the retained request.
        park_order: u64,
        /// Complete encoded participant request bytes.
        actual: u64,
        /// Replacement effective request limit `min(R, WF)`.
        limit: u64,
    },
    /// Recovery request exceeds replacement participant request bytes.
    RecoveryHandshakeRequestBytes {
        /// Complete exact widened sizing operands.
        operands: HandshakeSizeOperands,
        /// Replacement participant request limit.
        limit: u64,
    },
    /// Recovery request exceeds replacement wire-frame bytes.
    RecoveryHandshakeRequestWireFrameBytes {
        /// Complete exact widened sizing operands.
        operands: HandshakeSizeOperands,
        /// Replacement wire-frame limit.
        limit: u64,
    },
    /// Recovery response exceeds replacement wire-frame bytes.
    RecoveryHandshakeResponseWireFrameBytes {
        /// Complete exact widened sizing operands.
        operands: HandshakeSizeOperands,
        /// Replacement wire-frame limit.
        limit: u64,
    },
}
