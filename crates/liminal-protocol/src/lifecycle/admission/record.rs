use crate::algebra::{ResourceDimension, ResourceVector};
use crate::wire::{RecordAdmissionEnvelope, RecordTooLarge};

/// Successful static ordinary-record size preflight.
///
/// The exact encoded charge is carried forward so a later protocol operation
/// does not need to recompute or substitute the value that passed stage 8.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordSizePermit {
    encoded_record_charge: ResourceVector,
}

impl RecordSizePermit {
    /// Returns the exact encoded charge that passed both component limits.
    #[must_use]
    pub const fn encoded_record_charge(self) -> ResourceVector {
        self.encoded_record_charge
    }
}

/// Static ordinary-record size decision in entry-before-byte order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecordSizeDecision {
    /// Both components fit and later admission stages may run.
    Eligible(RecordSizePermit),
    /// The first failing component and unchanged request data.
    Respond(RecordTooLarge),
}

/// Applies the frozen stage-8 `RecordTooLarge` selector.
///
/// Entries are tested before bytes and equality passes. The function consumes
/// the request envelope so the refusal cannot accidentally name another
/// operation while the success permit retains the exact admitted charge.
#[must_use]
pub const fn check_record_size(
    request: RecordAdmissionEnvelope,
    encoded_record_charge: ResourceVector,
    max_ordinary_record_charge: ResourceVector,
) -> RecordSizeDecision {
    if encoded_record_charge.entries > max_ordinary_record_charge.entries {
        return RecordSizeDecision::Respond(RecordTooLarge {
            request,
            dimension: ResourceDimension::Entries,
            encoded_record_charge,
            max_ordinary_record_charge,
        });
    }
    if encoded_record_charge.bytes > max_ordinary_record_charge.bytes {
        return RecordSizeDecision::Respond(RecordTooLarge {
            request,
            dimension: ResourceDimension::Bytes,
            encoded_record_charge,
            max_ordinary_record_charge,
        });
    }
    RecordSizeDecision::Eligible(RecordSizePermit {
        encoded_record_charge,
    })
}
