#![allow(clippy::panic)]

use crate::algebra::{ResourceDimension, ResourceVector};
use crate::wire::{Generation, RecordAdmissionEnvelope};

use super::record::{RecordSizeDecision, check_record_size};

fn request() -> RecordAdmissionEnvelope {
    RecordAdmissionEnvelope {
        conversation_id: 32_004,
        participant_id: 32,
        capability_generation: Generation::ONE,
        record_admission_attempt_token: crate::wire::RecordAdmissionAttemptToken::new([0xA7; 16]),
    }
}

#[test]
fn entries_precede_bytes_when_both_components_fail() {
    let decision = check_record_size(
        request(),
        ResourceVector::new(2, 111),
        ResourceVector::new(1, 110),
    );
    let RecordSizeDecision::Respond(refusal) = decision else {
        panic!("both-component failure must refuse");
    };
    assert_eq!(refusal.dimension, ResourceDimension::Entries);
    assert_eq!(refusal.encoded_record_charge, ResourceVector::new(2, 111));
    assert_eq!(
        refusal.max_ordinary_record_charge,
        ResourceVector::new(1, 110)
    );
}

#[test]
fn bytes_fail_after_entries_pass() {
    let decision = check_record_size(
        request(),
        ResourceVector::new(1, 111),
        ResourceVector::new(1, 110),
    );
    let RecordSizeDecision::Respond(refusal) = decision else {
        panic!("byte overage must refuse");
    };
    assert_eq!(refusal.dimension, ResourceDimension::Bytes);
    assert_eq!(refusal.request, request());
}

#[test]
fn equality_passes_and_carries_the_exact_charge() {
    let charge = ResourceVector::new(1, 110);
    let decision = check_record_size(request(), charge, charge);
    let RecordSizeDecision::Eligible(permit) = decision else {
        panic!("equality must pass");
    };
    assert_eq!(permit.encoded_record_charge(), charge);
}
