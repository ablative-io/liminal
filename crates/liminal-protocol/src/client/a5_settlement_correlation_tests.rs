//! Correlation pins for the §0.16 A5 settlement-refusal family.
//!
//! The wire surface landed at the breaking window; the client-side answer to a
//! settlement refusal did not, and these tests pin the gap as a MEASURED limit
//! rather than leaving it to be discovered by the leg that builds on it.

use crate::wire::{
    AttachAttemptToken, AttachSecret, ClientDiscriminant, ClientRequest, CredentialAttachRequest,
    DetachAttemptToken, DetachRequest, EnrollmentRequest, EnrollmentSettlementBackpressure,
    EnrollmentToken, Generation, MarkerSettlementBackpressure, RecordAdmission,
    RecordAdmissionAttemptToken, ServerValue,
};

type TestResult<T = ()> = Result<T, &'static str>;

const CONVERSATION: u64 = 10;
const PARTICIPANT: u64 = 20;

fn generation() -> TestResult<Generation> {
    Generation::new(7).ok_or("generation must be nonzero")
}

fn attach_request() -> TestResult<ClientRequest> {
    Ok(ClientRequest::CredentialAttach(CredentialAttachRequest {
        conversation_id: CONVERSATION,
        participant_id: PARTICIPANT,
        capability_generation: generation()?,
        attach_secret: AttachSecret::new([5; 32]),
        attach_attempt_token: AttachAttemptToken::new([2; 16]),
        accept_marker_delivery_seq: None,
    }))
}

fn detach_request() -> TestResult<ClientRequest> {
    Ok(ClientRequest::Detach(DetachRequest {
        conversation_id: CONVERSATION,
        participant_id: PARTICIPANT,
        capability_generation: generation()?,
        detach_attempt_token: DetachAttemptToken::new([3; 16]),
    }))
}

fn enrollment_request() -> ClientRequest {
    ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: CONVERSATION,
        enrollment_token: EnrollmentToken::new([1; 16]),
    })
}

fn record_request(token: RecordAdmissionAttemptToken) -> TestResult<ClientRequest> {
    Ok(ClientRequest::RecordAdmission(RecordAdmission {
        conversation_id: CONVERSATION,
        participant_id: PARTICIPANT,
        capability_generation: generation()?,
        record_admission_attempt_token: token,
        payload: alloc::vec![1, 2, 3],
    }))
}

/// A4's new arm correlates exactly, and is the positive control proving the
/// predicate below can say yes at all.
#[test]
fn record_admission_body_conflict_correlates_to_its_own_request() -> TestResult {
    let token = RecordAdmissionAttemptToken::new([0xA7; 16]);
    let value = ServerValue::AttemptTokenBodyConflict(
        crate::wire::AttemptTokenBodyConflict::RecordAdmission {
            token,
            conversation_id: CONVERSATION,
            presented_participant_id: PARTICIPANT,
            presented_generation: generation()?,
        },
    );
    assert!(super::correlation::matches_request(
        &value,
        &record_request(token)?
    ));
    assert!(super::correlation::same_identity(
        &value,
        &record_request(token)?
    ));

    // A different token is a different request, so the predicate is not simply
    // agreeing with everything of the right family.
    assert!(!super::correlation::matches_request(
        &value,
        &record_request(RecordAdmissionAttemptToken::new([0xB8; 16]))?
    ));
    Ok(())
}

/// The settlement family names its request FAMILY on the wire but not its
/// request IDENTITY, so nothing here can correlate.
///
/// This is the ratified schema (participant contract §0.16 condition 2:
/// `{ conversation_id, refused_epoch }`, and `{ conversation_id }` for the
/// enrollment wrapper), not an unfinished body. Closing the gap is a
/// client-behavior decision owned by the leg that builds the retry discipline;
/// when it lands, this test is the one that must be deliberately rewritten.
#[test]
fn settlement_refusals_carry_no_correlating_request_identity() -> TestResult {
    let cases: [(ServerValue, ClientRequest, ClientDiscriminant); 3] = [
        (
            ServerValue::MarkerSettlementBackpressure(
                MarkerSettlementBackpressure::CredentialAttach {
                    conversation_id: CONVERSATION,
                    refused_epoch: 111,
                },
            ),
            attach_request()?,
            ClientDiscriminant::CredentialAttachRequest,
        ),
        (
            ServerValue::MarkerSettlementBackpressure(MarkerSettlementBackpressure::Detach {
                conversation_id: CONVERSATION,
                refused_epoch: 111,
            }),
            detach_request()?,
            ClientDiscriminant::DetachRequest,
        ),
        (
            ServerValue::EnrollmentSettlementBackpressure(EnrollmentSettlementBackpressure {
                conversation_id: CONVERSATION,
            }),
            enrollment_request(),
            ClientDiscriminant::EnrollmentRequest,
        ),
    ];

    for (value, request, family) in cases {
        // The family IS carried: the row routes to its own request kind.
        assert_eq!(value.originating_request(), Some(family));
        assert_eq!(request.discriminant(), family);
        // The identity is not, so both correlation predicates say no.
        assert!(!super::correlation::matches_request(&value, &request));
        assert!(!super::correlation::same_identity(&value, &request));
    }
    Ok(())
}
