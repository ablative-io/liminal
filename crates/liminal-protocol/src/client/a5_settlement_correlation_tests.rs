//! Correlation pins for the §0.16 A5 settlement-refusal family.
//!
//! Leg 1 landed the wire surface and pinned the correlation gap as a MEASURED
//! limit (`settlement_refusals_carry_no_correlating_request_identity`). Seat
//! ruling 4 (2026-08-13) ruled the closure REQUIRED in this lane, client-side,
//! with no new wire fields — a settlement refusal landing as `ForeignResponse`
//! would convert a lawful presentation back into a client error and defeat the
//! amendment at the last hop. That pin is therefore INVERTED here into the
//! closure's positive pin, and the limit it measured survives as the
//! `same_identity` half: the rows still carry no request identity, and nothing
//! pretends otherwise.

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

/// A settlement refusal CORRELATES to its own pending request, by conversation
/// and request family — and still carries no request identity.
///
/// The inverted Leg 1 pin. Both halves are asserted on every row: the closure
/// says yes (`matches_request`), and `same_identity` still says no, because the
/// ratified schema really does carry no participant, generation, or attempt
/// token and the client's `DelayedResponse`/`ForeignResponse` split depends on
/// that predicate staying truthful.
#[test]
fn settlement_refusals_correlate_to_their_own_pending_request() -> TestResult {
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
        // CLOSED: the refusal correlates to the request it answers.
        assert!(
            super::correlation::matches_request(&value, &request),
            "a settlement refusal must correlate to its own pending {family:?}"
        );
        // The identity is still not on the wire, and the predicate that reports
        // identity must not start claiming it is.
        assert!(!super::correlation::same_identity(&value, &request));
    }
    Ok(())
}

/// The closure is scoped, not blanket: another conversation, and another
/// request family, both still say no.
///
/// This is the discriminator's negative pole. Without it the closure above
/// would pass just as happily if `settlement_matches_request` returned
/// `Some(true)` unconditionally — a predicate that agrees with everything is a
/// green that cannot fail.
#[test]
fn a_settlement_refusal_correlates_to_no_other_conversation_or_family() -> TestResult {
    const OTHER_CONVERSATION: u64 = CONVERSATION + 1;

    let attach_row =
        ServerValue::MarkerSettlementBackpressure(MarkerSettlementBackpressure::CredentialAttach {
            conversation_id: OTHER_CONVERSATION,
            refused_epoch: 111,
        });
    assert!(!super::correlation::matches_request(
        &attach_row,
        &attach_request()?
    ));

    // Right conversation, WRONG family, in both directions across the per-family
    // split the ratified row carries.
    let detach_row =
        ServerValue::MarkerSettlementBackpressure(MarkerSettlementBackpressure::Detach {
            conversation_id: CONVERSATION,
            refused_epoch: 111,
        });
    assert!(!super::correlation::matches_request(
        &detach_row,
        &attach_request()?
    ));
    let same_conversation_attach_row =
        ServerValue::MarkerSettlementBackpressure(MarkerSettlementBackpressure::CredentialAttach {
            conversation_id: CONVERSATION,
            refused_epoch: 111,
        });
    assert!(!super::correlation::matches_request(
        &same_conversation_attach_row,
        &detach_request()?
    ));

    // The enrollment row answers ONLY an enrollment. It has no epoch and no
    // wake by ratified law, so a client holding an attach must not read it as
    // its own answer.
    let enrollment_row =
        ServerValue::EnrollmentSettlementBackpressure(EnrollmentSettlementBackpressure {
            conversation_id: CONVERSATION,
        });
    assert!(!super::correlation::matches_request(
        &enrollment_row,
        &attach_request()?
    ));
    assert!(!super::correlation::matches_request(
        &enrollment_row,
        &record_request(RecordAdmissionAttemptToken::new([0xC9; 16]))?
    ));
    assert!(super::correlation::matches_request(
        &enrollment_row,
        &enrollment_request()
    ));
    Ok(())
}

/// ⛔ RATIFICATION CONDITION (Waffles's adjacency declaration in the §0.16
/// status block, first under the trial adopted 2026-08-13): condition 2's
/// persist-the-waiting-state retry discipline MUST NOT EXTEND #195's orphan
/// window.
///
/// #195 is the killed-mid-attach identity orphan: an attach killed after the
/// server committed but before the client learned its identity. It is reached
/// by KILL, not by refusal, and the declaration is that it does not bear on
/// presentation — with exactly one build-lane flag riding it, which is this
/// one, and the build must SAY so at its pin. Here is the saying, and the
/// measurement behind it.
///
/// The waiting state a settlement refusal persists is the client's PENDING
/// REQUEST SLOT, which the refusal does not retire. That is a state the client
/// already held before the refusal arrived and would have held for exactly as
/// long had the server closed the connection instead — the pre-amendment
/// behaviour. What lengthens #195's window is an attach that COMMITTED without
/// the client learning its identity; a settlement refusal commits NOTHING (the
/// server restores the slot entry, the frontier, and both position allocators
/// before the refusal exists), so no identity is minted for the client to be
/// orphaned from, and the window is not entered at all, let alone extended.
///
/// The pin is this: a settlement refusal is not `Applied` in the sense that
/// retires the expected slot — it correlates to the request, and it is a
/// REFUSAL, so the client's own pending request remains the client's to retry
/// or abandon. Nothing here mints, commits, or discards an identity.
#[test]
fn the_settlement_retry_discipline_does_not_extend_the_195_orphan_window() -> TestResult {
    let row =
        ServerValue::MarkerSettlementBackpressure(MarkerSettlementBackpressure::CredentialAttach {
            conversation_id: CONVERSATION,
            refused_epoch: 111,
        });
    // It correlates (so the client is told, rather than closed on) ...
    assert!(super::correlation::matches_request(
        &row,
        &attach_request()?
    ));
    // ... and it carries NO committed identity: no bound receipt, no
    // generation, no attach token. There is nothing in this value from which a
    // client could believe an attach committed, which is the only way the
    // retry discipline could reach #195's window.
    assert!(!super::correlation::same_identity(&row, &attach_request()?));
    assert_eq!(
        row.originating_request(),
        Some(ClientDiscriminant::CredentialAttachRequest)
    );
    Ok(())
}
