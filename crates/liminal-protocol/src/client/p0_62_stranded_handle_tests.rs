//! Pins for the stranded-handle cluster (p0-61 residue, successor task #62).
//!
//! # The strand, exactly
//!
//! A credential attach is torn mid-handshake after the server BOUND it. The
//! server rotates the capability generation on binding, so the client's
//! credential — restored from the pre-tear checkpoint — is one generation
//! behind forever. Retried under the SAME attempt token, the server answers the
//! attempt it already committed by replaying its receipt, and that receipt
//! carries the rotated credential: the successor generation AND the newly
//! minted attach secret.
//!
//! The replay arrives as [`ServerValue::UnboundReceipt`] rather than
//! [`ServerValue::Bound`] because the tear killed the socket and the binding the
//! receipt names is no longer current — `lookup.rs`'s
//! `receipt_binding_is_current` split. Correlation already accepts it: the
//! attach-shaped arm of `correlation.rs` keys `AttachBound`, `Bound` and
//! `UnboundReceipt` identically. Only the APPLICATION was missing —
//! `apply_correlated_value` listed `ServerValue::UnboundReceipt(_)` in its
//! no-op arm, so the rotated credential the server had just handed over was
//! read, correlated, and dropped on the floor.
//!
//! Both doors then refuse forever: the client aggregate will not FORM an attach
//! at the rotated generation (`BindingMismatch`, because `accepts_request`
//! demands the retained generation match), and the server will not accept one at
//! the old generation (`StaleAuthority { current_generation: 2 }`). That is the
//! permanent strand named in `participant_churn_convergence_e2e.rs`'s
//! `enroll_attach_and_commit` doc and deliberately left unpinned there.
//!
//! # What the fix is, and what it deliberately is not
//!
//! The receipt is adopted into [`ClientBindingState::Detached`] — not `Bound`.
//! `UnboundReceipt` states precisely that the receipt no longer names its origin
//! binding, so claiming `Bound` would assert a live binding the server has
//! already released. `Detached` is the state whose meaning is "attached once,
//! not bound now, credential retained", and it is the state from which
//! `accepts_request` permits a fresh attach.
//!
//! # Why the new arm carries NO guards of its own
//!
//! The obvious instinct is to guard adoption on matching identity, on the
//! participant not having Left, and on strict generation monotonicity. Every one
//! of those guards would be DEAD CODE, and a test aimed at one would be vacuous
//! — it would exercise a predicate its input can never reach. The upstream door
//! already settles all three, which is also why the `Bound` arm this fix mirrors
//! carries no guards either:
//!
//! - `decide_correlated_inbound` refuses any value whose EXPECTED request the
//!   current binding does not accept, before application (`inbound.rs`'s
//!   `accepts_request` gate). A receipt naming a foreign participant, or one
//!   arriving after a durable Leave, is refused there and never reaches the arm.
//!   The two pins below prove that door holds, which is what licenses the arm's
//!   silence.
//! - Backward adoption is not merely guarded but STRUCTURALLY UNREACHABLE. That
//!   same gate forces the retained generation to equal the expected attach's
//!   presented generation `G`, and `AttachBound::ordinary` refuses to exist
//!   unless its granted generation is the exact successor of the presented one.
//!   So an applied attach receipt always grants exactly `G + 1`. A monotonicity
//!   check could never observe a non-forward value, so none is written; the pin
//!   below measures the successor relation instead of asserting a guard against
//!   an input that cannot occur.
//!
//! [`ServerValue::Bound`]: crate::wire::ServerValue::Bound
//! [`ServerValue::UnboundReceipt`]: crate::wire::ServerValue::UnboundReceipt

use super::*;
use crate::wire::{
    AttachAttemptToken, AttachSecret, ClientRequest, CredentialAttachRequest, Generation,
    ReceiptReplay, ServerValue,
};

use super::gen_skip_supersession_tests::{TestResult, epoch, generation};

/// The one conversation and participant these pins run on, shared with the
/// `gen_skip` fixtures so the two clusters read against the same identity.
const CONVERSATION: u64 = 141;
const PARTICIPANT: u64 = 142;
/// The credential the pre-tear checkpoint holds.
const STALE_SECRET: u8 = 143;
/// The credential the rotation minted, which only the receipt carries.
const ROTATED_SECRET: u8 = 144;
/// The attempt token the torn attach used, and which the retry re-presents.
const CHURN_TOKEN: u8 = 0xC3;

/// The pre-tear aggregate: detached, holding the generation it knew.
fn detached_at(generation_value: u64, secret: u8) -> TestResult<ClientParticipantAggregate> {
    let mut aggregate = ClientParticipantAggregate::new();
    aggregate.binding = ClientBindingState::Detached {
        conversation_id: CONVERSATION,
        participant_id: PARTICIPANT,
        generation: generation(generation_value)?,
        attach_secret: AttachSecret::new([secret; 32]),
    };
    Ok(aggregate)
}

/// One credential attach as the client forms it.
fn attach_request(
    participant_id: u64,
    generation_value: u64,
    secret: u8,
    token: u8,
) -> TestResult<ClientRequest> {
    Ok(ClientRequest::CredentialAttach(CredentialAttachRequest {
        conversation_id: CONVERSATION,
        participant_id,
        capability_generation: generation(generation_value)?,
        attach_secret: AttachSecret::new([secret; 32]),
        attach_attempt_token: AttachAttemptToken::new([token; 16]),
        accept_marker_delivery_seq: None,
    }))
}

/// The issued attach the tear lost the answer to.
fn expected_attach(
    participant_id: u64,
    generation_value: u64,
    secret: u8,
) -> TestResult<ExpectedOperationState> {
    Ok(ExpectedOperationState {
        request: attach_request(participant_id, generation_value, secret, CHURN_TOKEN)?,
        issued: true,
        authorization: 1,
        lost: None,
    })
}

/// The server's receipt for the committed attach: requested at
/// `request_generation`, granted its successor, carrying the rotated secret.
fn receipt(
    participant_id: u64,
    request_generation: u64,
    granted_generation: u64,
    secret: u8,
) -> TestResult<crate::wire::AttachBound> {
    crate::wire::AttachBound::ordinary(
        CONVERSATION,
        AttachAttemptToken::new([CHURN_TOKEN; 16]),
        participant_id,
        generation(request_generation)?,
        AttachSecret::new([secret; 32]),
        epoch(granted_generation)?,
        0,
        0,
        0,
    )
    .ok_or("the receipt's granted generation must succeed its request generation")
}

/// Feeds one correlated `UnboundReceipt` attach replay through the inbound door.
fn consume_replay(
    aggregate: ClientParticipantAggregate,
    replay: crate::wire::AttachBound,
) -> TestResult<ClientParticipantAggregate> {
    let ClientCorrelatedInboundDecision::Applied(applied) = decide_correlated_inbound(
        aggregate,
        ServerValue::UnboundReceipt(ReceiptReplay::CredentialAttach(replay)),
        ClientResponseCorrelation { authorization: 1 },
    ) else {
        return Err("the attach receipt replay must correlate to the issued attach");
    };
    let (aggregate, _) = applied.into_parts();
    Ok(aggregate)
}

/// Reports the retained binding's generation and secret, whatever the state.
fn credential(aggregate: &ClientParticipantAggregate) -> Option<(Generation, AttachSecret)> {
    match &aggregate.binding {
        ClientBindingState::Bound {
            generation,
            attach_secret,
            ..
        }
        | ClientBindingState::Detached {
            generation,
            attach_secret,
            ..
        } => Some((*generation, *attach_secret)),
        ClientBindingState::Unbound | ClientBindingState::Left { .. } => None,
    }
}

/// RED AT PARENT: the strand itself, and the exit from it.
///
/// The pre-tear client is detached at generation 1. Its retried attach is
/// answered with the receipt for the attach that DID commit — granted
/// generation 2, carrying a secret the client has never seen. The parent
/// dropped that replay (`ServerValue::UnboundReceipt(_)` sat in
/// `apply_correlated_value`'s no-op arm), leaving the client able to form only a
/// generation-1 attach the server refuses as `StaleAuthority` — the permanent
/// strand. Now the credential is adopted and a generation-2 attach records.
#[test]
fn a_torn_but_bound_attach_recovers_from_its_receipt_replay() -> TestResult {
    let mut aggregate = detached_at(1, STALE_SECRET)?;
    aggregate.expected = Some(expected_attach(PARTICIPANT, 1, STALE_SECRET)?);
    aggregate.next_operation_authorization = 1;

    let aggregate = consume_replay(aggregate, receipt(PARTICIPANT, 1, 2, ROTATED_SECRET)?)?;

    let Some((held_generation, held_secret)) = credential(&aggregate) else {
        return Err("#62 REPRODUCED: the replay left no usable credential at all");
    };
    if held_generation != generation(2)? {
        return Err(
            "#62 REPRODUCED: the rotated generation was dropped, the client is stranded one \
             generation behind forever",
        );
    }
    if held_secret != AttachSecret::new([ROTATED_SECRET; 32]) {
        return Err("#62 REPRODUCED: the rotated attach secret was dropped from the replay");
    }
    assert!(
        matches!(aggregate.binding, ClientBindingState::Detached { .. }),
        "an UnboundReceipt names a binding the server has already released, so adopting it as \
         Bound would claim a live binding that does not exist"
    );

    // The exit the strand denied: a fresh attach at the rotated credential.
    let mut aggregate = aggregate;
    aggregate.next_operation_authorization = 1;
    let decision = record_operation(
        aggregate,
        attach_request(PARTICIPANT, 2, ROTATED_SECRET, 0xC5)?,
    );
    let ClientOperationRecordDecision::Pending(_) = decision else {
        return Err(
            "#62 REPRODUCED: the strand -- the aggregate refuses to form an attach at the rotated \
             generation (BindingMismatch) while the server refuses one at the old generation",
        );
    };
    Ok(())
}

/// The stale credential is exactly what the strand consists of: pinned here so
/// the recovery above is measured against a request the server would refuse.
///
/// GREEN AT PARENT by construction — this pins the pre-fix client's only
/// reachable request, which the fix does not change.
#[test]
fn the_pre_tear_credential_is_the_one_the_server_calls_stale() -> TestResult {
    let mut aggregate = detached_at(1, STALE_SECRET)?;
    aggregate.next_operation_authorization = 1;
    let decision = record_operation(
        aggregate,
        attach_request(PARTICIPANT, 1, STALE_SECRET, 0xC5)?,
    );
    assert!(
        matches!(decision, ClientOperationRecordDecision::Pending(_)),
        "the pre-tear client can form only its own generation-1 attach -- the request the server \
         answers with StaleAuthority {{ current_generation: 2 }}"
    );
    Ok(())
}

/// Feeds one correlated `UnboundReceipt` attach replay through the inbound door
/// and reports the refusal reason, for the shapes that must never be applied.
fn refuse_replay(
    aggregate: ClientParticipantAggregate,
    replay: crate::wire::AttachBound,
) -> TestResult<ClientInboundRefusalReason> {
    let ClientCorrelatedInboundDecision::Refused(refusal) = decide_correlated_inbound(
        aggregate,
        ServerValue::UnboundReceipt(ReceiptReplay::CredentialAttach(replay)),
        ClientResponseCorrelation { authorization: 1 },
    ) else {
        return Err("this receipt replay must NOT be applied to the aggregate");
    };
    Ok(refusal.reason())
}

/// LICENSES THE ARM'S SILENCE: a foreign participant's receipt never reaches
/// application, so the adopting arm needs no identity guard of its own.
///
/// Green at parent and after the fix alike — this measures the upstream door,
/// not the new arm. It is pinned precisely because the fix relies on it: were
/// this door to stop refusing, the guard-free arm would rewrite this client's
/// credential from another participant's receipt.
#[test]
fn a_foreign_participants_receipt_never_reaches_the_adopting_arm() -> TestResult {
    const FOREIGN: u64 = 999;
    let mut aggregate = detached_at(1, STALE_SECRET)?;
    aggregate.expected = Some(expected_attach(FOREIGN, 1, STALE_SECRET)?);
    aggregate.next_operation_authorization = 1;

    assert_eq!(
        refuse_replay(aggregate, receipt(FOREIGN, 1, 2, ROTATED_SECRET)?)?,
        ClientInboundRefusalReason::ForeignResponse,
        "a receipt for participant 999 must be refused before it can rewrite participant 142"
    );
    Ok(())
}

/// LICENSES THE ARM'S SILENCE: a durable Leave is permanent, and the refusal
/// happens upstream, so the adopting arm needs no `is_left` guard of its own.
#[test]
fn a_receipt_replay_never_reaches_the_arm_after_a_durable_leave() -> TestResult {
    let mut aggregate = ClientParticipantAggregate::new();
    aggregate.binding = ClientBindingState::Left {
        conversation_id: CONVERSATION,
        participant_id: PARTICIPANT,
        generation: generation(1)?,
    };
    aggregate.expected = Some(expected_attach(PARTICIPANT, 1, STALE_SECRET)?);
    aggregate.next_operation_authorization = 1;

    let reason = refuse_replay(aggregate, receipt(PARTICIPANT, 1, 2, ROTATED_SECRET)?)?;
    assert!(
        matches!(
            reason,
            ClientInboundRefusalReason::AlreadyDead | ClientInboundRefusalReason::ForeignResponse
        ),
        "a durable Leave is permanent; a receipt replay must be refused before the adopting arm, \
         got {reason:?}"
    );
    Ok(())
}

/// LICENSES THE ABSENCE OF A MONOTONICITY GUARD: an applied attach receipt
/// grants EXACTLY the successor of the credential the client holds, so a
/// backward adoption is unreachable rather than merely guarded.
///
/// The two halves of that claim are measured separately: the receipt constructor
/// refuses to build a non-successor grant at all, and the applied grant equals
/// the retained generation plus one.
#[test]
fn an_applied_attach_receipt_can_only_ever_grant_the_next_generation() -> TestResult {
    assert!(
        crate::wire::AttachBound::ordinary(
            CONVERSATION,
            AttachAttemptToken::new([CHURN_TOKEN; 16]),
            PARTICIPANT,
            generation(5)?,
            AttachSecret::new([ROTATED_SECRET; 32]),
            epoch(2)?,
            0,
            0,
            0,
        )
        .is_none(),
        "a receipt granting a generation BELOW its presented one must not be constructible -- if \
         it ever becomes so, the adopting arm needs the monotonicity guard this pin retires"
    );

    let mut aggregate = detached_at(4, STALE_SECRET)?;
    aggregate.expected = Some(expected_attach(PARTICIPANT, 4, STALE_SECRET)?);
    aggregate.next_operation_authorization = 1;
    let aggregate = consume_replay(aggregate, receipt(PARTICIPANT, 4, 5, ROTATED_SECRET)?)?;
    assert_eq!(
        credential(&aggregate).map(|held| held.0),
        Some(generation(5)?),
        "the applied grant must be exactly the successor of the retained generation"
    );
    Ok(())
}
