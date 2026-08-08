//! Board #14 — a contract-correct refusal on the attach path must be
//! WIRE-VISIBLE, never a bare connection close.
//!
//! RE-AUTHORED from the red banked on branch `silent-close-attach-refusal` at
//! `557eed7` ("test(r4): pin the silent close -- credential attach answers a
//! client with NO FRAME"). Re-authored rather than merged or cherry-picked
//! under YG-560; the fixture shape, the seam choice, and the positive-control
//! argument are that commit's, and the analysis they rest on is
//! `docs/design/ATTACH-SILENCE-14.md`.
//!
//! # The defect
//!
//! `ConversationAuthority::apply_credential_attach_with_impact` can return a
//! [`StateError`](super::state::StateError) AFTER the lookup stage has already
//! authorized the request. That `StateError` becomes
//! `ParticipantSemanticError::Internal { message }` — whose own doc says the
//! text is "Diagnostic text for server logs; never placed on the participant
//! wire" — which `dispatch` turns into [`ParticipantDispatch::Fatal`], which
//! `connection::apply` turns into a bare `FrameAction::Close`, which
//! `connection::state` documents as the variant that "stays silent", as
//! opposed to `RespondThenClose`.
//!
//! The client therefore receives a socket close and NO FRAME. It cannot
//! classify the refusal, so a never-lose-a-record client retries forever, and
//! every retry that does succeed burns a rationed `ProvenanceParticipant`
//! receipt slot until `ReceiptCapacityExceeded` fires and the estate will not
//! boot.
//!
//! # The seam
//!
//! These tests assert on [`ParticipantDispatch`], obtained from the real
//! `dispatch_generic_frame` over a real encoded wire frame. That is the
//! innermost seam that still expresses "what is the client told":
//!
//! * It DOES cover the client-visible outcome class — a frame versus silence —
//!   because `connection::apply`'s mapping from `ParticipantDispatch` to
//!   `FrameAction` is total and injective for the three response variants.
//! * It DOES discriminate `Respond` from `RespondThenClose`. That distinction
//!   is load-bearing and NOT cosmetic: `RespondThenClose` delivery is
//!   documented as BEST-EFFORT in `connection::process` — a failed enqueue is
//!   logged and swallowed, and the single drain on the close path is neither
//!   forced nor awaited — so under load it degrades to exactly the silent
//!   close this test exists to forbid. Only `Respond` propagates an enqueue
//!   failure and leaves the connection open. Asserting merely "not Fatal"
//!   would be satisfied by a fix that still strands the client.
//! * It does NOT cover the socket layer below `FrameAction` (the actual write,
//!   flush, and drain). `FrameAction` is `pub(super)` inside
//!   `server::connection` and is unreachable from this module, so the last hop
//!   is established by reading, not by assertion.
//!
//! # Why the trigger is the pending-finalization state and nothing else
//!
//! `lookup_credential_attach` never inspects binding state when deciding
//! authorization, so a CURRENT generation with a CORRECT secret passes as
//! `AuthorizedFresh` even against a binding sitting in `PendingFinalization` —
//! and then dies in `ops_attach::allocate_attach_mode`. The stale-generation
//! tests below are the positive control: the SAME fixture, the SAME seam, one
//! field different, already answered with a real frame today. They prove this
//! harness can observe a `Respond`, so the red is a property of the state
//! under test and not of the plumbing.

use std::error::Error;

use liminal_protocol::wire::{
    AttachAttemptToken, ClientRequest, CommonStaleAuthorityEnvelope, ConnectionIncarnation,
    CredentialAttachRequest, Generation, ObserverBackpressure, ServerValue, StaleAuthority,
};

use crate::server::participant::{ParticipantConnectionConversations, ParticipantDispatch};

use super::ProductionParticipantHandler;
use super::tests::{
    decode_server_value, dispatch_outcome, open_disk_store_for_tests, test_participant_config,
};
use super::tests_receipts::{GEN_ONE, attach, attach_request, detach, enroll, generation};
use super::tests_w1b_pending_died_restart::pending_restart_fixture;

/// Names what the client actually receives, so a failure message says it.
fn client_outcome(outcome: &ParticipantDispatch) -> String {
    match outcome {
        ParticipantDispatch::NotParticipant => "NO FRAME (frame disowned)".to_string(),
        ParticipantDispatch::Respond(_) => "a response frame, connection open".to_string(),
        ParticipantDispatch::RespondThenClose(_) => {
            "a BEST-EFFORT frame, then close (may still be silence under load)".to_string()
        }
        ParticipantDispatch::Fatal(error) => {
            format!("NO FRAME AT ALL — silent close. Server-only text: {error}")
        }
    }
}

fn attach_against_pending_finalization(
    generation_value: u64,
    token_byte: u8,
) -> Result<ParticipantDispatch, Box<dyn Error>> {
    let fixture = pending_restart_fixture()?;
    let generation =
        Generation::new(generation_value).ok_or("zero generation in the #14 attach fixture")?;
    dispatch_outcome(
        &fixture.handler,
        ConnectionIncarnation::new(97, 9),
        &mut ParticipantConnectionConversations::default(),
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: fixture.conversation_id,
            participant_id: fixture.participant_id,
            capability_generation: generation,
            attach_secret: fixture.attach_secret,
            attach_attempt_token: AttachAttemptToken::new([token_byte; 16]),
            accept_marker_delivery_seq: None,
        }),
    )
}

/// THE RED. A well-formed credential attach — CURRENT generation, CORRECT
/// attach secret, fresh attempt token — against a participant whose binding
/// sits in `PendingFinalization` must not be answered with silence.
///
/// This test FAILS on the tree that funnels the refusal through
/// `StateError::invariant`, and passes once the refusal is presented.
#[test]
fn attach_against_pending_finalization_must_not_answer_with_silence() -> Result<(), Box<dyn Error>>
{
    let outcome = attach_against_pending_finalization(2, 0xA2)?;

    assert!(
        matches!(outcome, ParticipantDispatch::Respond(_)),
        "a credential attach that cannot be admitted must still TELL the client so, on the \
         Respond path that guarantees delivery and leaves the connection open. Instead the \
         client got: {}",
        client_outcome(&outcome)
    );
    Ok(())
}

/// The refusal is not merely A frame — it is the register row whose retry
/// discipline is the true one.
///
/// `PendingFinalization` exists in exactly one circumstance: a binding
/// terminal that hard-observer progress refused to append
/// (`binding_terminal.rs`'s `Pending` arm requires
/// `hard_observer_progress < key.delivery_seq`, and its own type is documented
/// as the "observer-blocked pending terminal admission"). The contract says
/// the same thing from the other side — §2's R-A2 fate paragraph requires that
/// a durably `PendingFinalization` slot is settled when "progress wake appends
/// exactly one correctly ordered record", and register row 5954 pairs the
/// detach that CREATES this state with `ObserverBackpressure`.
///
/// So the blocked resource is hard-observer progress, the wake that clears it
/// is `ObserverProgressed`, and `ObserverBackpressure` — an outcome the frozen
/// R-D1 register already admits for credential attach (row 5943) — is the row
/// that carries exactly that retry discipline. Asserting the row, not just the
/// frame, is what stops a later change from satisfying this file with an
/// arbitrary but useless answer.
#[test]
fn the_pending_finalization_refusal_carries_the_observer_backpressure_row()
-> Result<(), Box<dyn Error>> {
    let outcome = attach_against_pending_finalization(2, 0xA3)?;
    let ParticipantDispatch::Respond(response) = &outcome else {
        return Err(format!(
            "the refusal must reach the client as a frame, got: {}",
            client_outcome(&outcome)
        )
        .into());
    };
    let value = decode_server_value(response)?;
    let ServerValue::ObserverBackpressure(ObserverBackpressure::CredentialAttach {
        request,
        state,
    }) = value
    else {
        return Err(format!(
            "the pending-finalization refusal must present the credential-attach \
             ObserverBackpressure row, got: {value:?}"
        )
        .into());
    };
    assert_eq!(
        request.attach_attempt_token,
        AttachAttemptToken::new([0xA3; 16]),
        "the refusal must echo the presented attempt token"
    );
    assert_eq!(
        state.backpressure_epoch(),
        state.observer_progress(),
        "an initial refusal epoch is exactly the progress value observed by the serialized \
         operation (ObserverBackpressureState::initial)"
    );
    Ok(())
}

/// POSITIVE CONTROL for the two tests above.
///
/// Same fixture, same participant, same secret, same seam — only the presented
/// generation differs. A STALE generation is refused correctly today, with a
/// real frame on the `Respond` path. This proves the harness can observe a
/// `Respond`, so the siblings' failure is a property of the
/// pending-finalization state and not of this test's plumbing.
///
/// It also pins the LOOKUP-stage precedent the fix must follow: the refusal
/// travels as an ordinary response value, never as a close.
#[test]
fn stale_generation_attach_is_already_refused_with_a_frame() -> Result<(), Box<dyn Error>> {
    for (generation_value, token_byte) in [(1_u64, 0xB1_u8), (3, 0xB3)] {
        let outcome = attach_against_pending_finalization(generation_value, token_byte)?;
        let ParticipantDispatch::Respond(response) = &outcome else {
            return Err(format!(
                "generation {generation_value} should already be refused with a frame, got: {}",
                client_outcome(&outcome)
            )
            .into());
        };
        let value = decode_server_value(response)?;
        assert!(
            matches!(
                value,
                ServerValue::StaleAuthority(StaleAuthority::Live {
                    request: CommonStaleAuthorityEnvelope::CredentialAttach(_),
                    ..
                })
            ),
            "generation {generation_value} should refuse with the credential-attach \
             StaleAuthority::Live row, got: {value:?}"
        );
    }
    Ok(())
}

/// Pins the reachability fact that reshapes this incident: on the LIVE client
/// path a stale generation is caught by the LOOKUP stage, which answers with
/// the current generation. The VERIFY stage's own
/// `AttachVerificationError::Generation` arm is therefore never what a
/// connected client meets.
///
/// `lookup_credential_attach` refuses on exactly `request.capability_generation
/// != member.generation()`, and `apply_credential_attach_with_impact` returns
/// on any non-`AuthorizedFresh` lookup BEFORE `attach_commit` runs — so the
/// identical predicate inside `verify_attach_common` cannot fire beneath it.
/// That arm stays reachable only from `replay_attached`, the cold-restore path,
/// where no client is connected to be told anything.
#[test]
fn live_path_stale_generation_is_answered_by_lookup_with_the_current_generation()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(904, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
    let conversation_id = 9041;

    let receipt = enroll(&handler, incarnation, conversation_id, [11; 16])?;
    let participant_id = receipt.participant_id();
    detach(
        &handler,
        incarnation,
        conversation_id,
        participant_id,
        GEN_ONE,
        [12; 16],
    )?;
    let bound = attach(
        &handler,
        incarnation,
        attach_request(
            conversation_id,
            participant_id,
            GEN_ONE,
            receipt.attach_secret(),
            [13; 16],
        ),
    )?;
    let current = generation(2)?;
    assert_eq!(bound.capability_generation(), current);

    let outcome = dispatch_outcome(
        &handler,
        incarnation,
        &mut ParticipantConnectionConversations::default(),
        attach_request(
            conversation_id,
            participant_id,
            GEN_ONE,
            bound.attach_secret(),
            [14; 16],
        ),
    )?;
    let ParticipantDispatch::Respond(response) = &outcome else {
        return Err(format!(
            "a stale-generation attach must be refused with a frame, got: {}",
            client_outcome(&outcome)
        )
        .into());
    };
    let value = decode_server_value(response)?;
    let ServerValue::StaleAuthority(StaleAuthority::Live {
        request: CommonStaleAuthorityEnvelope::CredentialAttach(envelope),
        current_generation,
    }) = value
    else {
        return Err(
            format!("expected the credential-attach StaleAuthority row, got: {value:?}").into(),
        );
    };
    assert_eq!(envelope.conversation_id, conversation_id);
    assert_eq!(
        current_generation, current,
        "the refusal must carry the CURRENT live generation so the client can re-drive"
    );
    Ok(())
}
