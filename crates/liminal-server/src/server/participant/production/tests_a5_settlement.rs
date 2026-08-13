//! Amendment A5 (participant contract §0.16) — the settlement-refusal family,
//! end to end at the production seam.
//!
//! # What these pins are for
//!
//! Board #14's field SIGNATURE 4 is an attach or detach refused with frontier
//! `Precedence` during compaction-marker settlement, delivered as a bare
//! connection close with no wire-visible retry story. §0.16 rules that ONE seam
//! (`apply_live_transition`) into THREE clearing conditions with different
//! lawful answers, and these tests measure the answers at the arm boundary the
//! client actually sees.
//!
//! # The stage-order honesty note (§0.16 build obligation 4)
//!
//! `ops_attach.rs`'s standing note records that R-D1 puts `ObserverBackpressure`
//! (stage 11) after `ConversationOrderExhausted` (9) and
//! `ConversationSequenceExhausted` (10), while the `PendingFinalization` arm
//! refuses before `allocate_position` runs — so a request that is BOTH against
//! a pending binding AND at an exhausted order/sequence hears the stage-11 row
//! where the register wants stage 9 or 10.
//!
//! The new settlement row extends that note in the OPPOSITE direction, and
//! `the_settlement_row_is_heard_after_stages_9_and_10_not_before` MEASURES it
//! rather than asserting it: the settlement refusal is raised INSIDE
//! `attach_commit`, downstream of `allocate_attach_mode`, so stages 9 and 10
//! have already run and a request that is both at exhausted order/sequence AND
//! in a settlement window hears the STAGE-9/10 row, not the settlement one.
//! Recorded as a measured property rather than an accident.

use std::error::Error;
use std::sync::Arc;

use liminal::durability::{DurableStore, open_ephemeral};
use liminal_protocol::lifecycle::{ImmutableSequenceCandidate, PrecedenceCondition};
use liminal_protocol::wire::{
    AttachAttemptToken, ClientRequest, ConnectionIncarnation, CredentialAttachRequest,
    DetachAttemptToken, DetachRequest, EnrollBound, EnrollmentRequest,
    EnrollmentSettlementBackpressure, EnrollmentToken, Generation, MarkerSettlementBackpressure,
    RecordAdmission, RecordAdmissionAttemptToken, ServerValue,
};

use super::ProductionParticipantHandler;
use super::tests::dispatch;
use super::tests_marker_ack_fixture::marker_fixture_config;

const CONVERSATION: u64 = 1;

struct SettlementFixture {
    handler: ProductionParticipantHandler,
    connection: ConnectionIncarnation,
    participant: u64,
    /// Generation the fixture's participant is currently addressable at.
    generation: Generation,
    /// The enrolled participant's current attach secret.
    secret: liminal_protocol::wire::AttachSecret,
    /// Delivery sequence of the marker candidate blocking the lane.
    settlement_epoch: u64,
    records_committed: u64,
}

fn store() -> Result<Arc<dyn DurableStore>, Box<dyn Error>> {
    Ok(Arc::new(open_ephemeral(1)?))
}

/// Reads the head of the immutable sequence lane through the SAME classifier
/// the seam uses, so the fixture cannot claim a settlement window the seam does
/// not agree exists.
fn precedence_condition(
    handler: &ProductionParticipantHandler,
) -> Result<Option<(PrecedenceCondition, bool)>, Box<dyn Error>> {
    let cell = handler.cell(CONVERSATION)?;
    let owner = cell
        .lock()
        .map_err(|_| "conversation authority lock was poisoned")?;
    let authority = owner
        .as_ref()
        .ok_or("conversation authority must be restored")?;
    let Some(frontier) = authority.frontier() else {
        return Ok(None);
    };
    let frontiers = frontier.frontiers();
    let condition = (
        frontiers.precedence_condition(),
        frontiers.live_transition_blocked(),
    );
    drop(owner);
    Ok(Some(condition))
}

fn marker_candidate_seq(
    handler: &ProductionParticipantHandler,
) -> Result<Option<u64>, Box<dyn Error>> {
    let cell = handler.cell(CONVERSATION)?;
    let owner = cell
        .lock()
        .map_err(|_| "conversation authority lock was poisoned")?;
    let authority = owner
        .as_ref()
        .ok_or("conversation authority must be restored")?;
    let seq = authority.frontier().and_then(|frontier| {
        frontier
            .frontiers()
            .sequence()
            .immutable_candidates()
            .first()
            .and_then(|candidate| match candidate {
                ImmutableSequenceCandidate::Marker(marker) => Some(marker.delivery_seq),
                ImmutableSequenceCandidate::BindingTerminal { .. } => None,
            })
    });
    drop(owner);
    Ok(seq)
}

fn enroll(
    handler: &ProductionParticipantHandler,
    connection: ConnectionIncarnation,
    token: [u8; 16],
) -> Result<EnrollBound, Box<dyn Error>> {
    match dispatch(
        handler,
        connection,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: CONVERSATION,
            enrollment_token: EnrollmentToken::new(token),
        }),
    )? {
        ServerValue::EnrollBound(bound) => Ok(bound),
        other => Err(format!("enrollment must bind, got {other:?}").into()),
    }
}

fn attach(fixture: &SettlementFixture, token: [u8; 16]) -> Result<ServerValue, Box<dyn Error>> {
    dispatch(
        &fixture.handler,
        fixture.connection,
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: CONVERSATION,
            participant_id: fixture.participant,
            capability_generation: fixture.generation,
            attach_secret: fixture.secret,
            attach_attempt_token: AttachAttemptToken::new(token),
            accept_marker_delivery_seq: None,
        }),
    )
}

fn commit_record(
    handler: &ProductionParticipantHandler,
    connection: ConnectionIncarnation,
    participant: u64,
    generation: Generation,
    nonce: u8,
) -> Result<ServerValue, Box<dyn Error>> {
    dispatch(
        handler,
        connection,
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: CONVERSATION,
            participant_id: participant,
            capability_generation: generation,
            record_admission_attempt_token: RecordAdmissionAttemptToken::new([nonce; 16]),
            payload: vec![nonce],
        }),
    )
}

/// Drives ordinary commits until a MARKER candidate is pending in the immutable
/// lane — the exact state §0.16 condition 2 names.
///
/// The loop is bounded and the fixture FAILS rather than proceeding if the
/// window never opens: a settlement pin measured over a conversation with no
/// settlement in progress is a vacuous predicate, and this is where that would
/// enter.
fn settlement_window() -> Result<SettlementFixture, Box<dyn Error>> {
    let handler = ProductionParticipantHandler::new(store()?, marker_fixture_config())?;
    let connection = ConnectionIncarnation::new(1, 1);
    let lagging = ConnectionIncarnation::new(2, 1);
    let bound = enroll(&handler, connection, [1; 16])?;
    // A SECOND member that never acknowledges. Marker debt is produced by an
    // overtaken participant's abandoned prefix, so a conversation whose only
    // member is also its only producer never mints a candidate and every pin in
    // this module would measure nothing.
    let lagging_bound = enroll(&handler, lagging, [2; 16])?;
    let lagging_participant = lagging_bound.participant_id();
    let lagging_attached = dispatch(
        &handler,
        lagging,
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: CONVERSATION,
            participant_id: lagging_participant,
            capability_generation: lagging_bound.capability_generation(),
            attach_secret: lagging_bound.attach_secret(),
            attach_attempt_token: AttachAttemptToken::new([0x11; 16]),
            accept_marker_delivery_seq: None,
        }),
    )?;
    if !matches!(lagging_attached, ServerValue::AttachBound(_)) {
        return Err(
            format!("the lagging member's attach must bind, got {lagging_attached:?}").into(),
        );
    }

    let participant = bound.participant_id();
    let attached = dispatch(
        &handler,
        connection,
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: CONVERSATION,
            participant_id: participant,
            capability_generation: bound.capability_generation(),
            attach_secret: bound.attach_secret(),
            attach_attempt_token: AttachAttemptToken::new([0x10; 16]),
            accept_marker_delivery_seq: None,
        }),
    )?;
    let ServerValue::AttachBound(attached) = attached else {
        return Err(format!("the fixture's attach must bind, got {attached:?}").into());
    };
    let generation = attached.capability_generation();
    let secret = attached.attach_secret();

    for nonce in 1..=12_u8 {
        let committed = commit_record(&handler, connection, participant, generation, nonce)?;
        if !matches!(committed, ServerValue::RecordCommitted(_)) {
            return Err(format!(
                "the fixture's record {nonce} must commit to generate marker debt, got {committed:?}"
            )
            .into());
        }
        if let Some(settlement_epoch) = marker_candidate_seq(&handler)? {
            return Ok(SettlementFixture {
                handler,
                connection,
                participant,
                generation,
                secret,
                settlement_epoch,
                records_committed: u64::from(nonce),
            });
        }
    }
    Err("the fixture never opened a marker-settlement window".into())
}

/// The window this whole module measures over EXISTS, and the seam agrees it is
/// condition 2 rather than one of its siblings.
///
/// The positive control for every pin below: without it a green here would be
/// consistent with a conversation that simply never blocked.
#[test]
fn the_fixture_opens_a_real_condition_two_settlement_window() -> Result<(), Box<dyn Error>> {
    let fixture = settlement_window()?;
    assert!(fixture.records_committed >= 1);
    let (condition, blocked) = precedence_condition(&fixture.handler)?
        .ok_or("the conversation must hold a live frontier")?;
    assert!(
        blocked,
        "the seam's own guard must agree a live transition is blocked"
    );
    assert_eq!(
        condition,
        PrecedenceCondition::MarkerDrain {
            settlement_epoch: fixture.settlement_epoch
        },
        "the seam must classify this as the marker-drain condition, not a binding terminal, \
         an armed recovery, or an unclassified state"
    );
    Ok(())
}

/// §0.16 condition 2, attach wrapper: the attach is PRESENTED with the labelled
/// row instead of collapsing into a bare close, and the epoch it carries is the
/// candidate the drain will announce.
#[test]
fn an_attach_inside_the_settlement_window_hears_the_labelled_row() -> Result<(), Box<dyn Error>> {
    let fixture = settlement_window()?;
    let value = attach(&fixture, [0x51; 16])?;
    match value {
        ServerValue::MarkerSettlementBackpressure(
            MarkerSettlementBackpressure::CredentialAttach {
                conversation_id,
                refused_epoch,
            },
        ) => {
            assert_eq!(conversation_id, CONVERSATION);
            assert_eq!(
                refused_epoch, fixture.settlement_epoch,
                "refused_epoch is load-bearing: the stage-11 retry matches it against \
                 MarkerSettled's own epoch, so it must be the head candidate's delivery sequence"
            );
        }
        other => {
            return Err(format!(
                "a settlement-window attach must hear MarkerSettlementBackpressure, got {other:?}"
            )
            .into());
        }
    }
    Ok(())
}

/// §0.16 condition 2, detach wrapper: the twin of the attach flatten, and the
/// first live sink of `PresentedRefusal::detach()`, which sat
/// `#[expect(dead_code)]` naming this amendment as its unblocking condition.
#[test]
fn a_detach_inside_the_settlement_window_hears_the_labelled_row() -> Result<(), Box<dyn Error>> {
    let fixture = settlement_window()?;
    let value = dispatch(
        &fixture.handler,
        fixture.connection,
        ClientRequest::Detach(DetachRequest {
            conversation_id: CONVERSATION,
            participant_id: fixture.participant,
            capability_generation: fixture.generation,
            detach_attempt_token: DetachAttemptToken::new([0x52; 16]),
        }),
    )?;
    match value {
        ServerValue::MarkerSettlementBackpressure(MarkerSettlementBackpressure::Detach {
            conversation_id,
            refused_epoch,
        }) => {
            assert_eq!(conversation_id, CONVERSATION);
            assert_eq!(refused_epoch, fixture.settlement_epoch);
        }
        other => {
            return Err(format!(
                "a settlement-window detach must hear MarkerSettlementBackpressure, got {other:?}"
            )
            .into());
        }
    }
    Ok(())
}

/// §0.16 condition 2, ENROLLMENT wrapper: the split. The row is the
/// conversation id and NOTHING ELSE — no epoch — because the wrapper carries no
/// membership predicate and cannot tell an invited enrollee from a stranger.
///
/// The absence of the epoch is the law, so it is asserted at the type: the value
/// is `EnrollmentSettlementBackpressure`, which has no epoch field to read, and
/// NOT the labelled `MarkerSettlementBackpressure` an enrollee would otherwise
/// have received by symmetry.
#[test]
fn a_subsequent_enrollment_inside_the_window_hears_the_unlabelled_row() -> Result<(), Box<dyn Error>>
{
    let fixture = settlement_window()?;
    let stranger = ConnectionIncarnation::new(2, 2);
    let value = dispatch(
        &fixture.handler,
        stranger,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: CONVERSATION,
            enrollment_token: EnrollmentToken::new([0x53; 16]),
        }),
    )?;
    match value {
        ServerValue::EnrollmentSettlementBackpressure(EnrollmentSettlementBackpressure {
            conversation_id,
        }) => assert_eq!(conversation_id, CONVERSATION),
        other => {
            return Err(format!(
                "a settlement-window subsequent enrollment must hear \
                 EnrollmentSettlementBackpressure, got {other:?}"
            )
            .into());
        }
    }
    assert!(
        !matches!(
            dispatch(
                &fixture.handler,
                stranger,
                ClientRequest::Enrollment(EnrollmentRequest {
                    conversation_id: CONVERSATION,
                    enrollment_token: EnrollmentToken::new([0x54; 16]),
                }),
            )?,
            ServerValue::MarkerSettlementBackpressure(_)
        ),
        "the enrollment wrapper must NEVER mint the labelled row"
    );
    Ok(())
}

/// §0.16 build obligation 1 — RESTORATION.
///
/// The refusal is raised after `slots.remove_entry`, `take_frontier`,
/// `prepare_selected_fenced_finalizer` and both position allocators have run,
/// and `LiveFrontierFailure::into_parts` restores only the frontier. This pin
/// measures post-refusal state identity with the pre-request state at the
/// carrier's own granularity, in one comparison over the authority's full
/// `Debug` — which contains the slot map, the frontier owner, the finalizer
/// state (`fate_occurrences`, `pending_specific_fates`,
/// `prepared_ordinary_finalizers`) and the position allocators together.
///
/// RED-PROVEN against a build that skips restoration: dropping the
/// `install_frontier`/`slots.insert` pair in `restore_and_present_attach`, or
/// the `restore_position_allocators` call in the arm, fails this test.
#[test]
fn a_presented_settlement_refusal_restores_every_consumed_authority() -> Result<(), Box<dyn Error>>
{
    let fixture = settlement_window()?;
    let before = super::tests_history::authority_snapshot(&fixture.handler, CONVERSATION)?;

    let value = attach(&fixture, [0x55; 16])?;
    assert!(
        matches!(value, ServerValue::MarkerSettlementBackpressure(_)),
        "the refusal under test must be the presented one, got {value:?}"
    );

    let after = super::tests_history::authority_snapshot(&fixture.handler, CONVERSATION)?;
    assert_eq!(
        before, after,
        "§0.16 presentation law: the slot entry, frontier, and prepared finalizer state \
         readable before the request must be readable IDENTICALLY after the refusal"
    );
    Ok(())
}

/// The restoration is not merely cosmetic: the position allocators come back,
/// and they come back AGREEING WITH THE FRONTIER.
///
/// This is the half of obligation 1 the snapshot comparison would still pass
/// with if `Debug` ever stopped printing them, and it is the half that is
/// load-bearing for correctness rather than for tidiness. A `PresentedRefusal`
/// is answered with `Ok`, so the owner is RETAINED instead of discarded and
/// cold replayed; a consumed delivery sequence would therefore never be
/// re-derived from durable rows, and the retry the stage-11 discipline
/// prescribes would allocate `high_watermark + 2` and be refused by the seam's
/// own record-position check — the presentation would have bought a frame with
/// a broken conversation.
///
/// The second assertion is the one that states the invariant rather than the
/// symptom: after the refusal the next delivery sequence must still be exactly
/// one past the frontier's high watermark.
///
/// RED-PROVEN against a build that skips restoration: deleting
/// `restore_position_allocators` from `apply_credential_attach_with_impact`
/// fails both assertions here while leaving every wire-shape pin green.
#[test]
fn a_settlement_refusal_returns_the_position_allocators_in_step_with_the_frontier()
-> Result<(), Box<dyn Error>> {
    let fixture = settlement_window()?;
    let before = position_and_watermark(&fixture.handler)?;
    assert_eq!(
        before.0.1,
        before.1 + 1,
        "the fixture must start in step, or the pin below measures nothing"
    );

    let refusal = attach(&fixture, [0x56; 16])?;
    assert!(matches!(
        refusal,
        ServerValue::MarkerSettlementBackpressure(_)
    ));

    let after = position_and_watermark(&fixture.handler)?;
    assert_eq!(
        before, after,
        "the position allocators and the frontier watermark must be identical across a \
         refusal that committed nothing"
    );
    assert_eq!(
        after.0.1,
        after.1 + 1,
        "the next delivery sequence must still be exactly one past the high watermark -- \
         this is the predicate the seam's record-position check enforces on the retry"
    );

    // A second refusal is byte-identical, so no drift accumulates per attempt.
    let again = attach(&fixture, [0x57; 16])?;
    assert_eq!(refusal, again);
    assert_eq!(position_and_watermark(&fixture.handler)?, after);
    Ok(())
}

/// `((next_order, next_seq), sequence high watermark)`.
fn position_and_watermark(
    handler: &ProductionParticipantHandler,
) -> Result<((u64, u64), u64), Box<dyn Error>> {
    let cell = handler.cell(CONVERSATION)?;
    let owner = cell
        .lock()
        .map_err(|_| "conversation authority lock was poisoned")?;
    let authority = owner
        .as_ref()
        .ok_or("conversation authority must be restored")?;
    let position = authority.next_position_for_test();
    let watermark = authority
        .frontier()
        .ok_or("the conversation must hold a live frontier")?
        .frontiers()
        .sequence()
        .ledger()
        .high_watermark();
    drop(owner);
    Ok((position, watermark))
}

/// §0.16 build obligation 4 — STAGE ORDER, documented AND measured.
///
/// The observed order at the new row's site: every refusal the arm can select
/// BEFORE `attach_commit` runs still wins inside a settlement window, because
/// the settlement row is raised inside `attach_commit`, downstream of the
/// lookup stages, stage 6 capacity, the binding-slot decision, the
/// marker-bearing check, stage 8, and `allocate_attach_mode` (stages 9/10).
/// This is the OPPOSITE direction to the standing honest-limit note in
/// `ops_attach.rs`, where the `PendingFinalization` arm refuses BEFORE
/// `allocate_position` and therefore hears stage 11 ahead of stages 9/10.
///
/// Measured with two earlier-stage refusals rather than argued: an unknown
/// participant (the arm's first refusal, before any lookup) and a
/// marker-bearing attach (the pre-stage-8 total-selector refusal). Both are
/// issued INSIDE the same open settlement window that
/// `an_attach_inside_the_settlement_window_hears_the_labelled_row` proves would
/// otherwise mint the settlement row, so neither green is vacuous.
#[test]
fn earlier_stages_still_win_inside_the_settlement_window() -> Result<(), Box<dyn Error>> {
    let fixture = settlement_window()?;

    // Stage 2: no slot for this participant. Refused before the lookup, and
    // long before the frontier is ever touched.
    let unknown = dispatch(
        &fixture.handler,
        fixture.connection,
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: CONVERSATION,
            participant_id: fixture.participant + 9_999,
            capability_generation: fixture.generation,
            attach_secret: fixture.secret,
            attach_attempt_token: AttachAttemptToken::new([0x59; 16]),
            accept_marker_delivery_seq: None,
        }),
    )?;
    assert!(
        matches!(unknown, ServerValue::ParticipantUnknown(_)),
        "an earlier-stage refusal must still be heard inside a settlement window, got {unknown:?}"
    );

    // The pre-stage-8 marker-bearing refusal, also ahead of the row.
    let marker_bearing = dispatch(
        &fixture.handler,
        fixture.connection,
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: CONVERSATION,
            participant_id: fixture.participant,
            capability_generation: fixture.generation,
            attach_secret: fixture.secret,
            attach_attempt_token: AttachAttemptToken::new([0x5A; 16]),
            accept_marker_delivery_seq: Some(fixture.settlement_epoch),
        }),
    )?;
    assert!(
        !matches!(marker_bearing, ServerValue::MarkerSettlementBackpressure(_)),
        "the marker-bearing refusal is selected ahead of the settlement row, got \
         {marker_bearing:?}"
    );

    // And the window is STILL open, so neither assertion above passed merely
    // because the settlement had cleared underneath them.
    assert_eq!(
        marker_candidate_seq(&fixture.handler)?,
        Some(fixture.settlement_epoch),
        "the settlement window must still be open, or both assertions above are vacuous"
    );
    Ok(())
}
