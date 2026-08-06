//! THE `#26` REPRO: does a marker-ack strand the sealed binding-fate token?
//!
//! The claim under test, which until now has rested ENTIRELY ON READING and
//! has never once been executed:
//!
//! > The marker-ack path advances the member's cursor WITHOUT progressing the
//! > sealed binding-fate token. It is a MISSING CALL, not a corrupt record.
//!
//! The reading behind it: `ops_acks.rs:147 ack_commit` calls
//! `progress_pending_binding_fate` at `:251` and only then applies the commit
//! to the member at `:252`. The marker-ack path — `commit_marker_ack` at
//! `:361` — applies to the member at `:422` with NO token progression
//! anywhere, and `:267-:532` contains ZERO occurrences of `binding_fate`. If
//! that reading is right, a marker-ack leaves the token's cursor behind the
//! member's, and the NEXT ordinary ack fails
//! `binding_fate.rs:78 previous_cursor != self.cursor` and is refused at
//! `ops_acks.rs:590`.
//!
//! # THIS UNIT ASSERTS THE CORRECT BEHAVIOUR, SO IT IS RED WHILE THE DEFECT LIVES
//!
//! It is written as the fix's red arm, not as a celebration of the bug: an
//! ordinary ack that follows a marker-ack MUST commit. It fails today if the
//! reading is right, and it goes green when the missing progression lands.
//!
//! ⚠ AND IF IT COMMITS TODAY, `#26` IS WRONG. That outcome is not a test
//! failure to be explained away — it is the finding, and it goes back to the
//! board as a retraction. The unit is built so that outcome is unmissable
//! rather than something a reader has to infer from a green tick.
//!
//! # WHY IT ARMS WITH THE LIVE GENERATION
//!
//! `attached_marker_fixture` drives the CredentialAttach that MINTS the fate
//! token — `prepare_marker_fixture` mints none, so the incident cannot exist
//! in it at all (`tests_marker_ack_fixture.rs:549-552`). But an attach also
//! advances the capability generation to its successor
//! (`ops_attach.rs:144-148`, `checked_add(1)`), and the armed fixture attaches
//! BOTH members (`tests_marker_ack_fixture.rs:748` requires both fate tokens).
//! So every post-attach request must carry `Generation(2)`, and this unit
//! READS the live generation off the publication's own binding epoch rather
//! than hardcoding one.
//!
//! That is not a stylistic preference. Cally's ruling `af528395`, quoted at
//! the head of `tests_f8_marker_poison.rs`, records that those units are
//! LOUDLY RED on exactly this arming precondition — a hardcoded
//! `Generation::ONE` against a live `Generation(2)` — so they never witness
//! the defect at their own assertions. ⛔ THOSE UNITS ARE NOT TO BE TOUCHED.
//! This one is built alongside them, arming the way they could not, and if it
//! witnesses the defect it is the thing that satisfies their declared expiry.

use std::error::Error;
use std::sync::Arc;

use liminal::durability::bridge::block_on;
use liminal_protocol::wire::{
    BindingEpoch, ClientRequest, Generation, MarkerAck, ParticipantAck, RecordAdmission,
    RecordAdmissionAttemptToken, ServerValue,
};

use super::ProductionParticipantHandler;
use super::log::OperationLog;
use super::outbox_log::{OutboxLog, OutboxRow, StoredMarkerAckCommitted};

use crate::server::participant::{ParticipantOfferedProgress, ParticipantSemanticHandler};

use super::tests::dispatch;
use super::tests_marker_ack_fixture::{
    FixtureAppender, MarkerFixture, attached_marker_fixture, marker_fixture_config,
    marker_fixture_facts,
};

/// Walks the target's publications to its marker, records the offer that makes
/// a marker-ack admissible, and returns the LIVE binding epoch it was offered
/// under.
///
/// The epoch is the point: it carries the generation production currently
/// considers authoritative, so nothing downstream has to guess one.
fn offer_marker_and_read_live_epoch(
    fixture: &MarkerFixture,
) -> Result<BindingEpoch, Box<dyn Error>> {
    let mut offered = None;
    let mut marker_publication = None;
    for _ in 0..8 {
        let publication = fixture
            .handler
            .next_publication(
                fixture.target_connection,
                fixture.marker_delivery.conversation_id,
                offered,
            )?
            .ok_or("marker fixture obligations ended before its marker")?;
        offered = Some(ParticipantOfferedProgress {
            binding_epoch: publication.binding_epoch,
            through_seq: publication.delivery_seq(),
        });
        if publication.delivery == fixture.marker_delivery {
            marker_publication = Some(publication);
            break;
        }
    }
    let publication =
        marker_publication.ok_or("marker was not reached within the signed fixture bound")?;
    let epoch = publication.binding_epoch;
    fixture.handler.record_publication_offer(&publication)?;
    Ok(epoch)
}

/// THE ARMING PROOF. If the fixture is not post-attach, there is no sealed
/// token to strand and this unit would be measuring nothing at all — the exact
/// failure mode that keeps the F8 units from ever claiming.
fn assert_armed_post_attach(epoch: BindingEpoch) -> Result<(), Box<dyn Error>> {
    if epoch.capability_generation == Generation::ONE {
        return Err(format!(
            "NOT ARMED: the live generation is {:?}, so this fixture never drove the \
             CredentialAttach that mints the sealed binding-fate token. Without that token there \
             is nothing for a marker-ack to strand and this unit witnesses NOTHING. Do not read a \
             pass here as evidence about #26.",
            epoch.capability_generation
        )
        .into());
    }
    Ok(())
}

/// Walks and RECORDS every further publication the target is offered, and
/// returns the highest delivery sequence it was actually offered.
///
/// An ack is only a question about fate if the thing being acked was really
/// delivered; without this the ack lands in a gap and measures nothing.
fn offer_everything_available(
    fixture: &MarkerFixture,
    from: ParticipantOfferedProgress,
) -> Result<u64, Box<dyn Error>> {
    let mut offered = Some(from);
    let mut highest = from.through_seq;
    for _ in 0..8 {
        let Some(publication) = fixture.handler.next_publication(
            fixture.target_connection,
            fixture.marker_delivery.conversation_id,
            offered,
        )?
        else {
            break;
        };
        offered = Some(ParticipantOfferedProgress {
            binding_epoch: publication.binding_epoch,
            through_seq: publication.delivery_seq(),
        });
        highest = highest.max(publication.delivery_seq());
        fixture.handler.record_publication_offer(&publication)?;
    }
    Ok(highest)
}

/// `#26`, EXECUTED AT LAST.
///
/// Red while the marker-ack path fails to progress the sealed token; green
/// when the missing call lands. If it is green TODAY, `#26` is wrong.
#[test]
fn an_ordinary_ack_after_a_marker_ack_must_still_commit() -> Result<(), Box<dyn Error>> {
    let fixture = attached_marker_fixture()?;
    let epoch = offer_marker_and_read_live_epoch(&fixture)?;
    assert_armed_post_attach(epoch)?;

    let conversation_id = fixture.marker_delivery.conversation_id;
    let marker_seq = fixture.marker_delivery.delivery_seq;

    // 1. The marker-ack. This is the step the reading says advances the
    //    member's cursor while leaving the sealed token where it was.
    let marker_acked = dispatch(
        &fixture.handler,
        fixture.target_connection,
        ClientRequest::MarkerAck(MarkerAck {
            conversation_id,
            participant_id: fixture.target_participant,
            capability_generation: epoch.capability_generation,
            marker_delivery_seq: marker_seq,
        }),
    )
    .map_err(|error| {
        format!(
            "NOT ARMED: the marker-ack itself was refused, so the divergence this unit tests \
                 for was never created: {error}"
        )
    })?;

    // 1b. ORDINARY TRAFFIC AFTER THE MARKER. Without this there is nothing
    //     beyond the marker to ack: the member's cursor already sits AT the
    //     marker, so an ack past it is an `AckGap` about a delivery that does
    //     not exist yet, which says nothing about the sealed token. The peer
    //     admits one ordinary record so a real delivery lands after the
    //     marker, and only then is the ordinary ack a question about fate.
    let admitted = dispatch(
        &fixture.handler,
        fixture.catchup_connection,
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id,
            participant_id: fixture.catchup_participant,
            capability_generation: epoch.capability_generation,
            record_admission_attempt_token: RecordAdmissionAttemptToken::new([0xF2; 16]),
            payload: vec![0xF2],
        }),
    )
    .map_err(|error| {
        format!(
            "NOT ARMED: no ordinary record could be admitted after the marker-ack, so there is \
             nothing past the marker for an ordinary ack to reach: {error}"
        )
    })?;
    // The admission's own success variant is not the arming proof; the ack
    // below is. If nothing actually landed after the marker, that ack comes
    // back as an `AckGap` and is reported as NOT ARMED there.
    let _ = &admitted;

    // 2. The ordinary ack that follows it. THIS is the assertion: it must
    //    commit. Under the reading it cannot, because
    //    `participant_ack_progressed` compares the member's now-advanced
    //    cursor against a token still sitting at the pre-marker value.
    let deliverable = offer_everything_available(
        &fixture,
        ParticipantOfferedProgress {
            binding_epoch: epoch,
            through_seq: marker_seq,
        },
    )?;
    if deliverable <= marker_seq {
        return Err(format!(
            "NOT ARMED: nothing beyond the marker at {marker_seq} was ever offered to the target \
             (highest offered {deliverable}), so an ordinary ack cannot reach past it and this \
             unit witnesses NOTHING. Admission answered {admitted:?}."
        )
        .into());
    }

    let ordinary = dispatch(
        &fixture.handler,
        fixture.target_connection,
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id,
            participant_id: fixture.target_participant,
            capability_generation: epoch.capability_generation,
            through_seq: deliverable,
        }),
    );

    match ordinary {
        Ok(ServerValue::AckCommitted(_)) => Ok(()),
        Ok(ServerValue::AckGap(gap)) => Err(format!(
            "NOT ARMED: the ordinary ack fell in a delivery gap ({gap:?}), so nothing was \
             actually delivered past the marker and the sealed token was never asked to \
             progress. This unit witnessed NOTHING about #26. Admission answered {admitted:?}."
        )
        .into()),
        Ok(other) => Err(format!(
            "the ordinary ack after a marker-ack neither committed nor refused; it answered \
             {other:?}. Marker-ack answered {marker_acked:?}."
        )
        .into()),
        Err(error) => {
            let rendered = error.to_string();
            assert!(
                !rendered.contains("sealed binding-fate authority"),
                "#26 REPRODUCED: the marker-ack advanced the member's cursor and left the sealed \
                 binding-fate token behind it, so the very next ordinary ack was refused at \
                 ops_acks.rs:590. This is the missing progress_pending_binding_fate call on the \
                 marker-ack path, executed rather than argued. Refusal: {rendered}"
            );
            Err(format!(
                "the ordinary ack after a marker-ack was refused, but NOT by the sealed \
                 binding-fate check this unit exists to catch. #26 is not what stopped it, and \
                 this refusal needs its own diagnosis before anything is concluded: {rendered}"
            )
            .into())
        }
    }
}

/// Commits one marker ack on an ARMED fixture at its live generation and returns
/// the durable extension row the cold path will later replay.
///
/// `tests_marker_ack::commit_exact_marker_ack` cannot be reused: it hardcodes
/// `Generation::ONE`, which is the exact arming precondition that keeps the F8
/// units from ever witnessing this defect.
fn commit_marker_ack_on_armed_fixture(
    fixture: &MarkerFixture,
    epoch: BindingEpoch,
) -> Result<StoredMarkerAckCommitted, Box<dyn Error>> {
    let conversation_id = fixture.marker_delivery.conversation_id;
    let outbox_log = OutboxLog::new(Arc::clone(&fixture.store), conversation_id);
    let rows_before = block_on(outbox_log.read_all())??.len();

    let committed = dispatch(
        &fixture.handler,
        fixture.target_connection,
        ClientRequest::MarkerAck(MarkerAck {
            conversation_id,
            participant_id: fixture.target_participant,
            capability_generation: epoch.capability_generation,
            marker_delivery_seq: fixture.marker_delivery.delivery_seq,
        }),
    )
    .map_err(|error| {
        format!("NOT ARMED: the live marker-ack was refused, so no durable extension row exists to replay: {error}")
    })?;
    if !matches!(committed, ServerValue::MarkerAckCommitted(_)) {
        return Err(
            format!("NOT ARMED: the marker-ack did not commit, it answered {committed:?}").into(),
        );
    }

    let rows = block_on(outbox_log.read_all())??;
    if rows.len() != rows_before + 1 {
        return Err(format!(
            "NOT ARMED: the marker-ack wrote {} extension rows, not exactly one",
            rows.len().saturating_sub(rows_before)
        )
        .into());
    }
    let Some((_, OutboxRow::MarkerAckCommitted(stored))) = rows.last() else {
        return Err("NOT ARMED: the last durable row is not a MarkerAckCommitted extension".into());
    };
    Ok(stored.clone())
}

/// SITE TWO: the same missing progression on the COLD REPLAY path.
///
/// `replay_marker_ack_extension` advances the member's cursor and, exactly like
/// `commit_marker_ack`, progresses no sealed binding-fate token. If that reading
/// is right the defect is REBUILT ON EVERY BOOT from durable state, which is a
/// different and worse class than a live-only fault: it would be self-reproducing
/// across restarts.
///
/// # ⛔ WHY THIS DOES NOT COMPARE THE REPLAY SNAPSHOT TO THE LIVE ONE
///
/// That is the obvious assertion and it is VACUOUS here. The live path carries
/// the same defect, so both sides freeze identically and the comparison is GREEN
/// IN BOTH WORLDS — with the fix and without it. The only assertion that
/// discriminates is a STATE one: after the cold replay, an ordinary ack must
/// still commit.
///
/// ⚠ AND THAT VACUOUS COMPARISON IS ALREADY IN THE TREE:
/// `tests_marker_ack::assert_marker_replay` ends at
/// `assert_eq!(replay_snapshot, live_snapshot)`. It passes today WITH the defect
/// and would pass tomorrow WITHOUT it. It is not wrong about what it asserts —
/// live and cold agreeing IS worth pinning — but it cannot be cited as evidence
/// that cold replay is healthy, and this unit exists because it cannot.
///
/// # WHY THIS REPLAYS THE REAL STORE INSTEAD OF HAND-CALLING THE EXTENSION
///
/// THE FIRST TWO ATTEMPTS BUILT A SECOND FIXTURE AND CALLED
/// `replay_marker_ack_extension` BY HAND, AND BOTH FAILED AS APPARATUS FAULTS —
/// once on a poststate audit drift, once at the row's base log head (stored 19
/// vs cold 20). The diagnosis is worth keeping because it is not a coding slip:
/// **A SECOND FRESH FIXTURE IS NOT A COLD NODE.** It has its own store and never
/// contains the live node's rows, so NO ordering of hand-calls can make it
/// consistent — the arm was reporting on a node that had never lived the history
/// it was being asked to resume.
///
/// The real cold path needs no hand-call at all. `ConversationAuthority::replay`
/// is the only load path, and it reaches the extension by itself:
///   `ops_session_replay.rs:71/:183` -> `recipient_ack_obligations`
///   -> `outbox_replay.rs:107/:261` -> `replay_marker_ack_extension`
/// Those are the ONLY two production callers of the extension replay in the
/// crate; every other hit is a test. So reopening the live store and asking for
/// its replayed aggregate exercises site two exactly as a boot would, and the
/// unit asserts on state the loader built rather than on state it staged itself.
#[test]
fn a_cold_replayed_marker_ack_must_leave_the_sealed_token_in_step() -> Result<(), Box<dyn Error>> {
    let live = attached_marker_fixture()?;
    let epoch = offer_marker_and_read_live_epoch(&live)?;
    assert_armed_post_attach(epoch)?;

    let conversation_id = live.marker_delivery.conversation_id;
    let marker_seq = live.marker_delivery.delivery_seq;

    // THE WHOLE LIVE HISTORY, EXCEPT THE FINAL ORDINARY ACK. The ack is the
    // question this unit puts to the COLD node, so it must not be answered here.
    let stored = commit_marker_ack_on_armed_fixture(&live, epoch)?;

    let admitted = dispatch(
        &live.handler,
        live.catchup_connection,
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id,
            participant_id: live.catchup_participant,
            capability_generation: epoch.capability_generation,
            record_admission_attempt_token: RecordAdmissionAttemptToken::new([0xF3; 16]),
            payload: vec![0xF3],
        }),
    )
    .map_err(|error| {
        format!(
            "NOT ARMED: no ordinary record could be admitted after the marker-ack, so nothing \
             exists past the marker for the cold node's ordinary ack to reach: {error}"
        )
    })?;

    // Offering is what makes the later ack a question about FATE rather than a
    // gap, and the offers are durable, so the cold node inherits them.
    let deliverable = offer_everything_available(
        &live,
        ParticipantOfferedProgress {
            binding_epoch: epoch,
            through_seq: marker_seq,
        },
    )?;
    if deliverable <= marker_seq {
        return Err(format!(
            "NOT ARMED: nothing beyond the marker at {marker_seq} was ever offered (highest \
             offered {deliverable}), so no ordinary ack can reach past it on the cold node \
             either. Admission answered {admitted:?}."
        )
        .into());
    }

    // ===================== THE COLD BOOT =====================
    // A NEW handler over the SAME durable store. Nothing of the live handler's
    // in-memory state crosses this line; everything the cold node knows, it
    // rebuilt from rows — including the marker-ack extension.
    let config = marker_fixture_config();
    let cold = ProductionParticipantHandler::new(Arc::clone(&live.store), config.clone())?;
    let cold_log = OperationLog::new(Arc::clone(&live.store), conversation_id);
    let mut replayed = cold.replay_aggregate_reference(conversation_id, &cold_log)?;

    // ARMING PROOF #42: if the loader never consumed the extension row, this unit
    // is measuring a node that never replayed a marker-ack, and a pass here would
    // mean nothing whatever about site two.
    if replayed.next_log_sequence <= stored.base_log_head {
        return Err(format!(
            "NOT ARMED: the cold node replayed to log head {} but the marker-ack extension was \
             written against base head {}, so the row was never consumed by this replay. This is \
             an APPARATUS fault, not a verdict about #26.",
            replayed.next_log_sequence, stored.base_log_head
        )
        .into());
    }

    // THE ASSERTION: on state the LOADER built, the next ordinary ack must commit.
    let request = ParticipantAck {
        conversation_id,
        participant_id: live.target_participant,
        capability_generation: epoch.capability_generation,
        through_seq: deliverable,
    };
    let outcome = replayed.apply_ack(
        &request,
        &marker_fixture_facts(live.target_connection, &config)?,
        &FixtureAppender { log: &cold_log },
    );

    match outcome {
        Ok(arm) => match arm.value {
            ServerValue::AckCommitted(_) => Ok(()),
            ServerValue::AckGap(gap) => Err(format!(
                "NOT ARMED: the ordinary ack fell in a delivery gap ({gap:?}) on the cold node, so \
                 the sealed token was never asked to progress and this unit witnessed NOTHING."
            )
            .into()),
            other => Err(format!(
                "the ordinary ack after a COLD REPLAYED marker-ack neither committed nor refused; \
                 it answered {other:?}."
            )
            .into()),
        },
        Err(error) => {
            let rendered = error.to_string();
            assert!(
                !rendered.contains("sealed binding-fate authority"),
                "#26 SITE TWO REPRODUCED: the production load path replayed the marker-ack \
                 extension, advanced the member's cursor from durable state, and left the sealed \
                 binding-fate token behind it. THE DEFECT IS REBUILT ON EVERY BOOT, which makes it \
                 self-reproducing across restarts rather than a live-only fault. Refusal: {rendered}"
            );
            Err(format!(
                "the ordinary ack after a cold-replayed marker-ack was refused, but NOT by the \
                 sealed binding-fate check this unit exists to catch, so it needs its own \
                 diagnosis before anything is concluded: {rendered}"
            )
            .into())
        }
    }
}
