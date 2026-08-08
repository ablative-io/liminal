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
//! `attached_marker_fixture` drives the `CredentialAttach` that MINTS the fate
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

use crate::server::participant::{
    ParticipantConnectionConversations, ParticipantOfferedProgress, ParticipantSemanticHandler,
};

use super::tests::{dispatch, dispatch_tracked};
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
pub(super) fn offer_marker_and_read_live_epoch(
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
pub(super) fn assert_armed_post_attach(epoch: BindingEpoch) -> Result<(), Box<dyn Error>> {
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
    let cold = ProductionParticipantHandler::new(Arc::clone(&live.store), config)?;
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

/// SITE ONE'S PIN — the CACHED-AUTHORITY window, which neither unit above can see.
///
/// # Why this unit had to exist, stated against my own claim
///
/// I asserted the two sites were independent: kill `ops_acks.rs:426` and only
/// the live unit reddens. THE INDEPENDENCE MATRIX FALSIFIED THAT. Measured
/// three times — my run, and Waffles's arms A/B/C in a separate tree — killing
/// `:426` ALONE reddens NOTHING, while killing `:524` alone reddens BOTH. Both
/// units above were riding the REPLAY call. Site one was pinned by nothing, and
/// the commit message said otherwise.
///
/// # The mechanism, which is why a THIRD unit and not a tweak to the first
///
/// The handler CACHES the authority across requests
/// (`handler.rs:69`, `Mutex<HashMap<_, Arc<Mutex<Option<ConversationAuthority>>>>>`).
/// After an operation succeeds, `with_conversation_reconciliation` REPLACES that
/// cached owner with a fresh replay — but ONLY if the operation appended to the
/// operation log (`handler.rs:492`, `next_log_sequence > starting_log_sequence`,
/// re-replay at `:498`, install at `:504`). Otherwise `:513` RETAINS the mutated
/// in-memory authority.
///
/// ⚡ AND A MARKER-ACK STRUCTURALLY CANNOT APPEND TO THE OPERATION LOG. Its arm
/// at `handler_semantic.rs:136` binds the appender as `_appender` — UNUSED —
/// where the ordinary ack at `:127` uses it; `commit_marker_ack` writes only the
/// OUTBOX log (`ops_acks.rs:414`) and merely READS `next_log_sequence` at `:402`
/// as `base_log_head`. ⇒ A marker-ack never trips `:492`, so the owner it
/// mutated survives into the next request WITH NO REPLAY BEHIND IT. That window
/// is the one and only place site one is the sole protection.
///
/// # ⛔ CELL 3'S RED IS A PROPERTY OF THE CODE, NOT A DEFECT HERE. DO NOT "FIX" IT.
///
/// The natural isolation bar for this unit is "site 1 present + site 2 absent =>
/// GREEN", proving it pins site 1 alone rather than the union. **THAT CELL
/// CANNOT BE MADE GREEN BY ANY *APPENDING* OBSERVATION THAT JUDGES ON THE
/// REQUEST'S OUTCOME.**
///
/// ⚠ THAT IS THE NARROW CLAIM AND IT IS DELIBERATELY NARROW. A NON-APPENDING
/// observation never enters the reconcile arm at all, and an observation that
/// judges on DURABLE ROWS rather than the request's outcome is not covered
/// either. ⛔ DO NOT UPGRADE THIS TO "UNSATISFIABLE BY CONSTRUCTION" — an
/// overstated impossibility is exactly what stops a future reader finding a real
/// gap, and the wide version of this sentence was corrected out of this comment
/// once already. Measured ordering, probes at the entry of
/// `commit_marker_ack` / `replay_marker_ack_extension` / `ack_commit` /
/// `replay_and_repair`, with NO loader-calling probes in the test itself —
/// byte-identical in the both-present and site-2-absent cells:
///
/// ```text
/// SITE 1                <- the marker-ack commit
/// operation_facts       <- the ordinary ack's dispatch begins
/// ORDINARY ack_commit   <- THE ACK RUNS FIRST, on the cached un-replayed authority
/// replay_and_repair     <- POST-op reconcile (handler.rs:498), the ack having appended
///   ack_commit x7 -> SITE 2 -> ack_commit x1
/// ```
///
/// ⇒ No replay precedes the ack's commit, so site 1's window is real. But the
/// ack APPENDS, so the post-op reconcile fires and traverses site 2 — and it
/// returns THAT failure as the REQUEST's failure even though the operation
/// succeeded. The arm is in `ProductionParticipantHandler::
/// with_conversation_reconciliation`, gated on
/// `reconcile_appended_source && next_log_sequence > starting_log_sequence`,
/// and on reconcile `Err` it does `*owner = None; (Err(error), false)`,
/// DISCARDING the operation's own `Ok(value)`.
///
/// ⚠ CITED BY ENCLOSING FUNCTION ON PURPOSE — AND FOR THE RIGHT REASON: a
/// `file:line` carries an IMPLICIT REV and is meaningless without one — AND that
/// rev must be shown to be IN THE SET UNDER REVIEW. Naming a rev makes a citation
/// CHECKABLE; proving it an ancestor of the thing under review makes it RELEVANT.
/// The disagreement that produced this comment was neither drift inside the
/// review set nor identical trees: it was ONE READER STANDING OUTSIDE THE SET,
/// citing `1646ed2` — a perfectly nameable rev, not an ancestor of `70f5a19`,
/// whose `handler.rs` is 667 lines (blob `3d1af92e`) against 814 here. The files
/// differed by 147 lines and the BLOCK merely happened to match. Within the
/// review set proper, `handler.rs` IS BYTE-IDENTICAL across `5bdf5df`,
/// `6673d8d` and `70f5a19` (blob
/// `0ca75423decbde6f333d40789ef409f1ac34495c` at all three), and the arm is at
/// `:491-511` with the `Err`/`has_staged` sibling at `:519-543`, AT THOSE REVS.
///
/// ⛔ AN EARLIER DRAFT OF THIS COMMENT RECORDED TWO TREE-TAGGED READINGS AS IF
/// BOTH WERE VALID. They were not — one was simply wrong. ACCOMMODATING A
/// DISCREPANCY DESTROYS IT AS EVIDENCE: two readings that disagree are a working
/// detector, and "both are correct, tagged by tree" switches it off without
/// resolving anything, then preserves the wrong one where later readers will
/// take it as authoritative. ⇒ RECONCILE DISAGREEING READINGS TO ONE.
///
/// ⇒ **THE GATE IS WHY THE CLAIM IS NARROW: an operation that does not append
/// never reaches this arm.**
///
/// ⛔ **AND THE ACK COMMITS ANYWAY IN THAT CELL:** the reconcile's replay
/// re-executes an `ack_commit` AFTER `SITE 2`, and outbox ordering places the
/// ack's own row after the marker row — that re-execution is present in BOTH
/// cells, so the row was written. A COMMIT-THEN-RECONCILE-FAILURE IS
/// TEXT-IDENTICAL TO A REFUSAL; only durable rows tell them apart.
///
/// ⛔ THE FIRST UNIT MISSES IT BY ONE STEP: it admits an ordinary record BETWEEN
/// the two acks (`:192`), and a record admission DOES append, so `:492` fires
/// and the cached owner is thrown away and rebuilt by site two. The unit then
/// measures site two while appearing to measure site one. HERE EVERY APPENDING
/// STEP HAPPENS BEFORE THE MARKER-ACK, so nothing whatever runs between the two
/// acks.
#[test]
#[allow(
    clippy::too_many_lines,
    reason = "the unit's whole claim is that NOTHING runs between the two acks, \
              so the sequence has to stay in one body -- extracting helpers \
              would insert exactly the indirection the test exists to rule out."
)]
fn a_marker_ack_must_not_wedge_the_cached_authority_for_the_next_ordinary_ack()
-> Result<(), Box<dyn Error>> {
    let fixture = attached_marker_fixture()?;
    let conversation_id = fixture.marker_delivery.conversation_id;
    let marker_seq = fixture.marker_delivery.delivery_seq;

    // 1. EVERY APPENDING STEP HAPPENS FIRST — and "first" here means before the
    //    marker is even OFFERED, not merely before it is acked.
    //
    //    ⚠ MEASURED, NOT ASSUMED: admitting the record after the marker offer
    //    retires the marker expectation, and the marker-ack then answers
    //    `NoMarkerExpected`. That is how the first two drafts of this unit died,
    //    both times at this unit's own arming guard rather than as a false pass.
    //
    //    The generation comes from a PROBE publication that is deliberately NOT
    //    recorded — reading `binding_epoch` arms nothing, and it keeps the
    //    hardcoded-`Generation::ONE` mistake that blinds the F8 units out of
    //    this file.
    let probe = fixture
        .handler
        .next_publication(fixture.target_connection, conversation_id, None)?
        .ok_or("NOT ARMED: the fixture offered no publications at all")?;
    let live_generation = probe.binding_epoch.capability_generation;
    if live_generation == Generation::ONE {
        return Err(format!(
            "NOT ARMED: the live generation is {live_generation:?}, so this fixture never drove \
             the CredentialAttach that mints the sealed binding-fate token. There is nothing for \
             a marker-ack to strand and this unit witnesses NOTHING."
        )
        .into());
    }

    let admitted = dispatch(
        &fixture.handler,
        fixture.catchup_connection,
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id,
            participant_id: fixture.catchup_participant,
            capability_generation: live_generation,
            record_admission_attempt_token: RecordAdmissionAttemptToken::new([0xF3; 16]),
            payload: vec![0xF3],
        }),
    )
    .map_err(|error| {
        format!(
            "NOT ARMED: no ordinary record could be admitted, so there is nothing past the \
             marker for an ordinary ack to reach: {error}"
        )
    })?;

    // 2. ONLY NOW is the marker offered, so its expectation is the freshest
    //    thing in the conversation and nothing below retires it.
    let epoch = offer_marker_and_read_live_epoch(&fixture)?;
    assert_armed_post_attach(epoch)?;

    // 3. The durable operation-log head, read WITHOUT touching the cached owner:
    //    `replay_aggregate_reference` builds a throwaway authority from rows and
    //    never locks the cell, so measuring here cannot perturb the thing being
    //    measured.
    //
    //    ⚠ THE PUBLICATION WALK CANNOT BE HOISTED ABOVE THE MARKER-ACK. Recording
    //    offers PAST the marker retires the marker expectation, and the marker-ack
    //    then answers `NoMarkerExpected` — measured, not assumed: that is exactly
    //    how the first draft of this unit failed, at this unit's own arming guard.
    //    So the walk stays BELOW the marker-ack and the head check below is
    //    widened to span it.
    // ⛔ THE UPSTREAM-FREEZE GUARD. If a marker-ack extension row ALREADY exists
    //    before this window opens, then any rebuild before the window can strand
    //    the token for a reason that has nothing to do with this unit's subject,
    //    and a red here would be misattributed.
    //
    //    I verified BY HAND that today's fixture writes no such row (outbox at
    //    fixture-build: 18 rows, ZERO MarkerAckCommitted). ⚠ THAT VERIFICATION
    //    DOES NOT TRAVEL — a fixture change reintroduces the possibility
    //    silently — so the hand-check becomes a STANDING one here.
    //
    //    ⛔ IT READS ROWS ONLY. It must never call `replay_aggregate_reference`
    //    or any other loader: see the cell-3 note above — AN ARMING PROBE THAT
    //    EXECUTES THE PATH UNDER TEST IS A CONTAMINANT, NOT A CONTROL, and an
    //    earlier draft of this very guard was exactly that.
    let preexisting_marker_rows = {
        let outbox = OutboxLog::new(Arc::clone(&fixture.store), conversation_id);
        let rows = block_on(outbox.read_all())
            .map_err(|error| format!("outbox bridge failed: {error:?}"))?
            .map_err(|error| format!("outbox read failed: {error:?}"))?;
        rows.iter()
            .filter(|(_, row)| matches!(row, OutboxRow::MarkerAckCommitted(_)))
            .count()
    };
    if preexisting_marker_rows != 0 {
        return Err(format!(
            "NOT ARMED: {preexisting_marker_rows} marker-ack extension row(s) already existed              before this unit's marker-ack. A rebuild before the window can then strand the token              upstream of everything this unit tests, so a red below would be MISATTRIBUTED.              APPARATUS fault, not a verdict."
        )
        .into());
    }

    // 4. ONE connection map held across BOTH requests, so this is one
    //    connection's occupancy rather than a fresh one per call as plain
    //    `dispatch` would give.
    let mut conversations = ParticipantConnectionConversations::default();

    let marker_acked = dispatch_tracked(
        &fixture.handler,
        fixture.target_connection,
        &mut conversations,
        ClientRequest::MarkerAck(MarkerAck {
            conversation_id,
            participant_id: fixture.target_participant,
            capability_generation: epoch.capability_generation,
            marker_delivery_seq: marker_seq,
        }),
    )
    .map_err(|error| {
        format!("NOT ARMED: the marker-ack itself was refused, so nothing was stranded: {error}")
    })?;
    // A marker-ack that answered something OTHER than committed (a capacity
    // refusal, say) strands no token at all, and a pass below would be vacuous.
    if !matches!(marker_acked, ServerValue::MarkerAckCommitted(_)) {
        return Err(format!(
            "NOT ARMED: the marker-ack did not commit; it answered {marker_acked:?}. Nothing was \
             stranded and this unit witnesses NOTHING."
        )
        .into());
    }

    // 5. The publication walk, which gives the ordinary ack something past the
    //    marker to reach. It uses `next_publication`/`record_publication_offer`
    //    directly rather than the dispatch seam, so it does not run an
    //    operation — and step 6 PROVES it appended nothing rather than assuming
    //    it.
    let deliverable = offer_everything_available(
        &fixture,
        ParticipantOfferedProgress {
            binding_epoch: epoch,
            through_seq: marker_seq,
        },
    )?;
    if deliverable <= marker_seq {
        return Err(format!(
            "NOT ARMED: nothing beyond the marker at {marker_seq} was offered to the target \
             (highest offered {deliverable}), so the ordinary ack below cannot reach past it and \
             this unit witnesses NOTHING. Admission answered {admitted:?}."
        )
        .into());
    }

    // 7. THE ASSERTION. Nothing appending has run since the marker-ack, so the authority
    //    answering this request is the very one the marker-ack mutated in
    //    memory. Whether it commits is decided by site one alone.
    let ordinary = dispatch_tracked(
        &fixture.handler,
        fixture.target_connection,
        &mut conversations,
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
            "NOT ARMED: the ordinary ack fell in a delivery gap ({gap:?}), so the sealed token was \
             never asked to progress and this unit witnessed NOTHING."
        )
        .into()),
        Ok(other) => Err(format!(
            "the ordinary ack on the CACHED authority neither committed nor refused; it answered \
             {other:?}."
        )
        .into()),
        Err(error) => {
            let rendered = error.to_string();
            assert!(
                !rendered.contains("sealed binding-fate authority"),
                "#26 REPRODUCED: the sealed binding-fate token DID NOT PROGRESS across the \
                 marker-ack, so the next ordinary ack could not be carried through. ⛔ THIS UNIT \
                 NAMES NO SITE AND CANNOT: it is a UNION DETECTOR. Either the marker-ack path \
                 failed to progress the token (ops_acks.rs:426), or the replay path did \
                 (ops_acks.rs:524), or both — every one of those presents here identically. ⛔ AND \
                 THE TEXT CANNOT EVEN TELL YOU WHETHER THE ACK COMMITTED: a post-commit \
                 reconcile failure (handler.rs:498-510 returns the reconcile's error as the \
                 REQUEST's error) is string-identical to a refusal. ONLY THE DURABLE ROWS \
                 DISCRIMINATE. Do not attribute this red to a site without reading them. \
                 Refusal: {rendered}"
            );
            Err(format!(
                "the ordinary ack on the cached authority was refused, but NOT by the sealed \
                 binding-fate check this unit exists to catch, so it needs its own diagnosis \
                 before anything is concluded: {rendered}"
            )
            .into())
        }
    }
}

/// `#42`, EXECUTED — the 2026-08-07 manifold outage's primal cause, pinned.
///
/// A delivered compaction marker mints one active marker anchor in the
/// conversation's closure accounting. The marker-ack path retires it
/// (`apply_marker_ack_frontier` moves cursor and anchor count atomically). The
/// ORDINARY cumulative-ack path retires nothing: it advances the member's
/// cursor with the closure accounting untouched, and consults no marker at
/// all. An ordinary ack whose `through_seq` crosses the marker therefore
/// splits the two ledgers the next admission's projection cross-checks —
/// `derived` counts unaccepted anchors by CURSOR POSITION
/// (`claim_frontier.rs ordinary_unaccepted_marker_anchors`), `stored` counts
/// them by ACCOUNTING — and every admission from then on faults with
/// `MarkerAnchorAccounting { derived: 0, stored: 1 }`, which the funnel
/// converts to a silent connection close. The conversation is wedged for
/// every member, forever; only history compaction rebuilds the accounting.
///
/// Two lawful fix shapes, and this unit accepts either:
///  (a) the crossing ack retires the anchor exactly as a marker-ack would, or
///  (b) the crossing ack is REFUSED with a wire-visible response.
/// What must never survive: the crossing is ACCEPTED and the very next
/// admission faults. That is the wedge, and it is what this unit reds on
/// today.
#[test]
fn an_ordinary_ack_crossing_a_marker_must_not_wedge_the_conversation() -> Result<(), Box<dyn Error>>
{
    let fixture = attached_marker_fixture()?;
    let epoch = offer_marker_and_read_live_epoch(&fixture)?;
    assert_armed_post_attach(epoch)?;

    let conversation_id = fixture.marker_delivery.conversation_id;
    let marker_seq = fixture.marker_delivery.delivery_seq;

    // 1. Ordinary traffic PAST the marker, so a crossing ack has a real
    //    delivery to land on. Without it the ack below is an `AckGap` about a
    //    delivery that does not exist and the crossing never happens.
    let admitted = dispatch(
        &fixture.handler,
        fixture.catchup_connection,
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id,
            participant_id: fixture.catchup_participant,
            capability_generation: epoch.capability_generation,
            record_admission_attempt_token: RecordAdmissionAttemptToken::new([0xF3; 16]),
            payload: vec![0xF3],
        }),
    )
    .map_err(|error| {
        format!(
            "NOT ARMED: no ordinary record could be admitted after the marker delivery, so no \
             ack can cross the marker and this unit witnesses NOTHING: {error}"
        )
    })?;
    let _ = &admitted;

    // 2. Offer everything to the target so the crossing `through_seq` names a
    //    delivery the target was actually offered.
    let deliverable = offer_everything_available(
        &fixture,
        ParticipantOfferedProgress {
            binding_epoch: epoch,
            through_seq: marker_seq,
        },
    )?;
    if deliverable <= marker_seq {
        return Err(format!(
            "NOT ARMED: nothing beyond the marker at {marker_seq} was offered to the target \
             (highest offered {deliverable}), so no ordinary ack can cross it and this unit \
             witnesses NOTHING. Admission answered {admitted:?}."
        )
        .into());
    }

    // 3. THE CROSSING. An ordinary cumulative ack straight through the marker.
    //    No marker-ack is ever sent in this unit — that is the point: the
    //    field client (a bridge SDK) never sent one either.
    let crossing = dispatch(
        &fixture.handler,
        fixture.target_connection,
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id,
            participant_id: fixture.target_participant,
            capability_generation: epoch.capability_generation,
            through_seq: deliverable,
        }),
    );
    match crossing {
        // Fix shape (b): a wire-visible refusal of the crossing is lawful —
        // fall through and prove the conversation is still admittable.
        Ok(ServerValue::AckCommitted(_)) | Ok(_) => {}
        Err(error) => {
            return Err(format!(
                "the CROSSING ACK ITSELF failed closed rather than committing or refusing on \
                 the wire — a silent close one step earlier than the one this unit pins, and \
                 it needs its own diagnosis: {error}"
            )
            .into());
        }
    }

    // 4. THE ASSERTION. Whatever the crossing answered, the conversation must
    //    still admit records. Under the defect it cannot: the projection
    //    cross-check faults with `MarkerAnchorAccounting` and the funnel
    //    closes the connection with no wire frame.
    let follow_up = dispatch(
        &fixture.handler,
        fixture.catchup_connection,
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id,
            participant_id: fixture.catchup_participant,
            capability_generation: epoch.capability_generation,
            record_admission_attempt_token: RecordAdmissionAttemptToken::new([0xF4; 16]),
            payload: vec![0xF4],
        }),
    );
    match follow_up {
        Ok(ServerValue::RecordCommitted(_)) => Ok(()),
        Ok(other) => Err(format!(
            "the admission after the crossing ack neither committed nor failed closed; it \
             answered {other:?}, which needs its own diagnosis before anything is concluded."
        )
        .into()),
        Err(error) => {
            let rendered = error.to_string();
            assert!(
                !rendered.contains("MarkerAnchorAccounting"),
                "#42 REPRODUCED: the ordinary ack crossed the delivered marker, advanced the \
                 member's cursor, and retired NO anchor from the closure accounting — the two \
                 ledgers the admission projection cross-checks now disagree by exactly one and \
                 every admission to this conversation fails closed. This is the manifold \
                 2026-08-07 wedge (spine capture: `record admission protocol fault: \
                 Projection(MarkerAnchorAccounting {{ derived: 0, stored: 1 }})`), executed \
                 rather than argued. Refusal: {rendered}"
            );
            Err(format!(
                "the admission after the crossing ack failed, but NOT with the split-ledger \
                 fault this unit exists to catch; it needs its own diagnosis before anything \
                 is concluded: {rendered}"
            )
            .into())
        }
    }
}

/// `#12`, EXECUTED — the redundant marker-ack after a crossing ordinary ack.
///
/// The crossing fix (`67d780e`) made an ordinary ack through the marker retire
/// its anchor — cumulative acceptance means what it says. But a client whose
/// resume state predates the crossing still OWES the marker-ack and re-presents
/// it against a cursor already sitting AT the marker. The frozen selector's
/// AckNoOp arm (`marker_proof.rs:241-245`, `requested == cursor &&
/// accepted_marker_at_cursor && is_marker_ack`) exists for exactly this
/// acknowledgement, and it is dead only because the server hardcodes the flag
/// `false`.
///
/// MEASURED at the pre-fix bytes, the dead arm presents as
/// `MarkerMismatch { NoMarkerExpected }` — the exact response the kernel died
/// on at 2026-08-07's second boot — WITHOUT any restart in this unit: the
/// selector's expected-marker input exists only while the in-memory offer
/// entry survives, and it does not survive to the re-present. Were the entry
/// still present, the same missing flag would instead fall through to the
/// commit path and double-retire the anchor (`checked_sub` on 0, frontier
/// transition refused). Both are the one defect; this unit reds on either.
///
/// Green = the marker-ack answers `AckNoOp` (or lawfully commits, if a future
/// accounting shape makes the re-commit sound). The fix computes the flag from
/// the retained marker-record census, which is durable — so it answers the
/// restart presentation too, not just the live one.
#[test]
fn a_redundant_marker_ack_at_the_cursor_must_answer_ack_noop() -> Result<(), Box<dyn Error>> {
    let fixture = attached_marker_fixture()?;
    let epoch = offer_marker_and_read_live_epoch(&fixture)?;
    assert_armed_post_attach(epoch)?;

    let conversation_id = fixture.marker_delivery.conversation_id;
    let marker_seq = fixture.marker_delivery.delivery_seq;

    // 1. Ordinary cumulative ack EXACTLY through the marker. Crossing includes
    //    equality: the anchor is retired and the cursor lands AT the marker —
    //    the precise state a resuming client that still owes its marker-ack
    //    finds itself against.
    let crossing = dispatch(
        &fixture.handler,
        fixture.target_connection,
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id,
            participant_id: fixture.target_participant,
            capability_generation: epoch.capability_generation,
            through_seq: marker_seq,
        }),
    )
    .map_err(|error| {
        format!(
            "NOT ARMED: the ordinary ack through the marker was refused, so the cursor never \
             reached the marker and there is nothing for a redundant marker-ack to be \
             redundant ABOUT: {error}"
        )
    })?;
    if !matches!(crossing, ServerValue::AckCommitted(_)) {
        return Err(format!(
            "NOT ARMED: the ordinary ack through the marker did not commit; it answered \
             {crossing:?}, so the cursor never reached the marker and this unit witnesses \
             NOTHING."
        )
        .into());
    }

    // 2. THE REDUNDANT MARKER-ACK. Same marker, same epoch, cursor already at
    //    the marker's sequence. Merely redundant — not a fault.
    let redundant = dispatch(
        &fixture.handler,
        fixture.target_connection,
        ClientRequest::MarkerAck(MarkerAck {
            conversation_id,
            participant_id: fixture.target_participant,
            capability_generation: epoch.capability_generation,
            marker_delivery_seq: marker_seq,
        }),
    );
    match redundant {
        Ok(ServerValue::AckNoOp(_)) | Ok(ServerValue::MarkerAckCommitted(_)) => Ok(()),
        Ok(ServerValue::MarkerMismatch(mismatch)) => Err(format!(
            "#12 REPRODUCED: the redundant marker-ack for an already-accepted marker was \
             answered with a MISMATCH — the frozen selector's AckNoOp arm is dead because \
             the server hardcodes `accepted_marker_at_cursor = false`, so an acknowledgement \
             that is merely redundant presents as a genuine fault. This is the response the \
             kernel lawfully died on at 2026-08-07's second boot (`MarkerMismatch {{ \
             NoMarkerExpected }}`), executed rather than argued. Answered: {mismatch:?}"
        )
        .into()),
        Ok(other) => Err(format!(
            "the redundant marker-ack neither no-opped nor committed; it answered {other:?}, \
             which needs its own diagnosis before anything is concluded."
        )
        .into()),
        Err(error) => {
            let rendered = error.to_string();
            assert!(
                !rendered.contains("marker ack frontier transition failed"),
                "#12 REPRODUCED: the redundant marker-ack fell through the dead AckNoOp arm \
                 (server hardcodes `accepted_marker_at_cursor = false`) into the commit path \
                 and DOUBLE-RETIRED the anchor the crossing ordinary ack already retired — \
                 the frontier transition's `checked_sub` refused and the acknowledgement of \
                 an already-accepted marker killed the request. This is the same missing \
                 server half that presented in the field as `MarkerMismatch {{ \
                 NoMarkerExpected }}` and took the kernel down on 2026-08-07's second boot. \
                 Refusal: {rendered}"
            );
            Err(format!(
                "the redundant marker-ack failed, but NOT by the double-retire this unit \
                 exists to catch; it needs its own diagnosis before anything is concluded: \
                 {rendered}"
            )
            .into())
        }
    }
}
