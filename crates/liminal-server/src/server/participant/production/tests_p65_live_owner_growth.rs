//! Board #65. A live owner that survives its commits accumulates in-memory
//! state for the whole process lifetime.
//!
//! This is the ceiling board #60 §5 named and did not move: "Neither bounds
//! `committed_admissions`, which grows with admissions in the retained op-log
//! window. It is memory, not store I/O, and it is the next ceiling after this
//! one."
//!
//! These pins are the MECHANICAL form of that sentence. They are green today
//! **because the growth is present**, exactly as `tests_p0_60_admission_cost`
//! was green while the from-zero replay was still on the commit path. The lane
//! that bounds a structure inverts its pin here to a flat assertion; until
//! then a green is the statement "the ceiling is still where #65 measured it".
//!
//! # What makes these measurements rather than silences
//!
//! Every pin below carries its positive control INSIDE the fixture (the house
//! keepalive-honest pattern). A growth assertion is trivially satisfiable by a
//! fixture that drove no admissions at all — every counter stays at its
//! starting value and a "grew by N" test would fail loudly, but a "stayed
//! flat" inversion would pass VACUOUSLY. So each pin first proves that the
//! unrelated durable counters (`next_seq`, `next_log_sequence`) moved by the
//! admissions it claims to have driven. Without that, the flat assertions the
//! bounding lane writes here would be measuring a dead fixture.
//!
//! The measurement is a SLOPE across two histories, not a level: whatever
//! fixed footprint an enrolled conversation carries cancels, and only growth
//! that tracks admissions survives.

use std::error::Error;
use std::sync::Arc;

use liminal::durability::{DurableStore, open_ephemeral};
use liminal_protocol::wire::{
    ClientRequest, ConnectionIncarnation, EnrollmentRequest, EnrollmentToken, Generation,
    RecordAdmission, RecordAdmissionAttemptToken, ServerValue,
};

use super::ProductionParticipantHandler;
use super::tests::{dispatch, test_participant_config};

const CONVERSATION: u64 = 0xF0_65_01;

/// Histories the slope is taken across. Both sit well below
/// `max_retained_record_rows` (1,024), so no drain fires and the two
/// measurements describe the same conversation shape.
const HISTORIES: [u64; 2] = [32, 96];

/// Payload bytes each admission carries.
///
/// Fixed and non-trivial so the outbox's retained batch bodies are measurable
/// in BYTES rather than in entry counts alone — the structure whose growth is
/// counted in copies of the conversation's own traffic.
const PAYLOAD_BYTES: usize = 256;

/// One enrolled participant, bound by its enrollment connection.
#[derive(Clone, Copy)]
struct Member {
    connection: ConnectionIncarnation,
    participant_id: u64,
    generation: Generation,
}

/// Sixteen-byte attempt token from a `u64` nonce.
///
/// Distinct per admission: a repeated token is answered from the A2 dedup map
/// instead of committing, which would measure the dedup path and call it an
/// admission.
const fn nonce_token(nonce: u64) -> [u8; 16] {
    let bytes = nonce.to_be_bytes();
    [
        0x65, bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], 0x56,
        0, 0, 0, 0, 0, 0,
    ]
}

fn enroll(
    handler: &ProductionParticipantHandler,
    connection: ConnectionIncarnation,
    nonce: u64,
) -> Result<Member, Box<dyn Error>> {
    let value = dispatch(
        handler,
        connection,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: CONVERSATION,
            enrollment_token: EnrollmentToken::new(nonce_token(nonce)),
        }),
    )?;
    let ServerValue::EnrollBound(bound) = value else {
        return Err(format!("enrollment {nonce} did not bind: {value:?}").into());
    };
    Ok(Member {
        connection,
        participant_id: bound.participant_id(),
        generation: Generation::ONE,
    })
}

fn admit(
    handler: &ProductionParticipantHandler,
    sender: Member,
    nonce: u64,
) -> Result<(), Box<dyn Error>> {
    let value = dispatch(
        handler,
        sender.connection,
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: CONVERSATION,
            participant_id: sender.participant_id,
            capability_generation: sender.generation,
            record_admission_attempt_token: RecordAdmissionAttemptToken::new(nonce_token(nonce)),
            payload: vec![u8::try_from(nonce % 251)?; PAYLOAD_BYTES],
        }),
    )?;
    let ServerValue::RecordCommitted(_) = value else {
        return Err(format!("record {nonce} did not commit: {value:?}").into());
    };
    Ok(())
}

/// Everything one live owner is holding, measured through the seam that owns
/// it rather than inferred from the outside.
#[derive(Clone, Copy, Debug)]
struct LiveOwnerFootprint {
    /// A2/A4 committed-identity dedup map (`state.rs:282`).
    committed_admissions: usize,
    /// Enriched replay vector board #60 §3c changed from a drain to a BORROW
    /// (`state.rs:598`, `observer_progress.rs:460`).
    observer_witnesses: usize,
    /// Canonical produced-batch BODIES retained by the outbox
    /// (`outbox.rs:166`).
    source_batch_count: usize,
    source_batch_owned_bytes: usize,
    /// Per-recipient obligation sequences the outbox never sheds
    /// (`outbox.rs:168`).
    obligation_sequence_count: usize,
    /// POSITIVE CONTROL counters. Unrelated to any bound this lane proposes,
    /// and they MUST grow with the admissions the fixture drove.
    next_seq: u64,
    next_log_sequence: u64,
}

/// Builds a conversation with `history` committed admissions and measures what
/// the still-live owner is holding afterwards.
///
/// The authority stays live in the handler across the whole run, so nothing
/// here is a cold-load artifact: every entry counted is state the process has
/// carried since the commit that produced it.
fn footprint_after(history: u64) -> Result<LiveOwnerFootprint, Box<dyn Error>> {
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
    build_history(&handler, history)?;
    measure(&handler)
}

/// Builds `history` admissions, drops the process that made them, and measures
/// what a FRESHLY RESTORED owner holds.
///
/// This is the path that fills the observer-progress witness vector: the
/// bracketing calls that produce witnesses live in the replay
/// (`ops_session_replay.rs:78,93`, `outbox_replay.rs:88,116,143,148`) and, as
/// board #60 §2 records, are "called from no live commit site at all". A
/// restored owner therefore starts life holding one witness per durable
/// source and sheds none of them for as long as the process runs.
fn footprint_after_cold_restore(history: u64) -> Result<LiveOwnerFootprint, Box<dyn Error>> {
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    {
        let handler =
            ProductionParticipantHandler::new(Arc::clone(&store), test_participant_config())?;
        build_history(&handler, history)?;
    }
    let restored = ProductionParticipantHandler::new(store, test_participant_config())?;
    measure(&restored)
}

fn build_history(
    handler: &ProductionParticipantHandler,
    history: u64,
) -> Result<(), Box<dyn Error>> {
    let sender = enroll(handler, ConnectionIncarnation::new(0x65, 1), 1)?;
    let _recipient = enroll(handler, ConnectionIncarnation::new(0x65, 2), 2)?;
    for nonce in 0..history {
        admit(handler, sender, 100 + nonce)?;
    }
    Ok(())
}

fn measure(handler: &ProductionParticipantHandler) -> Result<LiveOwnerFootprint, Box<dyn Error>> {
    let cell = handler.cell(CONVERSATION)?;
    let owner = cell
        .lock()
        .map_err(|_| "live-owner footprint inspection lock was poisoned")?;
    let authority = owner
        .as_ref()
        .ok_or("live-owner footprint inspection found no conversation owner")?;
    let outbox = authority
        .outbox
        .as_ref()
        .ok_or("live-owner footprint inspection found no outbox owner")?;
    let measurements = outbox
        .retained_authority_measurements()
        .map_err(|error| format!("retained authority measurement: {error:?}"))?;
    let footprint = LiveOwnerFootprint {
        committed_admissions: authority.committed_admissions.len(),
        observer_witnesses: authority.observer_progress_witnesses().len(),
        source_batch_count: measurements.source_batch_count,
        source_batch_owned_bytes: measurements.source_batch_owned_bytes,
        obligation_sequence_count: measurements.obligation_sequence_count,
        next_seq: authority.next_seq,
        next_log_sequence: authority.next_log_sequence,
    };
    drop(owner);
    Ok(footprint)
}

/// The two footprints the slope is taken between, with the positive control
/// already discharged.
///
/// Every pin calls this rather than measuring alone, so no pin can assert a
/// growth (or, once inverted, a flatness) over a fixture that never admitted
/// anything.
fn slope_across_histories() -> Result<(LiveOwnerFootprint, LiveOwnerFootprint), Box<dyn Error>> {
    let [small, large] = HISTORIES;
    let smaller = footprint_after(small)?;
    let larger = footprint_after(large)?;
    let admissions = large
        .checked_sub(small)
        .ok_or("HISTORIES must be strictly increasing")?;

    // POSITIVE CONTROL. Unrelated counters, the same two fixtures, the same
    // probe. These MUST move by the admissions the fixture claims to have
    // driven; if they did not, every assertion built on this pair is a
    // statement about a fixture that did nothing.
    let seq_growth = larger
        .next_seq
        .checked_sub(smaller.next_seq)
        .ok_or("delivery sequence went backwards between histories")?;
    assert!(
        seq_growth >= admissions,
        "positive control failed: {admissions} extra admissions moved the delivery sequence by \
         only {seq_growth} ({} to {}). The fixture did not drive the admissions the growth \
         assertions below are about, so those assertions measure nothing",
        smaller.next_seq,
        larger.next_seq
    );
    let log_growth = larger
        .next_log_sequence
        .checked_sub(smaller.next_log_sequence)
        .ok_or("durable log head went backwards between histories")?;
    assert!(
        log_growth >= admissions,
        "positive control failed: {admissions} extra admissions moved the durable log head by \
         only {log_growth} ({} to {}). The fixture did not drive the admissions the growth \
         assertions below are about",
        smaller.next_log_sequence,
        larger.next_log_sequence
    );
    Ok((smaller, larger))
}

/// Board #65's named ceiling. The A2/A4 dedup map holds one entry per
/// committed admission and never sheds one while the owner lives.
///
/// Board #60 §5 named this structure and left it: it grows with admissions,
/// it is memory rather than store I/O, and nothing removes from it. The two
/// insert sites are `ops_frontier.rs:432` (`persist_record_commit`, the live
/// commit) and `ops_frontier.rs:725` (`publish_replayed_record_admission`, the
/// replay). There is no third site and there is no removal site.
///
/// The pin asserts the growth is EXACTLY one entry per admission, which is
/// stronger than "it grows": it says the map is a permanent copy of the
/// conversation's admission history, one key per record, with no reuse and no
/// displacement. A bounding lane inverts this to a stated ceiling.
///
/// ⛔ Inverting it is a CONTRACT question, not a build choice. The participant
/// contract's A2 amendment fixes the dedup window at the retained op-log
/// window (`docs/design/PARTICIPANT-CONTRACT.md:474-479`) and records in the
/// same breath that "No server-side op-log compaction exists at the time of
/// this amendment", so today that window is the whole log. Narrowing it is a
/// contract-text change; see `docs/design/P65-LIVE-OWNER-GROWTH.md` §4.
#[test]
fn every_committed_admission_permanently_occupies_the_dedup_map() -> Result<(), Box<dyn Error>> {
    let (smaller, larger) = slope_across_histories()?;
    let [small, large] = HISTORIES;
    let admissions = usize::try_from(large.checked_sub(small).ok_or("history underflow")?)?;
    let growth = larger
        .committed_admissions
        .checked_sub(smaller.committed_admissions)
        .ok_or("committed_admissions shrank between histories")?;
    assert_eq!(
        growth, admissions,
        "{admissions} further admissions added {growth} entries to committed_admissions \
         ({} at history {small}, {} at history {large}). Board #65 measures this map as one \
         permanent entry per committed admission with no shedding site; a bounding lane \
         inverts this pin to its stated ceiling",
        smaller.committed_admissions, larger.committed_admissions
    );
    Ok(())
}

/// ⚠ THE CORRECTION. The observer-progress witness vector does NOT grow with
/// admissions — on the live path or after a restore. This pin holds that
/// measurement in place.
///
/// The lane was dispatched with the witness vector named as part of the
/// admission growth family. It measures at ZERO on both paths and at both
/// histories, so the claim does not survive contact with the instrument, and a
/// bound built for this structure on the admission path would have been a
/// bound for growth that does not happen (see
/// `docs/design/P65-LIVE-OWNER-GROWTH.md` §3).
///
/// Why it is zero is structural, not incidental. An ordinary `RecordAdmission`
/// projects no observer progress: the bracketing calls that produce witnesses
/// (`begin/end_observer_progress_source`) sit on the REPLAY of
/// progress-bearing sources — binding fates, leaves, marker acks — and board
/// #60 §2 records that they are "called from no live commit site at all". An
/// admission-only history has no such source, so the vector stays empty
/// whether the owner is live or freshly restored.
///
/// What this pin does NOT clear. `ObserverProgressWitnessState` still has no
/// shedding site on any path — not `witnesses` (`observer_progress.rs:460`,
/// the only `take` is `#[cfg(test)]` at `:559`), not `occurrences`
/// (`:462`), not `lineage_progress` (`:463`) — and board #60 §3c deliberately
/// changed this vector from a drain to a borrow (`state.rs:588-597`). Under a
/// workload that DOES project observer progress the growth family is live and
/// unmeasured. This lane measured the admission axis and found it flat; the
/// fate/ack axis is named residue, not a cleared structure.
#[test]
fn admissions_project_no_observer_progress_witnesses_on_either_path()
-> Result<(), Box<dyn Error>> {
    let [small, large] = HISTORIES;
    let restored_small = footprint_after_cold_restore(small)?;
    let restored_large = footprint_after_cold_restore(large)?;
    let admissions = large.checked_sub(small).ok_or("history underflow")?;

    // POSITIVE CONTROL, in the same fixture and through the same probe. A
    // zero-witness assertion is exactly what a fixture that admitted nothing
    // would also produce, so the durable counters must first show that the
    // extra admissions were really built and really restored.
    let log_growth = restored_large
        .next_log_sequence
        .checked_sub(restored_small.next_log_sequence)
        .ok_or("durable log head went backwards between histories")?;
    assert!(
        admissions <= log_growth,
        "positive control failed: {admissions} extra admissions moved the restored log head \
         by only {log_growth} ({} to {}). The zero-witness readings below would be a silence, \
         not a measurement",
        restored_small.next_log_sequence,
        restored_large.next_log_sequence
    );
    let seq_growth = restored_large
        .next_seq
        .checked_sub(restored_small.next_seq)
        .ok_or("delivery sequence went backwards between histories")?;
    assert!(
        admissions <= seq_growth,
        "positive control failed: {admissions} extra admissions moved the restored delivery \
         sequence by only {seq_growth}",
    );

    let live_only = footprint_after(large)?;
    assert_eq!(
        (
            live_only.observer_witnesses,
            restored_small.observer_witnesses,
            restored_large.observer_witnesses
        ),
        (0, 0, 0),
        "observer-progress witnesses read (live {}, restored@{small} {}, restored@{large} {}) \
         where board #60 §2's replay-only bracketing predicts zero for an admission-only \
         history. A nonzero reading means a live commit site started recording witnesses, \
         which puts an unshed vector back on the admission path and makes #65 §3's \
         correction stale",
        live_only.observer_witnesses,
        restored_small.observer_witnesses,
        restored_large.observer_witnesses
    );
    Ok(())
}

/// The outbox retains the canonical BODY of every produced batch for the life
/// of the owner.
///
/// `source_batches` (`outbox.rs:166`) maps source log sequence to the batch's
/// canonical bytes, written by `apply_produced` (`outbox.rs:292`) as a
/// conflicting-source idempotence check (`outbox.rs:249`). Its neighbour
/// `records` IS reclaimed once every recipient has discharged
/// (`reclaim_empty_records`, `outbox.rs:458`); `source_batches` is not
/// reclaimed anywhere. The live owner therefore holds a second copy of the
/// conversation's entire payload traffic, in bytes, indefinitely.
///
/// This pin measures BYTES, not entries, because that asymmetry is the point:
/// the map's entry count understates it by the size of a record body.
#[test]
fn the_outbox_retains_every_produced_batch_body_for_the_owners_life()
-> Result<(), Box<dyn Error>> {
    let (smaller, larger) = slope_across_histories()?;
    let [small, large] = HISTORIES;
    let admissions = usize::try_from(large.checked_sub(small).ok_or("history underflow")?)?;
    let entry_growth = larger
        .source_batch_count
        .checked_sub(smaller.source_batch_count)
        .ok_or("retained source batches shrank between histories")?;
    assert!(
        entry_growth >= admissions,
        "{admissions} further admissions retained only {entry_growth} further source batches \
         ({} at history {small}, {} at history {large})",
        smaller.source_batch_count,
        larger.source_batch_count
    );

    // The bytes are the finding. Each retained batch carries its record's
    // canonical body, so the floor is the payload the admissions carried.
    let byte_growth = larger
        .source_batch_owned_bytes
        .checked_sub(smaller.source_batch_owned_bytes)
        .ok_or("retained source batch bytes shrank between histories")?;
    let payload_floor = admissions
        .checked_mul(PAYLOAD_BYTES)
        .ok_or("payload floor overflow")?;
    assert!(
        byte_growth >= payload_floor,
        "{admissions} further admissions of {PAYLOAD_BYTES} payload bytes each grew the \
         owner's retained batch bodies by {byte_growth} bytes, under the {payload_floor}-byte \
         floor of the payloads themselves ({} to {}). The measurement is not seeing the \
         retained bodies",
        smaller.source_batch_owned_bytes,
        larger.source_batch_owned_bytes
    );
    Ok(())
}

/// The outbox's per-recipient obligation sequence set is never shed, even for
/// records that have been fully discharged and reclaimed.
///
/// `all_obligations` (`outbox.rs:168`) gains one `u64` per (record ×
/// recipient) at `install_record` (`outbox.rs:306`) and has no removal site:
/// `discharge_through` (`outbox.rs:406`), `discharge_retired`
/// (`outbox.rs:427`) and `reclaim_empty_records` (`outbox.rs:458`) all touch
/// `records` or `next_live_obligations` and none of them touches
/// `all_obligations`. It is the same asymmetry `source_batches` has against
/// `records`, one field over.
#[test]
fn discharged_records_leave_their_obligation_sequences_behind() -> Result<(), Box<dyn Error>> {
    let (smaller, larger) = slope_across_histories()?;
    let [small, large] = HISTORIES;
    let admissions = usize::try_from(large.checked_sub(small).ok_or("history underflow")?)?;
    let growth = larger
        .obligation_sequence_count
        .checked_sub(smaller.obligation_sequence_count)
        .ok_or("retained obligation sequences shrank between histories")?;
    assert!(
        growth >= admissions,
        "{admissions} further admissions added only {growth} retained obligation sequences \
         ({} at history {small}, {} at history {large}). Board #65 measures this set as \
         never shed; a bounding lane inverts this pin",
        smaller.obligation_sequence_count,
        larger.obligation_sequence_count
    );
    Ok(())
}
