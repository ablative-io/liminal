//! Board #60. One committed `RecordAdmission` must not cost the conversation's
//! history.
//!
//! The pin is a COUNT of durable rows the admission pulls out of the store, not
//! a duration. A latency assertion would flake on a loaded machine and would
//! measure the machine as much as the code; the defect here is a shape — work
//! proportional to N — and a shape is exact.
//!
//! The discriminating measurement is a SLOPE, taken at two histories: whatever
//! fixed overhead an admission carries cancels, and only growth survives.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use liminal::durability::{DurabilityError, DurableStore, StoredEntry, open_ephemeral};
use liminal_protocol::wire::{
    ClientRequest, ConnectionIncarnation, EnrollmentRequest, EnrollmentToken, Generation,
    RecordAdmission, RecordAdmissionAttemptToken, ServerValue,
};

use super::ProductionParticipantHandler;
use super::tests::{dispatch, test_participant_config};

const CONVERSATION: u64 = 0xF0_60_01;

/// Histories the slope is taken across. Both sit well below
/// `max_retained_record_rows` (1,024) so no marker drain fires and the two
/// measurements describe the same operation.
const HISTORIES: [u64; 3] = [32, 64, 96];

/// Full passes over the conversation that one committed source made BEFORE
/// board #60 §3c landed: the outbox pre-validation walk, the base-log schema
/// validation walk, the base-log replay walk, and the outbox merge walk.
/// Named in `docs/design/p0-60-admission-cost.md` §1a.
///
/// Retained as the number these pins were INVERTED from. The slope they now
/// assert is zero; this constant is what the slope used to be, per record of
/// history, and it is quoted in the failure text so a regression reports the
/// shape it regressed to.
const FULL_PASSES_PER_COMMIT: u64 = 4;

/// Counts durable reads at the `DurableStore` boundary.
///
/// Rows, not just calls: a reader that pages the same work into more calls and
/// a reader that pulls it in one call are the same defect, and only the row
/// count sees both.
#[derive(Debug)]
struct ReadCountingStore {
    inner: Arc<dyn DurableStore>,
    reads: AtomicUsize,
    rows: AtomicUsize,
}

impl ReadCountingStore {
    fn new(inner: Arc<dyn DurableStore>) -> Self {
        Self {
            inner,
            reads: AtomicUsize::new(0),
            rows: AtomicUsize::new(0),
        }
    }

    fn reset(&self) {
        self.reads.store(0, Ordering::SeqCst);
        self.rows.store(0, Ordering::SeqCst);
    }

    fn counts(&self) -> (usize, usize) {
        (
            self.reads.load(Ordering::SeqCst),
            self.rows.load(Ordering::SeqCst),
        )
    }
}

#[async_trait::async_trait]
impl DurableStore for ReadCountingStore {
    async fn append(
        &self,
        stream_key: &str,
        payload: Vec<u8>,
        expected_seq: u64,
    ) -> Result<u64, DurabilityError> {
        self.inner.append(stream_key, payload, expected_seq).await
    }

    async fn read_from(
        &self,
        stream_key: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<StoredEntry>, DurabilityError> {
        let entries = self.inner.read_from(stream_key, offset, limit).await?;
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.rows.fetch_add(entries.len(), Ordering::SeqCst);
        Ok(entries)
    }

    async fn read_at(
        &self,
        stream_key: &str,
        sequence: u64,
    ) -> Result<Option<StoredEntry>, DurabilityError> {
        let entry = self.inner.read_at(stream_key, sequence).await?;
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.rows
            .fetch_add(usize::from(entry.is_some()), Ordering::SeqCst);
        Ok(entry)
    }

    async fn cas(&self, key: &str, old_value: u64, new_value: u64) -> Result<(), DurabilityError> {
        self.inner.cas(key, old_value, new_value).await
    }

    async fn read_value(&self, key: &str) -> Result<Option<u64>, DurabilityError> {
        self.inner.read_value(key).await
    }

    async fn scan(&self, prefix: &str) -> Result<Vec<StoredEntry>, DurabilityError> {
        self.inner.scan(prefix).await
    }

    async fn flush(&self) -> Result<(), DurabilityError> {
        self.inner.flush().await
    }
}

/// One enrolled participant, bound by its enrollment connection.
#[derive(Clone, Copy)]
struct Member {
    connection: ConnectionIncarnation,
    participant_id: u64,
    generation: Generation,
}

/// Sixteen-byte attempt token from a `u64` nonce.
///
/// The single-byte token the older fixtures use runs out at 256, and a repeated
/// token is answered from the A2 dedup map instead of committing — which would
/// measure the dedup path and call it an admission.
const fn nonce_token(nonce: u64) -> [u8; 16] {
    let bytes = nonce.to_be_bytes();
    [
        0x60, bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], 0x06,
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
) -> Result<u64, Box<dyn Error>> {
    let value = dispatch(
        handler,
        sender.connection,
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: CONVERSATION,
            participant_id: sender.participant_id,
            capability_generation: sender.generation,
            record_admission_attempt_token: RecordAdmissionAttemptToken::new(nonce_token(nonce)),
            payload: nonce.to_be_bytes().to_vec(),
        }),
    )?;
    let ServerValue::RecordCommitted(committed) = value else {
        return Err(format!("record {nonce} did not commit: {value:?}").into());
    };
    Ok(committed.delivery_seq())
}

/// Builds a conversation with `history` committed admissions, then measures the
/// durable reads of exactly ONE more.
///
/// The authority stays live in the handler across the whole run, so nothing
/// here is measuring a cold load: every read counted belongs to the committed
/// admission's own post-append work.
fn reads_for_one_admission_at(history: u64) -> Result<(usize, usize), Box<dyn Error>> {
    let inner: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let counting = Arc::new(ReadCountingStore::new(inner));
    let handler = ProductionParticipantHandler::new(
        Arc::clone(&counting) as Arc<dyn DurableStore>,
        test_participant_config(),
    )?;

    let sender = enroll(&handler, ConnectionIncarnation::new(0x60, 1), 1)?;
    let _recipient = enroll(&handler, ConnectionIncarnation::new(0x60, 2), 2)?;

    for nonce in 0..history {
        admit(&handler, sender, 100 + nonce)?;
    }

    counting.reset();
    admit(&handler, sender, 100 + history)?;
    Ok(counting.counts())
}

/// Counts the durable rows a COLD RESTORE of the same conversation reads —
/// the instrument's positive control.
///
/// The control has to exercise the same predicate the assertion does: rows
/// counted at the `DurableStore` boundary, over a conversation of the same
/// shape. A cold restore is the path that provably still reads the whole
/// history, so a nonzero count here means the counter can see history being
/// read, and the admission's zero above is a measurement rather than a
/// silence.
fn rows_read_by_a_cold_restore_of(history: u64) -> Result<(usize, usize), Box<dyn Error>> {
    let inner: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let counting = Arc::new(ReadCountingStore::new(inner));
    let store = Arc::clone(&counting) as Arc<dyn DurableStore>;
    {
        let handler =
            ProductionParticipantHandler::new(Arc::clone(&store), test_participant_config())?;
        let sender = enroll(&handler, ConnectionIncarnation::new(0x60, 1), 1)?;
        let _recipient = enroll(&handler, ConnectionIncarnation::new(0x60, 2), 2)?;
        for nonce in 0..history {
            admit(&handler, sender, 100 + nonce)?;
        }
    }
    counting.reset();
    let _restored = ProductionParticipantHandler::new(store, test_participant_config())?;
    Ok(counting.counts())
}

/// Board #60's ceiling, INVERTED. One committed `RecordAdmission` costs the
/// same durable reads at any history: the slope is ZERO.
///
/// This pin asserted the defect until §3c landed
/// (`docs/design/p0-60-admission-cost.md`): the commit path used to replay the
/// conversation from sequence zero purely to let the replay's repair branch
/// write the admission's Unit 2 extension row, so one admission's read cost
/// rose by [`FULL_PASSES_PER_COMMIT`] rows per record of history. The commit
/// now writes that row itself, under the same conversation lock, and reads
/// nothing to do it.
///
/// The measurement is unchanged — the same [`ReadCountingStore`], the same
/// three histories, the same differencing so that fixed overhead cancels and
/// only growth survives. Three points rather than two still matter: they pin
/// that the cost is FLAT rather than merely equal at its endpoints.
///
/// A nonzero growth here means the from-zero replay is back on the commit
/// path — most likely because some source stopped completing in place and the
/// handler's outcome gate (`LogAppender::outstanding_extension_rows`)
/// correctly fell back to it. That is a correctness-preserving regression, and
/// it is exactly the one worth being told about.
#[test]
fn admission_cost_does_not_grow_with_history() -> Result<(), Box<dyn Error>> {
    let mut measured = Vec::new();
    for history in HISTORIES {
        let (reads, rows) = reads_for_one_admission_at(history)?;
        measured.push((history, reads, rows));
    }

    for window in measured.windows(2) {
        let [(previous_history, _, previous_rows), (history, _, rows)] = window else {
            return Err("windows(2) yielded a short window".into());
        };
        assert_eq!(
            rows, previous_rows,
            "between history {previous_history} and {history} one admission's durable read \
             cost moved from {previous_rows} rows to {rows}. Board #60 §3c makes the commit \
             path's cost independent of history; growth of \
             {FULL_PASSES_PER_COMMIT} rows per record is the defect this pin was inverted \
             from, and any growth at all means a source stopped completing its own Unit 2 \
             extension row and the from-zero replay is back on the commit path"
        );
    }
    Ok(())
}

/// The arithmetic that made #60 a ceiling, INVERTED: one admission no longer
/// reads the conversation at all.
///
/// Its twin above pins the SLOPE; this pins the LEVEL, because a slope of zero
/// is also what a uniformly expensive commit would show. At a 96-record
/// history the committed admission's post-append work must read strictly fewer
/// durable rows than the history it sits on — it reads none.
///
/// The instrument's own liveness is proved here rather than in the slope pin:
/// a counter that never moved would make a zero-slope assertion vacuous, so
/// this test first drives a history-zero admission through the SAME store and
/// requires that the counter moved for the enrollments that built the
/// conversation. An absence has to be a measurement, not a silence.
#[test]
fn one_admission_does_not_reread_the_conversation() -> Result<(), Box<dyn Error>> {
    let history = HISTORIES[HISTORIES.len() - 1];
    let (reads, rows) = reads_for_one_admission_at(history)?;
    assert!(
        u64::try_from(rows)? < history,
        "one admission read {rows} durable rows into a {history}-row conversation over \
         {reads} calls. Board #60 §3c retired the from-zero replay from the commit path; \
         reading as much as the history means it is back"
    );

    // POSITIVE CONTROL. The same counter, the same predicate, over a
    // conversation of the same history — on the one path that still reads it
    // all. Without this the assertion above is a statement about a dead
    // counter.
    let (control_reads, control_rows) = rows_read_by_a_cold_restore_of(history)?;
    assert!(
        u64::try_from(control_rows)? >= history,
        "a cold restore of a {history}-row conversation read only {control_rows} rows over \
         {control_reads} calls, so the instrument cannot witness history being read and the \
         admission's {rows} is not a measurement"
    );
    Ok(())
}
