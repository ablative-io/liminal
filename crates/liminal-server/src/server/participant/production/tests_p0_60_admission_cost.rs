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

/// Full passes over the conversation that one committed source currently
/// makes: the outbox pre-validation walk, the base-log schema validation
/// walk, the base-log replay walk, and the outbox merge walk. Named in
/// `docs/design/p0-60-admission-cost.md` §1a, and measured below.
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

/// ⛔ THIS PIN ASSERTS THE DEFECT, DELIBERATELY, AND MUST BE INVERTED BY THE
/// FIX. ⛔
///
/// Board #60's fix — completing a committed source in place instead of
/// replaying the conversation to write its Unit 2 extension row
/// (`docs/design/p0-60-admission-cost.md` §3c) — is NOT landed. Until it is,
/// admission cost grows with history, and the honest thing to do with a defect
/// that is going to survive a lane is to make it MECHANICAL rather than prose:
/// pinned here, it cannot drift, cannot be partially fixed unnoticed, and
/// cannot be believed to be gone.
///
/// The pin is the SLOPE, and the slope has a named cause. Each committed source
/// makes [`FULL_PASSES_PER_COMMIT`] complete walks of the conversation, so one
/// admission's durable read cost rises by exactly that many rows per record of
/// history. Measuring at three histories rather than two also pins that the
/// growth is LINEAR: two points can be joined by any curve.
///
/// The lane that lands §3c turns this into `growth == 0` and renames it. A
/// green here means the ceiling is still there.
#[test]
fn admission_cost_still_grows_by_one_pass_per_history_record() -> Result<(), Box<dyn Error>> {
    let mut measured = Vec::new();
    for history in HISTORIES {
        let (reads, rows) = reads_for_one_admission_at(history)?;
        measured.push((history, reads, rows));
    }

    // The instrument must be able to see what it claims to measure: a counter
    // that never moved would make every difference below vacuously zero.
    let (_, first_reads, first_rows) = measured[0];
    assert!(
        first_reads > 0 && first_rows > 0,
        "the admission path must have reached the store"
    );

    for window in measured.windows(2) {
        let [(previous_history, _, previous_rows), (history, _, rows)] = window else {
            return Err("windows(2) yielded a short window".into());
        };
        let extra_history = history
            .checked_sub(*previous_history)
            .ok_or("histories must ascend")?;
        let extra_rows = u64::try_from(
            rows.checked_sub(*previous_rows)
                .ok_or("admission read cost fell as history grew: the shape changed, re-measure")?,
        )?;
        assert_eq!(
            extra_rows,
            extra_history * FULL_PASSES_PER_COMMIT,
            "between history {previous_history} and {history} one admission's durable \
             read cost grew by {extra_rows} rows; {FULL_PASSES_PER_COMMIT} full passes \
             over the conversation predict {}. If this fell to zero the fix landed and \
             this pin must be inverted; anything else means the number of full passes \
             changed and docs/design/p0-60-admission-cost.md is stale",
            extra_history * FULL_PASSES_PER_COMMIT
        );
    }
    Ok(())
}

/// The arithmetic that makes #60 a ceiling rather than a slow patch: cost per
/// admission is proportional to history with no constant term worth speaking of.
///
/// Also a defect pin, and inverted by the same fix.
#[test]
fn one_admission_rereads_the_whole_conversation() -> Result<(), Box<dyn Error>> {
    let history = HISTORIES[HISTORIES.len() - 1];
    let (_, rows) = reads_for_one_admission_at(history)?;
    assert!(
        u64::try_from(rows)? >= history,
        "one admission read {rows} durable rows into a {history}-row conversation. \
         Fewer than the history means the commit stopped replaying it — the fix \
         landed, and this pin must be inverted"
    );
    Ok(())
}
