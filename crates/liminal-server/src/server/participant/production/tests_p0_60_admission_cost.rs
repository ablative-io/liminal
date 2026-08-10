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
const SHORT_HISTORY: u64 = 32;
const LONG_HISTORY: u64 = 96;

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

/// The slope pin. A cost that does not grow is the whole requirement, and two
/// histories are what makes "does not grow" an assertion rather than a hope.
#[test]
fn one_admission_reads_the_same_at_any_history() -> Result<(), Box<dyn Error>> {
    let (short_reads, short_rows) = reads_for_one_admission_at(SHORT_HISTORY)?;
    let (long_reads, long_rows) = reads_for_one_admission_at(LONG_HISTORY)?;

    // The instrument must be able to see the defect it is pinning: if the
    // counter never moved, an equality below would be vacuous.
    assert!(
        short_reads > 0,
        "the admission path must have reached the store"
    );

    assert_eq!(
        long_rows, short_rows,
        "one admission read {long_rows} durable rows at history {LONG_HISTORY} but \
         {short_rows} at history {SHORT_HISTORY}: admission cost grows with the \
         conversation"
    );
    assert_eq!(
        long_reads, short_reads,
        "one admission made {long_reads} store reads at history {LONG_HISTORY} but \
         {short_reads} at history {SHORT_HISTORY}: admission cost grows with the \
         conversation"
    );
    Ok(())
}

/// A committed admission must not re-read the history it is appending to. The
/// absolute bound complements the slope: a constant that happens to be large is
/// still a constant, and this says the constant is small.
#[test]
fn one_admission_does_not_reread_the_conversation() -> Result<(), Box<dyn Error>> {
    let (_, rows) = reads_for_one_admission_at(LONG_HISTORY)?;
    assert!(
        rows < usize::try_from(LONG_HISTORY)?,
        "one admission read {rows} durable rows into a {LONG_HISTORY}-row \
         conversation: the commit is replaying its own history"
    );
    Ok(())
}
