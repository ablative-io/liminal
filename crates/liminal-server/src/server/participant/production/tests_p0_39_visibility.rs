//! Lane p0-39 visibility: displacement is SILENT TO EXPERIENCE, LOUD TO
//! RECORD.
//!
//! The stage-8 receipt windows used to refuse at their bound, which was loud on
//! the wire — a typed `ReceiptCapacityExceeded` the client could read. They
//! displace now, which the arriving client cannot see at all, by design. A
//! bound that neither refuses nor discloses would hide exactly what the old
//! wall at least made loud, so both halves of the replacement are pinned here:
//!
//! * every displacement counts into `liminal_receipt_displacements_total`,
//!   labelled by window scope;
//! * every observation of a shared pool at or above its reporting threshold
//!   counts into `liminal_receipt_pool_runaway_total`, labelled by pool, and
//!   warns on the rising edge.
//!
//! # Two harness hazards these pins are built against
//!
//! The metric families are PROCESS-global and the sibling tests in this binary
//! also displace fingerprints, so every assertion below is a strict delta
//! (`after > before`) taken under [`PIN_GATE`], never an equality — a
//! concurrent sibling can only inflate the counter, never suppress it. And a
//! `None` reading means "the family is absent from the registry", which is a
//! failure, never a zero.
//!
//! The log capture is a SCOPED subscriber (`with_default`), not a global
//! install: the dispatch it wraps is synchronous on this thread, and a global
//! install would silently change what 700 sibling tests see.

use std::error::Error;
use std::io::Write;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use liminal_protocol::wire::ConnectionIncarnation;

use crate::metrics::{ReceiptWindowScope, SharedReceiptPool};

use super::ProductionParticipantHandler;
use super::tests::open_disk_store_for_tests;
use super::tests_capacity::capacity_config;
use super::tests_p0_39_capacity_hybrid::{
    enrollment_fingerprint_retained, enrollment_request, rotate,
};
use super::tests_receipts::{enroll, enroll_proving_provenance};

/// Serialises the delta-taking pins in this file against each other.
static PIN_GATE: Mutex<()> = Mutex::new(());

type CapturedLog = Arc<Mutex<Vec<u8>>>;

#[derive(Clone)]
struct CaptureWriter(CapturedLog);

impl Write for CaptureWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut guard = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        guard.extend_from_slice(buf);
        drop(guard);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Runs `body` with warnings captured on THIS THREAD only.
fn with_captured_warnings<T>(body: impl FnOnce() -> T) -> (T, String) {
    let buffer: CapturedLog = Arc::new(Mutex::new(Vec::new()));
    let writer = CaptureWriter(Arc::clone(&buffer));
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || writer.clone())
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .finish();
    let value = tracing::subscriber::with_default(subscriber, body);
    let guard = buffer.lock().unwrap_or_else(PoisonError::into_inner);
    let text = String::from_utf8_lossy(&guard).into_owned();
    drop(guard);
    (value, text)
}

fn gate() -> MutexGuard<'static, ()> {
    PIN_GATE.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A displaced fingerprint increments the labelled displacement counter.
///
/// The triggering event is asserted FIRST — the enrollment fingerprint really
/// is gone from the participant's window — because every assertion after it is
/// about an instrument that would otherwise never have been fired.
#[test]
fn a_displacement_counts_into_the_labelled_displacement_counter() -> Result<(), Box<dyn Error>> {
    let _gate = gate();
    crate::metrics::init();
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(151, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    // A window of one: the second rotation must displace the first
    // fingerprint.
    let config = capacity_config(|c| c.max_receipt_provenance_per_participant = 1);
    let handler = ProductionParticipantHandler::new(store, config)?;
    let conversation_id = 3930;
    let enrollment_token = [0xF0; 16];

    let before =
        crate::metrics::receipt_displacements_value(ReceiptWindowScope::ProvenanceParticipant)
            .ok_or("liminal_receipt_displacements_total is absent from the registry")?;

    let proven = enroll_proving_provenance(
        &handler,
        incarnation,
        conversation_id,
        [enrollment_token, [0xF1; 16], [0xF2; 16]],
    )?;
    // Nothing displaced yet: the window of one holds the enrollment
    // fingerprint the proving rotation just promoted.
    assert!(
        enrollment_fingerprint_retained(&handler, incarnation, conversation_id, enrollment_token)?,
        "fixture precondition: the proving rotation must leave the enrollment fingerprint held"
    );

    rotate(
        &handler,
        incarnation,
        conversation_id,
        proven.participant_id,
        2,
        proven.attach_secret,
        ([0xF3; 16], [0xF4; 16]),
    )?;

    // THE DISPLACEMENT HAPPENED. Without this the counter assertion below
    // would be a statement about an untriggered instrument.
    assert!(
        !enrollment_fingerprint_retained(&handler, incarnation, conversation_id, enrollment_token)?,
        "the fixture failed to force a displacement: the enrollment fingerprint still holds the \
         participant's only window slot"
    );

    let after =
        crate::metrics::receipt_displacements_value(ReceiptWindowScope::ProvenanceParticipant)
            .ok_or("liminal_receipt_displacements_total vanished from the registry")?;
    assert!(
        after > before,
        "a displacement must be counted: {} did not move (before={before} after={after})",
        "liminal_receipt_displacements_total{scope=\"provenance_participant\"}",
    );
    Ok(())
}

/// A shared pool at or above its reporting threshold counts an observation and
/// warns on the rising edge — and refuses nothing while it does.
///
/// Both halves matter. The counter is what an alert fires on; the warning is
/// what the alert is then read against, and it has to name the pool, the
/// occupancy, and the threshold or an operator cannot tell a storm from a
/// tightened knob.
#[test]
fn a_shared_pool_past_its_threshold_counts_and_warns_without_refusing() -> Result<(), Box<dyn Error>>
{
    let _gate = gate();
    crate::metrics::init();
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(152, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    // A threshold of one on the server live-receipt pool: the very first
    // enrollment's receipt reaches it.
    let config = capacity_config(|c| c.live_receipt_server_report_threshold = 1);
    let handler = ProductionParticipantHandler::new(store, config)?;

    let before = crate::metrics::receipt_pool_runaway_value(SharedReceiptPool::LiveReceiptServer)
        .ok_or("liminal_receipt_pool_runaway_total is absent from the registry")?;

    let (result, warnings) = with_captured_warnings(|| {
        enroll(&handler, incarnation, 3931, [0xA1; 16])?;
        // The second arrival observes the pool at its threshold, and LANDS.
        enroll(&handler, incarnation, 3932, [0xA2; 16])?;
        Ok::<(), Box<dyn Error>>(())
    });
    result?;

    let after = crate::metrics::receipt_pool_runaway_value(SharedReceiptPool::LiveReceiptServer)
        .ok_or("liminal_receipt_pool_runaway_total vanished from the registry")?;
    assert!(
        after > before,
        "a shared pool at its reporting threshold must be counted: \
         liminal_receipt_pool_runaway_total{{pool=\"live_receipt_server\"}} did not move \
         (before={before} after={after})"
    );

    assert!(
        warnings.contains("shared receipt pool runaway"),
        "the rising edge must warn; captured warnings were: {warnings:?}"
    );
    assert!(
        warnings.contains("live_receipt_server"),
        "the warning must name the pool; captured warnings were: {warnings:?}"
    );
    assert!(
        warnings.contains("threshold"),
        "the warning must carry the threshold it crossed; captured warnings were: {warnings:?}"
    );
    Ok(())
}

/// The rising edge warns ONCE, not once per request — but every observation is
/// still counted, so a sustained storm shows up as a rate rather than as a log
/// flood.
///
/// This is the honest cost of the design and it is pinned rather than left to
/// be discovered: an operator who sees one warning and a climbing counter is
/// seeing a storm, not a single event.
#[test]
fn a_sustained_storm_warns_once_but_keeps_counting() -> Result<(), Box<dyn Error>> {
    let _gate = gate();
    crate::metrics::init();
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(153, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    let config = capacity_config(|c| c.live_receipt_server_report_threshold = 1);
    let handler = ProductionParticipantHandler::new(store, config)?;

    let before = crate::metrics::receipt_pool_runaway_value(SharedReceiptPool::LiveReceiptServer)
        .ok_or("liminal_receipt_pool_runaway_total is absent from the registry")?;

    let (result, warnings) = with_captured_warnings(|| {
        for round in 0..5_u64 {
            let token = [0xB1 + u8::try_from(round).unwrap_or(0); 16];
            enroll(&handler, incarnation, 3940 + round, token)?;
        }
        Ok::<(), Box<dyn Error>>(())
    });
    result?;

    let after = crate::metrics::receipt_pool_runaway_value(SharedReceiptPool::LiveReceiptServer)
        .ok_or("liminal_receipt_pool_runaway_total vanished from the registry")?;
    assert!(
        after >= before + 4,
        "every observation past the threshold must be counted, not just the first \
         (before={before} after={after})"
    );
    assert_eq!(
        warnings.matches("shared receipt pool runaway").count(),
        1,
        "a sustained storm must warn exactly once on its rising edge; captured warnings were: \
         {warnings:?}"
    );
    Ok(())
}

/// NEGATIVE CONTROL: below the threshold nothing is reported.
///
/// Without this, every green above could be produced by a tripwire that fires
/// unconditionally — which would be a counter that means nothing at all.
#[test]
fn a_pool_below_its_threshold_reports_nothing() -> Result<(), Box<dyn Error>> {
    let _gate = gate();
    crate::metrics::init();
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(154, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    // Thresholds far above anything this fixture can reach.
    let config = capacity_config(|c| {
        c.live_receipt_server_report_threshold = 1_000;
        c.receipt_provenance_server_report_threshold = 1_000;
        c.receipt_provenance_per_conversation_report_threshold = 1_000;
    });
    let handler = ProductionParticipantHandler::new(store, config)?;

    let (result, warnings) = with_captured_warnings(|| {
        enroll(&handler, incarnation, 3950, [0xC1; 16])?;
        // A landed enrollment against an untripped conversation pool.
        let landed = super::tests::dispatch(
            &handler,
            ConnectionIncarnation::new(154, 2),
            enrollment_request(3950, [0xC2; 16]),
        )?;
        Ok::<_, Box<dyn Error>>(landed)
    });
    let landed = result?;
    assert!(
        matches!(landed, liminal_protocol::wire::ServerValue::EnrollBound(_)),
        "the negative control must still exercise a real admitted operation, got: {landed:?}"
    );
    assert!(
        !warnings.contains("shared receipt pool runaway"),
        "a pool below its reporting threshold must stay silent; captured warnings were: \
         {warnings:?}"
    );
    Ok(())
}
