//! Board #60 §3c. A committed source writes its OWN Unit 2 extension row.
//!
//! The lane's whole risk is that live and cold stop agreeing. The pins here
//! are built so that agreement cannot be greened by accident, and in
//! particular so that the ONE failure the obvious comparison cannot see is
//! covered:
//!
//! > Comparing a live authority against a cold-reopened one is BLIND to a
//! > missing durable extension row. If the live commit forgets to write the
//! > row, the cold restore's repair branch appends it and rebuilds an
//! > identical owner — the states match, and the durable stream was silently
//! > repaired. So every step below asserts, FIRST, that the cold restore
//! > appended nothing: the extension stream read before the reopen must be
//! > byte-identical to the one read after it. That is the pin that fails if
//! > this lane is half-landed, and it fails before the state comparison ever
//! > runs.
//!
//! The second comparison is then the full `Debug` of the authority — state,
//! outbox owner, slots, frontier, AND the observer-progress witness vector,
//! whose merged positions are derived from durable coordinates precisely so
//! that a live producer and a replay producer mint the same ones.

use std::error::Error;
use std::path::Path;
use std::sync::Arc;

use liminal::durability::bridge::block_on;
use liminal_protocol::wire::{
    AttachAttemptToken, AttachBound, ClientRequest, ConnectionIncarnation, CredentialAttachRequest,
    DetachAttemptToken, DetachRequest, EnrollBound, EnrollmentRequest, EnrollmentToken, Generation,
    LeaveAttemptToken, LeaveRequest, MarkerAck, ParticipantAck, RecordAdmission,
    RecordAdmissionAttemptToken, ServerValue,
};

use crate::config::types::ParticipantConfig;

use super::ProductionParticipantHandler;
use super::log::{DecodedStoredOperation, OperationLog, OperationSchemaPhase, StoredOperation};
use super::outbox_log::{OutboxLog, OutboxRow};
use super::outbox_projection::owes_extension_row;
use super::tests::{dispatch, open_disk_store_for_tests, test_participant_config};
use super::tests_history::authority_snapshot;

/// The walk's default: no retention pressure, so a cold reopen replays the log
/// and does nothing else. Boot drain (§6.2 R-BOOT-DRAIN) APPENDS base rows of
/// its own when it finds an occupied candidate lane, which would make a
/// live-vs-cold comparison a comparison of two different logs; the drain arm
/// gets its own pin below, measured without a reopen.
fn walk_config() -> ParticipantConfig {
    test_participant_config()
}

/// Retention tuned to force MANDATORY MARKER DRAINS, so the drain-prefix arm
/// of `apply_record_admission` (`ops_frontier.rs::persist_drain_first` then
/// `persist_record_commit` — two completing sources inside ONE operation) is
/// exercised rather than merely described.
///
/// The drain is driven by RETAINED CAPACITY, not by
/// `max_retained_record_rows`: these are the knobs
/// `tests_marker_ack_fixture::marker_fixture_config` uses to make a handful of
/// ordinary commits generate marker debt that the next one drains.
fn drain_forcing_config() -> ParticipantConfig {
    let mut config = test_participant_config();
    config.retained_capacity_entries = 14;
    config.retained_capacity_bytes = 65_536;
    config.max_retained_record_rows = 16;
    config.max_ordinary_record_bytes = 58;
    config
}

/// Every base row's source kind, counted so the walk can prove it actually
/// contained the shapes these pins claim to cover.
#[derive(Debug, Default, PartialEq, Eq)]
struct SourceCensus {
    genesis: usize,
    enrolled: usize,
    attached: usize,
    detached_owing: usize,
    record_admission: usize,
    marker_drained: usize,
    acks: usize,
    left: usize,
    non_owing: usize,
}

impl SourceCensus {
    fn observe(&mut self, operation: &StoredOperation) {
        if !owes_extension_row(operation) {
            self.non_owing += 1;
        }
        match operation {
            StoredOperation::Genesis { .. } => self.genesis += 1,
            StoredOperation::Enrolled { .. } => self.enrolled += 1,
            StoredOperation::Attached { .. } => self.attached += 1,
            StoredOperation::Detached { .. } => {
                if owes_extension_row(operation) {
                    self.detached_owing += 1;
                }
            }
            StoredOperation::RecordAdmission { .. } => self.record_admission += 1,
            StoredOperation::MarkerDrained { .. } => self.marker_drained += 1,
            StoredOperation::ZeroDebtAck { .. } | StoredOperation::NonzeroDebtAck { .. } => {
                self.acks += 1;
            }
            StoredOperation::Left { .. } => self.left += 1,
            StoredOperation::Ordinary { .. }
            | StoredOperation::Recovered { .. }
            | StoredOperation::Died { .. } => {}
        }
    }
}

fn extension_rows(
    handler: &ProductionParticipantHandler,
    conversation_id: u64,
) -> Result<Vec<(u64, OutboxRow)>, Box<dyn Error>> {
    let log = OutboxLog::new(Arc::clone(&handler.store), conversation_id);
    Ok(block_on(log.read_all())??)
}

fn base_rows(
    handler: &ProductionParticipantHandler,
    conversation_id: u64,
) -> Result<Vec<(u64, StoredOperation)>, Box<dyn Error>> {
    let log = OperationLog::new(Arc::clone(&handler.store), conversation_id);
    let mut rows = Vec::new();
    let mut sequence = 0_u64;
    let mut phase = OperationSchemaPhase::V2Prefix;
    loop {
        let page = block_on(log.read_page(sequence, phase))??;
        phase = page.next_phase;
        if page.rows.is_empty() {
            return Ok(rows);
        }
        let page_len = page.rows.len();
        for decoded in page.rows {
            let DecodedStoredOperation::V3(operation) = decoded.operation else {
                return Err("the live fixture wrote a non-v3 base row".into());
            };
            sequence = decoded.sequence + 1;
            rows.push((decoded.sequence, operation));
        }
        if page_len < super::log::READ_BATCH_SIZE {
            return Ok(rows);
        }
    }
}

/// Pins `owes_extension_row` against the durable bytes, row for row.
///
/// A row that owes an extension row must have exactly one, carrying its own
/// base sequence; a row that owes none must have none. This is the mechanical
/// comparison that keeps the syntactic predicate and the exhaustive projection
/// from drifting apart — a drift that would otherwise show up as either a
/// stranded projection or a fast path wedged off forever, both silent.
fn owed_rows_match_written_rows(
    base: &[(u64, StoredOperation)],
    extension: &[(u64, OutboxRow)],
    label: &str,
) {
    for (sequence, operation) in base {
        let written = extension
            .iter()
            .filter(|(_, row)| match row {
                OutboxRow::Produced(batch) => batch.source_log_sequence() == *sequence,
                OutboxRow::AckAdvanced {
                    source_log_sequence,
                    ..
                } => source_log_sequence == sequence,
                OutboxRow::MarkerAckCommitted(_) => false,
            })
            .count();
        let owed = usize::from(owes_extension_row(operation));
        assert_eq!(
            written, owed,
            "after {label}, base row {sequence} owes {owed} Unit 2 extension rows and has \
             {written}. A shortfall means the live commit did not complete its source and the \
             retired replay is no longer there to repair it; a surplus means the row was \
             written twice"
        );
    }
}

/// One committed request, then the two comparisons, in the order that makes
/// the durable one unskippable.
fn step(
    handler: ProductionParticipantHandler,
    data_dir: &Path,
    conversation_id: u64,
    census: &mut SourceCensus,
    label: &str,
) -> Result<ProductionParticipantHandler, Box<dyn Error>> {
    let live_state = authority_snapshot(&handler, conversation_id)?;
    let live_extension = extension_rows(&handler, conversation_id)?;
    let base = base_rows(&handler, conversation_id)?;
    owed_rows_match_written_rows(&base, &live_extension, label);
    *census = SourceCensus::default();
    for (_, operation) in &base {
        census.observe(operation);
    }
    drop(handler);

    let store = open_disk_store_for_tests(data_dir)?;
    let restored = ProductionParticipantHandler::new(store, walk_config())?;
    let cold_extension = extension_rows(&restored, conversation_id)?;
    assert_eq!(
        live_extension, cold_extension,
        "after {label} a cold restore CHANGED the Unit 2 extension stream. The commit path no \
         longer replays, so a row the live commit failed to write is a row that is durably \
         missing — this is the exact failure a live/cold state comparison alone cannot see"
    );
    let cold_state = authority_snapshot(&restored, conversation_id)?;
    assert_eq!(
        live_state, cold_state,
        "after {label} the live authority and a from-zero replay of the same bytes disagree"
    );
    Ok(restored)
}

fn require_enrolled(value: ServerValue) -> Result<EnrollBound, Box<dyn Error>> {
    let ServerValue::EnrollBound(receipt) = value else {
        return Err(format!("enrollment did not bind: {value:?}").into());
    };
    Ok(receipt)
}

/// Drives every live commit path that writes its own extension row, plus the
/// paths that deliberately still do not, and compares live against cold after
/// each one.
///
/// Walked, in order: genesis+enrollment (`ops_enroll`), a second enrollment
/// that leaves an UNMATCHED open — enrolled, never attached — for the whole
/// run, credential attach (`ops_attach`), ordinary admissions past the
/// retention limit so the drain-prefix arm fires (`ops_frontier`
/// `persist_drain_first` then `persist_record_commit`), an interleaved marker
/// ack and participant ack, an explicit detach (`ops_session`), a resumed
/// attach, more admissions, and a leave.
/// One walk's carried state: the handler moves through every step, and the
/// census is recomputed from the durable log after each one.
struct Walk {
    handler: ProductionParticipantHandler,
    data_dir: std::path::PathBuf,
    conversation_id: u64,
    census: SourceCensus,
    first: ConnectionIncarnation,
    second: ConnectionIncarnation,
}

impl Walk {
    fn advance(mut self, label: &str) -> Result<Self, Box<dyn Error>> {
        self.handler = step(
            self.handler,
            &self.data_dir,
            self.conversation_id,
            &mut self.census,
            label,
        )?;
        Ok(self)
    }

    fn send(
        &self,
        connection: ConnectionIncarnation,
        request: ClientRequest,
    ) -> Result<ServerValue, Box<dyn Error>> {
        dispatch(&self.handler, connection, request)
    }
}

/// Genesis enrollment, then the unmatched open: a member enrolled and never
/// attached, which stays a recipient of every produced record for the rest of
/// the walk.
fn walk_enrollments(walk: Walk) -> Result<(Walk, EnrollBound, EnrollBound), Box<dyn Error>> {
    let conversation_id = walk.conversation_id;
    let sender = require_enrolled(walk.send(
        walk.first,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([0x64; 16]),
        }),
    )?)?;
    let walk = walk.advance("genesis enrollment")?;

    let recipient = require_enrolled(walk.send(
        walk.second,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([0x65; 16]),
        }),
    )?)?;
    let walk = walk.advance("unmatched-open enrollment")?;
    Ok((walk, sender, recipient))
}

/// Attach, explicit detach, reattach: `ops_attach.rs` and `ops_session.rs`,
/// the two binding-side `record_produced_source` callers, plus a superseding
/// rotation so the attach projection needs its replay prestate.
fn walk_binding_rotations(
    walk: Walk,
    sender: &EnrollBound,
) -> Result<(Walk, AttachBound), Box<dyn Error>> {
    let conversation_id = walk.conversation_id;
    let attached = walk.send(
        walk.first,
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id,
            participant_id: sender.participant_id(),
            capability_generation: Generation::ONE,
            attach_secret: sender.attach_secret(),
            attach_attempt_token: AttachAttemptToken::new([0x66; 16]),
            accept_marker_delivery_seq: None,
        }),
    )?;
    let ServerValue::AttachBound(attached) = attached else {
        return Err(format!("attach did not bind: {attached:?}").into());
    };
    let walk = walk.advance("credential attach")?;

    let detached = walk.send(
        walk.first,
        ClientRequest::Detach(DetachRequest {
            conversation_id,
            participant_id: sender.participant_id(),
            capability_generation: attached.capability_generation(),
            detach_attempt_token: DetachAttemptToken::new([0x67; 16]),
        }),
    )?;
    assert!(
        matches!(detached, ServerValue::DetachCommitted(_)),
        "detach did not commit: {detached:?}"
    );
    let walk = walk.advance("explicit detach")?;

    let reattached = walk.send(
        walk.first,
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id,
            participant_id: sender.participant_id(),
            capability_generation: attached.capability_generation(),
            attach_secret: attached.attach_secret(),
            attach_attempt_token: AttachAttemptToken::new([0x69; 16]),
            accept_marker_delivery_seq: None,
        }),
    )?;
    let ServerValue::AttachBound(reattached) = reattached else {
        return Err(format!("reattach did not bind: {reattached:?}").into());
    };
    let walk = walk.advance("reattach")?;
    Ok((walk, reattached))
}

/// A participant ack and a marker ack. Both owe an extension row and NEITHER
/// writes it in place, so their operations keep the from-zero replay as their
/// writer — the other half of the outcome gate, walked in the same fixture.
fn walk_acks(walk: Walk, recipient: &EnrollBound) -> Result<Walk, Box<dyn Error>> {
    let conversation_id = walk.conversation_id;
    let acknowledged = walk.send(
        walk.second,
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id,
            participant_id: recipient.participant_id(),
            capability_generation: Generation::ONE,
            through_seq: 3,
        }),
    )?;
    assert!(
        matches!(acknowledged, ServerValue::AckCommitted(_)),
        "participant ack did not commit: {acknowledged:?}"
    );
    let walk = walk.advance("participant ack")?;

    let marker_ack = walk.send(
        walk.second,
        ClientRequest::MarkerAck(MarkerAck {
            conversation_id,
            participant_id: recipient.participant_id(),
            capability_generation: Generation::ONE,
            marker_delivery_seq: 4,
        }),
    )?;
    walk.advance(&format!("marker ack ({marker_ack:?})"))
}

/// The plain admission arm: `ops_frontier.rs::persist_record_commit`, once per
/// record, each one compared against a cold replay of its own bytes.
fn walk_admissions(
    mut walk: Walk,
    sender: &EnrollBound,
    generation: Generation,
) -> Result<Walk, Box<dyn Error>> {
    let conversation_id = walk.conversation_id;
    for nonce in 0_u8..14 {
        let recorded = walk.send(
            walk.first,
            ClientRequest::RecordAdmission(RecordAdmission {
                conversation_id,
                participant_id: sender.participant_id(),
                capability_generation: generation,
                record_admission_attempt_token: RecordAdmissionAttemptToken::new([
                    0x70, nonce, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                ]),
                payload: vec![nonce; 4],
            }),
        )?;
        assert!(
            matches!(recorded, ServerValue::RecordCommitted(_)),
            "admission {nonce} did not commit: {recorded:?}"
        );
        walk = walk.advance(&format!("ordinary admission {nonce}"))?;
    }
    Ok(walk)
}

/// Drives every live commit path that writes its own extension row, plus the
/// paths that deliberately still do not, and compares live against cold after
/// each one.
///
/// Walked, in order: genesis+enrollment (`ops_enroll`), a second enrollment
/// that leaves an UNMATCHED open for the whole run, credential attach
/// (`ops_attach`), an explicit detach (`ops_session`), a superseding reattach,
/// a participant ack and a marker ack interleaved, and fourteen ordinary
/// admissions (`ops_frontier`).
#[test]
fn every_live_commit_path_matches_a_cold_replay_of_its_own_bytes() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let conversation_id = 0xF0_64_01;
    let store = open_disk_store_for_tests(&data_dir)?;
    let walk = Walk {
        handler: ProductionParticipantHandler::new(store, walk_config())?,
        data_dir,
        conversation_id,
        census: SourceCensus::default(),
        first: ConnectionIncarnation::new(0x64, 1),
        second: ConnectionIncarnation::new(0x64, 2),
    };

    let (walk, sender, recipient) = walk_enrollments(walk)?;
    let (walk, reattached) = walk_binding_rotations(walk, &sender)?;
    let walk = walk_acks(walk, &recipient)?;
    let walk = walk_admissions(walk, &sender, reattached.capability_generation())?;

    // The walk must have CONTAINED what the pins above claim to have covered.
    // Without this, every comparison over a thin log is vacuously true.
    let census = &walk.census;
    assert!(census.genesis >= 1, "no genesis row: {census:?}");
    assert!(
        census.enrolled >= 2,
        "fewer than two enrollments: {census:?}"
    );
    assert!(
        census.attached >= 2,
        "fewer than two attach rows: {census:?}"
    );
    assert!(
        census.detached_owing >= 1,
        "no owing detach row: {census:?}"
    );
    assert!(
        census.record_admission >= 14,
        "fewer than fourteen admissions: {census:?}"
    );
    assert!(census.acks >= 1, "no participant ack row: {census:?}");
    assert!(
        census.non_owing >= 1,
        "every row owed an extension row, so `owes_extension_row` was never observed \
         discriminating: {census:?}"
    );
    Ok(())
}

/// Lifts observer progress off zero and fills the retained window.
///
/// A transient third member enrolls, everyone acks, and it leaves; without
/// those acks the very first admission is refused for observer backpressure.
/// This is the shape `tests_marker_ack_fixture::prepare_marker_fixture` uses.
fn prepare_drain_fixture(
    handler: &ProductionParticipantHandler,
    conversation_id: u64,
    connections: [ConnectionIncarnation; 3],
    members: [&EnrollBound; 2],
) -> Result<(), Box<dyn Error>> {
    let [first, second, third] = connections;
    let transient = require_enrolled(dispatch(
        handler,
        third,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([0x73; 16]),
        }),
    )?)?;
    ack_both(handler, conversation_id, [first, second], members, 3)?;
    let left = dispatch(
        handler,
        third,
        ClientRequest::Leave(LeaveRequest {
            conversation_id,
            participant_id: transient.participant_id(),
            capability_generation: Generation::ONE,
            attach_secret: transient.attach_secret(),
            leave_attempt_token: LeaveAttemptToken::new([0x78; 16]),
        }),
    )?;
    let ServerValue::LeaveCommitted(left) = left else {
        return Err(format!("transient member did not leave: {left:?}").into());
    };
    ack_both(
        handler,
        conversation_id,
        [first, second],
        members,
        left.left_delivery_seq(),
    )
}

fn ack_both(
    handler: &ProductionParticipantHandler,
    conversation_id: u64,
    connections: [ConnectionIncarnation; 2],
    members: [&EnrollBound; 2],
    through_seq: u64,
) -> Result<(), Box<dyn Error>> {
    for (connection, member) in connections.into_iter().zip(members) {
        let acknowledged = dispatch(
            handler,
            connection,
            ClientRequest::ParticipantAck(ParticipantAck {
                conversation_id,
                participant_id: member.participant_id(),
                capability_generation: Generation::ONE,
                through_seq,
            }),
        )?;
        assert!(
            matches!(
                acknowledged,
                ServerValue::AckCommitted(_) | ServerValue::AckNoOp(_)
            ),
            "ack through {through_seq} for participant {} was refused: {acknowledged:?}",
            member.participant_id()
        );
    }
    Ok(())
}

/// Physical extension order must follow base-log order.
///
/// `apply_boundary` walks the extension stream forward and refuses a boundary
/// below the base head it has reached, so a pair written out of order is not a
/// cosmetic fault — it is an unloadable conversation.
fn extension_boundaries_ascend(extension: &[(u64, OutboxRow)]) -> Result<(), Box<dyn Error>> {
    let mut previous_boundary = 0_u64;
    for (physical_sequence, row) in extension {
        let boundary = row
            .base_log_head()
            .ok_or("extension row boundary overflowed")?;
        assert!(
            boundary >= previous_boundary,
            "extension row at physical sequence {physical_sequence} carries base boundary \
             {boundary} below its predecessor's {previous_boundary}: the live commit wrote its \
             sources out of base-log order"
        );
        previous_boundary = boundary;
    }
    Ok(())
}

/// Admits until the drain-prefix arm fires, checking the durable stream after
/// every admission, and returns the census that proves it fired.
///
/// The sender never acks, so the retention floor climbs past its cursor and
/// plans a compaction marker; the recipient acks every record, which lifts the
/// observer-progress clamp that would otherwise refuse the admission for
/// backpressure. The marker planned by one admission is DRAINED by the next —
/// that admission is the drain-prefix arm. It stops there: pushing further
/// only walks the marker-anchor capacity refusal, a different lane's subject.
fn drive_admissions_until_drain(
    handler: &ProductionParticipantHandler,
    conversation_id: u64,
    connections: [ConnectionIncarnation; 2],
    sender: &EnrollBound,
    recipient: &EnrollBound,
) -> Result<SourceCensus, Box<dyn Error>> {
    let [first, second] = connections;
    let mut census = SourceCensus::default();
    for nonce in 0_u8..12 {
        let recorded = dispatch(
            handler,
            first,
            ClientRequest::RecordAdmission(RecordAdmission {
                conversation_id,
                participant_id: sender.participant_id(),
                capability_generation: Generation::ONE,
                record_admission_attempt_token: RecordAdmissionAttemptToken::new([
                    0x76, nonce, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                ]),
                payload: vec![nonce; 1],
            }),
        )?;
        let ServerValue::RecordCommitted(committed) = recorded else {
            return Err(format!("admission {nonce} did not commit: {recorded:?}").into());
        };

        // After EVERY admission the durable extension stream must already be
        // complete against the durable base log. Checking inside the loop —
        // rather than once at the end — means the first drain-bearing
        // admission is the one that reports.
        let base = base_rows(handler, conversation_id)?;
        let extension = extension_rows(handler, conversation_id)?;
        owed_rows_match_written_rows(&base, &extension, &format!("admission {nonce}"));
        extension_boundaries_ascend(&extension)?;
        census = SourceCensus::default();
        for (_, operation) in &base {
            census.observe(operation);
        }
        if census.marker_drained >= 1 {
            break;
        }

        ack_both(
            handler,
            conversation_id,
            [second, second],
            [recipient, recipient],
            committed.delivery_seq(),
        )?;
    }
    Ok(census)
}

/// The drain-prefix arm: ONE operation that appends TWO sources, both of which
/// must complete themselves, in base-log order.
///
/// `apply_record_admission` answers a planned compaction marker by draining
/// first (`ops_frontier.rs::persist_drain_first` → `persist_next_marker`, one
/// of the five `record_produced_source` callers) and then committing the
/// admission (`persist_record_commit`, another). Under §3c both write their
/// own extension row, and the ORDER is the whole risk: the extension stream is
/// physically ordered and the replay's repair branch can only append at
/// confirmed EOF, so a marker row written after the admission's row would make
/// the conversation unloadable — which is exactly how this lane's first
/// implementation failed, across five suites at once.
///
/// This is measured on the live handler's own store with NO reopen, because a
/// reopen of a drain-forcing conversation runs boot drain, which appends base
/// rows of its own — a second writer whose rows would be indistinguishable
/// from a repair. Nothing here is inferred from a comparison; the durable
/// bytes are read directly and required to be complete and ordered.
#[test]
fn a_drain_prefixed_admission_writes_both_extension_rows_in_base_log_order()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let conversation_id = 0xF0_64_02;
    let first = ConnectionIncarnation::new(0x65, 1);
    let second = ConnectionIncarnation::new(0x65, 2);
    let third = ConnectionIncarnation::new(0x65, 3);

    let store = open_disk_store_for_tests(&data_dir)?;
    let handler = ProductionParticipantHandler::new(store, drain_forcing_config())?;

    let sender = require_enrolled(dispatch(
        &handler,
        first,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([0x74; 16]),
        }),
    )?)?;
    let recipient = require_enrolled(dispatch(
        &handler,
        second,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([0x75; 16]),
        }),
    )?)?;
    prepare_drain_fixture(
        &handler,
        conversation_id,
        [first, second, third],
        [&sender, &recipient],
    )?;
    let census = drive_admissions_until_drain(
        &handler,
        conversation_id,
        [first, second],
        &sender,
        &recipient,
    )?;

    assert!(
        census.marker_drained >= 1,
        "retention never forced a marker drain, so the drain-prefix arm — the only shape in \
         which one operation completes TWO sources — was never walked: {census:?}"
    );
    assert!(
        census.record_admission >= 2,
        "the drain fired before the conversation had a history to compact: {census:?}"
    );
    Ok(())
}
