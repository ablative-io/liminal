use crate::config::types::ParticipantConfig;

use liminal::durability::bridge::block_on;
use liminal::durability::{DurableStore, open_ephemeral};

use liminal_protocol::lifecycle::{CapacityCounter, ConnectionConversationTracking};
use liminal_protocol::wire::{
    AttachAttemptToken, ClientRequest, ConnectionIncarnation, CredentialAttachRequest, EnrollBound,
    EnrollmentRequest, EnrollmentToken, Generation, LeaveAttemptToken, LeaveRequest,
    ParticipantAck, ParticipantDelivery, ParticipantId, ParticipantRecord, RecordAdmission,
    RecordAdmissionAttemptToken, RecordCommitted, ServerValue,
};

use std::error::Error;
use std::sync::Arc;

use super::ProductionParticipantHandler;
use super::barrier::{OperationFacts, ReceiptCapacityLimits};
use super::log::{OperationLog, OperationLogError, StoredOperation};
use super::outbox_log::{OutboxLog, OutboxRow, ProducedBatch, ProducedSourceKind, ProjectedRecord};
use super::state::{ConversationAuthority, DurableAppend};
use super::tests::{dispatch, test_participant_config};

pub(super) struct FixtureAppender<'a> {
    pub(super) log: &'a OperationLog,
}

impl DurableAppend for FixtureAppender<'_> {
    fn append(
        &self,
        operation: &StoredOperation,
        expected_sequence: u64,
    ) -> Result<(), OperationLogError> {
        block_on(self.log.append(operation, expected_sequence))?
    }
}

pub(super) struct MarkerFixture {
    pub(super) handler: ProductionParticipantHandler,
    pub(super) store: Arc<dyn DurableStore>,
    pub(super) target_connection: ConnectionIncarnation,
    pub(super) target_participant: ParticipantId,
    pub(super) record_connection: ConnectionIncarnation,
    pub(super) record_participant: ParticipantId,
    pub(super) catchup_connection: ConnectionIncarnation,
    pub(super) catchup_participant: ParticipantId,
    pub(super) catchup_through_seq: u64,
    pub(super) marker_delivery: ParticipantDelivery,
}

/// The capability generation each member is currently addressable at.
///
/// Threaded as a PARAMETER rather than hardcoded, mirroring
/// `tests_w1b_pending_died_restart.rs:151/:167`. Pre-attach callers pass
/// `FixtureGenerations::PRE_ATTACH` EXPLICITLY, so they hand these helpers the
/// exact value the helpers used to hardcode -- behaviour-identity by
/// construction rather than by comparison.
#[derive(Clone, Copy)]
pub(super) struct FixtureGenerations {
    pub(super) first: Generation,
    pub(super) second: Generation,
}

impl FixtureGenerations {
    /// The value every caller hardcoded before this parameter existed.
    pub(super) const PRE_ATTACH: Self = Self {
        first: Generation::ONE,
        second: Generation::ONE,
    };
}

struct FixtureMembers {
    first_connection: ConnectionIncarnation,
    second_connection: ConnectionIncarnation,
    first: EnrollBound,
    second: EnrollBound,
}

pub(super) fn marker_fixture_config() -> ParticipantConfig {
    let mut config = test_participant_config();
    // After retiring the transient peer, three ordinary commits generate marker
    // debt and the fourth drains it, preserving the original two-member fixture.
    config.retained_capacity_entries = 14;
    config.retained_capacity_bytes = 65_536;
    config.max_retained_record_rows = 16;
    config.max_ordinary_record_bytes = 58;
    config
}

pub(super) fn marker_fixture_facts(
    connection: ConnectionIncarnation,
    config: &ParticipantConfig,
) -> Result<OperationFacts, Box<dyn Error>> {
    let connection_capacity =
        CapacityCounter::try_new(config.max_semantic_conversations_per_connection, 0)
            .map_err(|error| format!("marker fixture connection capacity is invalid: {error:?}"))?;
    Ok(OperationFacts {
        receiving_incarnation: connection,
        now_ms: 0,
        identity_slots: config.identity_slots,
        attach_receipt_ttl_ms: config.attach_receipt_ttl_ms,
        receipt_provenance_ttl_ms: config.receipt_provenance_ttl_ms,
        receipt_limits: ReceiptCapacityLimits::from_config(config),
        connection_tracking: ConnectionConversationTracking::Untracked,
        connection_capacity,
    })
}

fn append_fixture_outbox_row(
    authority: &mut ConversationAuthority,
    outbox_log: &OutboxLog,
    row: OutboxRow,
) -> Result<(), Box<dyn Error>> {
    let extension_sequence = authority
        .outbox
        .as_ref()
        .ok_or("marker fixture outbox owner is absent")?
        .next_extension_sequence();
    block_on(outbox_log.append(&row, extension_sequence))??;
    authority
        .outbox
        .as_mut()
        .ok_or("marker fixture outbox owner disappeared")?
        .apply_row(extension_sequence, row)?;
    Ok(())
}

fn enroll_members(
    handler: &ProductionParticipantHandler,
    conversation_id: u64,
) -> Result<FixtureMembers, Box<dyn Error>> {
    let first_connection = ConnectionIncarnation::new(0xA7, 1);
    let second_connection = ConnectionIncarnation::new(0xA7, 2);
    let first = dispatch(
        handler,
        first_connection,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([0xA1; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(first) = first else {
        return Err(format!("first marker fixture enrollment failed: {first:?}").into());
    };
    let second = dispatch(
        handler,
        second_connection,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([0xA2; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(second) = second else {
        return Err(format!("second marker fixture enrollment failed: {second:?}").into());
    };
    Ok(FixtureMembers {
        first_connection,
        second_connection,
        first,
        second,
    })
}

/// One participant's ack endpoint, DERIVED from the live obligations index
/// rather than hardcoded (Cally c1a876e0).
///
/// The discriminator proved the fixture's hardcoded `through_seq: 3` was the
/// whole defect: pre-attach the index is `[2, 3]` so 3 IS an endpoint and the
/// ack commits; post-attach the attach's own deliveries consume sequence
/// numbers, the index becomes `[2, 5]`, 3 is NOT an endpoint, and
/// `participant_ack.rs:310-311` refuses with `AckGap`. The sequence number was
/// never the thing that mattered -- being an ENDPOINT was.
///
/// So the fixture stops guessing. `contiguously_available_through`, the second
/// return of `recipient_ack_obligations`, is the final endpoint of that index
/// -- measured Ok(3) against `[2, 3]` and Ok(5) against `[2, 5]` -- and it is
/// the same observable that decided the discriminator.
#[derive(Clone, Debug)]
#[allow(
    dead_code,
    reason = "these fields are read through Debug when a fixture fails -- that \
              is their whole job. Deleting them would shrink the failure output \
              of the units that exist to explain this observable."
)]
pub(super) struct DerivedAckEndpoint {
    pub(super) participant_id: ParticipantId,
    pub(super) acknowledged_through: u64,
    pub(super) endpoint: u64,
    pub(super) index_debug: String,
}

/// Mirrors production's own derivation at `ops_acks.rs:65-70` exactly, so the
/// fixture asks the question the server asks.
fn live_ack_endpoint(
    handler: &ProductionParticipantHandler,
    conversation_id: u64,
    participant_id: ParticipantId,
) -> Result<DerivedAckEndpoint, Box<dyn Error>> {
    let cell = handler.cell(conversation_id)?;
    let guard = cell
        .lock()
        .map_err(|_| "live ack endpoint owner lock was poisoned")?;
    let authority = guard
        .as_ref()
        .ok_or("live ack endpoint conversation owner was absent")?;
    let outbox = authority
        .outbox
        .as_ref()
        .ok_or("live ack endpoint outbox is absent")?;
    let acknowledged_through = authority.slots.get(&participant_id).map_or_else(
        || outbox.durable_ack_through(participant_id),
        |slot| slot.member.cursor(),
    );
    let (obligations, endpoint) = outbox
        .recipient_ack_obligations(participant_id, acknowledged_through)
        .map_err(|error| format!("live ack endpoint obligations refused: {error:?}"))?;
    let derived = DerivedAckEndpoint {
        participant_id,
        acknowledged_through,
        endpoint,
        index_debug: format!("{obligations:?}"),
    };
    drop(guard);
    Ok(derived)
}

fn ack_members_through(
    handler: &ProductionParticipantHandler,
    conversation_id: u64,
    members: &FixtureMembers,
    generations: FixtureGenerations,
) -> Result<Vec<DerivedAckEndpoint>, Box<dyn Error>> {
    let mut derived_all = Vec::new();
    for (connection, participant_id, capability_generation) in [
        (
            members.first_connection,
            members.first.participant_id(),
            generations.first,
        ),
        (
            members.second_connection,
            members.second.participant_id(),
            generations.second,
        ),
    ] {
        // DERIVE, DO NOT GUESS.
        let derived = live_ack_endpoint(handler, conversation_id, participant_id)?;
        let through_seq = derived.endpoint;
        derived_all.push(derived);
        if through_seq == 0 {
            // Nothing outstanding: acking zero is a no-op the server would
            // refuse as a regression, so skip rather than manufacture a request.
            continue;
        }
        let outcome = dispatch(
            handler,
            connection,
            ClientRequest::ParticipantAck(ParticipantAck {
                conversation_id,
                participant_id,
                capability_generation,
                through_seq,
            }),
        )?;
        if !matches!(outcome, ServerValue::AckCommitted(_)) {
            return Err(format!(
                "marker fixture prefix ack did not commit for participant {participant_id} at \
                 generation {capability_generation:?} through_seq {through_seq}: {outcome:?}"
            )
            .into());
        }
    }
    Ok(derived_all)
}

fn ack_marker_prefix(
    handler: &ProductionParticipantHandler,
    conversation_id: u64,
    members: &FixtureMembers,
    generations: FixtureGenerations,
) -> Result<Vec<DerivedAckEndpoint>, Box<dyn Error>> {
    let third_connection = ConnectionIncarnation::new(0xA7, 3);
    let third = dispatch(
        handler,
        third_connection,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id,
            enrollment_token: EnrollmentToken::new([0xA0; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(third) = third else {
        return Err(format!("third marker fixture enrollment failed: {third:?}").into());
    };
    let mut derived_all = ack_members_through(handler, conversation_id, members, generations)?;

    let left = dispatch(
        handler,
        third_connection,
        ClientRequest::Leave(LeaveRequest {
            conversation_id,
            participant_id: third.participant_id(),
            capability_generation: Generation::ONE,
            attach_secret: third.attach_secret(),
            leave_attempt_token: LeaveAttemptToken::new([0xA6; 16]),
        }),
    )?;
    if !matches!(left, ServerValue::LeaveCommitted(_)) {
        return Err(format!("marker fixture transient peer did not leave: {left:?}").into());
    }
    derived_all.extend(ack_members_through(
        handler,
        conversation_id,
        members,
        generations,
    )?);
    Ok(derived_all)
}

fn commit_fixture_record(
    authority: &mut ConversationAuthority,
    operation_log: &OperationLog,
    config: &ParticipantConfig,
    connection: ConnectionIncarnation,
    request: &RecordAdmission,
    expected_rows: u64,
) -> Result<(u64, RecordCommitted), Box<dyn Error>> {
    let source_sequence = authority.next_log_sequence;
    let outcome = authority.apply_record_admission(
        request,
        &marker_fixture_facts(connection, config)?,
        config,
        &FixtureAppender { log: operation_log },
    )?;
    let ServerValue::RecordCommitted(record) = outcome.value else {
        return Err(format!("marker fixture record did not commit: {:?}", outcome.value).into());
    };
    if authority.next_log_sequence != source_sequence + expected_rows {
        return Err(format!(
            "record at source {source_sequence} appended an unexpected row count: expected {}, \
             got {}",
            source_sequence + expected_rows,
            authority.next_log_sequence
        )
        .into());
    }
    Ok((source_sequence, record))
}

fn project_fixture_ordinary(
    authority: &mut ConversationAuthority,
    outbox_log: &OutboxLog,
    source_sequence: u64,
    record: &RecordCommitted,
    request: &RecordAdmission,
    members: &FixtureMembers,
) -> Result<(), Box<dyn Error>> {
    let projected = ProjectedRecord::try_new(
        request.conversation_id,
        record.delivery_seq(),
        ParticipantRecord::OrdinaryRecord {
            sender_participant_id: members.first.participant_id(),
            payload: request.payload.clone(),
        },
        vec![members.second.participant_id()],
        Some(members.first.participant_id()),
    )?;
    append_fixture_outbox_row(
        authority,
        outbox_log,
        OutboxRow::Produced(ProducedBatch::new(
            source_sequence,
            ProducedSourceKind::RecordAdmission,
            vec![projected],
        )),
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "a test fixture helper: each argument is one piece of the durable \
              state an ack commit reads, and bundling them into a struct would \
              hide which pieces a given call actually exercises -- the thing \
              these fixtures exist to make visible."
)]
fn commit_fixture_ack(
    authority: &mut ConversationAuthority,
    operation_log: &OperationLog,
    outbox_log: &OutboxLog,
    config: &ParticipantConfig,
    conversation_id: u64,
    members: &FixtureMembers,
    generations: FixtureGenerations,
    through_seq: u64,
) -> Result<(), Box<dyn Error>> {
    let source_log_sequence = authority.next_log_sequence;
    let request = ParticipantAck {
        conversation_id,
        participant_id: members.second.participant_id(),
        capability_generation: generations.second,
        through_seq,
    };
    let outcome = authority.apply_ack(
        &request,
        &marker_fixture_facts(members.second_connection, config)?,
        &FixtureAppender { log: operation_log },
    )?;
    if !matches!(outcome.value, ServerValue::AckCommitted(_)) {
        return Err(format!(
            "marker fixture ordinary ack did not commit: {:?}",
            outcome.value
        )
        .into());
    }
    append_fixture_outbox_row(
        authority,
        outbox_log,
        OutboxRow::AckAdvanced {
            source_log_sequence,
            participant_id: members.second.participant_id(),
            through_seq,
        },
    )
}

fn record_request(
    conversation_id: u64,
    participant_id: u64,
    capability_generation: Generation,
    token: u8,
) -> RecordAdmission {
    RecordAdmission {
        conversation_id,
        participant_id,
        capability_generation,
        record_admission_attempt_token: RecordAdmissionAttemptToken::new([token; 16]),
        payload: vec![token],
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "a fixture that drives one continuous durable sequence; the steps \
              are ordered and each depends on the last, so splitting it would \
              scatter a single scenario across helpers and make the ordering \
              harder to check than it is to read."
)]
fn drive_marker_drain(
    handler: &ProductionParticipantHandler,
    store: Arc<dyn DurableStore>,
    config: &ParticipantConfig,
    conversation_id: u64,
    members: &FixtureMembers,
    generations: FixtureGenerations,
) -> Result<
    (
        ConnectionIncarnation,
        ParticipantId,
        ParticipantDelivery,
        u64,
    ),
    Box<dyn Error>,
> {
    let operation_log = OperationLog::new(Arc::clone(&store), conversation_id);
    let outbox_log = OutboxLog::new(store, conversation_id);
    let cell = handler.cell(conversation_id)?;
    let mut owner = cell
        .lock()
        .map_err(|_| "marker fixture conversation owner lock was poisoned")?;
    let authority = owner
        .as_mut()
        .ok_or("marker fixture conversation owner was absent")?;

    for token in [0xA3, 0xA4, 0xA5] {
        let request = record_request(
            conversation_id,
            members.first.participant_id(),
            generations.first,
            token,
        );
        let (source, record) = commit_fixture_record(
            authority,
            &operation_log,
            config,
            members.first_connection,
            &request,
            1,
        )?;
        if let Some(early) = authority.last_marker_projection.take() {
            return Err(format!(
                "a marker projected EARLY, at debt-building record {token:#x} rather than at the \
                 draining commit: {early:?}"
            )
            .into());
        }
        project_fixture_ordinary(authority, &outbox_log, source, &record, &request, members)?;
        commit_fixture_ack(
            authority,
            &operation_log,
            &outbox_log,
            config,
            conversation_id,
            members,
            generations,
            record.delivery_seq(),
        )?;
    }

    let request = record_request(
        conversation_id,
        members.first.participant_id(),
        generations.first,
        0xA8,
    );
    let (marker_source, record) = commit_fixture_record(
        authority,
        &operation_log,
        config,
        members.first_connection,
        &request,
        2,
    )?;
    let marker = authority
        .last_marker_projection
        .take()
        .ok_or("drain admission did not surrender a marker projection")?;
    let target = match marker.record {
        ParticipantRecord::HistoryCompacted {
            affected_participant_id,
            ..
        } => affected_participant_id,
        ref other => return Err(format!("marker projection was not a marker: {other:?}").into()),
    };
    let target_connection = if target == members.first.participant_id() {
        members.first_connection
    } else if target == members.second.participant_id() {
        members.second_connection
    } else {
        return Err("marker targeted an unknown participant".into());
    };

    let marker_record = ProjectedRecord::try_new(
        conversation_id,
        marker.delivery_seq,
        marker.record.clone(),
        vec![
            members.first.participant_id(),
            members.second.participant_id(),
        ],
        None,
    )?;
    append_fixture_outbox_row(
        authority,
        &outbox_log,
        OutboxRow::Produced(ProducedBatch::new(
            marker_source,
            ProducedSourceKind::MarkerDrained,
            vec![marker_record],
        )),
    )?;
    project_fixture_ordinary(
        authority,
        &outbox_log,
        marker_source + 1,
        &record,
        &request,
        members,
    )?;
    drop(owner);
    Ok((target_connection, target, marker, record.delivery_seq()))
}

pub(super) fn prepare_marker_fixture() -> Result<MarkerFixture, Box<dyn Error>> {
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let config = marker_fixture_config();
    let conversation_id = 0xA7;
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), config)?;
    let members = enroll_members(&handler, conversation_id)?;
    // Unattached path: the derivation must reproduce the values this call site
    // used to hardcode (3 then 4). It is discarded here and PRINTED on the
    // attached path, where the whole question lives.
    let _derived = ack_marker_prefix(
        &handler,
        conversation_id,
        &members,
        FixtureGenerations::PRE_ATTACH,
    )?;
    let (target_connection, target_participant, marker_delivery, catchup_through_seq) =
        drive_marker_drain(
            &handler,
            Arc::clone(&store),
            &config,
            conversation_id,
            &members,
            FixtureGenerations::PRE_ATTACH,
        )?;
    Ok(MarkerFixture {
        handler,
        store,
        target_connection,
        target_participant,
        record_connection: members.first_connection,
        record_participant: members.first.participant_id(),
        catchup_connection: members.second_connection,
        catchup_participant: members.second.participant_id(),
        catchup_through_seq,
        marker_delivery,
    })
}

/// Outcome of the F8 PRECONDITION MEASUREMENT (ruling a35c1cb7): does the
/// marker still drain under `marker_fixture_config` AS TUNED once the members
/// have attached?
///
/// This is a measurement, not a rework. `prepare_marker_fixture` above is
/// untouched; this is an additive sibling that differs from it in exactly one
/// respect — two `CredentialAttach` dispatches after enrolment — so any
/// difference in outcome is attributable to the attaches and to nothing else.
/// The config is used exactly as tuned; no debt arithmetic is retuned here.
#[allow(
    dead_code,
    reason = "`derived_ack_endpoints` is carried so a failing drain can print \
              the endpoint index it derived. It is evidence for the reader of a \
              failure, not an input to the assertion -- which is exactly the \
              shape dead_code cannot distinguish."
)]
pub(super) struct AttachedDrainAttempt {
    /// POSITIVE CONTROL that the attaches actually landed. Part A established
    /// that without a `CredentialAttach` no participant ever holds a
    /// binding-fate token (`ops_attach.rs:331-337` is the sole mint), so the
    /// APPEARANCE of these tokens IS the attach observed. `false` here means
    /// the measurement never got the state it was built to test.
    pub(super) first_has_binding_fate: bool,
    pub(super) second_has_binding_fate: bool,
    /// CONDITION (b): the derived endpoint AND the live index it came from,
    /// printed in the teed log. The observable that decided the discriminator
    /// now serves the fixture.
    pub(super) derived_ack_endpoints: Vec<DerivedAckEndpoint>,
    /// The DRAIN WITNESS, non-idempotent by construction: `drive_marker_drain`
    /// only returns `Ok` if `authority.last_marker_projection` surrendered a
    /// marker projection at the fourth commit — a one-shot value consumed by
    /// `.take()` that exists ONLY if a drain actually projected. It is an
    /// event, not a state that could legitimately sit still.
    pub(super) drain: Result<MarkerFixture, String>,
}

pub(super) fn attempt_marker_fixture_with_attaches() -> Result<AttachedDrainAttempt, Box<dyn Error>>
{
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let config = marker_fixture_config();
    let conversation_id = 0xA7;
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), config)?;
    let members = enroll_members(&handler, conversation_id)?;

    // THE ONLY DIFFERENCE FROM `prepare_marker_fixture`: two CredentialAttach
    // dispatches, and the POST-ATTACH generation each one mints is captured so
    // every later request is addressed to the generation that now exists rather
    // than to the pre-attach ONE.
    let mut attached_generations = Vec::new();
    for (connection, receipt) in [
        (members.first_connection, &members.first),
        (members.second_connection, &members.second),
    ] {
        let attached = dispatch(
            &handler,
            connection,
            ClientRequest::CredentialAttach(CredentialAttachRequest {
                conversation_id,
                participant_id: receipt.participant_id(),
                // PRE-ATTACH by definition: this is the request that ends the
                // pre-attach era, so it is addressed at the enrolled generation.
                capability_generation: Generation::ONE,
                attach_secret: receipt.attach_secret(),
                attach_attempt_token: AttachAttemptToken::new([0xB1; 16]),
                accept_marker_delivery_seq: None,
            }),
        )?;
        let ServerValue::AttachBound(bound) = attached else {
            return Err(format!(
                "precondition measurement: participant {} did not attach: {attached:?}",
                receipt.participant_id()
            )
            .into());
        };
        attached_generations.push(bound.origin_binding_epoch().capability_generation);
    }
    let [first_generation, second_generation] = attached_generations.as_slice() else {
        return Err("precondition measurement: expected exactly two attach generations".into());
    };
    let generations = FixtureGenerations {
        first: *first_generation,
        second: *second_generation,
    };

    let (first_has_binding_fate, second_has_binding_fate) = {
        let cell = handler.cell(conversation_id)?;
        let guard = cell
            .lock()
            .map_err(|_| "precondition measurement owner lock was poisoned")?;
        let authority = guard
            .as_ref()
            .ok_or("precondition measurement conversation owner was absent")?;
        let has = |participant_id: ParticipantId| {
            authority
                .slots
                .get(&participant_id)
                .is_some_and(|slot| slot.binding_fate.is_some())
        };
        let pair = (
            has(members.first.participant_id()),
            has(members.second.participant_id()),
        );
        drop(guard);
        pair
    };

    // CONDITION 4: every failure exit of everything this measurement DRIVES --
    // the helpers' former assert!s included, now returned errors -- lands in the
    // reportable field. Nothing escapes as a panic, so the positive control
    // above always reaches the report.
    let mut derived_ack_endpoints = Vec::new();
    let driven =
        ack_marker_prefix(&handler, conversation_id, &members, generations).and_then(|derived| {
            derived_ack_endpoints = derived;
            drive_marker_drain(
                &handler,
                Arc::clone(&store),
                &config,
                conversation_id,
                &members,
                generations,
            )
        });
    let drain = match driven {
        Ok((target_connection, target_participant, marker_delivery, catchup_through_seq)) => {
            Ok(MarkerFixture {
                handler,
                store,
                target_connection,
                target_participant,
                record_connection: members.first_connection,
                record_participant: members.first.participant_id(),
                catchup_connection: members.second_connection,
                catchup_participant: members.second.participant_id(),
                catchup_through_seq,
                marker_delivery,
            })
        }
        Err(error) => Err(error.to_string()),
    };

    Ok(AttachedDrainAttempt {
        first_has_binding_fate,
        second_has_binding_fate,
        derived_ack_endpoints,
        drain,
    })
}

/// THE ARMED FIXTURE (attempt 2, Cally b23e4cc1). `prepare_marker_fixture`
/// mints no binding-fate token, so every Died row it produces is intent-free
/// and §1's incident cannot occur in it -- Part A's finding, and the honest
/// refusal's cited cause. This entry point drives the `CredentialAttach` that
/// mints the intent, and the drain verdict at tree 9440a06 proved the marker
/// still mints and retains under it (EXIT (1), projection at `delivery_seq` 12).
pub(super) fn attached_marker_fixture() -> Result<MarkerFixture, Box<dyn Error>> {
    let attempt = attempt_marker_fixture_with_attaches()?;
    if !(attempt.first_has_binding_fate && attempt.second_has_binding_fate) {
        return Err(format!(
            "armed fixture: the attaches did not mint binding-fate tokens (first={} second={}), \
             so the incident geometry is NOT built and no unit may claim to witness it",
            attempt.first_has_binding_fate, attempt.second_has_binding_fate
        )
        .into());
    }
    attempt.drain.map_err(|error| -> Box<dyn Error> {
        format!("armed fixture: the marker did not drain with attaches present: {error}").into()
    })
}

pub(super) fn marker_protocol_snapshot(
    handler: &ProductionParticipantHandler,
    conversation_id: u64,
    participant_id: ParticipantId,
) -> Result<(u64, String), Box<dyn Error>> {
    let cell = handler.cell(conversation_id)?;
    let owner = cell
        .lock()
        .map_err(|_| "marker snapshot owner lock was poisoned")?;
    let authority = owner.as_ref().ok_or("marker snapshot owner was absent")?;
    let cursor = authority
        .slots
        .get(&participant_id)
        .ok_or("marker snapshot participant was absent")?
        .member
        .cursor();
    let frontier = format!("{:?}", authority.obligation_debt_dispatch);
    drop(owner);
    Ok((cursor, frontier))
}
