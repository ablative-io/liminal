use std::error::Error;
use std::sync::Arc;

use liminal::durability::bridge::block_on;
use liminal::durability::{DurableStore, open_ephemeral};
use liminal_protocol::lifecycle::{CapacityCounter, ConnectionConversationTracking};
use liminal_protocol::wire::{
    AttachAttemptToken, ClientRequest, ConnectionIncarnation, CredentialAttachRequest, EnrollBound,
    EnrollmentRequest, EnrollmentToken, Generation, LeaveAttemptToken, LeaveRequest, ParticipantAck,
    ParticipantDelivery, ParticipantId, ParticipantRecord, RecordAdmission,
    RecordAdmissionAttemptToken, RecordCommitted, ServerValue,
};

use super::ProductionParticipantHandler;
use super::barrier::{OperationFacts, ReceiptCapacityLimits};
use super::log::{OperationLog, OperationLogError, StoredOperation};
use super::outbox_log::{OutboxLog, OutboxRow, ProducedBatch, ProducedSourceKind, ProjectedRecord};
use super::state::{ConversationAuthority, DurableAppend};
use super::tests::{dispatch, test_participant_config};
use crate::config::types::ParticipantConfig;

struct FixtureAppender<'a> {
    log: &'a OperationLog,
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

fn marker_fixture_facts(
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
        receipt_limits: ReceiptCapacityLimits {
            identity_server: config.max_retired_identity_slots_server,
            live_receipts_server: config.max_live_attach_receipts_server,
            live_receipts_per_participant: config.max_live_attach_receipts_per_participant,
            provenance_server: config.max_receipt_provenance_server,
            provenance_per_conversation: config.max_receipt_provenance_per_conversation,
            provenance_per_participant: config.max_receipt_provenance_per_participant,
        },
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

fn ack_members_through(
    handler: &ProductionParticipantHandler,
    conversation_id: u64,
    members: &FixtureMembers,
    generations: FixtureGenerations,
    through_seq: u64,
) -> Result<(), Box<dyn Error>> {
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
    Ok(())
}

fn ack_marker_prefix(
    handler: &ProductionParticipantHandler,
    conversation_id: u64,
    members: &FixtureMembers,
    generations: FixtureGenerations,
) -> Result<(), Box<dyn Error>> {
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
    ack_members_through(handler, conversation_id, members, generations, 3)?;

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
    ack_members_through(handler, conversation_id, members, generations, 4)
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
    ack_marker_prefix(
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
pub(super) struct AttachedDrainAttempt {
    /// POSITIVE CONTROL that the attaches actually landed. Part A established
    /// that without a `CredentialAttach` no participant ever holds a
    /// binding-fate token (`ops_attach.rs:331-337` is the sole mint), so the
    /// APPEARANCE of these tokens IS the attach observed. `false` here means
    /// the measurement never got the state it was built to test.
    pub(super) first_has_binding_fate: bool,
    pub(super) second_has_binding_fate: bool,
    /// A/B EVIDENCE for the AckGap's cause. My previous report attributed it to
    /// "attach resets the persisted cursor to zero", inferred from the doc
    /// comment at `wire/response.rs:653`. That was an inference, not a
    /// measurement. These are the members' cursors after enrolment-only and
    /// after enrol+attach, taken at the same point in the same sequence, so the
    /// reset is either shown or refuted instead of assumed.
    pub(super) cursors_enrolled_only: (u64, u64),
    pub(super) cursors_after_attach: (u64, u64),
    /// The DRAIN WITNESS, non-idempotent by construction: `drive_marker_drain`
    /// only returns `Ok` if `authority.last_marker_projection` surrendered a
    /// marker projection at the fourth commit — a one-shot value consumed by
    /// `.take()` that exists ONLY if a drain actually projected. It is an
    /// event, not a state that could legitimately sit still.
    pub(super) drain: Result<MarkerFixture, String>,
}

/// One arm of the AckGap discriminator (Cally 84dc0265 / 083f4a01). Every field
/// is read from the SERVER side using the same calls production makes, so the
/// two arms are compared on identical observables.
pub(super) struct AckGapArm {
    pub(super) arm: &'static str,
    /// OUTCOME (iv), the routing discriminator. This is the PRODUCTION
    /// PREDICATE COPIED VERBATIM from `ops_acks.rs:51-56`, not a paraphrase of
    /// it: `obligation_debt_dispatch().is_some_and(|s| s.episode().is_some())`.
    /// True routes to the nonzero arm (selector sites 2/3); false routes to the
    /// zero-debt arm (selector site 1). The two selectors implement DIFFERENT
    /// RULES, so a routing difference between arms is itself an answer.
    pub(super) routing_nonzero: bool,
    /// Site 1's deciding input side: `through_seq > contiguously_available_through`
    /// (`participant_ack.rs:221`). Both operands reported.
    pub(super) acknowledged_through: u64,
    pub(super) contiguously_available_through: Result<u64, String>,
    /// Site 2's evidence. `contains_endpoint` is `pub(in crate::lifecycle)` and
    /// therefore NOT callable from this crate, so the index itself is reported
    /// and membership is read from it rather than recomputed. Declared, not
    /// silently substituted.
    pub(super) obligations_debug: String,
    /// What the ack actually did, so the accepting arm proves it accepts.
    pub(super) ack_outcome: String,
}

pub(super) fn ackgap_arm_probe(
    attach: bool,
    through_seq: u64,
) -> Result<AckGapArm, Box<dyn Error>> {
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let config = marker_fixture_config();
    let conversation_id = 0xA7;
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), config)?;
    let members = enroll_members(&handler, conversation_id)?;

    let mut generation = Generation::ONE;
    if attach {
        let attached = dispatch(
            &handler,
            members.first_connection,
            ClientRequest::CredentialAttach(CredentialAttachRequest {
                conversation_id,
                participant_id: members.first.participant_id(),
                capability_generation: Generation::ONE,
                attach_secret: members.first.attach_secret(),
                attach_attempt_token: AttachAttemptToken::new([0xB2; 16]),
                accept_marker_delivery_seq: None,
            }),
        )?;
        let ServerValue::AttachBound(bound) = attached else {
            return Err(format!("ackgap probe: first member did not attach: {attached:?}").into());
        };
        generation = bound.origin_binding_epoch().capability_generation;
    }

    let participant_id = members.first.participant_id();
    let (routing_nonzero, acknowledged_through, contiguously_available_through, obligations_debug) = {
        let cell = handler.cell(conversation_id)?;
        let guard = cell
            .lock()
            .map_err(|_| "ackgap probe owner lock was poisoned")?;
        let authority = guard
            .as_ref()
            .ok_or("ackgap probe conversation owner was absent")?;
        // PRODUCTION PREDICATE, copied verbatim from ops_acks.rs:51-56.
        let routing = authority
            .obligation_debt_dispatch()
            .is_some_and(|state| state.episode().is_some());
        let acked = authority
            .slots
            .get(&participant_id)
            .map_or(u64::MAX, |slot| slot.member.cursor());
        let (available, obligations) = match authority.outbox.as_ref() {
            Some(outbox) => match outbox.recipient_ack_obligations(participant_id, acked) {
                Ok((obligations, available)) => {
                    (Ok(available), format!("{obligations:?}"))
                }
                Err(error) => (Err(format!("{error:?}")), "<unavailable>".to_owned()),
            },
            None => (
                Err("outbox absent".to_owned()),
                "<no outbox>".to_owned(),
            ),
        };
        let tuple = (routing, acked, available, obligations);
        drop(guard);
        tuple
    };

    let ack_outcome = match dispatch(
        &handler,
        members.first_connection,
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id,
            participant_id,
            capability_generation: generation,
            through_seq,
        }),
    ) {
        Ok(value) => format!("{value:?}"),
        Err(error) => format!("dispatch error: {error}"),
    };

    Ok(AckGapArm {
        arm: if attach { "POST-ATTACH (refusing)" } else { "PRE-ATTACH (accepting, control)" },
        routing_nonzero,
        acknowledged_through,
        contiguously_available_through,
        obligations_debug,
        ack_outcome,
    })
}

fn member_cursors(
    handler: &ProductionParticipantHandler,
    conversation_id: u64,
    members: &FixtureMembers,
) -> Result<(u64, u64), Box<dyn Error>> {
    let cell = handler.cell(conversation_id)?;
    let guard = cell
        .lock()
        .map_err(|_| "cursor probe owner lock was poisoned")?;
    let authority = guard
        .as_ref()
        .ok_or("cursor probe conversation owner was absent")?;
    let read = |participant_id: ParticipantId| {
        authority
            .slots
            .get(&participant_id)
            .map_or(u64::MAX, |slot| slot.member.cursor())
    };
    let pair = (
        read(members.first.participant_id()),
        read(members.second.participant_id()),
    );
    drop(guard);
    Ok(pair)
}

pub(super) fn attempt_marker_fixture_with_attaches() -> Result<AttachedDrainAttempt, Box<dyn Error>>
{
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let config = marker_fixture_config();
    let conversation_id = 0xA7;
    let handler = ProductionParticipantHandler::new(Arc::clone(&store), config)?;
    let members = enroll_members(&handler, conversation_id)?;

    // A/B CONTROL ARM: an identically-built server taken to the SAME point with
    // enrolment only and no attach, so the two cursor readings differ in exactly
    // one respect.
    let cursors_enrolled_only = {
        let control_store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
        let control_handler =
            ProductionParticipantHandler::new(Arc::clone(&control_store), marker_fixture_config())?;
        let control_members = enroll_members(&control_handler, conversation_id)?;
        member_cursors(&control_handler, conversation_id, &control_members)?
    };

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

    let cursors_after_attach = member_cursors(&handler, conversation_id, &members)?;
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
    let driven = ack_marker_prefix(&handler, conversation_id, &members, generations).and_then(
        |()| {
            drive_marker_drain(
                &handler,
                Arc::clone(&store),
                &config,
                conversation_id,
                &members,
                generations,
            )
        },
    );
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
        cursors_enrolled_only,
        cursors_after_attach,
        drain,
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
