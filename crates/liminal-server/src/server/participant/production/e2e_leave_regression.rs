//! Real-socket regression for Leave behind supersession-created marker work.

use std::error::Error;
use std::path::Path;

use liminal_protocol::algebra::ResourceDimension;
use liminal_protocol::wire::{
    AttachAttemptToken, AttachBound, ClientRequest, ClosureRefusalReason, CredentialAttachRequest,
    DetachAttemptToken, DetachRequest, EnrollBound, EnrollmentRequest, EnrollmentToken, Generation,
    LeaveAttemptToken, LeaveRequest, ParticipantFrame, ParticipantId, ParticipantRecord,
    RecordAdmission, RecordAdmissionAttemptToken, RecordCommitted, ServerPush, ServerValue,
    encoded_len,
};

use super::e2e_cold_all_shapes_fixture::{
    BoundClosePath, decoded_history_from_store, expected_bound_close_fate,
    semantic_rows_and_typed_fate_suffix,
};
use super::e2e_tests::{SocketFixture, SocketPeer};
use super::tests::test_participant_config;
use super::tests_marker_ack_fixture::marker_fixture_config;
use super::tests_outbox_barrier_fixture::OutboxBarrierKind;
use super::tests_outbox_log::measured_fixed_outbox_overhead;

pub(super) const CONVERSATION: u64 = 527;

pub(super) fn enroll_three(
    primary: &mut SocketFixture,
    peer: &mut SocketPeer,
    leaver: &mut SocketPeer,
) -> Result<(EnrollBound, EnrollBound, EnrollBound), Box<dyn Error>> {
    let alpha = primary.request(ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: CONVERSATION,
        enrollment_token: EnrollmentToken::new([0xA1; 16]),
    }))?;
    let ServerValue::EnrollBound(alpha) = alpha else {
        return Err(format!("participant A did not enroll: {alpha:?}").into());
    };
    let bravo = peer.request(ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: CONVERSATION,
        enrollment_token: EnrollmentToken::new([0xB1; 16]),
    }))?;
    let ServerValue::EnrollBound(bravo) = bravo else {
        return Err(format!("participant B did not enroll: {bravo:?}").into());
    };
    let charlie = leaver.request(ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: CONVERSATION,
        enrollment_token: EnrollmentToken::new([0xC1; 16]),
    }))?;
    let ServerValue::EnrollBound(charlie) = charlie else {
        return Err(format!("participant C did not enroll: {charlie:?}").into());
    };
    Ok((alpha, bravo, charlie))
}

fn rotate_leaver(
    primary: &SocketFixture,
    original: &mut SocketPeer,
    leaver: &EnrollBound,
) -> Result<(SocketPeer, AttachBound), Box<dyn Error>> {
    let detached = original.request(ClientRequest::Detach(DetachRequest {
        conversation_id: CONVERSATION,
        participant_id: leaver.participant_id(),
        capability_generation: Generation::ONE,
        detach_attempt_token: DetachAttemptToken::new([0xC2; 16]),
    }))?;
    assert!(matches!(detached, ServerValue::DetachCommitted(_)));

    let mut reconnect = primary.spawn_peer()?;
    let ordinary = reconnect.request(ClientRequest::CredentialAttach(CredentialAttachRequest {
        conversation_id: CONVERSATION,
        participant_id: leaver.participant_id(),
        capability_generation: Generation::ONE,
        attach_secret: leaver.attach_secret(),
        attach_attempt_token: AttachAttemptToken::new([0xC3; 16]),
        accept_marker_delivery_seq: None,
    }))?;
    let ServerValue::AttachBound(ordinary) = ordinary else {
        return Err(format!("participant C ordinary reattach failed: {ordinary:?}").into());
    };

    let mut replacement = primary.spawn_peer()?;
    let superseding =
        replacement.request(ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: CONVERSATION,
            participant_id: leaver.participant_id(),
            capability_generation: ordinary.capability_generation(),
            attach_secret: ordinary.attach_secret(),
            attach_attempt_token: AttachAttemptToken::new([0xC4; 16]),
            accept_marker_delivery_seq: None,
        }))?;
    let ServerValue::AttachBound(superseding) = superseding else {
        return Err(format!("participant C superseding attach failed: {superseding:?}").into());
    };
    drop(reconnect);
    Ok((replacement, superseding))
}

fn commit_and_deliver_record(
    primary: &mut SocketFixture,
    recipient: &mut SocketPeer,
    sender: ParticipantId,
) -> Result<RecordCommitted, Box<dyn Error>> {
    let outcome = primary.request(ClientRequest::RecordAdmission(RecordAdmission {
        conversation_id: CONVERSATION,
        participant_id: sender,
        capability_generation: Generation::ONE,
        record_admission_attempt_token: RecordAdmissionAttemptToken::new([0xA2; 16]),
        payload: vec![0xD3],
    }))?;
    let ServerValue::RecordCommitted(committed) = outcome else {
        return Err(format!("participant A's ordinary record did not commit: {outcome:?}").into());
    };

    primary.open_publication_replay()?;
    let mut wake_peer = primary.spawn_peer()?;
    let wake = wake_peer.request(ClientRequest::RecordAdmission(RecordAdmission {
        conversation_id: CONVERSATION,
        participant_id: u64::MAX,
        capability_generation: Generation::ONE,
        record_admission_attempt_token: RecordAdmissionAttemptToken::new([0xD1; 16]),
        payload: Vec::new(),
    }))?;
    assert!(matches!(wake, ServerValue::ParticipantUnknown(_)));
    let ServerPush::ParticipantDelivery(delivery) = recipient.read_push()? else {
        return Err("participant C did not receive the ordinary record".into());
    };
    assert_eq!(delivery.delivery_seq, committed.delivery_seq());
    assert!(matches!(
        delivery.record,
        ParticipantRecord::OrdinaryRecord { sender_participant_id, ref payload }
            if sender_participant_id == sender && payload == &[0xD3]
    ));
    Ok(committed)
}

fn reopened_fixture(data_dir: &Path) -> Result<SocketFixture, Box<dyn Error>> {
    SocketFixture::start_replay_gated_with_barriers(data_dir, marker_fixture_config())
}

#[test]
fn leave_after_detach_reattach_supersession_discharges_unacked_obligation_and_reopens()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let mut primary = reopened_fixture(&data_dir)?;
    let mut peer = primary.spawn_peer()?;
    let mut original = primary.spawn_peer()?;
    let (sender, observer, leaver) = enroll_three(&mut primary, &mut peer, &mut original)?;
    let (mut replacement, binding) = rotate_leaver(&primary, &mut original, &leaver)?;
    let record =
        commit_and_deliver_record(&mut primary, &mut replacement, sender.participant_id())?;

    let before = primary.outbox_owner_facts(CONVERSATION, leaver.participant_id())?;
    assert_eq!(before.next_live_obligation, Some(record.delivery_seq()));
    assert_eq!(primary.immutable_candidate_counts(CONVERSATION)?, (3, 1));
    let leave = LeaveRequest {
        conversation_id: CONVERSATION,
        participant_id: leaver.participant_id(),
        capability_generation: binding.capability_generation(),
        attach_secret: binding.attach_secret(),
        leave_attempt_token: LeaveAttemptToken::new([0xC5; 16]),
    };
    let outcome = replacement.request(ClientRequest::Leave(leave.clone()))?;
    let ServerValue::LeaveCommitted(committed) = outcome else {
        return Err(format!("participant C Leave did not commit: {outcome:?}").into());
    };
    let after = primary.outbox_owner_facts(CONVERSATION, leaver.participant_id())?;
    assert_eq!(after.next_live_obligation, None);
    let ids = [
        sender.participant_id(),
        observer.participant_id(),
        leaver.participant_id(),
    ];
    let semantic_durable = [
        primary.outbox_owner_facts(CONVERSATION, ids[0])?,
        primary.outbox_owner_facts(CONVERSATION, ids[1])?,
        after,
    ];
    let (base_before_teardown, _) =
        decoded_history_from_store(primary.durable_store(), CONVERSATION)?;
    let (semantic_rows, pre_teardown_fate_suffix) =
        semantic_rows_and_typed_fate_suffix(base_before_teardown)?;
    assert!(pre_teardown_fate_suffix.is_empty());

    // Derive §10.1's exact suffix from this test's observed Bound receipts and
    // the two classified close paths, then serialize those paths for an ordered
    // census after the final semantic Left row.
    let expected_fate_suffix = [
        expected_bound_close_fate(
            observer.participant_id(),
            observer.origin_binding_epoch(),
            BoundClosePath::DroppedSocket,
        ),
        expected_bound_close_fate(
            sender.participant_id(),
            sender.origin_binding_epoch(),
            BoundClosePath::ServerStop,
        ),
    ];
    primary.arm_outbox_barriers([OutboxBarrierKind::OperationFlush])?;
    peer.shutdown_transport()?;
    primary.wait_for_outbox_barrier(OutboxBarrierKind::OperationFlush)?;
    primary.release_outbox_barrier(OutboxBarrierKind::OperationFlush)?;
    drop(peer);
    drop(original);
    drop(replacement);
    primary.force_close_and_wait();

    let durable = [
        primary.outbox_owner_facts(CONVERSATION, ids[0])?,
        primary.outbox_owner_facts(CONVERSATION, ids[1])?,
        primary.outbox_owner_facts(CONVERSATION, ids[2])?,
    ];
    for (before, after) in semantic_durable.iter().zip(&durable) {
        assert_eq!(after.ack_through, before.ack_through);
        assert_eq!(after.next_live_obligation, before.next_live_obligation);
    }
    let (base_after_teardown, _) =
        decoded_history_from_store(primary.durable_store(), CONVERSATION)?;
    let (teardown_semantic_rows, fate_suffix) =
        semantic_rows_and_typed_fate_suffix(base_after_teardown)?;
    assert_eq!(teardown_semantic_rows, semantic_rows);
    assert_eq!(fate_suffix, expected_fate_suffix);

    primary.stop();
    let mut reopened = reopened_fixture(&data_dir)?;
    for (participant_id, expected) in ids.into_iter().zip(durable) {
        assert_eq!(
            reopened.outbox_owner_facts(CONVERSATION, participant_id)?,
            expected
        );
    }
    assert_eq!(
        reopened.request(ClientRequest::Leave(leave))?,
        ServerValue::LeaveCommitted(committed)
    );
    reopened.stop();
    Ok(())
}

fn fill_item29_cycle(
    sender_socket: &mut SocketFixture,
    sender: &EnrollBound,
    recipient: &EnrollBound,
    payload_len: usize,
    retained_capacity_bytes: u64,
    signed_outbox_bound: u64,
    admission_attempt: &mut u8,
) -> Result<(u64, bool, u64), Box<dyn Error>> {
    let mut committed_in_cycle = 0_u64;
    let mut closure_refused = false;
    let mut maximum_live_outbox_bytes = 0_u64;
    loop {
        *admission_attempt = admission_attempt
            .checked_add(1)
            .ok_or("item 29 attempt counter overflowed")?;
        let outcome = sender_socket.request(ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: CONVERSATION + 1,
            participant_id: sender.participant_id(),
            capability_generation: Generation::ONE,
            record_admission_attempt_token: RecordAdmissionAttemptToken::new(
                [*admission_attempt; 16],
            ),
            payload: vec![0xD3; payload_len],
        }))?;
        let facts =
            sender_socket.outbox_owner_facts(CONVERSATION + 1, recipient.participant_id())?;
        maximum_live_outbox_bytes = maximum_live_outbox_bytes.max(facts.charged_bytes);
        assert!(
            facts.charged_bytes <= signed_outbox_bound,
            "item 29 live outbox {} exceeded signed bound {signed_outbox_bound}",
            facts.charged_bytes
        );
        match outcome {
            ServerValue::RecordCommitted(_) => {
                committed_in_cycle = committed_in_cycle
                    .checked_add(1)
                    .ok_or("item 29 commit counter overflowed")?;
            }
            ServerValue::ObserverBackpressure(
                liminal_protocol::wire::ObserverBackpressure::RecordAdmission { state, .. },
            ) => {
                assert_eq!(state.backpressure_epoch(), state.observer_progress());
                break;
            }
            ServerValue::MarkerClosureCapacityExceeded(refusal) => {
                let ClosureRefusalReason::Capacity(reason) = refusal.reason else {
                    return Err(format!(
                        "item 29 closure edge was not typed capacity refusal: {refusal:?}"
                    )
                    .into());
                };
                assert_eq!(reason.dimension, ResourceDimension::Bytes);
                assert_eq!(reason.limit, u128::from(retained_capacity_bytes));
                closure_refused = true;
                break;
            }
            other => {
                return Err(format!(
                    "item 29 admission attempt {admission_attempt} returned {other:?}"
                )
                .into());
            }
        }
    }
    Ok((
        committed_in_cycle,
        closure_refused,
        maximum_live_outbox_bytes,
    ))
}

fn run_item29_turnover_cycle(
    sender_socket: &mut SocketFixture,
    sender: &EnrollBound,
    cycle: u8,
    payload_len: usize,
    retained_capacity_bytes: u64,
    signed_outbox_bound: u64,
    admission_attempt: &mut u8,
) -> Result<u64, Box<dyn Error>> {
    let mut recipient_socket = sender_socket.spawn_peer()?;
    let recipient = recipient_socket.request(ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: CONVERSATION + 1,
        enrollment_token: EnrollmentToken::new([0x20 + cycle; 16]),
    }))?;
    let ServerValue::EnrollBound(recipient) = recipient else {
        return Err(format!("item 29 recipient {cycle} did not enroll: {recipient:?}").into());
    };
    let (committed, closure_refused, maximum) = fill_item29_cycle(
        sender_socket,
        sender,
        &recipient,
        payload_len,
        retained_capacity_bytes,
        signed_outbox_bound,
        admission_attempt,
    )?;
    assert_eq!(
        (committed, closure_refused),
        if cycle < 2 { (7, false) } else { (1, true) }
    );

    let before_leave =
        sender_socket.outbox_owner_facts(CONVERSATION + 1, recipient.participant_id())?;
    assert!(before_leave.next_live_obligation.is_some());
    let left = recipient_socket.request(ClientRequest::Leave(LeaveRequest {
        conversation_id: CONVERSATION + 1,
        participant_id: recipient.participant_id(),
        capability_generation: Generation::ONE,
        attach_secret: recipient.attach_secret(),
        leave_attempt_token: LeaveAttemptToken::new([0x30 + cycle; 16]),
    }))?;
    assert!(matches!(left, ServerValue::LeaveCommitted(_)));
    let after_leave =
        sender_socket.outbox_owner_facts(CONVERSATION + 1, recipient.participant_id())?;
    assert_eq!(after_leave.next_live_obligation, None);
    assert!(after_leave.charged_bytes < before_leave.charged_bytes);
    assert!(after_leave.source_batch_count > before_leave.source_batch_count);
    assert!(after_leave.charged_bytes <= signed_outbox_bound);
    Ok(maximum)
}

#[test]
fn leave_discharges_the_left_identitys_obligations_and_bounds_live_payload()
-> Result<(), Box<dyn Error>> {
    const RETAINED_BYTES_PER_IDENTITY_SLOT: u64 = 131_072;

    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let mut config = test_participant_config();
    config.retained_capacity_bytes = RETAINED_BYTES_PER_IDENTITY_SLOT
        .checked_mul(config.identity_slots)
        .ok_or("item 29 retained-capacity fixture overflowed")?;
    let (maximum_fixed_per_record, fixed_outbox_overhead) =
        measured_fixed_outbox_overhead(&config)?;
    assert_eq!(
        fixed_outbox_overhead,
        maximum_fixed_per_record
            .checked_mul(config.max_retained_record_rows)
            .ok_or("item 29 fixed outbox metadata term overflowed")?
    );
    let signed_outbox_bound = config
        .retained_capacity_bytes
        .checked_add(fixed_outbox_overhead)
        .ok_or("item 29 signed outbox bound overflowed")?;
    let fixed_request_bytes = encoded_len(&ParticipantFrame::ClientRequest(
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: CONVERSATION + 1,
            participant_id: u64::MAX,
            capability_generation: Generation::ONE,
            record_admission_attempt_token: RecordAdmissionAttemptToken::new([u8::MAX; 16]),
            payload: Vec::new(),
        }),
    ))
    .map_err(|error| format!("item 29 request codec failed: {error:?}"))?;
    let payload_len = usize::try_from(config.wire_frame_limit)?
        .checked_sub(fixed_request_bytes)
        .ok_or("wire frame cannot contain an item 29 RecordAdmission")?;
    assert_eq!(payload_len, 65_476);

    let conversation_id = CONVERSATION + 1;
    let mut sender_socket = SocketFixture::start_replay_gated_with_config(&data_dir, config)?;
    let sender = sender_socket.request(ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id,
        enrollment_token: EnrollmentToken::new([0x10; 16]),
    }))?;
    let ServerValue::EnrollBound(sender) = sender else {
        return Err(format!("item 29 sender did not enroll: {sender:?}").into());
    };

    let mut maximum_live_outbox_bytes = 0_u64;
    let mut admission_attempt = 0_u8;
    for cycle in 0..3_u8 {
        let cycle_maximum = run_item29_turnover_cycle(
            &mut sender_socket,
            &sender,
            cycle,
            payload_len,
            config.retained_capacity_bytes,
            signed_outbox_bound,
            &mut admission_attempt,
        )?;
        maximum_live_outbox_bytes = maximum_live_outbox_bytes.max(cycle_maximum);
    }

    assert_eq!(maximum_live_outbox_bytes, 458_985);
    assert!(maximum_live_outbox_bytes <= signed_outbox_bound);
    println!(
        "MEASURED_ITEM29_PAYLOAD_BYTES={payload_len} MAXIMUM_LIVE_OUTBOX_BYTES={maximum_live_outbox_bytes} FIXED_OUTBOX_OVERHEAD_BYTES={fixed_outbox_overhead} SIGNED_OUTBOX_BOUND_BYTES={signed_outbox_bound}"
    );
    sender_socket.stop();
    Ok(())
}
