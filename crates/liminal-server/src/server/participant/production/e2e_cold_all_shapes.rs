//! Full mapped-shape real-socket history across a same-directory cold reopen.

use std::error::Error;
use std::path::Path;

use liminal_protocol::wire::{
    AttachAttemptToken, ClientRequest, CredentialAttachRequest, DetachAttemptToken, DetachRequest,
    EnrollmentRequest, EnrollmentToken, Generation, LeaveAttemptToken, LeaveRequest, MarkerAck,
    ParticipantDelivery, ParticipantRecord, RecordAdmission, RecordAdmissionAttemptToken,
    ServerPush, ServerValue,
};

use crate::config::types::ParticipantConfig;

use super::e2e_cold_all_shapes_fixture::{
    ALL_SHAPES_CONVERSATION, ColdMember, MARKER_INTERLEAVING_CONVERSATION, ack_through,
    assert_decoded_source_census, decoded_history, expect_enrolled, expected_live_deliveries,
};
use super::e2e_tests::{SocketFixture, SocketPeer};
use super::log::{StoredAttachModeV3, StoredOperation};
use super::outbox_log::OutboxRow;
use super::tests_marker_ack_fixture::marker_fixture_config;
use super::tests_outbox_barrier_fixture::OutboxBarrierKind;

struct ExpectedDeliveries {
    main: Vec<ParticipantDelivery>,
    marker: Vec<ParticipantDelivery>,
}

fn enroll_main_members(
    first: &mut SocketFixture,
    second: &mut SocketPeer,
    observer: &mut SocketPeer,
    transient: &mut SocketPeer,
) -> Result<(ColdMember, ColdMember, ColdMember), Box<dyn Error>> {
    let primary = ColdMember::enrolled(&expect_enrolled(
        first.request(ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: ALL_SHAPES_CONVERSATION,
            enrollment_token: EnrollmentToken::new([0x24; 16]),
        }))?,
        "primary",
    )?);
    let actor = ColdMember::enrolled(&expect_enrolled(
        second.request(ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: ALL_SHAPES_CONVERSATION,
            enrollment_token: EnrollmentToken::new([0x25; 16]),
        }))?,
        "actor",
    )?);
    let replay_recipient = ColdMember::enrolled(&expect_enrolled(
        observer.request(ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: ALL_SHAPES_CONVERSATION,
            enrollment_token: EnrollmentToken::new([0x26; 16]),
        }))?,
        "replay recipient",
    )?);
    let retired = ColdMember::enrolled(&expect_enrolled(
        transient.request(ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: ALL_SHAPES_CONVERSATION,
            enrollment_token: EnrollmentToken::new([0x27; 16]),
        }))?,
        "transient",
    )?);
    let left = transient.request(ClientRequest::Leave(LeaveRequest {
        conversation_id: ALL_SHAPES_CONVERSATION,
        participant_id: retired.participant_id,
        capability_generation: retired.generation,
        attach_secret: retired.secret,
        leave_attempt_token: LeaveAttemptToken::new([0x28; 16]),
    }))?;
    assert!(matches!(left, ServerValue::LeaveCommitted(_)));
    Ok((primary, actor, replay_recipient))
}

fn commit_main_shapes(
    first: &mut SocketFixture,
    second: &mut SocketPeer,
    primary: ColdMember,
    mut actor: ColdMember,
) -> Result<(), Box<dyn Error>> {
    assert!(matches!(
        second.request(ClientRequest::Detach(DetachRequest {
            conversation_id: ALL_SHAPES_CONVERSATION,
            participant_id: actor.participant_id,
            capability_generation: actor.generation,
            detach_attempt_token: DetachAttemptToken::new([0x29; 16]),
        }))?,
        ServerValue::DetachCommitted(_)
    ));
    let mut ordinary_attach = first.spawn_peer()?;
    let attached =
        ordinary_attach.request(ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: ALL_SHAPES_CONVERSATION,
            participant_id: actor.participant_id,
            capability_generation: actor.generation,
            attach_secret: actor.secret,
            attach_attempt_token: AttachAttemptToken::new([0x2A; 16]),
            accept_marker_delivery_seq: None,
        }))?;
    let ServerValue::AttachBound(attached) = attached else {
        return Err(format!("ordinary attach did not bind: {attached:?}").into());
    };
    actor.generation = attached.capability_generation();
    actor.secret = attached.attach_secret();
    let mut superseding = first.spawn_peer()?;
    let superseded =
        superseding.request(ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: ALL_SHAPES_CONVERSATION,
            participant_id: actor.participant_id,
            capability_generation: actor.generation,
            attach_secret: actor.secret,
            attach_attempt_token: AttachAttemptToken::new([0x2B; 16]),
            accept_marker_delivery_seq: None,
        }))?;
    let ServerValue::AttachBound(superseded) = superseded else {
        return Err(format!("superseding attach did not bind: {superseded:?}").into());
    };
    actor.generation = superseded.capability_generation();
    actor.secret = superseded.attach_secret();
    let record = first.request(ClientRequest::RecordAdmission(RecordAdmission {
        conversation_id: ALL_SHAPES_CONVERSATION,
        participant_id: primary.participant_id,
        capability_generation: primary.generation,
        record_admission_attempt_token: RecordAdmissionAttemptToken::new([0x2C; 16]),
        payload: vec![0xA5],
    }))?;
    let ServerValue::RecordCommitted(record) = record else {
        return Err(format!("all-shapes record did not commit: {record:?}").into());
    };
    let actor_left = superseding.request(ClientRequest::Leave(LeaveRequest {
        conversation_id: ALL_SHAPES_CONVERSATION,
        participant_id: actor.participant_id,
        capability_generation: actor.generation,
        attach_secret: actor.secret,
        leave_attempt_token: LeaveAttemptToken::new([0x2D; 16]),
    }))?;
    let ServerValue::LeaveCommitted(actor_left) = actor_left else {
        return Err(format!("all-shapes Leave did not commit: {actor_left:?}").into());
    };
    ack_through(
        first,
        ALL_SHAPES_CONVERSATION,
        primary,
        actor_left.left_delivery_seq(),
    )?;
    assert!(record.delivery_seq() < actor_left.left_delivery_seq());
    Ok(())
}

fn fill_marker_history(
    first: &mut SocketFixture,
    second: &mut SocketPeer,
    transient: &mut SocketPeer,
) -> Result<(ColdMember, ColdMember, u64), Box<dyn Error>> {
    let marker_a = ColdMember::enrolled(&expect_enrolled(
        first.request(ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: MARKER_INTERLEAVING_CONVERSATION,
            enrollment_token: EnrollmentToken::new([0x31; 16]),
        }))?,
        "marker A",
    )?);
    let marker_b = ColdMember::enrolled(&expect_enrolled(
        second.request(ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: MARKER_INTERLEAVING_CONVERSATION,
            enrollment_token: EnrollmentToken::new([0x32; 16]),
        }))?,
        "marker B",
    )?);
    let marker_c = ColdMember::enrolled(&expect_enrolled(
        transient.request(ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: MARKER_INTERLEAVING_CONVERSATION,
            enrollment_token: EnrollmentToken::new([0x33; 16]),
        }))?,
        "marker C",
    )?);
    ack_through(first, MARKER_INTERLEAVING_CONVERSATION, marker_a, 3)?;
    ack_through(second, MARKER_INTERLEAVING_CONVERSATION, marker_b, 3)?;
    let marker_c_left = transient.request(ClientRequest::Leave(LeaveRequest {
        conversation_id: MARKER_INTERLEAVING_CONVERSATION,
        participant_id: marker_c.participant_id,
        capability_generation: marker_c.generation,
        attach_secret: marker_c.secret,
        leave_attempt_token: LeaveAttemptToken::new([0x34; 16]),
    }))?;
    let ServerValue::LeaveCommitted(marker_c_left) = marker_c_left else {
        return Err(format!("marker C Leave did not commit: {marker_c_left:?}").into());
    };
    ack_through(
        first,
        MARKER_INTERLEAVING_CONVERSATION,
        marker_a,
        marker_c_left.left_delivery_seq(),
    )?;
    ack_through(
        second,
        MARKER_INTERLEAVING_CONVERSATION,
        marker_b,
        marker_c_left.left_delivery_seq(),
    )?;
    let mut latest_record = 0_u64;
    for token in [0x35, 0x36, 0x37, 0x38] {
        if token == 0x38 {
            first.open_publication_replay()?;
        }
        let outcome = first.request(ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: MARKER_INTERLEAVING_CONVERSATION,
            participant_id: marker_a.participant_id,
            capability_generation: marker_a.generation,
            record_admission_attempt_token: RecordAdmissionAttemptToken::new([token; 16]),
            payload: vec![token],
        }))?;
        let ServerValue::RecordCommitted(committed) = outcome else {
            return Err(format!("marker-driving record {token:#x} failed: {outcome:?}").into());
        };
        latest_record = committed.delivery_seq();
        if token != 0x38 {
            ack_through(
                second,
                MARKER_INTERLEAVING_CONVERSATION,
                marker_b,
                latest_record,
            )?;
        }
    }
    Ok((marker_a, marker_b, latest_record))
}

fn finish_marker_history(
    first: &mut SocketFixture,
    second: &mut SocketPeer,
    marker_a: ColdMember,
    marker_b: ColdMember,
    latest_record: u64,
) -> Result<(), Box<dyn Error>> {
    let ServerPush::ParticipantDelivery(marker_on_a) = first.read_push()? else {
        return Err("marker A did not receive the generated marker".into());
    };
    let ServerPush::ParticipantDelivery(marker_on_b) = second.read_push()? else {
        return Err("marker B did not receive the generated marker".into());
    };
    assert_eq!(marker_on_a, marker_on_b);
    let ServerPush::ParticipantDelivery(last_on_b) = second.read_push()? else {
        return Err("marker B did not receive the post-marker ordinary record".into());
    };
    assert_eq!(last_on_b.delivery_seq, latest_record);
    let ParticipantRecord::HistoryCompacted {
        affected_participant_id,
        ..
    } = marker_on_a.record
    else {
        return Err(format!("generated delivery was not a marker: {marker_on_a:?}").into());
    };
    let marker_ack = MarkerAck {
        conversation_id: MARKER_INTERLEAVING_CONVERSATION,
        participant_id: affected_participant_id,
        capability_generation: Generation::ONE,
        marker_delivery_seq: marker_on_a.delivery_seq,
    };
    let marker_ack_outcome = if affected_participant_id == marker_a.participant_id {
        first.request(ClientRequest::MarkerAck(marker_ack))?
    } else if affected_participant_id == marker_b.participant_id {
        second.request(ClientRequest::MarkerAck(marker_ack))?
    } else {
        return Err("generated marker targeted neither live fixture member".into());
    };
    assert!(matches!(
        marker_ack_outcome,
        ServerValue::MarkerAckCommitted(_)
    ));
    ack_through(
        second,
        MARKER_INTERLEAVING_CONVERSATION,
        marker_b,
        latest_record,
    )?;
    first.block_publication_replay()?;
    let post_marker = second.request(ClientRequest::RecordAdmission(RecordAdmission {
        conversation_id: MARKER_INTERLEAVING_CONVERSATION,
        participant_id: marker_b.participant_id,
        capability_generation: marker_b.generation,
        record_admission_attempt_token: RecordAdmissionAttemptToken::new([0x39; 16]),
        payload: vec![0xFF],
    }))?;
    assert!(matches!(post_marker, ServerValue::RecordCommitted(_)));
    Ok(())
}

fn decoded_expectations(
    data_dir: &Path,
    replay_recipient: ColdMember,
    marker_a: ColdMember,
) -> Result<ExpectedDeliveries, Box<dyn Error>> {
    let (main_base, main_extension) = decoded_history(data_dir, ALL_SHAPES_CONVERSATION)?;
    let (marker_base, marker_extension) =
        decoded_history(data_dir, MARKER_INTERLEAVING_CONVERSATION)?;
    assert_decoded_source_census(&main_base, &main_extension)?;
    assert_decoded_source_census(&marker_base, &marker_extension)?;
    assert!(main_base.iter().any(|(sequence, row)| {
        *sequence == 0 && matches!(row, StoredOperation::Genesis { .. })
    }));
    assert!(main_base.iter().any(|(sequence, row)| {
        *sequence == 1 && matches!(row, StoredOperation::Enrolled { .. })
    }));
    assert!(main_base.iter().any(|(_, row)| matches!(
        row,
        StoredOperation::Attached { mode, .. }
            if matches!(mode.as_ref(), StoredAttachModeV3::Ordinary)
    )));
    assert!(main_base.iter().any(|(_, row)| matches!(
        row,
        StoredOperation::Attached { mode, .. }
            if matches!(mode.as_ref(), StoredAttachModeV3::Superseding { .. })
    )));
    assert!(
        marker_base
            .iter()
            .any(|(_, row)| matches!(row, StoredOperation::MarkerDrained { .. }))
    );
    let marker_ack_position = marker_extension
        .iter()
        .position(|(_, row)| matches!(row, OutboxRow::MarkerAckCommitted(_)))
        .ok_or("decoded MarkerAckCommitted interleaving was absent")?;
    assert!(
        marker_extension[..marker_ack_position]
            .iter()
            .any(|(_, row)| matches!(row, OutboxRow::Produced(_)))
    );
    assert!(
        marker_extension[marker_ack_position + 1..]
            .iter()
            .any(|(_, row)| matches!(row, OutboxRow::Produced(_)))
    );
    let main = expected_live_deliveries(
        ALL_SHAPES_CONVERSATION,
        replay_recipient.participant_id,
        &main_extension,
    );
    let marker = expected_live_deliveries(
        MARKER_INTERLEAVING_CONVERSATION,
        marker_a.participant_id,
        &marker_extension,
    );
    assert!(!main.is_empty());
    assert!(!marker.is_empty());
    Ok(ExpectedDeliveries { main, marker })
}

fn replay_expected_deliveries(
    data_dir: &Path,
    config: ParticipantConfig,
    replay_recipient: ColdMember,
    marker_a: ColdMember,
    expected: ExpectedDeliveries,
) -> Result<(), Box<dyn Error>> {
    let mut reopened = SocketFixture::start_replay_gated_with_config(data_dir, config)?;
    let first_main_attach =
        reopened.request(ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: ALL_SHAPES_CONVERSATION,
            participant_id: replay_recipient.participant_id,
            capability_generation: replay_recipient.generation,
            attach_secret: replay_recipient.secret,
            attach_attempt_token: AttachAttemptToken::new([0x3A; 16]),
            accept_marker_delivery_seq: None,
        }))?;
    let ServerValue::AttachBound(first_main_attach) = first_main_attach else {
        return Err(format!("initial main cold attach did not bind: {first_main_attach:?}").into());
    };
    assert!(reopened.blocked_publication_scans()? > 0);
    reopened.open_publication_replay()?;
    let mut main_replay = reopened.spawn_peer()?;
    let main_rebind =
        main_replay.request(ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: ALL_SHAPES_CONVERSATION,
            participant_id: replay_recipient.participant_id,
            capability_generation: first_main_attach.capability_generation(),
            attach_secret: first_main_attach.attach_secret(),
            attach_attempt_token: AttachAttemptToken::new([0x3C; 16]),
            accept_marker_delivery_seq: None,
        }))?;
    if !matches!(main_rebind, ServerValue::AttachBound(_)) {
        return Err(format!("committed main cold rebind did not bind: {main_rebind:?}").into());
    }
    let mut marker_replay = reopened.spawn_peer()?;
    let marker_attach =
        marker_replay.request(ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: MARKER_INTERLEAVING_CONVERSATION,
            participant_id: marker_a.participant_id,
            capability_generation: marker_a.generation,
            attach_secret: marker_a.secret,
            attach_attempt_token: AttachAttemptToken::new([0x3B; 16]),
            accept_marker_delivery_seq: None,
        }))?;
    if !matches!(marker_attach, ServerValue::AttachBound(_)) {
        return Err(format!("committed marker cold bind did not bind: {marker_attach:?}").into());
    }
    for (index, expected_delivery) in expected.main.into_iter().enumerate() {
        let delivered = main_replay.read_push().map_err(|error| {
            format!(
                "main cold replay stopped before decoded obligation {index} at sequence {}: {error}",
                expected_delivery.delivery_seq
            )
        })?;
        assert_eq!(
            delivered,
            ServerPush::ParticipantDelivery(expected_delivery)
        );
    }
    for (index, expected_delivery) in expected.marker.into_iter().enumerate() {
        let delivered = marker_replay.read_push().map_err(|error| {
            format!(
                "marker cold replay stopped before decoded obligation {index} at sequence {}: {error}",
                expected_delivery.delivery_seq
            )
        })?;
        assert_eq!(
            delivered,
            ServerPush::ParticipantDelivery(expected_delivery)
        );
    }
    reopened.stop();
    Ok(())
}

fn synchronize_dropped_connection_fold(
    server: &SocketFixture,
    peer: &SocketPeer,
    conversation_id: u64,
    participant_id: u64,
) -> Result<(), Box<dyn Error>> {
    server.arm_outbox_barriers([OutboxBarrierKind::OperationFlush])?;
    peer.shutdown_transport()?;
    server.wait_for_outbox_barrier(OutboxBarrierKind::OperationFlush)?;
    server.release_outbox_barrier(OutboxBarrierKind::OperationFlush)?;
    server
        .outbox_owner_facts(conversation_id, participant_id)
        .map(|_| ())
}

fn synchronize_server_stop_folds(
    server: &SocketFixture,
    folds: &[(u64, u64)],
) -> Result<(), Box<dyn Error>> {
    server.arm_outbox_barriers(folds.iter().map(|_| OutboxBarrierKind::OperationFlush))?;
    server.request_force_close();
    for &(conversation_id, participant_id) in folds {
        server.wait_for_outbox_barrier(OutboxBarrierKind::OperationFlush)?;
        server.release_outbox_barrier(OutboxBarrierKind::OperationFlush)?;
        server.outbox_owner_facts(conversation_id, participant_id)?;
    }
    server.force_close_and_wait();
    Ok(())
}

#[test]
pub(super) fn cold_reopen_reconciles_and_replays_all_record_shapes() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let config = marker_fixture_config();
    let mut first = SocketFixture::start_replay_gated_with_barriers(&data_dir, config)?;
    let mut second = first.spawn_peer()?;
    let mut observer = first.spawn_peer()?;
    let mut transient = first.spawn_peer()?;

    let (primary, actor, replay_recipient) =
        enroll_main_members(&mut first, &mut second, &mut observer, &mut transient)?;
    commit_main_shapes(&mut first, &mut second, primary, actor)?;
    let (marker_a, marker_b, latest_record) =
        fill_marker_history(&mut first, &mut second, &mut transient)?;
    finish_marker_history(&mut first, &mut second, marker_a, marker_b, latest_record)?;

    synchronize_dropped_connection_fold(
        &first,
        &second,
        MARKER_INTERLEAVING_CONVERSATION,
        marker_b.participant_id,
    )?;
    synchronize_dropped_connection_fold(
        &first,
        &observer,
        ALL_SHAPES_CONVERSATION,
        replay_recipient.participant_id,
    )?;
    drop(second);
    drop(observer);
    drop(transient);
    synchronize_server_stop_folds(
        &first,
        &[
            (ALL_SHAPES_CONVERSATION, primary.participant_id),
            (MARKER_INTERLEAVING_CONVERSATION, marker_a.participant_id),
        ],
    )?;
    first.stop();

    let expected = decoded_expectations(&data_dir, replay_recipient, marker_a)?;
    replay_expected_deliveries(&data_dir, config, replay_recipient, marker_a, expected)
}
