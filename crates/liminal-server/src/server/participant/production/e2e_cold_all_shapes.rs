//! Full mapped-shape real-socket history across a same-directory cold reopen.

use std::error::Error;

use liminal_protocol::wire::{
    AttachAttemptToken, ClientRequest, CredentialAttachRequest, DetachAttemptToken, DetachRequest,
    EnrollmentRequest, EnrollmentToken, Generation, LeaveAttemptToken, LeaveRequest, MarkerAck,
    ParticipantRecord, RecordAdmission, RecordAdmissionAttemptToken, ServerPush, ServerValue,
};

use super::e2e_cold_all_shapes_fixture::{
    ALL_SHAPES_CONVERSATION, ColdMember, MARKER_INTERLEAVING_CONVERSATION, ack_through,
    assert_decoded_source_census, decoded_history, expect_enrolled, expected_live_deliveries,
};
use super::e2e_tests::SocketFixture;
use super::log::StoredOperation;
use super::outbox_log::OutboxRow;
use super::tests_marker_ack_fixture::marker_fixture_config;

#[test]
fn cold_reopen_reconciles_and_replays_all_record_shapes() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let config = marker_fixture_config();
    let mut first = SocketFixture::start_replay_gated_with_config(&data_dir, config)?;
    let mut second = first.spawn_peer()?;
    let mut observer = first.spawn_peer()?;
    let mut transient = first.spawn_peer()?;

    let primary = ColdMember::enrolled(&expect_enrolled(
        first.request(ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: ALL_SHAPES_CONVERSATION,
            enrollment_token: EnrollmentToken::new([0x24; 16]),
        }))?,
        "primary",
    )?);
    let mut actor = ColdMember::enrolled(&expect_enrolled(
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
        &mut first,
        ALL_SHAPES_CONVERSATION,
        primary,
        actor_left.left_delivery_seq(),
    )?;
    assert!(record.delivery_seq() < actor_left.left_delivery_seq());

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
    ack_through(&mut first, MARKER_INTERLEAVING_CONVERSATION, marker_a, 3)?;
    ack_through(&mut second, MARKER_INTERLEAVING_CONVERSATION, marker_b, 3)?;
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
        &mut first,
        MARKER_INTERLEAVING_CONVERSATION,
        marker_a,
        marker_c_left.left_delivery_seq(),
    )?;
    ack_through(
        &mut second,
        MARKER_INTERLEAVING_CONVERSATION,
        marker_b,
        marker_c_left.left_delivery_seq(),
    )?;
    let mut latest_record = 0_u64;
    for token in [0x35, 0x36, 0x37, 0x38] {
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
                &mut second,
                MARKER_INTERLEAVING_CONVERSATION,
                marker_b,
                latest_record,
            )?;
        }
    }
    first.open_publication_replay()?;
    let mut marker_wake = first.spawn_peer()?;
    let _ = marker_wake.request(ClientRequest::RecordAdmission(RecordAdmission {
        conversation_id: MARKER_INTERLEAVING_CONVERSATION,
        participant_id: u64::MAX,
        capability_generation: Generation::ONE,
        record_admission_attempt_token: RecordAdmissionAttemptToken::new([0x3C; 16]),
        payload: Vec::new(),
    }))?;
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
        &mut second,
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

    drop(ordinary_attach);
    drop(superseding);
    drop(second);
    drop(observer);
    drop(transient);
    drop(marker_wake);
    first.stop();

    let (main_base, main_extension) = decoded_history(&data_dir, ALL_SHAPES_CONVERSATION)?;
    let (marker_base, marker_extension) =
        decoded_history(&data_dir, MARKER_INTERLEAVING_CONVERSATION)?;
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
        StoredOperation::Attached { allocation, .. }
            if allocation.superseded_terminal_seq.is_none()
    )));
    assert!(main_base.iter().any(|(_, row)| matches!(
        row,
        StoredOperation::Attached { allocation, .. }
            if allocation.superseded_terminal_seq.is_some()
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

    let expected_main = expected_live_deliveries(
        ALL_SHAPES_CONVERSATION,
        replay_recipient.participant_id,
        &main_extension,
    );
    let expected_marker = expected_live_deliveries(
        MARKER_INTERLEAVING_CONVERSATION,
        marker_a.participant_id,
        &marker_extension,
    );
    assert!(!expected_main.is_empty());
    assert!(!expected_marker.is_empty());

    let mut reopened = SocketFixture::start_replay_gated_with_config(&data_dir, config)?;
    let main_attach =
        reopened.request(ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: ALL_SHAPES_CONVERSATION,
            participant_id: replay_recipient.participant_id,
            capability_generation: replay_recipient.generation,
            attach_secret: replay_recipient.secret,
            attach_attempt_token: AttachAttemptToken::new([0x3A; 16]),
            accept_marker_delivery_seq: None,
        }))?;
    assert!(matches!(main_attach, ServerValue::AttachBound(_)));
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
    assert!(matches!(marker_attach, ServerValue::AttachBound(_)));
    assert!(
        reopened.blocked_publication_scans()? > 0,
        "cold reconciliation did not finish behind the publication gate"
    );
    reopened.open_publication_replay()?;
    let mut replay_wake = reopened.spawn_peer()?;
    let _ = replay_wake.request(ClientRequest::RecordAdmission(RecordAdmission {
        conversation_id: ALL_SHAPES_CONVERSATION,
        participant_id: u64::MAX,
        capability_generation: Generation::ONE,
        record_admission_attempt_token: RecordAdmissionAttemptToken::new([0x3D; 16]),
        payload: Vec::new(),
    }))?;
    for (index, expected) in expected_main.into_iter().enumerate() {
        let observed = reopened.read_push().map_err(|error| {
            format!(
                "main cold replay stopped before decoded obligation {index} at sequence {}: {error}",
                expected.delivery_seq
            )
        })?;
        assert_eq!(observed, ServerPush::ParticipantDelivery(expected));
    }
    let _ = marker_replay.request(ClientRequest::RecordAdmission(RecordAdmission {
        conversation_id: MARKER_INTERLEAVING_CONVERSATION,
        participant_id: u64::MAX,
        capability_generation: Generation::ONE,
        record_admission_attempt_token: RecordAdmissionAttemptToken::new([0x3E; 16]),
        payload: Vec::new(),
    }))?;
    for (index, expected) in expected_marker.into_iter().enumerate() {
        let observed = marker_replay.read_push().map_err(|error| {
            format!(
                "marker cold replay stopped before decoded obligation {index} at sequence {}: {error}",
                expected.delivery_seq
            )
        })?;
        assert_eq!(observed, ServerPush::ParticipantDelivery(expected));
    }
    drop(marker_replay);
    drop(replay_wake);
    reopened.stop();
    Ok(())
}
