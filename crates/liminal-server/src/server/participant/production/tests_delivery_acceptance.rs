use std::collections::VecDeque;
use std::error::Error;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;

use liminal::durability::{DurableStore, open_ephemeral};
use liminal::protocol::{encode, encoded_len};
use liminal_protocol::wire::{
    AttachAttemptToken, AttachSecret, ClientRequest, ConnectionIncarnation,
    CredentialAttachRequest, DetachAttemptToken, DetachRequest, EnrollmentRequest, EnrollmentToken,
    Generation, ParticipantAck, RecordAdmission, RecordAdmissionAttemptToken, ServerPush,
    ServerValue,
};

use crate::server::participant::{
    ParticipantOfferedProgress, ParticipantSemanticHandler, encode_server_push,
};

use super::ProductionParticipantHandler;
use super::tests::{dispatch, test_participant_config};

const CONVERSATION: u64 = 0xF0_C9_11;

#[derive(Clone, Copy)]
struct Member {
    connection: ConnectionIncarnation,
    participant_id: u64,
    generation: Generation,
    secret: AttachSecret,
}

fn enroll(
    handler: &ProductionParticipantHandler,
    connection: ConnectionIncarnation,
    token: u8,
) -> Result<Member, Box<dyn Error>> {
    let value = dispatch(
        handler,
        connection,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: CONVERSATION,
            enrollment_token: EnrollmentToken::new([token; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(bound) = value else {
        return Err(format!("enrollment {token:#x} did not bind: {value:?}").into());
    };
    Ok(Member {
        connection,
        participant_id: bound.participant_id(),
        generation: Generation::ONE,
        secret: bound.attach_secret(),
    })
}

fn attach(
    handler: &ProductionParticipantHandler,
    member: Member,
    connection: ConnectionIncarnation,
    token: u8,
) -> Result<Member, Box<dyn Error>> {
    let value = dispatch(
        handler,
        connection,
        ClientRequest::CredentialAttach(CredentialAttachRequest {
            conversation_id: CONVERSATION,
            participant_id: member.participant_id,
            capability_generation: member.generation,
            attach_secret: member.secret,
            attach_attempt_token: AttachAttemptToken::new([token; 16]),
            accept_marker_delivery_seq: None,
        }),
    )?;
    let ServerValue::AttachBound(bound) = value else {
        return Err(format!("attach {token:#x} did not bind: {value:?}").into());
    };
    Ok(Member {
        connection,
        participant_id: member.participant_id,
        generation: bound.capability_generation(),
        secret: bound.attach_secret(),
    })
}

fn detach(
    handler: &ProductionParticipantHandler,
    member: Member,
    token: u8,
) -> Result<(), Box<dyn Error>> {
    let value = dispatch(
        handler,
        member.connection,
        ClientRequest::Detach(DetachRequest {
            conversation_id: CONVERSATION,
            participant_id: member.participant_id,
            capability_generation: member.generation,
            detach_attempt_token: DetachAttemptToken::new([token; 16]),
        }),
    )?;
    if !matches!(value, ServerValue::DetachCommitted(_)) {
        return Err(format!("detach {token:#x} did not commit: {value:?}").into());
    }
    Ok(())
}

fn admit(
    handler: &ProductionParticipantHandler,
    sender: Member,
    token: u8,
) -> Result<u64, Box<dyn Error>> {
    let value = dispatch(
        handler,
        sender.connection,
        ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: CONVERSATION,
            participant_id: sender.participant_id,
            capability_generation: sender.generation,
            record_admission_attempt_token: RecordAdmissionAttemptToken::new([token; 16]),
            payload: vec![0xA5, token, 0x5A],
        }),
    )?;
    let ServerValue::RecordCommitted(committed) = value else {
        return Err(format!("record {token:#x} did not commit: {value:?}").into());
    };
    Ok(committed.delivery_seq())
}

fn ack(
    handler: &ProductionParticipantHandler,
    member: Member,
    through_seq: u64,
) -> Result<ServerValue, Box<dyn Error>> {
    dispatch(
        handler,
        member.connection,
        ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id: CONVERSATION,
            participant_id: member.participant_id,
            capability_generation: member.generation,
            through_seq,
        }),
    )
}

fn replay_sequences(
    handler: &ProductionParticipantHandler,
    member: Member,
) -> Result<Vec<u64>, Box<dyn Error>> {
    let mut offered = None;
    let mut sequences = Vec::new();
    loop {
        let Some(publication) =
            handler.next_publication(member.connection, CONVERSATION, offered)?
        else {
            break;
        };
        sequences.push(publication.delivery_seq());
        offered = Some(ParticipantOfferedProgress {
            binding_epoch: publication.binding_epoch,
            through_seq: publication.delivery_seq(),
        });
    }
    Ok(sequences)
}

fn socket_pair() -> Result<(TcpStream, TcpStream), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address: SocketAddr = listener.local_addr()?;
    let writer = TcpStream::connect(address)?;
    let (reader, _) = listener.accept()?;
    writer.set_nonblocking(true)?;
    Ok((writer, reader))
}

#[test]
fn socket_offer_and_write_never_reclaim() -> Result<(), Box<dyn Error>> {
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
    let recipient = enroll(&handler, ConnectionIncarnation::new(0xC9, 1), 0x11)?;
    let sender = enroll(&handler, ConnectionIncarnation::new(0xC9, 2), 0x12)?;
    assert!(matches!(
        ack(&handler, recipient, 2)?,
        ServerValue::AckCommitted(_)
    ));

    let expected = vec![
        admit(&handler, sender, 0x31)?,
        admit(&handler, sender, 0x32)?,
        admit(&handler, sender, 0x33)?,
    ];
    assert_eq!(expected, vec![3, 4, 5]);
    assert_eq!(replay_sequences(&handler, recipient)?, expected);

    let offered = handler
        .next_publication(recipient.connection, CONVERSATION, None)?
        .ok_or("durable recipient head disappeared before encode")?;
    let frame = encode_server_push(ServerPush::ParticipantDelivery(offered.delivery))
        .map_err(|error| format!("participant push encoding failed: {error:?}"))?;
    assert_eq!(replay_sequences(&handler, recipient)?, expected);

    let mut encoded = vec![0; encoded_len(&frame)?];
    let written = encode(&frame, &mut encoded)?;
    encoded.truncate(written);
    let mut outbound: VecDeque<_> = encoded.iter().copied().collect();
    assert_eq!(replay_sequences(&handler, recipient)?, expected);

    let (mut writer, mut reader) = socket_pair()?;
    let first = outbound
        .pop_front()
        .ok_or("encoded participant frame was unexpectedly empty")?;
    writer.write_all(&[first])?;
    assert!(
        !outbound.is_empty(),
        "partial write consumed the whole frame"
    );
    assert_eq!(replay_sequences(&handler, recipient)?, expected);

    writer.write_all(outbound.make_contiguous())?;
    outbound.clear();
    let mut received = vec![0; encoded.len()];
    reader.read_exact(&mut received)?;
    assert_eq!(received, encoded);
    assert_eq!(replay_sequences(&handler, recipient)?, expected);

    drop(reader);
    drop(writer);
    assert_eq!(replay_sequences(&handler, recipient)?, expected);

    let reattached = attach(
        &handler,
        recipient,
        ConnectionIncarnation::new(0xC9, 3),
        0x41,
    )?;
    assert_eq!(replay_sequences(&handler, reattached)?, expected);

    assert!(matches!(
        ack(&handler, reattached, 5)?,
        ServerValue::AckCommitted(_)
    ));
    assert!(replay_sequences(&handler, reattached)?.is_empty());
    Ok(())
}

#[test]
fn reattach_replays_unacked_in_order_after_acked_frontier() -> Result<(), Box<dyn Error>> {
    let store: Arc<dyn DurableStore> = Arc::new(open_ephemeral(1)?);
    let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
    let recipient = enroll(&handler, ConnectionIncarnation::new(0xC9, 11), 0x51)?;
    let sender = enroll(&handler, ConnectionIncarnation::new(0xC9, 12), 0x52)?;
    assert!(matches!(
        ack(&handler, recipient, 2)?,
        ServerValue::AckCommitted(_)
    ));

    let snapshot_included = vec![
        admit(&handler, sender, 0x61)?,
        admit(&handler, sender, 0x62)?,
        admit(&handler, sender, 0x63)?,
    ];
    assert_eq!(snapshot_included, vec![3, 4, 5]);

    detach(&handler, recipient, 0x64)?;
    let committed_while_detached = admit(&handler, sender, 0x65)?;
    assert_eq!(committed_while_detached, 7);

    // Under the B1 ruled contract, a publish accepted while the recipient is
    // connection-lost-Detached-but-resumable (its slot is still present) mints a
    // durable obligation for it, so the while-detached publish (seq 7) now
    // replays after the acked frontier alongside the earlier snapshot work.
    let mut expected_replay = snapshot_included;
    expected_replay.push(committed_while_detached);

    let first_reattach = attach(
        &handler,
        recipient,
        ConnectionIncarnation::new(0xC9, 13),
        0x66,
    )?;
    assert_eq!(
        replay_sequences(&handler, first_reattach)?,
        expected_replay,
        "obligations after the acked frontier — including the while-detached publish — replay in order"
    );

    let second_reattach = attach(
        &handler,
        first_reattach,
        ConnectionIncarnation::new(0xC9, 14),
        0x67,
    )?;
    assert_eq!(
        replay_sequences(&handler, second_reattach)?,
        expected_replay,
        "reattaching again before ack must duplicate the same ordered obligations"
    );
    assert!(
        replay_sequences(&handler, second_reattach)?.contains(&committed_while_detached),
        "a record committed while the recipient was resumable-Detached must replay on reattach (B1)"
    );
    Ok(())
}
