//! Terminal participant-response delivery through the real process close path.

use std::error::Error;
use std::io::Read;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use beamr::process::ExitReason;
use liminal::protocol::{Frame, decode as decode_generic};
use liminal_protocol::wire::{
    ClientRequest, EnrollmentRequest, EnrollmentToken, ParticipantFrame, ReceiverDirection,
    ServerValue, TransportRejectionReason, decode as decode_participant,
    encode as encode_participant, encoded_len as participant_encoded_len,
};

use super::{ConnectionProcess, ProcessStatus, SliceStep, process_buffer};
use crate::server::connection::supervisor::ConnectionRuntime;
use crate::server::connection::worker_front_door::WorkerFrontDoorServices;

fn tcp_pair() -> Result<(TcpStream, TcpStream), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address: SocketAddr = listener.local_addr()?;
    let client = TcpStream::connect(address)?;
    let (server, _) = listener.accept()?;
    Ok((client, server))
}

fn participant_request_bytes() -> Result<Vec<u8>, String> {
    let frame = ParticipantFrame::ClientRequest(ClientRequest::Enrollment(EnrollmentRequest {
        conversation_id: 41,
        enrollment_token: EnrollmentToken::new([7; 16]),
    }));
    let mut bytes = vec![0; participant_encoded_len(&frame).map_err(|error| format!("{error:?}"))?];
    let written = encode_participant(&frame, &mut bytes).map_err(|error| format!("{error:?}"))?;
    bytes.truncate(written);
    Ok(bytes)
}

#[test]
fn terminal_participant_rejection_drains_after_more_than_one_slice_budget()
-> Result<(), Box<dyn Error>> {
    let (mut client, server) = tcp_pair()?;
    client.set_read_timeout(Some(Duration::from_secs(5)))?;
    server.set_nonblocking(true)?;

    let runtime = Arc::new(ConnectionRuntime::for_tests(Arc::new(
        WorkerFrontDoorServices::new(),
    )));
    let holder = Arc::new(Mutex::new(Some(server)));
    let mut process = ConnectionProcess::from_holder(runtime, None, &holder, None);
    process.state.authenticated = true;

    // This complete frame alone exceeds the ordinary 8 KiB per-slice drain
    // budget. The terminal rejection queued behind it must still be written by
    // the one-shot unbudgeted close drain.
    let leading_payload = vec![0xA5; 9_000];
    process.outbound.enqueue_frame(&Frame::Push {
        flags: 0,
        stream_id: 1,
        correlation_id: 77,
        payload: leading_payload.clone(),
    })?;
    process.buffer = participant_request_bytes()?;
    let runtime = Arc::clone(&process.runtime);
    let status = process_buffer(
        1,
        &runtime,
        &mut process.state,
        &mut process.buffer,
        &mut process.outbound,
    )?;
    if status != ProcessStatus::Close {
        return Err("terminal participant rejection did not select close".into());
    }
    if !matches!(
        process.finish_normal_close(1),
        SliceStep::Stop(ExitReason::Normal)
    ) {
        return Err("terminal participant rejection did not close the process".into());
    }
    drop(process);

    let mut received = Vec::new();
    client.read_to_end(&mut received)?;
    let (leading, leading_bytes) = decode_generic(&received)?;
    assert!(matches!(
        leading,
        Frame::Push {
            correlation_id: 77,
            payload,
            ..
        } if payload == leading_payload
    ));
    let terminal_bytes = received
        .get(leading_bytes..)
        .ok_or("leading frame consumed beyond received bytes")?;
    let (_terminal_generic, terminal_consumed) = decode_generic(terminal_bytes)?;
    assert_eq!(terminal_consumed, terminal_bytes.len());
    let terminal = decode_participant(terminal_bytes, ReceiverDirection::Client)
        .map_err(|error| format!("{error:?}"))?;
    assert!(matches!(
        terminal,
        ParticipantFrame::ServerValue(ServerValue::ParticipantTransportRejected(rejection))
            if rejection.reason == TransportRejectionReason::ParticipantCapabilityRequired
    ));
    Ok(())
}
