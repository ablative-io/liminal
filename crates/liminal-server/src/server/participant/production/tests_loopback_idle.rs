//! The loopback idle-cost pin, keepalive-honest
//! (`docs/design/IN-PROCESS-TRANSPORT.md` §6 "Idle-cost pin", estate
//! no-silent-tradeoffs law).
//!
//! **The claim.** A parked in-process connection — connected, enrolled, then
//! left alone — schedules NO wakeups and services NO slices, however busy the
//! rest of the server is. The loopback is TOLD, never polls: a connection with
//! nothing to read parks on its ring and only the peer's write re-arms it
//! (design §3, "Wake — the NO-POLLING answer").
//!
//! **Why the flat reading is honest here.** A flat counter on its own is
//! consistent with a frozen scheduler, a dead process, or a fixture that never
//! wired the counter up — three ways to publish a meaningless green. So the pin
//! carries its own control: a SECOND in-process connection does real work in a
//! DIFFERENT conversation for the whole of the measured window, and its slice
//! counter must GROW across exactly that window. The two readings are taken over
//! the same interval, so "the parked connection stayed at zero" is only reported
//! alongside proof that the scheduler was running, that in-process connections
//! do consume slices when they have work, and that this fixture's counter moves.
//!
//! **Why slices are the right instrument for "zero wakeups".** The loopback has
//! no descriptor and arms no readiness; its only wake path is the duplex writer
//! enqueueing the peer connection's READY atom, and a READY atom is what makes
//! the scheduler service a slice. A wake that ran no slice would be a wake that
//! did nothing; a slice with no wake is the busy-poll this design retired. So a
//! flat slice count across a busy window IS the zero-wakeup reading, and the
//! marker below additionally proves not one slice was recorded — not merely that
//! the count returned to where it started.

use std::error::Error;
use std::io::Write;
use std::sync::mpsc::RecvTimeoutError;
use std::time::{Duration, Instant};

use liminal::protocol::{Frame, ProtocolError, ProtocolVersion, decode as decode_generic};
use liminal_protocol::wire::{
    ClientRequest, EnrollmentRequest, EnrollmentToken, Generation, PARTICIPANT_FRAME_TYPE,
    ParticipantFrame, ReceiverDirection, RecordAdmission, RecordAdmissionAttemptToken, ServerValue,
    decode as decode_participant,
};

use crate::server::connection::LoopbackClientEnd;
use crate::server::participant::PARTICIPANT_CAPABILITY_BIT;

use super::{SocketFixture, encode_frame, encode_request};

/// The conversation the parked connection enrolls in.
const PARKED_CONVERSATION: u64 = 0x50_06;
/// The conversation the busy connection works in.
///
/// Deliberately NOT [`PARKED_CONVERSATION`]: a participant enrolled in the same
/// conversation would hold a genuine delivery obligation for the busy
/// connection's records, so its wakes would be correct behaviour and the pin
/// would be measuring nothing. The parked participant must be a party with
/// nothing owed to it.
const BUSY_CONVERSATION: u64 = 0x50_07;

/// Ring size for the pin's duplexes: comfortably larger than any frame it
/// exchanges, so this never accidentally becomes a backpressure pin.
const PIN_RING_BYTES: usize = 64 * 1024;

/// Failure deadline for every bounded wait here. Reaching it is a FAILURE, never
/// a settling delay.
const PIN_DEADLINE: Duration = Duration::from_secs(5);

/// How long the parked connection is watched while the busy one works.
///
/// Long enough that a connection polling its ring at any plausible interval
/// would record many slices, short enough to keep the pin fast.
const IDLE_WINDOW: Duration = Duration::from_millis(200);

/// Reads one complete generic frame off a loopback client end, parking on the
/// duplex's own condvar rather than sampling.
fn read_frame(
    client: &mut LoopbackClientEnd,
    buffer: &mut Vec<u8>,
) -> Result<Frame, Box<dyn Error>> {
    let deadline = Instant::now() + PIN_DEADLINE;
    loop {
        match decode_generic(buffer) {
            Ok((frame, consumed)) => {
                buffer.drain(..consumed);
                return Ok(frame);
            }
            Err(
                ProtocolError::IncompleteHeader { .. } | ProtocolError::TruncatedPayload { .. },
            ) => {}
            Err(error) => return Err(Box::new(error)),
        }
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or("the loopback idle pin timed out waiting for a frame")?;
        let mut chunk = [0_u8; 4096];
        let read = client.read_timeout(&mut chunk, Some(remaining))?;
        if read == 0 {
            return Err("the loopback connection reached end of file".into());
        }
        buffer.extend_from_slice(chunk.get(..read).unwrap_or(&[]));
    }
}

/// Sends `Connect` over a fresh loopback end and requires the participant
/// capability back, so every pin below starts from a genuinely admitted,
/// participant-capable in-process connection.
fn handshake(client: &mut LoopbackClientEnd, buffer: &mut Vec<u8>) -> Result<(), Box<dyn Error>> {
    client.write_all(&encode_frame(&Frame::Connect {
        flags: 0,
        min_version: ProtocolVersion::new(1, 0),
        max_version: ProtocolVersion::new(1, 0),
        auth_token: Vec::new(),
    })?)?;
    let ack = read_frame(client, buffer)?;
    if !matches!(
        ack,
        Frame::ConnectAck { capabilities, .. } if capabilities == PARTICIPANT_CAPABILITY_BIT
    ) {
        return Err(
            format!("the in-process connection was not participant-capable: {ack:?}").into(),
        );
    }
    Ok(())
}

/// Sends one participant request over a loopback end and returns its response.
fn request(
    client: &mut LoopbackClientEnd,
    buffer: &mut Vec<u8>,
    request: ClientRequest,
) -> Result<ServerValue, Box<dyn Error>> {
    client.write_all(&encode_request(request)?)?;
    loop {
        let frame = read_frame(client, buffer)?;
        let Frame::Unknown {
            type_id: PARTICIPANT_FRAME_TYPE,
            ..
        } = frame
        else {
            return Err(format!("expected a participant frame, got {frame:?}").into());
        };
        let bytes = encode_frame(&frame)?;
        match decode_participant(&bytes, ReceiverDirection::Client)
            .map_err(|error| format!("{error:?}"))?
        {
            ParticipantFrame::ServerValue(value) => return Ok(value),
            ParticipantFrame::ServerPush(_) => {}
            ParticipantFrame::ClientRequest(unexpected) => {
                return Err(
                    format!("a client received a ClientRequest frame: {unexpected:?}").into(),
                );
            }
        }
    }
}

#[test]
fn a_parked_loopback_connection_costs_no_slices_while_another_one_works()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let server = SocketFixture::start(&home.path().join("loopback-idle"))?;

    // ---- the unit under measurement: one in-process connection, parked ----
    let (mut parked, parked_connection) = server.spawn_loopback(PIN_RING_BYTES)?;
    let parked_pid = parked_connection.pid();
    let mut parked_buffer = Vec::new();
    handshake(&mut parked, &mut parked_buffer)?;

    // The park marker is installed BEFORE the request that will cause the park,
    // because it reports the NEXT park — arming it afterwards could miss the
    // event and turn the wait into a sleep.
    let park_marker = server.observe_next_park(parked_pid);
    let enrolled = request(
        &mut parked,
        &mut parked_buffer,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: PARKED_CONVERSATION,
            enrollment_token: EnrollmentToken::new([0x60; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(bound) = enrolled else {
        return Err(format!("the parked connection did not enroll: {enrolled:?}").into());
    };
    assert_eq!(bound.capability_generation(), Generation::ONE);

    // A genuine park, not a guess: the process reported its final-probe park and
    // then settled, and the settled count is the slice count at that instant.
    park_marker
        .recv_timeout(PIN_DEADLINE)
        .map_err(|error| format!("the parked connection never reported its park: {error}"))?;
    let parked_at = server
        .observe_settled_park(parked_pid)
        .recv_timeout(PIN_DEADLINE)
        .map_err(|error| format!("the parked connection never settled after its park: {error}"))?;
    assert_eq!(
        server.slice_count(parked_pid),
        parked_at,
        "the settled park count must be the parked connection's slice count"
    );

    // Armed across the whole measured window. Unlike a count comparison, this
    // catches a slice that ran and then somehow left the counter where it was.
    let unexpected_slice = server.observe_next_slice(parked_pid);

    // ---- the control: a second in-process connection doing real work ----
    let (mut busy, busy_connection) = server.spawn_loopback(PIN_RING_BYTES)?;
    let busy_pid = busy_connection.pid();
    let mut busy_buffer = Vec::new();
    handshake(&mut busy, &mut busy_buffer)?;
    let busy_slices_before = server.slice_count(busy_pid);

    let busy_enrolled = request(
        &mut busy,
        &mut busy_buffer,
        ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: BUSY_CONVERSATION,
            enrollment_token: EnrollmentToken::new([0x61; 16]),
        }),
    )?;
    let ServerValue::EnrollBound(busy_bound) = busy_enrolled else {
        return Err(format!("the busy connection did not enroll: {busy_enrolled:?}").into());
    };

    // Real committed records, not pings: the busy connection drives the same
    // production handler and the same durable append the parked one would drive
    // if it had anything to do.
    let deadline = Instant::now() + IDLE_WINDOW;
    let mut committed = 0_u32;
    while Instant::now() < deadline {
        let outcome = request(
            &mut busy,
            &mut busy_buffer,
            ClientRequest::RecordAdmission(RecordAdmission {
                conversation_id: BUSY_CONVERSATION,
                participant_id: busy_bound.participant_id(),
                capability_generation: Generation::ONE,
                record_admission_attempt_token: RecordAdmissionAttemptToken::new(
                    [u8::try_from(committed % 251).unwrap_or(0); 16],
                ),
                payload: vec![0xB5; 32],
            }),
        )?;
        if !matches!(outcome, ServerValue::RecordCommitted(_)) {
            return Err(format!("the busy connection's record did not commit: {outcome:?}").into());
        }
        committed = committed.saturating_add(1);
    }
    assert!(
        committed > 0,
        "the control committed no records, so the window it is supposed to make busy \
         was empty and the flat reading below would prove nothing"
    );

    // ---- the two readings, over the same window ----
    let busy_slices_after = server.slice_count(busy_pid);
    assert!(
        busy_slices_after > busy_slices_before,
        "the unrelated in-process connection's slice count did not grow \
         ({busy_slices_before} -> {busy_slices_after}) across a window in which it \
         committed {committed} records; the scheduler was not running, so the parked \
         reading below would be meaningless"
    );

    assert!(
        matches!(
            unexpected_slice.recv_timeout(Duration::from_millis(100)),
            Err(RecvTimeoutError::Timeout)
        ),
        "the parked in-process connection serviced a slice with nothing to read — it \
         polled its ring instead of waiting to be told"
    );
    assert_eq!(
        server.slice_count(parked_pid),
        parked_at,
        "the parked in-process connection's slice count moved from {parked_at} while a \
         second in-process connection committed {committed} records in another \
         conversation; a parked loopback must cost nothing"
    );

    drop(parked);
    drop(busy);
    drop(parked_connection);
    drop(busy_connection);
    server.stop();
    Ok(())
}
