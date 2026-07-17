use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use liminal::protocol::{
    Frame, ProtocolVersion, decode as decode_generic, encode as encode_generic,
    encoded_len as generic_len,
};
use liminal_protocol::wire::{
    ParticipantFrame, ReceiverDirection, ServerValue, decode as decode_participant,
    encode as encode_participant, encoded_len as participant_len,
};

use crate::{ConnectionPoolConfig, SdkError};

use super::super::super::{ParticipantResumeStore, RemoteConfig};

pub(super) enum Action {
    Respond(Vec<ServerValue>),
    DropAfterRequest,
}

pub(super) struct Loopback {
    address: String,
    task: JoinHandle<io::Result<()>>,
}

impl Loopback {
    pub(super) fn spawn(sessions: Vec<Vec<Action>>) -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?.to_string();
        let task = thread::spawn(move || {
            for actions in sessions {
                let (mut stream, _) = listener.accept()?;
                handshake(&mut stream)?;
                for action in actions {
                    let request = read_generic(&mut stream)?;
                    ensure_participant_request(request)?;
                    match action {
                        Action::Respond(values) => {
                            for value in values {
                                let frame = ParticipantFrame::ServerValue(value);
                                write_participant(&mut stream, &frame)?;
                            }
                        }
                        Action::DropAfterRequest => break,
                    }
                }
            }
            Ok(())
        });
        Ok(Self { address, task })
    }

    pub(super) fn connected_config(&self) -> Result<RemoteConfig, SdkError> {
        RemoteConfig::new(
            self.address.clone(),
            "participant-tests",
            "participant-tests",
            ConnectionPoolConfig::new(1, 4, 16),
        )?
        .connect_tcp()
    }

    pub(super) fn finish(self) -> io::Result<()> {
        self.task
            .join()
            .unwrap_or_else(|_| Err(io::Error::other("loopback server thread panicked")))
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct MemoryStore {
    bytes: Arc<Mutex<Vec<u8>>>,
}

impl MemoryStore {
    pub(super) fn bytes(&self) -> io::Result<Vec<u8>> {
        self.bytes
            .lock()
            .map(|bytes| bytes.clone())
            .map_err(|_| io::Error::other("memory resume store lock poisoned"))
    }
}

impl ParticipantResumeStore for MemoryStore {
    fn persist(&mut self, canonical_lpcr: &[u8]) -> Result<(), SdkError> {
        let mut bytes = self.bytes.lock().map_err(|_| SdkError::Store {
            description: "memory resume store lock poisoned".to_string(),
        })?;
        bytes.clear();
        bytes.extend_from_slice(canonical_lpcr);
        drop(bytes);
        Ok(())
    }
}

fn handshake(stream: &mut TcpStream) -> io::Result<()> {
    match read_generic(stream)? {
        Frame::Connect { .. } => write_generic(
            stream,
            &Frame::ConnectAck {
                flags: 0,
                selected_version: ProtocolVersion::new(1, 0),
                capabilities: 1,
            },
        ),
        _ => Err(io::Error::other("loopback expected Connect handshake")),
    }
}

fn ensure_participant_request(frame: Frame) -> io::Result<()> {
    let complete = unknown_complete(frame)?;
    match decode_participant(&complete, ReceiverDirection::Server) {
        Ok(ParticipantFrame::ClientRequest(_)) => Ok(()),
        Ok(ParticipantFrame::ServerValue(_) | ParticipantFrame::ServerPush(_)) => Err(
            io::Error::other("loopback received a server-direction participant frame"),
        ),
        Err(error) => Err(io::Error::other(format!(
            "participant request decode failed: {error:?}"
        ))),
    }
}

fn unknown_complete(frame: Frame) -> io::Result<Vec<u8>> {
    let Frame::Unknown {
        type_id,
        flags,
        stream_id,
        payload,
    } = frame
    else {
        return Err(io::Error::other(
            "loopback expected generic unknown participant frame",
        ));
    };
    let payload_length = u32::try_from(payload.len())
        .map_err(|_| io::Error::other("participant payload length overflow"))?;
    let mut complete = Vec::with_capacity(10 + payload.len());
    complete.push(type_id);
    complete.push(flags);
    complete.extend_from_slice(&stream_id.to_be_bytes());
    complete.extend_from_slice(&payload_length.to_be_bytes());
    complete.extend_from_slice(&payload);
    Ok(complete)
}

fn read_generic(stream: &mut TcpStream) -> io::Result<Frame> {
    let mut header = [0_u8; 10];
    stream.read_exact(&mut header)?;
    let payload_length = u32::from_be_bytes([header[6], header[7], header[8], header[9]]);
    let payload_length = usize::try_from(payload_length)
        .map_err(|_| io::Error::other("generic payload length does not fit usize"))?;
    let mut complete = Vec::with_capacity(10 + payload_length);
    complete.extend_from_slice(&header);
    complete.resize(10 + payload_length, 0);
    stream.read_exact(&mut complete[10..])?;
    decode_generic(&complete)
        .map(|(frame, _)| frame)
        .map_err(|error| io::Error::other(format!("generic decode failed: {error}")))
}

fn write_generic(stream: &mut TcpStream, frame: &Frame) -> io::Result<()> {
    let needed = generic_len(frame)
        .map_err(|error| io::Error::other(format!("generic length failed: {error}")))?;
    let mut bytes = vec![0_u8; needed];
    let written = encode_generic(frame, &mut bytes)
        .map_err(|error| io::Error::other(format!("generic encode failed: {error}")))?;
    stream.write_all(&bytes[..written])?;
    stream.flush()
}

fn write_participant(stream: &mut TcpStream, frame: &ParticipantFrame) -> io::Result<()> {
    let needed = participant_len(frame)
        .map_err(|error| io::Error::other(format!("participant length failed: {error:?}")))?;
    let mut bytes = vec![0_u8; needed];
    let written = encode_participant(frame, &mut bytes)
        .map_err(|error| io::Error::other(format!("participant encode failed: {error:?}")))?;
    stream.write_all(&bytes[..written])?;
    stream.flush()
}
