//! Regression: a client-initiated close must not drop responses the server wrote
//! inline in the same slice.
//!
//! A client pipelines `[Ping, Disconnect]` into one TCP segment. The server decodes
//! both in a single `process_buffer` pass: the `Ping` enqueues a `Pong` into the
//! outbound buffer, then the `Disconnect` returns `Close`. Before the fix, the
//! close stop path finished the connection without draining the outbound buffer, so
//! the `Pong` was silently lost — a regression from the buffered-writer refactor
//! (`ForceClose` kept a best-effort drain; the client-initiated close did not). This
//! test binds a real server and asserts the `Pong` still arrives over the wire.

use std::error::Error;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use liminal::protocol::{Frame, ProtocolError, ProtocolVersion, decode, encode, encoded_len};
use liminal_server::config::{ChannelDef, LimitsConfig, ServerConfig, ServicesConfig};
use liminal_server::server::connection::ConnectionSupervisor;
use liminal_server::server::listener::ServerListener;

const CHANNEL: &str = "events";
const CLIENT_VERSION: ProtocolVersion = ProtocolVersion::new(1, 0);

/// Holds the running listener so it stays bound for the lifetime of a test.
struct RunningServer {
    _listener: ServerListener,
    address: SocketAddr,
}

impl RunningServer {
    fn start() -> Result<Self, Box<dyn Error>> {
        let health = std::net::TcpListener::bind("127.0.0.1:0")?;
        let health_listen_address = health.local_addr()?;
        drop(health);
        let config = ServerConfig {
            listen_address: "127.0.0.1:0".parse()?,
            health_listen_address,
            channels: vec![ChannelDef {
                name: CHANNEL.to_owned(),
                schema_ref: None,
                durable: false,
                loaded_schema: None,
            }],
            routing_rules: Vec::new(),
            persistence_path: None,
            cluster: None,
            auth: None,
            drain_timeout_ms: 30_000,
            services: ServicesConfig::default(),
            limits: LimitsConfig::default(),
        };
        let supervisor = ConnectionSupervisor::from_config(&config)?;
        let listener = ServerListener::bind(&config, supervisor)?;
        let address = listener.local_addr();
        Ok(Self {
            _listener: listener,
            address,
        })
    }
}

/// Encodes one frame into a byte vector.
fn encode_frame(frame: &Frame) -> Result<Vec<u8>, Box<dyn Error>> {
    let len = encoded_len(frame)?;
    let mut bytes = vec![0_u8; len];
    let written = encode(frame, &mut bytes)?;
    bytes.truncate(written);
    Ok(bytes)
}

/// Blocks reading `socket` until one complete frame decodes, returning it.
fn read_one_frame(socket: &mut TcpStream, buffer: &mut Vec<u8>) -> Result<Frame, Box<dyn Error>> {
    loop {
        match decode(buffer) {
            Ok((frame, consumed)) => {
                buffer.drain(..consumed);
                return Ok(frame);
            }
            Err(
                ProtocolError::IncompleteHeader { .. } | ProtocolError::TruncatedPayload { .. },
            ) => {
                let mut chunk = [0_u8; 512];
                let read = socket.read(&mut chunk)?;
                if read == 0 {
                    return Err("server closed before a full frame arrived".into());
                }
                buffer.extend_from_slice(chunk.get(..read).unwrap_or(&[]));
            }
            Err(error) => return Err(Box::new(error)),
        }
    }
}

#[test]
fn pipelined_ping_then_disconnect_still_gets_the_pong() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start()?;
    let mut socket = TcpStream::connect(server.address)?;
    socket.set_read_timeout(Some(Duration::from_secs(5)))?;
    socket.set_write_timeout(Some(Duration::from_secs(5)))?;
    let mut buffer = Vec::new();

    // Complete the handshake so the pipelined [Ping, Disconnect] below is the only
    // thing left in flight.
    socket.write_all(&encode_frame(&Frame::Connect {
        flags: 0,
        min_version: CLIENT_VERSION,
        max_version: CLIENT_VERSION,
        auth_token: Vec::new(),
    })?)?;
    socket.flush()?;
    assert!(
        matches!(
            read_one_frame(&mut socket, &mut buffer)?,
            Frame::ConnectAck { .. }
        ),
        "handshake must be acknowledged"
    );

    // Pipeline Ping and Disconnect into ONE segment: the server enqueues the Pong
    // for the Ping, then the Disconnect returns Close in the same slice.
    let mut segment = encode_frame(&Frame::Ping { flags: 0 })?;
    segment.extend_from_slice(&encode_frame(&Frame::Disconnect { flags: 0 })?);
    socket.write_all(&segment)?;
    socket.flush()?;

    // The Pong must still arrive: the close stop path drains the outbound buffer
    // before finishing the connection. Without the fix this read reaches EOF first.
    match read_one_frame(&mut socket, &mut buffer)? {
        Frame::Pong { .. } => Ok(()),
        other => Err(format!(
            "expected a Pong before the close, got {:?}",
            other.frame_type()
        )
        .into()),
    }
}
