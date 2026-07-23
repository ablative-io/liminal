//! End-to-end flush pins against a REAL server (SDK-PUSH-FLUSH §4).
//!
//! Finding (ii) of the design: a schema-0 validation rejection was invisible —
//! the server has always written a real `Frame::PublishError` (blanket
//! `reason_code = 0xFFFF`, the schema-mismatch text in `message`), but the SDK
//! reader discarded it, so a fire-and-forget publisher never saw the
//! rejection. These pins prove the 0.4.0 `flush()`/`close()` surface captures
//! that wire truth end-to-end with NO server change:
//!
//! * an invalid-JSON publish on a schema-bearing channel (every configured
//!   channel applies at least the permissive any-valid-JSON schema) surfaces
//!   through `flush()` as a raw `{0xFFFF, "invalid JSON payload: …"}`
//!   rejection, in wire order, with the surrounding valid publishes proven
//!   accepted;
//! * `close()` = flush-then-graceful-half-close, returning the clean
//!   proven-accepted shape for an all-valid burst and disclosing
//!   `FlushedAndHalfClosed` as sole socket owner.
//!
//! The storeless embedding mirrors `teardown_delivery_e2e.rs` (TCP listener
//! only — no WebSocket leg is involved in the push path).

use std::error::Error;
use std::sync::Arc;

use liminal_sdk::remote::{FlushMode, PushClient};
use liminal_server::config::{ChannelDef, LimitsConfig, ServerConfig, ServicesConfig};
use liminal_server::server::connection::LiminalConnectionServices;
use liminal_server::server::{ConnectionSupervisor, ServerListener};

const CHANNEL: &str = "app.events";
/// The blanket reason code the server stamps on every publish failure today
/// (`apply.rs`, ruling R3 defers splitting it). Flush must report it verbatim.
const SERVER_ERROR_CODE: u16 = 0xFFFF;

/// Storeless TCP-only embedding: one `durable = false` channel with no
/// explicit schema (the permissive any-valid-JSON default still validates and
/// rejects non-JSON payloads — the exact invisibility shape of finding (ii)).
struct RunningServer {
    tcp: ServerListener,
    addr: String,
}

impl RunningServer {
    fn start() -> Result<Self, Box<dyn Error>> {
        let config = ServerConfig {
            listen_address: "127.0.0.1:0".parse()?,
            health_listen_address: "127.0.0.1:0".parse()?,
            drain_timeout_ms: 4_000,
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
            services: ServicesConfig::default(),
            limits: LimitsConfig::default(),
            participant: None,
            websocket: None,
        };
        let services = Arc::new(LiminalConnectionServices::from_config(&config)?);
        let supervisor =
            ConnectionSupervisor::with_services_auth_and_limits(services, None, config.limits)?;
        let tcp = ServerListener::bind(&config, supervisor)?;
        let addr = tcp.local_addr().to_string();
        Ok(Self { tcp, addr })
    }
}

/// The invisibility fix, end-to-end: a schema-0 rejection between two valid
/// publishes surfaces through `flush()` as ONE raw wire-order rejection —
/// blanket reason code and the server's schema-mismatch text verbatim, no
/// SDK-side error fabrication (R4) — while the valid publishes resolve
/// accepted inside the same budget (T1 clean-window accounting).
#[test]
fn schema_rejection_surfaces_through_flush_with_raw_reason() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start()?;
    let publisher = PushClient::connect(&server.addr)?;

    publisher.publish(CHANNEL, b"\"ok-0\"".to_vec())?;
    publisher.publish(CHANNEL, b"not-json".to_vec())?;
    publisher.publish(CHANNEL, b"\"ok-1\"".to_vec())?;

    let outcome = publisher.flush()?;
    assert_eq!(
        outcome.unresolved(),
        0,
        "all three verdicts arrive inside the flush budget"
    );
    assert!(!outcome.is_proven_accepted());
    let failures = outcome.failures();
    assert_eq!(
        failures.len(),
        1,
        "exactly the invalid publish was rejected"
    );
    assert_eq!(failures[0].reason_code(), SERVER_ERROR_CODE);
    let message = failures[0]
        .message()
        .ok_or("rejection carried no server message")?;
    assert!(
        message.contains("invalid JSON payload"),
        "raw server schema-mismatch text is surfaced verbatim: {message}"
    );

    drop(publisher);
    server.tcp.shutdown()?;
    Ok(())
}

/// `close()` against the real server: an all-valid burst resolves to the ONLY
/// proven-accepted shape (`failures.is_empty() && unresolved == 0`), and the
/// sole-owner teardown discloses the graceful half-close.
#[test]
fn close_reports_clean_acceptance_and_half_close() -> Result<(), Box<dyn Error>> {
    const BURST: usize = 16;
    let server = RunningServer::start()?;
    let publisher = PushClient::connect(&server.addr)?;

    for index in 0..BURST {
        publisher.publish(CHANNEL, format!("\"event-{index}\"").into_bytes())?;
    }

    let outcome = publisher.close()?;
    assert!(
        outcome.is_proven_accepted(),
        "every burst publish must be proven accepted: {outcome:?}"
    );
    assert_eq!(outcome.mode(), FlushMode::FlushedAndHalfClosed);

    server.tcp.shutdown()?;
    Ok(())
}
