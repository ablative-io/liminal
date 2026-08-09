//! P0 #56 pin 6: an EMBEDDED server has a live metrics surface.
//!
//! `crate::metrics::init()` had exactly one production call site — `server::run`
//! (runtime.rs:34) — and `run` is the deployment wrapper that loads a config
//! file and binds listeners. An embedder never calls it. The field estate runs
//! liminal embedded, so on the machine where 82,166 connections were refused,
//! every recording helper in `metrics.rs` was a no-op against an uninstalled
//! registry: the counters that would have shown the refusals did not merely
//! read zero, they did not exist.
//!
//! THIS FILE DELIBERATELY CONTAINS EXACTLY ONE TEST. The metrics registry is
//! process-global and install-once, so a sibling test in the same binary that
//! called `metrics::init()` — directly or through `server::run` — would install
//! it first and this pin would green on someone else's work. A separate
//! integration binary is the only way to be sure the embedded path is what
//! armed the surface. Do not add a second test here.

use std::error::Error;
use std::sync::Arc;

use liminal::metrics::{global_registry, render};
use liminal_server::config::{LimitsConfig, ServerConfig, ServicesConfig};
use liminal_server::server::connection::{ConnectionServices, LiminalConnectionServices};
use liminal_server::server::embedded::EmbeddedServer;

fn config() -> Result<ServerConfig, Box<dyn Error>> {
    Ok(ServerConfig {
        listen_address: "127.0.0.1:0".parse()?,
        health_listen_address: "127.0.0.1:0".parse()?,
        drain_timeout_ms: 30_000,
        channels: Vec::new(),
        routing_rules: Vec::new(),
        persistence_path: None,
        cluster: None,
        auth: None,
        services: ServicesConfig::default(),
        limits: LimitsConfig::default(),
        participant: None,
        websocket: None,
    })
}

#[test]
fn an_embedded_server_arms_the_process_metrics_surface() -> Result<(), Box<dyn Error>> {
    let config = config()?;
    let services: Arc<dyn ConnectionServices> =
        Arc::new(LiminalConnectionServices::from_config(&config)?);
    let server = EmbeddedServer::with_services(services)?;

    let registry = global_registry().ok_or(
        "an embedded server must install the process metrics registry: without it every \
         recording helper is a no-op and an embedder's counters do not exist",
    )?;

    // Admit one connection so a counter has something to say, then read the
    // exposition an operator would actually scrape.
    let client = server.connect_loopback()?;
    let exposition = render(&registry.snapshot());
    assert!(
        exposition.contains("liminal_connections_active"),
        "the embedded server's connection gauge must be in the exposition, got:\n{exposition}"
    );
    drop(client);
    Ok(())
}
