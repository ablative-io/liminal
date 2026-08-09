//! P0 #56 pin 4: a refused admission moves a counter an operator can scrape.
//!
//! The field boot refused 82,166 connections in a row and the metrics surface
//! said nothing about it. `liminal_connections_active` cannot show this by
//! construction: it is incremented in `register_record`
//! (connection/supervisor.rs:2822), which is reached only AFTER admission has
//! already succeeded. A refusal is precisely the case where it never runs, so
//! on a fully-refusing server that gauge reads a flat, healthy-looking zero
//! while nothing at all is being served.
//!
//! What follows is the counter that can only go up when a connection was turned
//! away, labelled by a bounded class so an operator can tell "the server is
//! full" from "the server cannot allocate" without reading logs.

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
fn a_saturation_refusal_increments_a_labelled_admission_counter() -> Result<(), Box<dyn Error>> {
    let mut config = config()?;
    // One slot, so the second connection is refused by the §5 bound rather than
    // by anything injected: this is the ordinary production refusal path.
    config.limits.max_connections = 1;
    let services: Arc<dyn ConnectionServices> =
        Arc::new(LiminalConnectionServices::from_config(&config)?);
    let server = EmbeddedServer::with_services_auth_and_limits(services, None, config.limits)?;

    let _admitted = server.connect_loopback()?;
    let refused = server.connect_loopback();
    assert!(
        refused.is_err(),
        "the second connection must be refused at max_connections = 1"
    );

    let registry = global_registry().ok_or("the embedded server must install the registry")?;
    let exposition = render(&registry.snapshot());

    assert!(
        exposition.contains("liminal_admission_refusals_total"),
        "a refused admission must be countable, got:\n{exposition}"
    );
    assert!(
        exposition.contains("connections_saturated"),
        "the refusal counter must be labelled by its bounded reason class, got:\n{exposition}"
    );
    // The gauge that CANNOT show this stays where it was, which is the point of
    // adding a second family rather than reinterpreting the first.
    assert!(
        exposition.contains("liminal_connections_active"),
        "the pre-existing gauge must be unchanged and still exposed"
    );
    Ok(())
}
