//! `/metrics` Prometheus exposition body over the process-global registry.
//!
//! The health server already speaks a hand-rolled HTTP; this module only
//! supplies the `GET /metrics` body and its content type, leaving the response
//! framing to [`super::endpoint`].

use liminal::metrics::{global_registry, render};

/// Prometheus text-exposition content type served on `/metrics`.
pub(super) const CONTENT_TYPE: &str = "text/plain; version=0.0.4";

/// Renders the current global registry snapshot as Prometheus 0.0.4 text.
///
/// When metrics are disabled — no global registry installed, e.g. a health
/// server started without the server runtime having called
/// [`crate::metrics::init`] — the body is empty; the endpoint still answers 200
/// so a scraper observes a live target.
pub(super) fn render_body() -> String {
    global_registry().map_or_else(String::new, |registry| render(&registry.snapshot()))
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpStream};
    use std::time::Duration;

    use super::{CONTENT_TYPE, render_body};
    use crate::health::checks::SharedReadinessState;
    use crate::health::endpoint::start_health_server;

    fn get(address: SocketAddr, path: &str) -> Result<String, Box<dyn std::error::Error>> {
        let mut stream = TcpStream::connect(address)?;
        stream.set_read_timeout(Some(Duration::from_secs(2)))?;
        let request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n");
        stream.write_all(request.as_bytes())?;
        let mut response = String::new();
        stream.read_to_string(&mut response)?;
        Ok(response)
    }

    #[test]
    fn render_body_exposes_the_three_server_families_after_activity() {
        crate::metrics::init();
        crate::metrics::connection_spawned();
        crate::metrics::publish_accepted();
        crate::metrics::deliveries_recorded(3);

        let body = render_body();

        // Parseable exposition: every family carries a `# TYPE` header line.
        assert!(body.contains("# TYPE liminal_connections_active gauge"));
        assert!(body.contains("# TYPE liminal_publishes_total counter"));
        assert!(body.contains("# TYPE liminal_deliveries_total counter"));
    }

    #[test]
    fn metrics_endpoint_serves_prometheus_text_with_200() -> Result<(), Box<dyn std::error::Error>>
    {
        crate::metrics::init();
        crate::metrics::publish_accepted();

        let readiness = SharedReadinessState::default();
        let server = start_health_server("127.0.0.1:0".parse()?, readiness)?;
        let response = get(server.local_addr(), "/metrics")?;
        server.shutdown()?;

        assert!(
            response.starts_with("HTTP/1.1 200 "),
            "metrics endpoint must answer 200: {response}"
        );
        assert!(response.contains(&format!("Content-Type: {CONTENT_TYPE}\r\n")));
        let Some((_headers, body)) = response.split_once("\r\n\r\n") else {
            return Err("metrics response had no header/body separator".into());
        };
        assert!(body.contains("liminal_publishes_total"));

        Ok(())
    }

    #[test]
    fn health_and_ready_are_unaffected_by_the_metrics_route()
    -> Result<(), Box<dyn std::error::Error>> {
        crate::metrics::init();

        let readiness = SharedReadinessState::default();
        let server = start_health_server("127.0.0.1:0".parse()?, readiness)?;
        let health = get(server.local_addr(), "/health")?;
        let ready = get(server.local_addr(), "/ready")?;
        server.shutdown()?;

        assert!(health.starts_with("HTTP/1.1 200 "));
        assert!(health.contains("Content-Type: application/json\r\n"));
        // Default readiness is not ready (config not loaded), so /ready stays 503 —
        // proving the new route did not disturb the existing handlers.
        assert!(ready.starts_with("HTTP/1.1 503 "));

        Ok(())
    }
}
