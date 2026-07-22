use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::ServerError;
use crate::server::listener::{loopback_interrupt_target, shed_on_fd_exhaustion};

use super::checks::{SharedReadinessState, health_check, readiness_check};

use super::metrics_route;

const HEALTH_PATH: &str = "/health";
const READY_PATH: &str = "/ready";
const METRICS_PATH: &str = "/metrics";
const APPLICATION_JSON: &str = "application/json";
const READ_BUFFER_BYTES: usize = 2048;

/// Handle for a running health endpoint server.
///
/// W4 leg 2 (§4.2): the health accept worker BLOCKS in `accept` (kernel-parked,
/// zero idle wakes) rather than spinning a non-blocking poll with a backoff
/// sleep. Shutdown wakes the blocked `accept` with an explicit self-connect
/// interrupt to `interrupt_target` (the bound address, loopback-normalised),
/// mirroring the leg-1 listeners.
#[derive(Debug)]
pub struct HealthServerHandle {
    local_addr: SocketAddr,
    /// Loopback-normalised self-connect target used to interrupt the blocking
    /// `accept` at shutdown.
    interrupt_target: SocketAddr,
    shutdown: Arc<AtomicBool>,
    worker: Option<JoinHandle<Result<(), ServerError>>>,
    /// Count of `accept` calls issued by the worker (test observability for the
    /// zero-idle-wakes oracle: on a silent listener this stays at the single
    /// parked call). The worker always maintains the counter; only the host-side
    /// handle for reading it is test-scoped.
    #[cfg(test)]
    accept_attempts: Arc<AtomicU64>,
    /// Count of connections shed under fd exhaustion via the reserve descriptor
    /// (test observability for the shed helper reused from leg 1).
    #[cfg(test)]
    shed_count: Arc<AtomicU64>,
}

impl HealthServerHandle {
    /// Returns the bound address for the health endpoint server.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Stops the health endpoint server and waits for its worker thread to exit.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError::HealthEndpoint`] if the worker thread cannot be
    /// joined cleanly or if the server loop recorded a serving error.
    pub fn shutdown(mut self) -> Result<(), ServerError> {
        self.stop_worker()
    }

    fn stop_worker(&mut self) -> Result<(), ServerError> {
        self.shutdown.store(true, Ordering::SeqCst);
        let Some(worker) = self.worker.take() else {
            return Ok(());
        };
        // Explicit cross-platform interrupt (mirrors the leg-1 listeners): a
        // single self-connect wakes the blocked `accept`. The worker sees the
        // shutdown flag it observed under the same ordering and sheds the woken
        // (spurious) socket rather than serving it — at most one spurious accept
        // per interrupt. If the worker already exited (a real accept then flag
        // check), the listener is gone and this connect fails fast; the join then
        // returns immediately.
        if let Ok(waker) = TcpStream::connect(self.interrupt_target) {
            drop(waker);
        }

        worker.join().map_err(|_| ServerError::HealthEndpoint {
            message: "health endpoint worker thread terminated unexpectedly".to_owned(),
        })?
    }

    /// `accept` calls issued by the worker (test observability, oracle 8).
    #[cfg(test)]
    fn accept_attempts(&self) -> u64 {
        self.accept_attempts.load(Ordering::SeqCst)
    }

    /// Connections shed under fd exhaustion (test observability).
    #[cfg(test)]
    fn shed_count(&self) -> u64 {
        self.shed_count.load(Ordering::SeqCst)
    }
}

impl Drop for HealthServerHandle {
    fn drop(&mut self) {
        if let Err(error) = self.stop_worker() {
            tracing::debug!(%error, "health endpoint shutdown during drop failed");
        }
    }
}

/// Starts the health endpoint HTTP server on a distinct health bind address.
///
/// The returned server handle is independent from the main wire protocol
/// listener. Binding the health endpoint does not mark the main listener ready.
///
/// # Errors
///
/// Returns [`ServerError::HealthEndpoint`] when the health listener cannot bind
/// or cannot report its local address. The listener stays BLOCKING (W4 leg 2):
/// the accept worker kernel-parks in `accept` with zero idle wakes; shutdown
/// wakes it via the self-connect interrupt.
pub fn start_health_server(
    bind_address: SocketAddr,
    readiness: SharedReadinessState,
) -> Result<HealthServerHandle, ServerError> {
    let listener =
        TcpListener::bind(bind_address).map_err(|error| ServerError::HealthEndpoint {
            message: format!("failed to bind health endpoint at {bind_address}: {error}"),
        })?;
    // The listener stays BLOCKING (W4 leg 2): the accept worker kernel-parks in
    // `accept` with zero idle wakes; shutdown wakes it via the self-connect
    // interrupt below.
    let local_addr = listener
        .local_addr()
        .map_err(|error| ServerError::HealthEndpoint {
            message: format!("failed to inspect health endpoint listener address: {error}"),
        })?;
    let interrupt_target = loopback_interrupt_target(local_addr);
    let shutdown = Arc::new(AtomicBool::new(false));
    let accept_attempts = Arc::new(AtomicU64::new(0));
    let shed_count = Arc::new(AtomicU64::new(0));
    let worker_shutdown = Arc::clone(&shutdown);
    let worker_attempts = Arc::clone(&accept_attempts);
    let worker_shed = Arc::clone(&shed_count);
    let worker = thread::spawn(move || {
        serve(
            &listener,
            &readiness,
            &worker_shutdown,
            &worker_attempts,
            &worker_shed,
        )
    });

    Ok(HealthServerHandle {
        local_addr,
        interrupt_target,
        shutdown,
        worker: Some(worker),
        #[cfg(test)]
        accept_attempts,
        #[cfg(test)]
        shed_count,
    })
}

fn serve(
    listener: &TcpListener,
    readiness: &SharedReadinessState,
    shutdown: &AtomicBool,
    accept_attempts: &AtomicU64,
    shed_count: &AtomicU64,
) -> Result<(), ServerError> {
    // One reserve descriptor held for the shed-with-spare-fd EMFILE policy,
    // reusing the leg-1 helper.
    let mut reserve = listener.try_clone().ok();
    while !shutdown.load(Ordering::SeqCst) {
        accept_attempts.fetch_add(1, Ordering::SeqCst);
        match listener.accept() {
            Ok((stream, ..)) => {
                if shutdown.load(Ordering::SeqCst) {
                    // Accepted while shutdown fired — the self-connect interrupt,
                    // or a real probe racing the broadcast. Shed it, never serve:
                    // no request slips past the shutdown broadcast, and the worker
                    // exits promptly rather than sleeping.
                    drop(stream);
                    continue;
                }
                // A per-connection error (e.g. a TCP probe that connects but sends no HTTP
                // data within the read timeout) must NOT terminate the serve loop — otherwise
                // a single port probe kills the health server for the process lifetime and
                // subsequent liveness/readiness probes get connection-refused. Only fatal
                // listener-level accept errors (below) terminate serving.
                if let Err(error) = handle_connection(stream, readiness) {
                    tracing::debug!(%error, "health endpoint connection error");
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) if is_transient_accept_error(&error) => {
                shed_on_fd_exhaustion(listener, &mut reserve, shed_count, &error);
            }
            Err(error) => {
                return Err(ServerError::HealthEndpoint {
                    message: format!("health endpoint accept failed: {error}"),
                });
            }
        }
    }

    Ok(())
}

/// EMFILE/ENFILE resource exhaustion is transient, exactly as the leg-1 accept
/// loops treat it (mirrors the sibling WebSocket listener's local predicate).
fn is_transient_accept_error(error: &std::io::Error) -> bool {
    matches!(error.raw_os_error(), Some(code) if code == 24 || code == 23)
}

fn handle_connection(
    mut stream: TcpStream,
    readiness: &SharedReadinessState,
) -> Result<(), ServerError> {
    stream
        .set_nonblocking(false)
        .map_err(|error| ServerError::HealthEndpoint {
            message: format!("failed to configure health request stream: {error}"),
        })?;
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .map_err(|error| ServerError::HealthEndpoint {
            message: format!("failed to set health request read timeout: {error}"),
        })?;

    let mut buffer = [0_u8; READ_BUFFER_BYTES];
    let bytes_read = stream
        .read(&mut buffer)
        .map_err(|error| ServerError::HealthEndpoint {
            message: format!("failed to read health request: {error}"),
        })?;

    if bytes_read == 0 {
        return Ok(());
    }

    let response = response_for_request(&buffer[..bytes_read], readiness)?;
    stream
        .write_all(&response)
        .map_err(|error| ServerError::HealthEndpoint {
            message: format!("failed to write health response: {error}"),
        })?;
    stream.flush().map_err(|error| ServerError::HealthEndpoint {
        message: format!("failed to flush health response: {error}"),
    })
}

fn response_for_request(
    request: &[u8],
    readiness: &SharedReadinessState,
) -> Result<Vec<u8>, ServerError> {
    let Ok(request) = std::str::from_utf8(request) else {
        return Ok(empty_response(StatusCode::BadRequest));
    };
    let Some((method, path)) = parse_request_line(request) else {
        return Ok(empty_response(StatusCode::BadRequest));
    };

    match (method, path) {
        ("GET", HEALTH_PATH) => json_response(StatusCode::Ok, &health_check()),
        ("GET", READY_PATH) => {
            let status = readiness_check(&readiness.snapshot());
            let status_code = if status.ready {
                StatusCode::Ok
            } else {
                StatusCode::ServiceUnavailable
            };
            json_response(status_code, &status)
        }
        ("GET", METRICS_PATH) => Ok(response(
            StatusCode::Ok,
            Some(metrics_route::CONTENT_TYPE),
            metrics_route::render_body().as_bytes(),
        )),
        (_, HEALTH_PATH | READY_PATH | METRICS_PATH) => {
            Ok(empty_response(StatusCode::MethodNotAllowed))
        }
        _ => Ok(empty_response(StatusCode::NotFound)),
    }
}

fn parse_request_line(request: &str) -> Option<(&str, &str)> {
    let request_line = request.lines().next()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?;
    let path = parts.next()?;
    parts.next()?;

    Some((method, path))
}

fn json_response<T>(status: StatusCode, value: &T) -> Result<Vec<u8>, ServerError>
where
    T: serde::Serialize,
{
    let body = serde_json::to_vec(value).map_err(|error| ServerError::HealthEndpoint {
        message: format!("failed to serialize health response: {error}"),
    })?;
    Ok(response(status, Some(APPLICATION_JSON), &body))
}

fn empty_response(status: StatusCode) -> Vec<u8> {
    response(status, None, &[])
}

fn response(status: StatusCode, content_type: Option<&str>, body: &[u8]) -> Vec<u8> {
    let mut response = Vec::new();
    let status_line = format!("HTTP/1.1 {} {}\r\n", status.code(), status.reason());
    response.extend_from_slice(status_line.as_bytes());
    response.extend_from_slice(format!("Content-Length: {}\r\n", body.len()).as_bytes());
    response.extend_from_slice(b"Connection: close\r\n");
    if let Some(content_type) = content_type {
        response.extend_from_slice(format!("Content-Type: {content_type}\r\n").as_bytes());
    }
    response.extend_from_slice(b"\r\n");
    response.extend_from_slice(body);
    response
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StatusCode {
    Ok,
    BadRequest,
    NotFound,
    MethodNotAllowed,
    ServiceUnavailable,
}

impl StatusCode {
    const fn code(self) -> u16 {
        match self {
            Self::Ok => 200,
            Self::BadRequest => 400,
            Self::NotFound => 404,
            Self::MethodNotAllowed => 405,
            Self::ServiceUnavailable => 503,
        }
    }

    const fn reason(self) -> &'static str {
        match self {
            Self::Ok => "OK",
            Self::BadRequest => "Bad Request",
            Self::NotFound => "Not Found",
            Self::MethodNotAllowed => "Method Not Allowed",
            Self::ServiceUnavailable => "Service Unavailable",
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpStream};
    use std::time::Duration;

    use serde_json::Value;

    use super::{response_for_request, start_health_server};
    use crate::health::checks::{
        ClusterReadiness, ReadinessCondition, ReadinessState, SharedReadinessState,
    };

    fn loopback_ephemeral() -> Result<SocketAddr, Box<dyn std::error::Error>> {
        Ok("127.0.0.1:0".parse()?)
    }

    fn get(address: SocketAddr, path: &str) -> Result<String, Box<dyn std::error::Error>> {
        let mut stream = TcpStream::connect(address)?;
        stream.set_read_timeout(Some(Duration::from_secs(2)))?;
        let request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n");
        stream.write_all(request.as_bytes())?;

        let mut response = String::new();
        stream.read_to_string(&mut response)?;
        Ok(response)
    }

    fn assert_status(response: &str, status: u16) {
        let expected = format!("HTTP/1.1 {status} ");
        assert!(
            response.starts_with(&expected),
            "response status did not start with {expected}: {response}"
        );
    }

    fn body(response: &str) -> Result<&str, Box<dyn std::error::Error>> {
        let Some((_headers, body)) = response.split_once("\r\n\r\n") else {
            return Err("response did not contain a header/body separator".into());
        };
        Ok(body)
    }

    fn json_body(response: &str) -> Result<Value, Box<dyn std::error::Error>> {
        Ok(serde_json::from_str(body(response)?)?)
    }

    #[test]
    fn health_endpoint_returns_json_200_regardless_of_readiness()
    -> Result<(), Box<dyn std::error::Error>> {
        let readiness = SharedReadinessState::new(ReadinessState::default());
        let server = start_health_server(loopback_ephemeral()?, readiness)?;

        let response = get(server.local_addr(), "/health")?;
        server.shutdown()?;

        assert_status(&response, 200);
        assert!(response.contains("Content-Type: application/json\r\n"));
        let body = json_body(&response)?;
        assert_eq!(body["status"], "healthy");

        Ok(())
    }

    #[test]
    fn ready_endpoint_returns_503_before_main_listener_binds()
    -> Result<(), Box<dyn std::error::Error>> {
        let readiness = SharedReadinessState::new(ReadinessState::new(
            true,
            false,
            ClusterReadiness::NotConfigured,
        ));
        let server = start_health_server(loopback_ephemeral()?, readiness)?;

        let response = get(server.local_addr(), "/ready")?;
        server.shutdown()?;

        assert_status(&response, 503);
        assert!(response.contains("Content-Type: application/json\r\n"));
        let body = json_body(&response)?;
        assert_eq!(body["ready"], false);
        assert_eq!(body["unmet_conditions"][0], "listener_bound");

        Ok(())
    }

    #[test]
    fn ready_endpoint_returns_200_after_all_startup_gates() -> Result<(), Box<dyn std::error::Error>>
    {
        let readiness = SharedReadinessState::new(ReadinessState::ready_without_cluster());
        let server = start_health_server(loopback_ephemeral()?, readiness)?;

        let response = get(server.local_addr(), "/ready")?;
        server.shutdown()?;

        assert_status(&response, 200);
        let body = json_body(&response)?;
        assert_eq!(body["ready"], true);
        let Some(unmet_conditions) = body["unmet_conditions"].as_array() else {
            return Err("unmet_conditions should be an array".into());
        };
        assert!(unmet_conditions.is_empty());

        Ok(())
    }

    #[test]
    fn ready_endpoint_updates_from_shared_readiness_state() -> Result<(), Box<dyn std::error::Error>>
    {
        let readiness = SharedReadinessState::new(ReadinessState::default());
        let server = start_health_server(loopback_ephemeral()?, readiness.clone())?;

        let response = get(server.local_addr(), "/ready")?;
        assert_status(&response, 503);

        readiness.set_config_loaded(true);
        readiness.set_listener_bound(true);
        let response = get(server.local_addr(), "/ready")?;
        server.shutdown()?;

        assert_status(&response, 200);

        Ok(())
    }

    #[test]
    fn clustered_ready_transitions_503_to_200_when_membership_established()
    -> Result<(), Box<dyn std::error::Error>> {
        // A clustered server starts with the cluster gate unmet: config loaded and
        // listener bound, but membership not yet established (G2). /ready is 503.
        let readiness = SharedReadinessState::new(ReadinessState::new(
            true,
            true,
            ClusterReadiness::Configured {
                membership_established: false,
            },
        ));
        let server = start_health_server(loopback_ephemeral()?, readiness.clone())?;

        let response = get(server.local_addr(), "/ready")?;
        assert_status(&response, 503);
        let body = json_body(&response)?;
        assert_eq!(body["ready"], false);
        assert_eq!(
            body["unmet_conditions"][0],
            serde_json::to_value(ReadinessCondition::ClusterMembershipEstablished)?
        );

        // The cluster start's on_established hook flips exactly this flag; once set,
        // /ready must transition to 200 with no unmet conditions.
        readiness.set_cluster_membership_established(true);
        let response = get(server.local_addr(), "/ready")?;
        server.shutdown()?;

        assert_status(&response, 200);
        let body = json_body(&response)?;
        assert_eq!(body["ready"], true);
        let Some(unmet_conditions) = body["unmet_conditions"].as_array() else {
            return Err("unmet_conditions should be an array".into());
        };
        assert!(unmet_conditions.is_empty());

        Ok(())
    }

    #[test]
    fn cluster_readiness_is_listed_when_configured_but_not_joined()
    -> Result<(), Box<dyn std::error::Error>> {
        let readiness = SharedReadinessState::new(ReadinessState::new(
            true,
            true,
            ClusterReadiness::Configured {
                membership_established: false,
            },
        ));
        let response = response_for_request(b"GET /ready HTTP/1.1\r\n\r\n", &readiness)?;
        let response = String::from_utf8(response)?;

        assert_status(&response, 503);
        let body = json_body(&response)?;
        assert_eq!(
            body["unmet_conditions"][0],
            serde_json::to_value(ReadinessCondition::ClusterMembershipEstablished)?
        );

        Ok(())
    }

    #[test]
    fn unsupported_paths_are_not_served() -> Result<(), Box<dyn std::error::Error>> {
        let readiness = SharedReadinessState::default();
        let response = response_for_request(b"GET /unknown HTTP/1.1\r\n\r\n", &readiness)?;
        let response = String::from_utf8(response)?;

        assert_status(&response, 404);

        Ok(())
    }

    #[test]
    fn unsupported_methods_on_health_paths_are_rejected() -> Result<(), Box<dyn std::error::Error>>
    {
        let readiness = SharedReadinessState::default();
        let response = response_for_request(b"POST /health HTTP/1.1\r\n\r\n", &readiness)?;
        let response = String::from_utf8(response)?;

        assert_status(&response, 405);

        Ok(())
    }
}
