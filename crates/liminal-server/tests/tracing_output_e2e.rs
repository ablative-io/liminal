//! The shipped binary must actually SAY something.
//!
//! `liminal-server` emits 120 `tracing` events (58 `warn`, 17 `error` — connection
//! failures, drain timeouts, durable-flush failures). Without a subscriber
//! installed at startup every one of them is dropped on the floor: the process
//! runs from boot to shutdown printing nothing at all, so an operator watching
//! the logs cannot tell a healthy server from a wedged one.
//!
//! This test spawns the REAL binary against a minimal valid config, proves the
//! server actually booted (its wire port accepts a TCP connection), and then
//! asserts the `runtime.rs` startup event — `"liminal server started"` at `info`
//! — reaches the child's stderr. `RUST_LOG` is explicitly removed from the child
//! environment so this exercises the DEFAULT level, not an operator override.

use std::error::Error;
use std::io::{BufRead, BufReader};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};

/// The startup event `crate::server::runtime::run` emits once the listeners are
/// bound. Its arrival on stderr is the user-visible proof a subscriber is live.
const STARTUP_EVENT: &str = "liminal server started";

/// Bound on how long the spawned server may take to bind its wire port.
const BOOT_DEADLINE: Duration = Duration::from_secs(10);

/// Bound on how long the startup line may take to surface on stderr once the
/// server is demonstrably up.
const OUTPUT_DEADLINE: Duration = Duration::from_secs(10);

/// Kills and reaps the spawned server however the test exits, so a failing
/// assertion never leaks a listening process onto the box.
struct ServerProcess {
    child: Child,
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Reserves an ephemeral loopback port and releases it, returning the address.
///
/// Config validation rejects port `0` (`listen_address: port must be non-zero`),
/// so the binary cannot be handed an ephemeral bind directly; the kernel picks
/// the number here and the server re-binds it a moment later. This is the same
/// idiom the existing server e2e tests use for their health ports.
fn reserved_address() -> Result<SocketAddr, Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    drop(listener);
    Ok(address)
}

/// Blocks until `address` accepts a TCP connection or [`BOOT_DEADLINE`] expires.
///
/// This is the honesty guard: it separates "the server never started" from "the
/// server started but said nothing". Only the second is the failure under test.
fn wait_until_listening(address: SocketAddr) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + BOOT_DEADLINE;
    while Instant::now() < deadline {
        if TcpStream::connect_timeout(&address, Duration::from_millis(250)).is_ok() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(50));
    }
    Err(format!("server never bound {address} within {BOOT_DEADLINE:?}").into())
}

/// Drains the child's stderr on a helper thread, forwarding whole lines.
///
/// The reader owns the pipe and ends at EOF, which the [`ServerProcess`] kill
/// guarantees, so the thread is left detached rather than joined.
fn stderr_lines(child: &mut Child) -> Result<Receiver<String>, Box<dyn Error>> {
    let stderr = child
        .stderr
        .take()
        .ok_or("spawned server had no piped stderr")?;
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(stderr).lines() {
            let Ok(line) = line else { return };
            if sender.send(line).is_err() {
                return;
            }
        }
    });
    Ok(receiver)
}

/// Collects stderr lines until one contains `needle` or [`OUTPUT_DEADLINE`]
/// expires, returning everything seen so a failure names what DID arrive.
fn await_line(receiver: &Receiver<String>, needle: &str) -> Result<Vec<String>, Vec<String>> {
    let deadline = Instant::now() + OUTPUT_DEADLINE;
    let mut seen = Vec::new();
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(seen);
        }
        match receiver.recv_timeout(remaining) {
            Ok(line) => {
                let matched = line.contains(needle);
                seen.push(line);
                if matched {
                    return Ok(seen);
                }
            }
            Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => return Err(seen),
        }
    }
}

#[test]
fn spawned_server_reports_startup_on_stderr() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let listen_address = reserved_address()?;
    let health_listen_address = reserved_address()?;
    let config_path = directory.path().join("server.toml");
    std::fs::write(
        &config_path,
        format!(
            "listen_address = \"{listen_address}\"\n\
             health_listen_address = \"{health_listen_address}\"\n\
             drain_timeout_ms = 5000\n\
             channels = []\n\
             routing_rules = []\n"
        ),
    )?;

    let mut server = ServerProcess {
        child: Command::new(env!("CARGO_BIN_EXE_liminal-server"))
            .arg("--config")
            .arg(&config_path)
            // The DEFAULT level is what is under test: an operator who sets
            // nothing must still see the server's own info events.
            .env_remove("RUST_LOG")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?,
    };
    let stderr = stderr_lines(&mut server.child)?;

    // Proves the boot path succeeded. If this fails the server never got as far
    // as the startup event, and the assertion below would be testing nothing.
    wait_until_listening(listen_address)?;

    match await_line(&stderr, STARTUP_EVENT) {
        Ok(_) => Ok(()),
        Err(seen) => Err(format!(
            "server bound {listen_address} but never wrote {STARTUP_EVENT:?} to stderr \
             within {OUTPUT_DEADLINE:?}; stderr carried {} line(s): {seen:#?}",
            seen.len()
        )
        .into()),
    }
}
