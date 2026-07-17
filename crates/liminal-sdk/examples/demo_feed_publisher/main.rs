//! Demo graph feed publisher over the SDK's opaque-byte TCP publish path.
//!
//! This is deliberately an example target, not part of `liminal-sdk`'s public API.
//! Run it with `cargo run -p liminal-sdk --example demo_feed_publisher`.
//! `LIMINAL_ADDRESS`, `LIMINAL_DEMO_CHANNEL`, `LIMINAL_DEMO_COMPONENT_ID`, and
//! `LIMINAL_DEMO_GENERATION_FILE` override the documented defaults below.

mod authority;
mod cadence;
mod envelope;
mod graph;
mod jcs;

use std::env;
use std::path::PathBuf;
use std::process::ExitCode;
use std::thread;

use authority::{FeedAuthority, FileGenerationStore};
use cadence::{CadenceEngine, SNAPSHOT_PERIOD, TICK_INTERVAL};
use envelope::{ComponentId, FrameEnvelopeCodec};
use graph::GraphViewState;
use liminal_sdk::PushClient;

const DEFAULT_ADDRESS: &str = "127.0.0.1:9000";
const DEFAULT_CHANNEL: &str = "frame.demo.graph-view";
const DEFAULT_COMPONENT_ID: &str = "graph-view-demo";
const DEFAULT_GENERATION_FILE: &str = ".liminal-demo-feed-generation";

#[derive(Debug, thiserror::Error)]
enum AppError {
    #[error("configuration variable {name} is not valid Unicode: {source}")]
    Configuration {
        name: &'static str,
        source: env::VarError,
    },
    #[error(transparent)]
    Authority(#[from] authority::AuthorityError),
    #[error(transparent)]
    Contract(#[from] envelope::EnvelopeError),
    #[error(transparent)]
    Graph(#[from] graph::GraphError),
    #[error("failed to connect demo publisher: {0}")]
    Connect(liminal_sdk::SdkError),
    #[error(transparent)]
    Cadence(#[from] cadence::CadenceError),
}

fn configured(name: &'static str, default: &str) -> Result<String, AppError> {
    match env::var(name) {
        Ok(value) => Ok(value),
        Err(env::VarError::NotPresent) => Ok(default.to_owned()),
        Err(source) => Err(AppError::Configuration { name, source }),
    }
}

fn run() -> Result<(), AppError> {
    let address = configured("LIMINAL_ADDRESS", DEFAULT_ADDRESS)?;
    let channel = configured("LIMINAL_DEMO_CHANNEL", DEFAULT_CHANNEL)?;
    let component_id = ComponentId::new(&configured(
        "LIMINAL_DEMO_COMPONENT_ID",
        DEFAULT_COMPONENT_ID,
    )?)?;
    let generation_file = PathBuf::from(configured(
        "LIMINAL_DEMO_GENERATION_FILE",
        DEFAULT_GENERATION_FILE,
    )?);

    let authority = FeedAuthority::start(FileGenerationStore::new(generation_file))?;

    // PushWriter is the existing SDK path that preserves these opaque bytes exactly.
    // Its schema id is zero and its `Result<(), SdkError>` is a write outcome, not a
    // delivery acknowledgement; periodic snapshots provide demo resynchronization.
    let client = PushClient::connect(&address).map_err(AppError::Connect)?;
    let writer = client.writer_handle();
    let mut cadence = CadenceEngine::new(
        channel,
        component_id,
        authority,
        FrameEnvelopeCodec,
        GraphViewState::new()?,
        writer,
        SNAPSHOT_PERIOD,
    )?;

    cadence.emit_initial_snapshot()?;
    loop {
        // This wall clock is demo content pacing only. It is not protocol authority,
        // retry authority, reconnect backoff, or a delivery timer.
        thread::sleep(TICK_INTERVAL);
        cadence.emit_tick()?;
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("demo feed publisher failed: {error}");
            ExitCode::FAILURE
        }
    }
}
