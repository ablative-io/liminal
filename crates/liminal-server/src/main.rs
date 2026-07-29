use std::io;
use std::path::PathBuf;

use clap::Parser;

/// Level directives applied when the operator sets no `RUST_LOG`.
///
/// The server's own crates run at `info` — that is where the lifecycle events an
/// operator watches for live (startup, shutdown requested, drain outcomes) — while
/// everything else is floored at `warn` so a dependency that later starts emitting
/// `tracing` events cannot flood the log by default, yet still cannot go silent
/// about a warning or an error. `liminal` is the library crate's lib name (its
/// package is published as `liminal-rs`), which is what `tracing` uses as the
/// event target.
const DEFAULT_FILTER: &str = "warn,liminal_server=info,liminal=info";

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long)]
    config: PathBuf,
}

/// Installs the process-wide `tracing` subscriber that renders events to stderr.
///
/// Without this the server's 120 `tracing` events — connection failures, drain
/// timeouts, durable-flush failures among them — are dropped by the no-op global
/// dispatcher and the process runs from boot to shutdown printing nothing.
///
/// Output goes to **stderr** (the `fmt` builder's own default is stdout), leaving
/// stdout free for anything the operator pipes.
///
/// `RUST_LOG` is honoured when set; when it is unset — or set to something
/// `EnvFilter` cannot parse — [`DEFAULT_FILTER`] applies instead.
///
/// Installing a global subscriber can only ever succeed once per process, so this
/// uses `try_init` and treats an already-installed dispatcher as success: a
/// consumer that embeds this crate and installs its own subscriber first keeps it,
/// and neither case is worth aborting startup over.
fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(DEFAULT_FILTER));

    // Deliberately discarded: the sole error case is "a global subscriber is
    // already installed", which is the outcome this call wanted anyway.
    drop(
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(io::stderr)
            .try_init(),
    );
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    init_tracing();

    liminal_server::run(cli.config.as_path())?;

    Ok(())
}
