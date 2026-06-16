use std::path::PathBuf;

use clap::Parser;

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long)]
    config: PathBuf,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    liminal_server::run(cli.config.as_path())?;

    Ok(())
}
