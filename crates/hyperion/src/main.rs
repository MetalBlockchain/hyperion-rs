use clap::{Parser, Subcommand};
use hyperion::api;
use hyperion::config::Config;
use hyperion::indexer;
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "hyperion",
    version,
    about = "Hyperion history indexer and API for Antelope chains"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the indexer: consume state history and fill Elasticsearch.
    Indexer {
        #[arg(short, long, default_value = "config.toml")]
        config: PathBuf,
    },
    /// Run the HTTP API server.
    Api {
        #[arg(short, long, default_value = "config.toml")]
        config: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Indexer { config } => indexer::run(Config::load(&config)?).await,
        Command::Api { config } => api::run(Config::load(&config)?).await,
    }
}
