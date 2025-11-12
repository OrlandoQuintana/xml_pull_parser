use tracing::{info, warn, error, debug};
use tracing_subscriber::{fmt, EnvFilter};
use tracing_appender::rolling;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ─────────────────────────────────────────────
    // 1️⃣ Create a rolling file appender (daily files)
    // ─────────────────────────────────────────────
    let file_appender = rolling::daily("logs", "etl.log");

    // ─────────────────────────────────────────────
    // 2️⃣ Build a subscriber that writes both to stdout and to the file
    // ─────────────────────────────────────────────
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env()) // e.g., RUST_LOG=info
        .with_writer(non_blocking)
        .with_ansi(false) // no color codes in file
        .init();

    // ─────────────────────────────────────────────
    // 3️⃣ Example usage
    // ─────────────────────────────────────────────
    info!("Starting ETL pipeline...");
    debug!("Using {} threads", 8);

    // Simulate an operation
    tokio::time::sleep(Duration::from_secs(1)).await;
    warn!("This is a warning message.");
    error!("This is an error message.");

    Ok(())
}

//[dependencies]
//tracing = "0.1"
//tracing-subscriber = { version = "0.3", features = ["env-filter", "fmt"] }
//tracing-appender = "0.2"

//RUST_LOG=info cargo run