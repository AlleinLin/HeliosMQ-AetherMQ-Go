use std::sync::Arc;

use clap::Parser;
use heliosmq::{
    controller::Controller,
    config::ControllerConfig,
    raft::RaftNode,
};
use tokio::signal;
use tracing::{info, error};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, env = "HELIOS_NODE_ID")]
    node_id: Option<String>,

    #[arg(short, long, env = "HELIOS_LISTEN", default_value = "0.0.0.0:19091")]
    listen: String,

    #[arg(long, env = "HELIOS_RAFT_ADDR", default_value = "0.0.0.0:19092")]
    raft_addr: String,

    #[arg(short, long, env = "HELIOS_DATA_DIR", default_value = "./data/controller")]
    data_dir: String,

    #[arg(long, env = "HELIOS_JOIN")]
    join: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let args = Args::parse();

    let node_id = args.node_id.unwrap_or_else(|| {
        hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "controller-1".to_string())
    });

    let config = ControllerConfig {
        node_id: node_id.clone(),
        listen_addr: args.listen,
        raft_addr: args.raft_addr,
        data_dir: args.data_dir,
        join_addr: args.join,
        ..Default::default()
    };

    info!(
        node_id = %node_id,
        listen = %config.listen_addr,
        raft = %config.raft_addr,
        "Starting HeliosMQ controller"
    );

    let raft_node = Arc::new(RaftNode::new(&config).await?);
    let controller = Controller::new(config, raft_node);

    let handle = controller.start().await?;

    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("Shutting down controller...");
    handle.shutdown().await?;
    info!("Controller stopped");

    Ok(())
}
