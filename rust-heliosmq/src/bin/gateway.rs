use clap::Parser;
use heliosmq::{
    gateway::Gateway,
    config::GatewayConfig,
};
use tokio::signal;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, env = "HELIOS_NODE_ID")]
    node_id: Option<String>,

    #[arg(short, long, env = "HELIOS_LISTEN", default_value = "0.0.0.0:29092")]
    listen: String,

    #[arg(short, long, env = "HELIOS_CONTROLLER", default_value = "127.0.0.1:19091")]
    controller: String,

    #[arg(long, env = "HELIOS_METRICS", default_value = "0.0.0.0:9093")]
    metrics: String,
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
            .unwrap_or_else(|_| "gateway-1".to_string())
    });

    let config = GatewayConfig {
        node_id: node_id.clone(),
        listen_addr: args.listen,
        controller_addr: args.controller,
        metrics_addr: args.metrics,
        ..Default::default()
    };

    info!(
        node_id = %node_id,
        listen = %config.listen_addr,
        "Starting HeliosMQ gateway"
    );

    let gateway = Gateway::new(config);
    let handle = gateway.start().await?;

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

    info!("Shutting down gateway...");
    handle.shutdown().await?;
    info!("Gateway stopped");

    Ok(())
}
