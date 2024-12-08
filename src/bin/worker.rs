use clap::Parser;
use rdfss::health;
use rdfss::master::node_client::HTTPNodeClient;
use rdfss::tracing::init::{init_tracing, inject_tracing_layer};
use rdfss::worker::consul::register_worker;
use rdfss::worker::router::create_worker_router;
use rdfss::worker::rpc::WorkerHealth;
use rdfss::worker::storage::Storage;
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Number of times to greet
    #[arg(short, long)]
    port: u32,

    #[clap(short, long, default_value = "50051")]
    rpc_port: u32,

    #[clap(short, long)]
    name: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    init_tracing();

    // interface for files operations
    let storage = Storage::new(args.name.clone());

    // node might need to call another node (chunk replication)
    let node_client = HTTPNodeClient::new(reqwest::Client::new());

    // build our application with a single route
    let app = inject_tracing_layer(create_worker_router(storage, Arc::new(node_client)));

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", args.port)).await?;
    let axum_h = tokio::spawn(async { axum::serve(listener, app).await });

    let addr: SocketAddr = format!("[::1]:{}", args.rpc_port).parse()?;
    let h = WorkerHealth::default();

    let name = args.name.clone();
    let rpc_h = tokio::spawn(async move {
        Server::builder()
            .trace_fn(move |_| tracing::info_span!("rpc", name = name))
            .add_service(health::health_server::HealthServer::new(h))
            .serve(addr)
            .await
    });

    register_worker(
        &args.name,
        "localhost",
        args.port as u64,
        &format!("[::1]:{}", args.rpc_port),
    )
    .await?;
    tokio::join!(axum_h, rpc_h);

    Ok(())
}
