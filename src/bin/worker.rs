use clap::Parser;
use rdfss::health;
use rdfss::tracing::init::{init_tracing, inject_tracing_layer};
use rdfss::worker::router::{create_worker_router, WorkerHealth};
use rdfss::worker::storage::Storage;
use std::net::SocketAddr;
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

    let storage = Storage::new(args.name.clone());

    // build our application with a single route
    let app = inject_tracing_layer(create_worker_router(storage));

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", args.port)).await?;
    let axum_h = tokio::spawn(async { axum::serve(listener, app).await });

    let addr: SocketAddr = format!("[::1]:{}", args.rpc_port).parse()?;
    let h = WorkerHealth::default();

    let rpc_h = tokio::spawn(async move {
        Server::builder()
            .trace_fn(move |_| tracing::info_span!("rpc", name = args.name.clone()))
            .add_service(health::healthchecker_server::HealthcheckerServer::new(h))
            .serve(addr)
            .await
    });

    tokio::join!(axum_h, rpc_h);

    Ok(())
}
