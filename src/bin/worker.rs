use clap::Parser;
use rdfss::tracing::init::{init_tracing, inject_tracing_layer};
use rdfss::web::router::create_worker_router;
use rdfss::worker::storage::Storage;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Number of times to greet
    #[arg(short, long)]
    port: u32,

    #[clap(short, long)]
    name: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    init_tracing();

    let storage = Storage::new(args.name);

    // build our application with a single route
    let app = inject_tracing_layer(create_worker_router(storage));

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", args.port))
        .await
        .unwrap();
    axum::serve(listener, app).await.unwrap();
}
