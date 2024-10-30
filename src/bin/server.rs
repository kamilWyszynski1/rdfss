#![feature(duration_constructors)]

use consulrs::client::{ConsulClient, ConsulClientSettingsBuilder};
use diesel::{Connection, SqliteConnection};
use dotenvy::dotenv;
use rdfss::health;
use rdfss::master::master::Master;
use rdfss::master::node_client::NodeClient;
use rdfss::master::router::{create_master_router, MasterAppState};
use rdfss::metadata::sql::MetadataStorage;
use rdfss::tracing::init::{init_tracing, inject_tracing_layer};
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::{broadcast, Mutex};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    init_tracing();

    let sqlite_conn = establish_connection();
    let mut metadata = MetadataStorage::new(sqlite_conn.clone());
    let nodes = metadata.get_nodes(Some(true)).await?;

    let https: Vec<(String, String)> = nodes
        .clone()
        .into_iter()
        .clone()
        .map(|n| (n.id, n.web))
        .collect();
    let rpcs: Vec<(String, String)> = nodes.into_iter().map(|n| (n.id, n.rpc)).collect();

    let client = NodeClient::new(https, reqwest::Client::new());

    let state = MasterAppState::new(client, metadata.clone());

    let mut clients = vec![];
    for (node_id, url) in rpcs {
        clients.push((
            node_id,
            health::health_client::HealthClient::connect(url).await?,
        ));
    }

    let cancel_token = CancellationToken::new();

    // let healthcheck = MasterHealthChecker::new(clients, metadata.clone());
    // healthcheck
    //     .run(Duration::from_secs(5), cancel_token.clone())
    //     .await;

    let consul_client = ConsulClient::new(
        ConsulClientSettingsBuilder::default()
            .address("http://0.0.0.0:8500")
            .build()?,
    )?;

    let (s, r) = broadcast::channel(8);

    let master = Master::new(s, metadata, tokio::time::Duration::from_mins(1));
    master
        .run(Duration::from_secs(5), consul_client, cancel_token.clone())
        .await;

    // build our application with a single route
    let app = inject_tracing_layer(create_master_router(state));

    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(cancel_token))
        .await?;

    Ok(())
}

async fn shutdown_signal(cancel_token: CancellationToken) {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    cancel_token.cancel() // let other processes know to shut down
}

pub fn establish_connection() -> Arc<Mutex<SqliteConnection>> {
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    Arc::new(Mutex::new(
        SqliteConnection::establish(&database_url)
            .unwrap_or_else(|_| panic!("Error connecting to {}", database_url)),
    ))
}
