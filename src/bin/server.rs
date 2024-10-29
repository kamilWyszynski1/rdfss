use rdfss::client::client::NodeClient;
use rdfss::master::brain::Brain;
use rdfss::tracing::init::{init_tracing, inject_tracing_layer};
use rdfss::web::master_router::create_master_router;

#[tokio::main]
async fn main() {
    init_tracing();

    let workers = vec!["localhost:3001".to_string(), "localhost:3002".to_string()];
    let client = NodeClient::new(workers, reqwest::Client::new());

    let redis_client = redis::Client::open("redis://127.0.0.1/")
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();

    let brain = Brain::new(redis_client);
    // build our application with a single route
    let app = inject_tracing_layer(create_master_router(client, brain));

    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
