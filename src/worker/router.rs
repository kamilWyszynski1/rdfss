use crate::master::node_client::{HTTPNodeClient, NodeClient};
use crate::web::AppError;
use crate::worker::storage::Storage;
use anyhow::Context;
use axum::body::Body;
use axum::extract::{Path, Request, State};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use futures::TryStreamExt;
use std::io;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio_util::io::{ReaderStream, StreamReader};

#[derive(Clone)]
struct WorkerState {
    storage: Storage,
    node_client: Arc<dyn NodeClient + Send + Sync>,
}

impl WorkerState {
    pub fn new(storage: Storage, node_client: Arc<dyn NodeClient + Send + Sync>) -> Self {
        Self {
            storage,
            node_client,
        }
    }
}

pub fn create_worker_router(
    storage: Storage,
    node_client: Arc<dyn NodeClient + Send + Sync>,
) -> Router {
    let state = WorkerState::new(storage, node_client);
    let paths = Router::new()
        .route("/file-chunk/:chunk", post(save_request_body_sync))
        .route("/file-chunk/:chunk", get(get_chunk))
        .route("/file-chunk/:chunk", delete(delete_chunk))
        .route("/file-chunk/:chunk/replicate", post(replicate_chunk))
        .with_state(state);
    Router::new().nest("/v1", paths)
}

async fn save_request_body_sync(
    State(state): State<WorkerState>,
    Path(file_name): Path<String>,
    request: Request,
) -> Result<(), AppError> {
    save_chunk(state, file_name, request).await?;
    Ok(())
}

async fn save_chunk(state: WorkerState, file_name: String, request: Request) -> anyhow::Result<()> {
    async {
        let stream = request.into_body().into_data_stream();
        let body_with_io_error = stream.map_err(|err| io::Error::new(io::ErrorKind::Other, err));
        let body_reader = StreamReader::new(body_with_io_error);

        futures::pin_mut!(body_reader);
        state
            .storage
            .save(&mut body_reader, &file_name)
            .await
            .context("could not save chunk")?;
        anyhow::Ok(())
    }
    .await?;

    Ok(())
}

async fn get_chunk(
    State(state): State<WorkerState>,
    Path(file_name): Path<String>,
) -> Result<axum::body::Body, AppError> {
    Ok(get_saved_chunk(state, file_name).await?)
}

async fn get_saved_chunk(
    state: WorkerState,
    file_name: String,
) -> anyhow::Result<axum::body::Body> {
    let reader = state.storage.get(&file_name).await?;
    let stream = ReaderStream::new(reader);
    Ok(Body::from_stream(stream))
}

async fn delete_chunk(
    State(state): State<WorkerState>,
    Path(chunk_name): Path<String>,
) -> Result<axum::http::StatusCode, AppError> {
    delete_saved_chunk(state, chunk_name).await?;
    Ok(axum::http::StatusCode::NO_CONTENT)
}

async fn delete_saved_chunk(state: WorkerState, chunk_name: String) -> anyhow::Result<()> {
    state.storage.delete(&chunk_name).await?;
    Ok(())
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Order {
    pub new_chunk_id: String,
    pub node_web: String,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct ReplicationOrder {
    pub orders: Vec<Order>, // what nodes we should replicate to
}

async fn replicate_chunk(
    State(state): State<WorkerState>,
    Path(chunk_name): Path<String>,
    Json(order): Json<ReplicationOrder>,
) -> Result<axum::http::StatusCode, AppError> {
    replicate_chunk_to_nodes(state, chunk_name, order).await?;
    Ok(axum::http::StatusCode::NO_CONTENT)
}

async fn replicate_chunk_to_nodes(
    state: WorkerState,
    chunk_name: String,
    order: ReplicationOrder,
) -> anyhow::Result<()> {
    let mut chunk_reader = state.storage.get(&chunk_name).await?;
    let mut buffer = Vec::new();
    chunk_reader.read_to_end(&mut buffer).await?;

    // spawn async task and return success immediately
    tokio::spawn(async move {
        for Order {
            new_chunk_id,
            node_web,
        } in order.orders
        {
            if let Err(err) = state
                .node_client
                .send_chunk(&new_chunk_id, &node_web, buffer.clone())
                .await
            {
                tracing::error!(err = ?err, "replicate_chunk_to_nodes: could not send chunk to {} node", node_web);
            }
        }
    });

    Ok(())
}
