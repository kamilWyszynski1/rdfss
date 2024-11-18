use crate::master::node_client::NodeClient;
use crate::metadata::models;
use crate::metadata::sql::MetadataStorage;
use crate::web::AppError;
use anyhow::{bail, Context};
use async_stream::stream;
use axum::body::{Body, BodyDataStream};
use axum::extract::{Path, Request, State};
use axum::routing::{delete, get, post};
use axum::Router;
use futures::TryStreamExt;
use rand::seq::IteratorRandom;
use std::io;
use thiserror::Error;
use tokio::io::AsyncReadExt;
use tokio_util::io::StreamReader;

#[derive(Error, Debug)]
pub(crate) enum MasterAppError {
    #[error("file already exists")]
    FileExists,
}

const CHUNK_SIZE: u64 = 1 * 1024;

#[derive(Clone)]
pub struct MasterAppState {
    node_client: NodeClient,
    metadata: MetadataStorage,
}

impl MasterAppState {
    pub fn new(node_client: NodeClient, metadata: MetadataStorage) -> Self {
        Self {
            node_client,
            metadata,
        }
    }
}

pub fn create_master_router(state: MasterAppState) -> Router {
    let paths = Router::new()
        .route("/", get(|| async { "Hello, World!" }))
        .route("/upload/:path", post(upload_file))
        .route("/file/:file_name", get(get_file))
        .route("/file/:file_name", delete(delete_file))
        .with_state(state);
    Router::new().nest("/v1", paths)
}

async fn get_file(
    State(handler): State<MasterAppState>,
    Path(file_name): Path<String>,
) -> Result<axum::body::Body, AppError> {
    Ok(get_from_nodes(handler, file_name).await?)
}

async fn get_from_nodes(
    mut handler: MasterAppState,
    file_name: String,
) -> anyhow::Result<axum::body::Body> {
    let file_info = handler.metadata.get_chunks_with_web(&file_name).await?;

    tracing::debug!(chunks = format!("{:?}", file_info), "found file chunks");

    let s = stream! {
        for fi in file_info {
            match handler.node_client.get_chunk(&fi.chunk_id, &fi.web).await {
                Ok(data) => {yield Ok(data)}
                Err(err) => {yield Err(err)}
            }
        }
    };

    Ok(Body::from_stream(s))
}

// Handler that streams the request body to a file.
//
// POST'ing to `/file/foo.txt` will create a file called `foo.txt`.
async fn upload_file(
    State(state): State<MasterAppState>,
    Path(file_name): Path<String>,
    request: Request,
) -> Result<(), AppError> {
    stream_to_nodes(state, &file_name, request.into_body().into_data_stream()).await?;
    Ok(())
}

// Save a `Stream` to a file
async fn stream_to_nodes(
    mut handler: MasterAppState,
    filename: &str,
    stream: BodyDataStream,
) -> anyhow::Result<()> {
    if handler.metadata.file_exists(filename).await? {
        bail!(MasterAppError::FileExists);
    }

    async {
        // Convert the stream into an `AsyncRead`.
        let body_with_io_error = stream.map_err(|err| io::Error::new(io::ErrorKind::Other, err));

        let body_reader = StreamReader::new(body_with_io_error);

        // Calling next() on a stream requires the stream to be pinned.
        tokio::pin!(body_reader);

        let mut reader = body_reader.take(CHUNK_SIZE);
        let mut chunks: Vec<models::Chunk> = Vec::new();
        let mut chunk_locations: Vec<models::ChunkLocation> = Vec::new();

        loop {
            let mut buffer = vec![0; CHUNK_SIZE as usize];
            match reader.read(&mut buffer).await {
                Ok(n) => {
                    if n == 0 {
                        break;
                    }

                    // trim buffer because we could read less than CHUNK_SIZE bytes
                    buffer = buffer[..n].to_owned();
                }
                Err(err) => bail!(err),
            };

            if buffer.is_empty() {
                break;
            }

            // pick random node from active ones
            let nodes = handler.metadata.get_nodes(Some(true)).await?;
            let node = nodes
                .iter()
                .choose(&mut rand::thread_rng())
                .context("no nodes")?;

            let chunk_id = uuid::Uuid::new_v4().to_string();
            handler
                .node_client
                .send_chunk(&chunk_id, node, buffer)
                .await
                .context(format!("could not send chunk to {} node", node.id))?;
            tracing::info!(file = chunk_id, node = node.id, "file chunk sent");

            chunks.push(models::Chunk {
                filename: filename.to_string(),
                // node_id: node.id.to_string(),
                id: chunk_id.clone(),
            });
            chunk_locations.push(models::ChunkLocation {
                chunk_id,
                node_id: node.id.to_string(),
            });

            // our reader is now exhausted, but that doesn't mean the underlying reader
            // is. So we recover it, and we create the next chunked reader.
            reader = reader.into_inner().take(CHUNK_SIZE);
        }

        tracing::debug!(chunks = format!("{:?}", chunks), "saving chunks info");
        handler.metadata.save_chunks(chunks).await?;
        handler
            .metadata
            .save_chunk_locations(chunk_locations)
            .await?;

        anyhow::Ok(())
    }
    .await?;

    Ok(())
}

async fn delete_file(
    State(handler): State<MasterAppState>,
    Path(file_name): Path<String>,
) -> Result<axum::http::StatusCode, AppError> {
    delete_from_nodes(handler, file_name).await?;
    Ok(axum::http::StatusCode::NO_CONTENT)
}

async fn delete_from_nodes(mut handler: MasterAppState, file_name: String) -> anyhow::Result<()> {
    let chunks = handler.metadata.get_chunks_with_web(&file_name).await?;
    if chunks.is_empty() {
        return Ok(());
    }

    // TODO: refactor to use outbox pattern
    for ch in chunks {
        handler
            .node_client
            .delete_chunk(&ch.chunk_id, &ch.web)
            .await
            .context("could not delete chunk from node")?;
        handler
            .metadata
            .delete_chunk(&ch.chunk_id)
            .await
            .context("could not delete chunk from the metadata db")?;
    }
    Ok(())
}
