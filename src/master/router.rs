use crate::master::node_client::{HTTPNodeClient, NodeClient};
use crate::metadata::models;
use crate::metadata::models::{
    ChunkUpdate, ChunkWithWebQueryBuilder, File, FileUpdate, FilesQueryBuilder,
};
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
use std::sync::Arc;
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
    node_client: Arc<dyn NodeClient + Send + Sync>,
    metadata: MetadataStorage,
}

impl MasterAppState {
    pub fn new(node_client: Arc<dyn NodeClient + Send + Sync>, metadata: MetadataStorage) -> Self {
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
    let file_info = handler
        .metadata
        .get_chunks_with_web(
            &ChunkWithWebQueryBuilder::default()
                .file_name(&file_name)
                .node_active(true)
                .build()?,
        )
        .await?;

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

        let mut chunk_index = 0;

        let file_id = uuid::Uuid::new_v4().to_string();
        let file = File::new(file_id.clone(), filename.to_string());

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
                .send_chunk(&chunk_id, &node.web, buffer)
                .await
                .context(format!("could not send chunk to {} node", node.id))?;
            tracing::info!(
                file = filename,
                chunk_id = chunk_id,
                node = node.id,
                "file chunk sent"
            );

            chunks.push(models::Chunk::new(
                chunk_id.clone(),
                file_id.clone(),
                chunk_index,
            ));
            chunk_locations.push(models::ChunkLocation {
                chunk_id,
                node_id: node.id.to_string(),
            });

            // our reader is now exhausted, but that doesn't mean the underlying reader
            // is. So we recover it, and we create the next chunked reader.
            reader = reader.into_inner().take(CHUNK_SIZE);
            chunk_index += 1;
        }

        tracing::debug!(chunks = format!("{:?}", chunks), "saving chunks info");
        handler
            .metadata
            .tx(|uow| {
                // TODO: use outbox pattern and sync with sent files
                uow.save_file(&file)?;
                uow.save_chunks(chunks)?;
                uow.save_chunk_locations(chunk_locations)?;
                Ok(())
            })
            .await
            .context("could not save file, chunks and locations")?;

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
    let chunks = handler
        .metadata
        .get_chunks_with_web(
            &ChunkWithWebQueryBuilder::default()
                .file_name(&file_name)
                .node_active(true)
                .build()?,
        )
        .await?;
    if chunks.is_empty() {
        return Ok(());
    }

    let file = handler
        .metadata
        .get_file(&FilesQueryBuilder::default().name(&file_name).build()?)
        .await?;

    handler
        .metadata
        .tx(|uow| {
            for ch in chunks {
                uow.update_chunk(
                    &ch.chunk_id,
                    ChunkUpdate {
                        to_delete: Some(true),
                    },
                )
                .context("could not mark chunk to delete")?;
            }

            uow.update_file(
                &file.id,
                &FileUpdate {
                    to_delete: Some(true),
                },
            )
            .context("could not mark file to delete")?;
            Ok(())
        })
        .await?;
    Ok(())
}
