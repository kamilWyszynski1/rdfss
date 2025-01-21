use crate::master::node_client::NodeClient;
use crate::master::router::MasterAppError::FileNotFound;
use crate::metadata::models;
use crate::metadata::models::{
    ChunkUpdate, ChunkWithWeb, ChunkWithWebQueryBuilder, File, FileUpdate, FilesQueryBuilder,
};
use crate::metadata::sql::MetadataStorage;
use crate::web::AppError;
use anyhow::{anyhow, bail, Context};
use async_stream::stream;
use axum::body::{Body, BodyDataStream};
use axum::extract::{Path, Request, State};
use axum::http::HeaderMap;
use axum::routing::{delete, get, post};
use axum::Router;
use futures::TryStreamExt;
use rand::seq::IteratorRandom;
use reed_solomon_erasure::galois_8::ReedSolomon;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use thiserror::Error;
use tokio::io::AsyncReadExt;
use tokio_util::io::StreamReader;

#[derive(Error, Debug)]
pub(crate) enum MasterAppError {
    #[error("file already exists")]
    FileExists,

    #[error("invalid content-length")]
    InvalidContentLength,

    #[error("file not found")]
    FileNotFound,
}

const CHUNK_SIZE: usize = 1 * 1024;
const PARITY_SHARDS_AMOUNT: usize = 2;

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
        .route(
            "/file/correction/:file_name",
            get(get_file_error_correction),
        )
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

async fn get_file_error_correction(
    State(handler): State<MasterAppState>,
    Path(file_name): Path<String>,
) -> Result<axum::body::Body, AppError> {
    Ok(get_from_nodes_error_correction(handler, file_name).await?)
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
                .parity_shard(false)
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

/// It queries real chunks and parity shard locations and fetches them all, simulates lost data chunk
/// and reconstructs the data using parity shards.
/// It should be implemented as a part of get_from_nodes function,
/// but it's good enough to showcase capability like this.
async fn get_from_nodes_error_correction(
    mut handler: MasterAppState,
    file_name: String,
) -> anyhow::Result<axum::body::Body> {
    let chunks_with_webs = handler
        .metadata
        .get_chunks_with_web(
            &ChunkWithWebQueryBuilder::default()
                .file_name(&file_name)
                .node_active(true)
                .build()?,
        )
        .await?;

    if chunks_with_webs.is_empty() {
        bail!(FileNotFound)
    }

    let chunks = deduplicate_chunks_for_reconstruct(chunks_with_webs);
    let len = chunks.len();

    let mut shards = Vec::with_capacity(len);
    dbg!(&chunks);
    for fi in chunks {
        let chunk = handler.node_client.get_chunk(&fi.chunk_id, &fi.web).await?;
        println!("read {:?}", chunk[..2].to_vec());
        dbg!(fi.chunk_index, chunk.len(), fi.parity_shard);
        shards.push(Some(chunk));
    }

    let mut rollback_fillings = HashMap::new();
    for i in 0..len - 2 {
        if let Some(shard) = &mut shards[i] {
            if shard.len() < CHUNK_SIZE {
                let shard_len = shard.len();
                fill_buffer_with_padding(shard);
                rollback_fillings.insert(i, shard_len);
            }
        }
    }

    shards[2] = None;
    shards[1] = None; // simulate lost shard
    let r = ReedSolomon::new(len - PARITY_SHARDS_AMOUNT, PARITY_SHARDS_AMOUNT)?;
    r.reconstruct_data(&mut shards)?;

    let mut data = Vec::new();
    for i in 0..len - 2 {
        let mut sh = shards[i].to_owned().unwrap();
        if let Some(rollback_len) = rollback_fillings.get(&i) {
            sh = sh[..*rollback_len].to_owned();
        }
        data.append(&mut sh);
    }

    Ok(Body::from(data))
}

fn deduplicate_chunks_for_reconstruct(chunks: Vec<ChunkWithWeb>) -> Vec<ChunkWithWeb> {
    dbg!(&chunks);
    let mut res = vec![];
    let mut deduplicated = HashMap::new();

    chunks.into_iter().for_each(|chunk| {
        if chunk.parity_shard {
            res.push(chunk);
        } else {
            deduplicated.insert(chunk.chunk_index, chunk);
        }
    });

    deduplicated
        .into_values()
        .into_iter()
        .for_each(|ch| res.push(ch));

    res.sort_by(|a, b| {
        if a.parity_shard && b.parity_shard {
            b.chunk_index.cmp(&a.chunk_index)
        } else if a.parity_shard {
            Ordering::Less
        } else if b.parity_shard {
            Ordering::Less
        } else {
            a.chunk_index.cmp(&b.chunk_index)
        }
    });
    res
}

// Handler that streams the request body to a file.
//
// POST'ing to `/file/foo.txt` creates a file called `foo.txt`.
async fn upload_file(
    State(state): State<MasterAppState>,
    headers: HeaderMap,
    Path(file_name): Path<String>,
    request: Request,
) -> Result<(), AppError> {
    let content_length = headers
        .get("content-length")
        .context("missing content-length header")?
        .to_str()?
        .parse::<usize>()?;
    if content_length == 0 {
        return Err(AppError::from(anyhow!(
            MasterAppError::InvalidContentLength
        )));
    }
    stream_to_nodes(
        state,
        content_length,
        &file_name,
        request.into_body().into_data_stream(),
    )
    .await?;
    Ok(())
}

// Save a `Stream` to a file
async fn stream_to_nodes(
    mut handler: MasterAppState,
    content_length: usize,
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

        let mut reader = body_reader.take(CHUNK_SIZE as u64);
        let mut chunks: Vec<models::Chunk> = Vec::new();
        let mut chunk_locations: Vec<models::ChunkLocation> = Vec::new();

        let mut chunk_index = 0;

        let file_id = uuid::Uuid::new_v4().to_string();
        let file = File::new(file_id.clone(), filename.to_string());

        let data_shards = (content_length / CHUNK_SIZE) + 1;
        let r = ReedSolomon::new(data_shards, PARITY_SHARDS_AMOUNT).unwrap();
        let mut parities = [[0; CHUNK_SIZE]; 2];

        // pick random node from active ones
        let nodes = handler.metadata.get_nodes(Some(true)).await?;
        let pick_random_node = || {
            nodes
                .iter()
                .choose(&mut rand::thread_rng())
                .context("no nodes")
        };

        loop {
            let mut buffer = vec![0; CHUNK_SIZE];
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

            println!("write {:?}", buffer[..2].to_vec());

            {
                let buffer_len = buffer.len();
                fill_buffer_with_padding(&mut buffer);
                r.encode_single_sep(chunk_index, &buffer, &mut parities)
                    .with_context(|| format!("could not encode {chunk_index} chunk"))?;
                buffer = buffer[..buffer_len].to_owned();
            }

            let node = pick_random_node()?;
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
                chunk_index = chunk_index,
                "file chunk sent"
            );

            chunks.push(models::Chunk::new(
                chunk_id.clone(),
                file_id.clone(),
                chunk_index as i32,
            ));
            chunk_locations.push(models::ChunkLocation {
                chunk_id,
                node_id: node.id.to_string(),
            });

            // the reader is now exhausted, but that doesn't mean the underlying reader
            // ism recover it, and create the next chunked reader.
            reader = reader.into_inner().take(CHUNK_SIZE as u64);
            chunk_index += 1;
        }

        let mut parity_index = -1;
        for parity in &mut parities {
            println!("write {:?}", parity[..2].to_vec());
            let node = pick_random_node()?;
            let chunk_parity_shard_id = uuid::Uuid::new_v4().to_string();

            handler
                .node_client
                .send_chunk(&chunk_parity_shard_id, &node.web, parity.to_vec())
                .await
                .context(format!("could not send chunk to {} node", node.id))?;

            chunks.push(
                models::Chunk::new(chunk_parity_shard_id.clone(), file_id.clone(), parity_index)
                    .with_parity_shard(),
            );
            chunk_locations.push(models::ChunkLocation {
                chunk_id: chunk_parity_shard_id,
                node_id: node.id.to_string(),
            });
            parity_index -= 1;
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

fn fill_buffer_with_padding(buffer: &mut Vec<u8>) {
    while buffer.len() < CHUNK_SIZE {
        buffer.push(0u8);
    }
}
