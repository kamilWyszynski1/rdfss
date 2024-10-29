use crate::client::client::NodeClient;
use crate::master::brain::Brain;
use crate::web::AppError;
use anyhow::{bail, Context};
use async_stream::stream;
use axum::body::{Body, BodyDataStream};
use axum::extract::{Path, Request, State};
use axum::routing::{get, post};
use axum::Router;
use futures::TryStreamExt;
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
struct MasterAppState {
    node_client: NodeClient,
    brain: Brain,
}

pub fn create_master_router(client: NodeClient, brain: Brain) -> Router {
    let paths = Router::new()
        .route("/", get(|| async { "Hello, World!" }))
        .route("/upload/:path", post(upload_file))
        .route("/file/:file_name", get(get_file))
        .with_state(MasterAppState {
            node_client: client,
            brain,
        });
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
    let file_info = handler.brain.get_file_info(&file_name).await?;
    let chunks: Vec<(String, String)> = file_info.into_iter().collect();

    tracing::debug!(chunks = format!("{:?}", chunks), "found file chunks");

    let s = stream! {
        for (chunk, node_url) in chunks {
            match handler.node_client.get_chunk(&chunk, &node_url).await {
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
    path: &str,
    stream: BodyDataStream,
) -> anyhow::Result<()> {
    if handler.brain.file_exists(path).await? {
        bail!(MasterAppError::FileExists);
    }

    async {
        // Convert the stream into an `AsyncRead`.
        let body_with_io_error = stream.map_err(|err| io::Error::new(io::ErrorKind::Other, err));

        let body_reader = StreamReader::new(body_with_io_error);

        // Calling next() on a stream requires the stream to be pinned.
        tokio::pin!(body_reader);

        let mut reader = body_reader.take(CHUNK_SIZE);
        let mut i = 0;
        let mut chunks = Vec::new();
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

            let name = uuid::Uuid::new_v4().to_string();
            let node = handler
                .node_client
                .send_chunk(&name, buffer)
                .await
                .context("could not send chunk")?;
            tracing::info!(file = name, node = node, "file chunk sent");

            chunks.push((name, node));

            // our reader is now exhausted, but that doesn't mean the underlying reader
            // is. So we recover it, and we create the next chunked reader.
            reader = reader.into_inner().take(CHUNK_SIZE);
            i += 1;
        }

        tracing::debug!(chunks = format!("{:?}", chunks), "saving chunks info");
        handler.brain.save_file_info(path, chunks).await?;
        anyhow::Ok(())
    }
    .await?;

    Ok(())
}
