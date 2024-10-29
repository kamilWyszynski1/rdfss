use crate::client::client::NodeClient;
use crate::master::brain::Brain;
use crate::worker::storage::Storage;
use anyhow::{bail, Context};
use async_stream::stream;
use axum::body::{Body, BodyDataStream};
use axum::extract::{Path, Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use futures::TryStreamExt;
use std::collections::HashMap;
use std::io;
use tokio::io::{AsyncBufReadExt, AsyncReadExt};
use tokio_util::io::{ReaderStream, StreamReader};

const CHUNK_SIZE: u64 = 1 * 1024;

#[derive(Clone)]
struct MasterAppState {
    node_client: NodeClient,
    brain: Brain,
}

pub fn create_master_router(client: NodeClient, brain: Brain) -> Router {
    let paths = Router::new()
        .route("/", get(|| async { "Hello, World!" }))
        .route("/upload/:path", post(save_request_body))
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
    let mut chunks: Vec<(String, String)> = file_info.into_iter().collect();
    chunks.sort_by(|a, b| a.0.cmp(&b.0));

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
async fn save_request_body(
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
    async {
        // Convert the stream into an `AsyncRead`.
        let body_with_io_error = stream.map_err(|err| io::Error::new(io::ErrorKind::Other, err));

        let body_reader = StreamReader::new(body_with_io_error);

        // Calling next() on a stream requires the stream to be pinned.
        tokio::pin!(body_reader);

        let mut reader = body_reader.take(CHUNK_SIZE);
        let mut i = 0;
        let mut chunks = HashMap::new();
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

            let name = format!("{i}_{path}");
            let node = handler
                .node_client
                .send_chunk(&name, buffer)
                .await
                .context("could not send chunk")?;
            tracing::info!(file = name, node = node, "file chunk sent");

            chunks.insert(name, node);

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

pub fn create_worker_router(storage: Storage) -> Router {
    let paths = Router::new()
        .route("/file-chunk/:chunk", post(save_request_body_sync))
        .route("/file-chunk/:chunk", get(get_chunk))
        .with_state(storage);
    Router::new().nest("/v1", paths)
}

async fn save_request_body_sync(
    State(storage): State<Storage>,
    Path(file_name): Path<String>,
    request: Request,
) -> Result<(), AppError> {
    save_chunk(storage, file_name, request).await?;
    Ok(())
}

async fn save_chunk(storage: Storage, file_name: String, request: Request) -> anyhow::Result<()> {
    async {
        let stream = request.into_body().into_data_stream();
        let body_with_io_error = stream.map_err(|err| io::Error::new(io::ErrorKind::Other, err));
        let body_reader = StreamReader::new(body_with_io_error);

        futures::pin_mut!(body_reader);
        storage
            .save(&mut body_reader, &file_name)
            .await
            .context("could not save chunk")?;
        anyhow::Ok(())
    }
    .await?;

    Ok(())
}

async fn get_chunk(
    State(storage): State<Storage>,
    Path(file_name): Path<String>,
) -> Result<axum::body::Body, AppError> {
    Ok(get_saved_chunk(storage, file_name).await?)
}

async fn get_saved_chunk(storage: Storage, file_name: String) -> anyhow::Result<axum::body::Body> {
    let reader = storage.get(&file_name).await?;
    let stream = ReaderStream::new(reader);
    Ok(Body::from_stream(stream))
}

// Make our own error that wraps `anyhow::Error`.
struct AppError(anyhow::Error);

// Tell axum how to convert `AppError` into a response.
impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        tracing::error!(err = format!("{:#}", self.0), "error occurred");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Something went wrong: {}", self.0),
        )
            .into_response()
    }
}

// This enables using `?` on functions that return `Result<_, anyhow::Error>` to turn them into
// `Result<_, AppError>`. That way you don't need to do that manually.
impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}
