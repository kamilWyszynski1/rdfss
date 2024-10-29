use crate::web::AppError;
use crate::worker::storage::Storage;
use anyhow::Context;
use axum::body::Body;
use axum::extract::{Path, Request, State};
use axum::routing::{get, post};
use axum::Router;
use futures::TryStreamExt;
use std::io;
use tokio_util::io::{ReaderStream, StreamReader};

#[derive(Clone)]
struct WorkerState {
    storage: Storage,
}

impl WorkerState {
    pub fn new(storage: Storage) -> Self {
        Self { storage }
    }
}

pub fn create_worker_router(storage: Storage) -> Router {
    let state = WorkerState::new(storage);
    let paths = Router::new()
        .route("/file-chunk/:chunk", post(save_request_body_sync))
        .route("/file-chunk/:chunk", get(get_chunk))
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
