use crate::health;
use crate::health::health_check_response::ServingStatus;
use crate::health::{HealthCheckRequest, HealthCheckResponse};
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

/// Implements gRPC Healtchecker service.
#[derive(Debug, Default)]
pub struct WorkerHealth {}

#[tonic::async_trait]
impl health::health_server::Health for WorkerHealth {
    async fn check(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        tracing::debug!("health check");
        Ok(Response::new(HealthCheckResponse {
            status: ServingStatus::Serving.into(),
        }))
    }

    type WatchStream = ReceiverStream<Result<HealthCheckResponse, Status>>;

    async fn watch(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        todo!()
    }
}
