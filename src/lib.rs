pub mod master;
pub mod metadata;
mod schema;
pub mod tracing;
pub mod web;
pub mod worker;

pub mod health {
    tonic::include_proto!("grpc.health.v1"); // The string specified here must match the proto package name
}
