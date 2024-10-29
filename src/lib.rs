pub mod master;
pub mod metadata;
mod schema;
pub mod tracing;
pub mod web;
pub mod worker;

pub mod health {
    tonic::include_proto!("healthcheck"); // The string specified here must match the proto package name
}
