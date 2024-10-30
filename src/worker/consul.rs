use consulrs::api::check::common::AgentServiceCheckBuilder;
use consulrs::api::service::requests::RegisterServiceRequest;
use consulrs::client::{ConsulClient, ConsulClientSettingsBuilder};
use consulrs::service;
use std::collections::HashMap;

pub async fn register_worker(
    node_id: &str,
    address: &str,
    port: u64,
    rpc: &str,
) -> anyhow::Result<()> {
    let client = ConsulClient::new(
        ConsulClientSettingsBuilder::default()
            .address("http://0.0.0.0:8500")
            .build()?,
    )?;
    service::register(
        &client,
        node_id,
        Some(
            RegisterServiceRequest::builder()
                .address(address)
                .port(port)
                .meta(HashMap::from([("rpc".to_string(), rpc.to_string())]))
                .check(
                    AgentServiceCheckBuilder::default()
                        .name("health_check")
                        .interval("10s")
                        .grpc(format!("{}/healthcheck.Healthchecker", rpc))
                        .status("passing")
                        .build()?,
                ),
        ),
    )
    .await?;
    Ok(())
}
