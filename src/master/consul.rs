use consulrs::api::service::common::{AgentService, AgentServiceChecksInfo};
use consulrs::api::ApiResponse;
use consulrs::client::ConsulClient;
use consulrs::error::ClientError;
use consulrs::service;
#[cfg(test)]
use mockall::automock;
use std::collections::HashMap;

#[cfg_attr(test, automock)]
#[tonic::async_trait]
pub trait Consul {
    async fn list(&self) -> anyhow::Result<ApiResponse<HashMap<String, AgentService>>>;
    async fn health(
        &self,
        name: &str,
    ) -> Result<ApiResponse<Vec<AgentServiceChecksInfo>>, ClientError>;
    async fn deregister(&self, name: &str) -> anyhow::Result<()>;
}

pub struct Wrap {
    client: ConsulClient,
}

impl Wrap {
    pub fn new(client: ConsulClient) -> Self {
        Wrap { client }
    }
}

#[tonic::async_trait]
impl Consul for Wrap {
    async fn list(&self) -> anyhow::Result<ApiResponse<HashMap<String, AgentService>>> {
        Ok(service::list(&self.client, None).await?)
    }

    async fn health(
        &self,
        name: &str,
    ) -> Result<ApiResponse<Vec<AgentServiceChecksInfo>>, ClientError> {
        Ok(service::health(&self.client, name, None).await?)
    }

    async fn deregister(&self, name: &str) -> anyhow::Result<()> {
        service::deregister(&self.client, name, None).await?;
        Ok(())
    }
}
