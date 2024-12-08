use crate::worker::router::ReplicationOrder;
use anyhow::{bail, Context};
#[cfg(test)]
use mockall::automock;

#[cfg_attr(test, automock)]
#[tonic::async_trait]
pub trait NodeClient {
    async fn send_chunk(&self, name: &str, node_web: &str, data: Vec<u8>) -> anyhow::Result<()>;
    async fn get_chunk(&self, name: &str, node_url: &str) -> anyhow::Result<Vec<u8>>;
    async fn delete_chunk(&self, name: &str, node_url: &str) -> anyhow::Result<()>;
    async fn replicate(
        &self,
        node_url: &str,
        chunk_id: &str,
        order: ReplicationOrder,
    ) -> anyhow::Result<()>;
}

#[derive(Clone)]
pub struct HTTPNodeClient {
    client: reqwest::Client,
}

impl HTTPNodeClient {
    pub fn new(client: reqwest::Client) -> Self {
        Self { client }
    }
}

#[tonic::async_trait]
impl NodeClient for HTTPNodeClient {
    async fn send_chunk(&self, name: &str, node_web: &str, data: Vec<u8>) -> anyhow::Result<()> {
        self.client
            .post(format!("{}/{}/{}", node_web, "v1/file-chunk", name))
            .body(data)
            .send()
            .await
            .context("failed post")?;

        Ok(())
    }

    async fn get_chunk(&self, name: &str, node_url: &str) -> anyhow::Result<Vec<u8>> {
        let response = self
            .client
            .get(format!("{}/{}/{}", node_url, "v1/file-chunk", name))
            .send()
            .await
            .context("failed get")?;
        Ok(response.bytes().await?.to_vec())
    }

    async fn delete_chunk(&self, name: &str, node_url: &str) -> anyhow::Result<()> {
        let response = self
            .client
            .delete(format!("{}/{}/{}", node_url, "v1/file-chunk", name))
            .send()
            .await
            .context("failed get")?;
        if response.status() != reqwest::StatusCode::NO_CONTENT {
            bail!(format!("invalid delete response {}", response.status()))
        }
        Ok(())
    }

    async fn replicate(
        &self,
        node_url: &str,
        chunk_id: &str,
        order: ReplicationOrder,
    ) -> anyhow::Result<()> {
        let response = self
            .client
            .post(format!(
                "{}/{}/{}/replicate",
                node_url, "v1/file-chunk", chunk_id
            ))
            .json(&order)
            .send()
            .await
            .context("failed post")?;
        if response.status() != reqwest::StatusCode::NO_CONTENT {
            bail!(format!("invalid replicate response {}", response.status()))
        }
        Ok(())
    }
}
