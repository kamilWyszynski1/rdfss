use crate::metadata::models::Node;
use anyhow::{bail, Context};

#[derive(Clone)]
pub struct NodeClient {
    client: reqwest::Client,
}

impl NodeClient {
    pub fn new(client: reqwest::Client) -> Self {
        Self { client }
    }

    pub async fn send_chunk(&self, name: &str, node: &Node, data: Vec<u8>) -> anyhow::Result<()> {
        self.client
            .post(format!("{}/{}/{}", node.web, "v1/file-chunk", name))
            .body(data)
            .send()
            .await
            .context("failed post")?;

        Ok(())
    }

    pub async fn get_chunk(&self, name: &str, node_url: &str) -> anyhow::Result<Vec<u8>> {
        let response = self
            .client
            .get(format!("{}/{}/{}", node_url, "v1/file-chunk", name))
            .send()
            .await
            .context("failed get")?;
        Ok(response.bytes().await?.to_vec())
    }

    pub async fn delete_chunk(&self, name: &str, node_url: &str) -> anyhow::Result<()> {
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
}
