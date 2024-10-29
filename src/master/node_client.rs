use crate::web::WorkerNameURL;
use anyhow::{bail, Context};
use rand::seq::IndexedRandom;

#[derive(Clone)]
pub struct NodeClient {
    urls: Vec<WorkerNameURL>,
    client: reqwest::Client,
}

impl NodeClient {
    pub fn new(urls: Vec<WorkerNameURL>, client: reqwest::Client) -> Self {
        Self { urls, client }
    }

    pub async fn send_chunk(&self, name: &str, data: Vec<u8>) -> anyhow::Result<WorkerNameURL> {
        // pick random url
        let name_url = self
            .urls
            .choose(&mut rand::thread_rng())
            .context("no url")?;

        self.client
            .post(format!("{}/{}/{}", name_url.1, "v1/file-chunk", name))
            .body(data)
            .send()
            .await
            .context("failed post")?;

        Ok(name_url.clone())
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
