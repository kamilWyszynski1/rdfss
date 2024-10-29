// pub async fn send_buf<R>(reader: &mut R) -> io::Result<()>
// where
//     R: AsyncRead + Unpin + Sized + Send + 'static,
// {
//     let c = reqwest::Client::new();
//     c.post("http://localhost:8080/send")
//         .body(Body::wrap_stream(reader))
//         .send()
//         .await
//         .unwrap();
//
//     Ok(())
// }

use anyhow::Context;
use rand::seq::IndexedRandom;

#[derive(Clone)]
pub struct NodeClient {
    urls: Vec<String>,
    client: reqwest::Client,
}

impl NodeClient {
    pub fn new(urls: Vec<String>, client: reqwest::Client) -> Self {
        Self { urls, client }
    }

    pub async fn send_chunk(&self, name: &str, data: Vec<u8>) -> anyhow::Result<String> {
        // pick random url
        let url = self
            .urls
            .choose(&mut rand::thread_rng())
            .context("no url")?;

        self.client
            .post(format!("http://{}/{}/{}", url, "v1/file-chunk", name))
            .body(data)
            .send()
            .await
            .context("failed post")?;

        Ok(url.to_string())
    }

    pub async fn get_chunk(&self, name: &str, node_url: &str) -> anyhow::Result<Vec<u8>> {
        let response = self
            .client
            .get(format!("http://{}/{}/{}", node_url, "v1/file-chunk", name))
            .send()
            .await
            .context("failed get")?;
        Ok(response.bytes().await?.to_vec())
    }
}
