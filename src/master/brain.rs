use redis::AsyncCommands;
use std::collections::HashMap;

/// Brains takes care of memorizing how file is stored within the whole system -
/// what chunk of file is held in which worker node.
#[derive(Clone)]
pub struct Brain {
    redis_client: redis::aio::MultiplexedConnection,
}

impl Brain {
    pub fn new(redis_client: redis::aio::MultiplexedConnection) -> Self {
        Self { redis_client }
    }

    pub async fn save_file_info(
        &mut self,
        file_name: &str,
        chunks: HashMap<String, String>,
    ) -> anyhow::Result<()> {
        self.redis_client
            .hset_multiple(
                file_name,
                &chunks.into_iter().collect::<Vec<(String, String)>>(),
            )
            .await?;
        Ok(())
    }

    pub async fn get_file_info(
        &mut self,
        file_name: &str,
    ) -> anyhow::Result<HashMap<String, String>> {
        let info: HashMap<String, String> = self.redis_client.hgetall(file_name).await?;
        Ok(info)
    }
}
