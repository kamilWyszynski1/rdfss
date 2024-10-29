use redis::AsyncCommands;

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

    pub async fn file_exists(&mut self, file_name: &str) -> anyhow::Result<bool> {
        Ok(self.redis_client.exists(file_name).await?)
    }

    pub async fn save_file_info(
        &mut self,
        file_name: &str,
        chunks: Vec<(String, String)>,
    ) -> anyhow::Result<()> {
        self.redis_client.hset_multiple(file_name, &chunks).await?;
        Ok(())
    }

    pub async fn get_file_info(
        &mut self,
        file_name: &str,
    ) -> anyhow::Result<Vec<(String, String)>> {
        let info: Vec<(String, String)> = self.redis_client.hgetall(file_name).await?;

        Ok(info)
    }
}
