use anyhow::{bail, Context};
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncWriteExt, BufWriter};

#[derive(Clone)]
pub struct Storage {
    pub name: String, // directory name
}

impl Storage {
    pub fn new(name: String) -> Self {
        Self { name }
    }

    pub async fn save<'a, R>(&self, reader: &'a mut R, file_name: &str) -> anyhow::Result<()>
    where
        R: AsyncRead + Unpin + ?Sized,
    {
        println!("saved");
        let path = std::path::Path::new(&self.name).join(file_name);

        let f = match File::create(&path).await {
            Ok(f) => f,
            Err(err) => {
                if err.kind() == tokio::io::ErrorKind::NotFound {
                    tokio::fs::create_dir(&self.name)
                        .await
                        .context("could not create root directory")?;
                    File::create(&path).await?
                } else {
                    bail!(err)
                }
            }
        };

        let mut file = BufWriter::new(f);

        tokio::io::copy(reader, &mut file)
            .await
            .context("could not copy content")?;
        file.flush().await?;

        tracing::debug!(file_name = file_name, "chunk saved");

        Ok(())
    }

    pub async fn get(&self, file_name: &str) -> anyhow::Result<impl AsyncRead> {
        let path = std::path::Path::new(&self.name).join(file_name);
        Ok(File::open(&path).await?)
    }
}
