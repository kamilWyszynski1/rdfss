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

    pub async fn delete(&self, file_name: &str) -> anyhow::Result<()> {
        let path = std::path::Path::new(&self.name).join(file_name);
        tokio::fs::remove_file(&path).await?;
        tracing::debug!(file_name = file_name, "deleted file");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn test_save() -> anyhow::Result<()> {
        let tmp_dir = TempDir::new("example")?;
        let s = Storage::new(tmp_dir.path().to_string_lossy().into());
        let mut reader = tokio_test::io::Builder::new().read(b"Hi there").build();
        s.save(&mut reader, "test.txt").await?;

        let output = tokio::fs::read_to_string(tmp_dir.path().join("test.txt")).await?;
        assert_eq!(output, "Hi there");

        let mut reader = s.get("test.txt").await?;
        let mut dst = String::new();
        reader.read_to_string(&mut dst).await?;
        assert_eq!(dst, "Hi there");
        Ok(())
    }
}
