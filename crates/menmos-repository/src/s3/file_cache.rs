use std::path::PathBuf;

use anyhow::{anyhow, Result};
use aws_sdk_s3::error::GetObjectErrorKind;
use aws_sdk_s3::Client;
use aws_smithy_http::result::SdkError;
use futures::StreamExt;
use lfan::preconfig::concurrent::{new_lru_cache, LruCache};
use tokio::{fs, io::AsyncWriteExt};

pub struct FileCache {
    bucket: String,
    client: Client,
    file_path_cache: LruCache<String, PathBuf>,
    root_path: PathBuf,
}

impl FileCache {
    pub fn new<P: Into<PathBuf>, B: Into<String>>(
        directory: P,
        max_nb_of_files: usize,
        bucket: B,
        client: Client,
    ) -> Result<Self> {
        let root_path: PathBuf = directory.into();

        if !root_path.exists() {
            std::fs::create_dir_all(&root_path)?;
        }

        let file_path_cache = new_lru_cache(max_nb_of_files);

        Ok(Self {
            bucket: bucket.into(),
            client,
            file_path_cache,
            root_path,
        })
    }

    pub async fn contains<S: AsRef<str>>(&self, blob_id: S) -> Option<PathBuf> {
        self.file_path_cache.get(blob_id.as_ref()).await
    }

    pub async fn invalidate(&self, blob_id: &str) -> Result<()> {
        let file_path = self.root_path.join(blob_id);
        if file_path.exists() {
            fs::remove_file(&file_path).await?;
            tracing::trace!(path = ?file_path, "file existed and was removed");
        }
        self.file_path_cache.invalidate(blob_id).await;

        Ok(())
    }

    async fn download_blob(&self, blob_id: &str) -> Result<PathBuf> {
        match self
            .client
            .get_object()
            .bucket(self.bucket.clone())
            .key(blob_id.to_string())
            .send()
            .await
        {
            Ok(result) => {
                let mut bytestream = result.body;

                let file_path = self.root_path.join(blob_id);
                let mut f = fs::File::create(&file_path).await?;

                while let Some(chunk) = bytestream.next().await {
                    match chunk {
                        Ok(c) => f.write_all(c.as_ref()).await?,
                        Err(e) => {
                            fs::remove_file(&file_path).await?;
                            return Err(e.into());
                        }
                    }
                }
                tracing::debug!("pull successful",);
                Ok(file_path)
            }
            Err(SdkError::ServiceError { err, raw: _ }) => {
                if let GetObjectErrorKind::NoSuchKey(_) = err.kind {
                    // Create the file - empty - in the file cache.
                    let file_path = self.root_path.join(blob_id);
                    fs::File::create(&file_path).await?;
                    Ok(file_path)
                } else {
                    //Rethrow.
                    Err(err.into())
                }
            }
            Err(e) => {
                // Rethrow
                Err(e.into())
            }
        }
    }

    pub async fn get(&self, blob_id: &str) -> Result<PathBuf> {
        if let Some(cache_hit) = self.contains(&blob_id).await {
            tracing::trace!("cache hit");
            return Ok(cache_hit);
        }

        tracing::trace!("cache miss");

        let blob_path = self.download_blob(blob_id.as_ref()).await?;

        let (was_inserted, eviction_victim_maybe) = self
            .file_path_cache
            .insert(blob_id.to_string(), blob_path.clone())
            .await;

        if let Some(victim) = eviction_victim_maybe {
            // If a key was evicted from the cache, delete it from disk.
            fs::remove_file(&victim).await?;
            tracing::trace!(path=?victim, "removed victim from disk");
        }

        if !was_inserted {
            // The cache failed to keep our path, fail gracefully.
            fs::remove_file(&blob_path).await?;
            return Err(anyhow!("failed to insert blob in file cache"));
        }

        Ok(blob_path)
    }
}
