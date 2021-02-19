use std::path::PathBuf;

use anyhow::{anyhow, Result};
use futures::StreamExt;
use lfan::preconfig::concurrent::{new_lru_cache, LRUCache};
use rusoto_s3::{GetObjectRequest, S3Client, S3};
use tokio::{fs, io::AsyncWriteExt};

pub struct FileCache {
    bucket: String,
    client: S3Client,
    file_path_cache: LRUCache<String, PathBuf>,
    root_path: PathBuf,
}

impl FileCache {
    pub fn new<P: Into<PathBuf>, B: Into<String>>(
        directory: P,
        max_nb_of_files: usize,
        bucket: B,
        client: S3Client,
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
        if let Some(blob_path) = self.file_path_cache.get(blob_id.as_ref()).await {
            Some(blob_path.clone())
        } else {
            None
        }
    }

    pub async fn invalidate<S: AsRef<str>>(&self, blob_id: S) -> Result<()> {
        let file_path = self.root_path.join(blob_id.as_ref());
        if file_path.exists() {
            fs::remove_file(&file_path).await?;
        }
        self.file_path_cache.invalidate(blob_id.as_ref()).await;

        Ok(())
    }

    async fn download_blob<S: AsRef<str>>(&self, blob_id: S) -> Result<PathBuf> {
        let get_request = GetObjectRequest {
            bucket: self.bucket.clone(),
            key: blob_id.as_ref().to_string(),
            ..Default::default()
        };

        let result = self.client.get_object(get_request).await?;
        let mut bytestream = result.body.ok_or_else(|| anyhow!("missing stream"))?;

        let file_path = self.root_path.join(blob_id.as_ref());

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
        log::info!(
            "pulled blob '{}' into the local filecache",
            blob_id.as_ref()
        );
        Ok(file_path)
    }

    pub async fn get<S: AsRef<str>>(&self, blob_id: S) -> Result<PathBuf> {
        if let Some(cache_hit) = self.contains(&blob_id).await {
            return Ok(cache_hit);
        }

        let blob_path = self.download_blob(&blob_id).await?;

        let (was_inserted, eviction_victim_maybe) = self
            .file_path_cache
            .insert(blob_id.as_ref().to_string(), blob_path.clone())
            .await;

        if let Some(victim) = eviction_victim_maybe {
            // If a key was evicted from the cache, delete it from disk.
            fs::remove_file(&victim).await?
        }

        if !was_inserted {
            // The cache failed to keep our path, fail gracefully.
            fs::remove_file(&blob_path).await?;
            return Err(anyhow!("failed to insert blob in file cache"));
        }

        Ok(blob_path)
    }
}
