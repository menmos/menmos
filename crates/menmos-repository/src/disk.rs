use std::io::{self, SeekFrom};
use std::ops::Bound;
use std::path::{Path, PathBuf};

use anyhow::{ensure, Result};
use async_trait::async_trait;
use betterstreams::ChunkedStreamInfo;
use bytes::Bytes;
use futures::prelude::*;
use tokio::io::AsyncWriteExt;
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncSeekExt,
};

use super::iface::Repository;
use crate::util;

/// Represents a blob repository stored on disk.
pub struct DiskRepository {
    path: PathBuf,
}

impl DiskRepository {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let p = PathBuf::from(path.as_ref());

        if !p.exists() {
            std::fs::create_dir(path)?;
        }

        ensure!(p.is_dir(), "Path is not a directory");

        Ok(Self { path: p })
    }

    fn get_path_for_blob(&self, blob_id: &str) -> PathBuf {
        self.path.join(blob_id.to_string()).with_extension("blob")
    }
}

#[async_trait]
impl Repository for DiskRepository {
    async fn save(
        &self,
        id: String,
        _size: u64,
        stream: Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Sync + Unpin + 'static>,
    ) -> Result<()> {
        let file_path = self.get_path_for_blob(&id);

        if let Err(e) = betterstreams::fs::write_all(&file_path, stream).await {
            fs::remove_file(&file_path).await?;
            return Err(e);
        }

        Ok(())
    }

    async fn write(&self, id: String, range: (Bound<u64>, Bound<u64>), body: Bytes) -> Result<u64> {
        let file_path = self.get_path_for_blob(&id);

        let range = util::bounds_to_range(range, u64::MAX, 0);

        let (start, end) = (range.start, range.end);
        ensure!(start < end, "invalid range");

        let old_length = file_path.metadata()?.len();
        let new_length = (start + end).max(old_length);

        {
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&file_path)
                .await?;
            f.seek(SeekFrom::Start(start)).await?;
            f.write_all(body.as_ref()).await?;
        }

        Ok(new_length)
    }

    async fn get(
        &self,
        blob_id: &str,
        range: Option<(Bound<u64>, Bound<u64>)>,
    ) -> Result<ChunkedStreamInfo> {
        let file_path = self.get_path_for_blob(blob_id);
        ensure!(
            file_path.exists() && file_path.is_file(),
            "File doesn't exist"
        );

        let size = file_path.metadata()?.len() as u64;
        betterstreams::fs::read_range(&file_path, range.map(|r| util::bounds_to_range(r, 0, size)))
            .await
    }

    async fn delete(&self, blob_id: &str) -> Result<()> {
        let blob_path = self.get_path_for_blob(blob_id);

        if blob_path.exists() {
            fs::remove_file(&blob_path).await?;
        }

        Ok(())
    }

    async fn fsync(&self, _id: String) -> Result<()> {
        // Nothing to do for us.
        Ok(())
    }
}
