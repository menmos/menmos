use std::io::{self, SeekFrom};
use std::ops::Bound;
use std::path::{Path, PathBuf};

use anyhow::{bail, ensure, Context, Result};

use async_trait::async_trait;

use betterstreams::ChunkedStreamInfo;

use bytes::Bytes;

use futures::prelude::*;

use parking_lot::Mutex;

use sysinfo::{DiskExt, System, SystemExt};

use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

use super::iface::Repository;
use crate::util;

/// Represents a blob repository stored on disk.
pub struct DiskRepository {
    path: PathBuf,
    system: Mutex<System>,
}

impl DiskRepository {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        if !path.as_ref().exists() {
            std::fs::create_dir(&path)?;
        }

        let p = PathBuf::from(path.as_ref()).canonicalize()?;

        ensure!(p.is_dir(), "Path is not a directory");

        let system = Mutex::new(System::default());

        Ok(Self { path: p, system })
    }

    fn get_path_for_blob(&self, blob_id: &str) -> PathBuf {
        self.path.join(blob_id).with_extension("blob")
    }

    #[cfg(windows)]
    fn is_path_prefix_of(a: &Path, b: &Path) -> bool {
        const WEIRD_WINDOWS_VOLUME_PREFIX: &str = "\\\\?\\";
        let a = PathBuf::from(
            WEIRD_WINDOWS_VOLUME_PREFIX.to_string() + a.to_string_lossy().to_string().as_ref(),
        );
        b.starts_with(a)
    }

    #[cfg(unix)]
    fn is_path_prefix_of(a: &Path, b: &Path) -> bool {
        b.starts_with(a)
    }
}

#[async_trait]
impl Repository for DiskRepository {
    #[tracing::instrument(skip(self, stream))]
    async fn save(
        &self,
        id: String,
        stream: Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Sync + Unpin + 'static>,
    ) -> Result<u64> {
        let file_path = self.get_path_for_blob(&id);
        tracing::trace!(path = ?file_path, "begin writing to file");

        match betterstreams::fs::write_all(&file_path, stream)
            .await
            .context("failed to write stream to disk")
        {
            Ok(size) => Ok(size),
            Err(e) => {
                fs::remove_file(&file_path)
                    .await
                    .context("failed to rollback file creation")?;
                tracing::trace!(path=?file_path, "removed temporary file");
                Err(e)
            }
        }
    }

    #[tracing::instrument(skip(self, body))]
    async fn write(&self, id: String, range: (Bound<u64>, Bound<u64>), body: Bytes) -> Result<u64> {
        let file_path = self.get_path_for_blob(&id);

        let range = util::bounds_to_range(range, u64::MAX, 0);

        let (start, end) = (range.start, range.end);
        ensure!(start < end, "invalid range");

        let old_length = if file_path.exists() {
            file_path.metadata()?.len()
        } else {
            0
        };

        let new_length = (start + end).max(old_length);

        tracing::trace!(old_length = old_length, new_length = new_length, offset = start, path = ?file_path, "begin writing to file");

        {
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&file_path)
                .await?;
            f.seek(SeekFrom::Start(start)).await?;
            f.write_all(body.as_ref())
                .await
                .context("failed to write stream to file")?;
        }

        Ok(new_length)
    }

    #[tracing::instrument(skip(self))]
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

        tracing::trace!(size=size, path=?file_path, "begin read");

        betterstreams::fs::read_range(&file_path, range.map(|r| util::bounds_to_range(r, 0, size)))
            .await
            .context("failed to read byte range from file")
    }

    #[tracing::instrument(skip(self))]
    async fn delete(&self, blob_id: &str) -> Result<()> {
        let blob_path = self.get_path_for_blob(blob_id);

        if blob_path.exists() {
            fs::remove_file(&blob_path)
                .await
                .context("failed to delete file")?;
            tracing::trace!(path=?blob_path, "file deleted");
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn fsync(&self, _id: String) -> Result<()> {
        // Nothing to do for us.
        tracing::trace!("fsync on disk repository is a no-op");
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn available_space(&self) -> Result<Option<u64>> {
        let mut sys = self.system.lock();
        sys.refresh_disks_list();
        sys.refresh_disks();

        let eligible_disks = sys
            .disks_mut()
            .iter_mut()
            .filter(|d| Self::is_path_prefix_of(d.mount_point(), &self.path))
            .collect::<Vec<_>>();

        tracing::trace!(count = eligible_disks.len(), "found eligible disks");

        if eligible_disks.len() == 1 {
            // No need for complex stuff if only a single disk is a prefix of our path.
            let disk = eligible_disks.first().unwrap();
            tracing::trace!(disk = ?disk.name(), "found disk");
            return Ok(Some(disk.available_space()));
        }

        // It's possible that one disk is mounted as a child of another (e.g. /dev/sda1 => / , /dev/sda2 => /home).
        // In this case, we loop over all the disks and attempt to find one that's not the child of another disk.
        // This could be made faster, but unless the machine has a ton of disks it's not really a problem.
        for disk_a in eligible_disks.iter() {
            let mut skip_disk = false;
            for disk_b in eligible_disks.iter() {
                if disk_a.mount_point() != disk_b.mount_point()
                    && disk_b.mount_point().starts_with(disk_a.mount_point())
                {
                    skip_disk = true
                }
            }

            if !skip_disk {
                tracing::trace!(disk = ?disk_a.name(), "found disk");
                return Ok(Some(disk_a.available_space()));
            }
        }

        bail!("no disk found");
    }
}
