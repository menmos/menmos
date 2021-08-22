use std::io::{self, SeekFrom};
use std::ops::Bound;
use std::path::{Path, PathBuf};

use anyhow::{bail, ensure, Result};
use async_trait::async_trait;
use betterstreams::ChunkedStreamInfo;
use bytes::Bytes;
use futures::prelude::*;
use sysinfo::{DiskExt, System, SystemExt};
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncSeekExt;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

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
        self.path.join(blob_id.to_string()).with_extension("blob")
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

    async fn available_space(&self) -> Result<Option<u64>> {
        let mut sys = self.system.lock().await;
        sys.refresh_disks_list();
        sys.refresh_disks();

        let eligible_disks = sys
            .disks_mut()
            .iter_mut()
            .filter(|d| Self::is_path_prefix_of(d.mount_point(), &self.path))
            .collect::<Vec<_>>();

        log::trace!("eligible disks: {}", eligible_disks.len());

        if eligible_disks.len() == 1 {
            // No need for complex stuff if only a single disk is a prefix of our path.
            return Ok(Some(eligible_disks.first().unwrap().available_space()));
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
                return Ok(Some(disk_a.available_space()));
            }
        }

        bail!("no disk found");
    }
}
