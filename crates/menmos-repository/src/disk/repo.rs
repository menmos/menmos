use std::io::{self};
use std::ops::Bound;
use std::path::{Path, PathBuf};

use anyhow::{bail, ensure, Context, Result};

use async_trait::async_trait;

use betterstreams::ChunkedStreamInfo;

use bytes::Bytes;

use futures::prelude::*;

use sysinfo::{Disk, DiskExt, System, SystemExt};

use tokio::fs;
use tokio::sync::Mutex;

use crate::iface::{OperationGuard, Repository};
use crate::util;

use super::ops;

#[cfg(unix)]
async fn is_path_on_disk(disk: &Disk, path: &Path) -> Result<bool> {
    use std::os::unix::fs::MetadataExt;

    // We get the device IDs of the device mount point and of the repository root.
    // If the device ID is the same, its the correct disk.
    let (disk_meta, repo_meta) =
        tokio::try_join!(fs::metadata(disk.mount_point()), fs::metadata(path))?;

    let same_device = disk_meta.dev() == repo_meta.dev();

    if !same_device && path.starts_with("/tmp") && disk.mount_point() == PathBuf::from("/") {
        // HACK: Special case where /tmp is a tmpfs volume but is not listed as a disk (leading to it having a different device ID).
        // This isn't super elegant but only comes up in tests, so we'll live with that hack for now.
        return Ok(true);
    }

    Ok(same_device)
}

#[cfg(not(unix))]
async fn is_path_on_disk(disk: &Disk, path: &Path) -> Result<bool> {
    // FIXME(windows): This should work in 99% of cases on windows,
    //                 but I'm pretty sure it doesn't handle links/junctions properly.
    //                 A better solution should use windows APIs to get the device ID for
    //                 a given path, a bit like what we do on unix.
    const WEIRD_WINDOWS_VOLUME_PREFIX: &str = "\\\\?\\";
    let a = PathBuf::from(
        WEIRD_WINDOWS_VOLUME_PREFIX.to_string()
            + disk.mount_point().to_string_lossy().to_string().as_ref(),
    );
    Ok(path.starts_with(a))
}

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
}

#[async_trait]
impl Repository for DiskRepository {
    #[tracing::instrument(name = "disk.save", skip(self, stream))]
    async fn save(
        &self,
        id: String,
        stream: Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Sync + Unpin + 'static>,
        expected_size: u64,
    ) -> Result<Box<dyn OperationGuard>> {
        let file_path = self.get_path_for_blob(&id);
        let tmp_path = file_path.with_extension("buf");

        tracing::trace!(path = ?tmp_path, "begin writing to file");

        let blob_size = match betterstreams::fs::write_all(&tmp_path, stream, Some(expected_size))
            .await
            .context("failed to write stream to disk")
        {
            Ok(size) => Ok(size),
            Err(e) => {
                // Clean up our temp file and throw.
                fs::remove_file(&tmp_path)
                    .await
                    .context("failed to rollback file creation")?;
                tracing::trace!(path=?tmp_path, "removed temporary file");
                Err(e)
            }
        }?;

        ensure!(
            blob_size == expected_size,
            "stream size and size header were not equal"
        );

        Ok(Box::new(ops::SaveOperationGuard::new(tmp_path, file_path)))
    }

    #[tracing::instrument(name = "disk.write", skip(self, body))]
    async fn write(
        &self,
        id: String,
        range: (Bound<u64>, Bound<u64>),
        body: Bytes,
    ) -> Result<(u64, Box<dyn OperationGuard>)> {
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

        tracing::trace!(old_length = old_length, new_length = new_length, offset = start, path = ?file_path, "opened write operation");

        Ok((
            new_length,
            Box::new(ops::WriteOperationGuard::new(body, file_path, start)),
        ))
    }

    #[tracing::instrument(name = "disk.get", skip(self))]
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

    #[tracing::instrument(name = "disk.delete", skip(self))]
    async fn delete(&self, blob_id: &str) -> Result<Box<dyn OperationGuard>> {
        let blob_path = self.get_path_for_blob(blob_id);
        let tmp_path = blob_path.with_extension("deleted");

        ensure!(blob_path.exists(), "blob path does not exist");

        fs::rename(&blob_path, &tmp_path)
            .await
            .context("failed to move file pending deletion")?;

        Ok(Box::new(ops::DeleteOperationGuard::new(
            blob_path, tmp_path,
        )))
    }

    #[tracing::instrument(name = "disk.fsync", skip(self))]
    async fn fsync(&self, _id: String) -> Result<()> {
        // Nothing to do for us.
        tracing::trace!("fsync on disk repository is a no-op");
        Ok(())
    }

    #[tracing::instrument("disk.available_space", skip(self))]
    async fn available_space(&self) -> Result<Option<u64>> {
        let mut sys = self.system.lock().await;
        sys.refresh_disks_list();
        sys.refresh_disks();

        for disk in sys.disks_mut().iter_mut() {
            if !is_path_on_disk(disk, &self.path).await? {
                tracing::trace!(
                    "filtered out disk '{:?}' ({}) with mount point '{:?}'",
                    disk.name(),
                    disk.total_space(),
                    disk.mount_point()
                );
            } else {
                tracing::trace!(
                    "found disk '{:?} with mount point '{:?}'",
                    disk.name(),
                    disk.mount_point()
                );
                return Ok(Some(disk.available_space()));
            }
        }

        bail!("no disk found");
    }
}
