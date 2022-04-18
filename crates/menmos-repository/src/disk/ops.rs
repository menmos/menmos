use std::io::SeekFrom;
use std::path::PathBuf;

use bytes::Bytes;

use tokio::fs;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

use crate::iface::OperationGuard;

/// Guards the commit of a save operation.
pub struct SaveOperationGuard {
    src_path: PathBuf,
    dst_path: PathBuf,
    committed: bool,
}

impl SaveOperationGuard {
    pub fn new(src_path: PathBuf, dst_path: PathBuf) -> Self {
        Self {
            src_path,
            dst_path,
            committed: false,
        }
    }
}

impl Drop for SaveOperationGuard {
    /// Aborts the operation.
    fn drop(&mut self) {
        if !self.committed {
            if let Err(e) = std::fs::remove_file(&self.src_path) {
                panic!("failed to rollback save operation: {e}")
            }
        }
    }
}

#[async_trait::async_trait]
impl OperationGuard for SaveOperationGuard {
    /// Commits a save operation.
    ///
    /// # Panics
    /// All the following potential causes are checked by the repository, any
    /// of them occurring constitute an unrecoverable error:
    ///   - If the source file shouldn't exist
    ///   - If the target path's parent directory doesn't exist.
    ///   - If the target path is on a different disk.
    ///   - If the target path is a directory (on windows)
    async fn commit(&mut self) {
        fs::rename(&self.src_path, &self.dst_path)
            .await
            .expect("save commit should not fail");
        self.committed = true;
    }
}

pub struct WriteOperationGuard {
    buf: Bytes,
    path: PathBuf,
    offset: u64,
}

impl WriteOperationGuard {
    pub fn new(buf: Bytes, path: PathBuf, offset: u64) -> Self {
        Self { buf, path, offset }
    }
}

#[async_trait::async_trait]
impl OperationGuard for WriteOperationGuard {
    async fn commit(&mut self) {
        let mut f = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&self.path)
            .await
            .expect("write operation commit should not fail");

        f.seek(SeekFrom::Start(self.offset))
            .await
            .expect("write operation commit should not fail");

        f.write_all(self.buf.as_ref())
            .await
            .expect("write operation commit should not fail");
    }
}

pub struct DeleteOperationGuard {
    original_path: PathBuf,
    tmp_path: PathBuf,
    committed: bool,
}

impl DeleteOperationGuard {
    pub fn new(original_path: PathBuf, tmp_path: PathBuf) -> Self {
        Self {
            original_path,
            tmp_path,
            committed: false,
        }
    }
}

impl Drop for DeleteOperationGuard {
    fn drop(&mut self) {
        if !self.committed {
            if let Err(e) = std::fs::rename(&self.tmp_path, &self.original_path) {
                panic!("failed to rollback delete operation: {e}")
            }
        }
    }
}

#[async_trait::async_trait]
impl OperationGuard for DeleteOperationGuard {
    /// Commits a delete operation.
    ///
    /// # Panics
    /// All the following potential causes are checked by the repository, any
    /// of them occurring constitute an unrecoverable error:
    ///   - `tmp_path` points to a directory.
    ///   - The file doesn’t exist.
    ///   - The user lacks permissions to remove the file.
    async fn commit(&mut self) {
        fs::remove_file(&self.tmp_path)
            .await
            .expect("delete commit should not fail");
        tracing::trace!(path=?self.tmp_path, "file deleted");
        self.committed = true;
    }
}
