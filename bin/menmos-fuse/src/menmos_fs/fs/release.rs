use crate::MenmosFS;

use super::{Error, Result};

impl MenmosFS {
    pub async fn release_impl(&self, ino: u64) -> Result<()> {
        log::info!("release i{}", ino);
        let mut buffers_guard = self.write_buffers.lock().await;
        if let Some(buffer) = buffers_guard.remove(&ino) {
            log::info!("flushing pending write buffer for {}", ino);
            self.flush_buffer(ino, buffer).await?;
        }

        let blob_id = self
            .inode_to_blobid
            .get(&ino)
            .await
            .ok_or(Error::NotFound)?;

        // TODO: Don't call fsync if the file wasn't open for writing.
        log::info!("calling fsync");
        self.client.fsync(&blob_id).await.map_err(|e| {
            log::error!("menmos fsync error: {}", e);
            Error::IOError
        })?;
        log::info!("fsync complete");

        Ok(())
    }
}
