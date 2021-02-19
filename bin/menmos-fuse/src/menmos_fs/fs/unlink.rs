use std::ffi::OsStr;

use crate::MenmosFS;

use super::{Error, Result};

impl MenmosFS {
    pub async fn unlink_impl(&self, parent: u64, name: &OsStr) -> Result<()> {
        log::info!("unlink i{}/{:?}", parent, name);
        let str_name = name.to_string_lossy().to_string();

        let name_tuple = (parent, str_name);

        let blob_id = self
            .name_to_blobid
            .get(&name_tuple)
            .await
            .ok_or(Error::NotFound)?;

        // Delete from the server.
        self.client.delete(blob_id.clone()).await.map_err(|e| {
            log::error!("client error: {}", e);
            Error::IOError
        })?;

        // Clean up our internal maps.
        self.blobid_to_inode.remove(&blob_id).await;
        self.name_to_blobid.remove(&name_tuple).await;

        Ok(())
    }
}
