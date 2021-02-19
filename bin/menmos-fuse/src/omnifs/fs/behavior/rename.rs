use std::ffi::OsStr;

use anyhow::anyhow;

use super::{Error, Result};
use crate::OmniFS;

impl OmniFS {
    async fn delete_dst_blob_if_exists(&self, key: &(u64, String)) -> Result<()> {
        if let Some(dst_blob) = self.name_to_blobid.get(key).await {
            if let Some(inode) = self.blobid_to_inode.remove(&dst_blob).await {
                self.inode_to_blobid.remove(&inode).await;
            }

            // If so, delete it before our rename.
            self.client.delete(dst_blob).await.map_err(|e| {
                log::error!("client error: {}", e);
                Error::IOError
            })?;
        }
        Ok(())
    }

    async fn rename_blob(
        &self,
        source_parent_id: &str,
        source_blob: &str,
        new_name: &str,
        new_parent_id: &str,
    ) -> anyhow::Result<()> {
        let mut source_meta = self
            .client
            .get_meta(&source_blob)
            .await?
            .ok_or_else(|| anyhow!("missing blob"))?;

        source_meta.name = new_name.into();
        source_meta
            .parents
            .retain(|item| item != source_parent_id && item != new_parent_id);
        source_meta.parents.push(new_parent_id.into());

        self.client.update_meta(source_blob, source_meta).await?;

        Ok(())
    }

    pub(crate) async fn rename_impl(
        &self,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
    ) -> Result<()> {
        log::info!(
            "rename {}/{:?} => {}/{:?}",
            parent,
            name,
            newparent,
            newname
        );

        let src_name = name.to_string_lossy().to_string();
        let dst_name = newname.to_string_lossy().to_string();

        // Does the source file exist?
        let source_blob = self
            .name_to_blobid
            .get(&(parent, src_name.clone()))
            .await
            .ok_or(Error::NotFound)?;

        self.delete_dst_blob_if_exists(&(newparent, dst_name.clone()))
            .await?;

        let source_parent_id = self.inode_to_blobid.get(&parent).await.ok_or_else(|| {
            log::error!("source parent inode does not have a corresponding blob");
            Error::NotFound
        })?;

        // Does the new parent inode exist?;
        let new_parent_id = self
            .inode_to_blobid
            .get(&newparent)
            .await
            .ok_or(Error::NotFound)?;

        // Rename the blob.
        self.rename_blob(&source_parent_id, &source_blob, &dst_name, &new_parent_id)
            .await
            .map_err(|e| {
                log::error!("client error: {}", e);
                Error::NotFound
            })?;

        self.name_to_blobid.remove(&(parent, src_name)).await;
        self.name_to_blobid
            .insert((newparent, dst_name), source_blob)
            .await;

        Ok(())
    }
}
