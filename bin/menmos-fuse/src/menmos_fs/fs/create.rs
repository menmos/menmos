use std::{ffi::OsStr, time::Duration};

use async_fuse::FileAttr;
use menmos_client::{Meta, Type};

use super::{build_attributes, Error, Result};
use crate::{constants, MenmosFS};

pub struct CreateReply {
    pub ttl: Duration,
    pub attrs: FileAttr,
    pub generation: u64,
    pub file_handle: u64,
}

impl MenmosFS {
    pub async fn create_impl(&self, parent: u64, name: &OsStr) -> Result<CreateReply> {
        log::info!("create i{}/{:?}", parent, &name);

        let str_name = name.to_string_lossy().to_string();
        if let Some(blob_id) = self.name_to_blobid.get(&(parent, str_name)).await {
            if let Err(e) = self.client.delete(blob_id).await {
                log::error!("client error: {}", e);
            }
        }

        let parent_id = self
            .inode_to_blobid
            .get(&parent)
            .await
            .ok_or(Error::Forbidden)?;

        let str_name = name.to_string_lossy().to_string();

        let meta = Meta::new(&str_name, Type::File).with_parent(parent_id);

        let blob_id = self.client.create_empty(meta.clone()).await.map_err(|e| {
            log::error!("client error: {}", e);
            Error::IOError
        })?;

        let ino = self.get_inode(&blob_id).await;
        self.inode_to_blobid.insert(ino, blob_id.clone()).await;
        self.name_to_blobid
            .insert((parent, str_name), blob_id)
            .await;

        Ok(CreateReply {
            ttl: constants::TTL,
            attrs: build_attributes(ino, &meta, 0o764),
            generation: 0, // TODO: Implement.
            file_handle: 0,
        })
    }
}
