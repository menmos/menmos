use std::{ffi::OsStr, time::Duration};

use async_fuse::FileAttr;
use menmos_client::{Meta, Type};

use super::{build_attributes, Error, Result};
use crate::{constants, MenmosFS};

pub struct MkNodReply {
    pub ttl: Duration,
    pub attrs: FileAttr,
    pub generation: u64,
}

impl MenmosFS {
    pub async fn mknod_impl(&self, parent: u64, name: &OsStr) -> Result<MkNodReply> {
        log::info!("mknod i{}/{:?}", parent, name);

        let parent_id = self
            .inode_to_blobid
            .get(&parent)
            .await
            .ok_or(Error::Forbidden)?;

        let str_name = name.to_string_lossy().to_string();

        let meta = Meta::new(&str_name, Type::File).with_parent(parent_id);

        let blob_id = self.client.create_empty(meta.clone()).await.map_err(|e| {
            log::error!("client error: {}", e);
            Error::NotFound
        })?;

        let ino = self.get_inode(&blob_id).await;
        self.inode_to_blobid.insert(ino, blob_id.clone()).await;
        self.name_to_blobid
            .insert((parent, str_name), blob_id)
            .await;

        Ok(MkNodReply {
            ttl: constants::TTL,
            attrs: build_attributes(ino, &meta, 0o764),
            generation: 0, // TODO: Implement
        })
    }
}
