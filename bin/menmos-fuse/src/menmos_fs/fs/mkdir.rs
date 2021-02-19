use std::{ffi::OsStr, time::Duration};

use async_fuse::FileAttr;
use menmos_client::{Meta, Type};

use super::{build_attributes, Error, Result};
use crate::{constants, MenmosFS};

pub struct MkdirReply {
    pub ttl: Duration,
    pub attrs: FileAttr,
    pub generation: u64,
}

impl MenmosFS {
    pub async fn mkdir_impl(&self, parent_inode: u64, name: &OsStr) -> Result<MkdirReply> {
        log::info!("mkdir i{}/{:?}", parent_inode, name);

        let parent_blobid = self
            .inode_to_blobid
            .get(&parent_inode)
            .await
            .ok_or(Error::NotFound)?;

        let str_name = name.to_string_lossy().to_string();
        let meta = Meta::new(str_name.clone(), Type::Directory).with_parent(parent_blobid);
        let blob_id = self.client.create_empty(meta.clone()).await.map_err(|e| {
            log::error!("client error: {}", e);
            Error::Forbidden
        })?;

        let ino = self.get_inode(&blob_id).await;
        self.inode_to_blobid.insert(ino, blob_id.clone()).await;
        self.name_to_blobid
            .insert((parent_inode, str_name), blob_id)
            .await;

        Ok(MkdirReply {
            ttl: constants::TTL,
            attrs: build_attributes(ino, &meta, 0o764),
            generation: 0, // TODO: Implement
        })
    }
}
