use std::{ffi::OsStr, time::Duration};

use async_fuse::FileAttr;
use menmos_client::{Meta, Type};

use crate::{constants, MenmosFS};

use super::{build_attributes, Error, Result};

pub struct LookupReply {
    pub ttl: Duration,
    pub attrs: FileAttr,
    pub generation: u64,
}

impl MenmosFS {
    async fn lookup_vdir(&self, key: &(u64, String)) -> Option<LookupReply> {
        if let Some(inode) = self.virtual_directories.get(key).await {
            log::info!("lookup on {:?} found vdir inode: {}", key.1, inode,);
            let attrs = build_attributes(inode, &Meta::new(&key.1, Type::Directory), 0o444);

            Some(LookupReply {
                ttl: constants::TTL,
                attrs,
                generation: inode, // TODO: Use a nanosecond timestamp here instead.
            })
        } else {
            None
        }
    }

    pub async fn lookup_impl(&self, parent_inode: u64, name: &OsStr) -> Result<LookupReply> {
        log::info!("lookup i{}/{:?}", parent_inode, name);

        let str_name = name.to_string_lossy().to_string();

        // Before we do anything, we need to make sure the children of our parent directory were populated.
        // This is usually done by readdir when using this fuse mount with a file explorer, but in case someone kept a path or tries to directly access a file, we need to make sure everything is there.
        // TODO: Find a more efficient way to do this than calling readdir from here.
        self.readdir_impl(parent_inode, 0).await?;

        // First, check if it's a virtual directory.
        if let Some(resp) = self.lookup_vdir(&(parent_inode, str_name.clone())).await {
            return Ok(resp);
        }

        // If not, proceed as usual and lookup the blob.
        let blob_id = self
            .name_to_blobid
            .get(&(parent_inode, str_name.clone()))
            .await
            .ok_or(Error::NotFound)?;

        match self.client.get_meta(&blob_id).await {
            Ok(Some(blob_meta)) => {
                // We got the meta, time to make the item attribute.
                let inode = self.get_inode(&blob_id).await;
                let attributes = build_attributes(inode, &blob_meta, 0o764);
                log::info!(
                    "lookup on {:?} found inode: {} for ID {} ({:?})",
                    name,
                    inode,
                    blob_id,
                    blob_meta.blob_type
                );
                self.inode_to_blobid.insert(inode, blob_id).await;

                Ok(LookupReply {
                    ttl: constants::TTL,
                    attrs: attributes,
                    generation: inode, // TODO: Use nanosecond timestamp.
                })
            }
            Ok(None) => Err(Error::NotFound),
            Err(e) => {
                log::error!("lookup error: {}", e);
                Err(Error::IOError)
            }
        }
    }
}
