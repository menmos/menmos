use std::time::Duration;

use async_fuse::FileAttr;
use menmos_client::{Meta, Type};

use super::{build_attributes, Error, Result};
use crate::{constants, OmniFS};

pub struct GetAttrReply {
    pub ttl: Duration,
    pub attrs: FileAttr,
}

impl OmniFS {
    pub(crate) async fn getattr_impl(&self, ino: u64) -> Result<GetAttrReply> {
        log::info!("getattr: {}", ino);
        if ino == 1 {
            return Ok(GetAttrReply {
                ttl: constants::TTL,
                attrs: constants::ROOT_DIR_ATTR,
            });
        }

        // If virtual directory.
        if self.virtual_directories_inodes.get(&ino).await.is_some() {
            // TODO: Make a separate method to get attributes for virtual directories.
            let attrs = build_attributes(ino, &Meta::new("", Type::Directory), 0o444);
            return Ok(GetAttrReply {
                ttl: constants::TTL,
                attrs,
            });
        }

        match self.get_meta_by_inode(ino).await {
            Ok(Some(meta)) => Ok(GetAttrReply {
                ttl: constants::TTL,
                attrs: build_attributes(ino, &meta, 0o764),
            }),
            Ok(None) => Err(Error::NotFound),
            Err(e) => {
                log::error!("client error: {}", e);
                Err(Error::IOError)
            }
        }
    }
}
