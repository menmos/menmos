use std::ffi::OsString;

use async_fuse::FileType;
use menmos_client::Query;

use super::{Error, Result};
use crate::MenmosFS;

#[derive(Debug)]
pub struct ReadDirEntry {
    pub ino: u64,
    pub offset: i64,
    pub kind: FileType,
    pub name: OsString,
}

pub struct ReadDirReply {
    pub entries: Vec<ReadDirEntry>,
}

impl MenmosFS {
    pub async fn readdir_impl(&self, ino: u64, offset: i64) -> Result<ReadDirReply> {
        log::info!("readdir i{}", ino);
        let entries = if let Some(v) = self.virtual_directories_inodes.get(&ino).await {
            self.list_virtual_entries(v, ino).await
        } else {
            // We assume the inode points to a directory blob id.
            let blob_id = self
                .inode_to_blobid
                .get(&ino)
                .await
                .ok_or(Error::NotFound)?;
            self.list_entries(Query::default().and_parent(blob_id), ino)
                .await
        }
        .map_err(|e| {
            log::error!("client error: {}", e);
            Error::IOError
        })?
        .into_iter()
        .enumerate()
        .skip(offset as usize)
        .map(|(offset, (ino, kind, name))| ReadDirEntry {
            offset: (offset + 1) as i64,
            ino,
            kind,
            name: name.into(),
        })
        .collect::<Vec<ReadDirEntry>>();

        Ok(ReadDirReply { entries })
    }
}
