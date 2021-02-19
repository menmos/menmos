use anyhow::ensure;

use super::{Error, Result};
use crate::MenmosFS;

pub struct ReadReply {
    pub data: Vec<u8>,
}

impl MenmosFS {
    async fn read(&self, inode: u64, offset: i64, size: u32) -> anyhow::Result<Option<Vec<u8>>> {
        ensure!(offset >= 0, "invalid offset");

        let blob_id = match self.inode_to_blobid.get(&inode).await {
            Some(blob_id) => blob_id,
            None => {
                return Ok(None);
            }
        };

        let bounds = (offset as u64, (offset + (size - 1) as i64) as u64);
        let bytes = self.client.read_range(&blob_id, bounds).await?;
        Ok(Some(bytes))
    }

    pub async fn read_impl(&self, ino: u64, offset: i64, size: u32) -> Result<ReadReply> {
        log::info!("read i{} {}-{}", ino, offset, (offset + size as i64) - 1);
        match self.read(ino, offset, size).await {
            Ok(Some(bytes)) => {
                log::debug!(
                    "read {}-{} on i{} => got {} bytes",
                    offset,
                    (offset + size as i64) - 1,
                    ino,
                    bytes.len()
                );
                Ok(ReadReply { data: bytes })
            }
            Ok(None) => Err(Error::NotFound),
            Err(e) => {
                log::error!("read error: {}", e);
                Err(Error::IOError)
            }
        }
    }
}
