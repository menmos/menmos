use std::io::SeekFrom;

use bytes::Bytes;

use futures::{TryStream, TryStreamExt};

use interface::Query;

use menmos_client::Meta;

use snafu::prelude::*;

use crate::util;
use crate::{ClientRC, FileMetadata};

use super::error::*;

fn make_file_meta(m: FileMetadata) -> Meta {
    Meta {
        fields: m.fields,
        tags: m.tags,
    }
}

/// A handle to a file in a menmos cluster.
#[derive(Clone)]
pub struct MenmosFile {
    blob_id: String,
    client: ClientRC,
    offset: u64,
}

impl MenmosFile {
    #[doc(hidden)]
    pub async fn create(client: ClientRC, metadata: FileMetadata) -> Result<Self> {
        let metadata = make_file_meta(metadata);

        let blob_id = client
            .create_empty(metadata)
            .await
            .context(FileCreateSnafu)?;

        Ok(Self {
            blob_id,
            client,
            offset: 0,
        })
    }

    pub fn open(client: ClientRC, id: &str) -> Result<Self> {
        Ok(Self {
            blob_id: String::from(id),
            client,
            offset: 0,
        })
    }

    /// Returns the ID of this file.
    pub fn id(&self) -> &str {
        &self.blob_id
    }

    /// Write the contents of the provided buffer to the file, at the current offset.
    ///
    /// Returns the number of bytes written. If no errors occured,
    /// the value returned will always be the length of the provided buffer.
    pub async fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let buf = Bytes::copy_from_slice(buf);
        let buf_len = buf.len();
        self.client
            .write(&self.blob_id, self.offset, buf)
            .await
            .context(FileWriteSnafu)?;
        self.offset += buf_len as u64;
        Ok(buf_len)
    }

    /// Seek to a new position in the file.
    ///
    /// Going past the end of the file will not return an error,
    /// the offset will simply be truncated to the length of the file.
    ///
    /// # Errors
    /// Seeking to a negative offset will return an error variant.
    pub async fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        match pos {
            SeekFrom::Current(offset) => {
                let new_offset = self.offset as i64 + offset;
                ensure!(new_offset >= 0, NegativeOffsetSnafu);
                self.offset = new_offset as u64;
            }
            SeekFrom::Start(new_offset) => {
                self.offset = new_offset;
            }
            SeekFrom::End(relative) => {
                let metadata = util::get_meta(&self.client, &self.blob_id)
                    .await
                    .context(SeekMetaSnafu)?;

                let end_offset = metadata.size as i64;
                let new_offset = end_offset + relative;
                ensure!(new_offset >= 0, NegativeOffsetSnafu);
                self.offset = new_offset as u64;
            }
        }
        Ok(self.offset)
    }

    /// Read a number of bytes from the file.
    ///
    /// Returns the number of bytes read `0 <= n <= buf.len()`.
    ///
    /// If the number of bytes read is 0, the current offset is past the end of the file.
    ///
    /// If the number of bytes read is inferior to `buf.len()`, the no more bytes
    /// could be read at this moment.
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let r = self
            .client
            .read_range(
                &self.blob_id,
                (self.offset, (self.offset + buf.len() as u64) - 1),
            )
            .await
            .with_context(|_| FileReadSnafu {
                blob_id: self.blob_id.clone(),
            })?;
        buf.copy_from_slice(&r);
        self.offset += r.len() as u64;
        Ok(r.len())
    }

    /// Read bytes from the current offset to the end of the file.
    ///
    /// Returns the number of bytes read.
    pub async fn read_to_end(&mut self, buf: &mut Vec<u8>) -> Result<usize> {
        let metadata = util::get_meta(&self.client, &self.blob_id)
            .await
            .context(SeekMetaSnafu)?;
        let out = self
            .client
            .read_range(&self.blob_id, (self.offset, metadata.size))
            .await
            .with_context(|_| FileReadSnafu {
                blob_id: self.blob_id.clone(),
            })?;
        *buf = out;
        self.offset += buf.len() as u64;
        Ok(buf.len())
    }

    /// Read bytes from the current offset to the end of the file and decode those bytes
    /// as a UTF-8 string.
    ///
    /// Returns the number of bytes read.
    pub async fn read_to_string(&mut self, string: &mut String) -> Result<usize> {
        let mut v = Vec::new();
        self.read_to_end(&mut v).await?;

        let buf_read = v.len();

        *string = String::from_utf8(v).context(BufferEncodingSnafu)?;

        Ok(buf_read)
    }

    /// Get a stream of entries present in this directory.
    pub fn list(&self) -> impl TryStream<Ok = Self, Error = FsError> + Unpin {
        let query = Query::default()
            .and_field("parent", &self.blob_id)
            .with_from(0)
            .with_size(50);

        let client = self.client.clone();
        Box::pin(
            util::scroll_query(query, &client)
                .map_err(|source| FsError::DirQueryError { source })
                .and_then(move |hit| {
                    let client = client.clone();
                    async move {
                        let entry = MenmosFile::open(client, &hit.id)?;
                        Ok(entry)
                    }
                }),
        )
    }

    /// Get whether this directory has any children.
    pub async fn is_empty(&self) -> Result<bool> {
        let query = Query::default()
            .and_field("parent", &self.blob_id)
            .with_size(0);

        let results = self.client.query(query).await.context(DirListSnafu)?;

        Ok(results.total == 0)
    }
}
