use futures::{TryStream, TryStreamExt};

use interface::BlobMeta;
use menmos_client::{Meta, Query};

use snafu::prelude::*;

use crate::{ClientRC, FileMetadata};

use super::error::*;
use super::file::MenmosFile;
use crate::util;

fn make_dir_meta(m: FileMetadata) -> Meta {
    Meta {
        fields: m.metadata,
        tags: m.tags,
    }
}

/// All types of blobs that can be found in a directory.
#[derive(Clone)]
pub enum DirEntry {
    File(MenmosFile),
    Directory(MenmosDirectory),
}

/// A handle to a directory in a menmos cluster.
#[derive(Clone)]
pub struct MenmosDirectory {
    blob_id: String,
    client: ClientRC,
}

impl MenmosDirectory {
    #[doc(hidden)]
    pub async fn create(client: ClientRC, metadata: FileMetadata) -> Result<Self> {
        let metadata = make_dir_meta(metadata);

        let blob_id = client
            .create_empty(metadata)
            .await
            .map_err(|_| FsError::DirCreateError)?;

        Ok(Self { blob_id, client })
    }

    #[doc(hidden)]
    pub async fn open(client: ClientRC, id: &str) -> Result<Self> {
        let metadata = util::get_meta(&client, id).await.context(DirOpenSnafu)?;
        Self::open_raw(client, id, metadata)
    }

    pub(crate) fn open_raw(client: ClientRC, id: &str, meta: BlobMeta) -> Result<Self> {
        ensure!(
            meta.blob_type == Type::Directory,
            ExpectedDirectorySnafu {
                blob_id: String::from(id)
            }
        );

        Ok(Self {
            blob_id: String::from(id),
            client,
        })
    }

    /// Returns the ID of this directory.
    pub fn id(&self) -> &str {
        &self.blob_id
    }

    /// Get a stream of entries present in this directory.
    pub fn list(&self) -> impl TryStream<Ok = DirEntry, Error = FsError> + Unpin {
        let query = Query::default()
            .and_parent(&self.blob_id)
            .with_from(0)
            .with_size(50);

        let client = self.client.clone();
        Box::pin(
            util::scroll_query(query, &client)
                .map_err(|source| FsError::DirQueryError { source })
                .and_then(move |hit| {
                    let client = client.clone();
                    async move {
                        let entry = if hit.meta.blob_type == Type::File {
                            DirEntry::File(MenmosFile::open_raw(client, &hit.id, hit.meta)?)
                        } else {
                            DirEntry::Directory(MenmosDirectory::open_raw(
                                client, &hit.id, hit.meta,
                            )?)
                        };
                        Ok(entry)
                    }
                }),
        )
    }

    /// Get whether this directory has any children.
    pub async fn is_empty(&self) -> Result<bool> {
        let query = Query::default().and_parent(&self.blob_id).with_size(0);
        let results = self
            .client
            .query(query)
            .await
            .map_err(|_| FsError::DirListError)?;

        Ok(results.total == 0)
    }
}
