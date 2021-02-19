use std::ffi::OsStr;

use menmos_client::{Query, Type};

use crate::MenmosFS;

use super::{Error, Result};

impl MenmosFS {
    async fn rm_rf(&self, blob_id: &str) -> anyhow::Result<()> {
        let mut working_stack = vec![(String::from(blob_id), Type::Directory)];

        while !working_stack.is_empty() {
            // Get a new root.
            let (target_id, blob_type) = working_stack.pop().unwrap();

            if blob_type == Type::Directory {
                // List the root's children.
                let results = self
                    .client
                    .query(Query::default().and_parent(&target_id).with_size(5000))
                    .await?;
                for hit in results.hits.into_iter() {
                    working_stack.push((hit.id, hit.meta.blob_type));
                }
            }

            // Delete the root.
            // TODO: Batch delete would be a nice addition.
            self.client.delete(target_id).await?;
        }

        Ok(())
    }

    pub async fn rmdir_impl(&self, parent: u64, name: &OsStr) -> Result<()> {
        log::info!("rmdir i{}/{:?}", parent, name);

        let str_name = name.to_string_lossy().to_string();

        let blob_id = self
            .name_to_blobid
            .get(&(parent, str_name))
            .await
            .ok_or(Error::NotFound)?;

        self.rm_rf(&blob_id).await.map_err(|e| {
            log::error!("client error: {}", e);
            Error::IOError
        })?;

        Ok(())
    }
}
