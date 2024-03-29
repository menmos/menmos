use std::sync::Arc;

use anyhow::{ensure, Result};

use async_trait::async_trait;

use interface::{BlobInfo, BlobInfoRequest, StorageNodeInfo};

use menmos_std::tx;

use crate::node::{
    routing::NodeRouter,
    store::iface::{DynDocumentIDStore, DynMetadataStore, DynStorageMappingStore},
};

pub struct IndexerService {
    documents: Arc<DynDocumentIDStore>,
    metadata: Arc<DynMetadataStore>,
    storage: Arc<DynStorageMappingStore>,

    routing_service: Arc<dyn interface::RoutingConfigManager + Send + Sync>,
    router: Arc<NodeRouter>,
}

impl IndexerService {
    pub fn new(
        documents: Arc<DynDocumentIDStore>,
        metadata: Arc<DynMetadataStore>,
        storage: Arc<DynStorageMappingStore>,
        routing_service: Arc<dyn interface::RoutingConfigManager + Send + Sync>,
        router: Arc<NodeRouter>,
    ) -> Self {
        Self {
            documents,
            metadata,
            storage,
            routing_service,
            router,
        }
    }
}

#[async_trait]
impl interface::BlobIndexer for IndexerService {
    #[tracing::instrument(name = "indexer.pick_node", skip(self, info_request))]
    async fn pick_node_for_blob(
        &self,
        blob_id: &str,
        info_request: BlobInfoRequest,
    ) -> Result<StorageNodeInfo> {
        let routing_config = self
            .routing_service
            .get_routing_config(&info_request.owner)
            .await?;
        self.router
            .route_blob(
                blob_id,
                &info_request.meta_request,
                info_request.size,
                &routing_config,
            )
            .await
    }

    #[tracing::instrument(name = "indexer.get_blob_meta", skip(self))]
    async fn get_blob_meta(&self, blob_id: &str, username: &str) -> Result<Option<BlobInfo>> {
        let blob_idx_maybe = self.documents.get(blob_id)?;
        let blob_info_maybe = blob_idx_maybe
            .map(|i| self.metadata.get(i))
            .transpose()?
            .flatten();

        if let Some(info) = &blob_info_maybe {
            if info.owner != username {
                return Ok(None);
            }
        }

        Ok(blob_info_maybe)
    }

    #[tracing::instrument(name = "indexer.get_blob_storage_node", skip(self))]
    async fn get_blob_storage_node(&self, blob_id: &str) -> Result<Option<StorageNodeInfo>> {
        // These next two lines could technically be in one line, but since its an async
        // function and index::storage() returns a ref to the storage provider, the borrow checker can't guarantee that there
        // won't be concurrent accesses to the storage provider while awaiting Router::get_node.
        // Doing it in two lines makes it explicit that the ref to the storage provider ref is dropped
        // before the await point.
        let node_id_maybe = self.storage.get_node_for_blob(blob_id)?;
        if let Some(node_id) = node_id_maybe {
            Ok(self.router.get_node(&node_id).await)
        } else {
            Ok(None)
        }
    }

    #[tracing::instrument(name = "indexer.index_blob", skip(self, info))]
    async fn index_blob(&self, blob_id: &str, info: BlobInfo, storage_node_id: &str) -> Result<()> {
        tx::try_rollback(move |tx_state| async move {
            let old_node = self
                .storage
                .set_node_for_blob(blob_id, storage_node_id.to_string())?;

            if old_node != Some(String::from(storage_node_id)) {
                // Revert the storage node change, if any.
                tx_state
                    .complete({
                        let blob_id = String::from(blob_id);
                        let storage = self.storage.clone();
                        Box::pin(async move {
                            if let Some(node) = old_node {
                                storage.set_node_for_blob(&blob_id, node)?;
                            }
                            Ok(())
                        })
                    })
                    .await;
            }

            let doc_idx = self.documents.insert(blob_id)?;

            // Panicking here isn't ideal, but I feel it is a reasonable compromise.
            // Contrary to the amphora index, the metadata index allows concurrent mutations,
            // on the same key making it extremely difficult to revert an change should a later operation fail.
            //
            // In addition, the only real way metadata indexing might fail once we make it here
            // is if there is a disk issue or a corruption in the underlying sled database.
            // In both cases, we can't possibly expect to recover from this gracefully,
            // so a panic is acceptable.
            self.metadata
                .insert(doc_idx, &info)
                .expect("metadata indexing shouldn't fail");

            Ok(())
        })
        .await
    }

    #[tracing::instrument(name = "indexer.delete_blob", skip(self))]
    async fn delete_blob(
        &self,
        blob_id: &str,
        storage_node_id: &str,
    ) -> Result<Option<StorageNodeInfo>> {
        // FIXME: Use sled transactions
        let node_maybe = self.storage.get_node_for_blob(blob_id)?;

        if let Some(node) = node_maybe {
            ensure!(
                node == storage_node_id,
                "blob node is different from the storage node id that requested deletion"
            )
        } else {
            tracing::error!("no node found for blob_id={}", blob_id);
        }

        // Deleting a document is a multi-step process.
        // Quick recap of the index structures related to documents:
        // - The storage node index maps the blobID (its UUID) to the ID of the storage node on
        //   which it is stored.
        // - The documents index keeps a bidirectional map of BlobID <=> BlobIndex
        // - The BlobIndex (sequential u32) is used by the metadata index to represent document
        //   sets as bitvectors (the index of a given document is given by its DocumentIndex, and
        //   the corresponding bit is true if that document is present in the set).
        //      - e.g. an index of five documents where the last document contains the tag hello
        //        will look like {"hello" => [0, 0, 0, 0, 1]}
        //
        // Now, for the delete process.
        // 1. Remove the BlobID from the storage node index, this "unlinks" the blob from the
        //    storage node that contains it.
        //
        // 2. Remove the association between the BlobID and its BlobIndex.
        //  2.1. If the BlobID was in the index, we need to add its BlobIndex to a list of free
        //       indices so it can be reused. There are only u32::MAX BlobIndices available so we
        //       need to be smart so that delete-heavy usecases dont overflow the ID limit.
        //
        // 3. If the BlobIndex exists in the index, we need to "purge" uses of this BlobIndex.
        //    If we didn't, recycling a BlobIndex would carry over the metadata of the previous
        //    blob that had this index.
        //
        //    The recycling can be done either right now, or "lazily" when the document is
        //    recycled in the future. We're doing it now.
        //  3.1. For all tag and field values in the metadata index, set to `0`  the bit
        //       corresponding to this BlobIndex.
        //
        // 4. Return the ID of the storage node containing the blob so it can be deleted there
        //    also.

        // See [1]
        let node_id_maybe = self.storage.delete_blob(blob_id)?;

        // See [2, 2.1]
        if let Some(blob_idx) = self.documents.delete(blob_id)? {
            // See [3.1]
            self.metadata
                .purge(blob_idx)
                .expect("metadata indexing shouldn't fail");

            ensure!(
                node_id_maybe.is_some(),
                "blob '{}' was in the document index but was not assigned to a storage node",
                blob_id
            );
        }

        if let Some(node_id) = node_id_maybe {
            // See [4]
            Ok(self.router.get_node(&node_id).await)
        } else {
            Ok(None)
        }
    }

    #[tracing::instrument(name = "indexer.clear", skip(self))]
    async fn clear(&self) -> Result<()> {
        self.metadata.clear()?;
        self.documents.clear()?;
        self.storage.clear()?;
        Ok(())
    }

    #[tracing::instrument(name = "indexer.flush", skip(self))]
    async fn flush(&self) -> Result<()> {
        tokio::try_join!(
            self.metadata.flush(),
            self.documents.flush(),
            self.storage.flush()
        )?;
        Ok(())
    }
}
