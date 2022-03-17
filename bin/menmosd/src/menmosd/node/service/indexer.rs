use std::sync::Arc;

use anyhow::{ensure, Result};

use async_trait::async_trait;

use interface::{BlobInfo, BlobInfoRequest, StorageNodeInfo};

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

    async fn get_blob_storage_node(&self, blob_id: &str) -> Result<Option<StorageNodeInfo>> {
        // These next two lines could technically be in one line, but since its an async
        // function and index::storage() returns a ref to the storage provider, the borrow checker can't guarantee that there
        // won't be concurrent accesses to the storage provider. Doing it in two lines makes it explicit that the ref. to the
        // storage provider is dropped before the await point.
        let node_id_maybe = self.storage.get_node_for_blob(blob_id)?;
        if let Some(node_id) = node_id_maybe {
            Ok(self.router.get_node(&node_id).await)
        } else {
            Ok(None)
        }
    }

    async fn index_blob(&self, blob_id: &str, info: BlobInfo, storage_node_id: &str) -> Result<()> {
        self.storage
            .set_node_for_blob(blob_id, storage_node_id.to_string())?;

        // TODO: Figure out a way to implement transactions here, so that a failed insert won't pollute the document index..
        let doc_idx = self.documents.insert(blob_id)?;
        self.metadata.insert(doc_idx, &info)?;

        Ok(())
    }

    async fn delete_blob(
        &self,
        blob_id: &str,
        storage_node_id: &str,
    ) -> Result<Option<StorageNodeInfo>> {
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
        // - The BlobIndex (sequential u64) is used by the metadata index to represent document
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
            self.metadata.purge(blob_idx)?;

            ensure!(node_id_maybe.is_some(), "blob '{}' was in the document index but was not assigned to a storage node", blob_id);
        }

        if let Some(node_id) = node_id_maybe {
            // See [4]
            Ok(self.router.get_node(&node_id).await)
        } else {
            Ok(None)
        }
    }

    async fn clear(&self) -> Result<()> {
        self.metadata.clear()?;
        self.documents.clear()?;
        self.storage.clear()?;
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        let (a, b, c) = tokio::join!(
            self.metadata.flush(),
            self.documents.flush(),
            self.storage.flush()
        );

        a?;
        b?;
        c?;

        Ok(())
    }
}
