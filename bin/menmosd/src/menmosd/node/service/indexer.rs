use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, ensure, Result};

use async_trait::async_trait;

use interface::{
    BlobInfo, BlobMetaRequest, FacetResponse, Hit, MetadataList, Query, QueryResponse,
    StorageNodeInfo,
};

use crate::node::{
    routing::NodeRouter,
    store::iface::{DynDocumentIDStore, DynMetadataStore, DynStorageMappingStore},
};

pub struct IndexerService {
    documents: Arc<DynDocumentIDStore>,
    metadata: Arc<DynMetadataStore>,
    storage: Arc<DynStorageMappingStore>,

    routing_service: Arc<Box<dyn interface::RoutingConfigManager + Send + Sync>>,
    router: Arc<NodeRouter>,
}

impl IndexerService {
    pub fn new(
        documents: Arc<DynDocumentIDStore>,
        metadata: Arc<DynMetadataStore>,
        storage: Arc<DynStorageMappingStore>,
        routing_service: Arc<Box<dyn interface::RoutingConfigManager + Send + Sync>>,
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
        meta: BlobMetaRequest,
        username: &str,
    ) -> Result<StorageNodeInfo> {
        let routing_config = self.routing_service.get_routing_config(username).await?;
        self.router
            .route_blob(blob_id, &meta, &routing_config)
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
        let node_id_maybe = self.storage.get_node_for_blob(&blob_id)?;
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
            log::error!("no node found for blob_id={}", blob_id)
        }

        // This is tricky, because since our internal document IDs are sequential, we can't just delete the blob from the index and call it a day.
        // Also, since we have a limit of u32::MAX document IDs, we'd better recycle those deleted IDs so we don't creep
        // towards the limit too fast in delete-heavy implementations.
        //
        // This is the algorithm:
        //  - Delete the BlobID from the storage node index.
        let node_id_maybe = self.storage.delete_blob(blob_id)?;

        //  - Delete the BlobID -> BlobIDX mapping in the document index.
        //  - Add the BlobIDX to a list of "free" indices (up for recycling) kept in the document index.
        if let Some(blob_idx) = self.documents.delete(blob_id)? {
            //  - If the blob exists in the index we need to "purge" the BlobIDX:
            //  - For all tag, k/v and parent values in the metadata index, set to `0` the bit corresponding to the deleted BlobIDX.
            //  - Once a new insert comes, prioritize using a recycled BlobIDX over allocating a new one.
            self.metadata.purge(blob_idx)?;
        }

        if let Some(node_id) = node_id_maybe {
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

    async fn commit(&self) -> Result<()> {
        // TODO: Actually commit
        Ok(())
    }
}
