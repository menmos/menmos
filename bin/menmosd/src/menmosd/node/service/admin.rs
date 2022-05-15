use std::sync::Arc;

use anyhow::Result;

use async_trait::async_trait;

use interface::{StorageNodeInfo, StorageNodeResponseData};

use parking_lot::Mutex;

use crate::node::routing::NodeRouter;

pub struct NodeAdminService {
    rebuild_queue: Mutex<Vec<StorageNodeInfo>>,

    indexer_service: Arc<dyn interface::BlobIndexer + Send + Sync>,
    router: Arc<NodeRouter>,
}

impl NodeAdminService {
    pub fn new(
        indexer_service: Arc<dyn interface::BlobIndexer + Send + Sync>,
        router: Arc<NodeRouter>,
    ) -> Self {
        Self {
            rebuild_queue: Default::default(),
            indexer_service,
            router,
        }
    }
}

#[async_trait]
impl interface::NodeAdminController for NodeAdminService {
    #[tracing::instrument(name = "admin.register_storage_node", skip(self))]
    async fn register_storage_node(&self, def: StorageNodeInfo) -> Result<StorageNodeResponseData> {
        let rebuild_requested = {
            let rebuild_queue_guard = self.rebuild_queue.lock();

            if let Some(storage_node) = rebuild_queue_guard.last() {
                storage_node.id == def.id
            } else {
                false
            }
        };

        self.router.add_node(def).await;

        Ok(StorageNodeResponseData { rebuild_requested })
    }

    #[tracing::instrument(name = "admin.list_storage_nodes", skip(self))]
    async fn list_storage_nodes(&self) -> Result<Vec<StorageNodeInfo>> {
        Ok(self.router.list_nodes().await)
    }

    #[tracing::instrument(name = "admin.start_rebuild", skip(self))]
    async fn start_rebuild(&self) -> Result<()> {
        let storage_nodes = self.router.list_nodes().await;
        tracing::info!(node_count = storage_nodes.len(), "starting rebuild");

        tracing::debug!("nuking the whole index");
        self.indexer_service.clear().await?;

        let mut rebuild_queue_guard = self.rebuild_queue.lock();
        (*rebuild_queue_guard).extend(storage_nodes.into_iter());

        Ok(())
    }

    #[tracing::instrument(name = "admin.rebuild_complete", skip(self))]
    async fn rebuild_complete(&self, storage_node_id: &str) -> Result<()> {
        let mut rebuild_queue_guard = self.rebuild_queue.lock();
        rebuild_queue_guard.retain(|item| item.id != storage_node_id);
        tracing::info!("finished rebuild for node id={}", storage_node_id);
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}
