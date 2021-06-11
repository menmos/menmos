use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Result};

use async_trait::async_trait;

use interface::{StorageNodeInfo, StorageNodeResponseData};

use crate::node::routing::NodeRouter;

pub struct NodeAdminService {
    rebuild_queue: Mutex<Vec<StorageNodeInfo>>,

    indexer_service: Arc<Box<dyn interface::BlobIndexer + Send + Sync>>,
    router: Arc<NodeRouter>,
}

impl NodeAdminService {
    pub fn new(
        indexer_service: Arc<Box<dyn interface::BlobIndexer + Send + Sync>>,
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
    async fn register_storage_node(&self, def: StorageNodeInfo) -> Result<StorageNodeResponseData> {
        let rebuild_requested = {
            let rebuild_queue_guard = self.rebuild_queue.lock().map_err(|e| anyhow!("{}", e))?;

            if let Some(storage_node) = rebuild_queue_guard.last() {
                storage_node.id == def.id
            } else {
                false
            }
        };

        self.router.add_node(def).await;

        Ok(StorageNodeResponseData { rebuild_requested })
    }

    async fn list_storage_nodes(&self) -> Result<Vec<StorageNodeInfo>> {
        Ok(self.router.list_nodes().await)
    }

    async fn start_rebuild(&self) -> Result<()> {
        let storage_nodes = self.router.list_nodes().await;
        log::info!("starting rebuild for {} nodes", storage_nodes.len());

        log::debug!("nuking the whole index");
        self.indexer_service.clear().await?;

        let mut rebuild_queue_guard = self.rebuild_queue.lock().map_err(|e| anyhow!("{}", e))?;
        (&mut *rebuild_queue_guard).extend(storage_nodes.into_iter());

        Ok(())
    }

    async fn rebuild_complete(&self, storage_node_id: &str) -> Result<()> {
        let mut rebuild_queue_guard = self.rebuild_queue.lock().map_err(|e| anyhow!("{}", e))?;
        rebuild_queue_guard.retain(|item| item.id != storage_node_id);
        log::info!("finished rebuild for node id={}", storage_node_id);
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}
