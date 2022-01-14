use async_trait::async_trait;

use interface::StorageNodeInfo;

use menmos_std::collections::AsyncHashMap;

use super::routing_policy::RoutingPolicy;

pub struct MinSizePolicy {
    data: AsyncHashMap<String, u64>,
}

impl MinSizePolicy {
    pub fn new() -> Self {
        Self {
            data: AsyncHashMap::new(),
        }
    }
}

impl Default for MinSizePolicy {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl RoutingPolicy for MinSizePolicy {
    async fn add_node(&self, node: StorageNodeInfo) {
        self.data
            .insert(node.id.clone(), node.available_space)
            .await;
    }

    async fn update_node(&self, node: StorageNodeInfo) {
        self.add_node(node).await
    }

    async fn get_candidate(&self) -> Option<String> {
        let data = self.data.get_all().await;
        data.into_iter()
            .max_by_key(|(_node_id, free_space)| *free_space)
            .map(|(node_id, _free_space)| node_id)
    }

    async fn prune_last(&self) -> Option<String> {
        if let Some(candidate) = self.get_candidate().await {
            self.data.remove(&candidate).await;
            Some(candidate)
        } else {
            None
        }
    }
}
