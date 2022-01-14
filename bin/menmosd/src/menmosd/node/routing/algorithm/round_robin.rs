use async_trait::async_trait;

use interface::StorageNodeInfo;

use menmos_std::collections::AsyncList;

use super::routing_policy::RoutingPolicy;

#[derive(Default)]
pub struct RoundRobinPolicy {
    list: AsyncList<StorageNodeInfo>,
}

#[async_trait]
impl RoutingPolicy for RoundRobinPolicy {
    async fn add_node(&self, node: StorageNodeInfo) {
        self.list.push_back(node).await
    }

    async fn update_node(&self, _node: StorageNodeInfo) {}

    async fn get_candidate(&self) -> Option<String> {
        self.list.fetch_swap().await.map(|n| n.id)
    }

    async fn prune_last(&self) -> Option<String> {
        self.list.pop_back().await.map(|n| n.id)
    }
}
