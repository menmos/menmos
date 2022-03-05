use async_trait::async_trait;

use interface::StorageNodeInfo;

use menmos_std::collections::ConcurrentList;

use super::routing_policy::RoutingPolicy;

#[derive(Default)]
pub struct RoundRobinPolicy {
    list: ConcurrentList<StorageNodeInfo>,
}

#[async_trait]
impl RoutingPolicy for RoundRobinPolicy {
    async fn add_node(&self, node: StorageNodeInfo) {
        self.list.push_back(node)
    }

    async fn update_node(&self, _node: StorageNodeInfo) {}

    async fn get_candidate(&self) -> Option<String> {
        self.list.fetch_swap().map(|n| n.id)
    }

    async fn prune_last(&self) -> Option<String> {
        self.list.pop_back().map(|n| n.id)
    }
}
