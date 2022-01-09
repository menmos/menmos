use async_trait::async_trait;

use interface::StorageNodeInfo;

#[async_trait]
pub trait RoutingPolicy {
    async fn add_node(&self, node: StorageNodeInfo);
    async fn update_node(&self, node: StorageNodeInfo);
    async fn get_candidate(&self) -> Option<String>;
    async fn prune_last(&self) -> Option<String>;
}
