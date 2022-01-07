use async_trait::async_trait;
use menmos_std::collections::AsyncList;

#[async_trait]
pub trait RoutingPolicy {
    async fn add_node(&self, node_id: String);
    async fn get_candidate(&self) -> Option<String>;
    async fn prune_last(&self) -> Option<String>;
}

#[derive(Default)]
pub struct RoundRobinPolicy {
    list: AsyncList<String>,
}

#[async_trait]
impl RoutingPolicy for RoundRobinPolicy {
    async fn add_node(&self, node_id: String) {
        self.list.push_back(node_id).await
    }

    async fn get_candidate(&self) -> Option<String> {
        self.list.fetch_swap().await
    }

    async fn prune_last(&self) -> Option<String> {
        self.list.pop_back().await
    }
}
