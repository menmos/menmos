use async_trait::async_trait;

use interface::StorageNodeInfo;

use menmos_std::collections::ConcurrentHashMap;

use super::routing_policy::RoutingPolicy;

pub struct MinSizePolicy {
    data: ConcurrentHashMap<String, u64>,
}

impl MinSizePolicy {
    pub fn new() -> Self {
        Self {
            data: ConcurrentHashMap::new(),
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
        self.data.insert(node.id.clone(), node.available_space);
    }

    async fn update_node(&self, node: StorageNodeInfo) {
        self.add_node(node).await
    }

    async fn get_candidate(&self) -> Option<String> {
        let data = self.data.get_all();
        data.into_iter()
            .max_by_key(|(_node_id, free_space)| *free_space)
            .map(|(node_id, _free_space)| node_id)
    }

    async fn prune_last(&self) -> Option<String> {
        if let Some(candidate) = self.get_candidate().await {
            self.data.remove(&candidate);
            Some(candidate)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;

    use anyhow::Result;
    use interface::RedirectInfo;

    use super::*;

    fn get_storage_node(name: &str, available_space: u64) -> StorageNodeInfo {
        StorageNodeInfo {
            id: String::from(name),
            redirect_info: RedirectInfo::Static {
                static_address: IpAddr::from([127, 0, 0, 1]),
            },
            port: 4242,
            size: 0,
            available_space,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn policy_selects_node_with_most_free_space() -> Result<()> {
        let policy = MinSizePolicy::default();

        policy.add_node(get_storage_node("a", 200)).await;
        policy.add_node(get_storage_node("b", 50)).await;

        let candidate = policy.get_candidate().await.unwrap();

        assert_eq!(&candidate, "a");

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn node_updates_changes_selected_candidate() -> Result<()> {
        let policy = MinSizePolicy::default();

        let node_a = get_storage_node("a", 200);
        let mut node_b = get_storage_node("b", 300);

        policy.add_node(node_a.clone()).await;
        policy.add_node(node_b.clone()).await;

        // validate the first candidate is node b.
        let candidate = policy.get_candidate().await.unwrap();
        assert_eq!(&candidate, "b");

        node_b.available_space = 100;
        policy.update_node(node_b).await;

        // validate the first candidate is now node a.
        let candidate = policy.get_candidate().await.unwrap();
        assert_eq!(&candidate, "a");

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prune_last_removes_best_candidate() -> Result<()> {
        let policy = MinSizePolicy::default();

        policy.add_node(get_storage_node("a", 200)).await;
        policy.add_node(get_storage_node("b", 100)).await;

        let candidate = policy.get_candidate().await.unwrap();
        assert_eq!(&candidate, "a");

        assert_eq!(policy.prune_last().await, Some(String::from("a")));

        // make sure "a" isn't in anymore
        let candidate = policy.get_candidate().await.unwrap();
        assert_eq!(&candidate, "b");

        // pruning again should remove b.
        assert_eq!(policy.prune_last().await, Some(String::from("b")));

        // pruning again should return none
        assert_eq!(policy.prune_last().await, None);

        Ok(())
    }
}
