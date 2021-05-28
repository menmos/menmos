use anyhow::{anyhow, Result};

use chrono::{DateTime, Duration, Utc};

use interface::{BlobMetaRequest, RoutingConfig, StorageNodeInfo};

use menmos_std::collections::{AsyncHashMap, AsyncList};

const NODE_FORGET_DURATION_SECONDS: i64 = 60;

pub struct NodeRouter {
    storage_nodes: AsyncHashMap<String, (StorageNodeInfo, DateTime<Utc>)>,
    round_robin: AsyncList<String>,

    node_forget_duration: Duration,
}

impl NodeRouter {
    pub fn new() -> Self {
        Self {
            storage_nodes: AsyncHashMap::new(),
            round_robin: Default::default(),
            node_forget_duration: Duration::seconds(NODE_FORGET_DURATION_SECONDS),
        }
    }

    async fn prune_last_node(&self) {
        if let Some(node_id) = self.round_robin.pop_back().await {
            self.storage_nodes.remove(&node_id).await;
        } else {
            log::warn!("called pruned_last_node with an empty node list");
        }
    }

    async fn get_node_if_fresh(&self, node_id: &str) -> Option<StorageNodeInfo> {
        if let Some((node_info, seen_at)) = self.storage_nodes.get(node_id).await {
            if Utc::now() - seen_at > self.node_forget_duration {
                // Node is expired.
                None
            } else {
                Some(node_info)
            }
        } else {
            // Node doesn't exist.
            None
        }
    }

    pub async fn add_node(&self, storage_node: StorageNodeInfo) {
        let node_id = storage_node.id.clone();

        let already_existed = self
            .storage_nodes
            .insert(storage_node.id.clone(), (storage_node, chrono::Utc::now()))
            .await
            .is_some();

        if !already_existed {
            self.round_robin.push_back(node_id).await;
        }
    }

    pub async fn get_node(&self, node_id: &str) -> Option<StorageNodeInfo> {
        self.get_node_if_fresh(node_id).await
    }

    pub async fn list_nodes(&self) -> Vec<StorageNodeInfo> {
        self.storage_nodes
            .get_all()
            .await
            .into_iter()
            .map(|(_k, (node_info, _last_seen))| node_info)
            .collect()
    }

    async fn route_routing_key(
        &self,
        meta_request: &BlobMetaRequest,
        routing_config: &Option<RoutingConfig>,
    ) -> Result<Option<StorageNodeInfo>> {
        let routed_storage_node_maybe = if let Some(cfg) = routing_config {
            meta_request
                .metadata
                .get(&cfg.routing_key)
                .and_then(|field_value| cfg.routes.get(field_value).cloned())
        } else {
            None
        };

        if let Some(storage_node_id) = routed_storage_node_maybe {
            self.get_node(&storage_node_id)
                .await
                .map(Some)
                .ok_or_else(|| {
                    anyhow!("routing configuration routes to node '{}' but node is unreachable")
                })
        } else {
            Ok(None)
        }
    }

    async fn route_round_robin(&self) -> Result<StorageNodeInfo> {
        loop {
            let node_id = self
                .round_robin
                .pop_front()
                .await
                .ok_or_else(|| anyhow!("no storage node defined"))?;

            self.round_robin.push_back(node_id.clone()).await;

            if let Some(node) = self.get_node_if_fresh(&node_id).await {
                return Ok(node);
            } else {
                self.prune_last_node().await;
            }
        }
    }

    pub async fn route_blob(
        &self,
        _blob_id: &str,
        meta_request: &BlobMetaRequest,
        routing_config: &Option<RoutingConfig>,
    ) -> Result<StorageNodeInfo> {
        match self.route_routing_key(meta_request, routing_config).await? {
            Some(v) => Ok(v),
            None => self.route_round_robin().await,
        }
    }
}
