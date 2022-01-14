use anyhow::{anyhow, Result};

use chrono::{DateTime, Duration, Utc};

use interface::{BlobMetaRequest, RoutingAlgorithm, RoutingConfig, StorageNodeInfo};

use menmos_std::collections::AsyncHashMap;

use super::algorithm::{MinSizePolicy, RoundRobinPolicy, RoutingPolicy};

const NODE_FORGET_DURATION_SECONDS: i64 = 60;

pub struct NodeRouter {
    storage_nodes: AsyncHashMap<String, (StorageNodeInfo, DateTime<Utc>)>,
    routing_policy: Box<dyn RoutingPolicy + Send + Sync>,

    node_forget_duration: Duration,
}

impl NodeRouter {
    pub fn new(routing_algorithm: RoutingAlgorithm) -> Self {
        let routing_policy: Box<dyn RoutingPolicy + Send + Sync> = match routing_algorithm {
            RoutingAlgorithm::RoundRobin => Box::new(RoundRobinPolicy::default()),
            RoutingAlgorithm::MinSize => Box::new(MinSizePolicy::default()),
        };

        Self {
            storage_nodes: AsyncHashMap::new(),
            routing_policy,
            node_forget_duration: Duration::seconds(NODE_FORGET_DURATION_SECONDS),
        }
    }

    async fn prune_last_node(&self) {
        if let Some(node_id) = self.routing_policy.prune_last().await {
            self.storage_nodes.remove(&node_id).await;
        } else {
            tracing::warn!("called pruned_last_node with an empty node list");
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
        let already_existed = self
            .storage_nodes
            .insert(
                storage_node.id.clone(),
                (storage_node.clone(), chrono::Utc::now()),
            )
            .await
            .is_some();

        if !already_existed {
            self.routing_policy.add_node(storage_node).await;
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

    #[tracing::instrument(skip(self, meta_request, routing_config))]
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
                    anyhow!(
                        "routing configuration routes to node '{}' but node is unreachable",
                        &storage_node_id
                    )
                })
        } else {
            tracing::trace!("found no storage node from routing config");
            Ok(None)
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn route_policy(&self) -> Result<StorageNodeInfo> {
        loop {
            let node_id = self
                .routing_policy
                .get_candidate()
                .await
                .ok_or_else(|| anyhow!("no storage node defined"))?;

            if let Some(node) = self.get_node_if_fresh(&node_id).await {
                tracing::trace!("routed to {}", &node.id);
                return Ok(node);
            } else {
                self.prune_last_node().await;
            }
        }
    }

    #[tracing::instrument(level = "trace", skip(self, meta_request, routing_config))]
    pub async fn route_blob(
        &self,
        _blob_id: &str,
        meta_request: &BlobMetaRequest,
        routing_config: &Option<RoutingConfig>,
    ) -> Result<StorageNodeInfo> {
        match self.route_routing_key(meta_request, routing_config).await? {
            Some(v) => Ok(v),
            None => self.route_policy().await,
        }
    }
}
