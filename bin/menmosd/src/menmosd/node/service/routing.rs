use std::sync::Arc;

use anyhow::{anyhow, Result};

use async_trait::async_trait;

use interface::{
    DirtyState, MoveInformation, Query, QueryExecutor, RoutingConfig, RoutingConfigState,
    UserManagement,
};

use crate::node::{
    routing::NodeRouter,
    store::iface::{DynDocumentIDStore, DynMetadataStore, DynRoutingStore},
};

const MOVE_REQUEST_BATCH_SIZE: usize = 10;

pub struct RoutingService {
    store: DynRoutingStore,
    document_store: Arc<DynDocumentIDStore>,
    metadata_store: Arc<DynMetadataStore>,
    router: Arc<NodeRouter>,
    users_service: Arc<dyn UserManagement + Send + Sync>,
    query_service: Arc<dyn QueryExecutor + Send + Sync>,
}

impl RoutingService {
    pub fn new(
        store: DynRoutingStore,
        document_store: Arc<DynDocumentIDStore>,
        metadata_store: Arc<DynMetadataStore>,
        router: Arc<NodeRouter>,
        users_service: Arc<dyn UserManagement + Send + Sync>,
        query_service: Arc<dyn QueryExecutor + Send + Sync>,
    ) -> Self {
        Self {
            store,
            document_store,
            metadata_store,
            router,
            users_service,
            query_service,
        }
    }
}

#[async_trait]
impl interface::RoutingConfigManager for RoutingService {
    async fn get_routing_config(&self, user: &str) -> Result<Option<RoutingConfig>> {
        Ok(self
            .store
            .get_routing_config(user)?
            .map(|cfg_state| cfg_state.routing_config))
    }

    async fn set_routing_config(&self, user: &str, routing_config: &RoutingConfig) -> Result<()> {
        self.store.set_routing_config(
            user,
            &RoutingConfigState {
                routing_config: routing_config.clone(),
                state: DirtyState::Dirty,
            },
        )
    }

    async fn delete_routing_config(&self, user: &str) -> Result<()> {
        self.store.delete_routing_config(user)
    }

    async fn get_move_requests(&self, src_node: &str) -> Result<Vec<MoveInformation>> {
        // TODO: Split this into multiple functions once node refactor issue is complete.
        // (because this refactor will split the node in multiple files, making splitting these things
        // easier).

        let mut move_requests = Vec::with_capacity(MOVE_REQUEST_BATCH_SIZE);

        for username in self.users_service.list().await {
            let routing_config_maybe = self.store.get_routing_config(&username)?;
            if routing_config_maybe.is_none() {
                continue;
            }

            let mut routing_config_state = routing_config_maybe.unwrap();
            if routing_config_state.state == DirtyState::Clean {
                continue;
            }

            let routing_field = &routing_config_state.routing_config.routing_key;
            for (field_value, dst_node_id) in routing_config_state.routing_config.routes.iter() {
                if dst_node_id == src_node {
                    // no need to move when src = dst.
                    continue;
                }

                let destination_node_maybe = self.router.get_node(dst_node_id).await;
                if destination_node_maybe.is_none() {
                    // This is not an error, this is simply that the node we need to move blobs to is not
                    // online right now. We'll skip it and try again later.
                    continue;
                }

                let destination_node = destination_node_maybe.unwrap();

                let query = Query::default().and_field(routing_field.clone(), field_value);
                let out_of_place_blobs = self
                    .query_service
                    .query_move_requests(&query, &username, src_node)
                    .await?;

                if out_of_place_blobs.is_empty() {
                    // No pending move requests, this routing config is clean.
                    routing_config_state.state = DirtyState::Clean;
                    self.store
                        .set_routing_config(&username, &routing_config_state)?;
                }

                for blob_id in out_of_place_blobs.into_iter() {
                    // This document is stored on the src node and needs to go to dst_node_id.
                    // We must issue a move request.

                    let doc_idx = self
                        .document_store
                        .get(&blob_id)?
                        .ok_or_else(|| anyhow!("missing document ID"))?;

                    let blob_info = self
                        .metadata_store
                        .get(doc_idx)?
                        .ok_or_else(|| anyhow!("missing blob info for doc ID '{}'", doc_idx))?;

                    move_requests.push(MoveInformation {
                        blob_id,
                        owner_username: blob_info.owner,
                        destination_node: destination_node.clone(),
                    })
                }
            }
        }

        Ok(move_requests)
    }

    async fn flush(&self) -> Result<()> {
        self.store.flush().await?;
        Ok(())
    }
}
