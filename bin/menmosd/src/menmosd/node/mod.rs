use anyhow::Result;
use std::path::Path;

mod node_impl;
mod routing;
mod service;
mod store;

pub use node_impl::Directory;

use crate::Config;

use self::{
    routing::NodeRouter,
    service::{IndexerService, NodeAdminService, QueryService, RoutingService, UserService},
    store::{
        iface::{DynDocumentIDStore, DynMetadataStore, DynStorageMappingStore},
        sled::{
            SledDocumentIdStore, SledMetadataStore, SledRoutingStore, SledStorageMappingStore,
            SledUserStore,
        },
    },
};
use std::sync::Arc;
use tokio::time::Instant;

#[tracing::instrument]
fn init_sled(path: &Path) -> Result<sled::Db> {
    let start = Instant::now();
    let db = sled::open(path)?;
    let load_duration = Instant::now().duration_since(start);
    tracing::debug!(time=?load_duration, "complete");
    Ok(db)
}

#[tracing::instrument(skip(c))]
pub fn make_node(c: &Config) -> Result<Directory> {
    tracing::debug!("begin node init");
    let db = init_sled(&c.node.db_path)?;

    let router = Arc::from(NodeRouter::new(c.node.routing_algorithm));

    // Init the indices
    let documents_idx: Arc<DynDocumentIDStore> =
        Arc::new(Box::from(SledDocumentIdStore::new(&db)?));

    let metadata_idx: Arc<DynMetadataStore> = Arc::new(Box::from(SledMetadataStore::new(&db)?));
    let storage_idx: Arc<DynStorageMappingStore> =
        Arc::new(Box::from(SledStorageMappingStore::new(&db)?));
    let routing_idx = Box::from(SledRoutingStore::new(&db)?);
    let user_idx = Box::from(SledUserStore::new(&db)?);

    // Init the services.
    let users_service: Arc<dyn interface::UserManagement + Send + Sync> =
        Arc::new(UserService::new(user_idx));
    let query_service: Arc<dyn interface::QueryExecutor + Send + Sync> =
        Arc::new(QueryService::new(
            documents_idx.clone(),
            metadata_idx.clone(),
            storage_idx.clone(),
        ));
    let routing_service: Arc<dyn interface::RoutingConfigManager + Send + Sync> =
        Arc::new(RoutingService::new(
            routing_idx,
            documents_idx.clone(),
            metadata_idx.clone(),
            router.clone(),
            users_service.clone(),
            query_service.clone(),
        ));
    let indexer_service: Arc<dyn interface::BlobIndexer + Send + Sync> =
        Arc::new(IndexerService::new(
            documents_idx,
            metadata_idx,
            storage_idx,
            routing_service.clone(),
            router.clone(),
        ));

    let admin_service: Arc<dyn interface::NodeAdminController + Send + Sync> =
        Arc::new(NodeAdminService::new(indexer_service.clone(), router));

    let node = Directory::new(
        indexer_service,
        routing_service,
        admin_service,
        users_service,
        query_service,
    );

    tracing::debug!("node init complete");

    Ok(node)
}

#[cfg(test)]
mod test;
