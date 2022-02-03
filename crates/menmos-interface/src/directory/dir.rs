use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;

use anyhow::Result;

use async_trait::async_trait;

pub use rapidquery::Expression;

use serde::{Deserialize, Serialize};

use crate::{BlobInfo, BlobInfoRequest, BlobMeta, BlobMetaRequest, ExpressionField};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Hit {
    pub id: String,
    pub meta: BlobMeta,
    pub url: String,
}

impl Hit {
    pub fn new(id: String, meta: BlobMeta, url: String) -> Self {
        Self { id, meta, url }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FacetResponse {
    pub tags: HashMap<String, u64>,
    pub meta: HashMap<String, HashMap<String, u64>>,
}

/// The results of a query.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct QueryResponse {
    /// The number of hits returned with this response.
    pub count: usize,

    /// The total number of hits.
    pub total: usize,

    /// The query hits.
    pub hits: Vec<Hit>,

    /// The facets computed for this query.
    ///
    /// Returned only if requested with the query.
    pub facets: Option<FacetResponse>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum RedirectInfo {
    Automatic {
        public_address: IpAddr,
        local_address: IpAddr,
        subnet_mask: IpAddr,
    },
    Static {
        static_address: IpAddr,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct StorageNodeInfo {
    pub id: String,
    pub redirect_info: RedirectInfo,
    pub port: u16,
    pub size: u64,
    pub available_space: u64,
}

/// Data sent back to the storage node from the directory.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct StorageNodeResponseData {
    pub rebuild_requested: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MetadataList {
    pub tags: HashMap<String, usize>,
    pub meta: HashMap<String, HashMap<String, usize>>,
}

/// A sorting order.
#[derive(Clone, Debug, Deserialize, Hash, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SortOrder {
    CreationAscending,
    CreationDescending,
}

/// A query that can be sent to a Menmos cluster.
#[derive(Clone, Debug, Deserialize, Hash, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Query {
    /// The query expression.
    pub expression: Expression<ExpressionField>,
    pub from: usize,
    pub size: usize,
    pub sign_urls: bool,
    pub facets: bool,
    // TODO: Permit requesting facets for specific tags instead of doing it for all.
    pub sort_order: SortOrder,
}

impl Query {
    pub fn with_expression<S: Into<String>>(mut self, expression: S) -> Result<Self> {
        self.expression = Expression::parse(expression.into())?;
        Ok(self)
    }

    #[must_use]
    pub fn and_tag<S: Into<String>>(mut self, tag: S) -> Self {
        let new_expr = Expression::Field(ExpressionField::Tag { tag: tag.into() });
        self.expression = Expression::And {
            and: (Box::from(self.expression), Box::from(new_expr)),
        };
        self
    }

    #[must_use]
    pub fn and_meta<K: Into<String>, V: Into<String>>(mut self, k: K, v: V) -> Self {
        let new_expr = Expression::Field(ExpressionField::KeyValue {
            key: k.into(),
            value: v.into(),
        });
        self.expression = Expression::And {
            and: (Box::from(self.expression), Box::from(new_expr)),
        };
        self
    }

    #[must_use]
    pub fn and_parent<P: Into<String>>(mut self, p: P) -> Self {
        let new_expr = Expression::Field(ExpressionField::Parent { parent: p.into() });
        self.expression = Expression::And {
            and: (Box::from(self.expression), Box::from(new_expr)),
        };
        self
    }

    #[must_use]
    pub fn with_from(mut self, f: usize) -> Self {
        self.from = f;
        self
    }

    #[must_use]
    pub fn with_size(mut self, s: usize) -> Self {
        self.size = s;
        self
    }

    #[must_use]
    pub fn with_facets(mut self, f: bool) -> Self {
        self.facets = f;
        self
    }

    pub fn with_sort_order(mut self, sort_order: SortOrder) -> Self {
        self.sort_order = sort_order;
        self
    }
}

impl Default for Query {
    fn default() -> Self {
        Query {
            expression: Default::default(),
            from: 0,
            size: 30,
            sign_urls: true,
            facets: false,
            sort_order: SortOrder::CreationAscending,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RoutingConfig {
    /// The field name to use for routing.
    pub routing_key: String,

    /// A map of field values -> storage node IDs.
    pub routes: HashMap<String, String>,
}

impl RoutingConfig {
    pub fn new<S: Into<String>>(key: S) -> Self {
        Self {
            routing_key: key.into(),
            routes: HashMap::new(),
        }
    }

    #[must_use]
    pub fn with_route<K: Into<String>, V: Into<String>>(
        mut self,
        field_value: K,
        storage_node_id: V,
    ) -> Self {
        self.routes
            .insert(field_value.into(), storage_node_id.into());
        self
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum DirtyState {
    Dirty,
    Clean,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RoutingConfigState {
    pub routing_config: RoutingConfig,
    pub state: DirtyState,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MoveInformation {
    pub blob_id: String,
    pub owner_username: String,
    pub destination_node: StorageNodeInfo,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RoutingAlgorithm {
    RoundRobin,
    MinSize,
}

#[async_trait]
pub trait BlobIndexer {
    async fn pick_node_for_blob(
        &self,
        blob_id: &str,
        info_request: BlobInfoRequest,
    ) -> Result<StorageNodeInfo>;
    async fn get_blob_meta(&self, blob_id: &str, user: &str) -> Result<Option<BlobInfo>>;
    async fn index_blob(&self, blob_id: &str, meta: BlobInfo, storage_node_id: &str) -> Result<()>;
    async fn delete_blob(&self, blob_id: &str, username: &str) -> Result<Option<StorageNodeInfo>>;
    async fn get_blob_storage_node(&self, blob_id: &str) -> Result<Option<StorageNodeInfo>>;
    async fn clear(&self) -> Result<()>;
    async fn flush(&self) -> Result<()>;
}

#[async_trait]
pub trait RoutingConfigManager {
    async fn get_routing_config(&self, user: &str) -> Result<Option<RoutingConfig>>;
    async fn set_routing_config(&self, user: &str, config: &RoutingConfig) -> Result<()>;
    async fn delete_routing_config(&self, user: &str) -> Result<()>;

    async fn get_move_requests(&self, src_node: &str) -> Result<Vec<MoveInformation>>;

    async fn flush(&self) -> Result<()>;
}

#[async_trait]
pub trait NodeAdminController {
    async fn register_storage_node(&self, def: StorageNodeInfo) -> Result<StorageNodeResponseData>;
    async fn list_storage_nodes(&self) -> Result<Vec<StorageNodeInfo>>;

    async fn start_rebuild(&self) -> Result<()>;
    async fn rebuild_complete(&self, storage_node_id: &str) -> Result<()>;
    async fn flush(&self) -> Result<()>;
}

#[async_trait]
pub trait UserManagement {
    async fn login(&self, user: &str, password: &str) -> Result<bool>;
    async fn register(&self, user: &str, password: &str) -> Result<()>;
    async fn has_user(&self, user: &str) -> Result<bool>;
    async fn list(&self) -> Vec<String>;
    async fn flush(&self) -> Result<()>;
}

#[async_trait]
pub trait QueryExecutor {
    async fn query(&self, q: &Query, username: &str) -> Result<QueryResponse>;
    async fn query_move_requests(
        &self,
        query: &Query,
        username: &str,
        src_node: &str,
    ) -> Result<Vec<String>>;
    async fn list_metadata(
        &self,
        tags: Option<Vec<String>>,
        meta_keys: Option<Vec<String>>,
        username: &str,
    ) -> Result<MetadataList>;
}

#[async_trait]
pub trait DirectoryNode {
    fn indexer(&self) -> Arc<dyn BlobIndexer + Send + Sync>;
    fn routing(&self) -> Arc<dyn RoutingConfigManager + Send + Sync>;
    fn admin(&self) -> Arc<dyn NodeAdminController + Send + Sync>;
    fn user(&self) -> Arc<dyn UserManagement + Send + Sync>;
    fn query(&self) -> Arc<dyn QueryExecutor + Send + Sync>;

    async fn flush(&self) -> Result<()>;
}
