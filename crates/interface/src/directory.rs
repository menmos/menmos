use std::collections::HashMap;
use std::net::IpAddr;

use anyhow::Result;

use async_trait::async_trait;

use serde::{Deserialize, Serialize};

use crate::{message::directory_node::Query, BlobMeta};

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

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct QueryResponse {
    pub count: usize,
    pub total: usize,
    pub hits: Vec<Hit>,
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
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ListMetadataResponse {
    pub tags: HashMap<String, usize>,
    pub meta: HashMap<String, HashMap<String, usize>>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ListMetadataRequest {
    /// Optionally filter which tags to return (defaults to all).
    pub tags: Option<Vec<String>>,

    /// Optionally filter which keys to return (defaults to all). [e.g. "filetype"]
    pub meta_keys: Option<Vec<String>>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ListStorageNodesResponse {
    pub storage_nodes: Vec<StorageNodeInfo>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RegisterStorageNodeResponse {
    pub rebuild_requested: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct GetMetaResponse {
    pub meta: Option<BlobMeta>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct LoginResponse {
    pub token: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RegisterRequest {
    pub username: String,
    pub password: String,
}

#[async_trait]
pub trait DirectoryNode {
    async fn add_blob(&self, blob_id: &str, meta: BlobMeta) -> Result<StorageNodeInfo>;
    async fn get_blob_meta(&self, blob_id: &str) -> Result<Option<BlobMeta>>;
    async fn index_blob(&self, blob_id: &str, meta: BlobMeta, storage_node_id: &str) -> Result<()>;
    async fn delete_blob(&self, blob_id: &str) -> Result<Option<StorageNodeInfo>>;

    async fn register_storage_node(
        &self,
        def: StorageNodeInfo,
    ) -> Result<RegisterStorageNodeResponse>;
    async fn get_blob_storage_node(&self, blob_id: &str) -> Result<Option<StorageNodeInfo>>;

    async fn commit(&self) -> Result<()>;
    async fn start_rebuild(&self) -> Result<()>;
    async fn rebuild_complete(&self, storage_node_id: &str) -> Result<()>;

    async fn query(&self, q: &Query) -> Result<QueryResponse>;
    async fn list_metadata(&self, r: &ListMetadataRequest) -> Result<ListMetadataResponse>;
    async fn list_storage_nodes(&self) -> Result<Vec<StorageNodeInfo>>;

    async fn login(&self, user: &str, password: &str) -> Result<bool>;
    async fn register(&self, user: &str, password: &str) -> Result<()>;
}
