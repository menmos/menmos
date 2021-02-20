use std::collections::HashMap;
use std::net::IpAddr;

use anyhow::Result;

use async_trait::async_trait;

use rapidquery::Expression;

use serde::{Deserialize, Serialize};

use crate::{BlobInfo, BlobMeta};

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

#[derive(Clone, Debug, Deserialize, Hash, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Query {
    pub expression: Expression,
    pub from: usize,
    pub size: usize,
    pub sign_urls: bool,
    pub facets: bool, // TODO: Permit requesting facets for specific tags instead of doing it for all.
}

impl Query {
    pub fn with_expression<S: Into<String>>(mut self, expression: S) -> Result<Self> {
        self.expression = Expression::parse(expression.into())?;
        Ok(self)
    }

    pub fn and_tag<S: Into<String>>(mut self, tag: S) -> Self {
        let new_expr = Expression::Tag { tag: tag.into() };
        self.expression = Expression::And {
            and: (Box::from(self.expression), Box::from(new_expr)),
        };
        self
    }

    pub fn and_meta<K: Into<String>, V: Into<String>>(mut self, k: K, v: V) -> Self {
        let new_expr = Expression::KeyValue {
            key: k.into(),
            value: v.into(),
        };
        self.expression = Expression::And {
            and: (Box::from(self.expression), Box::from(new_expr)),
        };
        self
    }

    pub fn and_parent<P: Into<String>>(mut self, p: P) -> Self {
        let new_expr = Expression::Parent { parent: p.into() };
        self.expression = Expression::And {
            and: (Box::from(self.expression), Box::from(new_expr)),
        };
        self
    }

    pub fn with_from(mut self, f: usize) -> Self {
        self.from = f;
        self
    }

    pub fn with_size(mut self, s: usize) -> Self {
        self.size = s;
        self
    }

    pub fn with_facets(mut self, f: bool) -> Self {
        self.facets = f;
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
        }
    }
}

#[async_trait]
pub trait DirectoryNode {
    async fn add_blob(&self, blob_id: &str, info: BlobInfo) -> Result<StorageNodeInfo>;
    async fn get_blob_meta(&self, blob_id: &str) -> Result<Option<BlobInfo>>;
    async fn index_blob(&self, blob_id: &str, meta: BlobInfo, storage_node_id: &str) -> Result<()>;
    async fn delete_blob(&self, blob_id: &str) -> Result<Option<StorageNodeInfo>>;

    async fn register_storage_node(&self, def: StorageNodeInfo) -> Result<StorageNodeResponseData>;
    async fn get_blob_storage_node(&self, blob_id: &str) -> Result<Option<StorageNodeInfo>>;

    async fn commit(&self) -> Result<()>;
    async fn start_rebuild(&self) -> Result<()>;
    async fn rebuild_complete(&self, storage_node_id: &str) -> Result<()>;

    async fn query(&self, q: &Query) -> Result<QueryResponse>;
    async fn list_metadata(
        &self,
        tags: Option<Vec<String>>,
        meta_keys: Option<Vec<String>>,
    ) -> Result<MetadataList>;
    async fn list_storage_nodes(&self) -> Result<Vec<StorageNodeInfo>>;

    async fn login(&self, user: &str, password: &str) -> Result<bool>;
    async fn register(&self, user: &str, password: &str) -> Result<()>;
}
