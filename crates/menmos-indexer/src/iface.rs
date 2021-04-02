use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;

use async_trait::async_trait;

use bitvec::prelude::*;

use interface::BlobInfo;

#[async_trait]
pub trait Flush {
    async fn flush(&self) -> Result<()>;
}

pub trait DocIDMapper {
    fn get_nb_of_docs(&self) -> u32;
    fn insert(&self, doc_id: &str) -> Result<u32>;
    fn get(&self, doc_id: &str) -> Result<Option<u32>>;
    fn lookup(&self, doc_idx: u32) -> Result<Option<String>>;
    fn delete(&self, doc_id: &str) -> Result<Option<u32>>;
    fn get_all_documents_mask(&self) -> Result<BitVec>;
    fn clear(&self) -> Result<()>;
}

pub trait MetadataMapper {
    fn get(&self, idx: u32) -> Result<Option<BlobInfo>>;
    fn insert(&self, id: u32, info: &BlobInfo) -> Result<()>;

    fn load_user_mask(&self, username: &str) -> Result<BitVec>;

    fn load_tag(&self, tag: &str) -> Result<BitVec>;

    fn load_key_value(&self, k: &str, v: &str) -> Result<BitVec>;

    fn load_key(&self, k: &str) -> Result<BitVec>;

    fn load_children(&self, parent_id: &str) -> Result<BitVec>;

    fn list_all_tags(&self, mask: Option<&BitVec>) -> Result<HashMap<String, usize>>;
    fn list_all_kv_fields(
        &self,
        key_filter: &Option<Vec<String>>,
        mask: Option<&BitVec>,
    ) -> Result<HashMap<String, HashMap<String, usize>>>;

    fn purge(&self, idx: u32) -> Result<()>;
    fn clear(&self) -> Result<()>;
}

pub trait RoutingMapper {
    fn get_routing_key(&self, username: &str) -> Result<Option<String>>;
    fn set_routing_key(&self, username: &str, routing_key: &str) -> Result<()>;
    fn delete_routing_key(&self, username: &str) -> Result<()>;
}

pub trait StorageNodeMapper {
    fn get_node_for_blob(&self, blob_id: &str) -> Result<Option<String>>;
    fn set_node_for_blob(&self, blob_id: &str, node_id: String) -> Result<()>;
    fn delete_blob(&self, blob_id: &str) -> Result<Option<String>>;
    fn clear(&self) -> Result<()>;
}

pub trait UserMapper {
    fn add_user(&self, username: &str, password: &str) -> Result<()>;
    fn authenticate(&self, username: &str, password: &str) -> Result<bool>;
    fn has_user(&self, username: &str) -> Result<bool>;
}

pub trait IndexProvider {
    type DocumentProvider: DocIDMapper + Send + Sync;
    type MetadataProvider: MetadataMapper + Send + Sync;
    type RoutingProvider: RoutingMapper + Send + Sync;
    type StorageProvider: StorageNodeMapper + Send + Sync;
    type UserProvider: UserMapper + Send + Sync;

    fn documents(&self) -> Arc<Self::DocumentProvider>;
    fn meta(&self) -> Arc<Self::MetadataProvider>;
    fn routing(&self) -> Arc<Self::RoutingProvider>;
    fn storage(&self) -> Arc<Self::StorageProvider>;
    fn users(&self) -> Arc<Self::UserProvider>;
}
