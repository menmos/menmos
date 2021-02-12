use std::collections::HashMap;

use anyhow::Result;

use async_trait::async_trait;

use bitvec::prelude::*;

use chrono::Utc;

use interface::{BlobMeta, StorageNodeInfo};

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
    fn get(&self, idx: u32) -> Result<Option<BlobMeta>>;
    fn insert(&self, id: u32, meta: &BlobMeta) -> Result<()>;

    fn load_tag(&self, tag: &str) -> Result<BitVec>;

    fn load_key_value(&self, k: &str, v: &str) -> Result<BitVec>;

    fn load_key(&self, k: &str) -> Result<BitVec>;

    fn load_children(&self, parent_id: &str) -> Result<BitVec>;

    fn list_all_tags(&self) -> Result<HashMap<String, usize>>;
    fn list_all_kv_fields(
        &self,
        key_filter: &Option<Vec<String>>,
    ) -> Result<HashMap<String, HashMap<String, usize>>>;

    fn purge(&self, idx: u32) -> Result<()>;
    fn clear(&self) -> Result<()>;
}

pub trait StorageNodeMapper {
    fn get_node(&self, node_id: &str) -> Result<Option<(StorageNodeInfo, chrono::DateTime<Utc>)>>;
    fn get_all_nodes(&self) -> Result<Vec<StorageNodeInfo>>;
    fn write_node(&self, info: StorageNodeInfo, seen_at: chrono::DateTime<Utc>) -> Result<bool>;
    fn delete_node(&self, node_id: &str) -> Result<()>;
    fn get_node_for_blob(&self, blob_id: &str) -> Result<Option<String>>;
    fn set_node_for_blob(&self, blob_id: &str, node_id: String) -> Result<()>;
    fn delete_blob(&self, blob_id: &str) -> Result<Option<String>>;
    fn clear(&self) -> Result<()>;
}

pub trait IndexProvider {
    type DocumentProvider: DocIDMapper;
    type MetadataProvider: MetadataMapper;
    type StorageProvider: StorageNodeMapper;

    fn documents(&self) -> &Self::DocumentProvider;
    fn meta(&self) -> &Self::MetadataProvider;
    fn storage(&self) -> &Self::StorageProvider;
}
