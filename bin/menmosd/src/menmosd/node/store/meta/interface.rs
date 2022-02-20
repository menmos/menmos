use std::collections::HashMap;

use anyhow::Result;

use bitvec::prelude::*;

use interface::{BlobInfo, FieldValue};

use crate::node::store::iface::Flush;

pub trait MetadataStore: Flush {
    fn get(&self, idx: u32) -> Result<Option<BlobInfo>>;
    fn insert(&self, id: u32, info: &BlobInfo) -> Result<()>;

    fn load_user_mask(&self, username: &str) -> Result<BitVec>;

    fn load_tag(&self, tag: &str) -> Result<BitVec>;

    fn load_key_value(&self, k: &str, v: &FieldValue) -> Result<BitVec>;

    fn load_key(&self, k: &str) -> Result<BitVec>;

    fn list_all_tags(&self, mask: Option<&BitVec>) -> Result<HashMap<String, usize>>;
    fn list_all_kv_fields(
        &self,
        key_filter: &Option<Vec<String>>,
        mask: Option<&BitVec>,
    ) -> Result<HashMap<String, HashMap<FieldValue, usize>>>;

    fn purge(&self, idx: u32) -> Result<()>;
    fn clear(&self) -> Result<()>;
}
