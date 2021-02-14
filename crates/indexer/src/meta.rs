use std::collections::HashMap;

use anyhow::{anyhow, ensure, Result};

use async_trait::async_trait;

use bitvec::prelude::*;

use futures::TryFutureExt;
use interface::BlobMeta;

use crate::iface::{Flush, MetadataMapper};
use crate::BitvecTree;

const META_MAP: &str = "metadata";
const TAG_MAP: &str = "tags";
const KV_MAP: &str = "keyvalue";
const PARENTS_MAP: &str = "parents";

fn kv_to_tag(key: &str, value: &str) -> String {
    format!("{}${}", key, value)
}

fn tag_to_kv(tag: &str) -> Result<(&str, &str)> {
    let splitted: Vec<_> = tag.split('$').collect();
    ensure!(splitted.len() == 2, "invalid kv tag");
    Ok((splitted[0], splitted[1]))
}

pub struct MetadataStore {
    meta_map: sled::Tree,
    tag_map: BitvecTree,
    kv_map: BitvecTree,
    parents_map: BitvecTree,
}

impl MetadataStore {
    pub fn new(db: &sled::Db) -> Result<Self> {
        let meta_map = db.open_tree(META_MAP)?;

        let tag_map = BitvecTree::new(db, TAG_MAP)?;
        let kv_map = BitvecTree::new(db, KV_MAP)?;
        let parents_map = BitvecTree::new(db, PARENTS_MAP)?;

        Ok(Self {
            meta_map,
            tag_map,
            kv_map,
            parents_map,
        })
    }

    fn diff_and_purge_on_meta_update(
        &self,
        old_meta: BlobMeta,
        new_meta: &BlobMeta,
        for_idx: u32,
    ) -> Result<()> {
        for tag in old_meta.tags.into_iter() {
            if !new_meta.tags.contains(&tag) {
                self.tag_map.purge_key(&tag, for_idx)?;
            }
        }

        for (key, value) in old_meta.metadata.into_iter() {
            if new_meta.metadata.get(&key) != Some(&value) {
                self.kv_map.purge_key(&kv_to_tag(&key, &value), for_idx)?;
            }
        }

        for parent in old_meta.parents.into_iter() {
            if !new_meta.parents.contains(&parent) {
                self.parents_map.purge_key(&parent, for_idx)?;
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Flush for MetadataStore {
    async fn flush(&self) -> Result<()> {
        log::debug!("starting flush");
        let meta_flush = self
            .meta_map
            .flush_async()
            .map_err(|e| anyhow!(e.to_string()));
        let tag_flush = self.tag_map.flush();
        let kv_flush = self.kv_map.flush();
        let parents_flush = self.parents_map.flush();

        tokio::try_join!(meta_flush, tag_flush, kv_flush, parents_flush).map(|_u| ())?;

        log::debug!("flush complete");
        Ok(())
    }
}

impl MetadataMapper for MetadataStore {
    fn get(&self, idx: u32) -> Result<Option<BlobMeta>> {
        if let Some(ivec) = self.meta_map.get(idx.to_le_bytes())? {
            let meta: BlobMeta = bincode::deserialize(&ivec)?;
            Ok(Some(meta))
        } else {
            Ok(None)
        }
    }

    fn insert(&self, id: u32, meta: &BlobMeta) -> Result<()> {
        // Validate tags are ok.
        for tag in meta.tags.iter() {
            ensure!(!tag.contains('$'), "tag cannot contain separator");
        }

        let serialized_id = id.to_le_bytes();

        // Save the whole meta for recuperation.
        let serialized = bincode::serialize(&meta)?;
        let r: &[u8] = serialized.as_ref();
        if let Some(last_meta_ivec) = self.meta_map.insert(&serialized_id, r)? {
            // We need to purge tags, parents, and k/v pairs that were _removed_ from the meta
            // so they don't come up in searches anymore.
            let old_meta: BlobMeta = bincode::deserialize(last_meta_ivec.as_ref())?;
            self.diff_and_purge_on_meta_update(old_meta, &meta, id)?;
        }

        // Save tags in the reverse map.
        for tag in meta.tags.iter() {
            self.tag_map.insert(tag, &serialized_id)?;
        }

        // Save key/value fields in the reverse map.
        for (k, v) in meta.metadata.iter().filter(|(_, v)| !v.is_empty()) {
            self.kv_map.insert(&kv_to_tag(k, v), &serialized_id)?;
        }

        // Set parent fields in the reverse map.
        for parent_id in meta.parents.iter() {
            self.parents_map.insert(parent_id, &serialized_id)?;
        }

        Ok(())
    }

    fn load_tag(&self, tag: &str) -> Result<BitVec> {
        self.tag_map.load(tag)
    }

    fn load_key_value(&self, k: &str, v: &str) -> Result<BitVec> {
        self.kv_map.load(&kv_to_tag(k, v))
    }

    fn load_key(&self, k: &str) -> Result<BitVec> {
        // TODO: This is WIP. Computing this at query time is expensive, we could store it at indexing time instead.
        let mut bv = BitVec::default();

        for (_, v_ivec) in self
            .kv_map
            .tree()
            .scan_prefix(format!("{}$", k).as_bytes())
            .filter_map(|f| f.ok())
        {
            let resolved: BitVec = bincode::deserialize_from(v_ivec.as_ref())?;
            let (biggest, smallest) = if bv.len() > resolved.len() {
                (bv, resolved)
            } else {
                (resolved, bv)
            };

            bv = biggest;
            bv |= smallest;
        }

        Ok(bv)
    }

    fn load_children(&self, parent_id: &str) -> Result<BitVec> {
        self.parents_map.load(parent_id)
    }

    fn list_all_tags(&self) -> Result<HashMap<String, usize>> {
        let mut hsh = HashMap::with_capacity(self.tag_map.tree().len());

        for r in self.tag_map.tree().iter() {
            let (tag, vector) = r?;

            let tag_str = String::from_utf8_lossy(tag.as_ref()).to_string();
            let bv: BitVec = bincode::deserialize_from(vector.as_ref())?;
            hsh.insert(tag_str, bv.count_ones());
        }

        Ok(hsh)
    }

    fn list_all_kv_fields(
        &self,
        key_filter: &Option<Vec<String>>,
    ) -> Result<HashMap<String, HashMap<String, usize>>> {
        let mut hsh: HashMap<String, HashMap<String, usize>> = HashMap::new();

        match key_filter {
            Some(filter) => {
                for key in filter.iter() {
                    for (kv_iv, vector) in self
                        .kv_map
                        .tree()
                        .scan_prefix(&format!("{}$", key))
                        .filter_map(|entry| entry.ok())
                    {
                        let tag_str = String::from_utf8_lossy(kv_iv.as_ref()).to_string();
                        let (k, v) = tag_to_kv(&tag_str)?;
                        let bv: BitVec = bincode::deserialize_from(vector.as_ref())?;

                        hsh.entry(k.to_string())
                            .or_insert_with(HashMap::default)
                            .insert(v.to_string(), bv.count_ones());
                    }
                }
            }
            None => {
                // List everything.
                // This will most likely lead to a massive response body - sorry about that.
                for r in self.kv_map.tree().iter() {
                    let (k_v_pair, vector) = r?;
                    let tag_str = String::from_utf8_lossy(k_v_pair.as_ref()).to_string();
                    let (k, v) = tag_to_kv(&tag_str)?;
                    let bv: BitVec = bincode::deserialize_from(vector.as_ref())?;

                    hsh.entry(k.to_string())
                        .or_insert_with(HashMap::default)
                        .insert(v.to_string(), bv.count_ones());
                }
            }
        }
        Ok(hsh)
    }

    fn purge(&self, idx: u32) -> Result<()> {
        let serialized_idx = idx.to_le_bytes();

        // Forget the metadata for this blob.
        self.meta_map.remove(&serialized_idx)?;

        // Purge.
        // TODO: Improve, this is _really_ expensive.
        // This is in O(2n) the number of unique [parents + tags + kv].
        self.tag_map.purge(idx)?;
        self.kv_map.purge(idx)?;
        self.parents_map.purge(idx)?;

        Ok(())
    }

    fn clear(&self) -> Result<()> {
        self.meta_map.clear()?;
        self.tag_map.clear()?;
        self.kv_map.clear()?;
        self.parents_map.clear()?;
        log::debug!("meta index destroyed");
        Ok(())
    }
}
