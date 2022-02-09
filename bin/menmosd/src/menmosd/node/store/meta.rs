use anyhow::{anyhow, ensure, Result};
use async_trait::async_trait;
use bitvec::prelude::*;
use futures::TryFutureExt;
use interface::BlobInfo;
use std::collections::HashMap;

use super::bitvec_tree::BitvecTree;
use super::iface::Flush;

pub trait MetadataStore: Flush {
    fn get(&self, idx: u32) -> Result<Option<BlobInfo>>;
    fn insert(&self, id: u32, info: &BlobInfo) -> Result<()>;

    fn load_user_mask(&self, username: &str) -> Result<BitVec>;

    fn load_tag(&self, tag: &str) -> Result<BitVec>;

    fn load_key_value(&self, k: &str, v: &str) -> Result<BitVec>;

    fn load_key(&self, k: &str) -> Result<BitVec>;

    fn list_all_tags(&self, mask: Option<&BitVec>) -> Result<HashMap<String, usize>>;
    fn list_all_kv_fields(
        &self,
        key_filter: &Option<Vec<String>>,
        mask: Option<&BitVec>,
    ) -> Result<HashMap<String, HashMap<String, usize>>>;

    fn purge(&self, idx: u32) -> Result<()>;
    fn clear(&self) -> Result<()>;
}

const META_MAP: &str = "metadata";
const TAG_MAP: &str = "tags";
const KV_MAP: &str = "keyvalue";
const USER_MASK_MAP: &str = "users";

fn kv_to_tag(key: &str, value: &str) -> String {
    format!("{}${}", key, value)
}

fn tag_to_kv(tag: &str) -> Result<(&str, &str)> {
    let splitted: Vec<_> = tag.split('$').collect();
    ensure!(splitted.len() == 2, "invalid kv tag");
    Ok((splitted[0], splitted[1]))
}

pub struct SledMetadataStore {
    meta_map: sled::Tree,
    tag_map: BitvecTree,
    kv_map: BitvecTree,
    user_mask_map: BitvecTree,
}

impl SledMetadataStore {
    pub fn new(db: &sled::Db) -> Result<Self> {
        let meta_map = db.open_tree(META_MAP)?;

        let tag_map = BitvecTree::new(db, TAG_MAP)?;
        let kv_map = BitvecTree::new(db, KV_MAP)?;
        let user_mask_map = BitvecTree::new(db, USER_MASK_MAP)?;

        Ok(Self {
            meta_map,
            tag_map,
            kv_map,
            user_mask_map,
        })
    }

    #[tracing::instrument(level = "trace", skip(self, old_meta, new_meta))]
    fn diff_and_purge_on_meta_update(
        &self,
        old_meta: BlobInfo,
        new_meta: &BlobInfo,
        for_idx: u32,
    ) -> Result<()> {
        for tag in old_meta.meta.tags.into_iter() {
            if !new_meta.meta.tags.contains(&tag) {
                self.tag_map.purge_key(&tag, for_idx)?;
                tracing::trace!(tag = %tag, index = for_idx, "purged tag");
            }
        }

        for (key, value) in old_meta.meta.fields.into_iter() {
            if new_meta.meta.fields.get(&key) != Some(&value) {
                self.kv_map.purge_key(&kv_to_tag(&key, &value), for_idx)?;
                tracing::trace!(key = %key, value = %value, index = for_idx, "purged key-value");
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Flush for SledMetadataStore {
    async fn flush(&self) -> Result<()> {
        tracing::debug!("starting flush");
        let meta_flush = self
            .meta_map
            .flush_async()
            .map_err(|e| anyhow!(e.to_string()));
        let tag_flush = self.tag_map.flush();
        let kv_flush = self.kv_map.flush();

        tokio::try_join!(meta_flush, tag_flush, kv_flush).map(|_u| ())?;

        tracing::debug!("flush complete");
        Ok(())
    }
}

impl MetadataStore for SledMetadataStore {
    #[tracing::instrument(level = "trace", skip(self))]
    fn get(&self, idx: u32) -> Result<Option<BlobInfo>> {
        if let Some(ivec) = self.meta_map.get(idx.to_le_bytes())? {
            let info: BlobInfo = bincode::deserialize(&ivec)?;
            Ok(Some(info))
        } else {
            tracing::trace!(index = idx, "not found");
            Ok(None)
        }
    }

    #[tracing::instrument(level = "trace", skip(self, info))]
    fn insert(&self, id: u32, info: &BlobInfo) -> Result<()> {
        // Validate tags are ok.
        for tag in info.meta.tags.iter() {
            ensure!(!tag.contains('$'), "tag cannot contain separator");
        }

        let serialized_id = id.to_le_bytes();

        // Set the owner field in the users mask.
        self.user_mask_map.insert(&info.owner, &serialized_id)?;

        // Save the whole meta for recuperation.
        let serialized = bincode::serialize(&info)?;
        let r: &[u8] = serialized.as_ref();
        if let Some(last_meta_ivec) = self.meta_map.insert(&serialized_id, r)? {
            tracing::trace!("blob already exists, we need to purge previous tags");
            // We need to purge tags, and k/v pairs that were _removed_ from the meta
            // so they don't come up in searches anymore.
            let old_info: BlobInfo = bincode::deserialize(last_meta_ivec.as_ref())?;
            self.diff_and_purge_on_meta_update(old_info, info, id)?;
        }

        // Save tags in the reverse map.
        for tag in info.meta.tags.iter() {
            self.tag_map.insert(tag, &serialized_id)?;
        }

        // Save key/value fields in the reverse map.
        for (k, v) in info.meta.fields.iter().filter(|(_, v)| !v.is_empty()) {
            self.kv_map.insert(&kv_to_tag(k, v), &serialized_id)?;
        }

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn load_user_mask(&self, username: &str) -> Result<BitVec> {
        self.user_mask_map.load(username)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn load_tag(&self, tag: &str) -> Result<BitVec> {
        self.tag_map.load(tag)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn load_key_value(&self, k: &str, v: &str) -> Result<BitVec> {
        self.kv_map.load(&kv_to_tag(k, v))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn load_key(&self, k: &str) -> Result<BitVec> {
        // TODO: This is WIP. Computing this at query time is expensive, we could store it at indexing time instead.
        let mut bv = BitVec::default();

        for (_, v_ivec) in self
            .kv_map
            .tree()
            .scan_prefix(format!("{}$", k).as_bytes())
            .filter_map(|f| f.ok())
        {
            let resolved: BitVec = bincode::deserialize(v_ivec.as_ref())?;
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

    #[tracing::instrument(level = "trace", skip(self, mask))]
    fn list_all_tags(&self, mask: Option<&BitVec>) -> Result<HashMap<String, usize>> {
        let mut hsh = HashMap::with_capacity(self.tag_map.tree().len());

        for r in self.tag_map.tree().iter() {
            let (tag, vector) = r?;

            let tag_str = String::from_utf8_lossy(tag.as_ref()).to_string();
            let mut bv: BitVec = bincode::deserialize(vector.as_ref())?;

            if let Some(user_bitvec) = mask {
                bv &= user_bitvec.clone();
            }

            let count = bv.count_ones();
            if count > 0 {
                tracing::trace!(tag = %tag_str, count = count, "loaded tag");
                hsh.insert(tag_str, count);
            }
        }

        Ok(hsh)
    }

    #[tracing::instrument(level = "trace", skip(self, key_filter, mask))]
    fn list_all_kv_fields(
        &self,
        key_filter: &Option<Vec<String>>,
        mask: Option<&BitVec>,
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
                        let mut bv: BitVec = bincode::deserialize(vector.as_ref())?;

                        if let Some(user_bitvec) = mask {
                            bv &= user_bitvec.clone();
                        }

                        let count = bv.count_ones();

                        if count > 0 {
                            tracing::trace!(key=%key, value=%v, count=count, "loaded key-value");
                            hsh.entry(k.to_string())
                                .or_insert_with(HashMap::default)
                                .insert(v.to_string(), count);
                        }
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
                    let mut bv: BitVec = bincode::deserialize(vector.as_ref())?;

                    if let Some(user_bitvec) = mask {
                        bv &= user_bitvec.clone();
                    }

                    let count = bv.count_ones();
                    if count > 0 {
                        tracing::trace!(key=%k, value=%v, count=count, "loaded key-value");
                        hsh.entry(k.to_string())
                            .or_insert_with(HashMap::default)
                            .insert(v.to_string(), count);
                    }
                }
            }
        }
        Ok(hsh)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn purge(&self, idx: u32) -> Result<()> {
        let serialized_idx = idx.to_le_bytes();

        // Forget the metadata for this blob.
        self.meta_map.remove(&serialized_idx)?;

        // Purge.
        // TODO: Improve, this is _really_ expensive.
        // This is in O(2n) the number of unique [tags + kv].
        self.tag_map.purge(idx)?;
        self.kv_map.purge(idx)?;
        self.user_mask_map.purge(idx)?;

        Ok(())
    }

    fn clear(&self) -> Result<()> {
        self.meta_map.clear()?;
        self.tag_map.clear()?;
        self.kv_map.clear()?;
        tracing::debug!("meta index destroyed");
        Ok(())
    }
}
