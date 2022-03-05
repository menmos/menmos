use std::collections::HashMap;

use anyhow::{anyhow, ensure, Result};

use bitvec::prelude::*;

use futures::TryFutureExt;

use interface::{BlobInfo, FieldValue, TaggedBlobInfo};

use crate::node::store::bitvec_tree::BitvecTree;
use crate::node::store::iface::Flush;

use super::{FieldsIndex, MetadataStore};

const META_MAP: &str = "metadata";
const TAG_MAP: &str = "tags";
const USER_MASK_MAP: &str = "users";

pub struct SledMetadataStore {
    /// Stores the raw metadata of every blob.
    meta_map: sled::Tree,

    /// Stores the index of blob bitvectors by tag.
    tag_map: BitvecTree,

    /// Stores and manages the field/value index
    field_index: FieldsIndex,

    /// Stores the masks tracking which documents can be seen by each user.
    user_mask_map: BitvecTree,
}

impl SledMetadataStore {
    #[tracing::instrument(name = "meta_store_init", skip(db))]
    pub fn new(db: &sled::Db) -> Result<Self> {
        let meta_map = db.open_tree(META_MAP)?;

        let tag_map = BitvecTree::new(db, TAG_MAP)?;

        let field_index = FieldsIndex::new(db)?;

        let user_mask_map = BitvecTree::new(db, USER_MASK_MAP)?;

        Ok(Self {
            meta_map,
            tag_map,
            field_index,
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
            let field_maybe = new_meta.meta.fields.get(&key);
            if field_maybe != Some(&value) {
                self.field_index
                    .purge_field_value(&key, &value, for_idx, field_maybe.is_none())?
            }
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl Flush for SledMetadataStore {
    async fn flush(&self) -> Result<()> {
        tracing::debug!("starting flush");
        let meta_flush = self
            .meta_map
            .flush_async()
            .map_err(|e| anyhow!(e.to_string()));
        let tag_flush = self.tag_map.flush();
        let kv_flush = self.field_index.flush();

        tokio::try_join!(meta_flush, tag_flush, kv_flush).map(|_u| ())?;

        tracing::debug!("flush complete");
        Ok(())
    }
}

impl MetadataStore for SledMetadataStore {
    #[tracing::instrument(level = "trace", skip(self))]
    fn get(&self, idx: u32) -> Result<Option<BlobInfo>> {
        if let Some(ivec) = self.meta_map.get(idx.to_le_bytes())? {
            let info: TaggedBlobInfo = bincode::deserialize(&ivec)?;
            Ok(Some(info.into()))
        } else {
            tracing::trace!(index = idx, "not found");
            Ok(None)
        }
    }

    #[tracing::instrument(level = "trace", skip(self, info))]
    fn insert(&self, id: u32, info: &BlobInfo) -> Result<()> {
        // Validate tags are ok.
        for tag in info.meta.tags.iter() {
            // TODO: Remove this.
            ensure!(!tag.contains('$'), "tag cannot contain separator");
        }

        let serialized_id = id.to_le_bytes();

        // Set the owner field in the users mask.
        self.user_mask_map.insert(&info.owner, &serialized_id)?;

        // Save the whole meta for recuperation.
        let tagged_info = TaggedBlobInfo::from(info.clone());
        let serialized = bincode::serialize(&tagged_info)?;
        let r: &[u8] = serialized.as_ref();
        if let Some(last_meta_ivec) = self.meta_map.insert(&serialized_id, r)? {
            tracing::trace!("blob already exists, we need to purge previous tags");
            // We need to purge tags, and k/v pairs that were _removed_ from the meta
            // so they don't come up in searches anymore.
            let old_info: TaggedBlobInfo = bincode::deserialize(last_meta_ivec.as_ref())?;
            self.diff_and_purge_on_meta_update(old_info.into(), info, id)?;
        }

        // Save tags in the reverse map.
        for tag in info.meta.tags.iter() {
            self.tag_map.insert(tag, &serialized_id)?;
        }

        // Save key/value fields in the reverse map.
        for (k, v) in info.meta.fields.iter() {
            self.field_index.insert(k, v, &serialized_id)?;
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
    fn load_key_value(&self, k: &str, v: &FieldValue) -> Result<BitVec> {
        self.field_index.load_field_value(k, v)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn load_key(&self, k: &str) -> Result<BitVec> {
        self.field_index.load_field(k)
    }

    #[tracing::instrument(level = "trace", skip(self, mask))]
    fn list_all_tags(&self, mask: Option<&BitVec>) -> Result<HashMap<String, usize>> {
        let mut hsh = HashMap::with_capacity(self.tag_map.tree().len());

        for r in self.tag_map.tree().iter() {
            let (tag, vector) = r?;

            let tag_str = String::from_utf8(tag.to_vec()).expect("tag is not UTF-8");
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

    #[tracing::instrument(level = "trace", skip(self, field_filter, mask))]
    fn list_all_kv_fields(
        &self,
        field_filter: &Option<Vec<String>>,
        mask: Option<&BitVec>,
    ) -> Result<HashMap<String, HashMap<FieldValue, usize>>> {
        let mut hsh: HashMap<String, HashMap<FieldValue, usize>> = HashMap::new();

        match field_filter {
            Some(filter) => {
                for field in filter.iter() {
                    if let Some(result_it) = self.field_index.get_field_values(field)? {
                        for result in result_it {
                            let ((field_name, value), mut bv) = result?;

                            // if this trips, the field name we got back from disk for the field the user requested
                            // does _not_ match the field the user requested. will _hopefully_ never be thrown.
                            ensure!(
                                &field_name == field,
                                "the loaded field key doesn't match the requested field"
                            );

                            if let Some(user_bitvec) = mask {
                                bv &= user_bitvec.clone();
                            }

                            let count = bv.count_ones();

                            if count > 0 {
                                tracing::trace!(key=%field, value=%value, count=count, "loaded field-value");
                                hsh.entry(field_name)
                                    .or_insert_with(HashMap::default)
                                    .insert(value, count);
                            }
                        }
                    }
                }
            }
            None => {
                // List everything.
                // This will most likely lead to a massive response body - sorry about that.
                for result in self.field_index.iter() {
                    let ((field_name, value), mut bv) = result?;

                    if let Some(user_bitvec) = mask {
                        bv &= user_bitvec.clone();
                    }

                    let count = bv.count_ones();
                    if count > 0 {
                        tracing::trace!(key=%field_name, value=%value, count=count, "loaded field-value");
                        hsh.entry(field_name)
                            .or_insert_with(HashMap::default)
                            .insert(value, count);
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
        self.field_index.purge(idx)?;
        self.user_mask_map.purge(idx)?;

        Ok(())
    }

    fn clear(&self) -> Result<()> {
        self.meta_map.clear()?;
        self.tag_map.clear()?;
        self.field_index.clear()?;
        tracing::debug!("meta index destroyed");
        Ok(())
    }
}
