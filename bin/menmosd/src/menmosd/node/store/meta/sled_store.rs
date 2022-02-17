use std::collections::HashMap;
use std::io::{Read, Write};
use std::mem;

use anyhow::{anyhow, ensure, Context, Result};

use bitvec::prelude::*;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use futures::TryFutureExt;

use interface::BlobInfo;

use crate::node::store::bitvec_tree::BitvecTree;
use crate::node::store::id_map::IDMap;
use crate::node::store::iface::Flush;

use super::MetadataStore;

const META_MAP: &str = "metadata";
const TAG_MAP: &str = "tags";
const FIELD_MAP: &str = "fields";
const USER_MASK_MAP: &str = "users";

// TODO: Extract all the field related stuff (field_map + field_ids) into a separate structure.
//       This would encapsulate all weird field computation away from the root metadata index.
pub struct SledMetadataStore {
    /// Stores the raw metadata of every blob.
    meta_map: sled::Tree,

    /// Stores the index of blob bitvectors by tag.
    tag_map: BitvecTree,

    /// Stores the index of blob bitvectors by field/value pair.
    field_map: BitvecTree,

    /// Stores the FieldName <=> FieldID mapping.
    field_ids: IDMap,

    /// Stores the masks tracking which documents can be seen by each user.
    user_mask_map: BitvecTree,
}

impl SledMetadataStore {
    pub fn new(db: &sled::Db) -> Result<Self> {
        let meta_map = db.open_tree(META_MAP)?;

        let tag_map = BitvecTree::new(db, TAG_MAP)?;
        let field_map = BitvecTree::new(db, FIELD_MAP)?;
        let field_ids = IDMap::new(db, FIELD_MAP)?;
        let user_mask_map = BitvecTree::new(db, USER_MASK_MAP)?;

        Ok(Self {
            meta_map,
            tag_map,
            field_map,
            field_ids,
            user_mask_map,
        })
    }

    fn build_field_key(
        &self,
        field: &str,
        value: &str,
        allocate_field: bool,
    ) -> Result<Option<Bytes>> {
        let field_id = if allocate_field {
            self.field_ids.get_or_assign(field)?
        } else {
            match self.field_ids.get(field)? {
                Some(id) => id,
                None => {
                    return Ok(None);
                }
            }
        };

        let value_slice = value.as_bytes();

        let buffer = BytesMut::with_capacity(mem::size_of::<u32>() + value_slice.len());
        let mut bufwriter = buffer.writer();

        // 4 Bytes for the field ID.
        bufwriter.write_u32::<BigEndian>(field_id)?;

        // The rest of the key for the field value.
        bufwriter.write_all(value_slice)?;

        Ok(Some(bufwriter.into_inner().freeze()))
    }

    /// Loads the field/value pair from a field key.
    fn parse_field_key<B: Buf>(&self, field_key: B) -> Result<(String, String)> {
        // If any of the errors in this method trip up, this means we've either written bad data
        // into the tree _or_ the tree got corrupted. Godspeed.

        let mut bufreader = field_key.reader();
        let field_id = bufreader
            .read_u32::<BigEndian>()
            .context("corrupted field key")?;

        let field_name_ivec = self
            .field_ids
            .lookup(field_id)?
            .ok_or_else(|| anyhow!("field ID not found"))?;

        let field_name = String::from_utf8(field_name_ivec.to_vec())
            .context("field name corruption: recuperated a non-UTF-8 byte sequence")?;

        let expected_len = bufreader.get_ref().remaining();
        let mut field_value = String::with_capacity(expected_len);
        let actual_len = bufreader.read_to_string(&mut field_value)?;
        ensure!(expected_len == actual_len, "unexpect field value for field_id={field_id}. expected:{expected_len}, got:{actual_len}");

        Ok((field_name, field_value))
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
                let field_key = self
                    .build_field_key(&key, &value, false)?
                    .ok_or_else(|| anyhow!("field ID should exist for field {key}"))?;

                self.field_map.purge_key(&field_key, for_idx)?;
                tracing::trace!(key = %key, value = %value, index = for_idx, "purged key-value");
            }

            // TODO: If the new value is None and no values in the index start by the field ID,
            //       recycle the ID.
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
        let kv_flush = self.field_map.flush();

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
            // TODO: Remove this.
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
            let k = k.to_lowercase();

            let field_key = self
                .build_field_key(&k, &v, true)?
                .ok_or_else(|| anyhow!("ID allocation for field {k} returned no ID"))?;
            self.field_map.insert_bytes(&field_key, &serialized_id)?;
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
        match self.build_field_key(k, v, false)? {
            Some(field_key) => self.field_map.load_bytes(&field_key),
            None => {
                tracing::debug!(field = k, "fieldID not found, returning empty bitvector");
                Ok(BitVec::default())
            }
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn load_key(&self, k: &str) -> Result<BitVec> {
        let mut bv = BitVec::default();

        if let Some(field_id) = self.field_ids.get(k)? {
            for (_, v_ivec) in self
                .field_map
                .tree()
                .scan_prefix(field_id.to_be_bytes())
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
        } else {
            // We can skip the whole scan if the fieldID doesn't exist in the map! :)
            tracing::debug!(field = k, "fieldID not found, returning empty bitvector");
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

    #[tracing::instrument(level = "trace", skip(self, field_filter, mask))]
    fn list_all_kv_fields(
        &self,
        field_filter: &Option<Vec<String>>,
        mask: Option<&BitVec>,
    ) -> Result<HashMap<String, HashMap<String, usize>>> {
        let mut hsh: HashMap<String, HashMap<String, usize>> = HashMap::new();

        match field_filter {
            Some(filter) => {
                for field in filter.iter() {
                    let field_id = {
                        match self.field_ids.get(field)? {
                            Some(field_id) => field_id,
                            None => continue,
                        }
                    };

                    for (field_key_ivec, vector) in self
                        .field_map
                        .tree()
                        .scan_prefix(field_id.to_be_bytes())
                        .filter_map(|entry| entry.ok())
                    {
                        let (k, v) = self.parse_field_key(field_key_ivec.as_ref())?;

                        // if this trips, the field name we got back from disk for the field the user requested
                        // does _not_ match the field the user requested. will _hopefully_ never be thrown.
                        ensure!(
                            &k == field,
                            "the loaded field key doesn't match the requested field"
                        );

                        let mut bv: BitVec = bincode::deserialize(vector.as_ref())?;

                        if let Some(user_bitvec) = mask {
                            bv &= user_bitvec.clone();
                        }

                        let count = bv.count_ones();

                        if count > 0 {
                            tracing::trace!(key=%field, value=%v, count=count, "loaded key-value");
                            hsh.entry(k)
                                .or_insert_with(HashMap::default)
                                .insert(v, count);
                        }
                    }
                }
            }
            None => {
                // List everything.
                // This will most likely lead to a massive response body - sorry about that.
                for r in self.field_map.tree().iter() {
                    let (field_key_ivec, vector) = r?;
                    let (k, v) = self.parse_field_key(field_key_ivec.as_ref())?;
                    let mut bv: BitVec = bincode::deserialize(vector.as_ref())?;

                    if let Some(user_bitvec) = mask {
                        bv &= user_bitvec.clone();
                    }

                    let count = bv.count_ones();
                    if count > 0 {
                        tracing::trace!(key=%k, value=%v, count=count, "loaded key-value");
                        hsh.entry(k)
                            .or_insert_with(HashMap::default)
                            .insert(v, count);
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
        self.field_map.purge(idx)?;
        self.user_mask_map.purge(idx)?;

        Ok(())
    }

    fn clear(&self) -> Result<()> {
        self.meta_map.clear()?;
        self.tag_map.clear()?;
        self.field_map.clear()?;
        tracing::debug!("meta index destroyed");
        Ok(())
    }
}
