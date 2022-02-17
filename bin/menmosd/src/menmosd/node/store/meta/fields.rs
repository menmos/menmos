use std::io::{Read, Write};
use std::mem;

use anyhow::{anyhow, ensure, Context, Result};
use bitvec::vec::BitVec;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use crate::node::store::bitvec_tree::BitvecTree;
use crate::node::store::id_map::IDMap;
use crate::node::store::iface::Flush;

const FIELDS_FILE_ID: &str = "fields";

/// Contains the data and behavior relating to the indexing of fields and their values.
pub struct FieldsIndex {
    field_map: BitvecTree,
    field_ids: IDMap,
}

impl FieldsIndex {
    pub fn new(db: &sled::Db) -> Result<Self> {
        let field_map = BitvecTree::new(db, FIELDS_FILE_ID)?;
        let field_ids = IDMap::new(db, FIELDS_FILE_ID)?;

        Ok(Self {
            field_map,
            field_ids,
        })
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

    pub fn purge_field_value(&self, field: &str, value: &str, for_idx: u32) -> Result<()> {
        let field_key = self
            .build_field_key(&field, &value, false)?
            .ok_or_else(|| anyhow!("field ID should exist for field {field}"))?;

        self.field_map.purge_key(&field_key, for_idx)?;
        tracing::trace!(key = %field, value = %value, index = for_idx, "purged field-value");
        Ok(())
    }

    pub fn insert(&self, field: &str, value: &str, serialized_docid: &[u8]) -> Result<()> {
        let field = field.to_lowercase();

        let field_key = self
            .build_field_key(&field, &value, true)?
            .ok_or_else(|| anyhow!("ID allocation for field {field} returned no ID"))?;

        self.field_map.insert_bytes(&field_key, &serialized_docid)
    }

    pub fn load_field_value(&self, field: &str, value: &str) -> Result<BitVec> {
        match self.build_field_key(field, value, false)? {
            Some(field_key) => self.field_map.load_bytes(&field_key),
            None => {
                tracing::debug!(
                    field = field,
                    "fieldID not found, returning empty bitvector"
                );
                Ok(BitVec::default())
            }
        }
    }

    pub fn load_field(&self, field: &str) -> Result<BitVec> {
        let mut bv = BitVec::default();

        if let Some(field_id) = self.field_ids.get(field)? {
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
            tracing::debug!(
                field = field,
                "fieldID not found, returning empty bitvector"
            );
        }

        Ok(bv)
    }

    /// Iterates on all field-value pairs in the fields index.
    pub fn iter(&self) -> impl Iterator<Item = Result<((String, String), BitVec)>> + '_ {
        self.field_map.tree().iter().map(|res| {
            let (field_key_ivec, bv_ivec) = res?;
            let (k, v) = self.parse_field_key(field_key_ivec.as_ref())?;
            let bv: BitVec = bincode::deserialize(bv_ivec.as_ref())?;
            Ok(((k, v), bv))
        })
    }

    pub fn get_field_values(
        &self,
        field: &str,
    ) -> Result<Option<impl Iterator<Item = Result<((String, String), BitVec)>> + '_>> {
        let field_id = {
            match self.field_ids.get(field)? {
                Some(field_id) => field_id,
                None => {
                    return Ok(None);
                }
            }
        };

        Ok(Some(
            self.field_map
                .tree()
                .scan_prefix(field_id.to_be_bytes())
                .map(|res| {
                    let (field_key_ivec, bv_ivec) = res?;
                    let (k, v) = self.parse_field_key(field_key_ivec.as_ref())?;
                    let bv = bincode::deserialize(bv_ivec.as_ref())?;
                    Ok(((k, v), bv))
                }),
        ))
    }

    pub fn purge(&self, idx: u32) -> Result<()> {
        self.field_map.purge(idx)
    }

    pub fn clear(&self) -> Result<()> {
        self.field_map.clear()?;
        self.field_ids.clear()
    }
}

#[async_trait::async_trait]
impl Flush for FieldsIndex {
    async fn flush(&self) -> Result<()> {
        tokio::try_join!(self.field_map.flush(), self.field_ids.flush())?;
        Ok(())
    }
}
