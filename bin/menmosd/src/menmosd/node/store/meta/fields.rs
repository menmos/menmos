use std::io::{Read, Write};
use std::mem;

use anyhow::{anyhow, bail, ensure, Context, Result};
use bitvec::vec::BitVec;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use interface::FieldValue;

use crate::node::store::bitvec_tree::BitvecTree;
use crate::node::store::id_map::IDMap;
use crate::node::store::iface::Flush;

const FIELDS_FILE_ID: &str = "fields";

const TYPEID_STR: u8 = 0;
const TYPEID_NUMERIC: u8 = 1;

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
    fn parse_field_key<B: Buf>(&self, field_key: B) -> Result<(String, FieldValue)> {
        // If any of the errors in this method trip up, this means we've either written bad data
        // into the tree _or_ the tree got corrupted. Godspeed.

        let mut bufreader = field_key.reader();
        let field_id = bufreader
            .read_u32::<BigEndian>()
            .context("corrupted field key")?;

        let type_id = bufreader.read_u8().context("corrupted field key")?;

        let field_name_ivec = self
            .field_ids
            .lookup(field_id)?
            .ok_or_else(|| anyhow!("field ID not found"))?;

        let field_value = match type_id {
            TYPEID_STR => {
                let expected_len = bufreader.get_ref().remaining();
                let mut str_buf = String::with_capacity(expected_len);
                let actual_len = bufreader.read_to_string(&mut str_buf)?;
                ensure!(expected_len == actual_len, "unexpected field value for field_id={field_id}. expected:{expected_len}, got:{actual_len}");
                FieldValue::Str(str_buf)
            }
            TYPEID_NUMERIC => {
                let expected_len = mem::size_of::<i64>();
                ensure!(
                    expected_len == bufreader.get_ref().remaining(),
                    "corrupted field key: expected {} bytes for numeric value",
                    expected_len
                );
                let value = bufreader.read_i64::<BigEndian>()?;
                FieldValue::Numeric(value)
            }
            _ => {
                bail!("unknown field type_id: {}", type_id);
            }
        };

        let field_name = String::from_utf8(field_name_ivec.to_vec())
            .context("field name corruption: recuperated a non-UTF-8 byte sequence")?;

        Ok((field_name, field_value))
    }

    fn build_field_key(
        &self,
        field: &str,
        value: &FieldValue,
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

        let (type_id, value_slice) = match value {
            FieldValue::Str(s) => (TYPEID_STR, s.as_bytes().to_vec()),
            FieldValue::Numeric(i) => (TYPEID_NUMERIC, i.to_be_bytes().to_vec()),
        };

        // Key format: [FieldID (4 bytes), TypeID (1 byte), FieldValue (N Bytes)]
        let buffer = BytesMut::with_capacity(
            mem::size_of::<u32>() + mem::size_of::<u8>() + value_slice.len(),
        );
        let mut bufwriter = buffer.writer();

        // 4 Bytes for the field ID.
        bufwriter.write_u32::<BigEndian>(field_id)?;

        // 1 Byte for the field type.
        bufwriter.write_u8(type_id)?;

        // The rest of the key for the field value.
        bufwriter.write_all(&value_slice)?;

        Ok(Some(bufwriter.into_inner().freeze()))
    }

    pub fn purge_field_value(
        &self,
        field: &str,
        value: &FieldValue,
        for_idx: u32,
        try_recycling: bool,
    ) -> Result<()> {
        let field_key = self
            .build_field_key(field, value, false)?
            .ok_or_else(|| anyhow!("field ID should exist for field {field}"))?;

        self.field_map.purge_key(&field_key, for_idx)?;
        tracing::trace!(key = %field, value = %value, index = for_idx, "purged field-value");

        // We can try recycling here because the caller indicated that the field value for this doc
        // was _removed_, not modified. In that case, we need to check if the field is still in use,
        // and recycle its ID if not.
        if try_recycling
            && self
                .field_map
                .tree()
                .scan_prefix(&field_key[0..mem::size_of::<u32>()])
                .next()
                .is_none()
        {
            // We can recycle the field.
            self.field_ids.delete(field)?;
        }

        Ok(())
    }

    pub fn insert(&self, field: &str, value: &FieldValue, serialized_docid: &[u8]) -> Result<()> {
        let field = field.to_lowercase();

        let field_key = self
            .build_field_key(&field, value, true)?
            .ok_or_else(|| anyhow!("ID allocation for field {field} returned no ID"))?;

        self.field_map.insert_bytes(&field_key, serialized_docid)
    }

    pub fn load_field_value(&self, field: &str, value: &FieldValue) -> Result<BitVec> {
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
    pub fn iter(&self) -> impl Iterator<Item = Result<((String, FieldValue), BitVec)>> + '_ {
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
    ) -> Result<Option<impl Iterator<Item = Result<((String, FieldValue), BitVec)>> + '_>> {
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

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use bitvec::prelude::*;

    use super::FieldsIndex;

    #[test]
    fn insert_load_field_value() -> Result<()> {
        let db = sled::Config::default().temporary(true).open().unwrap();
        let index = FieldsIndex::new(&db).unwrap();

        index.insert("somefield", &"somevalue".into(), &5_u32.to_le_bytes())?;
        index.insert("somefield", &"somevalue".into(), &3_u32.to_le_bytes())?;
        index.insert("somefield", &"othervalue".into(), &1_u32.to_le_bytes())?;

        let bv = index.load_field_value("somefield", &"somevalue".into())?;
        assert_eq!(&bv, bits![0, 0, 0, 1, 0, 1]);

        Ok(())
    }

    #[test]
    fn insert_load_field() -> Result<()> {
        let db = sled::Config::default().temporary(true).open().unwrap();
        let index = FieldsIndex::new(&db).unwrap();

        index.insert("somefield", &"somevalue".into(), &5_u32.to_le_bytes())?;
        index.insert("somefield", &"somevalue".into(), &3_u32.to_le_bytes())?;
        index.insert("somefield", &"othervalue".into(), &1_u32.to_le_bytes())?;

        let bv = index.load_field("somefield")?;
        assert_eq!(&bv, bits![0, 1, 0, 1, 0, 1]);

        Ok(())
    }

    #[test]
    fn insert_numeric_field() -> Result<()> {
        let db = sled::Config::default().temporary(true).open().unwrap();
        let index = FieldsIndex::new(&db).unwrap();

        index.insert("mynumeric", &18.into(), &5_u32.to_le_bytes())?;
        index.insert("mynumeric", &12.into(), &2_u32.to_le_bytes())?;

        let bv = index.load_field("mynumeric")?;
        assert_eq!(&bv, bits![0, 0, 1, 0, 0, 1]);

        let bv = index.load_field_value("mynumeric", &18_i64.into())?;
        assert_eq!(&bv, bits![0, 0, 0, 0, 0, 1]);

        Ok(())
    }

    #[test]
    fn field_mixed_types() -> Result<()> {
        let db = sled::Config::default().temporary(true).open().unwrap();
        let index = FieldsIndex::new(&db).unwrap();

        index.insert("myfield", &18.into(), &5_u32.to_le_bytes())?;
        index.insert("myfield", &"stringvalue".into(), &2_u32.to_le_bytes())?;

        let bv = index.load_field("myfield")?;
        assert_eq!(&bv, bits![0, 0, 1, 0, 0, 1]);

        let bv = index.load_field_value("myfield", &18_i64.into())?;
        assert_eq!(&bv, bits![0, 0, 0, 0, 0, 1]);

        Ok(())
    }

    #[test]
    fn field_id_recycling() -> Result<()> {
        let db = sled::Config::default().temporary(true).open().unwrap();
        let index = FieldsIndex::new(&db).unwrap();

        index.insert("somefield", &"somevalue".into(), &3_u32.to_le_bytes())?;
        index.insert("somefield", &"somevalue".into(), &4_u32.to_le_bytes())?;
        index.insert("somefield", &"othervalue".into(), &1_u32.to_le_bytes())?;

        // Create another field to make sure recycling works.
        index.insert("otherfield", &"somevalue".into(), &0_u32.to_le_bytes())?;

        let somefield_id = index.field_ids.get("somefield")?.unwrap();

        // Purge one field values & ask for recycling
        index.purge_field_value("somefield", &"somevalue".into(), 3, true)?;

        // Here the field ID should still exist.
        assert!(index.field_ids.get("somefield")?.is_some());

        // Purge the second document of the same value.
        index.purge_field_value("somefield", &"somevalue".into(), 4, true)?;

        // Here the field ID should still exist.
        assert!(index.field_ids.get("somefield")?.is_some());

        // Purge the second field value & ask for recycling - the field id should be recycled here.
        index.purge_field_value("somefield", &"othervalue".into(), 1, true)?;

        // Make sure we deleted the field correctly from the map.
        assert!(index.field_ids.get("somefield")?.is_none());
        // And as such we should be able to load anything.
        assert!(index.get_field_values("somefield")?.is_none());

        // Create a new field and make sure we used our recycled ID.
        index.insert("shinynewfield", &"myvalue".into(), &3_u32.to_le_bytes())?;
        let new_field_id = index.field_ids.get("shinynewfield")?.unwrap();
        assert_eq!(somefield_id, new_field_id);

        Ok(())
    }
}
