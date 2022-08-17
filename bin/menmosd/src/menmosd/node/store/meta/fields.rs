use core::panic;
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
        let field = field.to_lowercase();
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
            FieldValue::Str(s) => (TYPEID_STR, s.to_lowercase().as_bytes().to_vec()),
            FieldValue::Numeric(i) => (TYPEID_NUMERIC, i.to_be_bytes().to_vec()),
            FieldValue::Sequence(_) => {
                panic!("sequences should be stored using using multiple field keys (one per sequence element)");
            }
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

    #[tracing::instrument(name = "fields.purge_field_value", level = "trace", skip(self))]
    pub fn purge_field_value(
        &self,
        field: &str,
        value: &FieldValue,
        for_idx: u32,
        try_recycling: bool,
    ) -> Result<()> {
        if let FieldValue::Sequence(seq) = value {
            for elem in seq {
                self.purge_field_value(field, elem, for_idx, try_recycling)?;
            }
        } else {
            let field_key = self
                .build_field_key(field, value, false)?
                .ok_or_else(|| anyhow!("field ID should exist for field {field}"))?;

            self.field_map.purge_key(&field_key, for_idx)?;
            tracing::trace!(key = %field, value = %value, index = for_idx, "purged field-value");

            // We can try recycling here because the caller indicated that the field value for this doc
            // was _removed_, not modified. In that case, we need to check if the field is still in use,
            // and recycle its ID if not.
            if try_recycling {
                let field_in_use = tokio::task::block_in_place(|| {
                    self.field_map
                        .tree()
                        .scan_prefix(&field_key[0..mem::size_of::<u32>()])
                        .next()
                        .is_some()
                });

                if !field_in_use {
                    // We can recycle the field.
                    self.field_ids.delete(field)?;
                }
            }
        }

        Ok(())
    }

    #[tracing::instrument(name = "fields.insert", level = "trace", skip(self, serialized_docid))]
    pub fn insert(&self, field: &str, value: &FieldValue, serialized_docid: &[u8]) -> Result<()> {
        if let FieldValue::Sequence(seq) = value {
            for v in seq {
                self.insert(field, v, serialized_docid)?;
            }
            Ok(())
        } else {
            let field_key = self
                .build_field_key(field, value, true)?
                .ok_or_else(|| anyhow!("ID allocation for field {field} returned no ID"))?;

            self.field_map.insert_bytes(&field_key, serialized_docid)
        }
    }

    #[tracing::instrument(name = "fields.load_field_value", level = "trace", skip(self))]
    pub fn load_field_value(&self, field: &str, value: &FieldValue) -> Result<BitVec> {
        if let FieldValue::Sequence(seq) = value {
            // Load field value for a sequence is tricky. We need to load the bitvector for each sequence element,
            // and then AND them together to get the bitvector of documents that contain all values.

            if seq.is_empty() {
                return Ok(BitVec::new());
            }

            let mut bv = self.load_field_value(field, &seq[0])?;
            for v in seq[1..].iter() {
                let bitvec_element = self.load_field_value(field, v)?;

                let (biggest, smallest) = if bv.len() > bitvec_element.len() {
                    (bv, bitvec_element)
                } else {
                    (bitvec_element, bv)
                };

                bv = biggest;
                bv &= smallest;
            }

            Ok(bv)
        } else {
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
    }

    #[tracing::instrument(name = "fields.load_field", level = "trace", skip(self))]
    pub fn load_field(&self, field: &str) -> Result<BitVec> {
        let mut bv = BitVec::default();

        if let Some(field_id) = self.field_ids.get(&field.to_lowercase())? {
            bv = tokio::task::block_in_place(|| {
                for result in self.field_map.tree().scan_prefix(field_id.to_be_bytes()) {
                    let v_ivec = result?.1;
                    let resolved: BitVec = bincode::deserialize(v_ivec.as_ref())?;
                    let (biggest, smallest) = if bv.len() > resolved.len() {
                        (bv, resolved)
                    } else {
                        (resolved, bv)
                    };

                    bv = biggest;
                    bv |= smallest;
                }

                Ok::<_, anyhow::Error>(bv)
            })?;
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
        tokio::task::block_in_place(|| {
            self.field_map.tree().iter().map(|res| {
                let (field_key_ivec, bv_ivec) = res?;
                let (k, v) = self.parse_field_key(field_key_ivec.as_ref())?;
                let bv: BitVec = bincode::deserialize(bv_ivec.as_ref())?;
                Ok(((k, v), bv))
            })
        })
    }

    #[tracing::instrument(name = "fields.get_field_values", level = "trace", skip(self))]
    pub fn get_field_values(
        &self,
        field: &str,
    ) -> Result<Option<impl Iterator<Item = Result<((String, FieldValue), BitVec)>> + '_>> {
        let field_id = {
            match self.field_ids.get(&field.to_lowercase())? {
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

    #[tracing::instrument(name = "fields.purge", level = "trace", skip(self))]
    pub fn purge(&self, idx: u32) -> Result<()> {
        self.field_map.purge(idx)
    }

    #[tracing::instrument(name = "fields.clear", level = "trace", skip(self))]
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
    use interface::FieldValue;
    use std::collections::HashSet;

    use super::FieldsIndex;

    #[test]
    fn casing_load_field() -> Result<()> {
        let db = sled::Config::default().temporary(true).open().unwrap();
        let index = FieldsIndex::new(&db).unwrap();

        index.insert("SomeField", &"SomeValue".into(), &5_u32.to_le_bytes())?;
        index.insert("somefield", &"somevalue".into(), &1_u32.to_le_bytes())?;

        let bv = index.load_field("SOMEFIELD")?;
        assert_eq!(&bv, bits![0, 1, 0, 0, 0, 1]);

        Ok(())
    }

    #[test]
    fn casing_load_field_value() -> Result<()> {
        let db = sled::Config::default().temporary(true).open().unwrap();
        let index = FieldsIndex::new(&db).unwrap();

        index.insert("SomeField", &"SomeValue".into(), &5_u32.to_le_bytes())?;
        index.insert("SomeField", &"somevalue".into(), &1_u32.to_le_bytes())?;

        let bv = index.load_field_value("SOMEFIELD", &"SOMEVALUE".into())?;
        assert_eq!(&bv, bits![0, 1, 0, 0, 0, 1]);

        Ok(())
    }

    #[test]
    fn casing_get_field_values() -> Result<()> {
        let db = sled::Config::default().temporary(true).open().unwrap();
        let index = FieldsIndex::new(&db).unwrap();

        index.insert("SomeField", &"SomeValue".into(), &5_u32.to_le_bytes())?;
        index.insert("somefield", &"somevalue".into(), &1_u32.to_le_bytes())?;

        let values = index
            .get_field_values("SOMEFIELD")?
            .unwrap()
            .collect::<Result<Vec<_>>>()?;

        let expected_val = vec![(
            (
                String::from("somefield"),
                FieldValue::Str(String::from("somevalue")),
            ),
            bits![0, 1, 0, 0, 0, 1].to_owned(),
        )];
        assert_eq!(values, expected_val);

        Ok(())
    }

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
    fn list_numeric_field_values() -> Result<()> {
        let db = sled::Config::default().temporary(true).open().unwrap();
        let index = FieldsIndex::new(&db).unwrap();

        index.insert("mynumeric", &18.into(), &5_u32.to_le_bytes())?;
        index.insert("mynumeric", &22.into(), &1_u32.to_le_bytes())?;
        index.insert("mynumeric", &"asdf".into(), &2_u32.to_le_bytes())?;

        let mut seen_vals = HashSet::new();
        let mut val_count = 0;

        for value in index.get_field_values("mynumeric")?.unwrap() {
            let ((field_name, field_value), bv) = value?;
            assert_eq!(&field_name, "mynumeric");

            seen_vals.insert(field_value.clone());
            val_count += 1;

            match field_value {
                FieldValue::Str(s) => {
                    assert_eq!(&s, "asdf");
                    assert_eq!(&bv, bits![0, 0, 1]);
                }
                FieldValue::Numeric(18) => {
                    assert_eq!(&bv, bits![0, 0, 0, 0, 0, 1]);
                }
                FieldValue::Numeric(22) => {
                    assert_eq!(&bv, bits![0, 1]);
                }
                _ => panic!(),
            }
        }

        assert_eq!(seen_vals.len(), val_count);
        assert_eq!(val_count, 3);

        Ok(())
    }

    #[test]
    fn insert_sequence_field() -> Result<()> {
        let db = sled::Config::default().temporary(true).open().unwrap();
        let index = FieldsIndex::new(&db).unwrap();

        index.insert("mysequence", &vec!["a", "b"].into(), &5_u32.to_le_bytes())?;
        index.insert("mysequence", &vec!["a", "c"].into(), &2_u32.to_le_bytes())?;

        let bv = index.load_field("mysequence")?;
        assert_eq!(&bv, bits![0, 0, 1, 0, 0, 1]);

        let bv = index.load_field_value("mysequence", &vec!["a", "b"].into())?;
        assert_eq!(&bv, bits![0, 0, 0, 0, 0, 1]);

        Ok(())
    }

    #[test]
    fn list_sequence_field_values() -> Result<()> {
        let db = sled::Config::default().temporary(true).open().unwrap();
        let index = FieldsIndex::new(&db).unwrap();

        index.insert("mysequence", &vec!["a", "b"].into(), &5_u32.to_le_bytes())?;
        index.insert("mysequence", &vec!["a", "c"].into(), &2_u32.to_le_bytes())?;
        index.insert("mysequence", &vec![12, 13].into(), &1_u32.to_le_bytes())?;

        let mut seen_vals = HashSet::new();
        let mut val_count = 0;

        for value in index.get_field_values("mysequence")?.unwrap() {
            let ((field_name, field_value), bv) = value?;
            assert_eq!(&field_name, "mysequence");

            seen_vals.insert(field_value.clone());
            val_count += 1;

            match field_value {
                FieldValue::Str(s) => match s.as_ref() {
                    "a" => {
                        assert_eq!(&bv, bits![0, 0, 1, 0, 0, 1]);
                    }
                    "b" => {
                        assert_eq!(&bv, bits![0, 0, 0, 0, 0, 1]);
                    }
                    "c" => {
                        assert_eq!(&bv, bits![0, 0, 1]);
                    }
                    _ => panic!("unexpected value"),
                },
                FieldValue::Numeric(12) | FieldValue::Numeric(13) => {
                    assert_eq!(&bv, bits![0, 1]);
                }
                _ => panic!(),
            }
        }

        assert_eq!(seen_vals.len(), val_count);
        assert_eq!(val_count, 5);

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
