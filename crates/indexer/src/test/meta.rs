use std::collections::HashMap;

use anyhow::Result;

use bitvec::prelude::*;
use interface::{BlobMeta, Type};
use tempfile::TempDir;

use crate::{iface::MetadataMapper, meta::MetadataStore};

#[test]
fn init_doesnt_fail() {
    let d = TempDir::new().unwrap();
    let db = sled::open(d.path()).unwrap();
    let m = MetadataStore::new(&db);

    assert!(m.is_ok());
}

#[test]
fn get_nonexistent_index_returns_none() {
    let d = TempDir::new().unwrap();
    let db = sled::open(d.path()).unwrap();
    let m = MetadataStore::new(&db).unwrap();

    assert_eq!(m.get(3).unwrap(), None);
}

#[test]
fn insert_empty_meta() {
    let d = TempDir::new().unwrap();
    let db = sled::open(d.path()).unwrap();
    let m = MetadataStore::new(&db).unwrap();

    m.insert(0, &BlobMeta::new("somename", Type::File)).unwrap();
}

#[test]
fn insert_meta() {
    let d = TempDir::new().unwrap();
    let db = sled::open(d.path()).unwrap();
    let m = MetadataStore::new(&db).unwrap();

    let meta = BlobMeta::new("somename", Type::File)
        .with_parent("some_parent")
        .with_tag("bing")
        .with_tag("bong")
        .with_meta("a", "b");

    m.insert(0, &meta).unwrap();

    assert_eq!(m.get(0).unwrap().unwrap(), meta);
}

#[test]
fn load_single_tag_first_index() {
    let d = TempDir::new().unwrap();
    let db = sled::open(d.path()).unwrap();
    let m = MetadataStore::new(&db).unwrap();

    m.insert(0, &BlobMeta::new("somename", Type::File).with_tag("bing"))
        .unwrap();

    assert_eq!(m.load_tag("bing").unwrap(), bitvec![1]);
}

#[test]
fn load_single_tag_advanced_index() {
    let d = TempDir::new().unwrap();
    let db = sled::open(d.path()).unwrap();
    let m = MetadataStore::new(&db).unwrap();

    m.insert(6, &BlobMeta::new("somename", Type::File).with_tag("bing"))
        .unwrap();

    assert_eq!(m.load_tag("bing").unwrap(), bitvec![0, 0, 0, 0, 0, 0, 1]);
}

#[test]
fn load_nonexistent_tag_returns_empty_bv() {
    let d = TempDir::new().unwrap();
    let db = sled::open(d.path()).unwrap();
    let m = MetadataStore::new(&db).unwrap();

    assert_eq!(m.load_tag("bing").unwrap(), bitvec![]);
}

#[test]
fn load_single_k_v_first_index() {
    let d = TempDir::new().unwrap();
    let db = sled::open(d.path()).unwrap();
    let m = MetadataStore::new(&db).unwrap();

    m.insert(
        0,
        &BlobMeta::new("somename", Type::File).with_meta("mykey", "myval"),
    )
    .unwrap();

    assert_eq!(m.load_key_value("mykey", "myval").unwrap(), bitvec![1]);
}

#[test]
fn load_single_k_v_advanced_index() {
    let d = TempDir::new().unwrap();
    let db = sled::open(d.path()).unwrap();
    let m = MetadataStore::new(&db).unwrap();

    m.insert(
        6,
        &BlobMeta::new("somename", Type::File).with_meta("mykey", "myval"),
    )
    .unwrap();

    assert_eq!(
        m.load_key_value("mykey", "myval").unwrap(),
        bitvec![0, 0, 0, 0, 0, 0, 1]
    );
}

#[test]
fn load_nonexistent_k_v_returns_empty_bv() {
    let d = TempDir::new().unwrap();
    let db = sled::open(d.path()).unwrap();
    let m = MetadataStore::new(&db).unwrap();

    assert_eq!(m.load_key_value("mykey", "myval").unwrap(), bitvec![]);
}

#[test]
fn kv_empty_value_doesnt_get_inserted() -> Result<()> {
    let d = TempDir::new()?;
    let db = sled::open(d.path())?;
    let m = MetadataStore::new(&db)?;

    m.insert(
        2,
        &BlobMeta::new("somename", Type::File).with_meta("hello", ""),
    )?;

    assert_eq!(m.load_key_value("hello", "")?.count_ones(), 0);

    Ok(())
}

#[test]
fn load_key_basic() -> Result<()> {
    let d = TempDir::new()?;
    let db = sled::open(d.path())?;
    let m = MetadataStore::new(&db)?;

    m.insert(
        1,
        &BlobMeta::new("somename", Type::File).with_meta("hello", "there"),
    )?;
    m.insert(
        2,
        &BlobMeta::new("somename", Type::File).with_meta("hello", "world"),
    )?;
    assert_eq!(m.load_key("hello")?, bitvec![0, 1, 1]);

    Ok(())
}

#[test]
fn insert_single_tag_multi_doc_ordered() {
    let d = TempDir::new().unwrap();
    let db = sled::open(d.path()).unwrap();
    let m = MetadataStore::new(&db).unwrap();

    m.insert(1, &BlobMeta::new("somename", Type::File).with_tag("hello"))
        .unwrap();
    m.insert(2, &BlobMeta::new("somename", Type::File).with_tag("hello"))
        .unwrap();
    assert_eq!(m.load_tag("hello").unwrap(), bitvec![0, 1, 1]);
}

#[test]
fn insert_single_tag_multi_doc_unordered() {
    let d = TempDir::new().unwrap();
    let db = sled::open(d.path()).unwrap();
    let m = MetadataStore::new(&db).unwrap();

    m.insert(4, &BlobMeta::new("somename", Type::File).with_tag("hello"))
        .unwrap();
    m.insert(2, &BlobMeta::new("somename", Type::File).with_tag("hello"))
        .unwrap();

    assert_eq!(m.load_tag("hello").unwrap(), bitvec![0, 0, 1, 0, 1]);
}

#[test]
fn load_tag_after_reload() {
    let d = TempDir::new().unwrap();
    {
        let db = sled::open(d.path()).unwrap();
        let m = MetadataStore::new(&db).unwrap();

        m.insert(3, &BlobMeta::new("somename", Type::File).with_tag("hello"))
            .unwrap();
    }

    {
        let db = sled::open(d.path()).unwrap();
        let m = MetadataStore::new(&db).unwrap();

        assert_eq!(m.load_tag("hello").unwrap(), bitvec![0, 0, 0, 1]);
    }
}

#[test]
fn insert_parent() -> Result<()> {
    let d = TempDir::new()?;

    let db = sled::open(d.path())?;
    let m = MetadataStore::new(&db)?;

    m.insert(
        2,
        &BlobMeta::new("somename", Type::File).with_parent("bing"),
    )?;
    m.insert(
        3,
        &BlobMeta::new("somename", Type::File).with_parent("bing"),
    )?;

    assert_eq!(m.load_children("bing")?, bitvec![0, 0, 1, 1]);

    Ok(())
}

#[test]
fn list_all_tags_basic() -> Result<()> {
    let d = TempDir::new()?;

    let db = sled::open(d.path())?;
    let m = MetadataStore::new(&db)?;

    m.insert(0, &BlobMeta::new("somename", Type::File).with_tag("a"))?;
    m.insert(
        1,
        &BlobMeta::new("somename", Type::File)
            .with_tag("a")
            .with_tag("b"),
    )?;
    m.insert(
        2,
        &BlobMeta::new("somename", Type::File)
            .with_tag("b")
            .with_tag("c"),
    )?;

    let result_map = m.list_all_tags()?;
    assert_eq!(result_map.len(), 3);
    assert_eq!(*result_map.get("a").unwrap(), 2);
    assert_eq!(*result_map.get("b").unwrap(), 2);
    assert_eq!(*result_map.get("c").unwrap(), 1);

    Ok(())
}

#[test]
fn list_all_kv_nofilter() -> Result<()> {
    let d = TempDir::new()?;
    let db = sled::open(d.path())?;
    let m = MetadataStore::new(&db)?;

    m.insert(
        0,
        &BlobMeta::new("somename", Type::File).with_meta("a", "b"),
    )?;
    m.insert(
        1,
        &BlobMeta::new("somename", Type::File).with_meta("a", "c"),
    )?;
    m.insert(
        2,
        &BlobMeta::new("somename", Type::File).with_meta("d", "e"),
    )?;

    let mut result_map = HashMap::new();

    {
        let a_entry = result_map
            .entry("a".to_string())
            .or_insert_with(HashMap::default);
        a_entry.insert("b".to_string(), 1);
        a_entry.insert("c".to_string(), 1);
    }
    result_map
        .entry("d".to_string())
        .or_insert_with(HashMap::default)
        .insert("e".to_string(), 1);

    assert_eq!(result_map, m.list_all_kv_fields(&None)?);

    Ok(())
}

#[test]
fn list_all_kv_filter() -> Result<()> {
    let d = TempDir::new()?;
    let db = sled::open(d.path())?;
    let m = MetadataStore::new(&db)?;

    m.insert(
        0,
        &BlobMeta::new("somename", Type::File).with_meta("a", "b"),
    )?;
    m.insert(
        1,
        &BlobMeta::new("somename", Type::File).with_meta("a", "c"),
    )?;
    m.insert(
        2,
        &BlobMeta::new("somename", Type::File).with_meta("d", "e"),
    )?;

    let mut result_map = HashMap::new();

    {
        let a_entry = result_map
            .entry("a".to_string())
            .or_insert_with(HashMap::default);
        a_entry.insert("b".to_string(), 1);
        a_entry.insert("c".to_string(), 1);
    }

    assert_eq!(
        result_map,
        m.list_all_kv_fields(&Some(vec!["a".to_string()]))?
    );

    Ok(())
}

#[test]
fn purge() -> Result<()> {
    let d = TempDir::new()?;
    let db = sled::open(d.path())?;
    let m = MetadataStore::new(&db)?;

    m.insert(0, &BlobMeta::new("somename", Type::File).with_tag("bing"))?;
    m.insert(1, &BlobMeta::new("somename", Type::File).with_tag("bing"))?;

    assert_eq!(m.load_tag("bing")?, bitvec![1, 1]);

    m.purge(0)?;

    assert_eq!(m.load_tag("bing")?, bitvec![0, 1]);

    Ok(())
}

#[test]
fn meta_update_with_tag_removal() -> Result<()> {
    // Tests that updating a blob meta by _removing_ a tag it had previously will clear the association between that tag and that blob.
    let d = TempDir::new()?;
    let db = sled::open(d.path())?;
    let m = MetadataStore::new(&db)?;

    m.insert(0, &BlobMeta::new("somename", Type::File).with_tag("bing"))?;
    m.insert(1, &BlobMeta::new("other", Type::File).with_tag("bong"))?;

    assert_eq!(m.load_tag("bong")?, bitvec![0, 1]);

    // Update the doc, removing the bong tag and setting the bing tag instead.
    m.insert(1, &BlobMeta::new("other", Type::File).with_tag("bing"))?;

    assert_eq!(m.load_tag("bong")?, bitvec![0, 0]);

    Ok(())
}
