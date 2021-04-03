use anyhow::Result;

use bitvec::prelude::*;

use tempfile::TempDir;

use crate::{documents::DocumentIdStore, iface::DocIdMapper};

#[test]
fn nb_of_docs_initially_zero() {
    let d = TempDir::new().unwrap();
    let db = sled::open(d.path()).unwrap();

    let doc_map = DocumentIdStore::new(&db).unwrap();

    assert_eq!(doc_map.get_nb_of_docs(), 0);
}

#[test]
fn get_returns_same_doc_id() -> Result<()> {
    let d = TempDir::new()?;
    let db = sled::open(d.path())?;

    let doc_map = DocumentIdStore::new(&db)?;
    let id = doc_map.insert("abc")?;
    assert_eq!(doc_map.get("abc")?.unwrap(), id);

    Ok(())
}

#[test]
fn ids_increase_incrementally() {
    let d = TempDir::new().unwrap();
    let db = sled::open(d.path()).unwrap();

    let doc_map = DocumentIdStore::new(&db).unwrap();

    for i in 0..100 {
        assert_eq!(doc_map.insert(&format!("{}", i)).unwrap(), i);
    }
}

#[test]
fn same_key_gets_same_id() {
    let d = TempDir::new().unwrap();
    let db = sled::open(d.path()).unwrap();

    let doc_map = DocumentIdStore::new(&db).unwrap();

    for _i in 0..100 {
        assert_eq!(doc_map.insert("yeet").unwrap(), 0);
    }
}

#[test]
fn ids_lookup_are_reversible() {
    let d = TempDir::new().unwrap();
    let db = sled::open(d.path()).unwrap();

    let doc_map = DocumentIdStore::new(&db).unwrap();

    for i in 0..100 {
        let key = format!("{}", i);
        let idx = doc_map.insert(&key).unwrap();
        assert_eq!(doc_map.lookup(idx).unwrap().unwrap(), key);
    }
}

#[test]
fn reloading_map_keeps_keys_and_indices() {
    let d = TempDir::new().unwrap();

    {
        let db = sled::open(d.path()).unwrap();

        let doc_map = DocumentIdStore::new(&db).unwrap();

        for i in 0..100 {
            doc_map.insert(&format!("{}", i)).unwrap();
        }
    }

    // Reload the map and make sure everything is still there.
    {
        let db = sled::open(d.path()).unwrap();

        let doc_map = DocumentIdStore::new(&db).unwrap();

        for i in 0..100 {
            assert_eq!(doc_map.lookup(i).unwrap().unwrap(), format!("{}", i));
        }

        // Make sure the key count restarts at the right place.
        assert_eq!(doc_map.insert("bing").unwrap(), 100);
    }
}

#[test]
fn lookup_of_missing_key_doesnt_fail() {
    let d = TempDir::new().unwrap();
    let db = sled::open(d.path()).unwrap();

    let doc_map = DocumentIdStore::new(&db).unwrap();

    assert_eq!(doc_map.lookup(42).unwrap(), None);
}

/// Tests fix to #28 where we lost count on write due to keys being written in little-endian.
#[test]
fn no_data_loss_after_1024_inserts() -> Result<()> {
    let d = TempDir::new().unwrap();
    let db = sled::open(d.path()).unwrap();

    {
        let doc_map = DocumentIdStore::new(&db).unwrap();
        for i in 0..2000 {
            let id = doc_map.insert(&format!("{}", i))?;
            assert_eq!(id, i);
        }
    }

    {
        let doc_map = DocumentIdStore::new(&db).unwrap();
        let id = doc_map.insert("asdf")?;
        assert_eq!(id, 2000);
    }

    Ok(())
}

#[test]
fn delete_document() -> Result<()> {
    let d = TempDir::new().unwrap();
    let db = sled::open(d.path()).unwrap();

    let doc_map = DocumentIdStore::new(&db).unwrap();
    let id = doc_map.insert("hello")?;

    doc_map.delete("hello")?;

    assert_eq!(doc_map.lookup(id)?, None);

    Ok(())
}

#[test]
fn get_all_documents_mask() -> Result<()> {
    let d = TempDir::new().unwrap();
    let db = sled::open(d.path()).unwrap();

    let doc_map = DocumentIdStore::new(&db).unwrap();
    doc_map.insert("hello")?;
    doc_map.insert("there")?;
    doc_map.insert("world")?;

    assert_eq!(doc_map.get_all_documents_mask()?, bitvec![1, 1, 1]);

    doc_map.delete("there")?;

    assert_eq!(doc_map.get_all_documents_mask()?, bitvec![1, 0, 1]);

    Ok(())
}

#[test]
fn deleted_ids_are_recycled_properly() -> Result<()> {
    let d = TempDir::new().unwrap();
    let db = sled::open(d.path()).unwrap();

    let doc_map = DocumentIdStore::new(&db).unwrap();
    doc_map.insert("hello")?; // id 0
    doc_map.insert("there")?; // id 1
    doc_map.insert("world")?; // id 2

    doc_map.delete("there")?;

    assert_eq!(1, doc_map.insert("new")?);
    assert_eq!(0, doc_map.insert("hello")?);
    assert_eq!(3, doc_map.insert("another new")?);

    Ok(())
}
