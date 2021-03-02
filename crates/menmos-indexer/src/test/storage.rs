use std::net::IpAddr;

use anyhow::Result;

use interface::StorageNodeInfo;
use tempfile::TempDir;

use crate::{iface::StorageNodeMapper, storage::StorageDispatch};

#[test]
fn storage_dispatch_initializes_without_error() {
    let d = TempDir::new().unwrap();
    let db = sled::open(d.path()).unwrap();

    let s = StorageDispatch::new(&db);
    assert!(s.is_ok());
}

#[test]
fn get_node_for_nonexistent_blob_returns_none() {
    let d = TempDir::new().unwrap();
    let db = sled::open(d.path()).unwrap();
    let s = StorageDispatch::new(&db).unwrap();

    assert_eq!(s.get_node_for_blob("bing").unwrap(), None);
}

#[test]
fn single_set_node_works() {
    let d = TempDir::new().unwrap();
    let db = sled::open(d.path()).unwrap();
    let s = StorageDispatch::new(&db).unwrap();

    s.set_node_for_blob("some_blob", "some_node".to_string())
        .unwrap();

    assert_eq!(
        s.get_node_for_blob("some_blob").unwrap().unwrap(),
        "some_node"
    );
}

#[test]
fn update_node_returns_new_value() {
    let d = TempDir::new().unwrap();
    let db = sled::open(d.path()).unwrap();
    let s = StorageDispatch::new(&db).unwrap();

    s.set_node_for_blob("a", "b".to_string()).unwrap();
    s.set_node_for_blob("a", "c".to_string()).unwrap();

    assert_eq!(s.get_node_for_blob("a").unwrap().unwrap(), "c");
}

#[test]
fn reload_keeps_mapping() {
    let d = TempDir::new().unwrap();

    {
        let db = sled::open(d.path()).unwrap();
        let s = StorageDispatch::new(&db).unwrap();

        s.set_node_for_blob("a", "b".to_string()).unwrap();
    }

    // Reload the DB and make sure everything is still there.
    let db = sled::open(d.path()).unwrap();
    let s = StorageDispatch::new(&db).unwrap();

    assert_eq!(s.get_node_for_blob("a").unwrap().unwrap(), "b");
}

#[test]
fn get_set_storage_node() -> Result<()> {
    let d = TempDir::new()?;
    let db = sled::open(d.path())?;
    let s = StorageDispatch::new(&db)?;

    let node_info = StorageNodeInfo {
        id: String::from("bing"),
        redirect_info: interface::RedirectInfo::Static {
            static_address: IpAddr::from([192, 168, 2, 1]),
        },
        port: 3031,
    };

    let now = chrono::Utc::now();

    s.write_node(node_info.clone(), now.clone())?;

    let (n_info, timestamp) = s.get_node("bing")?.unwrap();

    assert_eq!(n_info, node_info);
    assert_eq!(timestamp.timestamp(), now.timestamp()); // We're losing nanoseconds in the serialization, which is acceptable.

    Ok(())
}

#[test]
fn delete_blob() -> Result<()> {
    let d = TempDir::new()?;
    let db = sled::open(d.path())?;
    let s = StorageDispatch::new(&db)?;

    s.set_node_for_blob("blob_a", "node_b".to_string())?;

    assert_eq!(s.get_node_for_blob("blob_a")?.unwrap(), "node_b");

    s.delete_blob("blob_a")?;
    assert_eq!(s.get_node_for_blob("blob_a")?, None);

    Ok(())
}
