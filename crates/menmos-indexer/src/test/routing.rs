use anyhow::Result;
use tempfile::TempDir;

use crate::{iface::RoutingMapper, routing::RoutingStore};

#[test]
fn init_doesnt_fail() -> Result<()> {
    let d = TempDir::new()?;
    let db = sled::open(d.path())?;
    let _r = RoutingStore::new(&db)?;
    Ok(())
}

#[test]
fn get_routing_key_with_no_key_returns_none() -> Result<()> {
    let d = TempDir::new()?;
    let db = sled::open(d.path())?;
    let r = RoutingStore::new(&db)?;
    assert_eq!(r.get_routing_key("bing")?, None);

    Ok(())
}

#[test]
fn set_routing_key_works() -> Result<()> {
    let d = TempDir::new()?;
    let db = sled::open(d.path())?;
    let r = RoutingStore::new(&db)?;
    r.set_routing_key("jdoe", "some_field")?;
    assert_eq!(r.get_routing_key("jdoe")?.unwrap(), "some_field");

    Ok(())
}

#[test]
fn delete_routing_key_works() -> Result<()> {
    let d = TempDir::new()?;
    let db = sled::open(d.path())?;
    let r = RoutingStore::new(&db)?;

    r.set_routing_key("jdoe", "some_field")?;
    r.delete_routing_key("jdoe")?;

    assert_eq!(r.get_routing_key("jdoe")?, None);

    Ok(())
}

#[test]
fn delete_nonexistent_routing_key_doesnt_fail() -> Result<()> {
    let d = TempDir::new()?;
    let db = sled::open(d.path())?;
    let r = RoutingStore::new(&db)?;

    r.delete_routing_key("i dont exist")?;

    Ok(())
}
