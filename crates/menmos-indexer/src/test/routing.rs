use anyhow::Result;
use interface::RoutingConfig;
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
fn get_routing_config_with_no_key_returns_none() -> Result<()> {
    let d = TempDir::new()?;
    let db = sled::open(d.path())?;
    let r = RoutingStore::new(&db)?;
    assert_eq!(r.get_routing_config("bing")?, None);

    Ok(())
}

#[test]
fn set_routing_config_works() -> Result<()> {
    let d = TempDir::new()?;
    let db = sled::open(d.path())?;
    let r = RoutingStore::new(&db)?;

    let cfg = RoutingConfig::new("some_field");

    r.set_routing_config("jdoe", &cfg)?;
    assert_eq!(&r.get_routing_config("jdoe")?.unwrap(), &cfg);

    Ok(())
}

#[test]
fn delete_routing_config_works() -> Result<()> {
    let d = TempDir::new()?;
    let db = sled::open(d.path())?;
    let r = RoutingStore::new(&db)?;

    let cfg = RoutingConfig::new("some_field").with_route("alpha", "beta");

    r.set_routing_config("jdoe", &cfg)?;
    r.delete_routing_config("jdoe")?;

    assert_eq!(r.get_routing_config("jdoe")?, None);

    Ok(())
}

#[test]
fn delete_nonexistent_routing_config_doesnt_fail() -> Result<()> {
    let d = TempDir::new()?;
    let db = sled::open(d.path())?;
    let r = RoutingStore::new(&db)?;

    r.delete_routing_config("i dont exist")?;

    Ok(())
}
