use anyhow::Result;
use tempfile::TempDir;

use crate::{iface::UserMapper, users::UsersStore};

#[test]
fn init_doesnt_fail() -> Result<()> {
    let d = TempDir::new()?;
    let db = sled::open(d.path())?;
    let _u = UsersStore::new(&db)?;
    Ok(())
}

#[test]
fn login_bad_password_doesnt_work() -> Result<()> {
    let d = TempDir::new()?;
    let db = sled::open(d.path())?;
    let u = UsersStore::new(&db)?;

    u.add_user("hello", "world")?;
    assert!(!u.authenticate("hello", "worlds")?);

    Ok(())
}

#[test]
fn login_bad_user_doesnt_work() -> Result<()> {
    let d = TempDir::new()?;
    let db = sled::open(d.path())?;
    let u = UsersStore::new(&db)?;

    u.add_user("hello", "world")?;
    assert!(!u.authenticate("bing", "world")?);

    Ok(())
}

#[test]
fn login_works_with_good_user_password_combo() -> Result<()> {
    let d = TempDir::new()?;
    let db = sled::open(d.path())?;
    let u = UsersStore::new(&db)?;

    u.add_user("hello", "world")?;
    assert!(u.authenticate("hello", "world")?);

    Ok(())
}
