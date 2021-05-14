use std::collections::HashSet;
use std::iter::FromIterator;

use anyhow::Result;

use interface::UserManagement;

use super::mock;

#[tokio::test]
async fn register_basic() -> Result<()> {
    let svc = mock::user_service();

    assert!(!svc.has_user("test").await?);

    svc.register("test", "asdf").await?;

    assert!(svc.has_user("test").await?);

    Ok(())
}

#[tokio::test]
async fn login_correct_password() -> Result<()> {
    let svc = mock::user_service();

    svc.register("test", "asdf").await?;
    assert!(svc.login("test", "asdf").await?);

    Ok(())
}

#[tokio::test]
async fn login_incorrect_password() -> Result<()> {
    let svc = mock::user_service();

    svc.register("test", "asdf").await?;
    assert!(!svc.login("test", "bad password").await?);

    Ok(())
}

#[tokio::test]
async fn list_users() -> Result<()> {
    let svc = mock::user_service();

    svc.register("testa", "testa").await?;
    svc.register("testb", "testb").await?;

    let results: HashSet<String> = HashSet::from_iter(svc.list().await.into_iter());

    assert!(results.contains("testa"));
    assert!(results.contains("testb"));

    Ok(())
}
