//! Tests authentication.
mod util;

use core::panic;

use anyhow::Result;
use bytes::Bytes;
use interface::Query;
use menmos_client::{Client, Meta};
use testing::fixtures::Menmos;
use util::stream_to_bytes;

#[tokio::test]
async fn permissions_query() -> Result<()> {
    let mut cluster = Menmos::new().await?;
    cluster.add_amphora("alpha").await?;

    cluster.push_document("bing bong", Meta::new()).await?;

    // Make sure our document is visible to our user.
    let results = cluster.client.query(Query::default()).await?;
    assert_eq!(results.count, 1);

    // Do the same query from another user.
    cluster.add_user("john", "bingbong").await?;
    let john_client = Client::new(&cluster.directory_url, "john", "bingbong").await?;
    let john_results = john_client.query(Query::default()).await?;
    assert_eq!(john_results.count, 0);
    assert_eq!(john_results.total, 0);
    assert_eq!(john_results.hits, Vec::default());

    cluster.stop_all().await?;

    Ok(())
}

#[tokio::test]
async fn list_metadata_permissions() -> Result<()> {
    let mut cluster = Menmos::new().await?;
    cluster.add_amphora("alpha").await?;

    // Document for the default user.
    cluster
        .push_document("bing bong", Meta::new().with_tag("hello").with_tag("world"))
        .await?;

    // Document for our other user.
    cluster.add_user("john", "bingbong").await?;
    let john_client = Client::new(&cluster.directory_url, "john", "bingbong").await?;
    cluster
        .push_document_client(
            "other bing",
            Meta::new().with_tag("hello").with_tag("there"),
            &john_client,
        )
        .await?;

    let meta_list = john_client.list_meta(None, None).await?;
    assert_eq!(meta_list.tags.len(), 2);
    assert_eq!(*meta_list.tags.get("hello").unwrap(), 1);

    cluster.stop_all().await?;
    Ok(())
}

#[tokio::test]
async fn direct_get_permissions() -> Result<()> {
    let mut cluster = Menmos::new().await?;
    cluster.add_amphora("alpha").await?;

    let blob_id = cluster.push_document("bing bong", Meta::new()).await?;

    // Assert base get works.
    let file_contents = cluster.client.get_file(&blob_id).await?;
    let file_bytes = stream_to_bytes(file_contents).await?;
    let file_string = String::from_utf8_lossy(file_bytes.as_ref());
    assert_eq!(file_string, "bing bong");

    // Create a user and try to access the file.
    cluster.add_user("john", "bingbong").await?;
    let john_client = Client::new(&cluster.directory_url, "john", "bingbong").await?;
    if let Err(e) = john_client.get_file(&blob_id).await {
        assert!(e.to_string().contains("forbidden"));
    } else {
        panic!("expected forbidden");
    }

    cluster.stop_all().await?;
    Ok(())
}

#[tokio::test]
async fn get_after_delete_permissions() -> Result<()> {
    let mut cluster = Menmos::new().await?;
    cluster.add_amphora("alpha").await?;

    let blob_id = cluster.push_document("bing bong", Meta::new()).await?;

    // Delete the blob to put the ID back in the pool.
    cluster.client.delete(blob_id).await?;

    // Create a user and create a new doc to recycle the old doc ID.
    cluster.add_user("john", "bingbong").await?;
    let john_client = Client::new(&cluster.directory_url, "john", "bingbong").await?;

    let blob_id = cluster
        .push_document_client("yayeet", Meta::new(), &john_client)
        .await?;

    // Assert base get works.
    let file_contents = john_client.get_file(&blob_id).await?;
    let file_bytes = stream_to_bytes(file_contents).await?;
    let file_string = String::from_utf8_lossy(file_bytes.as_ref());
    assert_eq!(file_string, "yayeet");

    // Make sure the last user can't read the doc.
    if let Err(e) = cluster.client.get_file(&blob_id).await {
        assert!(e.to_string().contains("forbidden"));
    } else {
        panic!("expected forbidden");
    }

    cluster.stop_all().await?;
    Ok(())
}

#[tokio::test]
async fn direct_get_meta() -> Result<()> {
    let mut cluster = Menmos::new().await?;
    cluster.add_amphora("alpha").await?;

    let blob_id = cluster
        .push_document("bing bong", Meta::new().with_meta("name", "test.txt"))
        .await?;

    // Make sure the owner can get the metadata.
    let meta = cluster.client.get_meta(&blob_id).await?.unwrap();
    assert_eq!(meta.metadata.get("name").unwrap(), "test.txt");

    // Make sure a new user can't get the metadata.
    cluster.add_user("john", "bingbong").await?;
    let john_client = Client::new(&cluster.directory_url, "john", "bingbong").await?;

    assert!(john_client.get_meta(&blob_id).await?.is_none());

    cluster.stop_all().await?;

    Ok(())
}

#[tokio::test]
async fn permissions_write() -> Result<()> {
    let mut cluster = Menmos::new().await?;
    cluster.add_amphora("alpha").await?;

    let blob_id = cluster.push_document("bing bong", Meta::new()).await?;

    // Write as owner.
    cluster
        .client
        .write(&blob_id, 0, Bytes::copy_from_slice(b"yeet"))
        .await?;

    // Write as non-owner.
    cluster.add_user("john", "bingbong").await?;
    let john_client = Client::new(&cluster.directory_url, "john", "bingbong").await?;
    assert!(john_client
        .write(&blob_id, 0, Bytes::copy_from_slice(b"skrt"))
        .await
        .is_err());

    // Make sure only the first write got through.
    let file_stream = cluster.client.get_file(&blob_id).await?;
    let file_bytes = stream_to_bytes(file_stream).await?;
    let file_str = String::from_utf8_lossy(file_bytes.as_ref());

    assert_eq!(file_str, "yeet bong");

    cluster.stop_all().await?;

    Ok(())
}

#[tokio::test]
async fn permissions_update_meta() -> Result<()> {
    let mut cluster = Menmos::new().await?;
    cluster.add_amphora("alpha").await?;

    let meta = Meta::new().with_tag("hello");

    let blob_id = cluster.push_document("bing bong", meta.clone()).await?;

    // Update the meta as owner.
    cluster
        .client
        .update_meta(&blob_id, meta.clone().with_tag("world"))
        .await?;

    // Update the meta as non-owner.
    cluster.add_user("john", "bingbong").await?;
    let john_client = Client::new(&cluster.directory_url, "john", "bingbong").await?;
    assert!(john_client
        .update_meta(&blob_id, meta.clone().with_tag("there"))
        .await
        .is_err());

    // Make sure only the first update went through.
    let updated_meta = cluster.client.get_meta(&blob_id).await?.unwrap();
    assert_eq!(
        updated_meta.tags,
        vec![String::from("hello"), String::from("world")]
    );

    cluster.stop_all().await?;

    Ok(())
}

#[tokio::test]
async fn permissions_delete() -> Result<()> {
    let mut cluster = Menmos::new().await?;
    cluster.add_amphora("alpha").await?;

    let blob_id = cluster
        .push_document("bing bong", Meta::new().with_meta("name", "test.txt"))
        .await?;

    // Try deleting from non-owner.
    cluster.add_user("john", "bingbong").await?;
    let john_client = Client::new(&cluster.directory_url, "john", "bingbong").await?;
    assert!(john_client.delete(blob_id.clone()).await.is_err());

    // Make sure the blob is still there.
    let meta = cluster.client.get_meta(&blob_id).await?.unwrap();
    assert_eq!(meta.metadata.get("name").unwrap(), "test.txt");

    // Delete as owner.
    cluster.client.delete(blob_id.clone()).await?;

    // Make sure blob was deleted.
    assert!(cluster.client.get_meta(&blob_id).await?.is_none());

    cluster.stop_all().await?;

    Ok(())
}

#[tokio::test]
async fn permissions_fsync() -> Result<()> {
    let mut cluster = Menmos::new().await?;
    cluster.add_amphora("alpha").await?;

    let blob_id = cluster.push_document("bing bong", Meta::new()).await?;

    // Try fsync from a non-owner.
    cluster.add_user("john", "bingbong").await?;
    let john_client = Client::new(&cluster.directory_url, "john", "bingbong").await?;
    assert!(john_client.fsync(&blob_id).await.is_err());

    // Try fsync from an owner.
    cluster.client.fsync(&blob_id).await?;

    cluster.stop_all().await?;

    Ok(())
}

#[tokio::test]
async fn permissions_update_blob() -> Result<()> {
    let mut cluster = Menmos::new().await?;
    cluster.add_amphora("alpha").await?;

    let blob_id = cluster.push_document("bing bong", Meta::new()).await?;

    // Try update from non-owner.
    cluster.add_user("john", "bingbong").await?;
    let john_client = Client::new(&cluster.directory_url, "john", "bingbong").await?;
    assert!(cluster
        .update_document_client(&blob_id, "ya yeet", Meta::new(), &john_client)
        .await
        .is_err());

    cluster.stop_all().await?;

    Ok(())
}
