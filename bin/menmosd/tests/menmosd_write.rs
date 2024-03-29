//! Test blob writing capabilities.
mod util;

use anyhow::Result;
use bytes::Bytes;
use interface::Query;
use menmos_client::Meta;
use testing::fixtures::Menmos;
use util::stream_to_bytes;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_blob_basic() -> Result<()> {
    let mut cluster = Menmos::new().await?;
    cluster.add_amphora("alpha").await?;

    let blob_id = cluster.push_document("Hello world!", Meta::new()).await?;

    cluster
        .client
        .write(&blob_id, 6, Bytes::from_static(b"there"))
        .await?;
    cluster.flush().await?;

    let file_stream = cluster.client.get_file(&blob_id).await?;
    let file_bytes = stream_to_bytes(file_stream).await?;
    let file_string = String::from_utf8_lossy(file_bytes.as_ref());

    assert_eq!(file_string, "Hello there!");

    cluster.stop_all().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extend_blob() -> Result<()> {
    let mut cluster = Menmos::new().await?;
    cluster.add_amphora("alpha").await?;

    let blob_id = cluster.push_document("Hello world", Meta::new()).await?;

    cluster
        .client
        .write(&blob_id, 11, Bytes::from_static(b" it's me."))
        .await?;

    cluster.flush().await?;

    let file_stream = cluster.client.get_file(&blob_id).await?;
    let file_bytes = stream_to_bytes(file_stream).await?;
    let file_string = String::from_utf8_lossy(file_bytes.as_ref());

    assert_eq!(file_string, "Hello world it's me.");

    cluster.stop_all().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_updates_datetime() -> Result<()> {
    let mut cluster = Menmos::new().await?;
    cluster.add_amphora("alpha").await?;

    let blob_id = cluster.push_document("Hello world", Meta::new()).await?;

    cluster.flush().await?;

    // Make sure datetimes make sense.
    let meta = cluster.client.get_meta(&blob_id).await?.unwrap();

    let created_at = meta.created_at;
    let modified_at = meta.modified_at;

    assert_eq!(created_at, modified_at);

    // Update the file and make sure the meta was updated.
    cluster
        .client
        .write(&blob_id, 0, Bytes::from_static(b"its me"))
        .await?;

    cluster.flush().await?;

    let after_meta = cluster.client.get_meta(&blob_id).await?.unwrap();

    // Created at shouldn't change.
    assert_eq!(after_meta.created_at, created_at);

    // Modified at should have changed.
    assert!(after_meta.modified_at > created_at);

    cluster.stop_all().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn meta_update_updates_datetime() -> Result<()> {
    let mut cluster = Menmos::new().await?;
    cluster.add_amphora("alpha").await?;

    let blob_id = cluster.push_document("Hello world", Meta::new()).await?;

    // Make sure datetimes make sense.
    let meta = cluster.client.get_meta(&blob_id).await?.unwrap();

    let created_at = meta.created_at;
    let modified_at = meta.modified_at;

    assert_eq!(created_at, modified_at);

    // Update the file and make sure the meta was updated.
    cluster.client.update_meta(&blob_id, Meta::new()).await?;

    cluster.flush().await?;

    let after_meta = cluster.client.get_meta(&blob_id).await?.unwrap();

    // Created at shouldn't change.
    assert_eq!(after_meta.created_at, created_at);

    // Modified at should have changed.
    assert!(after_meta.modified_at > created_at);

    cluster.stop_all().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_empty_blob_doesnt_create_file() -> Result<()> {
    let mut cluster = Menmos::new().await?;
    cluster.add_amphora("alpha").await?;

    let blob_id = cluster
        .client
        .create_empty(Meta::new().with_field("name", "myfile.txt"))
        .await?;

    // We make sure the blob is queryable.
    let results = cluster.client.query(Query::default()).await?;
    assert_eq!(results.count, 1);

    // Make sure the file isn't on disk.
    assert!(!cluster
        .root_directory
        .as_ref()
        .join("alpha-blobs")
        .join(&blob_id)
        .with_extension("blob")
        .exists());

    // Append to the file, make sure we don't crash.
    cluster
        .client
        .write(&blob_id, 0, Bytes::from_static(b"bing bong"))
        .await?;

    cluster.flush().await?;

    // The blob should be on disk now.
    assert!(cluster
        .root_directory
        .as_ref()
        .join("alpha-blobs")
        .join(&blob_id)
        .with_extension("blob")
        .exists());

    cluster.stop_all().await?;
    Ok(())
}
