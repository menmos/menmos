//! Test blob writing capabilities.
mod fixtures;
mod util;

use anyhow::Result;
use bytes::Bytes;
use fixtures::Menmos;
use menmos_client::Meta;
use util::stream_to_bytes;

#[tokio::test]
async fn write_blob_basic() -> Result<()> {
    let mut cluster = Menmos::new().await?;
    cluster.add_amphora("alpha").await?;

    let blob_id = cluster
        .push_document("Hello world!", Meta::file("test_blob"))
        .await?;

    cluster
        .client
        .write(&blob_id, 6, Bytes::from_static(b"there"))
        .await?;

    let file_stream = cluster.client.get_file(&blob_id).await?;
    let file_bytes = stream_to_bytes(file_stream).await?;
    let file_string = String::from_utf8_lossy(file_bytes.as_ref());

    assert_eq!(file_string, "Hello there!");

    cluster.stop_all().await?;

    Ok(())
}

#[tokio::test]
async fn extend_blob() -> Result<()> {
    let mut cluster = Menmos::new().await?;
    cluster.add_amphora("alpha").await?;

    let blob_id = cluster
        .push_document("Hello world", Meta::file("test_blob"))
        .await?;

    cluster
        .client
        .write(&blob_id, 11, Bytes::from_static(b" it's me."))
        .await?;

    let file_stream = cluster.client.get_file(&blob_id).await?;
    let file_bytes = stream_to_bytes(file_stream).await?;
    let file_string = String::from_utf8_lossy(file_bytes.as_ref());

    assert_eq!(file_string, "Hello world it's me.");

    cluster.stop_all().await?;

    Ok(())
}
