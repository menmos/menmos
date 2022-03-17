//! Test blob streaming capabilities.
// (e.g. readonly)
mod util;

use anyhow::Result;
use menmos_client::Meta;
use testing::fixtures::Menmos;
use util::stream_to_bytes;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_blob_basic() -> Result<()> {
    let mut cluster = Menmos::new().await?;
    cluster.add_amphora("alpha").await?;

    const DOCUMENT_BODY: &str = "Hello world!";

    let blob_id = cluster.push_document(DOCUMENT_BODY, Meta::new()).await?;

    cluster.flush().await?;

    let file_stream = cluster.client.get_file(&blob_id).await?;
    let file_bytes = stream_to_bytes(file_stream).await?;

    let content_string = String::from_utf8_lossy(file_bytes.as_ref()).to_string();
    assert_eq!(&content_string, DOCUMENT_BODY);

    cluster.stop_all().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_blob_range() -> Result<()> {
    let mut cluster = Menmos::new().await?;
    cluster.add_amphora("alpha").await?;

    let blob_id = cluster.push_document("Hello world", Meta::new()).await?;

    cluster.flush().await?;

    let range_bytes = cluster.client.read_range(&blob_id, (2, 8)).await?;
    let range_str = String::from_utf8_lossy(&range_bytes);

    assert_eq!(range_str, "llo wor");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_blob_range_overflow() -> Result<()> {
    let mut cluster = Menmos::new().await?;
    cluster.add_amphora("alpha").await?;

    let blob_id = cluster.push_document("Hello world", Meta::new()).await?;

    cluster.flush().await?;

    let range_bytes = cluster.client.read_range(&blob_id, (2, 999)).await?;
    let range_str = String::from_utf8_lossy(&range_bytes);

    assert_eq!(range_str, "llo world");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_blob_range_invalid() -> Result<()> {
    let mut cluster = Menmos::new().await?;
    cluster.add_amphora("alpha").await?;

    let blob_id = cluster.push_document("Hello world", Meta::new()).await?;

    cluster.flush().await?;

    assert!(cluster.client.read_range(&blob_id, (2, 1)).await.is_err());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_empty_blob_range() -> Result<()> {
    let mut cluster = Menmos::new().await?;
    cluster.add_amphora("alpha").await?;

    let blob_id = cluster.push_document("Hello world", Meta::new()).await?;

    cluster.flush().await?;

    assert_eq!(cluster.client.read_range(&blob_id, (1, 1)).await?, b"e");

    Ok(())
}
