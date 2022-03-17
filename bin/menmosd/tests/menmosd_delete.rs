//! Test blob deletion capabilities.
use anyhow::Result;
use menmos_client::Meta;
use testing::fixtures::Menmos;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_blob() -> Result<()> {
    let mut cluster = Menmos::new().await?;
    cluster.add_amphora("alpha").await?;

    let blob_id = cluster.push_document("Hello world!", Meta::new()).await?;

    cluster.client.delete(blob_id.clone()).await?;
    cluster.flush().await?;

    let file = cluster.client.get_file(&blob_id).await;
    assert!(file.is_err());

    cluster.stop_all().await?;

    Ok(())
}
