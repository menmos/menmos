//! Test blob routing.
mod util;

use std::time::Duration;

use anyhow::Result;
use interface::RoutingConfig;
use menmos_client::{Client, Meta};
use testing::fixtures::Menmos;
use util::stream_to_bytes;

#[tokio::test]
async fn get_set_delete_routing_config() -> Result<()> {
    let cluster = Menmos::new().await?;

    // Key doesn't exist in the beginning.
    let response = cluster.client.get_routing_config().await?;
    assert_eq!(response, None);

    let cfg = RoutingConfig::new("some_field").with_route("a", "b");

    cluster.client.set_routing_config(&cfg).await?;
    cluster.flush().await?;

    // Key exists afterwards.
    let response = cluster.client.get_routing_config().await?;
    assert_eq!(response, Some(cfg.clone()));

    // Other user doesn't see the routing key.
    cluster.add_user("john", "bingbong").await?;
    let john_client = Client::new(&cluster.directory_url, "john", "bingbong").await?;
    let response = john_client.get_routing_config().await?;
    assert_eq!(response, None);

    // Deleting the key works.
    cluster.client.delete_routing_config().await?;
    cluster.flush().await?;
    let response = cluster.client.get_routing_config().await?;
    assert_eq!(response, None);

    cluster.stop_all().await?;
    Ok(())
}

#[tokio::test]
async fn move_request_full_loop() -> Result<()> {
    let mut cluster = Menmos::new().await?;

    cluster.add_amphora("alpha").await?;

    // We add a blob on amphora alpha.
    let blob_id = cluster
        .push_document("yeet yeet", Meta::new().with_meta("some_file", "bing"))
        .await?;

    cluster.flush().await?;

    // We verify the blob is there.
    assert!(cluster
        .root_directory
        .as_ref()
        .join("alpha-blobs")
        .join(&blob_id)
        .with_extension("blob")
        .exists());

    // Then we add a new storage node, send a move request to move the blob over there, and wait a bit.
    cluster.add_amphora("beta").await?;
    cluster
        .client
        .set_routing_config(&RoutingConfig::new("some_file").with_route("bing", "beta"))
        .await?;

    cluster.flush().await?;

    tokio::time::sleep(Duration::from_secs(2)).await;

    // We verify the blob has moved.
    assert!(!cluster
        .root_directory
        .as_ref()
        .join("alpha-blobs")
        .join(&blob_id)
        .with_extension("blob")
        .exists());

    assert!(cluster
        .root_directory
        .as_ref()
        .join("beta-blobs")
        .join(&blob_id)
        .with_extension("blob")
        .exists());

    // And we verify we can still fetch the blob.
    let file_stream = cluster.client.get_file(&blob_id).await?;
    let file_bytes = stream_to_bytes(file_stream).await?;
    let file_string = String::from_utf8_lossy(file_bytes.as_ref());
    assert_eq!(file_string, "yeet yeet");

    cluster.stop_all().await?;
    Ok(())
}
