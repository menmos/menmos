//! Test blob routing.
use anyhow::Result;
use interface::RoutingConfig;
use menmos_client::{Client, Meta};
use testing::fixtures::Menmos;

#[tokio::test]
async fn get_set_delete_routing_config() -> Result<()> {
    let cluster = Menmos::new().await?;

    // Key doesn't exist in the beginning.
    let response = cluster.client.get_routing_config().await?;
    assert_eq!(response, None);

    let cfg = RoutingConfig::new("some_field").with_route("a", "b");

    cluster.client.set_routing_config(&cfg).await?;

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
    let response = cluster.client.get_routing_config().await?;
    assert_eq!(response, None);

    cluster.stop_all().await?;
    Ok(())
}

#[tokio::test]
async fn move_request_dispatch() -> Result<()> {
    let mut cluster = Menmos::new().await?;

    cluster.add_amphora("alpha").await?;

    let blob_a = cluster
        .push_document(
            "yeet yeet",
            Meta::file("file1.txt").with_meta("some_field", "bing"),
        )
        .await?;

    let blob_b = cluster
        .push_document(
            "bing bong",
            Meta::file("file2.txt").with_meta("some_field", "bong"),
        )
        .await?;

    // Create a new empty node after adding both documents.
    cluster.add_amphora("beta").await?;

    // Set the routing config.
    let cfg = RoutingConfig::new("some_field").with_route("bing", "beta");
    cluster.client.set_routing_config(&cfg).await?;

    // Check-in manually as the "alpha" storage node to check if there are pending move requests.
    let move_requests = cluster.get_move_requests_from("alpha").await?;
    assert_eq!(move_requests.len(), 1);
    assert_eq!(&move_requests[0].blob_id, &blob_a);

    // Update the routing config so the second blob is the one that should be moved.
    let cfg = RoutingConfig::new("some_field").with_route("bong", "beta");
    cluster.client.set_routing_config(&cfg).await?;
    // Check-in manually as the "alpha" storage node to check if there are pending move requests.
    let move_requests = cluster.get_move_requests_from("alpha").await?;
    assert_eq!(move_requests.len(), 1);
    assert_eq!(&move_requests[0].blob_id, &blob_b);

    cluster.stop_all().await?;
    Ok(())
}
