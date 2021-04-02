//! Test blob routing.
use anyhow::Result;
use interface::RoutingConfig;
use menmos_client::Client;
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
