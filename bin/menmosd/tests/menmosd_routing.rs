//! Test blob routing.
use anyhow::Result;
use menmos_client::Client;
use testing::fixtures::Menmos;

#[tokio::test]
async fn get_set_delete_routing_key() -> Result<()> {
    let cluster = Menmos::new().await?;

    // Key doesn't exist in the beginning.
    let response = cluster.client.get_routing_key().await?;
    assert_eq!(response.routing_key, None);

    cluster.client.set_routing_key("some_field").await?;

    // Key exists afterwards.
    let response = cluster.client.get_routing_key().await?;
    assert_eq!(response.routing_key, Some(String::from("some_field")));

    // Other user doesn't see the routing key.
    cluster.add_user("john", "bingbong").await?;
    let john_client = Client::new(&cluster.directory_url, "john", "bingbong").await?;
    let response = john_client.get_routing_key().await?;
    assert_eq!(response.routing_key, None);

    // Deleting the key works.
    cluster.client.delete_routing_key().await?;
    let response = cluster.client.get_routing_key().await?;
    assert_eq!(response.routing_key, None);

    cluster.stop_all().await?;
    Ok(())
}
