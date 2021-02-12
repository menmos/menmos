//! Tests basic functionality of menmosd, without a storage node attached.
mod fixtures;

use anyhow::Result;
use interface::{QueryResponse, Type};
use menmos_client::{Meta, Query};

#[tokio::test]
async fn comes_up_and_stops() -> Result<()> {
    let fixture = fixtures::Menmos::new().await?;

    // Make sure the server responds.
    fixture.client.health().await?;

    // Make sure it stops.
    fixture.stop_all().await?;

    Ok(())
}

#[tokio::test]
async fn queries_initially_return_empty() -> Result<()> {
    let fixture = fixtures::Menmos::new().await?;

    let actual_response = fixture.client.query(Query::default()).await?;

    let expected_response = QueryResponse {
        count: 0,
        total: 0,
        hits: Vec::default(),
        facets: None,
    };

    assert_eq!(expected_response, actual_response);

    fixture.stop_all().await?;

    Ok(())
}

#[tokio::test]
async fn no_storage_nodes_initially_registered() -> Result<()> {
    let fixture = fixtures::Menmos::new().await?;

    let resp = fixture.client.list_storage_nodes().await?;

    assert_eq!(resp.storage_nodes.len(), 0);

    fixture.stop_all().await?;

    Ok(())
}

#[tokio::test]
async fn add_single_storage_node() -> Result<()> {
    let mut fixture = fixtures::Menmos::new().await?;

    fixture.add_amphora("alpha").await?;

    let resp = fixture.client.list_storage_nodes().await?;
    assert_eq!(resp.storage_nodes.len(), 1);
    assert_eq!(&resp.storage_nodes[0].id, "alpha");

    fixture.stop_all().await?;

    Ok(())
}

#[tokio::test]
async fn simple_put_query_loop() -> Result<()> {
    let mut fixture = fixtures::Menmos::new().await?;
    fixture.add_amphora("alpha").await?;

    let blob_id = fixture
        .push_document("hello world", Meta::new("myfile", Type::File))
        .await?;

    let results = fixture.client.query(Query::default()).await?;

    assert_eq!(results.total, 1);
    assert_eq!(results.count, 1);
    assert_eq!(results.hits[0].id, blob_id);

    fixture.stop_all().await?;

    Ok(())
}
