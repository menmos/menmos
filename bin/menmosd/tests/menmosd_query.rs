//! Test query-related features.

use anyhow::Result;
use interface::{FieldValue, SortOrder};
use menmos_client::{Meta, Query};
use protocol::directory::auth::{LoginRequest, LoginResponse};
use reqwest::StatusCode;
use serde::Serialize;
use testing::fixtures::Menmos;

#[tokio::test]
async fn query_pagination() -> Result<()> {
    let mut cluster = Menmos::new().await?;
    cluster.add_amphora("alpha").await?;

    for i in 0..15 {
        cluster
            .push_document(
                "some text",
                Meta::new().with_field("name", format!("doc_{}", i)),
            )
            .await?;
    }

    for from in 0..15 {
        for size in 1..6 {
            let results = cluster
                .client
                .query(Query::default().with_size(size).with_from(from))
                .await?;

            let max_index = (from + size).min(15);
            let expected_count = max_index - from;

            assert_eq!(results.total, 15);
            assert_eq!(results.count, expected_count);
            assert_eq!(results.count, results.hits.len());

            for (i, hit) in results.hits.into_iter().enumerate() {
                let expected_name = FieldValue::Str(format!("doc_{}", from + i));
                assert_eq!(hit.meta.fields.get("name").unwrap(), &expected_name);
            }
        }
    }

    cluster.stop_all().await?;

    Ok(())
}

/// Tests that numeric fields are indexable & queryable properly.
#[tokio::test]
async fn query_numeric_fields() -> Result<()> {
    let mut cluster = Menmos::new().await?;
    cluster.add_amphora("alpha").await?;

    let blob_id = cluster
        .push_document(
            "some text",
            Meta::new()
                .with_field("name", "good movie")
                .with_field("rating", 5),
        )
        .await?;

    cluster
        .push_document(
            "some text",
            Meta::new()
                .with_field("name", "bad movie")
                .with_field("rating", 2),
        )
        .await?;

    // We insert one doc with a "rating" field with a string value to test that
    // we handle multi-type fields properly.
    cluster
        .push_document(
            "some text",
            Meta::new()
                .with_field("name", "not a movie")
                .with_field("rating", "string rating"),
        )
        .await?;

    let results = cluster
        .client
        .query(Query::default().and_field("rating", 5))
        .await?;

    assert_eq!(results.count, 1);
    assert_eq!(results.hits[0].id, blob_id);
    assert_eq!(
        results.hits[0].meta.fields.get("rating"),
        Some(FieldValue::Numeric(5)).as_ref()
    );

    Ok(())
}

#[tokio::test]
async fn query_bad_request() -> Result<()> {
    let cluster = Menmos::new().await?;

    #[derive(Serialize)]
    struct BadQuery {
        ya: String,
    }

    let client = reqwest::Client::new();

    // Get a token.
    let resp = client
        .post(&format!("{}/auth/login", &cluster.directory_url))
        .json(&LoginRequest {
            username: "admin".into(),
            password: cluster.directory_password.clone(),
        })
        .send()
        .await?;
    let r: LoginResponse = resp.json().await?;

    let response = client
        .post(&format!("{}/query", &cluster.directory_url))
        .bearer_auth(r.token)
        .json(&BadQuery { ya: "yeet".into() })
        .send()
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    cluster.stop_all().await?;

    Ok(())
}

// Makes sure that when datetime is updated, the new value is mirrored properly to the directory.
#[tokio::test]
async fn query_has_up_to_date_datetime() -> Result<()> {
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

    let results = cluster.client.query(Query::default()).await?;
    assert!(results.hits[0].meta.modified_at > created_at);

    cluster.stop_all().await?;

    Ok(())
}

#[tokio::test]
async fn query_sorting_order() -> Result<()> {
    let mut cluster = Menmos::new().await?;
    cluster.add_amphora("alpha").await?;

    cluster
        .push_document("Document 1", Meta::new().with_field("name", "blob_1"))
        .await?;

    cluster
        .push_document("Document 2", Meta::new().with_field("name", "blob_2"))
        .await?;

    cluster
        .push_document("Document 3", Meta::new().with_field("name", "blob_3"))
        .await?;

    let results = cluster.client.query(Query::default()).await?;
    assert_eq!(results.count, 3);
    assert_eq!(
        results
            .hits
            .iter()
            .map(|r| r.meta.fields.get("name").unwrap().clone())
            .collect::<Vec<_>>(),
        vec![
            FieldValue::from("blob_1"),
            FieldValue::from("blob_2"),
            FieldValue::from("blob_3"),
        ]
    );

    let results = cluster
        .client
        .query(Query::default().with_sort_order(SortOrder::CreationDescending))
        .await?;
    assert_eq!(results.count, 3);
    assert_eq!(
        results
            .hits
            .iter()
            .map(|r| r.meta.fields.get("name").unwrap().clone())
            .collect::<Vec<_>>(),
        vec![
            FieldValue::from("blob_3"),
            FieldValue::from("blob_2"),
            FieldValue::from("blob_1"),
        ]
    );

    Ok(())
}

// TODO: More advanced query tests.
