//! Test query-related features.

use anyhow::Result;
use menmos_client::{Meta, Query, Type};
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
            .push_document("some text", Meta::new(format!("doc_{}", i), Type::File))
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
                let expected_name = format!("doc_{}", from + i);
                assert_eq!(hit.meta.name, expected_name);
            }
        }
    }

    cluster.stop_all().await?;

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

    let blob_id = cluster
        .push_document("Hello world", Meta::file("test_blob"))
        .await?;

    // Make sure datetimes make sense.
    let meta = cluster.client.get_meta(&blob_id).await?.unwrap();

    let created_at = meta.created_at;
    let modified_at = meta.modified_at;

    assert_eq!(created_at, modified_at);

    // Update the file and make sure the meta was updated.
    cluster
        .client
        .update_meta(&blob_id, Meta::file("test_blob"))
        .await?;

    let results = cluster.client.query(Query::default()).await?;
    assert!(results.hits[0].meta.modified_at > created_at);

    Ok(())
}

// TODO: More advanced query tests.
