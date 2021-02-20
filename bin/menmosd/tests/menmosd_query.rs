//! Test query-related features.

use anyhow::Result;
use interface::{LoginRequest, LoginResponse};
use menmos_client::{Meta, Query, Type};
use reqwest::StatusCode;
use serde::Serialize;

#[tokio::test]
async fn query_pagination() -> Result<()> {
    let mut cluster = testing::fixtures::Menmos::new().await?;
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
    let cluster = testing::fixtures::Menmos::new().await?;

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

// TODO: More advanced query tests.
