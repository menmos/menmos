//! Test query-related features.
mod fixtures;

use anyhow::Result;
use menmos_client::{Meta, Query, Type};

#[tokio::test]
async fn query_pagination() -> Result<()> {
    let mut cluster = fixtures::Menmos::new().await?;
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

// TODO: More advanced query tests.
