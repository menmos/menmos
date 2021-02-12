use anyhow::Result;
use client::{Client, Query};
use rood::cli::OutputManager;

pub async fn execute(
    cli: OutputManager,
    expression: Option<String>,
    from: usize,
    size: usize,
    client: Client,
) -> Result<()> {
    let mut q = Query::default().with_from(from).with_size(size);

    if let Some(expr) = expression {
        q = q.with_expression(expr)?;
    }

    let resp = client.query(q).await?;

    for hit in resp.hits {
        cli.step(hit.url);
    }

    Ok(())
}
