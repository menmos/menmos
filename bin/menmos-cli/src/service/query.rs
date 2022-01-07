use anyhow::Result;
use interface::QueryResponse;
use menmos_client::{Client, Query};
use rood::cli::OutputManager;

fn output_results(resp: QueryResponse, cli: OutputManager) {
    let pushed = cli.push();

    for hit in resp.hits {
        cli.step(hit.id);
        pushed.debug(hit.url);
    }
}

pub async fn execute(
    cli: OutputManager,
    expression: Option<String>,
    mut from: usize,
    size: usize,
    all: bool,
    client: Client,
) -> Result<()> {
    let mut q = Query::default().with_from(from).with_size(size);
    if let Some(expr) = expression {
        q = q.with_expression(expr)?;
    }

    let resp = client.query(q.clone()).await?;

    let mut count = resp.count;
    let total = resp.total;

    output_results(resp, cli.clone());
    from += count;

    if all && count > total {
        loop {
            let query = q.clone().with_from(from);
            let resp = client.query(query.clone()).await?;
            count += resp.count;
            from += resp.count;

            output_results(resp, cli.clone());

            if count >= total {
                break;
            }
        }
    }

    cli.debug(format!("{}/{} hits", count, total));

    Ok(())
}
