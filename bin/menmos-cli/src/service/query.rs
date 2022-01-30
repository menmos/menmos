use anyhow::{anyhow, Result};
use futures::TryStreamExt;
use menmos::{Menmos, Query};
use rood::cli::OutputManager;

pub async fn execute(
    cli: OutputManager,
    expression: Option<String>,
    from: usize,
    size: usize,
    all: bool,
    client: Menmos,
) -> Result<()> {
    let mut q = Query::default().with_from(from).with_size(size);
    if let Some(expr) = expression {
        q = q.with_expression(expr)?;
    }

    let mut result_stream = client.query(q);
    let pushed = cli.push();
    let mut result_count = 0;

    while let Some(hit) = result_stream.try_next().await.map_err(|e| anyhow!("{e}"))? {
        cli.step(hit.id);
        pushed.debug(hit.url);

        result_count += 1;

        if !all && result_count >= size {
            break;
        }
    }

    Ok(())
}
