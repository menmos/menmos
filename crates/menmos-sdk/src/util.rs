use futures::TryStream;

use interface::{BlobMeta, Hit};

use menmos_client::{ClientError, Query};

use snafu::prelude::*;

use crate::ClientRC;

#[derive(Debug, Snafu)]
pub enum UtilError {
    #[snafu(display("failed to get metadata for blob '{}'", blob_id))]
    GetMetaError {
        blob_id: String,
        source: ClientError,
    },

    #[snafu(display("blob '{}' does not exist", blob_id))]
    BlobDoesNotExist { blob_id: String },

    #[snafu(display("query failed"))]
    QueryError { source: ClientError },
}

type Result<T> = std::result::Result<T, UtilError>;

pub async fn get_meta_if_exists(client: &ClientRC, blob_id: &str) -> Result<Option<BlobMeta>> {
    let r = client
        .get_meta(blob_id)
        .await
        .with_context(|_| GetMetaSnafu {
            blob_id: String::from(blob_id),
        })?;
    Ok(r)
}

pub async fn get_meta(client: &ClientRC, blob_id: &str) -> Result<BlobMeta> {
    get_meta_if_exists(client, blob_id)
        .await?
        .context(BlobDoesNotExistSnafu {
            blob_id: String::from(blob_id),
        })
}

/// Scrolls a given query until the end of results and returns the output lazily as a stream.
pub fn scroll_query(
    query: Query,
    client: &ClientRC,
) -> impl TryStream<Ok = Hit, Error = UtilError> + Unpin {
    Box::pin(futures::stream::try_unfold(
        (query, Vec::<Hit>::new(), false, client.clone()),
        move |(mut n_query, mut pending_hits, mut page_end_reached, client)| async move {
            if let Some(hit) = pending_hits.pop() {
                return Ok(Some((
                    hit,
                    (n_query, pending_hits, page_end_reached, client),
                )));
            }

            if page_end_reached {
                return Ok(None);
            }

            let results = client.query(n_query.clone()).await.context(QuerySnafu)?;

            pending_hits.extend(results.hits.into_iter());

            n_query.from += results.count;
            page_end_reached = n_query.from >= results.total;

            if let Some(r_val) = pending_hits.pop() {
                let ret_tuple = (n_query, pending_hits, page_end_reached, client);
                Ok(Some((r_val, ret_tuple)))
            } else {
                Ok(None)
            }
        },
    ))
}
