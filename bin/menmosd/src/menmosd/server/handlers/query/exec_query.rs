use std::convert::TryFrom;
use std::net::IpAddr;
use std::sync::Arc;

use anyhow::{anyhow, Result};

use axum::extract::Extension;
use axum::Json;
use axum_client_ip::ClientIp;

use apikit::reject::HTTPError;

use hyper::Uri;

use interface::{DynDirectoryNode, Query, QueryResponse};

use menmos_auth::UserIdentity;

use protocol::directory::query::QueryRequest;

use crate::{network::get_storage_node_address, Config};

async fn get_blob_url<S: AsRef<str>>(
    node: &DynDirectoryNode,
    config: &Arc<Config>,
    blob_id: S,
    request_ip: &IpAddr,
) -> Result<Uri> {
    let storage_node = node
        .indexer()
        .get_blob_storage_node(blob_id.as_ref())
        .await?
        .ok_or_else(|| anyhow!("blob not found"))?;

    let uri = get_storage_node_address(
        *request_ip,
        storage_node,
        config,
        &format!("blob/{}", blob_id.as_ref()),
    )?;

    Ok(uri)
}

#[tracing::instrument(skip(results, node, config, request_ip, identity), fields(len=results.count))]
async fn fetch_urls(
    signed: bool,
    results: &mut QueryResponse,
    node: DynDirectoryNode,
    config: Arc<Config>,
    request_ip: IpAddr,
    identity: UserIdentity,
) -> Result<()> {
    let mut new_hits = Vec::with_capacity(results.count);

    for hit in results.hits.iter_mut() {
        match get_blob_url(&node, &config, &hit.id, &request_ip).await {
            Ok(uri) => {
                let mut blob_uri = uri.to_string();
                // Sign the URL if requested
                if signed {
                    let mut identity = identity.clone();
                    identity.blobs_whitelist = Some(vec![hit.id.clone()]);
                    let tok = menmos_auth::make_token(&config.node.encryption_key, &identity)?;
                    blob_uri += &format!("?signature={}", tok);
                }

                hit.url = blob_uri;
                new_hits.push(hit.clone());
            }
            Err(e) => {
                tracing::warn!(blob_id = ?hit.id, "error getting blob uri: {}", e);
            }
        }
    }

    results.count = new_hits.len();
    results.hits = new_hits;

    Ok(())
}

#[tracing::instrument(skip(node, config, addr, query_request))]
pub async fn query(
    user: UserIdentity,
    Extension(node): Extension<DynDirectoryNode>,
    Extension(config): Extension<Arc<Config>>,
    ClientIp(addr): ClientIp,
    Json(query_request): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, HTTPError> {
    let query = Query::try_from(query_request).map_err(HTTPError::bad_request)?;
    tracing::debug!(query = ?query.expression, "query request");

    let mut query_response = node
        .query()
        .query(&query, &user.username)
        .await
        .map_err(HTTPError::internal_server_error)?;

    fetch_urls(
        query.sign_urls,
        &mut query_response,
        node,
        config,
        addr,
        user,
    )
    .await
    .map_err(HTTPError::internal_server_error)?;

    Ok(Json(query_response))
}
