use std::convert::TryFrom;
use std::net::{IpAddr, SocketAddr};

use anyhow::{anyhow, Result};

use apikit::{
    auth::UserIdentity,
    reject::{BadRequest, InternalServerError},
};

use interface::{Query, QueryResponse};

use protocol::directory::query::QueryRequest;

use warp::http::Uri;
use warp::reply;

use crate::{network::get_storage_node_address, server::context::Context};

async fn get_blob_url<S: AsRef<str>>(
    context: &Context,
    blob_id: S,
    request_ip: &IpAddr,
) -> Result<Uri> {
    let storage_node = context
        .node
        .indexer()
        .get_blob_storage_node(blob_id.as_ref())
        .await?
        .ok_or_else(|| anyhow!("blob {} not found", blob_id.as_ref()))?;

    let uri = get_storage_node_address(
        *request_ip,
        storage_node,
        &context.config,
        &format!("blob/{}", blob_id.as_ref()),
    )?;

    Ok(uri)
}

async fn fetch_urls(
    signed: bool,
    results: &mut QueryResponse,
    context: Context,
    request_ip: IpAddr,
    identity: UserIdentity,
) -> Result<()> {
    let mut new_hits = Vec::with_capacity(results.count);

    for hit in results.hits.iter_mut() {
        match get_blob_url(&context, &hit.id, &request_ip).await {
            Ok(uri) => {
                let mut blob_uri = uri.to_string();
                // Sign the URL if requested
                if signed {
                    let mut identity = identity.clone();
                    identity.blobs_whitelist = Some(vec![hit.id.clone()]);
                    let tok =
                        apikit::auth::make_token(&context.config.node.encryption_key, &identity)?;
                    blob_uri += &format!("?signature={}", tok);
                }

                hit.url = blob_uri;
                new_hits.push(hit.clone());
            }
            Err(e) => {
                log::warn!("error getting blob uri for {}: {}", hit.id, e);
            }
        }
    }

    results.count = new_hits.len();
    results.hits = new_hits;

    Ok(())
}

pub async fn query(
    user: UserIdentity,
    context: Context,
    addr: Option<SocketAddr>,
    query_request: QueryRequest,
) -> Result<reply::Response, warp::Rejection> {
    let socket_addr = addr.ok_or_else(|| InternalServerError::from("missing socket address"))?;

    let query = Query::try_from(query_request).map_err(|_| BadRequest)?;

    let mut query_response = context
        .node
        .query()
        .query(&query, &user.username)
        .await
        .map_err(InternalServerError::from)?;

    fetch_urls(
        query.sign_urls,
        &mut query_response,
        context,
        socket_addr.ip(),
        user,
    )
    .await
    .map_err(InternalServerError::from)?;

    Ok(apikit::reply::json(&query_response))
}
