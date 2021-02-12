use std::convert::TryFrom;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use anyhow::{anyhow, Result};

use apikit::reject::{BadRequest, InternalServerError};

use interface::message::directory_node as msg;
use interface::{DirectoryNode, QueryResponse};

use msg::Query;
use warp::http::Uri;
use warp::reply;

use crate::{network::get_storage_node_address, Config};

async fn get_blob_url<N: DirectoryNode, S: AsRef<str>>(
    node: Arc<N>,
    blob_id: S,
    request_ip: &IpAddr,
    cfg: &Config,
) -> Result<Uri> {
    let storage_node = node
        .get_blob_storage_node(blob_id.as_ref())
        .await?
        .ok_or_else(|| anyhow!("blob {} not found", blob_id.as_ref()))?;

    let uri = get_storage_node_address(
        *request_ip,
        storage_node,
        cfg,
        &format!("blob/{}", blob_id.as_ref()),
    )?;

    Ok(uri)
}

async fn fetch_urls<N: DirectoryNode>(
    signed: bool,
    results: &mut QueryResponse,
    node: Arc<N>,
    request_ip: IpAddr,
    cfg: &Config,
) -> Result<()> {
    let mut new_hits = Vec::with_capacity(results.count);

    for hit in results.hits.iter_mut() {
        match get_blob_url(node.clone(), &hit.id, &request_ip, cfg).await {
            Ok(uri) => {
                let mut blob_uri = uri.to_string();
                // Sign the URL if requested
                if signed {
                    let tok = urlsign::sign(&hit.id, &cfg.node.encryption_key)?;
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

pub async fn query<N: DirectoryNode>(
    cfg: Config,
    node: Arc<N>,
    addr: Option<SocketAddr>,
    query_request: msg::QueryRequest,
) -> Result<reply::Response, warp::Rejection> {
    let socket_addr = addr.ok_or_else(|| InternalServerError::from("missing socket address"))?;

    let query = Query::try_from(query_request).map_err(|_| BadRequest)?;

    let mut query_response = node
        .query(&query)
        .await
        .map_err(InternalServerError::from)?;

    fetch_urls(
        query.sign_urls,
        &mut query_response,
        node,
        socket_addr.ip(),
        &cfg,
    )
    .await
    .map_err(InternalServerError::from)?;

    Ok(apikit::reply::json(&query_response))
}
