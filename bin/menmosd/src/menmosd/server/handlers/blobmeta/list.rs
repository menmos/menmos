use apikit::reject::InternalServerError;

use interface::ListMetadataRequest;

use warp::reply;

use crate::server::Context;

pub async fn list(
    context: Context,
    req: ListMetadataRequest,
) -> Result<reply::Response, warp::Rejection> {
    let response = context
        .node
        .list_metadata(&req)
        .await
        .map_err(InternalServerError::from)?;
    Ok(apikit::reply::json(&response))
}
