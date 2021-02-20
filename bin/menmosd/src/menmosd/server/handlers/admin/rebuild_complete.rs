use apikit::reject::InternalServerError;

use interface::message as msg;

use warp::reply;

use crate::server::context::Context;

pub async fn rebuild_complete(
    context: Context,
    storage_node_id: String,
) -> Result<reply::Response, warp::Rejection> {
    context
        .node
        .rebuild_complete(&storage_node_id)
        .await
        .map_err(InternalServerError::from)?;

    Ok(apikit::reply::json(&msg::MessageResponse {
        message: String::from("OK"),
    }))
}
