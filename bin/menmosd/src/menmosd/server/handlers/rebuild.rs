use std::sync::Arc;

use apikit::reject::InternalServerError;

use interface::{message as msg, DirectoryNode};

use warp::reply;

pub async fn rebuild<N: DirectoryNode>(node: Arc<N>) -> Result<reply::Response, warp::Rejection> {
    node.start_rebuild()
        .await
        .map_err(InternalServerError::from)?;

    Ok(apikit::reply::json(&msg::MessageResponse {
        message: String::from("Rebuild started"),
    }))
}
