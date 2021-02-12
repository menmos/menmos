use std::sync::Arc;

use apikit::reject::InternalServerError;

use interface::message as msg;
use interface::DirectoryNode;
use warp::reply;

pub async fn commit<N: DirectoryNode>(node: Arc<N>) -> Result<reply::Response, warp::Rejection> {
    node.commit().await.map_err(InternalServerError::from)?;
    Ok(apikit::reply::json(&msg::MessageResponse {
        message: "OK".into(),
    }))
}
