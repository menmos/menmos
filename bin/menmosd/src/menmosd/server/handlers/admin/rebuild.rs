use apikit::reject::InternalServerError;

use interface::message as msg;

use warp::reply;

use crate::server::context::Context;

pub async fn rebuild(context: Context) -> Result<reply::Response, warp::Rejection> {
    context
        .node
        .start_rebuild()
        .await
        .map_err(InternalServerError::from)?;

    Ok(apikit::reply::json(&msg::MessageResponse {
        message: String::from("Rebuild started"),
    }))
}
