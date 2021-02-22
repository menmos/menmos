use apikit::reject::InternalServerError;

use interface::message as msg;
use warp::reply;

use crate::server::context::Context;

pub async fn commit(context: Context) -> Result<reply::Response, warp::Rejection> {
    context
        .node
        .commit()
        .await
        .map_err(InternalServerError::from)?;

    Ok(apikit::reply::json(&msg::MessageResponse {
        message: "OK".into(),
    }))
}
