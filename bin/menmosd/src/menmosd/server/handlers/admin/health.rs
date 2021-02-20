use interface::message as msg;

use warp::reply;

pub async fn health() -> Result<impl warp::Reply, warp::Rejection> {
    Ok(reply::json(&msg::MessageResponse {
        message: String::from("healthy"),
    }))
}
