#[tracing::instrument]
pub async fn health() -> Result<impl warp::Reply, warp::Rejection> {
    Ok(apikit::reply::message("healthy"))
}
