use warp::Filter;

use crate::server::handlers;

pub fn serve_static() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::get()
        .and(warp::path::tail())
        .and_then(handlers::webui::serve_static)
}
