mod util;

mod webui;

use std::convert::Infallible;

use warp::Filter;

use crate::server::Context;

#[cfg(not(any(debug_assertions, feature = "menmos_debug")))]
pub fn all(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = Infallible> + Clone {
    webui::serve_static()
        .recover(apikit::reject::recover)
        .with(warp::log::custom(
            |info| tracing::info!(status = ?info.status(), elapsed = ?info.elapsed(), "{} {}", info.method(), info.path()),
        ))
}

#[cfg(any(debug_assertions, feature = "menmos_debug"))]
pub fn all(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = Infallible> + Clone {
    webui::serve_static()
        .with(
            warp::cors()
                .allow_any_origin()
                .allow_headers(vec!["Content-Type", "x-blob-meta", "Authorization"])
                .allow_methods(vec!["GET", "POST", "DELETE", "PUT", "OPTIONS"]),
        )
        .with(warp::log::custom(
            |info| tracing::info!(status = ?info.status(), elapsed = ?info.elapsed(), "{} {}", info.method(), info.path()),
        ))
        .recover(apikit::reject::recover)
}
