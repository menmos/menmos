mod admin;
mod auth;
mod blob;
mod blobmeta;
mod query;
mod routing;
mod storage;
mod util;

use std::convert::Infallible;

use warp::Filter;

use crate::server::Context;

#[cfg(not(debug))]
pub fn all(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = Infallible> + Clone {
    storage::all(context.clone())
        .or(blob::all(context.clone()))
        .or(blobmeta::all(context.clone()))
        .or(admin::all(context.clone()))
        .or(query::all(context.clone()))
        .or(auth::all(context.clone()))
        .or(routing::all(context))
        .recover(apikit::reject::recover)
        .with(warp::log("directory::api"))
}

#[cfg(debug)]
pub fn all(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = Infallible> + Clone {
    storage::all(context.clone())
        .or(blob::all(context.clone()))
        .or(blobmeta::all(context.clone()))
        .or(admin::all(context.clone()))
        .or(query::all(context.clone()))
        .or(auth::all(context.clone()))
        .or(routing::all(context))
        .with(
            warp::cors()
                .allow_any_origin()
                .allow_headers(vec!["Content-Type", "x-blob-meta"])
                .allow_methods(vec!["GET", "POST", "DELETE", "PUT", "OPTIONS"]),
        )
        .with(warp::log("directory::api"))
        .recover(apikit::reject::recover);

    filters
}
