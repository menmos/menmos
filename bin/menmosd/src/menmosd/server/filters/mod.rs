mod admin;
mod auth;
mod blob;
mod blobmeta;
mod query;
mod storage;
mod util;

use std::convert::Infallible;

use warp::Filter;

use crate::server::Context;

pub fn all(
    context: Context,
) -> impl Filter<Extract = impl warp::Reply, Error = Infallible> + Clone {
    storage::all(context.clone())
        .or(blob::all(context.clone()))
        .or(blobmeta::all(context.clone()))
        .or(admin::all(context.clone()))
        .or(query::all(context.clone()))
        .or(auth::all(context))
        .recover(apikit::reject::recover)
        .with(warp::log("directory::api"))
}
