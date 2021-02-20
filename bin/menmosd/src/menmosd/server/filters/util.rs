use warp::Filter;

use crate::server::Context;

pub fn with_context(
    context: Context,
) -> impl Filter<Extract = (Context,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || context.clone())
}
