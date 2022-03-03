use axum::routing::*;

use super::handlers;

pub fn new() -> Router {
    Router::new()
        .route("/health", get(handlers::health))
        .route("/version", get(handlers::version))
        .route("/flush", post(handlers::flush))
        .route(
            "/blob/:blob_id",
            get(handlers::get)
                .post(handlers::put)
                .put(handlers::write)
                .delete(handlers::delete),
        )
        .route("/blob/:blob_id/metadata", put(handlers::update_meta))
        .route("/blob/:blob_id/fsync", post(handlers::fsync))
}
