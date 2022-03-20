use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::routing::*;
use headers::HeaderName;
use hyper::Method;
use tower_http::cors::{Any, CorsLayer};

use super::handlers;

fn auth() -> Router {
    Router::new()
        .route("/login", post(handlers::auth::login))
        .route("/register", post(handlers::auth::register))
}

fn blob() -> Router {
    Router::new()
        .route("/", post(handlers::blob::put))
        .route(
            "/:blob_id/metadata",
            get(handlers::blobmeta::get)
                .post(handlers::blobmeta::create)
                .put(handlers::blobmeta::update)
                .delete(handlers::blobmeta::delete),
        )
        // Blob routes
        .route(
            "/:blob_id",
            get(handlers::blob::get)
                .post(handlers::blob::update)
                .put(handlers::blob::write)
                .delete(handlers::blob::delete),
        )
        .route("/:blob_id/fsync", post(handlers::blob::fsync))
}

pub fn new() -> Router {
    let cors = CorsLayer::new()
        .allow_methods(vec![
            Method::GET,
            Method::POST,
            Method::DELETE,
            Method::PUT,
            Method::OPTIONS,
        ])
        // TODO: add config to allows specifying whitelisted origins
        .allow_origin(Any)
        .allow_headers(vec![
            CONTENT_TYPE,
            AUTHORIZATION,
            HeaderName::from_static("x-blob-meta"),
            HeaderName::from_static("x-blob-size"),
        ]);

    Router::new()
        // Admin Routes
        .route("/health", get(handlers::admin::health))
        .route("/version", get(handlers::admin::version))
        .route("/rebuild", post(handlers::admin::rebuild))
        .route(
            "/rebuild/:storage_node_id",
            delete(handlers::admin::rebuild_complete),
        )
        .route("/flush", post(handlers::admin::flush))
        .route("/config", get(handlers::admin::get_config))
        .route("/query", post(handlers::query::query))
        .route(
            "/node/storage",
            put(handlers::storage::put).get(handlers::storage::list),
        )
        .route(
            "/routing",
            get(handlers::routing::get)
                .put(handlers::routing::set)
                .delete(handlers::routing::delete),
        )
        .route("/metadata", get(handlers::blobmeta::list))
        .nest("/auth", auth())
        .nest("/blob", blob())
        .nest(
            "/web",
            Router::new().route("/*path", get(handlers::webui::serve_static)),
        )
        .layer(cors)
}
