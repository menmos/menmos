use std::time::Duration;

use axum::extract::Extension;
use axum::http::Request;
use axum::response::Response;
use axum::Router;

use tower_http::trace::TraceLayer;

use interface::DynStorageNode;

use menmos_auth::EncryptionKey;

use crate::Config;

pub fn wrap(router: Router, node: DynStorageNode, config: &Config) -> Router {
    router
        .layer(Extension(node))
        .layer(Extension(EncryptionKey {
            key: config.node.encryption_key.clone(),
        }))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|r: &Request<_>| {
                    if let Some(request_id) = r.headers().get("x-request-id") {
                        // Safe because a header value is always ascii as per http spec.
                        let request_id = String::from_utf8(request_id.as_bytes().to_vec()).unwrap();
                        tracing::info_span!(
                            "request",
                            id = %request_id,
                            method = %r.method(),
                            uri = %r.uri(),
                        )
                    } else {
                        tracing::info_span!(
                            "request",
                            method = %r.method(),
                            uri = %r.uri().path()
                        )
                    }
                })
                .on_request(|_r: &Request<_>, _s: &tracing::Span| {}) // We silence the on-request hook
                .on_response(
                    |response: &Response, latency: Duration, _span: &tracing::Span| {
                        tracing::info!(status = ?response.status(), elapsed = ?latency, "complete");
                    },
                ),
        )
}
