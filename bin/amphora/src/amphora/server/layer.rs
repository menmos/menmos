use std::time::Duration;

use axum::http::Request;
use axum::response::Response;
use axum::{AddExtensionLayer, Router};

use tower_http::trace::TraceLayer;

use interface::DynStorageNode;

use menmos_auth::EncryptionKey;

use crate::Config;

pub fn wrap(router: Router, node: DynStorageNode, config: &Config) -> Router {
    router
        .layer(AddExtensionLayer::new(node))
        .layer(AddExtensionLayer::new(EncryptionKey {
            key: config.node.encryption_key.clone(),
        }))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|r: &Request<_>| {
                    /* TODO: Make menmosd add a header/queryparam to the request.
                    // We get the request id from the extensions
                    let request_id = r
                        .extensions()
                        .get::<RequestId>()
                        .map(ToString::to_string)
                        .unwrap_or_else(|| "unknown".into());
                    // And then we put it along with other information into the `request` span
                    */
                    tracing::info_span!(
                        "request",
                        //id = %request_id,
                        method = %r.method(),
                        uri = %r.uri(),
                    )
                })
                .on_request(|_r: &Request<_>, _s: &tracing::Span| {}) // We silence the on-request hook
                .on_response(
                    |response: &Response, latency: Duration, _span: &tracing::Span| {
                        tracing::info!(status = ?response.status(), elapsed = ?latency, "complete");
                    },
                ),
        )
}
