use std::sync::Arc;
use std::time::Duration;

use axum::http::Request;
use axum::response::Response;
use axum::{AddExtensionLayer, Router};

use crate::Config;
use interface::{CertificateInfo, DynDirectoryNode};
use tower_http::trace::TraceLayer;
use tower_request_id::{RequestId, RequestIdLayer};

/// Wraps a router in a logging layer.
fn wrap_trace_layer(router: Router) -> Router {
    router.layer(
        TraceLayer::new_for_http()
            .make_span_with(|r: &Request<_>| {
                // We get the request id from the extensions
                let request_id = r
                    .extensions()
                    .get::<RequestId>()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "unknown".into());
                // And then we put it along with other information into the `request` span
                tracing::info_span!(
                    "request",
                    id = %request_id,
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

/// Wraps a router with our extension layers.
fn wrap_extension_layers(
    router: Router,
    config: Arc<Config>,
    node: DynDirectoryNode,
    certificate_info: Arc<Option<CertificateInfo>>,
) -> Router {
    router
        .layer(AddExtensionLayer::new(certificate_info))
        .layer(AddExtensionLayer::new(menmos_auth::EncryptionKey {
            key: config.node.encryption_key.clone(),
        }))
        .layer(AddExtensionLayer::new(config.clone()))
        .layer(AddExtensionLayer::new(node.clone()))
}

pub fn wrap(
    mut router: Router,
    config: Arc<Config>,
    node: DynDirectoryNode,
    certificate_info: Arc<Option<CertificateInfo>>,
) -> Router {
    router = wrap_trace_layer(router);
    router = wrap_extension_layers(router, config, node, certificate_info);

    // Generate an ID for each request.
    router.layer(RequestIdLayer)
}
