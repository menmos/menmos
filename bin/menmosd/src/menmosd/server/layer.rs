use std::sync::Arc;
use std::time::Duration;

use apikit::middleware::MakeRequestUuid;

use axum::extract::Extension;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::Request;
use axum::response::Response;
use axum::Router;

use headers::HeaderName;

use hyper::Method;

use interface::{CertificateInfo, DynDirectoryNode};

use opentelemetry::global;
use opentelemetry::sdk::propagation::TraceContextPropagator;
use opentelemetry::trace::SpanKind;
use opentelemetry_http::HeaderExtractor;

use tracing_opentelemetry::OpenTelemetrySpanExt;

use tower::ServiceBuilder;
use tower_http::cors::{Any, CorsLayer};
use tower_http::request_id::{PropagateRequestIdLayer, RequestId, SetRequestIdLayer};
use tower_http::trace::TraceLayer;

use crate::Config;

/// Wraps a router in a logging layer.
fn wrap_trace_layer(router: Router) -> Router {
    global::set_text_map_propagator(TraceContextPropagator::new());

    let svc = ServiceBuilder::new()
        .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|r: &Request<_>| {
                    // Extract the opentelemetry trace context.
                    let parent_cx = global::get_text_map_propagator(|propagator| {
                        propagator.extract(&HeaderExtractor(r.headers()))
                    });

                    let request_id = r
                        .extensions()
                        .get::<RequestId>()
                        .and_then(|id| id.header_value().to_str().ok())
                        .map(String::from)
                        .unwrap_or_else(|| String::from("unknown"));

                    let s = tracing::info_span!(
                        "request",
                        http.method = %r.method(),
                        http.url = %r.uri().path(),
                        otel.kind = %SpanKind::Server,
                        request.id = %request_id
                    );

                    s.set_parent(parent_cx);
                    s
                })
                .on_request(|_r: &Request<_>, _s: &tracing::Span| {}) // We silence the on-request hook
                .on_response(
                    |response: &Response, latency: Duration, _span: &tracing::Span| {
                        tracing::info!(
                            status = ?response.status(),
                            elapsed = ?latency,
                            "otel.status_code"=?response.status(),
                            http.status_code=%response.status(),
                            "complete");
                    },
                ),
        )
        .layer(axum::middleware::from_fn(
            apikit::middleware::propagate_tracing_context,
        ))
        .layer(PropagateRequestIdLayer::x_request_id());

    router.layer(svc)
}

/// Wraps a router with our extension layers.
fn wrap_extension_layers(
    router: Router,
    config: Arc<Config>,
    node: DynDirectoryNode,
    certificate_info: Arc<Option<CertificateInfo>>,
) -> Router {
    router
        .layer(Extension(certificate_info))
        .layer(Extension(menmos_auth::EncryptionKey {
            key: config.node.encryption_key.clone(),
        }))
        .layer(Extension(config.clone()))
        .layer(Extension(node.clone()))
}

fn wrap_cors_layer(router: Router) -> Router {
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
            HeaderName::from_static("x-request-id"),
        ]);

    router.layer(cors)
}

pub fn wrap(
    mut router: Router,
    config: Arc<Config>,
    node: DynDirectoryNode,
    certificate_info: Arc<Option<CertificateInfo>>,
) -> Router {
    router = wrap_trace_layer(router);
    router = wrap_extension_layers(router, config, node, certificate_info);
    wrap_cors_layer(router)
}
