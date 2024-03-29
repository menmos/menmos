use axum::error_handling::HandleErrorLayer;
use std::time::Duration;

use axum::extract::Extension;
use axum::http::Request;
use axum::response::Response;
use axum::{BoxError, Router};
use http::StatusCode;

use interface::DynStorageNode;

use menmos_auth::EncryptionKey;

use opentelemetry::global;
use opentelemetry::sdk::propagation::TraceContextPropagator;
use opentelemetry_http::HeaderExtractor;

use tower::ServiceBuilder;
use tower_http::request_id::{PropagateRequestIdLayer, RequestId, SetRequestIdLayer};
use tower_http::trace::TraceLayer;

use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::Config;

pub fn wrap_trace_layer(router: Router) -> Router {
    global::set_text_map_propagator(TraceContextPropagator::new());

    let svc = ServiceBuilder::new().layer(SetRequestIdLayer::x_request_id(apikit::middleware::MakeRequestUuid))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|r: &Request<_>| {
                    // Extract the opentelemetry trace context from the incoming request.
                    let parent_cx = global::get_text_map_propagator(|propagator| {
                        propagator.extract(&HeaderExtractor(r.headers()))
                    });

                    let request_id = r
                        .extensions()
                        .get::<RequestId>().map(|id| format!("{:?}", id)).unwrap_or_else(|| String::from("unknown"));

                    let s = tracing::info_span!(
                    "request",
                    method = %r.method(),
                    uri = %r.uri().path(),
                    "otel.kind" = "Server",
                    request.id = %request_id
                );

                    s.set_parent(parent_cx);
                    s
                })
                .on_request(|_r: &Request<_>, _s: &tracing::Span| {}) // We silence the on-request hook
                .on_response(
                    |response: &Response, latency: Duration, _span: &tracing::Span| {
                        tracing::info!(status = ?response.status(), elapsed = ?latency, "otel.status_code"=?response.status(), "complete");
                    },
                ),
        )
        .layer(axum::middleware::from_fn(apikit::middleware::propagate_tracing_context))
        .layer(PropagateRequestIdLayer::x_request_id());

    router.layer(svc)
}

async fn handle_loadshed_error(err: BoxError) -> (StatusCode, String) {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        format!("Too many requests: {}", err),
    )
}

pub fn wrap(router: Router, node: DynStorageNode, config: &Config) -> Router {
    // FIXME(pr): Set correct concurrency limits
    let router = router
        .layer(
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(handle_loadshed_error))
                .layer(
                    ServiceBuilder::new()
                        .load_shed()
                        .buffer(128) // TODO: Revise scaling this along with the concurrent call limit if appropriate.
                        .rate_limit(
                            config.server.max_concurrent_calls as u64,
                            Duration::from_secs(1),
                        )
                        .into_inner(),
                ),
        )
        .layer(Extension(node))
        .layer(Extension(EncryptionKey {
            key: config.node.encryption_key.clone(),
        }));
    wrap_trace_layer(router)
}
