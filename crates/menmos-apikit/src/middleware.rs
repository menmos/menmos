use axum::http::Request;
use axum::middleware::Next;
use axum::response::IntoResponse;

use opentelemetry::global;

use opentelemetry_http::HeaderInjector;

use tower_http::request_id::{MakeRequestId, RequestId};

use tracing_opentelemetry::OpenTelemetrySpanExt;

use uuid::Uuid;

#[derive(Clone, Copy)]
pub struct MakeRequestUuid;

impl MakeRequestId for MakeRequestUuid {
    fn make_request_id<B>(&mut self, _: &Request<B>) -> Option<RequestId> {
        let request_id = Uuid::new_v4().to_string().parse().unwrap();
        Some(RequestId::new(request_id))
    }
}

pub async fn propagate_tracing_context<B>(req: Request<B>, next: Next<B>) -> impl IntoResponse {
    let mut resp = next.run(req).await;
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(
            &tracing::Span::current().context(),
            &mut HeaderInjector(resp.headers_mut()),
        );
    });
    resp
}
