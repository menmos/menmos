use anyhow::Result;

use opentelemetry::sdk::{trace as sdktrace, Resource};
use opentelemetry::trace::TraceError;
use opentelemetry::KeyValue;

use crate::logging::TracingConfig;

pub fn init_tracer(
    name: &str,
    cfg: &TracingConfig,
) -> Result<Option<sdktrace::Tracer>, TraceError> {
    match cfg {
        TracingConfig::None => {
            // We do nothing.
            Ok(None)
        }
        TracingConfig::Jaeger { host } => {
            let mut builder = opentelemetry_jaeger::new_pipeline().with_service_name(name);

            if let Some(host) = host {
                builder = builder.with_agent_endpoint(host);
            }
            builder
                .install_batch(opentelemetry::runtime::Tokio)
                .map(Some)
        }
        TracingConfig::OTLP {} => opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(opentelemetry_otlp::new_exporter().tonic())
            .with_trace_config(sdktrace::config().with_resource(Resource::new(vec![
                KeyValue::new("service.name", String::from(name)),
            ])))
            .install_batch(opentelemetry::runtime::Tokio)
            .map(Some),
    }
}
