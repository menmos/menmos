use anyhow::Result;

use opentelemetry::sdk::{trace as sdktrace, Resource};
use opentelemetry::trace::TraceError;
use opentelemetry::KeyValue;

use crate::logging::TracingConfig;

pub fn init_tracer(name: &str, cfg: &TracingConfig) -> Result<sdktrace::Tracer, TraceError> {
    match cfg {
        TracingConfig::None => {
            opentelemetry_jaeger::new_pipeline() // We suppose jaeger in development mode (localhost)
                .with_service_name(name)
                .install_batch(opentelemetry::runtime::Tokio)
        }
        TracingConfig::OTLP {} => opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(opentelemetry_otlp::new_exporter().tonic())
            .with_trace_config(sdktrace::config().with_resource(Resource::new(vec![
                KeyValue::new("service.name", String::from(name)),
            ])))
            .install_batch(opentelemetry::runtime::Tokio),
    }
}
