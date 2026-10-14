//! Exports spans through a `BatchSpanProcessor` in an `ErrorSamplingProcessor`.
//!
//! Run the example with `cargo run --example batch`. The output shows one line
//! for each exported span:
//!
//! - The spans of head-sampled traces have `ot=th:c`.
//! - The error spans of the other traces have no `th` value.

use std::time::Duration;

use opentelemetry::{
    Context, KeyValue,
    trace::{Span, Status, TraceContextExt, Tracer, TracerProvider},
};
use opentelemetry_rare_error_sampling::{
    AlwaysRecordSampler, DEFAULT_STATUS_ATTRIBUTE, ErrorSamplingProcessor, ProbabilitySampler,
};
use opentelemetry_sdk::{
    error::OTelSdkResult,
    trace::{
        BatchConfigBuilder, BatchSpanProcessor, Sampler, SdkTracerProvider, SpanData, SpanExporter,
    },
};

fn main() {
    let batch = BatchSpanProcessor::builder(PrintExporter)
        .with_batch_config(
            BatchConfigBuilder::default()
                .with_scheduled_delay(Duration::from_millis(100))
                .build(),
        )
        .build();
    let provider = SdkTracerProvider::builder()
        // Sample 25% of the traces. Record the other spans. Then the processor
        // can export their errors.
        .with_sampler(AlwaysRecordSampler::new(Sampler::parent_based(
            ProbabilitySampler::new(0.25),
        )))
        .with_span_processor(ErrorSamplingProcessor::new(batch))
        .build();
    let tracer = provider.tracer("example");

    for request in 0..20 {
        let root = tracer.start("request");
        let cx = Context::current_with_span(root);

        let mut query = tracer.start_with_context("db.query", &cx);
        if request % 7 == 3 {
            query.set_status(Status::error("connection reset"));
        }
        query.end();

        let mut render = tracer.start_with_context("render", &cx);
        if request % 5 == 4 {
            // An instrumentation that cannot set the status can set this
            // attribute.
            render.set_attribute(KeyValue::new(DEFAULT_STATUS_ATTRIBUTE, "ERROR"));
        }
        render.end();

        cx.span().end();
    }

    // The shutdown exports the spans that the batch processor holds.
    provider.shutdown().expect("tracer provider shutdown");
}

/// Prints one line for each exported span.
#[derive(Debug)]
struct PrintExporter;

impl SpanExporter for PrintExporter {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        println!("exported batch of {} spans", batch.len());
        for span in batch {
            println!(
                "  trace={} span={:<9} status={:<5} tracestate={}",
                span.span_context.trace_id(),
                span.name,
                status(&span),
                span.span_context.trace_state().header(),
            );
        }
        Ok(())
    }
}

fn status(span: &SpanData) -> &'static str {
    let attribute = span
        .attributes
        .iter()
        .any(|kv| kv.key.as_str() == DEFAULT_STATUS_ATTRIBUTE && kv.value.as_str() == "ERROR");
    if attribute || matches!(span.status, Status::Error { .. }) {
        "error"
    } else {
        "ok"
    }
}
