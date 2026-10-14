//! The `th` checks follow the rules for downstream samplers in OTEP 235:
//! <https://github.com/open-telemetry/opentelemetry-specification/blob/main/oteps/trace/0235-sampling-threshold-in-trace-state.md#downstream-samplers>
//!
//! - A processor that keeps a span with the probability 1 does not change or
//!   set `th`. So sampled spans keep the `th` of the head sampler.
//! - A span that only the processor selects is not a random sample. Its
//!   adjusted count is not defined, so it has no `th`.

mod common;

use std::sync::atomic::Ordering;

use common::{Harness, LOW_RANDOMNESS, RecordingProcessor, remote_parent};
use opentelemetry::{
    KeyValue,
    trace::{Span, Status, Tracer, TracerProvider},
};
use opentelemetry_rare_error_sampling::{
    AlwaysRecordSampler, ErrorSamplingProcessor, ProbabilitySampler,
};
use opentelemetry_sdk::{
    Resource,
    trace::{ParentBasedSampler, Sampler, SdkTracerProvider},
};

#[test]
fn status_code_attribute_marks_error() {
    let harness = Harness::new(record_only(), LOW_RANDOMNESS);

    end_with_attribute(
        &harness,
        "error",
        KeyValue::new("otel.status_code", "ERROR"),
    );
    end_with_attribute(&harness, "ok", KeyValue::new("otel.status_code", "OK"));
    end_with_attribute(
        &harness,
        "lowercase",
        KeyValue::new("otel.status_code", "error"),
    );
    end_with_attribute(
        &harness,
        "not-a-string",
        KeyValue::new("otel.status_code", 2),
    );
    end_with_attribute(&harness, "other-key", KeyValue::new("status", "ERROR"));

    // Only the exact string "ERROR" in `otel.status_code` marks an error.
    assert_eq!(harness.names(), ["error"]);

    // The error span gets the sampled flag and no `th` value.
    let span = harness.span("error");
    assert!(span.span_context.is_sampled());
    assert_eq!(span.span_context.trace_state().header(), "");
}

#[test]
fn custom_status_attribute() {
    let harness = Harness::with_processor(record_only(), LOW_RANDOMNESS, |inner| {
        ErrorSamplingProcessor::new(inner).with_status_attribute("app.status")
    });

    end_with_attribute(&harness, "custom", KeyValue::new("app.status", "ERROR"));
    end_with_attribute(
        &harness,
        "default",
        KeyValue::new("otel.status_code", "ERROR"),
    );

    // Only the custom attribute marks an error. The default attribute does not.
    assert_eq!(harness.names(), ["custom"]);
}

#[test]
fn without_status_attribute_uses_only_the_status() {
    let harness = Harness::with_processor(record_only(), LOW_RANDOMNESS, |inner| {
        ErrorSamplingProcessor::new(inner).without_status_attribute()
    });

    end_with_attribute(
        &harness,
        "attribute",
        KeyValue::new("otel.status_code", "ERROR"),
    );
    let tracer = harness.provider.tracer("test");

    let mut span = tracer.start("status");
    span.set_status(Status::error("boom"));
    span.end();

    // Only the span status marks an error. The attribute does not.
    assert_eq!(harness.names(), ["status"]);
}

#[test]
fn ok_status_after_error_is_not_an_error() {
    let harness = Harness::new(record_only(), LOW_RANDOMNESS);
    let tracer = harness.provider.tracer("test");

    let mut span = tracer.start("recovered");
    span.set_status(Status::error("boom"));
    span.set_status(Status::Ok);
    span.end();

    // The last status is OK, so the span is not an error and is dropped.
    assert!(harness.spans().is_empty());
}

#[test]
fn unsampled_error_span_loses_only_its_threshold() {
    let harness = Harness::new(record_only(), LOW_RANDOMNESS);
    let tracer = harness.provider.tracer("test");
    let parent = remote_parent(LOW_RANDOMNESS, false, "vendor=x,ot=th:8;rv:00000000000001");

    let mut span = tracer.start_with_context("failed", &parent);
    span.set_status(Status::error("boom"));
    span.end();

    // The processor removes `th`, and keeps `rv` and the entries of other
    // vendors.
    let span = harness.span("failed");
    assert!(span.span_context.is_sampled());
    assert_eq!(
        span.span_context.trace_state().header(),
        "ot=rv:00000000000001,vendor=x"
    );
}

#[test]
fn error_span_keeps_ids_and_remote_flag() {
    let harness = Harness::new(record_only(), LOW_RANDOMNESS);
    let tracer = harness.provider.tracer("test");
    let parent = remote_parent(LOW_RANDOMNESS, false, "");

    let mut span = tracer.start_with_context("failed", &parent);
    let span_id = span.span_context().span_id();
    span.set_status(Status::error("boom"));
    span.end();

    // The processor changes only the sampled flag. The ids, the remote flags
    // and the status do not change.
    let span = harness.span("failed");
    assert_eq!(span.span_context.trace_id(), LOW_RANDOMNESS);
    assert_eq!(span.span_context.span_id(), span_id);
    assert!(!span.span_context.is_remote());
    assert!(span.parent_span_is_remote);
    assert_eq!(span.status, Status::error("boom"));
}

#[test]
fn delegates_lifecycle_to_inner() {
    let inner = RecordingProcessor::default();
    let provider = SdkTracerProvider::builder()
        .with_sampler(AlwaysRecordSampler::new(Sampler::parent_based(
            ProbabilitySampler::new(0.0),
        )))
        .with_resource(Resource::builder_empty().with_service_name("svc").build())
        .with_span_processor(ErrorSamplingProcessor::new(inner.clone()))
        .build();
    let tracer = provider.tracer("test");

    tracer.start("dropped").end();
    let mut failed = tracer.start("failed");
    failed.set_status(Status::error("boom"));
    failed.end();

    // The SDK calls on_start for all recorded spans, also for the dropped span.
    assert_eq!(inner.started.load(Ordering::Relaxed), 2);

    // Only the error span reaches the inner processor.
    let ended = inner.ended.lock().unwrap();
    assert_eq!(ended.len(), 1);
    assert_eq!(ended[0].name, "failed");
    drop(ended);

    // The flush and the shutdown reach the inner processor.
    provider.force_flush().unwrap();
    assert_eq!(inner.flushed.load(Ordering::Relaxed), 1);
    provider.shutdown().unwrap();
    assert_eq!(inner.shut_down.load(Ordering::Relaxed), 1);

    // The resource of the provider reaches the inner processor.
    let resources = inner.resources.lock().unwrap();
    assert!(resources.iter().any(|resource| {
        resource
            .get(&opentelemetry::Key::from_static_str("service.name"))
            .is_some_and(|value| value.as_str() == "svc")
    }));
}

#[test]
fn inner_returns_the_wrapped_processor() {
    let inner = RecordingProcessor::default();
    let processor = ErrorSamplingProcessor::new(inner.clone());

    // `inner` returns the processor that the wrapper got.
    assert!(std::sync::Arc::ptr_eq(
        &processor.inner().started,
        &inner.started
    ));
}

fn record_only() -> AlwaysRecordSampler<ParentBasedSampler> {
    AlwaysRecordSampler::new(Sampler::parent_based(Sampler::AlwaysOff))
}

fn end_with_attribute(harness: &Harness, name: &'static str, attribute: KeyValue) {
    let tracer = harness.provider.tracer("test");
    let mut span = tracer.start(name);
    span.set_attribute(attribute);
    span.end();
}
