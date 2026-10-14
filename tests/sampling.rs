//! The `th` checks follow the rules for downstream samplers in OTEP 235:
//! <https://github.com/open-telemetry/opentelemetry-specification/blob/main/oteps/trace/0235-sampling-threshold-in-trace-state.md#downstream-samplers>
//!
//! - A processor that keeps a span with the probability 1 does not change or
//!   set `th`. So sampled spans keep the `th` of the head sampler.
//! - A span that only the processor selects is not a random sample. Its
//!   adjusted count is not defined, so it has no `th`.

mod common;

use common::{
    HIGH_RANDOMNESS, Harness, LOW_RANDOMNESS, remote_parent, root_with_children, sampler,
};
use opentelemetry::{
    Context,
    trace::{Span, Status, TraceContextExt, Tracer, TracerProvider},
};
use opentelemetry_rare_error_sampling::AlwaysRecordSampler;
use opentelemetry_sdk::trace::Sampler;

#[test]
fn unsampled_trace_exports_only_error_spans() {
    let harness = Harness::new(sampler(0.25), LOW_RANDOMNESS);

    root_with_children(&harness, &Context::new());

    // Head sampling drops the trace, so only the error span is exported.
    assert_eq!(harness.names(), ["failed"]);

    // The error span gets the sampled flag and no `th` value.
    let failed = harness.span("failed");
    assert!(failed.span_context.is_sampled());
    assert_eq!(failed.span_context.trace_id(), LOW_RANDOMNESS);
    assert_eq!(failed.span_context.trace_state().header(), "");

    // The parent of the exported span is not exported.
    assert_ne!(failed.parent_span_id, opentelemetry::trace::SpanId::INVALID);
}

#[test]
fn sampled_trace_exports_all_spans() {
    let harness = Harness::new(sampler(0.25), HIGH_RANDOMNESS);

    root_with_children(&harness, &Context::new());

    // Head sampling keeps the trace, so all spans are exported.
    assert_eq!(harness.names(), ["failed", "ok", "root"]);

    // All spans keep the threshold of the head sampler, also the error span.
    for name in ["ok", "root"] {
        let span = harness.span(name);
        assert!(span.span_context.is_sampled());
        assert_eq!(span.span_context.trace_state().header(), "ot=th:c");
    }
    let failed = harness.span("failed");
    assert_eq!(failed.span_context.trace_state().header(), "ot=th:c");
}

#[test]
fn children_inherit_the_root_decision() {
    let harness = Harness::new(sampler(0.25), LOW_RANDOMNESS);
    let tracer = harness.provider.tracer("test");

    // The parent is sampled, but its randomness 0 is below the threshold of
    // 0.25. Only the parent decision samples the children.
    let parent = remote_parent(LOW_RANDOMNESS, true, "ot=th:c;rv:00000000000000");
    let root_cx = parent.with_span(tracer.start_with_context("root", &parent));
    let child_cx = root_cx.with_span(tracer.start_with_context("child", &root_cx));

    tracer.start_with_context("grandchild", &child_cx).end();
    child_cx.span().end();
    root_cx.span().end();

    // The children are sampled, because their parent is sampled.
    assert_eq!(harness.names(), ["child", "grandchild", "root"]);

    // The exported trace is complete.
    let root_id = harness.span("root").span_context.span_id();
    assert_eq!(harness.span("child").parent_span_id, root_id);
}

#[test]
fn root_error_in_unsampled_trace_is_exported() {
    let harness = Harness::new(sampler(0.0), HIGH_RANDOMNESS);
    let tracer = harness.provider.tracer("test");

    let mut root = tracer.start("root");
    root.set_status(Status::error("boom"));
    root.end();
    tracer.start("other-root").end();

    // Only the root span with the error is exported.
    assert_eq!(harness.names(), ["root"]);

    // It gets the sampled flag and no `th` value.
    let root = harness.span("root");
    assert!(root.span_context.is_sampled());
    assert_eq!(root.span_context.trace_state().header(), "");
}

#[test]
fn ratio_one_exports_all_spans_with_threshold_zero() {
    let harness = Harness::new(sampler(1.0), LOW_RANDOMNESS);

    root_with_children(&harness, &Context::new());

    // A ratio of 1 samples all traces.
    assert_eq!(harness.names(), ["failed", "ok", "root"]);

    // All spans have the threshold 0.
    for span in harness.spans() {
        assert_eq!(span.span_context.trace_state().header(), "ot=th:0");
    }
}

#[test]
fn ratio_zero_exports_only_error_spans() {
    let harness = Harness::new(sampler(0.0), HIGH_RANDOMNESS);

    root_with_children(&harness, &Context::new());

    // A ratio of 0 samples no trace, so only the error span is exported.
    assert_eq!(harness.names(), ["failed"]);
}

#[test]
fn remote_unsampled_parent_records_children() {
    let harness = Harness::new(sampler(1.0), HIGH_RANDOMNESS);
    let parent = remote_parent(HIGH_RANDOMNESS, false, "vendor=x");

    root_with_children(&harness, &parent);

    // The parent is not sampled, so only the error span is exported.
    assert_eq!(harness.names(), ["failed"]);

    // The error span keeps the entries of other vendors.
    let failed = harness.span("failed");
    assert!(failed.span_context.is_sampled());
    assert_eq!(failed.span_context.trace_state().header(), "vendor=x");
}

#[test]
fn remote_sampled_parent_keeps_its_trace_state() {
    // The ratio does not apply. The decision of the parent applies.
    let harness = Harness::new(sampler(0.0), LOW_RANDOMNESS);
    let parent = remote_parent(LOW_RANDOMNESS, true, "ot=th:8;rv:ffffffffffffff,vendor=x");

    root_with_children(&harness, &parent);

    // The parent is sampled, so all spans are exported.
    assert_eq!(harness.names(), ["failed", "ok", "root"]);

    // All spans keep the tracestate of the parent, also the error span.
    assert_eq!(
        harness.span("ok").span_context.trace_state().header(),
        "ot=th:8;rv:ffffffffffffff,vendor=x"
    );
    assert_eq!(
        harness.span("failed").span_context.trace_state().header(),
        "ot=th:8;rv:ffffffffffffff,vendor=x"
    );
}

#[test]
fn invalid_parent_uses_the_root_sampler() {
    let harness = Harness::new(sampler(0.25), HIGH_RANDOMNESS);
    let parent =
        Context::new().with_remote_span_context(opentelemetry::trace::SpanContext::empty_context());

    root_with_children(&harness, &parent);

    // An invalid parent is the same as no parent, so the root sampler samples
    // the trace.
    assert_eq!(harness.names(), ["failed", "ok", "root"]);

    // The root span gets the threshold of the root sampler.
    assert_eq!(
        harness.span("root").span_context.trace_state().header(),
        "ot=th:c"
    );
}

#[test]
fn works_with_sdk_root_samplers() {
    let harness = Harness::new(
        AlwaysRecordSampler::new(Sampler::parent_based(Sampler::AlwaysOff)),
        HIGH_RANDOMNESS,
    );

    root_with_children(&harness, &Context::new());

    // `AlwaysOff` drops the trace, so only the error span is exported.
    assert_eq!(harness.names(), ["failed"]);

    let harness = Harness::new(
        AlwaysRecordSampler::new(Sampler::parent_based(Sampler::AlwaysOn)),
        HIGH_RANDOMNESS,
    );

    root_with_children(&harness, &Context::new());

    // `AlwaysOn` keeps the trace, so all spans are exported.
    assert_eq!(harness.names(), ["failed", "ok", "root"]);

    // The SDK samplers do not set a threshold.
    assert_eq!(harness.span("ok").span_context.trace_state().header(), "");
    assert_eq!(
        harness.span("failed").span_context.trace_state().header(),
        ""
    );
}
