//! The `th` checks follow the rules for downstream samplers in OTEP 235:
//! <https://github.com/open-telemetry/opentelemetry-specification/blob/main/oteps/trace/0235-sampling-threshold-in-trace-state.md#downstream-samplers>
//!
//! - A processor that keeps a span with the probability 1 does not change or
//!   set `th`. So sampled spans keep the `th` of the head sampler.
//! - A span that only the processor selects is not a random sample. Its
//!   adjusted count is not defined, so it has no `th`.

mod common;

use std::{collections::HashSet, thread};

use common::{
    CollectingExporter, HIGH_RANDOMNESS, LOW_RANDOMNESS, SequenceIds, batch_processor,
    one_in_four_sampled,
};
use opentelemetry::{
    Context,
    trace::{Span, SpanContext, Status, TraceContextExt, TraceId, Tracer, TracerProvider},
};
use opentelemetry_rare_error_sampling::{
    AlwaysRecordSampler, ErrorSamplingProcessor, ProbabilitySampler,
};
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider, SpanData};

#[test]
fn exports_errors_and_head_sampled_traces() {
    let exporter = CollectingExporter::default();
    let provider = provider(&exporter, 0.25, vec![HIGH_RANDOMNESS, LOW_RANDOMNESS], true);

    trace(&provider);
    trace(&provider);
    provider.force_flush().unwrap();

    // The head-sampled trace is exported with its threshold. From the other
    // trace, only the error span is exported, without `th`.
    assert_eq!(
        exported(&exporter.spans()),
        [
            entry(HIGH_RANDOMNESS, "failed", "ot=th:c"),
            entry(HIGH_RANDOMNESS, "ok", "ot=th:c"),
            entry(HIGH_RANDOMNESS, "root", "ot=th:c"),
            entry(LOW_RANDOMNESS, "failed", ""),
        ]
    );
}

/// The `BatchSpanProcessor` exports all spans that it gets, also the spans
/// that are not sampled. Only the wrapper stops the record-only spans.
#[test]
fn batch_processor_alone_exports_record_only_spans() {
    let exporter = CollectingExporter::default();
    let provider = provider(&exporter, 0.0, vec![LOW_RANDOMNESS], false);

    trace(&provider);
    provider.force_flush().unwrap();

    // Without the wrapper, the batch processor exports all 3 recorded spans,
    // also the spans that are not sampled.
    let spans = exporter.spans();
    assert_eq!(spans.len(), 3);
    assert!(spans.iter().all(|span| !span.span_context.is_sampled()));
}

#[test]
fn shutdown_flushes_through_the_wrapper() {
    let exporter = CollectingExporter::default();
    let provider = provider(&exporter, 0.0, vec![LOW_RANDOMNESS], true);

    trace(&provider);
    provider.shutdown().unwrap();

    // The shutdown exports the held error span and shuts down the exporter.
    assert_eq!(
        exported(&exporter.spans()),
        [entry(LOW_RANDOMNESS, "failed", "")]
    );
    assert_eq!(
        exporter
            .shut_down
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}

#[test]
fn concurrent_traces_keep_all_errors() {
    const THREADS: usize = 8;
    const TRACES_PER_THREAD: usize = 200;

    let exporter = CollectingExporter::default();
    let trace_ids = one_in_four_sampled(THREADS * TRACES_PER_THREAD);
    let provider = provider(&exporter, 0.25, trace_ids, true);

    thread::scope(|scope| {
        for _ in 0..THREADS {
            scope.spawn(|| {
                for _ in 0..TRACES_PER_THREAD {
                    trace(&provider);
                }
            });
        }
    });
    provider.force_flush().unwrap();

    let spans = exporter.spans();
    let traces = THREADS * TRACES_PER_THREAD;

    // The exported root spans show which traces head sampling sampled.
    let sampled: HashSet<TraceId> = spans
        .iter()
        .filter(|s| s.name == "root")
        .map(|s| s.span_context.trace_id())
        .collect();

    // Each trace has one error span, and the processor exports all of them.
    // Error spans of head-sampled traces keep `th`. The others have no `th`.
    let failed: Vec<&SpanData> = spans.iter().filter(|s| s.name == "failed").collect();
    assert_eq!(failed.len(), traces, "every error span is exported");
    for span in failed {
        assert!(span.span_context.is_sampled());
        let expected = if sampled.contains(&span.span_context.trace_id()) {
            "ot=th:c"
        } else {
            ""
        };
        assert_eq!(span.span_context.trace_state().header(), expected);
    }

    // One trace in four is head-sampled. All its spans are exported with the
    // threshold.
    assert_eq!(sampled.len(), traces / 4);
    for span in spans.iter().filter(|s| s.name != "failed") {
        assert!(sampled.contains(&span.span_context.trace_id()));
        assert!(span.span_context.is_sampled());
        assert_eq!(span.span_context.trace_state().header(), "ot=th:c");
    }

    // No other span is exported: one error span for each trace, and two more
    // spans for each head-sampled trace.
    assert_eq!(spans.len(), traces + 2 * traces / 4);
}

#[test]
fn remote_sampled_parent_is_exported_through_batch() {
    let exporter = CollectingExporter::default();
    let provider = provider(&exporter, 0.0, vec![LOW_RANDOMNESS], true);
    let parent = Context::new().with_remote_span_context(SpanContext::new(
        HIGH_RANDOMNESS,
        opentelemetry::trace::SpanId::from(7_u64),
        opentelemetry::trace::TraceFlags::SAMPLED,
        true,
        "ot=th:8".parse().unwrap(),
    ));

    provider
        .tracer("test")
        .start_with_context("child", &parent)
        .end();
    provider.force_flush().unwrap();

    // The child of a sampled remote parent is exported with the tracestate of
    // the parent.
    assert_eq!(
        exported(&exporter.spans()),
        [entry(HIGH_RANDOMNESS, "child", "ot=th:8")]
    );
}

fn provider(
    exporter: &CollectingExporter,
    ratio: f64,
    trace_ids: Vec<TraceId>,
    wrap: bool,
) -> SdkTracerProvider {
    let batch = batch_processor(exporter);
    let builder = SdkTracerProvider::builder()
        .with_sampler(AlwaysRecordSampler::new(Sampler::parent_based(
            ProbabilitySampler::new(ratio),
        )))
        .with_id_generator(SequenceIds::new(trace_ids));
    if wrap {
        builder
            .with_span_processor(ErrorSamplingProcessor::new(batch))
            .build()
    } else {
        builder.with_span_processor(batch).build()
    }
}

/// Ends a trace with the spans `root`, `ok` and `failed`.
fn trace(provider: &SdkTracerProvider) {
    let tracer = provider.tracer("test");
    let cx = Context::current_with_span(tracer.start("root"));
    tracer.start_with_context("ok", &cx).end();
    let mut failed = tracer.start_with_context("failed", &cx);
    failed.set_status(Status::error("boom"));
    failed.end();
    cx.span().end();
}

fn exported(spans: &[SpanData]) -> Vec<(TraceId, String, String)> {
    let mut exported: Vec<_> = spans
        .iter()
        .map(|span| {
            assert!(
                span.span_context.is_sampled(),
                "{} is not sampled",
                span.name
            );
            (
                span.span_context.trace_id(),
                span.name.to_string(),
                span.span_context.trace_state().header(),
            )
        })
        .collect();
    exported.sort_by(|a, b| (a.0.to_bytes(), &a.1).cmp(&(b.0.to_bytes(), &b.1)));
    exported
}

fn entry(trace_id: TraceId, name: &str, trace_state: &str) -> (TraceId, String, String) {
    (trace_id, name.to_owned(), trace_state.to_owned())
}
