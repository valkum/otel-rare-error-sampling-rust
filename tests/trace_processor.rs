//! The `th` checks follow the rules for downstream samplers in OTEP 235:
//! <https://github.com/open-telemetry/opentelemetry-specification/blob/main/oteps/trace/0235-sampling-threshold-in-trace-state.md#downstream-samplers>
//!
//! - A processor that keeps a span with the probability 1 does not change or
//!   set `th`. So sampled spans keep the `th` of the head sampler.
//! - A span that only the processor selects is not a random sample. Its
//!   adjusted count is not defined, so it has no `th`.

mod common;

use std::{
    sync::{Arc, atomic::Ordering},
    thread,
    time::Duration,
};

use common::{
    CollectingExporter, HIGH_RANDOMNESS, Harness, LOW_RANDOMNESS, RecordingProcessor, SequenceIds,
    batch_processor, one_in_four_sampled, remote_parent, root_with_children, sampler,
};
use opentelemetry::{
    Context, KeyValue,
    trace::{Span, SpanId, Status, TraceContextExt, TraceId, Tracer, TracerProvider},
};
use opentelemetry_rare_error_sampling::TraceErrorSamplingProcessor;
use opentelemetry_sdk::{
    error::OTelSdkResult,
    trace::{
        InMemorySpanExporter, SdkTracerProvider, SimpleSpanProcessor, SpanData, SpanProcessor,
    },
};

#[test]
fn error_exports_the_whole_local_trace() {
    let harness = harness(0.0, LOW_RANDOMNESS);

    root_with_children(&harness, &Context::new());

    // Head sampling drops the trace, but the error exports all its spans.
    assert_eq!(harness.names(), ["failed", "ok", "root"]);

    // The exported trace is complete, and all spans get the sampled flag.
    let root = harness.span("root");
    assert_eq!(
        harness.span("ok").parent_span_id,
        root.span_context.span_id()
    );

    for span in harness.spans() {
        assert!(span.span_context.is_sampled(), "{}", span.name);
    }

    // The processor selected all spans for a reason that is not random. So
    // no span has a threshold.
    assert_eq!(
        harness.span("failed").span_context.trace_state().header(),
        ""
    );
    assert_eq!(harness.span("ok").span_context.trace_state().header(), "");
    assert_eq!(root.span_context.trace_state().header(), "");
}

#[test]
fn trace_without_error_is_dropped() {
    let harness = harness(0.0, LOW_RANDOMNESS);
    let tracer = harness.provider.tracer("test");
    let cx = Context::current_with_span(tracer.start("root"));

    tracer.start_with_context("ok", &cx).end();
    cx.span().end();

    // Without an error, the processor drops the held spans.
    assert!(harness.spans().is_empty());
}

#[test]
fn spans_before_and_after_the_error_are_exported() {
    let harness = harness(0.0, LOW_RANDOMNESS);
    let tracer = harness.provider.tracer("test");
    let cx = Context::current_with_span(tracer.start("root"));

    tracer.start_with_context("before", &cx).end();
    let mut failed = tracer.start_with_context("failed", &cx);
    fail(&mut failed);
    failed.end();
    tracer.start_with_context("after", &cx).end();
    cx.span().end();

    // The held span before the error and the span after it are both exported.
    assert_eq!(harness.names(), ["after", "before", "failed", "root"]);
}

#[test]
fn error_after_the_root_ended_exports_the_root() {
    let harness = harness(0.0, LOW_RANDOMNESS);
    let tracer = harness.provider.tracer("test");
    let cx = Context::current_with_span(tracer.start("root"));

    let mut background = tracer.start_with_context("background", &cx);
    cx.span().end();
    fail(&mut background);
    background.end();

    // The root span ended first, but the processor still held it.
    assert_eq!(harness.names(), ["background", "root"]);
}

#[test]
fn status_attribute_marks_the_trace() {
    let harness = harness(0.0, LOW_RANDOMNESS);
    let tracer = harness.provider.tracer("test");
    let cx = Context::current_with_span(tracer.start("root"));

    let mut failed = tracer.start_with_context("failed", &cx);
    failed.set_attribute(KeyValue::new("otel.status_code", "ERROR"));
    failed.end();
    cx.span().end();

    // The status attribute marks the error, so the whole trace is exported.
    assert_eq!(harness.names(), ["failed", "root"]);
}

#[test]
fn head_sampled_trace_is_exported_unchanged() {
    let harness = harness(0.25, HIGH_RANDOMNESS);
    let tracer = harness.provider.tracer("test");
    let cx = Context::current_with_span(tracer.start("root"));

    tracer.start_with_context("ok", &cx).end();
    cx.span().end();
    root_with_children(&harness, &Context::new());

    // Head sampling keeps both traces, so all spans are exported.
    assert_eq!(harness.names(), ["failed", "ok", "ok", "root", "root"]);

    // The processor does not change the threshold of a sampled span.
    for span in harness.spans() {
        let expected = "ot=th:c";
        assert_eq!(
            span.span_context.trace_state().header(),
            expected,
            "{}",
            span.name
        );
    }
}

#[test]
fn removes_the_threshold_and_keeps_other_entries() {
    let harness = harness(1.0, LOW_RANDOMNESS);
    let parent = remote_parent(LOW_RANDOMNESS, false, "ot=th:8;rv:ffffffffffffff,vendor=x");

    root_with_children(&harness, &parent);

    // The processor removes `th`, and keeps `rv` and the entries of other
    // vendors.
    assert_eq!(
        harness.span("ok").span_context.trace_state().header(),
        "ot=rv:ffffffffffffff,vendor=x"
    );
    assert_eq!(
        harness.span("failed").span_context.trace_state().header(),
        "ot=rv:ffffffffffffff,vendor=x"
    );
}

#[test]
fn later_spans_of_a_kept_trace_are_exported() {
    let harness = harness(0.0, LOW_RANDOMNESS);

    root_with_children(&harness, &Context::new());

    // A second request of the same trace arrives after the first one ended.
    later_span(&harness.provider.tracer("test"), LOW_RANDOMNESS, "later");

    // The processor remembers the error of the trace, so the later span is
    // exported.
    assert_eq!(harness.names(), ["failed", "later", "ok", "root"]);
}

#[test]
fn later_spans_of_a_dropped_trace_are_dropped() {
    let harness = harness(0.0, LOW_RANDOMNESS);

    let tracer = harness.provider.tracer("test");

    tracer.start("first").end();
    later_span(&tracer, LOW_RANDOMNESS, "later");

    // The first part of the trace had no error, so the later span is dropped
    // too.
    assert!(harness.spans().is_empty());
}

#[test]
fn drops_held_spans_above_the_limit() {
    let harness = Harness::with_processor(sampler(0.0), LOW_RANDOMNESS, |inner| {
        TraceErrorSamplingProcessor::new(inner).with_max_spans_per_trace(1)
    });
    let tracer = harness.provider.tracer("test");
    let cx = Context::current_with_span(tracer.start("root"));

    tracer.start_with_context("first", &cx).end();
    tracer.start_with_context("second", &cx).end();
    let mut failed = tracer.start_with_context("failed", &cx);
    fail(&mut failed);
    failed.end();
    cx.span().end();

    // The processor held only one span. It dropped "second".
    assert_eq!(harness.names(), ["failed", "first", "root"]);
}

#[test]
fn very_long_timeout_does_not_overflow() {
    let harness = Harness::with_processor(sampler(0.0), LOW_RANDOMNESS, |inner| {
        TraceErrorSamplingProcessor::new(inner).with_trace_timeout(Duration::MAX)
    });

    root_with_children(&harness, &Context::new());

    // A very long timeout does not cause a panic, and the trace is exported.
    assert_eq!(harness.names(), ["failed", "ok", "root"]);
}

#[test]
fn shutdown_drops_held_spans() {
    let inner = RecordingProcessor::default();
    let processor = Arc::new(TraceErrorSamplingProcessor::new(inner.clone()));
    let provider = SdkTracerProvider::builder()
        .with_sampler(sampler(0.0))
        .with_span_processor(Shared(processor.clone()))
        .build();
    let tracer = provider.tracer("test");
    let cx = Context::current_with_span(tracer.start("root"));

    tracer.start_with_context("held", &cx).end();

    // Shut down only the processor. The provider still sends spans to it.
    processor.shutdown().unwrap();
    let mut failed = tracer.start_with_context("failed", &cx);
    fail(&mut failed);
    failed.end();

    // The shutdown dropped the held span, so only the error span reaches the
    // inner processor.
    let ended: Vec<String> = inner
        .ended
        .lock()
        .unwrap()
        .iter()
        .map(|span| span.name.to_string())
        .collect();
    assert_eq!(ended, ["failed"]);
    assert_eq!(inner.shut_down.load(Ordering::Relaxed), 1);
}

#[test]
fn delegates_flush_and_shutdown_to_inner() {
    let inner = RecordingProcessor::default();
    let provider = SdkTracerProvider::builder()
        .with_sampler(sampler(0.0))
        .with_span_processor(TraceErrorSamplingProcessor::new(inner.clone()))
        .build();

    provider.tracer("test").start("root").end();

    provider.force_flush().unwrap();
    provider.shutdown().unwrap();

    // The start, the flush and the shutdown reach the inner processor.
    assert_eq!(inner.started.load(Ordering::Relaxed), 1);
    assert_eq!(inner.flushed.load(Ordering::Relaxed), 1);
    assert_eq!(inner.shut_down.load(Ordering::Relaxed), 1);
}

#[test]
fn concurrent_traces_through_batch_processor() {
    const THREADS: usize = 8;
    const TRACES_PER_THREAD: usize = 200;
    let traces = THREADS * TRACES_PER_THREAD;

    // Each 4th trace is head-sampled. Each 5th trace has an error.
    let trace_ids = one_in_four_sampled(traces);
    let exporter = CollectingExporter::default();
    let batch = batch_processor(&exporter);
    let provider = SdkTracerProvider::builder()
        .with_sampler(sampler(0.25))
        .with_id_generator(SequenceIds::new(trace_ids.clone()))
        .with_span_processor(TraceErrorSamplingProcessor::new(batch))
        .build();

    let has_error =
        |trace_id: TraceId| (u128::from_be_bytes(trace_id.to_bytes()) >> 64).is_multiple_of(5);
    let is_sampled =
        |trace_id: TraceId| u128::from_be_bytes(trace_id.to_bytes()) & 0xff_ffff_ffff_ffff != 1;

    thread::scope(|scope| {
        for _ in 0..THREADS {
            scope.spawn(|| {
                let tracer = provider.tracer("test");
                for _ in 0..TRACES_PER_THREAD {
                    let cx = Context::current_with_span(tracer.start("root"));
                    let trace_id = cx.span().span_context().trace_id();
                    let mut child = tracer.start_with_context("child", &cx);
                    if has_error(trace_id) {
                        fail(&mut child);
                    }
                    tracer.start_with_context("ok", &cx).end();
                    child.end();
                    cx.span().end();
                }
            });
        }
    });
    provider.force_flush().unwrap();

    // All exported spans are sampled. Group them by trace.
    let spans = exporter.spans();
    let mut per_trace = std::collections::HashMap::<TraceId, Vec<SpanData>>::new();
    for span in spans {
        assert!(span.span_context.is_sampled());
        per_trace
            .entry(span.span_context.trace_id())
            .or_default()
            .push(span);
    }

    // Exactly the head-sampled traces and the traces with an error are
    // exported.
    let expected: Vec<TraceId> = trace_ids
        .iter()
        .copied()
        .filter(|&id| is_sampled(id) || has_error(id))
        .collect();
    assert_eq!(per_trace.len(), expected.len());

    // Each exported trace is complete. The error span keeps `th` only in
    // head-sampled traces.
    for trace_id in expected {
        let spans = &per_trace[&trace_id];
        assert_eq!(spans.len(), 3, "trace {trace_id}");
        let error_state = spans
            .iter()
            .find(|s| s.name == "child")
            .map(|s| s.span_context.trace_state().header())
            .unwrap();
        if is_sampled(trace_id) {
            assert_eq!(error_state, "ot=th:c");
        } else {
            assert_eq!(error_state, "");
        }
    }
}

#[test]
fn span_id_of_parent_is_kept() {
    let harness = harness(0.0, LOW_RANDOMNESS);
    let parent = remote_parent(LOW_RANDOMNESS, false, "");
    let tracer = harness.provider.tracer("test");

    let mut span = tracer.start_with_context("failed", &parent);
    fail(&mut span);
    span.end();

    // The exported span keeps the id of its remote parent.
    assert_eq!(
        harness.span("failed").parent_span_id,
        SpanId::from(0xabcd_u64)
    );
}

#[test]
#[ignore = "stress test, run with `cargo test -- --ignored`"]
fn exports_complete_traces_under_pressure() {
    const THREADS: usize = 16;
    const TRACES_PER_THREAD: usize = 5_000;
    let traces = THREADS * TRACES_PER_THREAD;
    let trace_ids = one_in_four_sampled(traces);
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_sampler(sampler(0.25))
        .with_id_generator(SequenceIds::new(trace_ids.clone()))
        .with_span_processor(TraceErrorSamplingProcessor::new(SimpleSpanProcessor::new(
            exporter.clone(),
        )))
        .build();

    run_pressure(&provider, THREADS, TRACES_PER_THREAD);

    // All spans of the head-sampled traces and of the traces with an error
    // are exported. No other span is exported.
    let spans = exporter.get_finished_spans().unwrap();
    let per_trace = group_by_trace(&spans);
    let expected: Vec<TraceId> = trace_ids
        .iter()
        .copied()
        .filter(|&id| is_head_sampled(id) || pressure_error(id).is_some())
        .collect();
    assert_eq!(per_trace.len(), expected.len());

    // Each exported trace is complete. Head-sampled spans keep their `th`.
    // Spans that the processor selected have no `th`.
    for trace_id in expected {
        let spans = &per_trace[&trace_id];
        assert_eq!(spans.len(), PRESSURE_SPANS, "trace {trace_id}");
        let trace_state = if is_head_sampled(trace_id) {
            "ot=th:c"
        } else {
            ""
        };
        for span in spans {
            assert!(span.span_context.is_sampled());
            assert_eq!(span.span_context.trace_state().header(), trace_state);
        }
    }
}

#[test]
#[ignore = "stress test, run with `cargo test -- --ignored`"]
fn keeps_all_errors_when_traces_are_removed_under_pressure() {
    const THREADS: usize = 16;
    const TRACES_PER_THREAD: usize = 5_000;
    let traces = THREADS * TRACES_PER_THREAD;
    let trace_ids = one_in_four_sampled(traces);
    let exporter = InMemorySpanExporter::default();
    // With room for one trace in each shard, the processor removes traces
    // all the time.
    let provider = SdkTracerProvider::builder()
        .with_sampler(sampler(0.25))
        .with_id_generator(SequenceIds::new(trace_ids.clone()))
        .with_span_processor(
            TraceErrorSamplingProcessor::new(SimpleSpanProcessor::new(exporter.clone()))
                .with_max_traces(1),
        )
        .build();

    run_pressure(&provider, THREADS, TRACES_PER_THREAD);

    // All error spans are exported, also when the processor removed their
    // trace.
    let spans = exporter.get_finished_spans().unwrap();
    let errors = trace_ids
        .iter()
        .filter(|&&id| pressure_error(id).is_some())
        .count();
    let exported_errors = spans
        .iter()
        .filter(|span| matches!(span.status, Status::Error { .. }))
        .count();
    assert_eq!(exported_errors, errors);

    // The processor exports each span at most one time, and only from
    // head-sampled traces and traces with an error.
    let mut span_ids = std::collections::HashSet::new();
    for span in &spans {
        assert!(span.span_context.is_sampled());
        assert!(span_ids.insert(span.span_context.span_id()), "{span:?}");
        let trace_id = span.span_context.trace_id();
        assert!(is_head_sampled(trace_id) || pressure_error(trace_id).is_some());
    }
}

fn harness(ratio: f64, trace_id: TraceId) -> Harness {
    Harness::with_processor(sampler(ratio), trace_id, TraceErrorSamplingProcessor::new)
}

fn fail(span: &mut impl Span) {
    span.set_status(Status::error("boom"));
}

/// Ends a span of `trace_id` without an error, after its local trace ended.
fn later_span(tracer: &opentelemetry_sdk::trace::SdkTracer, trace_id: TraceId, name: &str) {
    let parent = remote_parent(trace_id, false, "");
    tracer.start_with_context(name.to_owned(), &parent).end();
}

/// The number of spans of a trace in the pressure tests.
const PRESSURE_SPANS: usize = 6;

/// Makes traces on `threads` threads with the same provider. Each trace has a
/// root span, 4 child spans, and a late span that ends after the root span.
/// Some traces have an error, see [`pressure_error`].
fn run_pressure(provider: &SdkTracerProvider, threads: usize, traces_per_thread: usize) {
    thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| {
                let tracer = provider.tracer("test");
                for _ in 0..traces_per_thread {
                    let cx = Context::current_with_span(tracer.start("root"));
                    let trace_id = cx.span().span_context().trace_id();
                    let error = pressure_error(trace_id);
                    let mut late = tracer.start_with_context("late", &cx);
                    for child in 0..4 {
                        let mut span = tracer.start_with_context("child", &cx);
                        if error == Some(child) {
                            fail(&mut span);
                        }
                        span.end();
                    }
                    cx.span().end();
                    if error == Some(4) {
                        fail(&mut late);
                    }
                    late.end();
                }
            });
        }
    });
    provider.force_flush().unwrap();
}

/// Returns the span with an error in a pressure trace: a child (0 to 3), the
/// late span (4), or `None`. Each 7th trace has an error in a child, and each
/// 11th trace has an error in the late span.
fn pressure_error(trace_id: TraceId) -> Option<usize> {
    let n = u128::from_be_bytes(trace_id.to_bytes()) >> 64;
    if n.is_multiple_of(11) {
        Some(4)
    } else if n.is_multiple_of(7) {
        Some((n % 4) as usize)
    } else {
        None
    }
}

/// Returns `true` for the trace ids of [`one_in_four_sampled`] that a ratio of
/// 0.25 samples.
fn is_head_sampled(trace_id: TraceId) -> bool {
    u128::from_be_bytes(trace_id.to_bytes()) & 0xff_ffff_ffff_ffff != 1
}

fn group_by_trace(spans: &[SpanData]) -> std::collections::HashMap<TraceId, Vec<&SpanData>> {
    let mut per_trace = std::collections::HashMap::<_, Vec<_>>::new();
    for span in spans {
        per_trace
            .entry(span.span_context.trace_id())
            .or_default()
            .push(span);
    }
    per_trace
}

/// Shares a processor with the tracer provider, so that a test can call the
/// processor directly.
#[derive(Debug)]
struct Shared<P>(Arc<P>);

impl<P: SpanProcessor> SpanProcessor for Shared<P> {
    fn on_start(&self, span: &mut opentelemetry_sdk::trace::Span, cx: &Context) {
        self.0.on_start(span, cx);
    }

    fn on_end(&self, span: SpanData) {
        self.0.on_end(span);
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.0.force_flush()
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.0.shutdown_with_timeout(timeout)
    }
}
