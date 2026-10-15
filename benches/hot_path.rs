use std::{
    borrow::Cow,
    hint::black_box,
    thread,
    time::{Duration, Instant, SystemTime},
};

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use opentelemetry::{
    Context, InstrumentationScope, KeyValue,
    trace::{
        Span as _, SpanContext, SpanId, SpanKind, Status, TraceContextExt, TraceFlags, TraceId,
        TraceState, Tracer as _, TracerProvider as _,
    },
};
use opentelemetry_rare_error_sampling::{
    AlwaysRecordSampler, ErrorSamplingProcessor, ProbabilitySampler, TraceErrorSamplingProcessor,
};
use opentelemetry_sdk::{
    error::OTelSdkResult,
    trace::{
        Sampler, SdkTracerProvider, ShouldSample, Span, SpanData, SpanEvents, SpanLinks,
        SpanProcessor,
    },
};

criterion_group!(
    benches,
    processor,
    sampler,
    end_to_end,
    end_to_end_with_open_traces,
    large_traces,
    contended
);
criterion_main!(benches);

/// A processor that drops all spans. Use it to measure only the cost of the
/// wrapper.
#[derive(Debug)]
struct Noop;

impl SpanProcessor for Noop {
    fn on_start(&self, _span: &mut Span, _cx: &Context) {}
    fn on_end(&self, span: SpanData) {
        black_box(span);
    }
    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }
    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        Ok(())
    }
}

fn processor(c: &mut Criterion) {
    let mut group = c.benchmark_group("processor_on_end");
    let wrapper = ErrorSamplingProcessor::new(Noop);
    let cases = [
        ("ok_unsampled", false, false, ""),
        ("ok_sampled", true, false, "ot=th:c"),
        ("error_unsampled_empty_ts", false, true, ""),
        (
            "error_unsampled_ts",
            false,
            true,
            "ot=th:c;rv:ffffffffffffff,vendor=x",
        ),
        ("error_sampled_th0", true, true, "ot=th:0"),
    ];
    for (name, sampled, error, trace_state) in cases {
        let span = span_data(sampled, error, trace_state);
        group.bench_function(name, |b| {
            b.iter_batched(
                || span.clone(),
                |span| wrapper.on_end(span),
                BatchSize::SmallInput,
            )
        });
    }
    let span = span_data(true, false, "ot=th:c");
    group.bench_function("baseline_inner_only", |b| {
        b.iter_batched(
            || span.clone(),
            |span| Noop.on_end(span),
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn sampler(c: &mut Criterion) {
    let mut group = c.benchmark_group("should_sample");
    let record_only =
        AlwaysRecordSampler::new(Sampler::parent_based(ProbabilitySampler::new(0.25)));
    let consistent = ProbabilitySampler::new(0.25);
    let sdk = Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(0.25)));
    let root = Context::new();
    let sampled_parent = parent(HIGH, true, "ot=th:c");
    let unsampled_parent = parent(LOW, false, "");
    let rv_parent = parent(LOW, true, "ot=rv:ffffffffffffff,vendor=x");

    let cases: [(&str, &dyn ShouldSample, &Context, TraceId); 7] = [
        ("record_only_root_sampled", &record_only, &root, HIGH),
        ("record_only_root_unsampled", &record_only, &root, LOW),
        (
            "record_only_child_sampled",
            &record_only,
            &sampled_parent,
            HIGH,
        ),
        (
            "record_only_child_unsampled",
            &record_only,
            &unsampled_parent,
            LOW,
        ),
        ("consistent_with_parent_rv", &consistent, &rv_parent, LOW),
        ("sdk_parent_based_root", &sdk, &root, HIGH),
        ("sdk_parent_based_child", &sdk, &sampled_parent, HIGH),
    ];
    for (name, sampler, cx, trace_id) in cases {
        group.bench_function(name, |b| {
            b.iter(|| {
                sampler.should_sample(
                    Some(black_box(cx)),
                    black_box(trace_id),
                    "span",
                    &SpanKind::Internal,
                    &[],
                    &[],
                )
            })
        });
    }
    group.finish();
}

fn end_to_end(c: &mut Criterion) {
    let mut group = c.benchmark_group("trace_of_two_spans");
    let providers = [
        (
            "sdk_parent_based_ratio_0.25",
            SdkTracerProvider::builder()
                .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
                    0.25,
                ))))
                .with_span_processor(Noop)
                .build(),
        ),
        (
            "record_only_ratio_0.25",
            SdkTracerProvider::builder()
                .with_sampler(AlwaysRecordSampler::new(Sampler::parent_based(
                    ProbabilitySampler::new(0.25),
                )))
                .with_span_processor(ErrorSamplingProcessor::new(Noop))
                .build(),
        ),
        (
            "record_only_ratio_0",
            SdkTracerProvider::builder()
                .with_sampler(AlwaysRecordSampler::new(Sampler::parent_based(
                    ProbabilitySampler::new(0.0),
                )))
                .with_span_processor(ErrorSamplingProcessor::new(Noop))
                .build(),
        ),
        (
            "record_only_ratio_1",
            SdkTracerProvider::builder()
                .with_sampler(AlwaysRecordSampler::new(Sampler::parent_based(
                    ProbabilitySampler::new(1.0),
                )))
                .with_span_processor(ErrorSamplingProcessor::new(Noop))
                .build(),
        ),
        (
            "trace_processor_ratio_0.25",
            SdkTracerProvider::builder()
                .with_sampler(AlwaysRecordSampler::new(Sampler::parent_based(
                    ProbabilitySampler::new(0.25),
                )))
                .with_span_processor(TraceErrorSamplingProcessor::new(Noop))
                .build(),
        ),
        (
            "trace_processor_ratio_0",
            SdkTracerProvider::builder()
                .with_sampler(AlwaysRecordSampler::new(Sampler::parent_based(
                    ProbabilitySampler::new(0.0),
                )))
                .with_span_processor(TraceErrorSamplingProcessor::new(Noop))
                .build(),
        ),
        (
            "always_on",
            SdkTracerProvider::builder()
                .with_sampler(Sampler::AlwaysOn)
                .with_span_processor(Noop)
                .build(),
        ),
    ];
    for (name, provider) in &providers {
        let tracer = provider.tracer("bench");
        group.bench_function(*name, |b| b.iter(|| trace(&tracer)));
    }
    group.finish();
}

/// Measures a trace while 1000 other traces are open. The open traces fill
/// the shards of `TraceErrorSamplingProcessor`.
fn end_to_end_with_open_traces(c: &mut Criterion) {
    let mut group = c.benchmark_group("trace_with_1000_open");
    let sampler = || AlwaysRecordSampler::new(Sampler::parent_based(ProbabilitySampler::new(0.25)));
    let providers = [
        (
            "error_sampling_processor",
            SdkTracerProvider::builder()
                .with_sampler(sampler())
                .with_span_processor(ErrorSamplingProcessor::new(Noop))
                .build(),
        ),
        (
            "trace_processor",
            SdkTracerProvider::builder()
                .with_sampler(sampler())
                .with_span_processor(TraceErrorSamplingProcessor::new(Noop))
                .build(),
        ),
    ];
    for (name, provider) in &providers {
        let tracer = provider.tracer("bench");
        let open: Vec<_> = (0..1000).map(|_| tracer.start("open")).collect();
        group.bench_function(*name, |b| b.iter(|| trace(&tracer)));
        drop(open);
    }
    group.finish();
}

/// Measures traces with 50 and 100 spans, with and without an error.
fn large_traces(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_trace");
    let sampler = || AlwaysRecordSampler::new(Sampler::parent_based(ProbabilitySampler::new(0.25)));
    let providers = [
        (
            "sdk_parent_based_ratio_0.25",
            SdkTracerProvider::builder()
                .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
                    0.25,
                ))))
                .with_span_processor(Noop)
                .build(),
        ),
        (
            "error_sampling_processor",
            SdkTracerProvider::builder()
                .with_sampler(sampler())
                .with_span_processor(ErrorSamplingProcessor::new(Noop))
                .build(),
        ),
        (
            "trace_processor",
            SdkTracerProvider::builder()
                .with_sampler(sampler())
                .with_span_processor(TraceErrorSamplingProcessor::new(Noop))
                .build(),
        ),
    ];
    for spans in [50, 100] {
        for (name, provider) in &providers {
            let tracer = provider.tracer("bench");
            for fail in [false, true] {
                let outcome = if fail { "error" } else { "ok" };
                group.bench_function(format!("{spans}_spans/{name}/{outcome}"), |b| {
                    b.iter(|| large_trace(&tracer, spans, fail))
                });
            }
        }
    }
    group.finish();
}

/// Measures the wall time for each trace while 8 threads make traces with
/// the same provider. The threads compete for the shard locks.
fn contended(c: &mut Criterion) {
    const THREADS: u64 = 8;
    let mut group = c.benchmark_group("contended_8_threads");
    let sampler = || AlwaysRecordSampler::new(Sampler::parent_based(ProbabilitySampler::new(0.25)));
    let providers = [
        (
            "sdk_parent_based_ratio_0.25",
            SdkTracerProvider::builder()
                .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
                    0.25,
                ))))
                .with_span_processor(Noop)
                .build(),
        ),
        (
            "error_sampling_processor",
            SdkTracerProvider::builder()
                .with_sampler(sampler())
                .with_span_processor(ErrorSamplingProcessor::new(Noop))
                .build(),
        ),
        (
            "trace_processor",
            SdkTracerProvider::builder()
                .with_sampler(sampler())
                .with_span_processor(TraceErrorSamplingProcessor::new(Noop))
                .build(),
        ),
    ];
    for spans in [2, 50] {
        for (name, provider) in &providers {
            let tracer = provider.tracer("bench");
            group.bench_function(format!("{spans}_spans/{name}"), |b| {
                b.iter_custom(|iters| {
                    let per_thread = iters.div_ceil(THREADS);
                    let start = Instant::now();
                    thread::scope(|scope| {
                        for _ in 0..THREADS {
                            scope.spawn(|| {
                                for _ in 0..per_thread {
                                    if spans == 2 {
                                        trace(&tracer);
                                    } else {
                                        large_trace(&tracer, spans, false);
                                    }
                                }
                            });
                        }
                    });
                    start.elapsed()
                })
            });
        }
    }
    group.finish();
}

fn attributes() -> Vec<KeyValue> {
    vec![
        KeyValue::new("http.request.method", "GET"),
        KeyValue::new("url.path", "/api/v1/items"),
        KeyValue::new("http.response.status_code", 200),
        KeyValue::new("server.address", "example.com"),
        KeyValue::new("server.port", 443),
        KeyValue::new("network.protocol.version", "1.1"),
        KeyValue::new("user_agent.original", "bench"),
        KeyValue::new("http.route", "/api/v1/items"),
    ]
}

fn span_data(sampled: bool, error: bool, trace_state: &str) -> SpanData {
    SpanData {
        span_context: SpanContext::new(
            HIGH,
            SpanId::from(2_u64),
            TraceFlags::default().with_sampled(sampled),
            false,
            trace_state.parse::<TraceState>().unwrap(),
        ),
        parent_span_id: SpanId::from(1_u64),
        parent_span_is_remote: false,
        span_kind: SpanKind::Server,
        name: Cow::Borrowed("GET /api/v1/items"),
        start_time: SystemTime::UNIX_EPOCH,
        end_time: SystemTime::UNIX_EPOCH,
        attributes: attributes(),
        dropped_attributes_count: 0,
        events: SpanEvents::default(),
        links: SpanLinks::default(),
        status: if error {
            Status::error("boom")
        } else {
            Status::Unset
        },
        instrumentation_scope: InstrumentationScope::default(),
    }
}

fn parent(trace_id: TraceId, sampled: bool, trace_state: &str) -> Context {
    Context::new().with_remote_span_context(SpanContext::new(
        trace_id,
        SpanId::from(1_u64),
        TraceFlags::default().with_sampled(sampled),
        true,
        trace_state.parse().unwrap(),
    ))
}

/// Starts and ends a root span and one child span. Both spans have attributes.
fn trace(tracer: &opentelemetry_sdk::trace::SdkTracer) {
    let cx = Context::current_with_span(
        tracer
            .span_builder("root")
            .with_attributes(attributes())
            .start(tracer),
    );
    let mut child = tracer
        .span_builder("child")
        .with_attributes(attributes())
        .start_with_context(tracer, &cx);
    child.set_attribute(KeyValue::new("db.rows", 10));
    child.end();
    cx.span().end();
}

/// Starts and ends a trace with `spans` spans: a root span, 10 middle spans,
/// and leaf spans below the middle spans. When `fail` is `true`, the last leaf
/// span ends with an error.
fn large_trace(tracer: &opentelemetry_sdk::trace::SdkTracer, spans: usize, fail: bool) {
    const MIDDLE: usize = 10;
    let root = Context::current_with_span(
        tracer
            .span_builder("root")
            .with_attributes(attributes())
            .start(tracer),
    );
    let middle: Vec<Context> = (0..MIDDLE)
        .map(|_| {
            root.with_span(
                tracer
                    .span_builder("middle")
                    .with_attributes(attributes())
                    .start_with_context(tracer, &root),
            )
        })
        .collect();
    let leaves = spans - 1 - MIDDLE;
    for leaf in 0..leaves {
        let mut span = tracer
            .span_builder("leaf")
            .with_attributes(attributes())
            .start_with_context(tracer, &middle[leaf % MIDDLE]);
        if fail && leaf == leaves - 1 {
            span.set_status(Status::error("boom"));
        }
        span.end();
    }
    for cx in &middle {
        cx.span().end();
    }
    root.span().end();
}

/// The randomness `ffffffffffffff`. All ratios above 0 sample it.
const HIGH: TraceId = TraceId::from_bytes([
    1, 2, 3, 4, 5, 6, 7, 8, 0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
]);

/// The randomness `00000000000001`. Only the ratio 1 samples it.
const LOW: TraceId = TraceId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 0xff, 0, 0, 0, 0, 0, 0, 1]);
