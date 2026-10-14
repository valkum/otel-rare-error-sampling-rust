#![allow(dead_code)]

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};
use std::time::Duration;

use opentelemetry::{
    Context,
    trace::{
        Span as _, SpanContext, SpanId, Status, TraceContextExt, TraceFlags, TraceId, TraceState,
        Tracer as _, TracerProvider as _,
    },
};
use opentelemetry_rare_error_sampling::{
    AlwaysRecordSampler, ErrorSamplingProcessor, ProbabilitySampler,
};
use opentelemetry_sdk::{
    Resource,
    error::OTelSdkResult,
    trace::{
        BatchConfigBuilder, BatchSpanProcessor, IdGenerator, InMemorySpanExporter,
        ParentBasedSampler, Sampler, SdkTracerProvider, ShouldSample, SimpleSpanProcessor, Span,
        SpanData, SpanExporter, SpanProcessor,
    },
};

/// Gives each provider one fixed trace id. The span ids increase by 1.
#[derive(Debug)]
pub struct FixedIds {
    trace_id: TraceId,
    next_span_id: AtomicU64,
}

impl FixedIds {
    pub fn new(trace_id: TraceId) -> Self {
        Self {
            trace_id,
            next_span_id: AtomicU64::new(1),
        }
    }
}

impl IdGenerator for FixedIds {
    fn new_trace_id(&self) -> TraceId {
        self.trace_id
    }

    fn new_span_id(&self) -> SpanId {
        SpanId::from(self.next_span_id.fetch_add(1, Ordering::Relaxed))
    }
}

/// Gives the root spans the trace ids of `trace_ids` in sequence. The span
/// ids increase by 1.
#[derive(Debug)]
pub struct SequenceIds {
    trace_ids: Vec<TraceId>,
    next_trace: AtomicUsize,
    next_span_id: AtomicU64,
}

impl SequenceIds {
    pub fn new(trace_ids: Vec<TraceId>) -> Self {
        Self {
            trace_ids,
            next_trace: AtomicUsize::new(0),
            next_span_id: AtomicU64::new(1),
        }
    }
}

impl IdGenerator for SequenceIds {
    fn new_trace_id(&self) -> TraceId {
        let index = self.next_trace.fetch_add(1, Ordering::Relaxed);
        self.trace_ids[index % self.trace_ids.len()]
    }

    fn new_span_id(&self) -> SpanId {
        SpanId::from(self.next_span_id.fetch_add(1, Ordering::Relaxed))
    }
}

/// An exporter that keeps the exported spans, also after shutdown.
#[derive(Debug, Clone, Default)]
pub struct CollectingExporter {
    pub spans: Arc<Mutex<Vec<SpanData>>>,
    pub shut_down: Arc<AtomicUsize>,
}

impl CollectingExporter {
    pub fn spans(&self) -> Vec<SpanData> {
        self.spans.lock().unwrap().clone()
    }
}

impl SpanExporter for CollectingExporter {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        self.spans.lock().unwrap().extend(batch);
        Ok(())
    }

    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        self.shut_down.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

pub struct Harness {
    pub provider: SdkTracerProvider,
    pub exporter: InMemorySpanExporter,
}

impl Harness {
    /// Makes a provider that exports spans to an in-memory exporter, through
    /// `ErrorSamplingProcessor` and `SimpleSpanProcessor`.
    pub fn new(sampler: impl ShouldSample + 'static, trace_id: TraceId) -> Self {
        Self::with_processor(sampler, trace_id, ErrorSamplingProcessor::new)
    }

    pub fn with_processor<P: SpanProcessor + 'static>(
        sampler: impl ShouldSample + 'static,
        trace_id: TraceId,
        wrap: impl FnOnce(SimpleSpanProcessor<InMemorySpanExporter>) -> P,
    ) -> Self {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_sampler(sampler)
            .with_id_generator(FixedIds::new(trace_id))
            .with_span_processor(wrap(SimpleSpanProcessor::new(exporter.clone())))
            .build();
        Self { provider, exporter }
    }

    pub fn spans(&self) -> Vec<SpanData> {
        self.exporter.get_finished_spans().unwrap()
    }

    pub fn span(&self, name: &str) -> SpanData {
        let spans = self.spans();
        let found: Vec<_> = spans.into_iter().filter(|s| s.name == name).collect();
        assert_eq!(found.len(), 1, "expected one exported span named {name}");
        found.into_iter().next().unwrap()
    }

    pub fn names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.spans().iter().map(|s| s.name.to_string()).collect();
        names.sort();
        names
    }
}

/// A processor that records all calls. Use it to make sure that the wrapper
/// forwards the calls.
#[derive(Debug, Clone, Default)]
pub struct RecordingProcessor {
    pub started: Arc<AtomicUsize>,
    pub ended: Arc<Mutex<Vec<SpanData>>>,
    pub flushed: Arc<AtomicUsize>,
    pub shut_down: Arc<AtomicUsize>,
    pub resources: Arc<Mutex<Vec<Resource>>>,
}

impl SpanProcessor for RecordingProcessor {
    fn on_start(&self, _span: &mut Span, _cx: &Context) {
        self.started.fetch_add(1, Ordering::Relaxed);
    }

    fn on_end(&self, span: SpanData) {
        self.ended.lock().unwrap().push(span);
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.flushed.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        self.shut_down.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.resources.lock().unwrap().push(resource.clone());
    }
}

pub fn remote_parent(trace_id: TraceId, sampled: bool, trace_state: &str) -> Context {
    let flags = TraceFlags::default().with_sampled(sampled);
    let trace_state: TraceState = trace_state.parse().unwrap();
    Context::new().with_remote_span_context(SpanContext::new(
        trace_id,
        SpanId::from(0xabcd_u64),
        flags,
        true,
        trace_state,
    ))
}

/// The head sampler of the tests: `AlwaysRecord` around a parent-based
/// probability sampler.
pub fn sampler(ratio: f64) -> AlwaysRecordSampler<ParentBasedSampler> {
    AlwaysRecordSampler::new(Sampler::parent_based(ProbabilitySampler::new(ratio)))
}

/// Starts `root` with the children `ok` and `failed`, and ends all of them.
/// `failed` ends with an error.
pub fn root_with_children(harness: &Harness, parent: &Context) {
    let tracer = harness.provider.tracer("test");
    let cx = parent.with_span(tracer.start_with_context("root", parent));
    tracer.start_with_context("ok", &cx).end();
    let mut failed = tracer.start_with_context("failed", &cx);
    failed.set_status(Status::error("boom"));
    failed.end();
    cx.span().end();
}

/// A `BatchSpanProcessor` with a queue that holds all spans of a test. So
/// the batch processor drops no span.
pub fn batch_processor(exporter: &CollectingExporter) -> BatchSpanProcessor {
    BatchSpanProcessor::builder(exporter.clone())
        .with_batch_config(
            BatchConfigBuilder::default()
                .with_max_queue_size(16_384)
                .build(),
        )
        .build()
}

/// Returns `count` trace ids. Each 4th trace id has high randomness, so a
/// ratio of 0.25 samples it. The other trace ids have low randomness.
pub fn one_in_four_sampled(count: usize) -> Vec<TraceId> {
    (0..count)
        .map(|i| {
            let randomness: u128 = if i % 4 == 0 { 0xff_ffff_ffff_ffff } else { 1 };
            TraceId::from(((i as u128 + 1) << 64) | randomness)
        })
        .collect()
}

/// A trace id with the randomness `ffffffffffffff`. All ratios above 0 sample
/// it.
pub const HIGH_RANDOMNESS: TraceId = TraceId::from_bytes([
    1, 2, 3, 4, 5, 6, 7, 8, 0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
]);

/// A trace id with the randomness `00000000000001`. Only the ratio 1 samples
/// it.
pub const LOW_RANDOMNESS: TraceId =
    TraceId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 0xff, 0, 0, 0, 0, 0, 0, 1]);
