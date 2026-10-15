use std::{
    collections::VecDeque,
    sync::{Mutex, PoisonError},
    time::{Duration, Instant},
};

use opentelemetry::{
    Context, Key,
    trace::{Span as _, TraceId},
};
use opentelemetry_sdk::{
    Resource,
    error::OTelSdkResult,
    trace::{Span, SpanData, SpanProcessor},
};

use crate::error::{self, ErrorRule};

/// A span processor that exports all spans of a local trace when one of them
/// ends with an error.
///
/// A local trace is the part of a trace in this process. This is inspired by
/// <https://docs.datadoghq.com/tracing/guide/ingestion_sampling_use_cases/#keeping-certain-types-of-traces>.
///
/// The processor holds the spans that are not sampled until all spans of their
/// local trace have ended. After an error, it exports the held spans and the
/// later spans of the trace without a `th` value
/// ([OTEP 235](https://github.com/open-telemetry/opentelemetry-specification/blob/main/oteps/trace/0235-sampling-threshold-in-trace-state.md#downstream-samplers)).
/// Sampled spans pass through unchanged.
///
/// # Limits
///
/// - [`DEFAULT_MAX_TRACES`] local traces, approximately. The processor removes
///   the oldest trace first.
/// - [`DEFAULT_MAX_SPANS_PER_TRACE`] held spans for each trace.
/// - [`DEFAULT_TRACE_TIMEOUT`] for each trace.
/// - [`DEFAULT_MAX_KEPT_TRACES`] decisions for traces with an error. Later
///   spans of these traces are also exported. The least recently used
///   decision goes first.
///
/// An error span is always exported, also when the processor removed its
/// trace. When the processor removes a trace with open spans, it can drop
/// later spans of the trace that have no error.
#[derive(Debug)]
pub struct TraceErrorSamplingProcessor<P> {
    inner: P,
    rule: ErrorRule,
    shards: Box<[Mutex<Shard>]>,
    limits: Limits,
}

impl<P: SpanProcessor> TraceErrorSamplingProcessor<P> {
    /// Creates a processor that sends the exported spans to `inner`.
    pub fn new(inner: P) -> Self {
        Self {
            inner,
            rule: ErrorRule::default(),
            shards: (0..SHARDS).map(|_| Mutex::default()).collect(),
            limits: Limits {
                max_traces_per_shard: DEFAULT_MAX_TRACES.div_ceil(SHARDS),
                max_spans_per_trace: DEFAULT_MAX_SPANS_PER_TRACE,
                trace_timeout: DEFAULT_TRACE_TIMEOUT,
                max_kept_per_shard: DEFAULT_MAX_KEPT_TRACES.div_ceil(SHARDS),
            },
        }
    }

    /// Sets the approximate maximum number of local traces that the processor
    /// holds.
    pub fn with_max_traces(mut self, max_traces: usize) -> Self {
        self.limits.max_traces_per_shard = max_traces.div_ceil(SHARDS).max(1);
        self
    }

    /// Sets the maximum number of spans that the processor holds for one
    /// local trace.
    pub fn with_max_spans_per_trace(mut self, max_spans: usize) -> Self {
        self.limits.max_spans_per_trace = max_spans;
        self
    }

    /// Sets the maximum time that the processor holds a local trace.
    pub fn with_trace_timeout(mut self, timeout: Duration) -> Self {
        self.limits.trace_timeout = timeout;
        self
    }

    /// Sets the approximate maximum number of decisions for traces with an
    /// error. 0 turns off the decisions. The maximum is
    /// [`MAX_KEPT_TRACES`].
    pub fn with_max_kept_traces(mut self, max_kept: usize) -> Self {
        self.limits.max_kept_per_shard = max_kept.div_ceil(SHARDS).min(MAX_KEPT_PER_SHARD);
        self
    }

    /// Sets the attribute that marks a span as an error when its value is
    /// `"ERROR"`.
    pub fn with_status_attribute(mut self, key: impl Into<Key>) -> Self {
        self.rule.set_status_attribute(Some(key.into()));
        self
    }

    /// Uses only the span status to find error spans.
    pub fn without_status_attribute(mut self) -> Self {
        self.rule.set_status_attribute(None);
        self
    }

    /// Returns the wrapped processor.
    pub fn inner(&self) -> &P {
        &self.inner
    }

    fn shard(&self, trace_id: TraceId) -> std::sync::MutexGuard<'_, Shard> {
        self.shards[shard_index(trace_id)]
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl<P: SpanProcessor> SpanProcessor for TraceErrorSamplingProcessor<P> {
    fn on_start(&self, span: &mut Span, cx: &Context) {
        let sc = span.span_context();
        if !sc.is_sampled() {
            let removed = self.shard(sc.trace_id()).start(sc.trace_id(), self.limits);
            // Drop the large buffers of removed traces without the lock.
            drop(removed);
        }
        self.inner.on_start(span, cx);
    }

    fn on_end(&self, span: SpanData) {
        if span.span_context.is_sampled() {
            self.inner.on_end(span);
            return;
        }
        let is_error = self.rule.is_error(&span);

        let trace_id = span.span_context.trace_id();
        let ended = self
            .shard(trace_id)
            .end(trace_id, span, is_error, self.limits);
        // Call the inner processor and drop large buffers without the lock.
        for mut span in ended.export.into_iter().chain(ended.span) {
            error::mark_sampled(&mut span);
            self.inner.on_end(span);
        }
        drop(ended.drop);
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        for shard in &self.shards {
            *shard.lock().unwrap_or_else(PoisonError::into_inner) = Shard::default();
        }
        self.inner.shutdown_with_timeout(timeout)
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}

#[derive(Debug, Clone, Copy)]
struct Limits {
    max_traces_per_shard: usize,
    max_spans_per_trace: usize,
    trace_timeout: Duration,
    max_kept_per_shard: usize,
}

/// The local traces of one shard.
///
/// A shard holds few traces: the traces in flight divided by the number of
/// shards, and at most the trace limit. So a linear search in a `Vec` is
/// faster than a hash map, and the shard needs no separate queue for the age
/// of the traces.
#[derive(Debug, Default)]
struct Shard {
    /// The local traces with open spans.
    traces: Vec<LocalTrace>,
    /// The decisions for ended local traces with an error, and the time of
    /// their last use. The least recently used decision is first.
    ///
    /// The idea comes from the sampled decision cache of the tail sampling
    /// processor in the OpenTelemetry Collector:
    /// <https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/processor/tailsamplingprocessor>.
    kept: VecDeque<(TraceId, Instant)>,
    /// Empty span buffers for new traces.
    pool: Vec<Vec<SpanData>>,
    /// The next time to look for traces that are too old.
    next_scan: Option<Instant>,
}

#[derive(Debug)]
struct LocalTrace {
    trace_id: TraceId,
    created: Instant,
    /// The number of spans that started and did not end.
    open: usize,
    /// `true` after a span of the trace ended with an error.
    keep: bool,
    /// The ended spans that wait for the decision.
    held: Vec<SpanData>,
}

/// The spans that `Shard::end` hands to the caller. The caller exports or
/// drops them without the lock.
#[derive(Debug, Default)]
struct Ended {
    /// The span that ended, when the processor exports it.
    span: Option<SpanData>,
    /// The held spans to export, after the first error of the trace.
    export: Vec<SpanData>,
    /// The held spans to drop. The shard drops small buffers itself.
    drop: Vec<SpanData>,
}

impl Shard {
    /// Records the start of a span. Returns the large span buffers of the
    /// traces that the shard removed.
    fn start(&mut self, trace_id: TraceId, limits: Limits) -> Vec<Vec<SpanData>> {
        if let Some(trace) = self.traces.iter_mut().find(|t| t.trace_id == trace_id) {
            trace.open += 1;
            return Vec::new();
        }
        let now = Instant::now();
        // Take the decision before the eviction. An eviction can remember
        // other decisions and push this one out.
        let keep = self.take_kept(trace_id, now, limits);
        let removed = self.evict(now, limits);
        let held = if keep {
            Vec::new()
        } else {
            self.pool.pop().unwrap_or_default()
        };
        self.traces.push(LocalTrace {
            trace_id,
            created: now,
            open: 1,
            keep,
            held,
        });
        removed
    }

    /// Records the end of a span.
    fn end(&mut self, trace_id: TraceId, span: SpanData, is_error: bool, limits: Limits) -> Ended {
        let Some(index) = self.traces.iter().position(|t| t.trace_id == trace_id) else {
            // The processor removed the trace. Export error spans, and remember
            // their decision. Also export the spans of a trace with an error.
            let now = Instant::now();
            let keep = if is_error {
                self.remember(trace_id, now, limits);
                true
            } else {
                self.use_kept(trace_id, now, limits)
            };
            return Ended {
                span: keep.then_some(span),
                ..Ended::default()
            };
        };
        let trace = &mut self.traces[index];
        trace.open = trace.open.saturating_sub(1);

        let mut ended = Ended::default();
        if is_error || trace.keep {
            if !trace.keep {
                trace.keep = true;
                ended.export = std::mem::take(&mut trace.held);
            }
            ended.span = Some(span);
        } else if trace.held.len() < limits.max_spans_per_trace {
            trace.held.push(span);
        }

        if trace.open == 0 {
            ended.drop = self.remove(index, limits).unwrap_or_default();
        }
        ended
    }

    /// Takes the decision for a trace out of the shard. Returns `true` when a
    /// recent trace with this id had an error. While the trace is live, the
    /// trace itself carries the decision.
    fn take_kept(&mut self, trace_id: TraceId, now: Instant, limits: Limits) -> bool {
        let Some(index) = self.kept.iter().position(|&(id, _)| id == trace_id) else {
            return false;
        };
        let (_, at) = self.kept.remove(index).expect("the index is valid");
        now.duration_since(at) < limits.trace_timeout
    }

    /// Returns `true` when a recent trace with this id had an error. Then
    /// moves the decision to the end, as in an LRU cache.
    fn use_kept(&mut self, trace_id: TraceId, now: Instant, limits: Limits) -> bool {
        let fresh = self.take_kept(trace_id, now, limits);
        if fresh {
            self.kept.push_back((trace_id, now));
        }
        fresh
    }

    /// Puts the decision for a trace with an error at the end. A trace has at
    /// most one decision.
    fn remember(&mut self, trace_id: TraceId, now: Instant, limits: Limits) {
        if let Some(index) = self.kept.iter().position(|&(id, _)| id == trace_id) {
            self.kept.remove(index);
        } else if limits.max_kept_per_shard == 0 {
            return;
        } else if self.kept.len() >= limits.max_kept_per_shard {
            self.kept.pop_front();
        }
        self.kept.push_back((trace_id, now));
    }

    /// Keeps an empty span buffer for reuse. A buffer without capacity saves
    /// no allocation, so the pool does not keep it.
    fn recycle(&mut self, mut held: Vec<SpanData>) {
        if self.pool.len() < POOL_SIZE && (1..=POOL_MAX_CAPACITY).contains(&held.capacity()) {
            held.clear();
            self.pool.push(held);
        }
    }

    /// Removes the local traces that are too old, and the old decisions. Then
    /// removes the oldest local trace when the shard is full. Returns the
    /// large span buffers of the removed traces.
    ///
    /// The search for old traces occurs at most one time in each 1/8 of the
    /// timeout. So a trace can stay up to 1/8 of the timeout longer.
    fn evict(&mut self, now: Instant, limits: Limits) -> Vec<Vec<SpanData>> {
        let mut removed = Vec::new();
        if self.next_scan.is_none_or(|next| now >= next) {
            // A day limits the interval, so that a very long timeout cannot
            // cause an overflow.
            let interval = (limits.trace_timeout / 8).min(Duration::from_secs(86_400));
            self.next_scan = now.checked_add(interval);
            let mut index = 0;
            while index < self.traces.len() {
                if now.duration_since(self.traces[index].created) >= limits.trace_timeout {
                    removed.extend(self.remove(index, limits));
                } else {
                    index += 1;
                }
            }
            // The least recently used decision is first.
            while self
                .kept
                .front()
                .is_some_and(|&(_, at)| now.duration_since(at) >= limits.trace_timeout)
            {
                self.kept.pop_front();
            }
        }
        if self.traces.len() >= limits.max_traces_per_shard {
            let oldest = self
                .traces
                .iter()
                .enumerate()
                .min_by_key(|(_, trace)| trace.created)
                .map(|(index, _)| index);
            if let Some(oldest) = oldest {
                removed.extend(self.remove(oldest, limits));
            }
        }
        removed
    }

    /// Removes a local trace. Remembers the decision of a trace with an error,
    /// so that later spans of the trace are also exported.
    ///
    /// The shard drops up to [`MAX_DROP_UNDER_LOCK`] held spans and keeps the
    /// empty buffer for reuse. It returns a larger buffer, which the caller
    /// drops without the lock. A second lock to return the buffer to the pool
    /// costs more than it saves.
    fn remove(&mut self, index: usize, limits: Limits) -> Option<Vec<SpanData>> {
        let trace = self.traces.swap_remove(index);
        if trace.keep {
            self.remember(trace.trace_id, Instant::now(), limits);
        }
        if trace.held.len() > MAX_DROP_UNDER_LOCK {
            return Some(trace.held);
        }
        self.recycle(trace.held);
        None
    }
}

/// Returns the shard of a trace id.
///
/// Some SDKs and proxies make trace ids that are not fully random. So the
/// function mixes all 128 bits with the splitmix64 finalizer, as the
/// OpenTelemetry Collector does:
/// <https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/processor/tailsamplingprocessor/sharded_processor.go>.
fn shard_index(trace_id: TraceId) -> usize {
    let bytes = trace_id.to_bytes();
    let (high, low) = bytes.split_at(8);
    let mut h = u64::from_le_bytes(high.try_into().expect("8 bytes"))
        ^ u64::from_le_bytes(low.try_into().expect("8 bytes"));
    h ^= h >> 30;
    h = h.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    h ^= h >> 27;
    h = h.wrapping_mul(0x94d0_49bb_1331_11eb);
    h ^= h >> 31;
    (h % SHARDS as u64) as usize
}

/// The default maximum number of local traces that the processor holds.
pub const DEFAULT_MAX_TRACES: usize = 10_000;

/// The default maximum number of spans that the processor holds for one
/// local trace.
pub const DEFAULT_MAX_SPANS_PER_TRACE: usize = 1_000;

/// The default maximum time that the processor holds a local trace.
pub const DEFAULT_TRACE_TIMEOUT: Duration = Duration::from_secs(30);

/// The default maximum number of decisions for traces with an error.
pub const DEFAULT_MAX_KEPT_TRACES: usize = 4_096;

/// The highest value for
/// [`with_max_kept_traces`](TraceErrorSamplingProcessor::with_max_kept_traces).
pub const MAX_KEPT_TRACES: usize = MAX_KEPT_PER_SHARD * SHARDS;

/// The number of shards. Each shard has its own lock.
const SHARDS: usize = 64;

/// The maximum number of empty span buffers that a shard keeps for reuse.
const POOL_SIZE: usize = 4;

/// The maximum capacity of a span buffer that a shard keeps for reuse. A
/// `SpanData` uses 352 bytes, so a full pool uses up to 180 KB for each
/// shard.
const POOL_MAX_CAPACITY: usize = 128;

/// The maximum number of decisions in one shard. A shard searches its
/// decisions linearly, so the number must stay small.
const MAX_KEPT_PER_SHARD: usize = 256;

/// The maximum number of held spans that a shard drops under its lock. A span
/// takes about 180 ns to drop, so the lock stays shorter than about 6 µs. The
/// caller drops larger buffers without the lock.
const MAX_DROP_UNDER_LOCK: usize = 32;

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, time::SystemTime};

    use opentelemetry::{
        InstrumentationScope,
        trace::{SpanContext, SpanId, SpanKind, Status, TraceFlags, TraceState},
    };
    use opentelemetry_sdk::trace::{
        InMemorySpanExporter, SimpleSpanProcessor, SpanEvents, SpanLinks,
    };

    use super::*;

    #[test]
    fn full_shard_removes_the_oldest_trace() {
        let limits = Limits {
            max_traces_per_shard: 2,
            ..limits()
        };
        let mut shard = Shard::default();

        // Two traces hold one span each, so the shard is full.
        shard.start(id(1), limits);
        assert!(run(&mut shard, 1, "a-ok", false, limits).is_empty());
        shard.start(id(2), limits);
        assert!(run(&mut shard, 2, "b-ok", false, limits).is_empty());

        // A third trace removes trace 1, which is the oldest.
        shard.start(id(3), limits);

        // Trace 2 still has its held span.
        assert_eq!(
            run(&mut shard, 2, "b-failed", true, limits),
            ["b-ok", "b-failed"]
        );

        // The root span of trace 1 ends with an error. Its held span is gone.
        assert_eq!(end(&mut shard, 1, "a-failed", true, limits), ["a-failed"]);
    }

    #[test]
    fn timeout_removes_old_traces() {
        let limits = Limits {
            trace_timeout: Duration::ZERO,
            ..limits()
        };
        let mut shard = Shard::default();

        // Trace 1 holds one span.
        shard.start(id(1), limits);
        assert!(run(&mut shard, 1, "a-ok", false, limits).is_empty());

        // A new trace starts the search for old traces.
        shard.start(id(2), limits);

        // The timeout removed trace 1, so its held span is gone.
        assert_eq!(run(&mut shard, 1, "a-failed", true, limits), ["a-failed"]);
    }

    #[test]
    fn remembers_the_most_recent_decisions() {
        let limits = limits();
        let mut shard = Shard::default();

        for trace in 1..=3 {
            run(&mut shard, trace, "failed", true, limits);
        }

        // The shard remembers only the 2 most recent errors, so trace 1 is
        // forgotten.
        assert!(run(&mut shard, 1, "later", false, limits).is_empty());
        assert_eq!(run(&mut shard, 2, "later", false, limits), ["later"]);
        assert_eq!(run(&mut shard, 3, "later", false, limits), ["later"]);
    }

    #[test]
    fn a_used_decision_moves_to_the_end() {
        let limits = limits();
        let mut shard = Shard::default();

        run(&mut shard, 1, "failed", true, limits);
        run(&mut shard, 2, "failed", true, limits);

        // A later span of trace 1 uses its decision, and the trace ends again.
        // Then trace 3 removes the least recently used decision, which is the
        // decision of trace 2.
        assert_eq!(run(&mut shard, 1, "later", false, limits), ["later"]);
        run(&mut shard, 3, "failed", true, limits);

        // Trace 2 is forgotten. Trace 1 is still remembered.
        assert!(run(&mut shard, 2, "again", false, limits).is_empty());
        assert_eq!(run(&mut shard, 1, "again", false, limits), ["again"]);
    }

    #[test]
    fn zero_decisions_remember_nothing() {
        let limits = Limits {
            max_kept_per_shard: 0,
            ..limits()
        };
        let mut shard = Shard::default();

        run(&mut shard, 1, "failed", true, limits);

        // The error is not remembered, so the later span is dropped.
        assert!(run(&mut shard, 1, "later", false, limits).is_empty());
    }

    #[test]
    fn max_kept_traces_is_divided_and_limited() {
        let per_shard = |max_kept| {
            TraceErrorSamplingProcessor::new(SimpleSpanProcessor::new(
                InMemorySpanExporter::default(),
            ))
            .with_max_kept_traces(max_kept)
            .limits
            .max_kept_per_shard
        };

        // The number is divided between the 64 shards, and each shard has a
        // maximum.
        assert_eq!(per_shard(DEFAULT_MAX_KEPT_TRACES), 64);
        assert_eq!(per_shard(128), 2);
        assert_eq!(per_shard(0), 0);
        assert_eq!(per_shard(usize::MAX), MAX_KEPT_PER_SHARD);
    }

    /// Trace ids with only a counter in the high bits, and zero low bits.
    #[test]
    fn spreads_skewed_trace_ids_over_all_shards() {
        let mut counts = [0usize; SHARDS];
        for n in 0..(SHARDS as u128 * 100) {
            counts[shard_index(TraceId::from(n << 64))] += 1;
        }

        // Each shard gets about the same number of trace ids.
        assert!(
            counts.iter().all(|&count| (50..=150).contains(&count)),
            "{counts:?}"
        );
    }

    /// Trace ids with a timestamp in the high bits, and a constant in the low
    /// bits.
    #[test]
    fn spreads_trace_ids_with_constant_low_bits() {
        let mut counts = [0usize; SHARDS];
        for n in 0..(SHARDS as u128 * 100) {
            let timestamp = 1_700_000_000 + n;
            counts[shard_index(TraceId::from((timestamp << 96) | 0xabcd))] += 1;
        }

        // Each shard gets about the same number of trace ids.
        assert!(
            counts.iter().all(|&count| (50..=150).contains(&count)),
            "{counts:?}"
        );
    }

    #[test]
    fn removed_trace_with_an_error_is_remembered() {
        let limits = Limits {
            max_traces_per_shard: 2,
            ..limits()
        };
        let mut shard = Shard::default();

        // Trace 1 has an error in a child span, and its root span is open.
        shard.start(id(1), limits);
        assert_eq!(run(&mut shard, 1, "child", true, limits), ["child"]);

        // Two new traces fill the shard and remove trace 1.
        shard.start(id(2), limits);
        shard.start(id(3), limits);

        // The decision for trace 1 is remembered, so its root span is
        // exported.
        assert_eq!(end(&mut shard, 1, "root", false, limits), ["root"]);
    }

    #[test]
    fn error_of_a_removed_trace_is_remembered() {
        let limits = Limits {
            max_traces_per_shard: 1,
            ..limits()
        };
        let mut shard = Shard::default();

        // Trace 1 has an open root span and an open child span. Trace 2
        // removes it.
        shard.start(id(1), limits);
        shard.start(id(1), limits);
        shard.start(id(2), limits);

        // The child ends with an error. The processor exports it and
        // remembers the decision, so the root span is exported too.
        assert_eq!(end(&mut shard, 1, "child", true, limits), ["child"]);
        assert_eq!(end(&mut shard, 1, "root", false, limits), ["root"]);
    }

    #[test]
    fn pool_keeps_only_buffers_with_capacity() {
        let limits = limits();
        let mut shard = Shard::default();

        // A remembered trace starts without a buffer. When it ends, the empty
        // buffer must not take a place in the pool.
        run(&mut shard, 1, "failed", true, limits);
        run(&mut shard, 1, "later", false, limits);
        shard.recycle(Vec::new());

        assert!(shard.pool.iter().all(|buffer| buffer.capacity() > 0));
    }

    #[test]
    fn use_of_a_decision_moves_it_to_the_end() {
        let limits = limits();
        let now = Instant::now();
        let mut shard = Shard::default();
        shard.remember(id(1), now, limits);
        shard.remember(id(2), now, limits);

        // A span of trace 1 ends after the shard removed the trace. The use
        // moves the decision of trace 1 to the end.
        assert!(shard.use_kept(id(1), now, limits));

        let order: Vec<TraceId> = shard.kept.iter().map(|&(id, _)| id).collect();
        assert_eq!(order, [id(2), id(1)]);
    }

    #[test]
    fn decision_is_taken_before_the_eviction() {
        let limits = Limits {
            max_traces_per_shard: 1,
            max_kept_per_shard: 1,
            ..limits()
        };
        let mut shard = Shard::default();
        run(&mut shard, 1, "failed", true, limits);

        // Trace 2 has an error and an open root span, so the shard is full.
        shard.start(id(2), limits);
        assert_eq!(run(&mut shard, 2, "failed", true, limits), ["failed"]);

        // A later span of trace 1 starts. The eviction of trace 2 remembers
        // its decision, which fills the decisions. Trace 1 took its decision
        // before, so its later span is still exported.
        assert_eq!(run(&mut shard, 1, "later", false, limits), ["later"]);
    }

    #[test]
    fn live_trace_carries_its_decision() {
        let limits = limits();
        let mut shard = Shard::default();
        run(&mut shard, 1, "failed", true, limits);

        // While trace 1 is live, its decision is not in the list of decisions.
        shard.start(id(1), limits);
        assert!(shard.kept.is_empty());

        // When the trace ends, the decision goes back to the list.
        end(&mut shard, 1, "later", false, limits);
        assert_eq!(shard.kept.len(), 1);
    }

    #[test]
    fn old_decisions_are_removed() {
        let limits = Limits {
            trace_timeout: Duration::ZERO,
            ..limits()
        };
        let mut shard = Shard::default();
        run(&mut shard, 1, "failed", true, limits);

        // A new trace starts the search for old traces and old decisions.
        shard.start(id(2), limits);

        assert!(shard.kept.is_empty());
    }

    #[test]
    fn large_held_buffers_are_dropped_by_the_caller() {
        let limits = Limits {
            max_spans_per_trace: 100,
            ..limits()
        };
        let mut shard = Shard::default();

        // A trace without an error ends with more than 32 held spans. The
        // shard returns them to the caller and keeps nothing in the pool.
        shard.start(id(1), limits);
        for _ in 0..MAX_DROP_UNDER_LOCK {
            run(&mut shard, 1, "held", false, limits);
        }
        let ended = shard.end(id(1), span(1, "root"), false, limits);
        assert_eq!(ended.drop.len(), MAX_DROP_UNDER_LOCK + 1);
        assert!(shard.pool.is_empty());

        // A trace with few held spans drops them under the lock and keeps the
        // buffer for reuse.
        shard.start(id(2), limits);
        run(&mut shard, 2, "held", false, limits);
        let ended = shard.end(id(2), span(2, "root"), false, limits);
        assert!(ended.drop.is_empty());
        assert_eq!(shard.pool.len(), 1);
    }

    #[test]
    fn large_buffers_of_removed_traces_are_dropped_by_the_caller() {
        let limits = Limits {
            max_traces_per_shard: 1,
            max_spans_per_trace: 100,
            ..limits()
        };
        let mut shard = Shard::default();
        shard.start(id(1), limits);
        for _ in 0..=MAX_DROP_UNDER_LOCK {
            run(&mut shard, 1, "held", false, limits);
        }

        // Trace 2 removes trace 1. The shard returns its held spans.
        let removed = shard.start(id(2), limits);
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].len(), MAX_DROP_UNDER_LOCK + 1);
    }

    /// Limits with room for 4 traces and 2 decisions.
    fn limits() -> Limits {
        Limits {
            max_traces_per_shard: 4,
            max_spans_per_trace: 10,
            trace_timeout: Duration::from_secs(60),
            max_kept_per_shard: 2,
        }
    }

    fn id(trace: u128) -> TraceId {
        TraceId::from(trace)
    }

    /// Starts and ends a span. Returns the names of the exported spans.
    fn run(
        shard: &mut Shard,
        trace: u128,
        name: &'static str,
        is_error: bool,
        limits: Limits,
    ) -> Vec<String> {
        shard.start(id(trace), limits);
        end(shard, trace, name, is_error, limits)
    }

    /// Ends a span. Returns the names of the exported spans.
    fn end(
        shard: &mut Shard,
        trace: u128,
        name: &'static str,
        is_error: bool,
        limits: Limits,
    ) -> Vec<String> {
        let ended = shard.end(id(trace), span(trace, name), is_error, limits);
        names(ended.export.into_iter().chain(ended.span))
    }

    fn names(spans: impl IntoIterator<Item = SpanData>) -> Vec<String> {
        spans
            .into_iter()
            .map(|span| span.name.into_owned())
            .collect()
    }

    fn span(trace: u128, name: &'static str) -> SpanData {
        SpanData {
            span_context: SpanContext::new(
                id(trace),
                SpanId::from(1u64),
                TraceFlags::default(),
                false,
                TraceState::NONE,
            ),
            parent_span_id: SpanId::INVALID,
            parent_span_is_remote: false,
            span_kind: SpanKind::Internal,
            name: Cow::Borrowed(name),
            start_time: SystemTime::UNIX_EPOCH,
            end_time: SystemTime::UNIX_EPOCH,
            attributes: Vec::new(),
            dropped_attributes_count: 0,
            events: SpanEvents::default(),
            links: SpanLinks::default(),
            status: Status::Unset,
            instrumentation_scope: InstrumentationScope::default(),
        }
    }
}
