use std::{
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
///
/// An error span is always exported, also when the processor removed its
/// trace.
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
            self.shard(sc.trace_id()).start(sc.trace_id(), self.limits);
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
        let (span, held) = self
            .shard(trace_id)
            .end(trace_id, span, is_error, self.limits);
        // Call the inner processor without the lock.
        for mut span in held.into_iter().chain(span) {
            error::mark_sampled(&mut span);
            self.inner.on_end(span);
        }
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
}

/// The local traces of one shard.
///
/// A shard holds few traces: the traces in flight divided by the number of
/// shards, and at most the trace limit. So a linear search in a `Vec` is
/// faster than a hash map, and the shard needs no separate queue for the age
/// of the traces.
#[derive(Debug, Default)]
struct Shard {
    traces: Vec<LocalTrace>,
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

impl Shard {
    fn start(&mut self, trace_id: TraceId, limits: Limits) {
        if let Some(trace) = self.traces.iter_mut().find(|t| t.trace_id == trace_id) {
            trace.open += 1;
            return;
        }
        let now = Instant::now();
        self.evict(now, limits);
        let held = self.pool.pop().unwrap_or_default();
        self.traces.push(LocalTrace {
            trace_id,
            created: now,
            open: 1,
            keep: false,
            held,
        });
    }

    /// Records the end of a span. Returns the span when the processor must
    /// export it, and the held spans to export.
    fn end(
        &mut self,
        trace_id: TraceId,
        span: SpanData,
        is_error: bool,
        limits: Limits,
    ) -> (Option<SpanData>, Vec<SpanData>) {
        let Some(index) = self.traces.iter().position(|t| t.trace_id == trace_id) else {
            // The processor removed the trace. Export only error spans.
            return (is_error.then_some(span), Vec::new());
        };
        let trace = &mut self.traces[index];
        trace.open = trace.open.saturating_sub(1);

        let mut held = Vec::new();
        let span = if is_error || trace.keep {
            if !trace.keep {
                trace.keep = true;
                held = std::mem::take(&mut trace.held);
            }
            Some(span)
        } else {
            if trace.held.len() < limits.max_spans_per_trace {
                trace.held.push(span);
            }
            None
        };

        // Keep a trace with an error until the timeout, so that later spans
        // of the trace are also exported.
        if trace.open == 0 && !trace.keep {
            self.remove(index);
        }
        (span, held)
    }

    /// Removes the local traces that are too old. Then removes the oldest
    /// local trace when the shard is full.
    ///
    /// The search for old traces occurs at most one time in each 1/8 of the
    /// timeout. So a trace can stay up to 1/8 of the timeout longer.
    fn evict(&mut self, now: Instant, limits: Limits) {
        if self.next_scan.is_none_or(|next| now >= next) {
            // A day limits the interval, so that a very long timeout cannot
            // cause an overflow.
            let interval = (limits.trace_timeout / 8).min(Duration::from_secs(86_400));
            self.next_scan = now.checked_add(interval);
            let mut index = 0;
            while index < self.traces.len() {
                if now.duration_since(self.traces[index].created) >= limits.trace_timeout {
                    self.remove(index);
                } else {
                    index += 1;
                }
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
                self.remove(oldest);
            }
        }
    }

    /// Removes a local trace, drops its held spans, and keeps the empty buffer
    /// for reuse.
    fn remove(&mut self, index: usize) {
        let mut held = self.traces.swap_remove(index).held;
        if self.pool.len() < POOL_SIZE && held.capacity() <= POOL_MAX_CAPACITY {
            held.clear();
            self.pool.push(held);
        }
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

/// The number of shards. Each shard has its own lock.
const SHARDS: usize = 64;

/// The maximum number of empty span buffers that a shard keeps for reuse.
const POOL_SIZE: usize = 16;

/// The maximum capacity of a span buffer that a shard keeps for reuse.
const POOL_MAX_CAPACITY: usize = 64;

#[cfg(test)]
mod tests {
    use super::*;

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
}
