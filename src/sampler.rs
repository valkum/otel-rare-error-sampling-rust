use opentelemetry::{
    Context, KeyValue,
    trace::{Link, SpanKind, TraceContextExt, TraceId, TraceState},
};
use opentelemetry_sdk::trace::{SamplingDecision, SamplingResult, ShouldSample};

use crate::tracestate::{self, MAX_PRECISION};

/// A sampler that records the spans that its root sampler drops.
///
/// This is the `AlwaysRecord` sampler of the OpenTelemetry specification. It
/// changes a [`SamplingDecision::Drop`] of the root sampler to
/// [`SamplingDecision::RecordOnly`]. Other decisions do not change.
///
/// The SDK calls the span processors for all recorded spans. So
/// [`ErrorSamplingProcessor`](crate::ErrorSamplingProcessor) can sample a span
/// when the span ends with an error.
///
/// Use it around a parent-based sampler:
///
/// ```
/// use opentelemetry_sdk::trace::Sampler;
/// use opentelemetry_rare_error_sampling::{AlwaysRecordSampler, ProbabilitySampler};
///
/// let sampler = AlwaysRecordSampler::new(Sampler::parent_based(ProbabilitySampler::new(0.1)));
/// ```
///
/// All recorded spans use memory and CPU, also when no exporter gets them.
///
/// See <https://opentelemetry.io/docs/specs/otel/trace/sdk/#alwaysrecord>.
#[derive(Clone, Debug)]
pub struct AlwaysRecordSampler<S> {
    root: S,
}

impl<S: ShouldSample> AlwaysRecordSampler<S> {
    /// Creates a sampler that records the spans that `root` drops.
    pub fn new(root: S) -> Self {
        Self { root }
    }
}

impl<S: ShouldSample + Clone + 'static> ShouldSample for AlwaysRecordSampler<S> {
    fn should_sample(
        &self,
        parent_context: Option<&Context>,
        trace_id: TraceId,
        name: &str,
        span_kind: &SpanKind,
        attributes: &[KeyValue],
        links: &[Link],
    ) -> SamplingResult {
        let mut result =
            self.root
                .should_sample(parent_context, trace_id, name, span_kind, attributes, links);
        if result.decision == SamplingDecision::Drop {
            result.decision = SamplingDecision::RecordOnly;
        }
        result
    }
}

/// A consistent probability sampler that sets the `th` sub-key of the `ot`
/// `tracestate` entry.
///
/// This is the `ProbabilitySampler` of the OpenTelemetry specification:
///
/// - The randomness value is the `rv` sub-key of the `ot` entry when it is
///   present. Else it is the 56 least significant bits of the trace id.
/// - The sampler samples a span when the randomness value is equal to or more
///   than the rejection threshold.
/// - A sampled span gets `ot=th:<threshold>`. Backends use this value to
///   calculate the adjusted count of each span.
/// - The sampler does not examine the sampled flag of the parent. Put it in
///   a parent-based sampler to keep the decision of the parent.
///
/// The `th` value has [`DEFAULT_PRECISION`] hex digits. Probabilities near 0
/// or near 1 get more digits, as the specification recommends.
///
/// The SDK does not set the W3C random trace flag. So the sampler presumes
/// that the trace id is random.
///
/// See <https://opentelemetry.io/docs/specs/otel/trace/sdk/#probabilitysampler>.
#[derive(Clone, Debug)]
pub struct ProbabilitySampler {
    ratio: f64,
    /// The rejection threshold, the encoded threshold, and a `tracestate`
    /// that holds only `ot=th:<threshold>`. `None` when the sampler samples
    /// no span.
    sampled: Option<(u64, Box<str>, TraceState)>,
}

impl ProbabilitySampler {
    /// Creates a sampler that samples a span with the probability `ratio`.
    ///
    /// A ratio of 1 or more samples all spans. A ratio less than
    /// 2<sup>-56</sup> samples no span.
    pub fn new(ratio: f64) -> Self {
        Self::with_ratio_and_precision(ratio, DEFAULT_PRECISION)
    }

    /// Sets the number of hex digits in the `th` value, from 1 to 12.
    pub fn with_precision(self, precision: u32) -> Self {
        Self::with_ratio_and_precision(self.ratio, precision)
    }

    fn with_ratio_and_precision(ratio: f64, precision: u32) -> Self {
        let sampled = tracestate::probability_to_threshold(ratio, precision.min(MAX_PRECISION))
            .map(|threshold| {
                let encoded = tracestate::encode_threshold(threshold);
                // Root spans have an empty parent `tracestate`. Make their
                // `tracestate` one time. Then each sampled root span only
                // clones it.
                let trace_state = tracestate::with_threshold(&TraceState::NONE, &encoded)
                    .expect("an encoded threshold is a valid tracestate value");
                (threshold, encoded.into_boxed_str(), trace_state)
            });
        Self { ratio, sampled }
    }

    /// Returns the encoded `th` value, or `None` when the sampler samples no
    /// span.
    pub fn threshold(&self) -> Option<&str> {
        self.sampled.as_ref().map(|(_, encoded, _)| &**encoded)
    }
}

impl ShouldSample for ProbabilitySampler {
    fn should_sample(
        &self,
        parent_context: Option<&Context>,
        trace_id: TraceId,
        _name: &str,
        _span_kind: &SpanKind,
        _attributes: &[KeyValue],
        _links: &[Link],
    ) -> SamplingResult {
        let span = parent_context
            .filter(|cx| cx.has_active_span())
            .map(|cx| cx.span());
        let trace_state = span
            .as_ref()
            .map_or(&TraceState::NONE, |span| span.span_context().trace_state());

        let sampled = self.sampled.as_ref().filter(|(threshold, _, _)| {
            tracestate::randomness(trace_id, trace_state) >= *threshold
        });
        let Some((_, encoded, sampled_state)) = sampled else {
            return SamplingResult {
                decision: SamplingDecision::Drop,
                attributes: Vec::new(),
                trace_state: trace_state.clone(),
            };
        };
        let trace_state = if *trace_state == TraceState::NONE {
            sampled_state.clone()
        } else {
            tracestate::with_threshold(trace_state, encoded).unwrap_or_else(|| trace_state.clone())
        };
        SamplingResult {
            decision: SamplingDecision::RecordAndSample,
            attributes: Vec::new(),
            trace_state,
        }
    }
}

/// The default number of hex digits in the `th` value. The specification
/// recommends this value.
pub const DEFAULT_PRECISION: u32 = 4;

#[cfg(test)]
mod tests {
    use opentelemetry::trace::{SpanContext, SpanId, TraceFlags};
    use opentelemetry_sdk::trace::Sampler;

    use super::*;

    #[test]
    fn encodes_common_ratios() {
        // Common ratios give short thresholds.
        assert_eq!(ProbabilitySampler::new(1.0).threshold(), Some("0"));
        assert_eq!(ProbabilitySampler::new(0.5).threshold(), Some("8"));
        assert_eq!(ProbabilitySampler::new(0.1).threshold(), Some("e666"));

        // Ratios outside 0 to 1 are limited. A ratio of 0 or less, or not a
        // number, samples no span.
        assert_eq!(ProbabilitySampler::new(0.0).threshold(), None);
        assert_eq!(ProbabilitySampler::new(f64::NAN).threshold(), None);
        assert_eq!(ProbabilitySampler::new(2.0).threshold(), Some("0"));
        assert_eq!(ProbabilitySampler::new(-1.0).threshold(), None);
    }

    #[test]
    fn sets_precision() {
        let sampler = ProbabilitySampler::new(0.1);

        // The precision sets the number of hex digits. The maximum is 12
        // digits.
        assert_eq!(sampler.clone().with_precision(1).threshold(), Some("e"));
        assert_eq!(
            sampler.clone().with_precision(6).threshold(),
            Some("e66666")
        );
        assert_eq!(sampler.with_precision(20).threshold(), Some("e66666666666"));
    }

    #[test]
    fn keeps_the_parent_trace_state_on_drop() {
        let parent = Context::new().with_remote_span_context(SpanContext::new(
            TraceId::from(1u128),
            SpanId::from(1u64),
            TraceFlags::SAMPLED,
            true,
            "ot=th:8;rv:00000000000000,vendor=x".parse().unwrap(),
        ));

        let result = ProbabilitySampler::new(0.25).should_sample(
            Some(&parent),
            TraceId::from(1u128),
            "span",
            &SpanKind::Internal,
            &[],
            &[],
        );

        // The randomness 0 is below the threshold, so the sampler drops the
        // span. The tracestate of the parent does not change.
        assert_eq!(result.decision, SamplingDecision::Drop);
        assert_eq!(
            result.trace_state.header(),
            "ot=th:8;rv:00000000000000,vendor=x"
        );
    }

    #[test]
    fn always_record_changes_only_drop() {
        // A drop becomes record-only. A sample decision does not change.
        let sampler = AlwaysRecordSampler::new(Sampler::AlwaysOff);

        assert_eq!(sample(&sampler), SamplingDecision::RecordOnly);

        let sampler = AlwaysRecordSampler::new(Sampler::AlwaysOn);

        assert_eq!(sample(&sampler), SamplingDecision::RecordAndSample);
    }

    fn sample(sampler: &dyn ShouldSample) -> SamplingDecision {
        sampler
            .should_sample(
                None,
                TraceId::from(1u128),
                "span",
                &SpanKind::Internal,
                &[],
                &[],
            )
            .decision
    }
}
