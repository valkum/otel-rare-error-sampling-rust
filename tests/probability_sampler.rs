mod common;

use common::{HIGH_RANDOMNESS, LOW_RANDOMNESS, remote_parent};
use opentelemetry::trace::{SpanKind, TraceId};
use opentelemetry_rare_error_sampling::ProbabilitySampler;
use opentelemetry_sdk::trace::{SamplingDecision, ShouldSample};

#[test]
fn samples_by_trace_id_randomness() {
    let sampler = ProbabilitySampler::new(0.25);

    // High randomness is above the threshold. The sampler samples the span and
    // writes the threshold.
    let sampled = decide(&sampler, HIGH_RANDOMNESS, None);
    assert_eq!(sampled.decision, SamplingDecision::RecordAndSample);
    assert_eq!(sampled.trace_state.header(), "ot=th:c");

    // Low randomness is below the threshold. The sampler drops the span and
    // writes no threshold.
    let dropped = decide(&sampler, LOW_RANDOMNESS, None);
    assert_eq!(dropped.decision, SamplingDecision::Drop);
    assert_eq!(dropped.trace_state.header(), "");
}

#[test]
fn threshold_is_inclusive() {
    let sampler = ProbabilitySampler::new(0.25);

    // The randomness 0xc0000000000000 is equal to the threshold.
    let at = TraceId::from_bytes([0, 0, 0, 0, 0, 0, 0, 0, 0, 0xc0, 0, 0, 0, 0, 0, 0]);
    let below = TraceId::from_bytes([
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0xbf, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
    ]);

    // Randomness equal to the threshold is sampled. Randomness one below it is
    // dropped.
    assert_eq!(
        decide(&sampler, at, None).decision,
        SamplingDecision::RecordAndSample
    );
    assert_eq!(
        decide(&sampler, below, None).decision,
        SamplingDecision::Drop
    );
}

#[test]
fn explicit_randomness_overrides_the_trace_id() {
    let sampler = ProbabilitySampler::new(0.25);

    // High `rv` samples the span, also with a trace id of low randomness. `rv`
    // and the vendor entry stay.
    let parent = remote_parent(LOW_RANDOMNESS, true, "ot=rv:ffffffffffffff,vendor=x");

    let result = decide(&sampler, LOW_RANDOMNESS, Some(&parent));
    assert_eq!(result.decision, SamplingDecision::RecordAndSample);
    assert_eq!(
        result.trace_state.header(),
        "ot=th:c;rv:ffffffffffffff,vendor=x"
    );

    // Low `rv` drops the span, also with a trace id of high randomness.
    let parent = remote_parent(HIGH_RANDOMNESS, true, "ot=rv:00000000000000");

    let result = decide(&sampler, HIGH_RANDOMNESS, Some(&parent));
    assert_eq!(result.decision, SamplingDecision::Drop);
    assert_eq!(result.trace_state.header(), "ot=rv:00000000000000");
}

#[test]
fn malformed_randomness_falls_back_to_the_trace_id() {
    let sampler = ProbabilitySampler::new(0.25);
    let parent = remote_parent(HIGH_RANDOMNESS, true, "ot=rv:zz");

    // `rv:zz` is not valid, so the sampler uses the high randomness of the
    // trace id.
    let result = decide(&sampler, HIGH_RANDOMNESS, Some(&parent));
    assert_eq!(result.decision, SamplingDecision::RecordAndSample);
}

#[test]
fn sampled_share_matches_the_ratio() {
    const TRACES: u64 = 20_000;

    for ratio in [0.01, 0.1, 0.25, 0.5, 0.9] {
        let sampler = ProbabilitySampler::new(ratio);

        let sampled = random_trace_ids(TRACES)
            .filter(|&trace_id| {
                decide(&sampler, trace_id, None).decision == SamplingDecision::RecordAndSample
            })
            .count();

        // The share of sampled traces is close to the ratio.
        let share = sampled as f64 / TRACES as f64;

        // Four standard deviations of the binomial distribution.
        let tolerance = 4.0 * (ratio * (1.0 - ratio) / TRACES as f64).sqrt();
        assert!(
            (share - ratio).abs() < tolerance,
            "ratio {ratio}: sampled share {share}, tolerance {tolerance}"
        );
    }
}

#[test]
fn threshold_uses_spec_precision() {
    // The default precision is 4 hex digits, as in the examples of the
    // specification.
    assert_eq!(ProbabilitySampler::new(0.1).threshold(), Some("e666"));
    assert_eq!(ProbabilitySampler::new(1.0 / 3.0).threshold(), Some("aaab"));

    // A small probability gets one more digit for each factor of 16.
    assert_eq!(ProbabilitySampler::new(0.01).threshold(), Some("fd70a"));
    assert_eq!(ProbabilitySampler::new(0.001).threshold(), Some("ffbe77"));

    // `with_precision` sets more digits.
    assert_eq!(
        ProbabilitySampler::new(0.1).with_precision(12).threshold(),
        Some("e66666666666")
    );
}

fn decide(
    sampler: &ProbabilitySampler,
    trace_id: TraceId,
    parent: Option<&opentelemetry::Context>,
) -> opentelemetry_sdk::trace::SamplingResult {
    sampler.should_sample(parent, trace_id, "span", &SpanKind::Internal, &[], &[])
}

/// Returns `count` random trace ids from SplitMix64. The trace ids are the
/// same in each run.
fn random_trace_ids(count: u64) -> impl Iterator<Item = TraceId> {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    (0..count).map(move |_| {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^= z >> 31;
        TraceId::from(u128::from(z) | (1 << 64))
    })
}
