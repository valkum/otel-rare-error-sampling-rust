//! Functions for the OpenTelemetry `ot` entry in the W3C `tracestate`.
//!
//! See <https://opentelemetry.io/docs/specs/otel/trace/tracestate-handling/>.

use opentelemetry::trace::{TraceId, TraceState};

/// Returns the explicit randomness value (`rv`) of the `ot` entry when it is
/// exactly 14 hex digits.
pub(crate) fn explicit_randomness(trace_state: &TraceState) -> Option<u64> {
    let rv = sub_key(trace_state.get(OT_KEY)?, RV_PREFIX)?;
    if rv.len() != 14 {
        return None;
    }
    u64::from_str_radix(rv, 16).ok()
}

/// Returns the randomness value of a trace. This is the explicit `rv` when it
/// is present. Else it is the 56 least significant bits of the trace id.
pub(crate) fn randomness(trace_id: TraceId, trace_state: &TraceState) -> u64 {
    explicit_randomness(trace_state).unwrap_or_else(|| {
        let bytes = trace_id.to_bytes();
        let mut low = [0u8; 8];
        low.copy_from_slice(&bytes[8..]);
        u64::from_be_bytes(low) & RANDOMNESS_MASK
    })
}

/// Converts a probability into a rejection threshold. The function rounds the
/// threshold to `precision` hex digits.
///
/// A probability near 0 or near 1 gets one more digit for each factor of 16,
/// up to 14 digits. The function returns `None` when the probability is less
/// than [`MIN_PROBABILITY`] or is not a number.
///
/// The function is the same as `ProbabilityToThresholdWithPrecision` in the
/// OpenTelemetry Collector:
/// <https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/pkg/sampling/probability.go>.
/// The reference code of the specification adds digits only near 0.
pub(crate) fn probability_to_threshold(probability: f64, precision: u32) -> Option<u64> {
    if probability.is_nan() || probability < MIN_PROBABILITY {
        return None;
    }
    if probability >= 1.0 {
        return Some(0);
    }
    let extra = |x: f64| (-frexp_exponent(x)).max(0) as u32 / 4;
    let precision =
        (precision + extra(probability).max(extra(1.0 - probability))).clamp(1, THRESHOLD_DIGITS);
    let scaled = (probability * MAX_THRESHOLD as f64).round() as u64;
    let exact = MAX_THRESHOLD - scaled.clamp(1, MAX_THRESHOLD);
    let shift = 4 * (THRESHOLD_DIGITS - precision);
    if shift == 0 {
        return Some(exact);
    }
    let half = 1 << (shift - 1);
    let rounded = (exact + half) >> shift << shift;
    // A threshold of 2^56 samples no span. So round down in this case.
    Some(if rounded < MAX_THRESHOLD {
        rounded
    } else {
        exact >> shift << shift
    })
}

/// Encodes a rejection threshold as 14 lowercase hex digits, and removes the
/// trailing zeros. A threshold of zero gives `"0"`.
pub(crate) fn encode_threshold(threshold: u64) -> String {
    debug_assert!(threshold < MAX_THRESHOLD);
    let mut encoded = format!("{threshold:014x}");
    let trimmed = encoded.trim_end_matches('0').len().max(1);
    encoded.truncate(trimmed);
    encoded
}

/// Returns a copy of `trace_state` in which the `ot` entry has `th:<th>`.
///
/// The other `ot` sub-keys and the entries of other vendors do not change.
/// When the new value is not valid, for example too long, the function
/// removes `th` instead. A wrong threshold is worse than no threshold. The
/// function returns `None` when nothing changes.
pub(crate) fn with_threshold(trace_state: &TraceState, th: &str) -> Option<TraceState> {
    let ot = trace_state.get(OT_KEY).unwrap_or_default();
    if sub_key(ot, TH_PREFIX) == Some(th) {
        return None;
    }
    let mut value = String::with_capacity(TH_PREFIX.len() + th.len() + 1 + ot.len());
    value.push_str(TH_PREFIX);
    value.push_str(th);
    for entry in ot
        .split(';')
        .filter(|entry| !entry.is_empty() && !entry.starts_with(TH_PREFIX))
    {
        value.push(';');
        value.push_str(entry);
    }
    trace_state
        .insert(OT_KEY, value)
        .ok()
        .or_else(|| without_threshold(trace_state))
}

/// Returns a copy of `trace_state` without the `th` sub-key of the `ot`
/// entry.
///
/// The other `ot` sub-keys and the entries of other vendors do not change.
/// The function removes the `ot` entry when it has no other sub-keys. It
/// returns `None` when `trace_state` has no `th` value.
pub(crate) fn without_threshold(trace_state: &TraceState) -> Option<TraceState> {
    let ot = trace_state.get(OT_KEY)?;
    sub_key(ot, TH_PREFIX)?;
    let mut value = String::with_capacity(ot.len());
    for entry in ot
        .split(';')
        .filter(|entry| !entry.is_empty() && !entry.starts_with(TH_PREFIX))
    {
        if !value.is_empty() {
            value.push(';');
        }
        value.push_str(entry);
    }
    if value.is_empty() {
        trace_state.delete(OT_KEY).ok()
    } else {
        trace_state.insert(OT_KEY, value).ok()
    }
}

fn sub_key<'a>(ot: &'a str, prefix: &str) -> Option<&'a str> {
    ot.split(';').find_map(|entry| entry.strip_prefix(prefix))
}

/// Returns the binary exponent `e` of a positive normal `x`. The exponent
/// gives `x = m * 2^e`, with `m` in `[0.5, 1)`.
fn frexp_exponent(x: f64) -> i32 {
    ((x.to_bits() >> 52) & 0x7ff) as i32 - 1022
}

pub(crate) const OT_KEY: &str = "ot";
const TH_PREFIX: &str = "th:";
const RV_PREFIX: &str = "rv:";

/// The rejection threshold that samples no span (2^56).
pub(crate) const MAX_THRESHOLD: u64 = 1 << RANDOMNESS_BITS;
/// The number of bits in the randomness value and in the rejection threshold.
const RANDOMNESS_BITS: u32 = 56;
const RANDOMNESS_MASK: u64 = MAX_THRESHOLD - 1;

/// The maximum precision that a caller can set. The reference code of the
/// specification uses this value.
pub(crate) const MAX_PRECISION: u32 = 12;

/// The lowest probability that samples a span.
pub(crate) const MIN_PROBABILITY: f64 = 1.0 / MAX_THRESHOLD as f64;

/// The number of hex digits in an encoded threshold.
const THRESHOLD_DIGITS: u32 = 14;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_threshold() {
        // The encoding removes trailing zeros, but keeps leading zeros.
        assert_eq!(encode_threshold(0), "0");
        assert_eq!(encode_threshold(MAX_THRESHOLD / 2), "8");
        assert_eq!(encode_threshold(MAX_THRESHOLD / 4 * 3), "c");
        assert_eq!(encode_threshold(1), "00000000000001");
        assert_eq!(encode_threshold(RANDOMNESS_MASK), "ffffffffffffff");
    }

    /// The rows of the example table in the specification, with the
    /// precisions 3, 4 and 5.
    #[test]
    fn matches_specification_examples() {
        let rows: [(f64, [&str; 3]); 6] = [
            (1.0, ["0", "0", "0"]),
            (0.5, ["8", "8", "8"]),
            (0.25, ["c", "c", "c"]),
            (1.0 / 3.0, ["aab", "aaab", "aaaab"]),
            (0.1, ["e66", "e666", "e6666"]),
            (0.01, ["fd71", "fd70a", "fd70a4"]),
        ];
        for (probability, expected) in rows {
            for (precision, expected) in (3..=5).zip(expected) {
                assert_eq!(
                    encoded(probability, precision).as_deref(),
                    Some(expected),
                    "probability {probability}, precision {precision}"
                );
            }
        }
    }

    #[test]
    fn probability_near_one_keeps_its_precision() {
        // 1 - 0.999999 is close to 16^-5, so the threshold gets 4 more digits.
        // The value stays close to the real rejection probability of 1e-6.
        let threshold = probability_to_threshold(0.999_999, 4).unwrap();
        assert_eq!(encode_threshold(threshold), "000010c7");

        let rejection = threshold as f64 / MAX_THRESHOLD as f64;
        assert!((rejection - 1e-6).abs() < 1e-9, "{rejection}");
    }

    #[test]
    fn handles_probability_limits() {
        // Probabilities that are too small or not a number sample no span.
        // Probabilities above 1 sample all spans.
        assert_eq!(probability_to_threshold(0.0, 4), None);
        assert_eq!(probability_to_threshold(f64::NAN, 4), None);
        assert_eq!(probability_to_threshold(MIN_PROBABILITY / 2.0, 4), None);
        assert_eq!(probability_to_threshold(2.0, 4), Some(0));

        // The smallest probability gives a threshold below the maximum, with
        // all 14 digits.
        let smallest = probability_to_threshold(MIN_PROBABILITY, 4).unwrap();
        assert!(smallest < MAX_THRESHOLD);
        assert_eq!(encode_threshold(smallest), "ffffffffffffff");
    }

    #[test]
    fn reads_sub_keys() {
        let state = ts("vendor=x,ot=rv:0123456789abcd;th:c");

        // The sub-keys are found in any order. An `rv` without 14 digits is not
        // valid.
        assert_eq!(sub_key(state.get(OT_KEY).unwrap(), TH_PREFIX), Some("c"));
        assert_eq!(explicit_randomness(&state), Some(0x0123456789abcd));
        assert_eq!(explicit_randomness(&ts("ot=rv:123")), None);
    }

    #[test]
    fn randomness_falls_back_to_trace_id() {
        let trace_id = TraceId::from(0xffff_ffff_ffff_ffff_ff12_3456_789a_bcdeu128);

        // Without `rv`, the randomness is the 56 low bits of the trace id. With
        // `rv`, it is the `rv` value.
        assert_eq!(randomness(trace_id, &TraceState::NONE), 0x12_3456_789a_bcde);
        assert_eq!(randomness(trace_id, &ts("ot=rv:00000000000001")), 1);
    }

    #[test]
    fn sets_threshold_and_keeps_other_entries() {
        let state = ts("vendor=x,ot=th:c;rv:0123456789abcd;p:8");

        // `th` changes, and the other sub-keys and vendors stay. The `ot` entry
        // moves to the front.
        let updated = with_threshold(&state, "0").unwrap();
        assert_eq!(updated.header(), "ot=th:0;rv:0123456789abcd;p:8,vendor=x");
    }

    #[test]
    fn adds_ot_entry() {
        // An empty tracestate gets a new `ot` entry.
        let updated = with_threshold(&TraceState::NONE, "0").unwrap();
        assert_eq!(updated.header(), "ot=th:0");
    }

    #[test]
    fn removes_threshold() {
        let state = ts("vendor=x,ot=th:c;rv:0123456789abcd");

        // Only `th` goes. `rv` and the vendor entry stay.
        let updated = without_threshold(&state).unwrap();
        assert_eq!(updated.header(), "ot=rv:0123456789abcd,vendor=x");

        // When `th` is the only sub-key, the whole `ot` entry goes.
        let updated = without_threshold(&ts("vendor=x,ot=th:c")).unwrap();
        assert_eq!(updated.header(), "vendor=x");

        // Without `th`, there is nothing to change.
        assert!(without_threshold(&ts("vendor=x,ot=rv:0123456789abcd")).is_none());
        assert!(without_threshold(&TraceState::NONE).is_none());
    }

    #[test]
    fn removes_threshold_when_the_new_value_is_too_long() {
        // The `ot` value has 254 characters. With `th:e666`, it has 257
        // characters, which is more than the limit of 256.
        let state = ts(&format!("ot=th:8;x:{}", "a".repeat(247)));
        let updated = with_threshold(&state, "e666").unwrap();
        assert_eq!(
            updated.get(OT_KEY),
            Some(format!("x:{}", "a".repeat(247)).as_str())
        );
    }

    #[test]
    fn skips_unchanged_threshold() {
        // The same threshold needs no new tracestate.
        assert!(with_threshold(&ts("ot=rv:0123456789abcd;th:0"), "0").is_none());
    }

    fn ts(header: &str) -> TraceState {
        header.parse().unwrap()
    }

    fn encoded(probability: f64, precision: u32) -> Option<String> {
        probability_to_threshold(probability, precision).map(encode_threshold)
    }
}
