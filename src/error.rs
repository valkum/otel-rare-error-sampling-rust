use opentelemetry::{
    Key, Value,
    trace::{SpanContext, Status},
};
use opentelemetry_sdk::trace::SpanData;

use crate::tracestate;

/// Finds error spans.
#[derive(Debug)]
pub(crate) struct ErrorRule {
    status_attribute: Option<Key>,
}

impl ErrorRule {
    pub(crate) fn set_status_attribute(&mut self, key: Option<Key>) {
        self.status_attribute = key;
    }

    /// Returns `true` when the status of the span is [`Status::Error`], or
    /// when the status attribute has the string value `"ERROR"`.
    pub(crate) fn is_error(&self, span: &SpanData) -> bool {
        if matches!(span.status, Status::Error { .. }) {
            return true;
        }
        let Some(key) = &self.status_attribute else {
            return false;
        };
        span.attributes.iter().any(|kv| {
            kv.key.as_str() == key.as_str()
                && matches!(&kv.value, Value::String(value) if value.as_str() == ERROR_STATUS_VALUE)
        })
    }
}

impl Default for ErrorRule {
    fn default() -> Self {
        Self {
            status_attribute: Some(Key::from_static_str(DEFAULT_STATUS_ATTRIBUTE)),
        }
    }
}

/// Samples a span that head sampling did not sample, and removes the `th`
/// value.
///
/// The processor selects such a span for a reason that is not random. So
/// the adjusted count of the span is not defined, and the span must not have
/// a `th` value. Refer to the rules for downstream samplers in OTEP 235:
/// <https://github.com/open-telemetry/opentelemetry-specification/blob/main/oteps/trace/0235-sampling-threshold-in-trace-state.md>.
pub(crate) fn mark_sampled(span: &mut SpanData) {
    let sc = &span.span_context;
    let trace_state = tracestate::without_threshold(sc.trace_state());
    if trace_state.is_none() && sc.is_sampled() {
        return;
    }
    span.span_context = SpanContext::new(
        sc.trace_id(),
        sc.span_id(),
        sc.trace_flags().with_sampled(true),
        sc.is_remote(),
        trace_state.unwrap_or_else(|| sc.trace_state().clone()),
    );
}

/// The default attribute that can mark a span as an error.
pub const DEFAULT_STATUS_ATTRIBUTE: &str = "otel.status_code";

/// The value of the status attribute that marks a span as an error.
const ERROR_STATUS_VALUE: &str = "ERROR";
