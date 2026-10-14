use std::time::Duration;

use opentelemetry::{Context, Key};
use opentelemetry_sdk::{
    Resource,
    error::OTelSdkResult,
    trace::{Span, SpanData, SpanProcessor},
};

use crate::error::{self, ErrorRule};

/// A span processor that also exports the error spans that head sampling
/// dropped.
///
/// Wrap the processor that exports the spans, for example a
/// `BatchSpanProcessor`. Use [`AlwaysRecordSampler`](crate::AlwaysRecordSampler),
/// so that the SDK records the spans that head sampling does not sample.
///
/// - Sampled spans pass through unchanged.
/// - An error span that is not sampled gets the sampled flag and loses its
///   `th` value ([OTEP 235](https://github.com/open-telemetry/opentelemetry-specification/blob/main/oteps/trace/0235-sampling-threshold-in-trace-state.md#downstream-samplers)).
/// - The processor drops all other spans.
///
/// A span is an error span when its status is
/// [`Status::Error`](opentelemetry::trace::Status::Error), or when the status
/// attribute ([`DEFAULT_STATUS_ATTRIBUTE`](crate::DEFAULT_STATUS_ATTRIBUTE)) is
/// `"ERROR"`.
///
/// The processor decides for each span, thus an error span can arrive without
/// its parent.
///
/// Wrap each processor that exports spans.
#[derive(Debug)]
pub struct ErrorSamplingProcessor<P> {
    inner: P,
    rule: ErrorRule,
}

impl<P: SpanProcessor> ErrorSamplingProcessor<P> {
    /// Creates a processor that sends the sampled spans to `inner`.
    pub fn new(inner: P) -> Self {
        Self {
            inner,
            rule: ErrorRule::default(),
        }
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
}

impl<P: SpanProcessor> SpanProcessor for ErrorSamplingProcessor<P> {
    fn on_start(&self, span: &mut Span, cx: &Context) {
        self.inner.on_start(span, cx);
    }

    fn on_end(&self, mut span: SpanData) {
        if !span.span_context.is_sampled() {
            if !self.rule.is_error(&span) {
                return;
            }
            error::mark_sampled(&mut span);
        }
        self.inner.on_end(span);
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}
