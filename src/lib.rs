#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]

mod error;
mod processor;
mod sampler;
mod trace_processor;
mod tracestate;

pub use error::DEFAULT_STATUS_ATTRIBUTE;
pub use processor::ErrorSamplingProcessor;
pub use sampler::{AlwaysRecordSampler, DEFAULT_PRECISION, ProbabilitySampler};
pub use trace_processor::{
    DEFAULT_MAX_SPANS_PER_TRACE, DEFAULT_MAX_TRACES, DEFAULT_TRACE_TIMEOUT,
    TraceErrorSamplingProcessor,
};
