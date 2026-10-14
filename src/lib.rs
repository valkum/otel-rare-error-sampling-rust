#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]

mod error;
mod processor;
mod sampler;
mod tracestate;

pub use error::DEFAULT_STATUS_ATTRIBUTE;
pub use processor::ErrorSamplingProcessor;
pub use sampler::{AlwaysRecordSampler, DEFAULT_PRECISION, ProbabilitySampler};
