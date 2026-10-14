#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]

mod sampler;
mod tracestate;

pub use sampler::{AlwaysRecordSampler, DEFAULT_PRECISION, ProbabilitySampler};
