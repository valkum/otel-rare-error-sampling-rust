# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/valkum/otel-rare-error-sampling-rust/releases/tag/v0.1.0) - 2026-10-15

### Added

- *(processor)* add TraceErrorSamplingProcessor
- *(processor)* sample error spans that head sampling dropped
- *(sampler)* add AlwaysRecord and probability samplers

### Other

- *(processor)* keep error decisions in an LRU and reuse span buffers
- *(bench)* add large-trace, open-trace and contention benchmarks
- cover TraceErrorSamplingProcessor
- *(sampler)* prebuild root tracestate and borrow parent state
- *(bench)* add hot path benchmarks
- cover the BatchSpanProcessor path
- *(example)* add BatchSpanProcessor example
- add test, lint, audit and release workflows
- describe usage, ot=th handling and limits
- add integration tests for samplers and processor
- scaffold crate
