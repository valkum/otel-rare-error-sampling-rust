# opentelemetry-rare-error-sampling

Keep every error, and sample the rest.

Head sampling decides at the start of a trace, so it cannot know if the trace
will fail. This crate records all spans and decides again when they end. If a
span fails, it is exported, also when head sampling dropped its trace.

## Two processors

- `ErrorSamplingProcessor` exports each error span on its own. It holds
  nothing in memory, but an error span can arrive without its parent. This is
  the approach of the OpenTelemetry .NET guide
  [Tail-based sampling](https://opentelemetry.io/docs/languages/dotnet/traces/tail-based-sampling/).
- `TraceErrorSamplingProcessor` holds the spans of each trace until the trace
  ends in this process. If one span fails, it exports all of them. This costs
  memory, and it has limits for the number of traces, the spans for each
  trace, and the time. This is based on [Datadog: Error sampling rate](https://docs.datadoghq.com/tracing/guide/ingestion_sampling_use_cases/#keeping-certain-types-of-traces).

Both only see the spans of this process. For complete traces across services,
use tail sampling in the OpenTelemetry Collector.

## Usage

```rust
use opentelemetry::trace::{Span, Status, Tracer, TracerProvider};
use opentelemetry_sdk::trace::{
    BatchSpanProcessor, InMemorySpanExporter, Sampler, SdkTracerProvider,
};
use opentelemetry_rare_error_sampling::{
    AlwaysRecordSampler, ProbabilitySampler, TraceErrorSamplingProcessor,
};

let exporter = InMemorySpanExporter::default();
let provider = SdkTracerProvider::builder()
    // Sample 10% of the traces, and record the other spans.
    .with_sampler(AlwaysRecordSampler::new(Sampler::parent_based(
        ProbabilitySampler::new(0.1),
    )))
    // Export all spans of a trace when one of them fails.
    .with_span_processor(TraceErrorSamplingProcessor::new(
        BatchSpanProcessor::builder(exporter.clone()).build(),
    ))
    .build();

let tracer = provider.tracer("app");
let mut request = tracer.start("request");
request.set_status(Status::error("upstream timeout"));
request.end();

provider.force_flush().unwrap();
let spans = exporter.get_finished_spans().unwrap();
assert!(spans[0].span_context.is_sampled());
```

`ProbabilitySampler` and `AlwaysRecordSampler` are the samplers of the
[OpenTelemetry SDK specification](https://opentelemetry.io/docs/specs/otel/trace/sdk/).
The Rust SDK does not have them yet
([opentelemetry-rust#3195](https://github.com/open-telemetry/opentelemetry-rust/issues/3195)).
When it adds them, change the imports. The processors stay.

## Adjusted counts

Head-sampled spans keep the `th` of the sampler, for example `ot=th:c` at
25%. Spans that a processor exports only because of an error get no `th`,
because they are not a random sample
([OTEP 235](https://github.com/open-telemetry/opentelemetry-specification/blob/main/oteps/trace/0235-sampling-threshold-in-trace-state.md#downstream-samplers)).

## Things to know

- All spans are recorded, so unsampled spans still cost memory and CPU.
- Always put the `BatchSpanProcessor` in one of the processors. The batch
  processor alone exports all recorded spans, also the unsampled ones.

## License

Apache-2.0 or MIT, at your option.
