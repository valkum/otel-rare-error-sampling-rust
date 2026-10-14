# opentelemetry-rare-error-sampling

Keep every error span, and sample the rest.

Head sampling decides at the start of a trace, so it cannot know if the trace
will fail. This crate records all spans. When a span ends with an error, it
exports the span, also if head sampling dropped the trace. The approach comes
from the OpenTelemetry .NET guide
[Tail-based sampling](https://opentelemetry.io/docs/languages/dotnet/traces/tail-based-sampling/).

## Usage

```rust
use opentelemetry::trace::{Span, Status, Tracer, TracerProvider};
use opentelemetry_sdk::trace::{
    BatchSpanProcessor, InMemorySpanExporter, Sampler, SdkTracerProvider,
};
use opentelemetry_rare_error_sampling::{
    AlwaysRecordSampler, ErrorSamplingProcessor, ProbabilitySampler,
};

let exporter = InMemorySpanExporter::default();
let provider = SdkTracerProvider::builder()
    // Sample 10% of the traces, and record the other spans.
    .with_sampler(AlwaysRecordSampler::new(Sampler::parent_based(
        ProbabilitySampler::new(0.1),
    )))
    // Export all error spans.
    .with_span_processor(ErrorSamplingProcessor::new(
        BatchSpanProcessor::builder(exporter.clone()).build(),
    ))
    .build();

let mut span = provider.tracer("app").start("request");
span.set_status(Status::error("upstream timeout"));
span.end();

provider.force_flush().unwrap();
let spans = exporter.get_finished_spans().unwrap();
assert!(spans[0].span_context.is_sampled());
```

`ProbabilitySampler` and `AlwaysRecordSampler` are the samplers of the
[OpenTelemetry SDK specification](https://opentelemetry.io/docs/specs/otel/trace/sdk/).
The Rust SDK does not have them yet
([opentelemetry-rust#3195](https://github.com/open-telemetry/opentelemetry-rust/issues/3195)).
When it adds them, change the imports. `ErrorSamplingProcessor` stays.

## Adjusted counts

Head-sampled spans keep the `th` of the sampler, for example `ot=th:c` at
25%. Error spans that only the processor exports get no `th`, because they
are not a random sample
([OTEP 235](https://github.com/open-telemetry/opentelemetry-specification/blob/main/oteps/trace/0235-sampling-threshold-in-trace-state.md#downstream-samplers)).

## Things to know

- The processor decides for each span, not for each trace. An error span can
  arrive without its parent.
- All spans are recorded, so unsampled spans still cost memory and CPU.
- Always put the `BatchSpanProcessor` in `ErrorSamplingProcessor`. The batch
  processor alone exports all recorded spans, also the unsampled ones.

## License

Apache-2.0 or MIT, at your option.
