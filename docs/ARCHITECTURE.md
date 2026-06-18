# Ferro-Stash Architecture

## System Overview

Ferro-Stash is a Rust reimplementation of Logstash, the log processing pipeline from the Elastic Stack. It processes events through a three-stage pipeline: input, filter, output.

```
                         Ferro-Stash Pipeline
  ┌─────────────────────────────────────────────────────────────────┐
  │                                                                 │
  │  ┌─────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐  │
  │  │  INPUT   │───>│  CODEC   │───>│  FILTER  │───>│  OUTPUT   │  │
  │  │ plugins  │    │ (decode) │    │ plugins  │    │ plugins   │  │
  │  └─────────┘    └──────────┘    └──────────┘    └──────────┘  │
  │   stdin          json            grok             stdout       │
  │   file           plain           mutate           elasticsearch│
  │   tcp            rubydebug       date             file         │
  │   udp            line            json             tcp          │
  │   http           multiline       kv               kafka        │
  │   beats          msgpack         drop             http         │
  │   kafka                          clone                         │
  │   syslog                         dissect                       │
  │                                  ruby (Artichoke)              │
  │                                  geoip                         │
  │                                  sleep                         │
  │                                                                 │
  │  ┌─────────────────────────────────────────────────────────┐   │
  │  │              tokio async runtime                         │   │
  │  │         (mpsc channels + backpressure)                   │   │
  │  └─────────────────────────────────────────────────────────┘   │
  └─────────────────────────────────────────────────────────────────┘
```

## Crate Dependency Graph

```
ferro-stash-cli
├── ferro-stash-core
│   └── (event model, pipeline orchestration, traits)
├── ferro-stash-config
│   └── ferro-stash-core
├── ferro-stash-input
│   ├── ferro-stash-core
│   └── ferro-stash-codec
├── ferro-stash-filter
│   ├── ferro-stash-core
│   └── ferro-stash-ruby (optional, for ruby filter)
├── ferro-stash-output
│   ├── ferro-stash-core
│   └── ferro-stash-codec
├── ferro-stash-codec
│   └── ferro-stash-core
└── ferro-stash-ruby
    └── ferro-stash-core
```

### Crate Responsibilities

| Crate | Purpose |
|-------|---------|
| `ferro-stash-core` | Event model, plugin traits, pipeline orchestration, conditions, buffering, DLQ, metrics |
| `ferro-stash-config` | Logstash DSL parser and YAML config parser, config validation |
| `ferro-stash-input` | Input plugins (15 registered; some — kafka/redis/s3 — are stubs) |
| `ferro-stash-filter` | Filter plugins (29 registered; geoip/dns/elasticsearch are stubs) |
| `ferro-stash-output` | Output plugins (11 registered; kafka/redis/s3/datadog are stubs) |
| `ferro-stash-codec` | Codec implementations (21 registered) |
| `ferro-stash-cli` | `ferro-stash` binary: CLI, signal handling, `_node/*` metrics API |
| `ferro-stash-ruby` | Artichoke (mruby) Ruby interpreter bridge for the ruby filter (local fork, path dep) |
| `ferro-script` | Native Painless-style scripting engine (Cranelift JIT) for the `script` filter/codec |
| `ferro-stash-e2e` | Integration / Logstash-parity test harness |

(See the README and Compatibility Matrix for the authoritative per-plugin
status, including which plugins are stubs.)

## Plugin Trait Architecture

All plugins implement phase-specific traits defined in `ferro-stash-core`.

### InputPlugin

```rust
#[async_trait]
pub trait InputPlugin: Send + Sync {
    async fn start(&mut self, sender: EventSender) -> Result<()>;
    async fn stop(&mut self) -> Result<()>;
    fn plugin_name(&self) -> &'static str;
}
```

Inputs run as independent tokio tasks, pushing events into an `mpsc` channel.

### FilterPlugin

```rust
pub trait FilterPlugin: Send + Sync {
    fn filter(&self, event: &mut Event) -> FilterResult;
    fn plugin_name(&self) -> &'static str;
}
```

Filters are applied sequentially in config order. `FilterResult` signals whether the event should continue, be dropped, or be cloned into additional events.

### OutputPlugin

```rust
#[async_trait]
pub trait OutputPlugin: Send + Sync {
    async fn output(&mut self, events: &[Event]) -> Result<()>;
    async fn flush(&mut self) -> Result<()>;
    fn plugin_name(&self) -> &'static str;
}
```

Outputs receive batches of events. Batching is handled by the pipeline orchestrator to amortize I/O costs.

## Event Model

Events are the central data structure flowing through the pipeline.

### EventValue

```rust
pub enum EventValue {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
    Array(Vec<EventValue>),
    Map(BTreeMap<String, EventValue>),
    Timestamp(DateTime<Utc>),
    Bytes(Vec<u8>),
}
```

### Event Structure

Each event contains:

- **fields**: `BTreeMap<String, EventValue>` -- the primary data payload. The `message` field holds the raw input by convention.
- **metadata**: `BTreeMap<String, EventValue>` -- pipeline-internal metadata (e.g., `_id`, `_index`). Not sent to outputs unless explicitly referenced.
- **tags**: `Vec<String>` -- labels added by filters (e.g., `_grokparsefailure`). Used for conditional routing.
- **@timestamp**: `DateTime<Utc>` -- event creation or parsed timestamp.

Field access supports nested paths using bracket notation: `[nested][field][name]`.

## Configuration Parsing

Ferro-Stash supports two configuration formats:

### Logstash DSL

The native Logstash configuration language with `input {}`, `filter {}`, `output {}` blocks, conditionals (`if [field] == "value"`), and plugin sections.

```
input {
  file { path => "/var/log/*.log" }
}
filter {
  grok { match => { "message" => "%{COMMONAPACHELOG}" } }
}
output {
  elasticsearch { hosts => ["localhost:9200"] }
}
```

The parser in `ferro-stash-config` implements a recursive descent parser for this DSL, including string interpolation (`%{field}`), conditionals, and the full operator set (`==`, `!=`, `=~`, `in`, `not in`, `and`, `or`).

### YAML Configuration

An alternative YAML format is also supported for environments that prefer structured configuration.

## Ruby Filter Integration

The `ruby` filter embeds an Artichoke Ruby interpreter, enabling inline Ruby code and external Ruby scripts for event transformation.

### Architecture

```
ferro-stash-filter (ruby filter)
  │
  ├── ferro-stash-ruby (bridge crate)
  │   ├── Artichoke interpreter lifecycle
  │   ├── Event <-> Ruby Hash serialization
  │   └── Error propagation (Ruby exceptions -> Rust Result)
  │
  └── unsafe code isolated to ferro-stash-ruby only
```

### Event Serialization

Events are converted to Ruby `Hash` objects before script execution and converted back after. The bridge handles type mapping between `EventValue` variants and Ruby types. This serialization boundary ensures the Ruby interpreter cannot corrupt Rust memory.

### Safety

- `unsafe_code = "deny"` applies workspace-wide; `ferro-stash-ruby` (Artichoke/mruby FFI) and `ferro-script` (Cranelift JIT FFI) opt in with `allow`.
- Ruby code runs within the Artichoke sandbox. File system and network access from Ruby are restricted.

## Async Pipeline Execution Model

### Runtime

The pipeline runs on the tokio multi-threaded runtime.

### Channel Architecture

```
Input_1 ──┐
Input_2 ──┤    ┌─────────────┐    ┌─────────────┐
Input_3 ──┼───>│ filter_rx    │───>│ output_rx   │──> Output_1
  ...     │    │ (mpsc)       │    │ (mpsc)      │──> Output_2
Input_N ──┘    └─────────────┘    └─────────────┘
```

- **Input -> Filter**: All inputs share a single `mpsc::Sender`. The filter stage consumes from the corresponding receiver.
- **Filter -> Output**: Filtered events are sent to an output channel. Outputs consume in batches.
- **Backpressure**: Bounded channels provide natural backpressure. When the filter channel is full, inputs block on send, preventing memory exhaustion.

### Shutdown

Graceful shutdown propagates via channel closure. Inputs stop producing, the filter stage drains remaining events, and outputs flush before the process exits. SIGTERM and SIGINT are handled via tokio signal handlers.

## Error Handling Patterns

- **No `unwrap()` in production code**: All fallible operations return `Result`. Plugin errors are logged and optionally tag the event (e.g., `_grokparsefailure`) rather than crashing the pipeline.
- **Anyhow for application errors**: The CLI and pipeline orchestration use `anyhow::Result` for ergonomic error chains.
- **Thiserror for library errors**: Core crate errors use `thiserror` for structured, typed errors.
- **Poison pill avoidance**: A malformed event never crashes the pipeline. Filters that fail on an event log the error and pass the event through (or drop it, depending on config).
