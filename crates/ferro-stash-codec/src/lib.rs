// SPDX-License-Identifier: Apache-2.0
//! Codec plugins for `FerroStash` — encode/decode events between input/output boundaries.
//!
//! Implements all Logstash-compatible codecs plus additional formats:
//!
//! | Codec | Description |
//! |-------|-------------|
//! | `plain` / `line` | Plain text, one event per line |
//! | `json` / `json_lines` | JSON object per event |
//! | `multiline` | Merge multiple lines into one event |
//! | `csv` | Comma-separated values |
//! | `script` / `ruby` | User-defined DSL expressions |
//! | `rubydebug` | Ruby pp-style pretty print |
//! | `dots` | Dot per event (throughput monitor) |
//! | `bytes` | Raw binary passthrough |
//! | `es_bulk` | Elasticsearch Bulk API NDJSON |
//! | `msgpack` | MessagePack binary format |
//! | `fluent` | Fluent Forward Protocol |
//! | `graphite` | Graphite plaintext line protocol |
//! | `cef` | Common Event Format (SIEM) |
//! | `netflow` | NetFlow v5/v9/IPFIX |
//! | `collectd` | collectd binary/JSON protocol |
//! | `avro` | Apache Avro container format |
//! | `protobuf` | Protocol Buffers wire format |
//! | `cloudfront` | AWS CloudFront access logs |
//! | `cloudtrail` | AWS CloudTrail JSON logs |
//! | `nmap` | nmap XML scan results |
//! | `edn` / `edn_lines` | EDN (Extensible Data Notation) |

pub mod avro;
pub mod bytes_codec;
pub mod cef;
pub mod cloudfront;
pub mod cloudtrail;
pub mod collectd;
pub mod csv_codec;
pub mod dots;
pub mod edn;
pub mod es_bulk;
pub mod fluent;
pub mod graphite;
pub mod json;
pub mod msgpack;
pub mod multiline;
pub mod netflow;
pub mod nmap;
pub mod plain;
pub mod protobuf;
pub mod rubydebug;
pub mod script;

use ferro_stash_core::error::Result;
use ferro_stash_core::event::Event;

/// A codec that decodes raw bytes/lines into events and encodes events to bytes.
pub trait Codec: Send + Sync + std::fmt::Debug {
    /// Codec name.
    fn name(&self) -> &str;

    /// Decode a chunk of data into one or more events.
    ///
    /// Returns a `Vec<Event>` because some formats (netflow, es_bulk, cloudtrail)
    /// naturally produce multiple events from a single input.
    fn decode(&self, data: &[u8]) -> Result<Vec<Event>>;

    /// Encode an event to bytes for output.
    fn encode(&self, event: &Event) -> Result<Vec<u8>>;

    /// Whether this codec maintains internal state across decode calls.
    ///
    /// Stateful codecs (e.g., multiline) buffer data across calls and
    /// may need explicit flushing.
    fn is_stateful(&self) -> bool {
        false
    }
}

/// Creates a codec by name.
pub fn create_codec(name: &str, settings: &serde_json::Value) -> Result<Box<dyn Codec>> {
    match name {
        "json" | "json_lines" => Ok(Box::new(json::JsonCodec::from_config(settings)?)),
        "plain" | "line" => Ok(Box::new(plain::PlainCodec::from_config(settings)?)),
        "multiline" => Ok(Box::new(multiline::MultilineCodec::from_config(settings)?)),
        "csv" => Ok(Box::new(csv_codec::CsvCodec::from_config(settings)?)),
        "script" | "ruby" => Ok(Box::new(script::ScriptCodec::from_config(settings)?)),
        "rubydebug" => Ok(Box::new(rubydebug::RubydebugCodec::from_config(settings)?)),
        "dots" => Ok(Box::new(dots::DotsCodec::from_config(settings)?)),
        "bytes" => Ok(Box::new(bytes_codec::BytesCodec::from_config(settings)?)),
        "es_bulk" => Ok(Box::new(es_bulk::EsBulkCodec::from_config(settings)?)),
        "msgpack" => Ok(Box::new(msgpack::MsgpackCodec::from_config(settings)?)),
        "fluent" => Ok(Box::new(fluent::FluentCodec::from_config(settings)?)),
        "graphite" => Ok(Box::new(graphite::GraphiteCodec::from_config(settings)?)),
        "cef" => Ok(Box::new(cef::CefCodec::from_config(settings)?)),
        "netflow" => Ok(Box::new(netflow::NetflowCodec::from_config(settings)?)),
        "collectd" => Ok(Box::new(collectd::CollectdCodec::from_config(settings)?)),
        "avro" => Ok(Box::new(avro::AvroCodec::from_config(settings)?)),
        "protobuf" => Ok(Box::new(protobuf::ProtobufCodec::from_config(settings)?)),
        "cloudfront" => Ok(Box::new(cloudfront::CloudfrontCodec::from_config(
            settings,
        )?)),
        "cloudtrail" => Ok(Box::new(cloudtrail::CloudtrailCodec::from_config(
            settings,
        )?)),
        "nmap" => Ok(Box::new(nmap::NmapCodec::from_config(settings)?)),
        "edn" | "edn_lines" => Ok(Box::new(edn::EdnCodec::from_config(settings)?)),
        _ => Err(ferro_stash_core::error::FerroStashError::Codec(format!(
            "unknown codec: {name}"
        ))),
    }
}

/// Returns the default codec (plain).
pub fn default_codec() -> Box<dyn Codec> {
    Box::new(plain::PlainCodec::default())
}

/// Returns all supported codec names.
pub fn supported_codecs() -> Vec<&'static str> {
    vec![
        "json",
        "json_lines",
        "plain",
        "line",
        "multiline",
        "csv",
        "script",
        "ruby",
        "rubydebug",
        "dots",
        "bytes",
        "es_bulk",
        "msgpack",
        "fluent",
        "graphite",
        "cef",
        "netflow",
        "collectd",
        "avro",
        "protobuf",
        "cloudfront",
        "cloudtrail",
        "nmap",
        "edn",
        "edn_lines",
    ]
}
