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

/// Resolves a plugin's `codec` setting into `(name, settings)`, handling both
/// Logstash DSL forms:
///
/// * String form (`codec => json`) → `("json", {})`.
/// * Descriptor form (`codec => json { target => "data" }`) →
///   `("json", { "target": "data" })`. The DSL parser represents this as an
///   object carrying a `_plugin` discriminator; the discriminator is stripped.
/// * Missing / unrecognized → `(default_name, {})`.
///
/// Both input and output plugins use this so codec sub-settings are honored
/// instead of being silently dropped (a `get_string("codec")` lookup returns
/// `None` for the descriptor form and falls back to the default codec).
pub fn resolve_codec(
    settings: &serde_json::Value,
    default_name: &str,
) -> (String, serde_json::Value) {
    let empty = || serde_json::Value::Object(serde_json::Map::new());

    match settings.get("codec") {
        Some(serde_json::Value::String(name)) => (name.clone(), empty()),
        Some(serde_json::Value::Object(map)) => {
            let name = map
                .get("_plugin")
                .and_then(|v| v.as_str())
                .map_or_else(|| default_name.to_string(), String::from);
            let mut sub = map.clone();
            sub.remove("_plugin");
            (name, serde_json::Value::Object(sub))
        }
        _ => (default_name.to_string(), empty()),
    }
}

/// Resolves the `codec` setting (both DSL forms) and builds the codec,
/// threading the codec's own sub-settings through to [`create_codec`].
pub fn create_codec_from_settings(
    settings: &serde_json::Value,
    default_name: &str,
) -> Result<Box<dyn Codec>> {
    let (name, sub) = resolve_codec(settings, default_name);
    create_codec(&name, &sub)
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

#[cfg(test)]
mod resolve_codec_tests {
    use super::*;

    #[test]
    fn resolve_string_form() {
        let s = serde_json::json!({ "codec": "json" });
        let (name, sub) = resolve_codec(&s, "plain");
        assert_eq!(name, "json");
        assert_eq!(sub, serde_json::json!({}));
    }

    #[test]
    fn resolve_descriptor_form_keeps_sub_settings_and_strips_plugin() {
        let s = serde_json::json!({
            "codec": { "_plugin": "json", "target": "data", "pretty": true }
        });
        let (name, sub) = resolve_codec(&s, "plain");
        assert_eq!(name, "json");
        assert_eq!(sub, serde_json::json!({ "target": "data", "pretty": true }));
        assert!(sub.get("_plugin").is_none());
    }

    #[test]
    fn resolve_missing_uses_default() {
        let (name, sub) = resolve_codec(&serde_json::json!({}), "plain");
        assert_eq!(name, "plain");
        assert_eq!(sub, serde_json::json!({}));
    }

    #[test]
    fn resolve_descriptor_without_plugin_uses_default_name() {
        let s = serde_json::json!({ "codec": { "target": "data" } });
        let (name, sub) = resolve_codec(&s, "plain");
        assert_eq!(name, "plain");
        assert_eq!(sub, serde_json::json!({ "target": "data" }));
    }

    #[test]
    fn create_from_settings_descriptor_form_builds_named_codec() {
        // Descriptor form must build the named codec, not the default — this is
        // the bug the output plugins had with get_string("codec").
        let s = serde_json::json!({ "codec": { "_plugin": "json" } });
        let codec = create_codec_from_settings(&s, "plain").expect("codec builds");
        assert_eq!(codec.name(), "json");
    }
}
