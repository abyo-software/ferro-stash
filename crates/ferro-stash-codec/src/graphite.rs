// SPDX-License-Identifier: Apache-2.0
//! Graphite codec — Graphite plaintext line protocol.
//!
//! Format: `metric_path value timestamp\n`
//!
//! Example: `servers.web01.cpu.load 0.75 1712880000`

use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};

use crate::Codec;

/// Graphite line protocol codec.
#[derive(Debug, Clone, Default)]
pub struct GraphiteCodec {
    /// Whether to include the metric timestamp in output (default: true).
    pub include_timestamp: bool,
    /// Metrics prefix for encoding.
    pub metrics_prefix: Option<String>,
}

impl GraphiteCodec {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let include_timestamp = settings
            .get("include_timestamp")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(true);
        let metrics_prefix = settings
            .get("metrics_prefix")
            .and_then(|v| v.as_str())
            .map(String::from);
        Ok(Self {
            include_timestamp,
            metrics_prefix,
        })
    }
}

impl Codec for GraphiteCodec {
    fn name(&self) -> &'static str {
        "graphite"
    }

    fn decode(&self, data: &[u8]) -> Result<Vec<Event>> {
        let text = String::from_utf8_lossy(data);
        let text = text.trim();

        if text.is_empty() || text.starts_with('#') {
            return Err(FerroStashError::Codec(
                "empty or comment graphite line".to_string(),
            ));
        }

        let parts: Vec<&str> = text.splitn(3, ' ').collect();
        if parts.len() < 2 {
            return Err(FerroStashError::Codec(format!(
                "invalid graphite line: expected 'metric value [timestamp]', got: {text}"
            )));
        }

        let metric = parts[0];
        let value: f64 = parts[1]
            .parse()
            .map_err(|e| FerroStashError::Codec(format!("invalid graphite value: {e}")))?;

        let mut event = Event::empty();
        event.set("metric", EventValue::String(metric.to_string()));
        event.set("value", EventValue::Float(value));
        event.set_message(text);

        // Parse optional timestamp
        if parts.len() >= 3 {
            if let Ok(ts) = parts[2].parse::<i64>() {
                event.set("timestamp_epoch", EventValue::Integer(ts));
                if let Some(dt) = chrono::DateTime::from_timestamp(ts, 0) {
                    event.timestamp = dt;
                }
            }
        }

        // Split metric path into components
        let path_parts: Vec<&str> = metric.split('.').collect();
        if path_parts.len() > 1 {
            let arr: Vec<EventValue> = path_parts
                .iter()
                .map(|p| EventValue::String((*p).to_string()))
                .collect();
            event.set("metric_path", EventValue::Array(arr));
        }

        Ok(vec![event])
    }

    fn encode(&self, event: &Event) -> Result<Vec<u8>> {
        let metric = event
            .get("metric")
            .and_then(EventValue::as_str)
            .ok_or_else(|| FerroStashError::Codec("missing 'metric' field".to_string()))?;

        let value = event
            .get("value")
            .and_then(EventValue::as_f64)
            .ok_or_else(|| FerroStashError::Codec("missing 'value' field".to_string()))?;

        let full_metric = if let Some(ref prefix) = self.metrics_prefix {
            format!("{prefix}.{metric}")
        } else {
            metric.to_string()
        };

        let line = if self.include_timestamp {
            let ts = event.timestamp.timestamp();
            format!("{full_metric} {value} {ts}\n")
        } else {
            format!("{full_metric} {value}\n")
        };

        Ok(line.into_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_graphite_decode() {
        let codec = GraphiteCodec::default();
        let event = codec
            .decode(b"servers.web01.cpu 0.75 1712880000")
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(
            event.get("metric"),
            Some(&EventValue::String("servers.web01.cpu".into()))
        );
        assert_eq!(event.get("value"), Some(&EventValue::Float(0.75)));
        assert_eq!(
            event.get("timestamp_epoch"),
            Some(&EventValue::Integer(1_712_880_000))
        );
    }

    #[test]
    fn test_graphite_decode_no_timestamp() {
        let codec = GraphiteCodec::default();
        let event = codec
            .decode(b"cpu.load 2.5")
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(
            event.get("metric"),
            Some(&EventValue::String("cpu.load".into()))
        );
        assert_eq!(event.get("value"), Some(&EventValue::Float(2.5)));
    }

    #[test]
    fn test_graphite_decode_metric_path() {
        let codec = GraphiteCodec::default();
        let event = codec
            .decode(b"a.b.c 1.0")
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        let path = event.get("metric_path").expect("metric_path");
        assert!(matches!(path, EventValue::Array(_)));
    }

    #[test]
    fn test_graphite_encode() {
        let codec = GraphiteCodec {
            include_timestamp: false,
            metrics_prefix: None,
        };
        let mut event = Event::empty();
        event.set("metric", EventValue::String("cpu.load".into()));
        event.set("value", EventValue::Float(0.5));
        let bytes = codec.encode(&event).expect("encode");
        assert_eq!(bytes, b"cpu.load 0.5\n");
    }

    #[test]
    fn test_graphite_encode_with_prefix() {
        let codec = GraphiteCodec {
            include_timestamp: false,
            metrics_prefix: Some("prod".to_string()),
        };
        let mut event = Event::empty();
        event.set("metric", EventValue::String("cpu".into()));
        event.set("value", EventValue::Float(1.0));
        let bytes = codec.encode(&event).expect("encode");
        assert_eq!(bytes, b"prod.cpu 1\n");
    }

    #[test]
    fn test_graphite_invalid() {
        let codec = GraphiteCodec::default();
        assert!(codec.decode(b"").is_err());
        assert!(codec.decode(b"# comment").is_err());
        assert!(codec.decode(b"just_metric").is_err());
        assert!(codec.decode(b"metric not_a_number").is_err());
    }

    #[test]
    fn test_graphite_name() {
        assert_eq!(GraphiteCodec::default().name(), "graphite");
    }
}
