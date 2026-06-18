// SPDX-License-Identifier: Apache-2.0
//! CloudFront codec — AWS CloudFront access log format decoder.
//!
//! CloudFront logs are tab-separated with two comment header lines:
//! ```text
//! #Version: 1.0
//! #Fields: date time x-edge-location ...
//! 2026-04-12\t00:00:00\tIAD89-C1\t...
//! ```

use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};

use crate::Codec;

/// Default CloudFront log field names (W3C Extended Log Format).
const DEFAULT_FIELDS: &[&str] = &[
    "date",
    "time",
    "x-edge-location",
    "sc-bytes",
    "c-ip",
    "cs-method",
    "cs-host",
    "cs-uri-stem",
    "sc-status",
    "cs-referer",
    "cs-user-agent",
    "cs-uri-query",
    "cs-cookie",
    "x-edge-result-type",
    "x-edge-request-id",
    "x-host-header",
    "cs-protocol",
    "cs-bytes",
    "time-taken",
    "x-forwarded-for",
    "ssl-protocol",
    "ssl-cipher",
    "x-edge-response-result-type",
    "cs-protocol-version",
    "fle-status",
    "fle-encrypted-fields",
    "c-port",
    "time-to-first-byte",
    "x-edge-detailed-result-type",
    "sc-content-type",
    "sc-content-len",
    "sc-range-start",
    "sc-range-end",
];

/// AWS CloudFront access log codec.
#[derive(Debug, Clone)]
pub struct CloudfrontCodec {
    /// Custom field names (overrides auto-detection from header).
    pub fields: Vec<String>,
}

impl Default for CloudfrontCodec {
    fn default() -> Self {
        Self {
            fields: DEFAULT_FIELDS.iter().map(|s| (*s).to_string()).collect(),
        }
    }
}

impl CloudfrontCodec {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let fields = settings
            .get("fields")
            .and_then(|v| v.as_array())
            .map_or_else(
                || DEFAULT_FIELDS.iter().map(|s| (*s).to_string()).collect(),
                |arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                },
            );
        Ok(Self { fields })
    }

    /// Try to convert a string value to a typed `EventValue`.
    fn typed_value(field_name: &str, raw: &str) -> EventValue {
        if raw == "-" {
            return EventValue::Null;
        }

        match field_name {
            "sc-bytes" | "cs-bytes" | "sc-status" | "c-port" | "sc-content-len"
            | "sc-range-start" | "sc-range-end" => {
                if let Ok(n) = raw.parse::<i64>() {
                    return EventValue::Integer(n);
                }
            }
            "time-taken" | "time-to-first-byte" => {
                if let Ok(f) = raw.parse::<f64>() {
                    return EventValue::Float(f);
                }
            }
            _ => {}
        }

        EventValue::String(raw.to_string())
    }
}

impl Codec for CloudfrontCodec {
    fn name(&self) -> &'static str {
        "cloudfront"
    }

    fn decode(&self, data: &[u8]) -> Result<Vec<Event>> {
        let text = String::from_utf8_lossy(data);
        let text = text.trim();

        // Skip comment lines
        if text.starts_with('#') {
            // Check if it's a #Fields: header — update field list for context
            if let Some(fields_str) = text.strip_prefix("#Fields:") {
                let mut event = Event::empty();
                let field_names: Vec<String> =
                    fields_str.split_whitespace().map(String::from).collect();
                event.set(
                    "cloudfront_fields",
                    EventValue::Array(
                        field_names
                            .iter()
                            .map(|f| EventValue::String(f.clone()))
                            .collect(),
                    ),
                );
                event.add_tag("_cloudfront_header");
                return Ok(vec![event]);
            }
            // Version or other comment — create a minimal event
            let mut event = Event::new(text);
            event.add_tag("_cloudfront_comment");
            return Ok(vec![event]);
        }

        let values: Vec<&str> = text.split('\t').collect();
        if values.is_empty() {
            return Err(FerroStashError::Codec(
                "empty CloudFront log line".to_string(),
            ));
        }

        let mut event = Event::empty();
        event.set_message(text);

        for (i, value) in values.iter().enumerate() {
            let field_name = if i < self.fields.len() {
                &self.fields[i]
            } else {
                // Auto-generate field name for extra columns
                &format!("field{}", i + 1)
            };
            event.set(field_name.clone(), Self::typed_value(field_name, value));
        }

        // Combine date + time into @timestamp if both present
        if let (Some(date), Some(time)) = (
            event
                .get("date")
                .and_then(EventValue::as_str)
                .map(String::from),
            event
                .get("time")
                .and_then(EventValue::as_str)
                .map(String::from),
        ) {
            let ts_str = format!("{date}T{time}Z");
            if let Ok(dt) = ts_str.parse::<chrono::DateTime<chrono::Utc>>() {
                event.timestamp = dt;
            }
        }

        Ok(vec![event])
    }

    fn encode(&self, event: &Event) -> Result<Vec<u8>> {
        let mut values = Vec::new();
        for field_name in &self.fields {
            let val = event
                .get(field_name)
                .map_or_else(|| "-".to_string(), EventValue::to_string_lossy);
            values.push(val);
        }
        let line = values.join("\t");
        Ok(format!("{line}\n").into_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cloudfront_decode() {
        let codec = CloudfrontCodec::default();
        let line = "2026-04-12\t00:00:00\tIAD89-C1\t1234\t10.0.0.1\tGET\texample.com\t/index.html\t200\t-\tMozilla/5.0\t-\t-\tHit\tabc123\texample.com\thttps\t500\t0.001\t-\t-\t-\tHit\tHTTP/2.0\t-\t-\t443\t0.001\tHit\ttext/html\t1234\t-\t-";
        let event = codec
            .decode(line.as_bytes())
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(
            event.get("x-edge-location"),
            Some(&EventValue::String("IAD89-C1".into()))
        );
        assert_eq!(event.get("sc-bytes"), Some(&EventValue::Integer(1234)));
        assert_eq!(
            event.get("c-ip"),
            Some(&EventValue::String("10.0.0.1".into()))
        );
        assert_eq!(event.get("sc-status"), Some(&EventValue::Integer(200)));
    }

    #[test]
    fn test_cloudfront_comment_line() {
        let codec = CloudfrontCodec::default();
        let event = codec
            .decode(b"#Version: 1.0")
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert!(event.has_tag("_cloudfront_comment"));
    }

    #[test]
    fn test_cloudfront_fields_header() {
        let codec = CloudfrontCodec::default();
        let event = codec
            .decode(b"#Fields: date time x-edge-location")
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert!(event.has_tag("_cloudfront_header"));
    }

    #[test]
    fn test_cloudfront_null_value() {
        let codec = CloudfrontCodec::default();
        let event = codec
            .decode(b"2026-04-12\t00:00:00\tIAD89\t-")
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(event.get("sc-bytes"), Some(&EventValue::Null));
    }

    #[test]
    fn test_cloudfront_encode() {
        let codec = CloudfrontCodec::default();
        let mut event = Event::empty();
        event.set("date", EventValue::String("2026-04-12".into()));
        event.set("time", EventValue::String("00:00:00".into()));
        event.set("x-edge-location", EventValue::String("IAD89".into()));
        let bytes = codec.encode(&event).expect("encode");
        let text = String::from_utf8(bytes).expect("utf8");
        assert!(text.contains("2026-04-12"));
        assert!(text.contains("IAD89"));
    }

    #[test]
    fn test_cloudfront_name() {
        assert_eq!(CloudfrontCodec::default().name(), "cloudfront");
    }

    #[test]
    fn test_cloudfront_timestamp_parsing() {
        let codec = CloudfrontCodec::default();
        let line = "2026-04-12\t10:30:00\tIAD89\t0";
        let event = codec
            .decode(line.as_bytes())
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(event.timestamp.format("%Y-%m-%d").to_string(), "2026-04-12");
    }
}
