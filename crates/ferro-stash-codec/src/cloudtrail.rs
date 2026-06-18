// SPDX-License-Identifier: Apache-2.0
//! CloudTrail codec — AWS CloudTrail JSON log format decoder.
//!
//! CloudTrail logs are JSON documents with a `Records` array containing
//! individual API call events. This codec extracts individual records
//! from the CloudTrail log file format.
//!
//! Example CloudTrail record:
//! ```json
//! {
//!   "Records": [
//!     {
//!       "eventVersion": "1.08",
//!       "eventTime": "2026-04-12T00:00:00Z",
//!       "eventSource": "s3.amazonaws.com",
//!       "eventName": "GetObject",
//!       "awsRegion": "us-east-1",
//!       ...
//!     }
//!   ]
//! }
//! ```

use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};

use crate::Codec;

/// AWS CloudTrail JSON codec.
#[derive(Debug, Clone, Default)]
pub struct CloudtrailCodec {
    /// Target field for the record (None = merge into root).
    pub target: Option<String>,
}

impl CloudtrailCodec {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let target = settings
            .get("target")
            .and_then(|v| v.as_str())
            .map(String::from);
        Ok(Self { target })
    }
}

impl Codec for CloudtrailCodec {
    fn name(&self) -> &'static str {
        "cloudtrail"
    }

    fn decode(&self, data: &[u8]) -> Result<Vec<Event>> {
        let value: serde_json::Value = serde_json::from_slice(data)
            .map_err(|e| FerroStashError::Codec(format!("CloudTrail JSON error: {e}")))?;

        // Check if this is a CloudTrail log file with Records array
        if let Some(records) = value.get("Records").and_then(|v| v.as_array()) {
            let total = records.len() as i64;
            let mut events = Vec::with_capacity(records.len());

            for record in records {
                let mut event = if let Some(ref target) = self.target {
                    let mut e = Event::empty();
                    e.set(target.clone(), EventValue::from(record.clone()));
                    e
                } else {
                    Event::from_json(record.clone())
                };

                event.set("cloudtrail_record_count", EventValue::Integer(total));

                if let Some(event_time) = record.get("eventTime").and_then(|v| v.as_str()) {
                    if let Ok(dt) = event_time.parse::<chrono::DateTime<chrono::Utc>>() {
                        event.timestamp = dt;
                    }
                }

                event.add_tag("cloudtrail");
                events.push(event);
            }

            return Ok(events);
        }

        // Single record (not wrapped in Records array)
        let mut event = if let Some(ref target) = self.target {
            let mut e = Event::empty();
            e.set(target.clone(), EventValue::from(value.clone()));
            e
        } else {
            Event::from_json(value.clone())
        };

        if let Some(event_time) = value.get("eventTime").and_then(|v| v.as_str()) {
            if let Ok(dt) = event_time.parse::<chrono::DateTime<chrono::Utc>>() {
                event.timestamp = dt;
            }
        }

        event.add_tag("cloudtrail");
        Ok(vec![event])
    }

    fn encode(&self, event: &Event) -> Result<Vec<u8>> {
        let json = event.to_json();
        let mut bytes = serde_json::to_vec(&json)
            .map_err(|e| FerroStashError::Codec(format!("CloudTrail encode error: {e}")))?;
        bytes.push(b'\n');
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cloudtrail_decode_records() {
        let codec = CloudtrailCodec::default();
        let data = br#"{
            "Records": [
                {
                    "eventVersion": "1.08",
                    "eventTime": "2026-04-12T10:00:00Z",
                    "eventSource": "s3.amazonaws.com",
                    "eventName": "GetObject",
                    "awsRegion": "us-east-1"
                }
            ]
        }"#;
        let event = codec
            .decode(data)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(
            event.get("eventSource"),
            Some(&EventValue::String("s3.amazonaws.com".into()))
        );
        assert_eq!(
            event.get("eventName"),
            Some(&EventValue::String("GetObject".into()))
        );
        assert!(event.has_tag("cloudtrail"));
        assert_eq!(
            event.get("cloudtrail_record_count"),
            Some(&EventValue::Integer(1))
        );
    }

    #[test]
    fn test_cloudtrail_decode_single_record() {
        let codec = CloudtrailCodec::default();
        let data = br#"{
            "eventVersion": "1.08",
            "eventTime": "2026-04-12T10:00:00Z",
            "eventSource": "iam.amazonaws.com",
            "eventName": "CreateUser"
        }"#;
        let event = codec
            .decode(data)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(
            event.get("eventName"),
            Some(&EventValue::String("CreateUser".into()))
        );
        assert!(event.has_tag("cloudtrail"));
    }

    #[test]
    fn test_cloudtrail_decode_with_target() {
        let codec = CloudtrailCodec {
            target: Some("ct".to_string()),
        };
        let data = br#"{"Records": [{"eventName": "ListBuckets"}]}"#;
        let event = codec
            .decode(data)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert!(event.has_field("ct"));
    }

    #[test]
    fn test_cloudtrail_timestamp_parsing() {
        let codec = CloudtrailCodec::default();
        let data = br#"{"Records": [{"eventTime": "2026-04-12T15:30:00Z"}]}"#;
        let event = codec
            .decode(data)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(event.timestamp.format("%Y-%m-%d").to_string(), "2026-04-12");
    }

    #[test]
    fn test_cloudtrail_encode() {
        let codec = CloudtrailCodec::default();
        let mut event = Event::empty();
        event.set("eventName", EventValue::String("PutObject".into()));
        let bytes = codec.encode(&event).expect("encode");
        let text = String::from_utf8(bytes).expect("utf8");
        assert!(text.contains("PutObject"));
    }

    #[test]
    fn test_cloudtrail_invalid_json() {
        let codec = CloudtrailCodec::default();
        assert!(codec.decode(b"not json").is_err());
    }

    #[test]
    fn test_cloudtrail_name() {
        assert_eq!(CloudtrailCodec::default().name(), "cloudtrail");
    }

    #[test]
    fn test_cloudtrail_decode_multiple_records() {
        let codec = CloudtrailCodec::default();
        let data = br#"{
            "Records": [
                {"eventName": "GetObject", "eventTime": "2026-04-12T10:00:00Z"},
                {"eventName": "PutObject", "eventTime": "2026-04-12T10:01:00Z"},
                {"eventName": "DeleteObject", "eventTime": "2026-04-12T10:02:00Z"}
            ]
        }"#;
        let events = codec.decode(data).expect("decode");
        assert_eq!(events.len(), 3);
        assert_eq!(
            events[0].get("eventName"),
            Some(&EventValue::String("GetObject".into()))
        );
        assert_eq!(
            events[1].get("eventName"),
            Some(&EventValue::String("PutObject".into()))
        );
        assert_eq!(
            events[2].get("eventName"),
            Some(&EventValue::String("DeleteObject".into()))
        );
        // Each event has the total count
        assert_eq!(
            events[0].get("cloudtrail_record_count"),
            Some(&EventValue::Integer(3))
        );
        // Each event has the cloudtrail tag
        assert!(events[2].has_tag("cloudtrail"));
    }
}
