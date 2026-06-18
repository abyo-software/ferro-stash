// SPDX-License-Identifier: Apache-2.0
//! Date filter — parses dates from fields and sets @timestamp.

use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::FilterPlugin;

#[allow(dead_code)]
#[derive(Debug)]
pub struct DateFilter {
    source: String,
    target: String,
    patterns: Vec<String>,
    timezone: Option<String>,
    tag_on_failure: Vec<String>,
    condition: Option<Condition>,
}

impl DateFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let match_config = settings.get("match").and_then(|v| v.as_array());
        let (source, patterns) = if let Some(arr) = match_config {
            let source = arr
                .first()
                .and_then(|v| v.as_str())
                .unwrap_or("timestamp")
                .to_string();
            let patterns: Vec<String> = arr
                .iter()
                .skip(1)
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
            (source, patterns)
        } else {
            ("timestamp".to_string(), vec!["ISO8601".to_string()])
        };

        let target = settings
            .get("target")
            .and_then(|v| v.as_str())
            .unwrap_or("@timestamp")
            .to_string();
        let timezone = settings
            .get("timezone")
            .and_then(|v| v.as_str())
            .map(String::from);
        let tag_on_failure = settings
            .get("tag_on_failure")
            .and_then(|v| v.as_array())
            .map_or_else(
                || vec!["_dateparsefailure".to_string()],
                |a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                },
            );

        Ok(Self {
            source,
            target,
            patterns,
            timezone,
            tag_on_failure,
            condition,
        })
    }

    fn try_parse(&self, text: &str) -> Option<DateTime<Utc>> {
        for pattern in &self.patterns {
            match pattern.as_str() {
                "ISO8601" | "iso8601" => {
                    if let Ok(dt) = text.parse::<DateTime<Utc>>() {
                        return Some(dt);
                    }
                    // Try without timezone
                    if let Ok(ndt) = NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S") {
                        return Some(ndt.and_utc());
                    }
                    if let Ok(ndt) = NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f") {
                        return Some(ndt.and_utc());
                    }
                }
                "UNIX" | "unix" => {
                    if let Ok(ts) = text.parse::<i64>() {
                        return DateTime::from_timestamp(ts, 0);
                    }
                }
                "UNIX_MS" | "unix_ms" => {
                    if let Ok(ts) = text.parse::<i64>() {
                        return DateTime::from_timestamp_millis(ts);
                    }
                }
                fmt => {
                    // Try chrono format
                    if let Ok(ndt) = NaiveDateTime::parse_from_str(text, fmt) {
                        return Some(ndt.and_utc());
                    }
                }
            }
        }
        None
    }
}

#[async_trait]
impl FilterPlugin for DateFilter {
    fn name(&self) -> &'static str {
        "date"
    }

    async fn filter(&self, mut event: Event) -> Result<Vec<Event>> {
        let text = if let Some(val) = event.get(&self.source) {
            val.to_string_lossy()
        } else {
            for tag in &self.tag_on_failure {
                event.add_tag(tag);
            }
            return Ok(vec![event]);
        };

        if let Some(dt) = self.try_parse(&text) {
            if self.target == "@timestamp" {
                event.timestamp = dt;
            } else {
                // Match Logstash's `LogStash::Timestamp` wire format:
                // millisecond precision and a literal `Z` UTC suffix
                // (`2024-01-15T10:30:00.000Z`), not chrono's RFC3339
                // default (`2024-01-15T10:30:00+00:00`).
                event.set(
                    self.target.clone(),
                    EventValue::String(dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()),
                );
            }
        } else {
            for tag in &self.tag_on_failure {
                event.add_tag(tag);
            }
        }

        Ok(vec![event])
    }

    fn condition(&self) -> Option<&Condition> {
        self.condition.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;

    #[tokio::test]
    async fn test_date_iso8601() {
        let settings = serde_json::json!({
            "match": ["timestamp", "ISO8601"]
        });
        let filter = DateFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set(
            "timestamp",
            EventValue::String("2024-01-15T10:30:00Z".into()),
        );
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].timestamp.year(), 2024);
    }

    #[tokio::test]
    async fn test_date_unix() {
        let settings = serde_json::json!({
            "match": ["ts", "UNIX"]
        });
        let filter = DateFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("ts", EventValue::String("1705312200".into()));
        let result = filter.filter(event).await.expect("filter");
        assert!(!result[0].has_tag("_dateparsefailure"));
    }

    #[tokio::test]
    async fn test_date_custom_format() {
        let settings = serde_json::json!({
            "match": ["timestamp", "%d/%b/%Y:%H:%M:%S"]
        });
        let filter = DateFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set(
            "timestamp",
            EventValue::String("15/Jan/2024:10:30:00".into()),
        );
        let result = filter.filter(event).await.expect("filter");
        assert!(!result[0].has_tag("_dateparsefailure"));
    }

    #[tokio::test]
    async fn test_date_failure() {
        let settings = serde_json::json!({
            "match": ["timestamp", "ISO8601"]
        });
        let filter = DateFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("timestamp", EventValue::String("not a date".into()));
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_tag("_dateparsefailure"));
    }

    #[tokio::test]
    async fn test_date_unix_ms() {
        let settings = serde_json::json!({
            "match": ["ts", "UNIX_MS"]
        });
        let filter = DateFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("ts", EventValue::String("1705312200000".into()));
        let result = filter.filter(event).await.expect("filter");
        assert!(!result[0].has_tag("_dateparsefailure"));
    }

    #[tokio::test]
    async fn test_date_missing_source() {
        let settings = serde_json::json!({
            "match": ["nonexistent", "ISO8601"]
        });
        let filter = DateFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_tag("_dateparsefailure"));
    }

    #[tokio::test]
    async fn test_date_custom_target() {
        let settings = serde_json::json!({
            "match": ["timestamp", "ISO8601"],
            "target": "parsed_date"
        });
        let filter = DateFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set(
            "timestamp",
            EventValue::String("2024-01-15T10:30:00Z".into()),
        );
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_field("parsed_date"));
    }

    #[tokio::test]
    async fn test_date_default_config() {
        let settings = serde_json::json!({});
        let filter = DateFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.name(), "date");
    }

    #[tokio::test]
    async fn test_date_custom_failure_tag() {
        let settings = serde_json::json!({
            "match": ["timestamp", "ISO8601"],
            "tag_on_failure": ["my_date_error"]
        });
        let filter = DateFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("timestamp", EventValue::String("bad".into()));
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_tag("my_date_error"));
        assert!(!result[0].has_tag("_dateparsefailure"));
    }

    #[tokio::test]
    async fn test_date_iso8601_without_tz() {
        let settings = serde_json::json!({
            "match": ["timestamp", "ISO8601"]
        });
        let filter = DateFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set(
            "timestamp",
            EventValue::String("2024-01-15T10:30:00".into()),
        );
        let result = filter.filter(event).await.expect("filter");
        assert!(!result[0].has_tag("_dateparsefailure"));
        assert_eq!(result[0].timestamp.year(), 2024);
    }

    #[tokio::test]
    async fn test_date_target_logstash_format() {
        let settings = serde_json::json!({
            "match": ["timestamp", "ISO8601"],
            "target": "event_time"
        });
        let filter = DateFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set(
            "timestamp",
            EventValue::String("2024-01-15T10:30:00Z".into()),
        );
        let result = filter.filter(event).await.expect("filter");
        let target = result[0].get("event_time").expect("event_time set");
        assert_eq!(target.to_string_lossy(), "2024-01-15T10:30:00.000Z");
    }

    #[tokio::test]
    async fn test_date_target_logstash_format_with_subsecond() {
        let settings = serde_json::json!({
            "match": ["timestamp", "ISO8601"],
            "target": "event_time"
        });
        let filter = DateFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set(
            "timestamp",
            EventValue::String("2024-01-15T10:30:00.123Z".into()),
        );
        let result = filter.filter(event).await.expect("filter");
        let target = result[0].get("event_time").expect("event_time set");
        assert_eq!(target.to_string_lossy(), "2024-01-15T10:30:00.123Z");
    }

    #[tokio::test]
    async fn test_date_iso8601_with_millis() {
        let settings = serde_json::json!({
            "match": ["timestamp", "ISO8601"]
        });
        let filter = DateFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set(
            "timestamp",
            EventValue::String("2024-01-15T10:30:00.123".into()),
        );
        let result = filter.filter(event).await.expect("filter");
        assert!(!result[0].has_tag("_dateparsefailure"));
    }
}
