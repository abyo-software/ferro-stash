// SPDX-License-Identifier: Apache-2.0
//! Bytes filter — convert human-readable byte strings to integer byte counts.
//!
//! Supports parsing strings like "1.5GB", "500MB", "1024KB", "100B", "2TB", "1PB".

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::FilterPlugin;

#[derive(Debug)]
pub struct BytesFilter {
    /// Source field containing the human-readable byte string.
    source: String,
    /// Target field to store the integer result. Defaults to same as source.
    target: String,
    condition: Option<Condition>,
}

impl BytesFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let source = settings
            .get("source")
            .and_then(|v| v.as_str())
            .unwrap_or("message")
            .to_string();

        let target = settings
            .get("target")
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| {
                settings
                    .get("source")
                    .and_then(|v| v.as_str())
                    .unwrap_or("message")
            })
            .to_string();

        Ok(Self {
            source,
            target,
            condition,
        })
    }

    /// Parse a human-readable byte string into bytes.
    ///
    /// Supported units (case-insensitive):
    /// - B / bytes
    /// - KB / KiB
    /// - MB / MiB
    /// - GB / GiB
    /// - TB / TiB
    /// - PB / PiB
    fn parse_bytes(input: &str) -> Option<i64> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return None;
        }

        // Find where the numeric part ends and the unit begins
        let mut num_end = 0;
        let chars: Vec<char> = trimmed.chars().collect();
        for (i, ch) in chars.iter().enumerate() {
            if ch.is_ascii_digit() || *ch == '.' || *ch == '-' || *ch == '+' {
                num_end = i + 1;
            } else if !ch.is_whitespace() {
                break;
            } else {
                num_end = i;
            }
        }

        if num_end == 0 {
            return None;
        }

        let num_str = trimmed[..num_end].trim();
        let unit_str = trimmed[num_end..].trim().to_uppercase();

        let number: f64 = num_str.parse().ok()?;

        let multiplier: f64 = match unit_str.as_str() {
            "" | "B" | "BYTES" | "BYTE" => 1.0,
            "KB" | "KIB" | "K" => 1024.0,
            "MB" | "MIB" | "M" => 1024.0 * 1024.0,
            "GB" | "GIB" | "G" => 1024.0 * 1024.0 * 1024.0,
            "TB" | "TIB" | "T" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
            "PB" | "PIB" | "P" => 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0,
            _ => return None,
        };

        Some((number * multiplier) as i64)
    }
}

#[async_trait]
impl FilterPlugin for BytesFilter {
    fn name(&self) -> &'static str {
        "bytes"
    }

    async fn filter(&self, mut event: Event) -> Result<Vec<Event>> {
        let value_str = match event.get(&self.source) {
            Some(val) => val.to_string_lossy(),
            None => return Ok(vec![event]),
        };

        match Self::parse_bytes(&value_str) {
            Some(bytes) => {
                event.set(self.target.clone(), EventValue::Integer(bytes));
            }
            None => {
                event.add_tag("_bytesparsefailure");
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

    #[test]
    fn test_parse_bytes_units() {
        assert_eq!(BytesFilter::parse_bytes("100B"), Some(100));
        assert_eq!(BytesFilter::parse_bytes("1KB"), Some(1024));
        assert_eq!(BytesFilter::parse_bytes("1MB"), Some(1024 * 1024));
        assert_eq!(BytesFilter::parse_bytes("1GB"), Some(1024 * 1024 * 1024));
        assert_eq!(
            BytesFilter::parse_bytes("1TB"),
            Some(1024_i64 * 1024 * 1024 * 1024)
        );
        assert_eq!(
            BytesFilter::parse_bytes("1PB"),
            Some(1024_i64 * 1024 * 1024 * 1024 * 1024)
        );
    }

    #[test]
    fn test_parse_bytes_decimal() {
        assert_eq!(
            BytesFilter::parse_bytes("1.5GB"),
            Some((1.5 * 1024.0 * 1024.0 * 1024.0) as i64)
        );
        assert_eq!(BytesFilter::parse_bytes("500MB"), Some(500 * 1024 * 1024));
        assert_eq!(BytesFilter::parse_bytes("1024KB"), Some(1024 * 1024));
    }

    #[test]
    fn test_parse_bytes_case_insensitive() {
        assert_eq!(BytesFilter::parse_bytes("1kb"), Some(1024));
        assert_eq!(BytesFilter::parse_bytes("1Mb"), Some(1024 * 1024));
        assert_eq!(BytesFilter::parse_bytes("1gb"), Some(1024 * 1024 * 1024));
    }

    #[test]
    fn test_parse_bytes_with_spaces() {
        assert_eq!(BytesFilter::parse_bytes("  100 B  "), Some(100));
        assert_eq!(BytesFilter::parse_bytes("1 KB"), Some(1024));
    }

    #[test]
    fn test_parse_bytes_plain_number() {
        assert_eq!(BytesFilter::parse_bytes("1024"), Some(1024));
    }

    #[test]
    fn test_parse_bytes_invalid() {
        assert_eq!(BytesFilter::parse_bytes(""), None);
        assert_eq!(BytesFilter::parse_bytes("abc"), None);
        assert_eq!(BytesFilter::parse_bytes("1XB"), None);
    }

    #[tokio::test]
    async fn test_bytes_filter_basic() {
        let settings = serde_json::json!({
            "source": "filesize",
            "target": "filesize_bytes"
        });
        let filter = BytesFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("filesize", EventValue::String("1.5GB".into()));
        let result = filter.filter(event).await.expect("filter");
        let bytes = result[0].get("filesize_bytes").expect("target");
        assert_eq!(
            bytes,
            &EventValue::Integer((1.5 * 1024.0 * 1024.0 * 1024.0) as i64)
        );
    }

    #[tokio::test]
    async fn test_bytes_filter_same_target() {
        let settings = serde_json::json!({
            "source": "size",
            "target": "size"
        });
        let filter = BytesFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("size", EventValue::String("500MB".into()));
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("size"),
            Some(&EventValue::Integer(500 * 1024 * 1024))
        );
    }

    #[tokio::test]
    async fn test_bytes_filter_invalid_adds_tag() {
        let settings = serde_json::json!({
            "source": "size"
        });
        let filter = BytesFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("size", EventValue::String("not a size".into()));
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_tag("_bytesparsefailure"));
    }

    #[tokio::test]
    async fn test_bytes_filter_missing_source() {
        let settings = serde_json::json!({
            "source": "nonexistent"
        });
        let filter = BytesFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        assert!(!result[0].has_tag("_bytesparsefailure"));
    }

    #[test]
    fn test_bytes_name() {
        let settings = serde_json::json!({});
        let filter = BytesFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.name(), "bytes");
    }
}
