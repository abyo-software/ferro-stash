// SPDX-License-Identifier: Apache-2.0
//! CSV output plugin — appends events as CSV rows to a file.
//!
//! ```logstash
//! output {
//!   csv {
//!     path        => "/var/log/events.csv"
//!     fields      => [ "@timestamp", "host", "message" ]
//!     csv_options => { "separator" => ";" }
//!   }
//! }
//! ```
//!
//! One row is written per event with the configured `fields` as columns, in
//! order. A field absent from an event yields an empty cell. Rows are appended
//! (the file is created if missing); no header row is written, matching the
//! common Logstash usage where `fields` defines the column order explicitly.
//! Proper RFC-4180 quoting/escaping is handled by the `csv` crate.

use std::io::Write;
use std::sync::Mutex;

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::Event;
use ferro_stash_core::plugin::OutputPlugin;

#[derive(Debug)]
pub struct CsvOutput {
    path: String,
    fields: Vec<String>,
    separator: u8,
    quote_char: u8,
    writer: Mutex<Option<std::io::BufWriter<std::fs::File>>>,
    condition: Option<Condition>,
}

impl CsvOutput {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let err = |m: String| FerroStashError::Output {
            plugin: "csv".to_string(),
            message: m,
        };

        let path = settings
            .get("path")
            .and_then(|v| v.as_str())
            .ok_or_else(|| err("path is required".to_string()))?
            .to_string();

        let fields: Vec<String> = settings
            .get("fields")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        if fields.is_empty() {
            return Err(err(
                "csv output requires a non-empty `fields` array (column order)".to_string(),
            ));
        }

        let opts = settings.get("csv_options").and_then(|v| v.as_object());
        let separator = opts
            .and_then(|o| o.get("separator"))
            .and_then(|v| v.as_str())
            .and_then(|s| s.as_bytes().first().copied())
            .unwrap_or(b',');
        let quote_char = opts
            .and_then(|o| o.get("quote_char"))
            .and_then(|v| v.as_str())
            .and_then(|s| s.as_bytes().first().copied())
            .unwrap_or(b'"');

        Ok(Self {
            path,
            fields,
            separator,
            quote_char,
            writer: Mutex::new(None),
            condition,
        })
    }

    /// Renders the cell value for `field`. `@timestamp` is rendered as RFC 3339;
    /// a missing field yields an empty cell.
    fn cell(&self, event: &Event, field: &str) -> String {
        if field == "@timestamp" {
            return event.timestamp.to_rfc3339();
        }
        event
            .get(field)
            .map(|v| v.to_string_lossy())
            .unwrap_or_default()
    }
}

#[async_trait]
impl OutputPlugin for CsvOutput {
    fn name(&self) -> &'static str {
        "csv"
    }

    async fn output(&self, events: Vec<Event>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        // Format the whole batch into a buffer using the csv crate (handles
        // separator/quote escaping), then append it to the file.
        let mut wtr = csv::WriterBuilder::new()
            .delimiter(self.separator)
            .quote(self.quote_char)
            .has_headers(false)
            .from_writer(Vec::new());
        for event in &events {
            let row: Vec<String> = self.fields.iter().map(|f| self.cell(event, f)).collect();
            wtr.write_record(&row).map_err(|e| FerroStashError::Output {
                plugin: "csv".to_string(),
                message: format!("csv encode error: {e}"),
            })?;
        }
        let data = wtr.into_inner().map_err(|e| FerroStashError::Output {
            plugin: "csv".to_string(),
            message: format!("csv flush error: {e}"),
        })?;

        let mut guard = self.writer.lock().map_err(|e| FerroStashError::Output {
            plugin: "csv".to_string(),
            message: format!("lock error: {e}"),
        })?;
        if guard.is_none() {
            if let Some(parent) = std::path::Path::new(&self.path).parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent).map_err(|e| FerroStashError::Output {
                        plugin: "csv".to_string(),
                        message: format!("cannot create directory: {e}"),
                    })?;
                }
            }
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)
                .map_err(|e| FerroStashError::Output {
                    plugin: "csv".to_string(),
                    message: format!("cannot open {}: {e}", self.path),
                })?;
            *guard = Some(std::io::BufWriter::new(file));
        }
        if let Some(writer) = guard.as_mut() {
            writer.write_all(&data).map_err(|e| FerroStashError::Output {
                plugin: "csv".to_string(),
                message: format!("write error: {e}"),
            })?;
            writer.flush().map_err(|e| FerroStashError::Output {
                plugin: "csv".to_string(),
                message: format!("flush error: {e}"),
            })?;
        }
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        let mut guard = self.writer.lock().map_err(|e| FerroStashError::Output {
            plugin: "csv".to_string(),
            message: format!("lock error: {e}"),
        })?;
        if let Some(writer) = guard.as_mut() {
            writer.flush().map_err(|e| FerroStashError::Output {
                plugin: "csv".to_string(),
                message: format!("flush error: {e}"),
            })?;
        }
        Ok(())
    }

    fn condition(&self) -> Option<&Condition> {
        self.condition.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferro_stash_core::event::EventValue;

    #[test]
    fn test_csv_output_name() {
        let settings = serde_json::json!({ "path": "/tmp/x.csv", "fields": ["a"] });
        let output = CsvOutput::from_config(&settings, None).expect("config");
        assert_eq!(output.name(), "csv");
    }

    #[test]
    fn test_csv_output_missing_path() {
        let settings = serde_json::json!({ "fields": ["a"] });
        assert!(CsvOutput::from_config(&settings, None).is_err());
    }

    #[test]
    fn test_csv_output_missing_fields() {
        let settings = serde_json::json!({ "path": "/tmp/x.csv" });
        assert!(CsvOutput::from_config(&settings, None).is_err());
        let settings = serde_json::json!({ "path": "/tmp/x.csv", "fields": [] });
        assert!(CsvOutput::from_config(&settings, None).is_err());
    }

    #[test]
    fn test_csv_options_parsed() {
        let settings = serde_json::json!({
            "path": "/tmp/x.csv",
            "fields": ["a"],
            "csv_options": { "separator": ";", "quote_char": "'" }
        });
        let output = CsvOutput::from_config(&settings, None).expect("config");
        assert_eq!(output.separator, b';');
        assert_eq!(output.quote_char, b'\'');
    }

    #[tokio::test]
    async fn test_csv_output_writes_rows() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("out.csv");
        let settings = serde_json::json!({
            "path": path.to_string_lossy(),
            "fields": ["name", "age", "city"]
        });
        let output = CsvOutput::from_config(&settings, None).expect("config");

        let mut e1 = Event::new("x");
        e1.set("name", EventValue::String("John".into()));
        e1.set("age", EventValue::Integer(30));
        e1.set("city", EventValue::String("Tokyo".into()));
        let mut e2 = Event::new("y");
        e2.set("name", EventValue::String("Jane".into()));
        // age missing → empty cell
        e2.set("city", EventValue::String("Osaka".into()));

        output.output(vec![e1, e2]).await.expect("output");

        let content = std::fs::read_to_string(&path).expect("read");
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0], "John,30,Tokyo");
        assert_eq!(lines[1], "Jane,,Osaka");
    }

    #[tokio::test]
    async fn test_csv_output_appends() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("append.csv");
        let settings = serde_json::json!({
            "path": path.to_string_lossy(),
            "fields": ["v"]
        });
        let output = CsvOutput::from_config(&settings, None).expect("config");

        let mut e1 = Event::new("x");
        e1.set("v", EventValue::String("first".into()));
        output.output(vec![e1]).await.expect("output 1");

        let mut e2 = Event::new("y");
        e2.set("v", EventValue::String("second".into()));
        output.output(vec![e2]).await.expect("output 2");

        let content = std::fs::read_to_string(&path).expect("read");
        assert_eq!(content.lines().count(), 2);
        assert!(content.contains("first"));
        assert!(content.contains("second"));
    }

    #[tokio::test]
    async fn test_csv_output_quotes_separator() {
        // A value containing the separator must be quoted by the csv crate.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("quote.csv");
        let settings = serde_json::json!({
            "path": path.to_string_lossy(),
            "fields": ["msg", "n"]
        });
        let output = CsvOutput::from_config(&settings, None).expect("config");
        let mut e = Event::new("x");
        e.set("msg", EventValue::String("hello, world".into()));
        e.set("n", EventValue::Integer(1));
        output.output(vec![e]).await.expect("output");

        let content = std::fs::read_to_string(&path).expect("read");
        assert_eq!(content.trim(), "\"hello, world\",1");
    }

    #[tokio::test]
    async fn test_csv_output_custom_separator() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("sep.csv");
        let settings = serde_json::json!({
            "path": path.to_string_lossy(),
            "fields": ["a", "b"],
            "csv_options": { "separator": ";" }
        });
        let output = CsvOutput::from_config(&settings, None).expect("config");
        let mut e = Event::new("x");
        e.set("a", EventValue::String("1".into()));
        e.set("b", EventValue::String("2".into()));
        output.output(vec![e]).await.expect("output");

        let content = std::fs::read_to_string(&path).expect("read");
        assert_eq!(content.trim(), "1;2");
    }
}
