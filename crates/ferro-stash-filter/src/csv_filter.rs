// SPDX-License-Identifier: Apache-2.0
//! CSV filter — parse CSV-formatted fields into event fields.

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::FilterPlugin;

#[derive(Debug)]
pub struct CsvFilter {
    source: String,
    columns: Vec<String>,
    separator: u8,
    quote_char: u8,
    skip_header: bool,
    condition: Option<Condition>,
}

impl CsvFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let source = settings
            .get("source")
            .and_then(|v| v.as_str())
            .unwrap_or("message")
            .to_string();

        let columns = if let Some(arr) = settings.get("columns").and_then(|v| v.as_array()) {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        } else {
            Vec::new()
        };

        let separator = settings
            .get("separator")
            .and_then(|v| v.as_str())
            .and_then(|s| s.as_bytes().first().copied())
            .unwrap_or(b',');

        let quote_char = settings
            .get("quote_char")
            .and_then(|v| v.as_str())
            .and_then(|s| s.as_bytes().first().copied())
            .unwrap_or(b'"');

        let skip_header = settings
            .get("skip_header")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        Ok(Self {
            source,
            columns,
            separator,
            quote_char,
            skip_header,
            condition,
        })
    }
}

#[async_trait]
impl FilterPlugin for CsvFilter {
    fn name(&self) -> &'static str {
        "csv"
    }

    async fn filter(&self, mut event: Event) -> Result<Vec<Event>> {
        let csv_data = match event.get(&self.source) {
            Some(v) => v.to_string_lossy(),
            None => return Ok(vec![event]),
        };

        let mut rdr = csv::ReaderBuilder::new()
            .delimiter(self.separator)
            .quote(self.quote_char)
            .has_headers(self.skip_header)
            .from_reader(csv_data.as_bytes());

        // Read the first record (or the one after header if skip_header is true)
        let record = if self.skip_header {
            // With has_headers(true), csv crate auto-skips first row
            let mut iter = rdr.records();
            match iter.next() {
                Some(Ok(r)) => r,
                _ => return Ok(vec![event]),
            }
        } else {
            // has_headers(false), read the first record
            let mut iter = rdr.records();
            match iter.next() {
                Some(Ok(r)) => r,
                _ => return Ok(vec![event]),
            }
        };

        for (i, field_value) in record.iter().enumerate() {
            let field_name = if i < self.columns.len() {
                self.columns[i].clone()
            } else {
                format!("column{}", i + 1)
            };
            event.set(field_name, EventValue::String(field_value.to_string()));
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

    #[tokio::test]
    async fn test_csv_basic() {
        let settings = serde_json::json!({
            "source": "message",
            "columns": ["name", "age", "city"]
        });
        let filter = CsvFilter::from_config(&settings, None).expect("config");
        let event = Event::new("John,30,Tokyo");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("name"),
            Some(&EventValue::String("John".into()))
        );
        assert_eq!(result[0].get("age"), Some(&EventValue::String("30".into())));
        assert_eq!(
            result[0].get("city"),
            Some(&EventValue::String("Tokyo".into()))
        );
    }

    #[tokio::test]
    async fn test_csv_custom_separator() {
        let settings = serde_json::json!({
            "source": "message",
            "columns": ["a", "b", "c"],
            "separator": ";"
        });
        let filter = CsvFilter::from_config(&settings, None).expect("config");
        let event = Event::new("x;y;z");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].get("a"), Some(&EventValue::String("x".into())));
        assert_eq!(result[0].get("b"), Some(&EventValue::String("y".into())));
        assert_eq!(result[0].get("c"), Some(&EventValue::String("z".into())));
    }

    #[tokio::test]
    async fn test_csv_quoted_fields() {
        let settings = serde_json::json!({
            "source": "message",
            "columns": ["name", "desc"]
        });
        let filter = CsvFilter::from_config(&settings, None).expect("config");
        let event = Event::new(r#""John","Hello, World""#);
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("name"),
            Some(&EventValue::String("John".into()))
        );
        assert_eq!(
            result[0].get("desc"),
            Some(&EventValue::String("Hello, World".into()))
        );
    }

    #[tokio::test]
    async fn test_csv_auto_column_names() {
        let settings = serde_json::json!({
            "source": "message"
        });
        let filter = CsvFilter::from_config(&settings, None).expect("config");
        let event = Event::new("a,b,c");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("column1"),
            Some(&EventValue::String("a".into()))
        );
        assert_eq!(
            result[0].get("column2"),
            Some(&EventValue::String("b".into()))
        );
    }

    #[test]
    fn test_csv_name() {
        let settings = serde_json::json!({});
        let filter = CsvFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.name(), "csv");
    }
}
