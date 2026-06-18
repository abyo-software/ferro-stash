// SPDX-License-Identifier: Apache-2.0
//! CSV codec — decode/encode CSV data.

use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};

use crate::Codec;

/// CSV codec configuration.
#[derive(Debug, Clone)]
pub struct CsvCodec {
    /// Column names. If empty, columns are named `column1`, `column2`, etc.
    pub columns: Vec<String>,
    /// Separator character (default: `,`).
    pub separator: u8,
    /// Quote character (default: `"`).
    pub quote: u8,
    /// Whether the first line is a header.
    pub has_header: bool,
    /// Target field (None = merge into root).
    pub target: Option<String>,
}

impl Default for CsvCodec {
    fn default() -> Self {
        Self {
            columns: Vec::new(),
            separator: b',',
            quote: b'"',
            has_header: false,
            target: None,
        }
    }
}

impl CsvCodec {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let columns = settings
            .get("columns")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let separator = settings
            .get("separator")
            .and_then(|v| v.as_str())
            .and_then(|s| s.as_bytes().first().copied())
            .unwrap_or(b',');
        let quote = settings
            .get("quote")
            .and_then(|v| v.as_str())
            .and_then(|s| s.as_bytes().first().copied())
            .unwrap_or(b'"');
        let has_header = settings
            .get("has_header")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        let target = settings
            .get("target")
            .and_then(|v| v.as_str())
            .map(String::from);

        Ok(Self {
            columns,
            separator,
            quote,
            has_header,
            target,
        })
    }

    /// Reconstruct a single record's source text from its parsed fields,
    /// using the same separator/quote the parser used so the resulting
    /// `message` is faithful to the record (and record-sized, not
    /// full-input-sized).
    ///
    /// Uses the `csv` writer so quoting matches the parser's dialect. On
    /// the (unexpected) event of a writer error, falls back to a plain
    /// separator-join — never panics (production path: no
    /// `unwrap`/`expect`).
    fn reconstruct_record(&self, record: &csv::StringRecord) -> String {
        let mut wtr = csv::WriterBuilder::new()
            .delimiter(self.separator)
            .quote(self.quote)
            // The writer appends a record terminator we trim off below;
            // keep it as the default `\n` so behaviour is deterministic.
            .from_writer(Vec::new());

        let faithful = wtr
            .write_record(record.iter())
            .ok()
            .and_then(|()| wtr.flush().ok())
            .and_then(|()| wtr.into_inner().ok())
            .map(|bytes| String::from_utf8_lossy(&bytes).trim_end().to_string());

        faithful.unwrap_or_else(|| {
            // Fallback: join fields with the configured separator.
            let sep = String::from_utf8_lossy(&[self.separator]).into_owned();
            record
                .iter()
                .collect::<Vec<_>>()
                .join(&sep)
        })
    }
}

impl Codec for CsvCodec {
    fn name(&self) -> &'static str {
        "csv"
    }

    fn decode(&self, data: &[u8]) -> Result<Vec<Event>> {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .delimiter(self.separator)
            .quote(self.quote)
            .from_reader(data);

        let mut events = Vec::new();

        for record_result in reader.records() {
            let record = record_result
                .map_err(|e| FerroStashError::Codec(format!("CSV parse error: {e}")))?;

            let mut event = Event::empty();
            for (i, field) in record.iter().enumerate() {
                let key = if i < self.columns.len() {
                    self.columns[i].clone()
                } else {
                    format!("column{}", i + 1)
                };
                event.set(key, EventValue::String(field.to_string()));
            }
            // Each event's `message` is ITS OWN record's text, not the
            // entire input. Previously this set `message` to a fresh
            // full-input copy per event, which is both semantically wrong
            // (every event got the whole input) and O(N×R) live memory
            // (R full-input copies retained simultaneously → O(N²) for
            // many short records → OOM). Reconstruct the record from its
            // own fields using the configured separator/quote so the
            // message is faithful and record-sized.
            event.set_message(self.reconstruct_record(&record));
            events.push(event);
        }

        if events.is_empty() {
            return Err(FerroStashError::Codec("empty CSV data".to_string()));
        }

        Ok(events)
    }

    fn encode(&self, event: &Event) -> Result<Vec<u8>> {
        let mut wtr = csv::WriterBuilder::new()
            .delimiter(self.separator)
            .quote(self.quote)
            .from_writer(Vec::new());

        if self.columns.is_empty() {
            // Output all fields
            let values: Vec<String> = event
                .fields()
                .values()
                .map(ferro_stash_core::EventValue::to_string_lossy)
                .collect();
            wtr.write_record(&values)
                .map_err(|e| FerroStashError::Codec(format!("CSV write error: {e}")))?;
        } else {
            let values: Vec<String> = self
                .columns
                .iter()
                .map(|col| {
                    event
                        .get(col)
                        .map(ferro_stash_core::EventValue::to_string_lossy)
                        .unwrap_or_default()
                })
                .collect();
            wtr.write_record(&values)
                .map_err(|e| FerroStashError::Codec(format!("CSV write error: {e}")))?;
        }

        wtr.flush()
            .map_err(|e| FerroStashError::Codec(format!("CSV flush error: {e}")))?;
        let bytes = wtr
            .into_inner()
            .map_err(|e| FerroStashError::Codec(format!("CSV finalize error: {e}")))?;
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_csv_decode_with_columns() {
        let codec = CsvCodec {
            columns: vec!["name".into(), "age".into(), "city".into()],
            ..Default::default()
        };
        let event = codec
            .decode(b"Alice,30,Tokyo")
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(event.get("name"), Some(&EventValue::String("Alice".into())));
        assert_eq!(event.get("age"), Some(&EventValue::String("30".into())));
        assert_eq!(event.get("city"), Some(&EventValue::String("Tokyo".into())));
    }

    #[test]
    fn test_csv_decode_auto_columns() {
        let codec = CsvCodec::default();
        let event = codec
            .decode(b"a,b,c")
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(event.get("column1"), Some(&EventValue::String("a".into())));
        assert_eq!(event.get("column2"), Some(&EventValue::String("b".into())));
    }

    #[test]
    fn test_csv_encode() {
        let codec = CsvCodec {
            columns: vec!["name".into(), "age".into()],
            ..Default::default()
        };
        let mut event = Event::empty();
        event.set("name", EventValue::String("Alice".into()));
        event.set("age", EventValue::String("30".into()));
        let bytes = codec.encode(&event).expect("encode");
        let text = String::from_utf8(bytes).expect("utf8");
        assert!(text.contains("Alice"));
        assert!(text.contains("30"));
    }

    #[test]
    fn test_csv_custom_separator() {
        let codec = CsvCodec {
            columns: vec!["a".into(), "b".into()],
            separator: b'\t',
            ..Default::default()
        };
        let event = codec
            .decode(b"foo\tbar")
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(event.get("a"), Some(&EventValue::String("foo".into())));
        assert_eq!(event.get("b"), Some(&EventValue::String("bar".into())));
    }

    /// DD round-16 MEDIUM finding: each event's `message` must be ITS OWN
    /// record's text, not the entire multi-record input (the old code set
    /// `message = raw_full_input.trim_end()` per event, which was wrong
    /// and O(N×R) live memory). With multiple records, each event's
    /// `message` should be that record's own line.
    #[test]
    fn test_csv_per_record_message() {
        let codec = CsvCodec {
            columns: vec!["name".into(), "age".into()],
            ..Default::default()
        };
        let events = codec
            .decode(b"Alice,30\nBob,25\nCarol,40")
            .expect("decode");
        assert_eq!(events.len(), 3, "expected one event per record");

        assert_eq!(
            events[0].get("message"),
            Some(&EventValue::String("Alice,30".into())),
            "event message must be its own record, not the full input"
        );
        assert_eq!(
            events[1].get("message"),
            Some(&EventValue::String("Bob,25".into()))
        );
        assert_eq!(
            events[2].get("message"),
            Some(&EventValue::String("Carol,40".into()))
        );
    }

    /// The reconstructed per-record message must be faithful to the
    /// configured dialect: a field containing the separator is quoted
    /// using the configured quote char, matching how the parser read it.
    #[test]
    fn test_csv_per_record_message_quoting() {
        let codec = CsvCodec {
            columns: vec!["a".into(), "b".into()],
            ..Default::default()
        };
        // Second field contains a comma, so it was quoted on input.
        let events = codec
            .decode(b"plain,\"has,comma\"")
            .expect("decode");
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].get("a"),
            Some(&EventValue::String("plain".into()))
        );
        assert_eq!(
            events[0].get("b"),
            Some(&EventValue::String("has,comma".into()))
        );
        // The message round-trips the quoting of the embedded separator.
        assert_eq!(
            events[0].get("message"),
            Some(&EventValue::String("plain,\"has,comma\"".into()))
        );
    }
}
