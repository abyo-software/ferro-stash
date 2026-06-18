// SPDX-License-Identifier: Apache-2.0
//! Dissect filter — structured text extraction without regex.
//!
//! Uses `%{field}` tokens with delimiter-based parsing (faster than grok for simple formats).

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::FilterPlugin;

#[derive(Debug)]
pub struct DissectFilter {
    mapping: DissectMapping,
    source: String,
    tag_on_failure: Vec<String>,
    condition: Option<Condition>,
}

#[derive(Debug)]
struct DissectMapping {
    tokens: Vec<DissectToken>,
}

#[allow(dead_code)]
#[derive(Debug)]
enum DissectToken {
    Literal(String),
    Field {
        name: String,
        append: bool,
        append_separator: Option<String>,
        indirect: bool,
        pad_right: bool,
    },
    SkipField,
}

impl DissectFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let mapping_str = settings
            .get("mapping")
            .and_then(|v| {
                if let Some(obj) = v.as_object() {
                    obj.values().next().and_then(|v| v.as_str())
                } else {
                    v.as_str()
                }
            })
            .ok_or_else(|| FerroStashError::Filter {
                plugin: "dissect".to_string(),
                message: "mapping is required".to_string(),
            })?;

        let source = settings
            .get("source")
            .and_then(|v| v.as_str())
            .or_else(|| {
                settings
                    .get("mapping")
                    .and_then(|v| v.as_object())
                    .and_then(|o| o.keys().next())
                    .map(String::as_str)
            })
            .unwrap_or("message")
            .to_string();

        let tag_on_failure = settings
            .get("tag_on_failure")
            .and_then(|v| v.as_array())
            .map_or_else(
                || vec!["_dissectfailure".to_string()],
                |a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                },
            );

        let mapping = parse_dissect_pattern(mapping_str)?;

        Ok(Self {
            mapping,
            source,
            tag_on_failure,
            condition,
        })
    }
}

fn parse_dissect_pattern(pattern: &str) -> Result<DissectMapping> {
    let mut tokens = Vec::new();
    let mut i = 0;
    let bytes = pattern.as_bytes();

    while i < bytes.len() {
        if i + 1 < bytes.len() && bytes[i] == b'%' && bytes[i + 1] == b'{' {
            i += 2;
            let start = i;
            while i < bytes.len() && bytes[i] != b'}' {
                i += 1;
            }
            let field_spec = &pattern[start..i];
            i += 1; // skip '}'

            if field_spec == "?" || field_spec.starts_with('?') {
                tokens.push(DissectToken::SkipField);
            } else {
                let mut name = field_spec.to_string();
                let mut append = false;
                let mut append_separator = None;
                let mut indirect = false;
                let mut pad_right = false;

                if name.starts_with('+') {
                    append = true;
                    name = name[1..].to_string();
                    if let Some(sep_pos) = name.find('/') {
                        append_separator = Some(name[sep_pos + 1..].to_string());
                        name = name[..sep_pos].to_string();
                    }
                }
                if name.starts_with('&') {
                    indirect = true;
                    name = name[1..].to_string();
                }
                if name.ends_with('>') {
                    pad_right = true;
                    name = name[..name.len() - 1].to_string();
                }

                tokens.push(DissectToken::Field {
                    name,
                    append,
                    append_separator,
                    indirect,
                    pad_right,
                });
            }
        } else {
            let start = i;
            while i < bytes.len()
                && !(i + 1 < bytes.len() && bytes[i] == b'%' && bytes[i + 1] == b'{')
            {
                i += 1;
            }
            tokens.push(DissectToken::Literal(pattern[start..i].to_string()));
        }
    }

    Ok(DissectMapping { tokens })
}

#[async_trait]
impl FilterPlugin for DissectFilter {
    fn name(&self) -> &'static str {
        "dissect"
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

        let mut pos = 0;
        let mut success = true;
        let mut extracted: Vec<(String, String)> = Vec::new();

        for (idx, token) in self.mapping.tokens.iter().enumerate() {
            match token {
                DissectToken::Literal(lit) => {
                    if let Some(found) = text[pos..].find(lit.as_str()) {
                        pos += found + lit.len();
                    } else {
                        success = false;
                        break;
                    }
                }
                DissectToken::Field { .. } | DissectToken::SkipField => {
                    // Find the next literal delimiter
                    let end_pos = if let Some(next_token) = self.mapping.tokens.get(idx + 1) {
                        match next_token {
                            DissectToken::Literal(lit) => {
                                text[pos..].find(lit.as_str()).map(|p| pos + p)
                            }
                            _ => Some(text.len()),
                        }
                    } else {
                        Some(text.len())
                    };

                    if let Some(end) = end_pos {
                        let value = &text[pos..end];
                        let value = if let DissectToken::Field {
                            pad_right: true, ..
                        } = token
                        {
                            value.trim_end()
                        } else {
                            value
                        };

                        if let DissectToken::Field { name, .. } = token {
                            extracted.push((name.clone(), value.to_string()));
                        }
                        pos = end;
                    } else {
                        success = false;
                        break;
                    }
                }
            }
        }

        if success {
            for (name, value) in extracted {
                event.set(name, EventValue::String(value));
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

    #[tokio::test]
    async fn test_dissect_simple() {
        let settings = serde_json::json!({
            "mapping": { "message": "%{ip} %{method} %{path} %{status}" }
        });
        let filter = DissectFilter::from_config(&settings, None).expect("config");
        let event = Event::new("192.168.1.1 GET /index.html 200");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("ip"),
            Some(&EventValue::String("192.168.1.1".into()))
        );
        assert_eq!(
            result[0].get("method"),
            Some(&EventValue::String("GET".into()))
        );
        assert_eq!(
            result[0].get("status"),
            Some(&EventValue::String("200".into()))
        );
    }

    #[tokio::test]
    async fn test_dissect_with_delimiters() {
        let settings = serde_json::json!({
            "mapping": { "message": "[%{timestamp}] %{level}: %{message_text}" }
        });
        let filter = DissectFilter::from_config(&settings, None).expect("config");
        let event = Event::new("[2024-01-15] ERROR: something went wrong");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("timestamp"),
            Some(&EventValue::String("2024-01-15".into()))
        );
        assert_eq!(
            result[0].get("level"),
            Some(&EventValue::String("ERROR".into()))
        );
    }

    #[tokio::test]
    async fn test_dissect_failure() {
        let settings = serde_json::json!({
            "mapping": { "message": "%{a}|%{b}|%{c}" }
        });
        let filter = DissectFilter::from_config(&settings, None).expect("config");
        let event = Event::new("no pipes here");
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_tag("_dissectfailure"));
    }

    #[tokio::test]
    async fn test_dissect_skip_field() {
        let settings = serde_json::json!({
            "mapping": { "message": "%{ip} %{?skip} %{path}" }
        });
        let filter = DissectFilter::from_config(&settings, None).expect("config");
        let event = Event::new("192.168.1.1 GET /index.html");
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_field("ip"));
        assert!(result[0].has_field("path"));
        assert!(!result[0].has_field("skip"));
    }

    #[tokio::test]
    async fn test_dissect_missing_source() {
        let settings = serde_json::json!({
            "mapping": { "nonexistent": "%{a} %{b}" }
        });
        let filter = DissectFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_tag("_dissectfailure"));
    }

    #[tokio::test]
    async fn test_dissect_custom_failure_tag() {
        let settings = serde_json::json!({
            "mapping": { "message": "%{a}|%{b}" },
            "tag_on_failure": ["my_error"]
        });
        let filter = DissectFilter::from_config(&settings, None).expect("config");
        let event = Event::new("no pipes");
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_tag("my_error"));
    }

    #[tokio::test]
    async fn test_dissect_string_mapping() {
        let settings = serde_json::json!({
            "mapping": "%{ip} %{method} %{path}"
        });
        let filter = DissectFilter::from_config(&settings, None).expect("config");
        let event = Event::new("10.0.0.1 POST /api");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("ip"),
            Some(&EventValue::String("10.0.0.1".into()))
        );
        assert_eq!(
            result[0].get("method"),
            Some(&EventValue::String("POST".into()))
        );
    }

    #[test]
    fn test_dissect_name() {
        let settings = serde_json::json!({
            "mapping": "%{a} %{b}"
        });
        let filter = DissectFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.name(), "dissect");
    }

    #[test]
    fn test_dissect_config_missing_mapping() {
        let settings = serde_json::json!({});
        assert!(DissectFilter::from_config(&settings, None).is_err());
    }

    #[tokio::test]
    async fn test_dissect_pad_right() {
        let settings = serde_json::json!({
            "mapping": { "message": "%{name>} %{value}" }
        });
        let filter = DissectFilter::from_config(&settings, None).expect("config");
        let event = Event::new("Alice    42");
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_field("name"));
    }
}
