// SPDX-License-Identifier: Apache-2.0
//! Translate filter — lookup table replacement for field values.

use std::collections::HashMap;

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::FilterPlugin;

#[derive(Debug)]
pub struct TranslateFilter {
    field: String,
    destination: String,
    dictionary: HashMap<String, String>,
    regex_dictionary: Vec<(regex::Regex, String)>,
    fallback: Option<String>,
    exact: bool,
    condition: Option<Condition>,
}

impl TranslateFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        // Logstash 8.x renamed the lookup/output keys to `source`/`target`
        // (the legacy `field`/`destination` names are still accepted here for
        // backwards-compat). Modern names win when both are present.
        let field = settings
            .get("source")
            .or_else(|| settings.get("field"))
            .and_then(|v| v.as_str())
            .unwrap_or("message")
            .to_string();

        let destination = settings
            .get("target")
            .or_else(|| settings.get("destination"))
            .and_then(|v| v.as_str())
            .unwrap_or("translation")
            .to_string();

        let fallback = settings
            .get("fallback")
            .and_then(|v| v.as_str())
            .map(String::from);

        let exact = settings
            .get("exact")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        let use_regex = settings
            .get("regex")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let mut dictionary = HashMap::new();
        let mut regex_dictionary = Vec::new();

        if let Some(dict) = settings.get("dictionary").and_then(|v| v.as_object()) {
            for (k, v) in dict {
                if let Some(val) = v.as_str() {
                    if use_regex {
                        if let Ok(re) = regex::Regex::new(k) {
                            regex_dictionary.push((re, val.to_string()));
                        }
                    } else {
                        dictionary.insert(k.clone(), val.to_string());
                    }
                }
            }
        }

        // Load from dictionary_path if specified (line format: "key,value" or "key: value")
        if let Some(path) = settings.get("dictionary_path").and_then(|v| v.as_str()) {
            if let Ok(content) = std::fs::read_to_string(path) {
                for line in content.lines() {
                    let line = line.trim();
                    if line.is_empty() || line.starts_with('#') {
                        continue;
                    }
                    // Try YAML-style "key: value" first, then CSV "key,value"
                    let parts: Vec<&str> = if line.contains(": ") {
                        line.splitn(2, ": ").collect()
                    } else {
                        line.splitn(2, ',').collect()
                    };
                    if parts.len() == 2 {
                        let k = parts[0].trim().trim_matches('"');
                        let v = parts[1].trim().trim_matches('"');
                        dictionary.insert(k.to_string(), v.to_string());
                    }
                }
            }
        }

        Ok(Self {
            field,
            destination,
            dictionary,
            regex_dictionary,
            fallback,
            exact,
            condition,
        })
    }
}

#[async_trait]
impl FilterPlugin for TranslateFilter {
    fn name(&self) -> &'static str {
        "translate"
    }

    async fn filter(&self, mut event: Event) -> Result<Vec<Event>> {
        let field_value = match event.get(&self.field) {
            Some(v) => v.to_string_lossy(),
            None => return Ok(vec![event]),
        };

        let translation = if !self.regex_dictionary.is_empty() {
            // Regex mode
            self.regex_dictionary
                .iter()
                .find(|(re, _)| re.is_match(&field_value))
                .map(|(_, v)| v.clone())
        } else if self.exact {
            self.dictionary.get(&field_value).cloned()
        } else {
            // Substring match
            self.dictionary
                .iter()
                .find(|(k, _)| field_value.contains(k.as_str()))
                .map(|(_, v)| v.clone())
        };

        if let Some(translated) = translation {
            event.set(self.destination.clone(), EventValue::String(translated));
        } else if let Some(ref fb) = self.fallback {
            event.set(self.destination.clone(), EventValue::String(fb.clone()));
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
    async fn test_translate_exact_match() {
        let settings = serde_json::json!({
            "field": "status",
            "destination": "status_text",
            "dictionary": {
                "200": "OK",
                "404": "Not Found",
                "500": "Internal Server Error"
            }
        });
        let filter = TranslateFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("status", EventValue::String("200".into()));
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("status_text"),
            Some(&EventValue::String("OK".into()))
        );
    }

    #[tokio::test]
    async fn test_translate_fallback() {
        let settings = serde_json::json!({
            "field": "status",
            "destination": "status_text",
            "dictionary": { "200": "OK" },
            "fallback": "Unknown"
        });
        let filter = TranslateFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("status", EventValue::String("999".into()));
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("status_text"),
            Some(&EventValue::String("Unknown".into()))
        );
    }

    #[tokio::test]
    async fn test_translate_no_match_no_fallback() {
        let settings = serde_json::json!({
            "field": "status",
            "destination": "status_text",
            "dictionary": { "200": "OK" }
        });
        let filter = TranslateFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("status", EventValue::String("999".into()));
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].get("status_text").is_none());
    }

    #[tokio::test]
    async fn test_translate_missing_field() {
        let settings = serde_json::json!({
            "field": "nonexistent",
            "dictionary": { "a": "b" }
        });
        let filter = TranslateFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].message(), Some("test"));
    }

    #[tokio::test]
    async fn test_translate_source_target_keys() {
        // Logstash 8.x parity: `source`/`target` are honoured (not just the
        // legacy field/destination), and an inline dictionary resolves.
        let settings = serde_json::json!({
            "source": "code",
            "target": "status",
            "dictionary": { "200": "OK", "404": "NotFound" },
            "fallback": "Unknown"
        });
        let filter = TranslateFilter::from_config(&settings, None).expect("config");
        let mut hit = Event::new("test");
        hit.set("code", EventValue::String("200".into()));
        let r = filter.filter(hit).await.expect("filter");
        assert_eq!(r[0].get("status"), Some(&EventValue::String("OK".into())));
        assert!(r[0].get("translation").is_none()); // legacy default unused

        let mut miss = Event::new("test");
        miss.set("code", EventValue::String("418".into()));
        let r = filter.filter(miss).await.expect("filter");
        assert_eq!(
            r[0].get("status"),
            Some(&EventValue::String("Unknown".into()))
        );
    }

    #[test]
    fn test_translate_name() {
        let settings = serde_json::json!({});
        let filter = TranslateFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.name(), "translate");
    }
}
