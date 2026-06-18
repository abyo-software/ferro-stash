// SPDX-License-Identifier: Apache-2.0
//! Prune filter — remove fields matching/not matching whitelist/blacklist patterns.

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::Event;
use ferro_stash_core::plugin::FilterPlugin;

#[derive(Debug)]
pub struct PruneFilter {
    whitelist_names: Vec<regex::Regex>,
    blacklist_names: Vec<regex::Regex>,
    whitelist_values: Vec<regex::Regex>,
    blacklist_values: Vec<regex::Regex>,
    condition: Option<Condition>,
}

impl PruneFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let whitelist_names =
            compile_patterns(settings.get("whitelist_names").and_then(|v| v.as_array()));
        let blacklist_names =
            compile_patterns(settings.get("blacklist_names").and_then(|v| v.as_array()));
        let whitelist_values =
            compile_patterns(settings.get("whitelist_values").and_then(|v| v.as_array()));
        let blacklist_values =
            compile_patterns(settings.get("blacklist_values").and_then(|v| v.as_array()));

        Ok(Self {
            whitelist_names,
            blacklist_names,
            whitelist_values,
            blacklist_values,
            condition,
        })
    }
}

fn compile_patterns(arr: Option<&Vec<serde_json::Value>>) -> Vec<regex::Regex> {
    arr.map(|patterns| {
        patterns
            .iter()
            .filter_map(|v| v.as_str().and_then(|s| regex::Regex::new(s).ok()))
            .collect()
    })
    .unwrap_or_default()
}

fn matches_any(regexes: &[regex::Regex], text: &str) -> bool {
    regexes.iter().any(|re| re.is_match(text))
}

#[async_trait]
impl FilterPlugin for PruneFilter {
    fn name(&self) -> &'static str {
        "prune"
    }

    async fn filter(&self, mut event: Event) -> Result<Vec<Event>> {
        let field_names: Vec<String> = event.field_names().cloned().collect();
        let mut to_remove = Vec::new();

        for name in &field_names {
            let mut should_remove = false;

            // If whitelist_names is set, only keep fields matching it
            if !self.whitelist_names.is_empty() && !matches_any(&self.whitelist_names, name) {
                should_remove = true;
            }

            // If blacklist_names is set, remove fields matching it
            if !self.blacklist_names.is_empty() && matches_any(&self.blacklist_names, name) {
                should_remove = true;
            }

            // Check value-based rules
            if !should_remove {
                if let Some(val) = event.get(name) {
                    let val_str = val.to_string_lossy();

                    if !self.whitelist_values.is_empty()
                        && !matches_any(&self.whitelist_values, &val_str)
                    {
                        should_remove = true;
                    }

                    if !self.blacklist_values.is_empty()
                        && matches_any(&self.blacklist_values, &val_str)
                    {
                        should_remove = true;
                    }
                }
            }

            if should_remove {
                to_remove.push(name.clone());
            }
        }

        for name in to_remove {
            event.remove(&name);
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
    use ferro_stash_core::event::EventValue;

    #[tokio::test]
    async fn test_prune_whitelist_names() {
        let settings = serde_json::json!({
            "whitelist_names": ["^message$", "^host$"]
        });
        let filter = PruneFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("hello");
        event.set("host", EventValue::String("server01".into()));
        event.set("secret", EventValue::String("password".into()));
        event.set("extra", EventValue::String("data".into()));
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_field("message"));
        assert!(result[0].has_field("host"));
        assert!(!result[0].has_field("secret"));
        assert!(!result[0].has_field("extra"));
    }

    #[tokio::test]
    async fn test_prune_blacklist_names() {
        let settings = serde_json::json!({
            "blacklist_names": ["^secret", "^temp_"]
        });
        let filter = PruneFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("hello");
        event.set("secret_key", EventValue::String("xyz".into()));
        event.set("temp_data", EventValue::String("abc".into()));
        event.set("host", EventValue::String("server".into()));
        let result = filter.filter(event).await.expect("filter");
        assert!(!result[0].has_field("secret_key"));
        assert!(!result[0].has_field("temp_data"));
        assert!(result[0].has_field("host"));
        assert!(result[0].has_field("message"));
    }

    #[tokio::test]
    async fn test_prune_blacklist_values() {
        let settings = serde_json::json!({
            "blacklist_values": ["^password$", "^secret$"]
        });
        let filter = PruneFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("hello");
        event.set("key1", EventValue::String("password".into()));
        event.set("key2", EventValue::String("normal".into()));
        event.set("key3", EventValue::String("secret".into()));
        let result = filter.filter(event).await.expect("filter");
        assert!(!result[0].has_field("key1"));
        assert!(result[0].has_field("key2"));
        assert!(!result[0].has_field("key3"));
    }

    #[tokio::test]
    async fn test_prune_empty_config() {
        let settings = serde_json::json!({});
        let filter = PruneFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("hello");
        event.set("field1", EventValue::String("value1".into()));
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_field("message"));
        assert!(result[0].has_field("field1"));
    }

    #[test]
    fn test_prune_name() {
        let settings = serde_json::json!({});
        let filter = PruneFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.name(), "prune");
    }
}
