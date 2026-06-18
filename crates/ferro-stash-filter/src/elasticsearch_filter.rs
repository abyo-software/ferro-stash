// SPDX-License-Identifier: Apache-2.0
//! Elasticsearch filter — enrich events by querying Elasticsearch.
//!
//! This is a stub implementation: actual HTTP requests are not made.
//! The filter builds the query, applies template substitution, and maps
//! result fields. In tests and non-networked environments, the event
//! data itself is used as a mock response.

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::FilterPlugin;
use indexmap::IndexMap;

#[derive(Debug)]
pub struct ElasticsearchFilter {
    /// Elasticsearch hosts (stub: not actually connected).
    #[allow(dead_code)]
    hosts: Vec<String>,
    /// Index to query.
    #[allow(dead_code)]
    index: String,
    /// Query template with `%{field}` substitution.
    query_template: String,
    /// Maximum number of results.
    result_size: usize,
    /// Fields to copy from ES result into the event.
    fields: Vec<(String, String)>,
    /// Whether to sort results.
    #[allow(dead_code)]
    enable_sort: bool,
    /// Target field for the ES result.
    target: String,
    /// Tag on failure.
    tag_on_failure: String,
    condition: Option<Condition>,
}

impl ElasticsearchFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let hosts = settings
            .get("hosts")
            .and_then(|v| v.as_array())
            .map_or_else(
                || vec!["http://localhost:9200".to_string()],
                |a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                },
            );

        let index = settings
            .get("index")
            .and_then(|v| v.as_str())
            .unwrap_or("logstash-*")
            .to_string();

        let query_template = settings
            .get("query_template")
            .or_else(|| settings.get("query"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let result_size = settings
            .get("result_size")
            .or_else(|| settings.get("size"))
            .and_then(|v| v.as_u64())
            .unwrap_or(1) as usize;

        let fields = settings
            .get("fields")
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();

        let enable_sort = settings
            .get("enable_sort")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(true);

        let target = settings
            .get("target")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let tag_on_failure = settings
            .get("tag_on_failure")
            .and_then(|v| v.as_str())
            .unwrap_or("_elasticsearch_lookup_failure")
            .to_string();

        Ok(Self {
            hosts,
            index,
            query_template,
            result_size,
            fields,
            enable_sort,
            target,
            tag_on_failure,
            condition,
        })
    }

    /// Build the query by substituting `%{field}` references from the event.
    fn build_query(&self, event: &Event) -> String {
        event.sprintf(&self.query_template)
    }

    /// Stub: simulate an ES query result using the event's own data.
    /// In a real implementation, this would perform an HTTP request.
    fn execute_query(
        &self,
        _query: &str,
        event: &Event,
    ) -> Option<Vec<IndexMap<String, EventValue>>> {
        // Return a single mock result containing the event's own fields
        // This allows the field mapping logic to be exercised in tests
        let mut result = IndexMap::new();
        for (k, v) in event.fields() {
            result.insert(k.clone(), v.clone());
        }
        Some(vec![result])
    }
}

#[async_trait]
impl FilterPlugin for ElasticsearchFilter {
    fn name(&self) -> &'static str {
        "elasticsearch"
    }

    async fn filter(&self, mut event: Event) -> Result<Vec<Event>> {
        let query = self.build_query(&event);

        match self.execute_query(&query, &event) {
            Some(results) => {
                if results.is_empty() {
                    event.add_tag(&self.tag_on_failure);
                    return Ok(vec![event]);
                }

                // Take up to result_size results
                let results: Vec<_> = results.into_iter().take(self.result_size).collect();

                // If fields mapping is configured, copy specific fields
                if !self.fields.is_empty() {
                    for result in &results {
                        for (es_field, event_field) in &self.fields {
                            if let Some(val) = result.get(es_field) {
                                let target_field = if self.target.is_empty() {
                                    event_field.clone()
                                } else {
                                    format!("{}.{}", self.target, event_field)
                                };
                                event.set(target_field, val.clone());
                            }
                        }
                    }
                } else {
                    // No field mapping — store entire result
                    let result_values: Vec<EventValue> =
                        results.into_iter().map(EventValue::Object).collect();

                    let value = if result_values.len() == 1 {
                        result_values.into_iter().next().unwrap_or(EventValue::Null)
                    } else {
                        EventValue::Array(result_values)
                    };

                    if self.target.is_empty() {
                        // Merge into root
                        if let EventValue::Object(map) = value {
                            for (k, v) in map {
                                event.set(k, v);
                            }
                        }
                    } else {
                        event.set(self.target.clone(), value);
                    }
                }
            }
            None => {
                event.add_tag(&self.tag_on_failure);
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
    async fn test_elasticsearch_basic_enrichment() {
        let settings = serde_json::json!({
            "hosts": ["http://localhost:9200"],
            "index": "users",
            "query_template": "{\"query\":{\"match\":{\"name\":\"%{name}\"}}}",
            "target": "es_result"
        });
        let filter = ElasticsearchFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("name", EventValue::String("Alice".into()));
        let result = filter.filter(event).await.expect("filter");
        // Stub returns event's own data under the target field
        assert!(result[0].has_field("es_result"));
    }

    #[tokio::test]
    async fn test_elasticsearch_field_mapping() {
        let settings = serde_json::json!({
            "index": "users",
            "query_template": "{}",
            "fields": {
                "message": "enriched_message"
            }
        });
        let filter = ElasticsearchFilter::from_config(&settings, None).expect("config");
        let event = Event::new("hello world");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("enriched_message"),
            Some(&EventValue::String("hello world".into()))
        );
    }

    #[tokio::test]
    async fn test_elasticsearch_query_template_substitution() {
        let settings = serde_json::json!({
            "query_template": "{\"term\":{\"user\":\"%{username}\"}}"
        });
        let filter = ElasticsearchFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("username", EventValue::String("bob".into()));
        let query = filter.build_query(&event);
        assert!(query.contains("bob"));
        assert!(!query.contains("%{username}"));
    }

    #[tokio::test]
    async fn test_elasticsearch_default_config() {
        let settings = serde_json::json!({});
        let filter = ElasticsearchFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.hosts, vec!["http://localhost:9200"]);
        assert_eq!(filter.index, "logstash-*");
        assert_eq!(filter.result_size, 1);
        assert!(filter.enable_sort);
    }

    #[test]
    fn test_elasticsearch_name() {
        let settings = serde_json::json!({});
        let filter = ElasticsearchFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.name(), "elasticsearch");
    }
}
