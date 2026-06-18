// SPDX-License-Identifier: Apache-2.0
//! Mutate filter — general-purpose field manipulation.

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::FilterPlugin;

#[derive(Debug)]
pub struct MutateFilter {
    operations: Vec<MutateOp>,
    condition: Option<Condition>,
}

#[allow(dead_code)]
#[derive(Debug)]
enum MutateOp {
    Rename(String, String),
    Replace(String, String),
    Uppercase(Vec<String>),
    Lowercase(Vec<String>),
    Strip(Vec<String>),
    Remove(Vec<String>),
    Copy(String, String),
    Merge(String, String),
    Gsub(Vec<GsubEntry>),
    Convert(String, ConvertType),
    Split(String, String),
    Join(String, String),
    AddField(String, String),
    AddTag(Vec<String>),
    RemoveTag(Vec<String>),
    Coerce(String, String),
}

#[derive(Debug)]
struct GsubEntry {
    field: String,
    pattern: String,
    replacement: String,
}

#[derive(Debug)]
enum ConvertType {
    Integer,
    Float,
    String,
    Boolean,
}

impl MutateFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let mut operations = Vec::new();

        if let Some(obj) = settings.get("rename").and_then(|v| v.as_object()) {
            for (from, to) in obj {
                if let Some(to_str) = to.as_str() {
                    operations.push(MutateOp::Rename(from.clone(), to_str.to_string()));
                }
            }
        }

        if let Some(obj) = settings.get("replace").and_then(|v| v.as_object()) {
            for (field, value) in obj {
                if let Some(val_str) = value.as_str() {
                    operations.push(MutateOp::Replace(field.clone(), val_str.to_string()));
                }
            }
        }

        if let Some(arr) = settings.get("uppercase").and_then(|v| v.as_array()) {
            let fields: Vec<String> = arr
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
            if !fields.is_empty() {
                operations.push(MutateOp::Uppercase(fields));
            }
        }

        if let Some(arr) = settings.get("lowercase").and_then(|v| v.as_array()) {
            let fields: Vec<String> = arr
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
            if !fields.is_empty() {
                operations.push(MutateOp::Lowercase(fields));
            }
        }

        if let Some(arr) = settings.get("strip").and_then(|v| v.as_array()) {
            let fields: Vec<String> = arr
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
            if !fields.is_empty() {
                operations.push(MutateOp::Strip(fields));
            }
        }

        if let Some(arr) = settings.get("remove_field").and_then(|v| v.as_array()) {
            let fields: Vec<String> = arr
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
            if !fields.is_empty() {
                operations.push(MutateOp::Remove(fields));
            }
        }

        if let Some(obj) = settings.get("copy").and_then(|v| v.as_object()) {
            for (src, dst) in obj {
                if let Some(dst_str) = dst.as_str() {
                    operations.push(MutateOp::Copy(src.clone(), dst_str.to_string()));
                }
            }
        }

        if let Some(arr) = settings.get("gsub").and_then(|v| v.as_array()) {
            let mut entries = Vec::new();
            let mut iter = arr.iter();
            while let Some(field_val) = iter.next() {
                if let (Some(field), Some(pattern), Some(replacement)) = (
                    field_val.as_str(),
                    iter.next().and_then(|v| v.as_str()),
                    iter.next().and_then(|v| v.as_str()),
                ) {
                    entries.push(GsubEntry {
                        field: field.to_string(),
                        pattern: pattern.to_string(),
                        replacement: replacement.to_string(),
                    });
                }
            }
            if !entries.is_empty() {
                operations.push(MutateOp::Gsub(entries));
            }
        }

        if let Some(obj) = settings.get("convert").and_then(|v| v.as_object()) {
            for (field, type_name) in obj {
                if let Some(t) = type_name.as_str() {
                    let convert_type = match t {
                        "integer" => ConvertType::Integer,
                        "float" => ConvertType::Float,
                        "string" => ConvertType::String,
                        "boolean" => ConvertType::Boolean,
                        _ => continue,
                    };
                    operations.push(MutateOp::Convert(field.clone(), convert_type));
                }
            }
        }

        if let Some(obj) = settings.get("split").and_then(|v| v.as_object()) {
            for (field, delim) in obj {
                if let Some(d) = delim.as_str() {
                    operations.push(MutateOp::Split(field.clone(), d.to_string()));
                }
            }
        }

        if let Some(obj) = settings.get("join").and_then(|v| v.as_object()) {
            for (field, delim) in obj {
                if let Some(d) = delim.as_str() {
                    operations.push(MutateOp::Join(field.clone(), d.to_string()));
                }
            }
        }

        if let Some(obj) = settings.get("add_field").and_then(|v| v.as_object()) {
            for (field, value) in obj {
                if let Some(v) = value.as_str() {
                    operations.push(MutateOp::AddField(field.clone(), v.to_string()));
                }
            }
        }

        if let Some(arr) = settings.get("add_tag").and_then(|v| v.as_array()) {
            let tags: Vec<String> = arr
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
            if !tags.is_empty() {
                operations.push(MutateOp::AddTag(tags));
            }
        }

        if let Some(arr) = settings.get("remove_tag").and_then(|v| v.as_array()) {
            let tags: Vec<String> = arr
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
            if !tags.is_empty() {
                operations.push(MutateOp::RemoveTag(tags));
            }
        }

        Ok(Self {
            operations,
            condition,
        })
    }
}

#[async_trait]
impl FilterPlugin for MutateFilter {
    fn name(&self) -> &'static str {
        "mutate"
    }

    async fn filter(&self, mut event: Event) -> Result<Vec<Event>> {
        for op in &self.operations {
            match op {
                MutateOp::Rename(from, to) => {
                    if let Some(val) = event.remove(from) {
                        event.set(to.clone(), val);
                    }
                }
                MutateOp::Replace(field, template) => {
                    let value = event.sprintf(template);
                    event.set(field.clone(), EventValue::String(value));
                }
                MutateOp::Uppercase(fields) => {
                    for field in fields {
                        if let Some(val) = event.get(field) {
                            let upper = val.to_string_lossy().to_uppercase();
                            event.set(field.clone(), EventValue::String(upper));
                        }
                    }
                }
                MutateOp::Lowercase(fields) => {
                    for field in fields {
                        if let Some(val) = event.get(field) {
                            let lower = val.to_string_lossy().to_lowercase();
                            event.set(field.clone(), EventValue::String(lower));
                        }
                    }
                }
                MutateOp::Strip(fields) => {
                    for field in fields {
                        if let Some(val) = event.get(field) {
                            let stripped = val.to_string_lossy().trim().to_string();
                            event.set(field.clone(), EventValue::String(stripped));
                        }
                    }
                }
                MutateOp::Remove(fields) => {
                    for field in fields {
                        event.remove(field);
                    }
                }
                MutateOp::Copy(src, dst) => {
                    if let Some(val) = event.get(src).cloned() {
                        event.set(dst.clone(), val);
                    }
                }
                MutateOp::Merge(src, dst) => {
                    // Merge src array into dst array
                    if let (Some(src_val), Some(dst_val)) =
                        (event.get(src).cloned(), event.get(dst).cloned())
                    {
                        let mut dst_arr = match dst_val {
                            EventValue::Array(a) => a,
                            other => vec![other],
                        };
                        match src_val {
                            EventValue::Array(a) => dst_arr.extend(a),
                            other => dst_arr.push(other),
                        }
                        event.set(dst.clone(), EventValue::Array(dst_arr));
                    }
                }
                MutateOp::Gsub(entries) => {
                    for entry in entries {
                        if let Some(val) = event.get(&entry.field) {
                            let text = val.to_string_lossy();
                            if let Ok(re) = regex::Regex::new(&entry.pattern) {
                                let replaced = re.replace_all(&text, &entry.replacement);
                                event.set(
                                    entry.field.clone(),
                                    EventValue::String(replaced.to_string()),
                                );
                            }
                        }
                    }
                }
                MutateOp::Convert(field, convert_type) => {
                    if let Some(val) = event.get(field).cloned() {
                        let text = val.to_string_lossy();
                        let new_val = match convert_type {
                            ConvertType::Integer => {
                                text.parse::<i64>().map(EventValue::Integer).unwrap_or(val)
                            }
                            ConvertType::Float => {
                                text.parse::<f64>().map(EventValue::Float).unwrap_or(val)
                            }
                            ConvertType::String => EventValue::String(text),
                            ConvertType::Boolean => {
                                let b = matches!(text.as_str(), "true" | "1" | "yes" | "t" | "y");
                                EventValue::Boolean(b)
                            }
                        };
                        event.set(field.clone(), new_val);
                    }
                }
                MutateOp::Split(field, delim) => {
                    if let Some(val) = event.get(field) {
                        let text = val.to_string_lossy();
                        let parts: Vec<EventValue> = text
                            .split(delim.as_str())
                            .map(|s| EventValue::String(s.to_string()))
                            .collect();
                        event.set(field.clone(), EventValue::Array(parts));
                    }
                }
                MutateOp::Join(field, delim) => {
                    if let Some(EventValue::Array(arr)) = event.get(field).cloned() {
                        let joined: String = arr
                            .iter()
                            .map(ferro_stash_core::EventValue::to_string_lossy)
                            .collect::<Vec<_>>()
                            .join(delim);
                        event.set(field.clone(), EventValue::String(joined));
                    }
                }
                MutateOp::AddField(field, template) => {
                    let value = event.sprintf(template);
                    event.set(field.clone(), EventValue::String(value));
                }
                MutateOp::AddTag(tags) => {
                    for tag in tags {
                        let resolved = event.sprintf(tag);
                        event.add_tag(resolved);
                    }
                }
                MutateOp::RemoveTag(tags) => {
                    for tag in tags {
                        event.remove_tag(tag);
                    }
                }
                MutateOp::Coerce(field, type_name) => {
                    // Same as convert
                    if let Some(val) = event.get(field).cloned() {
                        let text = val.to_string_lossy();
                        let new_val = match type_name.as_str() {
                            "integer" => {
                                text.parse::<i64>().map(EventValue::Integer).unwrap_or(val)
                            }
                            "float" => text.parse::<f64>().map(EventValue::Float).unwrap_or(val),
                            _ => EventValue::String(text),
                        };
                        event.set(field.clone(), new_val);
                    }
                }
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
    async fn test_mutate_rename() {
        let settings = serde_json::json!({
            "rename": { "old_field": "new_field" }
        });
        let filter = MutateFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("old_field", EventValue::String("value".into()));
        let result = filter.filter(event).await.expect("filter");
        assert!(!result[0].has_field("old_field"));
        assert_eq!(
            result[0].get("new_field"),
            Some(&EventValue::String("value".into()))
        );
    }

    #[tokio::test]
    async fn test_mutate_uppercase() {
        let settings = serde_json::json!({
            "uppercase": ["message"]
        });
        let filter = MutateFilter::from_config(&settings, None).expect("config");
        let event = Event::new("hello world");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].message(), Some("HELLO WORLD"));
    }

    #[tokio::test]
    async fn test_mutate_remove_field() {
        let settings = serde_json::json!({
            "remove_field": ["message"]
        });
        let filter = MutateFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        assert!(!result[0].has_field("message"));
    }

    #[tokio::test]
    async fn test_mutate_gsub() {
        let settings = serde_json::json!({
            "gsub": ["message", "\\s+", "_"]
        });
        let filter = MutateFilter::from_config(&settings, None).expect("config");
        let event = Event::new("hello world test");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].message(), Some("hello_world_test"));
    }

    #[tokio::test]
    async fn test_mutate_convert() {
        let settings = serde_json::json!({
            "convert": { "port": "integer" }
        });
        let filter = MutateFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("port", EventValue::String("8080".into()));
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].get("port"), Some(&EventValue::Integer(8080)));
    }

    #[tokio::test]
    async fn test_mutate_split_join() {
        let settings = serde_json::json!({
            "split": { "path": "/" }
        });
        let filter = MutateFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("path", EventValue::String("a/b/c".into()));
        let result = filter.filter(event).await.expect("filter");
        let arr = result[0].get("path").and_then(|v| v.as_array());
        assert!(arr.is_some());
        assert_eq!(arr.map(Vec::len), Some(3));
    }

    #[tokio::test]
    async fn test_mutate_add_tag() {
        let settings = serde_json::json!({
            "add_tag": ["processed"]
        });
        let filter = MutateFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_tag("processed"));
    }

    #[tokio::test]
    async fn test_mutate_lowercase() {
        let settings = serde_json::json!({ "lowercase": ["message"] });
        let filter = MutateFilter::from_config(&settings, None).expect("config");
        let event = Event::new("HELLO WORLD");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].message(), Some("hello world"));
    }

    #[tokio::test]
    async fn test_mutate_strip() {
        let settings = serde_json::json!({ "strip": ["message"] });
        let filter = MutateFilter::from_config(&settings, None).expect("config");
        let event = Event::new("  hello  ");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].message(), Some("hello"));
    }

    #[tokio::test]
    async fn test_mutate_replace() {
        let settings = serde_json::json!({
            "replace": { "message": "replaced: %{host}" }
        });
        let filter = MutateFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("original");
        event.set("host", EventValue::String("server01".into()));
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].message(), Some("replaced: server01"));
    }

    #[tokio::test]
    async fn test_mutate_copy() {
        let settings = serde_json::json!({ "copy": { "message": "message_copy" } });
        let filter = MutateFilter::from_config(&settings, None).expect("config");
        let event = Event::new("hello");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("message_copy"),
            Some(&EventValue::String("hello".into()))
        );
        assert_eq!(result[0].message(), Some("hello"));
    }

    #[tokio::test]
    async fn test_mutate_add_field() {
        let settings = serde_json::json!({
            "add_field": { "env": "production" }
        });
        let filter = MutateFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("env"),
            Some(&EventValue::String("production".into()))
        );
    }

    #[tokio::test]
    async fn test_mutate_remove_tag() {
        let settings = serde_json::json!({ "remove_tag": ["old_tag"] });
        let filter = MutateFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.add_tag("old_tag");
        event.add_tag("keep_tag");
        let result = filter.filter(event).await.expect("filter");
        assert!(!result[0].has_tag("old_tag"));
        assert!(result[0].has_tag("keep_tag"));
    }

    #[tokio::test]
    async fn test_mutate_convert_float() {
        let settings = serde_json::json!({ "convert": { "lat": "float" } });
        let filter = MutateFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("lat", EventValue::String("35.68".into()));
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].get("lat"), Some(&EventValue::Float(35.68)));
    }

    #[tokio::test]
    async fn test_mutate_convert_string() {
        let settings = serde_json::json!({ "convert": { "code": "string" } });
        let filter = MutateFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("code", EventValue::Integer(200));
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("code"),
            Some(&EventValue::String("200".into()))
        );
    }

    #[tokio::test]
    async fn test_mutate_convert_boolean() {
        let settings = serde_json::json!({ "convert": { "flag": "boolean" } });
        let filter = MutateFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("flag", EventValue::String("true".into()));
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].get("flag"), Some(&EventValue::Boolean(true)));
    }

    #[tokio::test]
    async fn test_mutate_convert_boolean_false() {
        let settings = serde_json::json!({ "convert": { "flag": "boolean" } });
        let filter = MutateFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("flag", EventValue::String("no".into()));
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].get("flag"), Some(&EventValue::Boolean(false)));
    }

    #[tokio::test]
    async fn test_mutate_join() {
        let settings = serde_json::json!({ "join": { "parts": "," } });
        let filter = MutateFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set(
            "parts",
            EventValue::Array(vec![
                EventValue::String("a".into()),
                EventValue::String("b".into()),
                EventValue::String("c".into()),
            ]),
        );
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("parts"),
            Some(&EventValue::String("a,b,c".into()))
        );
    }

    #[tokio::test]
    async fn test_mutate_rename_nonexistent() {
        let settings = serde_json::json!({ "rename": { "nonexistent": "new" } });
        let filter = MutateFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        assert!(!result[0].has_field("new"));
    }

    #[tokio::test]
    async fn test_mutate_multiple_operations() {
        let settings = serde_json::json!({
            "uppercase": ["message"],
            "add_field": { "processed": "true" },
            "add_tag": ["done"]
        });
        let filter = MutateFilter::from_config(&settings, None).expect("config");
        let event = Event::new("hello");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].message(), Some("HELLO"));
        assert!(result[0].has_field("processed"));
        assert!(result[0].has_tag("done"));
    }

    #[tokio::test]
    async fn test_mutate_empty_config() {
        let settings = serde_json::json!({});
        let filter = MutateFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].message(), Some("test"));
    }

    #[test]
    fn test_mutate_name() {
        let settings = serde_json::json!({});
        let filter = MutateFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.name(), "mutate");
    }

    #[tokio::test]
    async fn test_mutate_gsub_no_match() {
        let settings = serde_json::json!({ "gsub": ["message", "XYZ", "replaced"] });
        let filter = MutateFilter::from_config(&settings, None).expect("config");
        let event = Event::new("hello world");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].message(), Some("hello world"));
    }

    #[tokio::test]
    async fn test_mutate_convert_integer_failure() {
        let settings = serde_json::json!({ "convert": { "val": "integer" } });
        let filter = MutateFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set("val", EventValue::String("not_a_number".into()));
        let result = filter.filter(event).await.expect("filter");
        // Should keep original value on parse failure
        assert_eq!(
            result[0].get("val"),
            Some(&EventValue::String("not_a_number".into()))
        );
    }
}
