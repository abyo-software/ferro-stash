// SPDX-License-Identifier: Apache-2.0
//! XML filter — parse an XML string field into a structured Hash.

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::FilterPlugin;
use indexmap::IndexMap;

#[derive(Debug)]
pub struct XmlFilter {
    source: String,
    target: String,
    #[allow(dead_code)]
    force_array: bool,
    store_xml: bool,
    condition: Option<Condition>,
}

impl XmlFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let source = settings
            .get("source")
            .and_then(|v| v.as_str())
            .unwrap_or("message")
            .to_string();
        let target = settings
            .get("target")
            .and_then(|v| v.as_str())
            .unwrap_or("parsed_xml")
            .to_string();
        let force_array = settings
            .get("force_array")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(true);
        let store_xml = settings
            .get("store_xml")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(true);

        Ok(Self {
            source,
            target,
            force_array,
            store_xml,
            condition,
        })
    }

    /// Simple recursive XML parser using string operations.
    /// Handles basic XML: `<tag attr="val">content</tag>`, self-closing `<tag/>`,
    /// nested elements, and text content.
    fn parse_xml(input: &str) -> Option<EventValue> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return None;
        }
        // Try to parse as an element
        if trimmed.starts_with('<') {
            Self::parse_elements(trimmed)
        } else {
            Some(EventValue::String(trimmed.to_string()))
        }
    }

    fn parse_elements(input: &str) -> Option<EventValue> {
        let mut result: IndexMap<String, EventValue> = IndexMap::new();
        let mut pos = 0;
        let bytes = input.as_bytes();

        while pos < bytes.len() {
            // Skip whitespace
            while pos < bytes.len() && (bytes[pos] as char).is_whitespace() {
                pos += 1;
            }
            if pos >= bytes.len() {
                break;
            }

            // Expect '<'
            if bytes[pos] != b'<' {
                // Text content outside tags
                let end = input[pos..].find('<').map_or(bytes.len(), |i| pos + i);
                let text = input[pos..end].trim();
                if !text.is_empty() {
                    // Return text content directly
                    return Some(EventValue::String(text.to_string()));
                }
                pos = end;
                continue;
            }

            // Skip XML declaration / processing instructions / comments
            if input[pos..].starts_with("<?") {
                if let Some(end) = input[pos..].find("?>") {
                    pos += end + 2;
                    continue;
                }
                break;
            }
            if input[pos..].starts_with("<!--") {
                if let Some(end) = input[pos..].find("-->") {
                    pos += end + 3;
                    continue;
                }
                break;
            }

            // Parse opening tag
            let tag_end = input[pos..].find('>')?;
            let tag_content = &input[pos + 1..pos + tag_end];

            // Self-closing tag
            let self_closing = tag_content.ends_with('/');
            let tag_inner = if self_closing {
                &tag_content[..tag_content.len() - 1]
            } else {
                tag_content
            };

            // Extract tag name (first word)
            let tag_name = tag_inner.split_whitespace().next()?.to_string();

            // Extract attributes
            let mut attrs: IndexMap<String, EventValue> = IndexMap::new();
            let attr_str = &tag_inner[tag_name.len()..];
            Self::parse_attributes(attr_str, &mut attrs);

            pos += tag_end + 1;

            if self_closing {
                let value = if attrs.is_empty() {
                    EventValue::Null
                } else {
                    EventValue::Object(attrs)
                };
                Self::insert_into_map(&mut result, &tag_name, value);
                continue;
            }

            // Find closing tag
            let closing = format!("</{tag_name}>");
            if let Some(close_pos) = Self::find_closing_tag(&input[pos..], &tag_name) {
                let inner_content = &input[pos..pos + close_pos];
                let inner_trimmed = inner_content.trim();

                let child_value = if inner_trimmed.is_empty() {
                    if attrs.is_empty() {
                        EventValue::Null
                    } else {
                        EventValue::Object(attrs)
                    }
                } else if inner_trimmed.contains('<') {
                    // Nested elements
                    let mut child_map = attrs;
                    if let Some(EventValue::Object(children)) = Self::parse_elements(inner_trimmed)
                    {
                        for (k, v) in children {
                            child_map.insert(k, v);
                        }
                    }
                    if child_map.is_empty() {
                        EventValue::String(inner_trimmed.to_string())
                    } else {
                        EventValue::Object(child_map)
                    }
                } else {
                    // Text content
                    if attrs.is_empty() {
                        EventValue::String(inner_trimmed.to_string())
                    } else {
                        attrs.insert(
                            "_text".to_string(),
                            EventValue::String(inner_trimmed.to_string()),
                        );
                        EventValue::Object(attrs)
                    }
                };

                Self::insert_into_map(&mut result, &tag_name, child_value);
                pos += close_pos + closing.len();
            } else {
                // No closing tag found — treat as self-closing
                let value = if attrs.is_empty() {
                    EventValue::Null
                } else {
                    EventValue::Object(attrs)
                };
                Self::insert_into_map(&mut result, &tag_name, value);
            }
        }

        if result.is_empty() {
            None
        } else {
            Some(EventValue::Object(result))
        }
    }

    fn parse_attributes(attr_str: &str, attrs: &mut IndexMap<String, EventValue>) {
        let mut s = attr_str.trim();
        while !s.is_empty() {
            // Find '='
            let eq_pos = match s.find('=') {
                Some(p) => p,
                None => break,
            };
            let key = s[..eq_pos].trim();
            s = s[eq_pos + 1..].trim();

            // Parse quoted value
            if s.starts_with('"') {
                if let Some(end) = s[1..].find('"') {
                    let val = &s[1..=end];
                    attrs.insert(key.to_string(), EventValue::String(val.to_string()));
                    s = s[2 + end..].trim();
                } else {
                    break;
                }
            } else if s.starts_with('\'') {
                if let Some(end) = s[1..].find('\'') {
                    let val = &s[1..=end];
                    attrs.insert(key.to_string(), EventValue::String(val.to_string()));
                    s = s[2 + end..].trim();
                } else {
                    break;
                }
            } else {
                let end = s.find(char::is_whitespace).unwrap_or(s.len());
                let val = &s[..end];
                attrs.insert(key.to_string(), EventValue::String(val.to_string()));
                s = s[end..].trim();
            }
        }
    }

    /// Find the position of the matching closing tag, handling nesting.
    fn find_closing_tag(input: &str, tag_name: &str) -> Option<usize> {
        let open_tag = format!("<{tag_name}");
        let close_tag = format!("</{tag_name}>");
        let mut depth = 1;
        let mut pos = 0;

        while pos < input.len() {
            if input[pos..].starts_with(&close_tag) {
                depth -= 1;
                if depth == 0 {
                    return Some(pos);
                }
                pos += close_tag.len();
            } else if input[pos..].starts_with(&open_tag) {
                // Check it's not a different tag starting with same prefix
                let after = pos + open_tag.len();
                if after < input.len() {
                    let ch = input.as_bytes()[after];
                    if ch == b'>' || ch == b' ' || ch == b'/' {
                        depth += 1;
                    }
                }
                pos += 1;
            } else {
                pos += 1;
            }
        }
        None
    }

    fn insert_into_map(map: &mut IndexMap<String, EventValue>, key: &str, value: EventValue) {
        if let Some(existing) = map.get_mut(key) {
            // Convert to array if not already
            match existing {
                EventValue::Array(arr) => {
                    arr.push(value);
                }
                _ => {
                    let old = std::mem::replace(existing, EventValue::Null);
                    *existing = EventValue::Array(vec![old, value]);
                }
            }
        } else {
            map.insert(key.to_string(), value);
        }
    }
}

#[async_trait]
impl FilterPlugin for XmlFilter {
    fn name(&self) -> &'static str {
        "xml"
    }

    async fn filter(&self, mut event: Event) -> Result<Vec<Event>> {
        let xml_str = match event.get(&self.source) {
            Some(val) => val.to_string_lossy(),
            None => return Ok(vec![event]),
        };

        if let Some(parsed) = Self::parse_xml(&xml_str) {
            event.set(self.target.clone(), parsed);
            if !self.store_xml && self.source != self.target {
                event.remove(&self.source);
            }
        } else {
            event.add_tag("_xmlparsefailure");
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
    async fn test_xml_simple() {
        let settings = serde_json::json!({
            "source": "message",
            "target": "doc"
        });
        let filter = XmlFilter::from_config(&settings, None).expect("config");
        let event = Event::new("<root><name>Alice</name><age>30</age></root>");
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_field("doc"));
        let doc = result[0].get("doc").expect("doc field");
        // Should be an Object containing "root"
        assert!(doc.as_object().is_some());
    }

    #[tokio::test]
    async fn test_xml_attributes() {
        let settings = serde_json::json!({
            "source": "message",
            "target": "doc"
        });
        let filter = XmlFilter::from_config(&settings, None).expect("config");
        let event = Event::new(r#"<item id="123" type="book">Test</item>"#);
        let result = filter.filter(event).await.expect("filter");
        let doc = result[0].get("doc").expect("doc field");
        let obj = doc.as_object().expect("should be object");
        let item = obj.get("item").expect("item field");
        let item_obj = item.as_object().expect("item should be object");
        assert_eq!(item_obj.get("id"), Some(&EventValue::String("123".into())));
        assert_eq!(
            item_obj.get("_text"),
            Some(&EventValue::String("Test".into()))
        );
    }

    #[tokio::test]
    async fn test_xml_self_closing() {
        let settings = serde_json::json!({
            "source": "message",
            "target": "doc"
        });
        let filter = XmlFilter::from_config(&settings, None).expect("config");
        let event = Event::new("<root><empty/><data>hello</data></root>");
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_field("doc"));
    }

    #[tokio::test]
    async fn test_xml_nested() {
        let settings = serde_json::json!({
            "source": "message",
            "target": "doc"
        });
        let filter = XmlFilter::from_config(&settings, None).expect("config");
        let event = Event::new("<root><parent><child>value</child></parent></root>");
        let result = filter.filter(event).await.expect("filter");
        let doc = result[0].get("doc").expect("doc");
        let root = doc.as_object().expect("object").get("root").expect("root");
        let parent = root
            .as_object()
            .expect("obj")
            .get("parent")
            .expect("parent");
        let child = parent
            .as_object()
            .expect("obj")
            .get("child")
            .expect("child");
        assert_eq!(child, &EventValue::String("value".into()));
    }

    #[tokio::test]
    async fn test_xml_missing_source() {
        let settings = serde_json::json!({
            "source": "nonexistent",
            "target": "doc"
        });
        let filter = XmlFilter::from_config(&settings, None).expect("config");
        let event = Event::new("hello");
        let result = filter.filter(event).await.expect("filter");
        assert!(!result[0].has_field("doc"));
    }

    #[tokio::test]
    async fn test_xml_store_xml_false() {
        let settings = serde_json::json!({
            "source": "raw_xml",
            "target": "doc",
            "store_xml": false
        });
        let filter = XmlFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set(
            "raw_xml",
            EventValue::String("<root><a>1</a></root>".into()),
        );
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_field("doc"));
        assert!(!result[0].has_field("raw_xml"));
    }

    #[test]
    fn test_xml_name() {
        let settings = serde_json::json!({});
        let filter = XmlFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.name(), "xml");
    }
}
