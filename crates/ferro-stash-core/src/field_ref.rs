// SPDX-License-Identifier: Apache-2.0
//! Field reference parsing — supports dotted paths and bracket notation.
//!
//! Examples:
//! - `message` → top-level field
//! - `host.name` → nested field
//! - `[host][name]` → bracket notation (Logstash style)

use indexmap::IndexMap;

use crate::event::EventValue;

/// A parsed field reference with path segments.
#[derive(Debug, Clone)]
pub struct FieldRef {
    segments: Vec<String>,
}

impl FieldRef {
    /// Parses a field reference string.
    ///
    /// Supports:
    /// - `field` — single field
    /// - `field.subfield` — dotted notation
    /// - `[field][subfield]` — bracket notation
    pub fn parse(input: &str) -> Self {
        let segments = if input.starts_with('[') {
            // Bracket notation: [field][subfield]
            input
                .split(']')
                .filter_map(|s| {
                    let trimmed = s.trim_start_matches('[');
                    if trimmed.is_empty() {
                        None
                    } else {
                        Some(trimmed.to_string())
                    }
                })
                .collect()
        } else {
            // Dotted notation: field.subfield
            input.split('.').map(String::from).collect()
        };
        Self { segments }
    }

    /// Gets a value from an `IndexMap` following the path.
    pub fn get_from_fields<'a>(
        &self,
        fields: &'a IndexMap<String, EventValue>,
    ) -> Option<&'a EventValue> {
        if self.segments.is_empty() {
            return None;
        }
        let mut current = fields.get(&self.segments[0])?;
        for segment in &self.segments[1..] {
            match current {
                EventValue::Object(map) => {
                    current = map.get(segment)?;
                }
                _ => return None,
            }
        }
        Some(current)
    }

    /// Gets a mutable value from an `IndexMap` following the path.
    pub fn get_mut_from_fields<'a>(
        &self,
        fields: &'a mut IndexMap<String, EventValue>,
    ) -> Option<&'a mut EventValue> {
        if self.segments.is_empty() {
            return None;
        }
        if self.segments.len() == 1 {
            return fields.get_mut(&self.segments[0]);
        }
        let mut current = fields.get_mut(&self.segments[0])?;
        for segment in &self.segments[1..] {
            match current {
                EventValue::Object(map) => {
                    current = map.get_mut(segment)?;
                }
                _ => return None,
            }
        }
        Some(current)
    }

    /// Sets a value in an `IndexMap`, creating intermediate objects as needed.
    pub fn set_in_fields(&self, fields: &mut IndexMap<String, EventValue>, value: EventValue) {
        if self.segments.is_empty() {
            return;
        }
        if self.segments.len() == 1 {
            fields.insert(self.segments[0].clone(), value);
            return;
        }

        // Ensure intermediate objects exist
        let first = &self.segments[0];
        if !fields.contains_key(first) || !matches!(fields.get(first), Some(EventValue::Object(_)))
        {
            fields.insert(first.clone(), EventValue::Object(IndexMap::new()));
        }

        let mut current = fields.get_mut(first);
        for segment in &self.segments[1..self.segments.len() - 1] {
            match current {
                Some(EventValue::Object(map)) => {
                    if !map.contains_key(segment)
                        || !matches!(map.get(segment), Some(EventValue::Object(_)))
                    {
                        map.insert(segment.clone(), EventValue::Object(IndexMap::new()));
                    }
                    current = map.get_mut(segment);
                }
                _ => return,
            }
        }

        if let Some(EventValue::Object(map)) = current {
            let last = &self.segments[self.segments.len() - 1];
            map.insert(last.clone(), value);
        }
    }

    /// Removes a value from an `IndexMap`.
    pub fn remove_from_fields(
        &self,
        fields: &mut IndexMap<String, EventValue>,
    ) -> Option<EventValue> {
        if self.segments.is_empty() {
            return None;
        }
        if self.segments.len() == 1 {
            return fields.swap_remove(&self.segments[0]);
        }

        // Navigate to parent
        let mut current: Option<&mut EventValue> = fields.get_mut(&self.segments[0]);
        for segment in &self.segments[1..self.segments.len() - 1] {
            match current {
                Some(EventValue::Object(map)) => {
                    current = map.get_mut(segment);
                }
                _ => return None,
            }
        }

        if let Some(EventValue::Object(map)) = current {
            let last = &self.segments[self.segments.len() - 1];
            map.swap_remove(last)
        } else {
            None
        }
    }

    /// Returns the segments of this field reference.
    pub fn segments(&self) -> &[String] {
        &self.segments
    }

    /// Returns the top-level field name.
    pub fn root(&self) -> Option<&str> {
        self.segments.first().map(String::as_str)
    }

    /// Returns the field as a dotted path string.
    pub fn to_dotted(&self) -> String {
        self.segments.join(".")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple() {
        let fr = FieldRef::parse("message");
        assert_eq!(fr.segments(), &["message"]);
    }

    #[test]
    fn test_parse_dotted() {
        let fr = FieldRef::parse("host.name");
        assert_eq!(fr.segments(), &["host", "name"]);
    }

    #[test]
    fn test_parse_bracket() {
        let fr = FieldRef::parse("[host][name]");
        assert_eq!(fr.segments(), &["host", "name"]);
    }

    #[test]
    fn test_get_nested() {
        let mut fields = IndexMap::new();
        let mut inner = IndexMap::new();
        inner.insert("name".to_string(), EventValue::String("server01".into()));
        fields.insert("host".to_string(), EventValue::Object(inner));

        let fr = FieldRef::parse("host.name");
        assert_eq!(
            fr.get_from_fields(&fields),
            Some(&EventValue::String("server01".into()))
        );
    }

    #[test]
    fn test_set_nested_creates_intermediate() {
        let mut fields = IndexMap::new();
        let fr = FieldRef::parse("a.b.c");
        fr.set_in_fields(&mut fields, EventValue::Integer(42));

        let result = FieldRef::parse("a.b.c").get_from_fields(&fields);
        assert_eq!(result, Some(&EventValue::Integer(42)));
    }

    #[test]
    fn test_remove_nested() {
        let mut fields = IndexMap::new();
        let fr = FieldRef::parse("a.b");
        fr.set_in_fields(&mut fields, EventValue::Integer(1));

        let removed = FieldRef::parse("a.b").remove_from_fields(&mut fields);
        assert_eq!(removed, Some(EventValue::Integer(1)));
    }

    #[test]
    fn test_root() {
        let fr = FieldRef::parse("host.name");
        assert_eq!(fr.root(), Some("host"));
    }

    #[test]
    fn test_to_dotted() {
        let fr = FieldRef::parse("[host][name]");
        assert_eq!(fr.to_dotted(), "host.name");
    }

    #[test]
    fn test_get_from_fields_empty_segments() {
        let fr = FieldRef { segments: vec![] };
        let fields = IndexMap::new();
        assert!(fr.get_from_fields(&fields).is_none());
    }

    #[test]
    fn test_get_mut_from_fields_single() {
        let mut fields = IndexMap::new();
        fields.insert("key".to_string(), EventValue::Integer(1));
        let fr = FieldRef::parse("key");
        let val = fr.get_mut_from_fields(&mut fields);
        assert!(val.is_some());
    }

    #[test]
    fn test_get_from_non_object_path() {
        let mut fields = IndexMap::new();
        fields.insert("host".to_string(), EventValue::String("s".into()));
        let fr = FieldRef::parse("host.name");
        // host is a string, not an object, so nested access fails
        assert!(fr.get_from_fields(&fields).is_none());
    }

    #[test]
    fn test_remove_simple() {
        let mut fields = IndexMap::new();
        fields.insert("key".to_string(), EventValue::Integer(42));
        let fr = FieldRef::parse("key");
        let removed = fr.remove_from_fields(&mut fields);
        assert_eq!(removed, Some(EventValue::Integer(42)));
        assert!(!fields.contains_key("key"));
    }

    #[test]
    fn test_remove_missing() {
        let mut fields = IndexMap::new();
        let fr = FieldRef::parse("nonexistent");
        let removed = fr.remove_from_fields(&mut fields);
        assert!(removed.is_none());
    }

    #[test]
    fn test_set_overwrites_non_object() {
        let mut fields = IndexMap::new();
        fields.insert("host".to_string(), EventValue::String("old".into()));
        let fr = FieldRef::parse("host.name");
        fr.set_in_fields(&mut fields, EventValue::String("server01".into()));
        // Should create intermediate object
        let result = FieldRef::parse("host.name").get_from_fields(&fields);
        assert_eq!(result, Some(&EventValue::String("server01".into())));
    }

    #[test]
    fn test_deeply_nested() {
        let mut fields = IndexMap::new();
        let fr = FieldRef::parse("a.b.c.d.e");
        fr.set_in_fields(&mut fields, EventValue::String("deep".into()));
        let result = FieldRef::parse("a.b.c.d.e").get_from_fields(&fields);
        assert_eq!(result, Some(&EventValue::String("deep".into())));
    }

    #[test]
    fn test_bracket_single() {
        let fr = FieldRef::parse("[message]");
        assert_eq!(fr.segments(), &["message"]);
    }

    #[test]
    fn test_segments() {
        let fr = FieldRef::parse("a.b.c");
        assert_eq!(fr.segments().len(), 3);
    }
}
