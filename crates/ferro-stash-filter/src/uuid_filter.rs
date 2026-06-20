// SPDX-License-Identifier: Apache-2.0
//! UUID filter — sets a (v4) UUID into a target field.
//!
//! ```logstash
//! filter {
//!   uuid {
//!     target    => "uuid"
//!     overwrite => false
//!   }
//! }
//! ```
//!
//! By default an existing value in `target` is preserved (`overwrite => false`,
//! matching Logstash). Set `overwrite => true` to always mint a fresh UUID.

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::FilterPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;

#[derive(Debug)]
pub struct UuidFilter {
    target: String,
    overwrite: bool,
    condition: Option<Condition>,
}

impl UuidFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let target = settings
            .get_string("target")
            .ok_or_else(|| FerroStashError::Filter {
                plugin: "uuid".to_string(),
                message: "uuid filter requires `target`".to_string(),
            })?;
        let overwrite = settings.get_bool("overwrite").unwrap_or(false);
        Ok(Self {
            target,
            overwrite,
            condition,
        })
    }
}

#[async_trait]
impl FilterPlugin for UuidFilter {
    fn name(&self) -> &'static str {
        "uuid"
    }

    async fn filter(&self, mut event: Event) -> Result<Vec<Event>> {
        // Logstash semantics: only set the UUID when the field is absent unless
        // `overwrite` is requested.
        if self.overwrite || !event.has_field(&self.target) {
            let id = uuid::Uuid::new_v4().to_string();
            event.set(self.target.clone(), EventValue::String(id));
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

    #[test]
    fn test_uuid_name() {
        let f = UuidFilter::from_config(&serde_json::json!({ "target": "uuid" }), None)
            .expect("config");
        assert_eq!(f.name(), "uuid");
    }

    #[test]
    fn test_uuid_requires_target() {
        assert!(UuidFilter::from_config(&serde_json::json!({}), None).is_err());
    }

    #[tokio::test]
    async fn test_uuid_sets_field() {
        let f = UuidFilter::from_config(&serde_json::json!({ "target": "uuid" }), None)
            .expect("config");
        let out = f.filter(Event::new("x")).await.expect("filter");
        let v = out[0].get("uuid").expect("uuid field");
        let s = v.as_str().expect("string");
        // Canonical UUID v4 string is 36 chars with 4 hyphens.
        assert_eq!(s.len(), 36);
        assert_eq!(s.matches('-').count(), 4);
        // Parses as a real UUID.
        assert!(uuid::Uuid::parse_str(s).is_ok());
    }

    #[tokio::test]
    async fn test_uuid_unique_per_event() {
        let f = UuidFilter::from_config(&serde_json::json!({ "target": "uuid" }), None)
            .expect("config");
        let a = f.filter(Event::new("a")).await.expect("filter");
        let b = f.filter(Event::new("b")).await.expect("filter");
        assert_ne!(a[0].get("uuid"), b[0].get("uuid"));
    }

    #[tokio::test]
    async fn test_uuid_no_overwrite_preserves_existing() {
        let f = UuidFilter::from_config(
            &serde_json::json!({ "target": "uuid", "overwrite": false }),
            None,
        )
        .expect("config");
        let mut event = Event::new("x");
        event.set("uuid", EventValue::String("keep-me".into()));
        let out = f.filter(event).await.expect("filter");
        assert_eq!(
            out[0].get("uuid"),
            Some(&EventValue::String("keep-me".into()))
        );
    }

    #[tokio::test]
    async fn test_uuid_overwrite_replaces_existing() {
        let f = UuidFilter::from_config(
            &serde_json::json!({ "target": "uuid", "overwrite": true }),
            None,
        )
        .expect("config");
        let mut event = Event::new("x");
        event.set("uuid", EventValue::String("replace-me".into()));
        let out = f.filter(event).await.expect("filter");
        let s = out[0].get("uuid").expect("uuid").as_str().expect("str");
        assert_ne!(s, "replace-me");
        assert!(uuid::Uuid::parse_str(s).is_ok());
    }
}
