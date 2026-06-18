// SPDX-License-Identifier: Apache-2.0
//! Shared helpers for resolving a plugin's `codec` setting into a codec name
//! plus its sub-settings object.
//!
//! In the Logstash DSL a codec may be declared in two forms:
//!
//! ```text
//! codec => json                      # name only
//! codec => json { target => "data" } # name + sub-settings
//! ```
//!
//! The DSL parser represents the second form as an object that carries a
//! `_plugin` discriminator alongside the codec's own settings, e.g.
//! `{ "_plugin": "json", "target": "data" }`. The first form is just the
//! string `"json"`.
//!
//! These helpers normalize both forms so input plugins can thread the real
//! codec sub-settings through to [`ferro_stash_codec::create_codec`] instead of
//! discarding them.

/// Resolves a plugin's `codec` setting into `(name, settings)`.
///
/// * String form (`codec => json`) → `("json", {})`.
/// * Descriptor form (`codec => json { target => "data" }`) →
///   `("json", { "target": "data" })` (the `_plugin` discriminator is stripped).
/// * Missing / unrecognized → `(default_name, {})`.
///
/// The returned settings object is always a JSON object value, suitable to pass
/// straight to `create_codec`.
pub fn resolve_codec(
    settings: &serde_json::Value,
    default_name: &str,
) -> (String, serde_json::Value) {
    let empty = || serde_json::Value::Object(serde_json::Map::new());

    match settings.get("codec") {
        // `codec => json`
        Some(serde_json::Value::String(name)) => (name.clone(), empty()),
        // `codec => json { target => "data" }`
        Some(serde_json::Value::Object(map)) => {
            let name = map
                .get("_plugin")
                .and_then(|v| v.as_str())
                .map_or_else(|| default_name.to_string(), String::from);
            let mut sub = map.clone();
            // The discriminator is an internal marker, not a codec setting.
            sub.remove("_plugin");
            (name, serde_json::Value::Object(sub))
        }
        _ => (default_name.to_string(), empty()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_string_form() {
        let s = serde_json::json!({ "codec": "json" });
        let (name, sub) = resolve_codec(&s, "plain");
        assert_eq!(name, "json");
        assert_eq!(sub, serde_json::json!({}));
    }

    #[test]
    fn resolve_descriptor_form_keeps_sub_settings() {
        let s = serde_json::json!({
            "codec": { "_plugin": "json", "target": "data", "pretty": true }
        });
        let (name, sub) = resolve_codec(&s, "plain");
        assert_eq!(name, "json");
        assert_eq!(sub, serde_json::json!({ "target": "data", "pretty": true }));
        // The internal discriminator must not leak into codec settings.
        assert!(sub.get("_plugin").is_none());
    }

    #[test]
    fn resolve_missing_uses_default() {
        let s = serde_json::json!({});
        let (name, sub) = resolve_codec(&s, "plain");
        assert_eq!(name, "plain");
        assert_eq!(sub, serde_json::json!({}));
    }

    #[test]
    fn resolve_descriptor_without_plugin_uses_default_name() {
        let s = serde_json::json!({ "codec": { "target": "data" } });
        let (name, sub) = resolve_codec(&s, "plain");
        assert_eq!(name, "plain");
        assert_eq!(sub, serde_json::json!({ "target": "data" }));
    }
}
