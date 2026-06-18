// SPDX-License-Identifier: Apache-2.0
//! Logstash-compatible settings value helpers.
//!
//! Logstash DSL allows values to be specified as strings or primitives interchangeably:
//!   port => 9999        # number
//!   port => "9999"      # double-quoted string
//!   port => '9999'      # single-quoted string
//!   flush_interval => 0 # number
//!   flush_interval => "0"
//!
//! These helpers accept both forms.

use serde_json::Value;

/// Extract a u64 from a JSON value, accepting number or string-encoded number.
pub fn as_u64_flexible(v: &Value) -> Option<u64> {
    v.as_u64()
        .or_else(|| v.as_i64().and_then(|n| u64::try_from(n).ok()))
        .or_else(|| v.as_str().and_then(|s| s.parse::<u64>().ok()))
}

/// Extract an i64 from a JSON value, accepting number or string-encoded number.
pub fn as_i64_flexible(v: &Value) -> Option<i64> {
    v.as_i64()
        .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
}

/// Extract an f64 from a JSON value, accepting number or string-encoded number.
pub fn as_f64_flexible(v: &Value) -> Option<f64> {
    v.as_f64()
        .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
}

/// Extract a bool from a JSON value, accepting bool or "true"/"false" string.
pub fn as_bool_flexible(v: &Value) -> Option<bool> {
    v.as_bool().or_else(|| {
        v.as_str()
            .and_then(|s| match s.to_ascii_lowercase().as_str() {
                "true" | "1" => Some(true),
                "false" | "0" => Some(false),
                _ => None,
            })
    })
}

/// Helper trait for getting settings values by field name with flexible type coercion.
pub trait SettingsExt {
    fn get_u64(&self, key: &str) -> Option<u64>;
    fn get_i64(&self, key: &str) -> Option<i64>;
    fn get_f64(&self, key: &str) -> Option<f64>;
    fn get_bool(&self, key: &str) -> Option<bool>;
    fn get_string(&self, key: &str) -> Option<String>;
    /// Read a TCP/UDP port, validating it is in `1..=65535`.
    ///
    /// Returns `Ok(default)` when the key is absent/null, and `Err(message)`
    /// when a value is PRESENT but invalid (non-integer, negative, or out of
    /// `1..=65535`) — so `port => 70000` / `port => "638O"` fail loudly at
    /// config time instead of silently truncating or falling back to default.
    fn get_port(&self, key: &str, default: u16) -> Result<u16, String>;
    /// Read a `u32` config value (e.g. a Redis DB index), validating range.
    /// `Ok(default)` when absent/null; `Err(message)` when present but invalid
    /// or `> u32::MAX` — so a value like `db => 4294967296` fails loudly
    /// instead of truncating (`as u32`) to a wrong/0 value.
    fn get_u32(&self, key: &str, default: u32) -> Result<u32, String>;
}

impl SettingsExt for Value {
    fn get_u64(&self, key: &str) -> Option<u64> {
        self.get(key).and_then(as_u64_flexible)
    }
    fn get_port(&self, key: &str, default: u16) -> Result<u16, String> {
        match self.get(key) {
            None | Some(Value::Null) => Ok(default),
            Some(v) => match as_u64_flexible(v) {
                Some(n) if (1..=65535).contains(&n) => Ok(n as u16),
                _ => Err(format!("{key} must be a port in 1..=65535, got {v}")),
            },
        }
    }
    fn get_u32(&self, key: &str, default: u32) -> Result<u32, String> {
        match self.get(key) {
            None | Some(Value::Null) => Ok(default),
            Some(v) => match as_u64_flexible(v) {
                Some(n) if n <= u64::from(u32::MAX) => Ok(n as u32),
                _ => Err(format!(
                    "{key} must be an integer in 0..=4294967295, got {v}"
                )),
            },
        }
    }
    fn get_i64(&self, key: &str) -> Option<i64> {
        self.get(key).and_then(as_i64_flexible)
    }
    fn get_f64(&self, key: &str) -> Option<f64> {
        self.get(key).and_then(as_f64_flexible)
    }
    fn get_bool(&self, key: &str) -> Option<bool> {
        self.get(key).and_then(as_bool_flexible)
    }
    fn get_string(&self, key: &str) -> Option<String> {
        self.get(key).and_then(|v| v.as_str().map(String::from))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_port_absent_uses_default() {
        assert_eq!(serde_json::json!({}).get_port("port", 6379), Ok(6379));
        assert_eq!(serde_json::json!({"port": null}).get_port("port", 6379), Ok(6379));
    }

    #[test]
    fn get_port_valid_in_range() {
        assert_eq!(serde_json::json!({"port": 9200}).get_port("port", 1), Ok(9200));
        // Logstash DSL string form.
        assert_eq!(serde_json::json!({"port": "6390"}).get_port("port", 1), Ok(6390));
    }

    #[test]
    fn get_port_present_but_invalid_errors() {
        // Out of range, non-numeric string, negative, zero, and wrong type all error
        // (present-but-invalid must NOT silently fall back to the default).
        for bad in [
            serde_json::json!({"port": 70000}),
            serde_json::json!({"port": "638O"}),
            serde_json::json!({"port": -1}),
            serde_json::json!({"port": 0}),
            serde_json::json!({"port": true}),
        ] {
            assert!(bad.get_port("port", 6379).is_err(), "should reject {bad}");
        }
    }

    #[test]
    fn get_u32_absent_default_valid_and_overflow() {
        assert_eq!(serde_json::json!({}).get_u32("db", 0), Ok(0));
        assert_eq!(serde_json::json!({"db": 5}).get_u32("db", 0), Ok(5));
        assert!(serde_json::json!({"db": 4_294_967_296u64}).get_u32("db", 0).is_err());
        assert!(serde_json::json!({"db": "x"}).get_u32("db", 0).is_err());
    }

    #[test]
    fn test_u64_from_number() {
        assert_eq!(as_u64_flexible(&serde_json::json!(9999)), Some(9999));
    }

    #[test]
    fn test_u64_from_string() {
        assert_eq!(as_u64_flexible(&serde_json::json!("9999")), Some(9999));
    }

    #[test]
    fn test_u64_from_quoted_string() {
        assert_eq!(as_u64_flexible(&serde_json::json!("12345")), Some(12345));
    }

    #[test]
    fn test_u64_invalid() {
        assert_eq!(as_u64_flexible(&serde_json::json!("abc")), None);
        assert_eq!(as_u64_flexible(&serde_json::json!(null)), None);
        assert_eq!(as_u64_flexible(&serde_json::json!(-1)), None);
    }

    #[test]
    fn test_bool_variants() {
        assert_eq!(as_bool_flexible(&serde_json::json!(true)), Some(true));
        assert_eq!(as_bool_flexible(&serde_json::json!("true")), Some(true));
        assert_eq!(as_bool_flexible(&serde_json::json!("TRUE")), Some(true));
        assert_eq!(as_bool_flexible(&serde_json::json!("1")), Some(true));
        assert_eq!(as_bool_flexible(&serde_json::json!(false)), Some(false));
        assert_eq!(as_bool_flexible(&serde_json::json!("false")), Some(false));
        assert_eq!(as_bool_flexible(&serde_json::json!("0")), Some(false));
        assert_eq!(as_bool_flexible(&serde_json::json!("maybe")), None);
    }

    #[test]
    fn test_f64_variants() {
        assert_eq!(as_f64_flexible(&serde_json::json!(2.5)), Some(2.5));
        assert_eq!(as_f64_flexible(&serde_json::json!("2.5")), Some(2.5));
        assert_eq!(as_f64_flexible(&serde_json::json!(42)), Some(42.0));
    }

    #[test]
    fn test_settings_ext() {
        let s = serde_json::json!({
            "port": "9999",
            "flush_interval": 0,
            "enabled": "true",
            "host": "localhost"
        });
        assert_eq!(s.get_u64("port"), Some(9999));
        assert_eq!(s.get_u64("flush_interval"), Some(0));
        assert_eq!(s.get_bool("enabled"), Some(true));
        assert_eq!(s.get_string("host"), Some("localhost".to_string()));
    }
}
