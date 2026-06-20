// SPDX-License-Identifier: Apache-2.0
//! `syslog_pri` filter — decodes a numeric syslog PRI value into facility /
//! severity code (and, optionally, label) fields.
//!
//! ```logstash
//! filter {
//!   syslog_pri {
//!     syslog_pri_field_name => "syslog_pri"
//!     use_labels            => true
//!   }
//! }
//! ```
//!
//! Per RFC 3164, `PRI = facility * 8 + severity`. This filter sets
//! `syslog_facility_code` and `syslog_severity_code`, and (when `use_labels`)
//! `syslog_facility` and `syslog_severity` from the standard label tables.
//! When the PRI field is absent (or unparseable) the default PRI is `13`
//! (facility `user-level`, severity `notice`), matching Logstash.

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::FilterPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;

/// Default severity labels (index = severity code 0..=7), per Logstash.
const DEFAULT_SEVERITY_LABELS: [&str; 8] = [
    "emergency",
    "alert",
    "critical",
    "error",
    "warning",
    "notice",
    "informational",
    "debug",
];

/// Default facility labels (index = facility code 0..=23), per Logstash.
const DEFAULT_FACILITY_LABELS: [&str; 24] = [
    "kernel",
    "user-level",
    "mail",
    "daemon",
    "security/authorization",
    "syslogd",
    "line printer",
    "network news",
    "uucp",
    "clock",
    "security/authorization",
    "ftp",
    "ntp",
    "log audit",
    "log alert",
    "clock",
    "local0",
    "local1",
    "local2",
    "local3",
    "local4",
    "local5",
    "local6",
    "local7",
];

/// Default PRI used when the field is missing or unparseable (Logstash default).
const DEFAULT_PRI: i64 = 13;

#[derive(Debug)]
pub struct SyslogPriFilter {
    field_name: String,
    use_labels: bool,
    severity_labels: Vec<String>,
    facility_labels: Vec<String>,
    condition: Option<Condition>,
}

impl SyslogPriFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let field_name = settings
            .get_string("syslog_pri_field_name")
            .unwrap_or_else(|| "syslog_pri".to_string());
        let use_labels = settings.get_bool("use_labels").unwrap_or(true);

        let severity_labels = override_labels(settings, "severity_labels").unwrap_or_else(|| {
            DEFAULT_SEVERITY_LABELS
                .iter()
                .map(|s| (*s).to_string())
                .collect()
        });
        let facility_labels = override_labels(settings, "facility_labels").unwrap_or_else(|| {
            DEFAULT_FACILITY_LABELS
                .iter()
                .map(|s| (*s).to_string())
                .collect()
        });

        Ok(Self {
            field_name,
            use_labels,
            severity_labels,
            facility_labels,
            condition,
        })
    }
}

/// Reads a label-override array, returning `None` when the key is absent so the
/// caller falls back to the standard table.
fn override_labels(settings: &serde_json::Value, key: &str) -> Option<Vec<String>> {
    settings.get(key).and_then(|v| v.as_array()).map(|arr| {
        arr.iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect()
    })
}

#[async_trait]
impl FilterPlugin for SyslogPriFilter {
    fn name(&self) -> &'static str {
        "syslog_pri"
    }

    async fn filter(&self, mut event: Event) -> Result<Vec<Event>> {
        // Resolve the PRI: present & parseable → that value; otherwise 13.
        let priority = event
            .get(&self.field_name)
            .map_or(DEFAULT_PRI, |v| match v {
                EventValue::Integer(n) => *n,
                other => other
                    .to_string_lossy()
                    .trim()
                    .parse::<i64>()
                    .unwrap_or(DEFAULT_PRI),
            });

        // Negative PRIs are nonsensical; clamp to the default to avoid negative
        // modulo / division surprises.
        let priority = if priority < 0 { DEFAULT_PRI } else { priority };
        let severity = priority % 8;
        let facility = priority / 8;

        event.set("syslog_severity_code", EventValue::Integer(severity));
        event.set("syslog_facility_code", EventValue::Integer(facility));

        if self.use_labels {
            if let Ok(idx) = usize::try_from(severity) {
                if let Some(label) = self.severity_labels.get(idx) {
                    event.set("syslog_severity", EventValue::String(label.clone()));
                }
            }
            if let Ok(idx) = usize::try_from(facility) {
                if let Some(label) = self.facility_labels.get(idx) {
                    event.set("syslog_facility", EventValue::String(label.clone()));
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

    #[test]
    fn test_syslog_pri_name() {
        let f = SyslogPriFilter::from_config(&serde_json::json!({}), None).expect("config");
        assert_eq!(f.name(), "syslog_pri");
    }

    #[tokio::test]
    async fn test_decode_pri_34() {
        // PRI 34 → facility 4 (security/authorization), severity 2 (critical).
        let f = SyslogPriFilter::from_config(&serde_json::json!({}), None).expect("config");
        let mut event = Event::new("x");
        event.set("syslog_pri", EventValue::Integer(34));
        let out = f.filter(event).await.expect("filter");
        assert_eq!(
            out[0].get("syslog_facility_code"),
            Some(&EventValue::Integer(4))
        );
        assert_eq!(
            out[0].get("syslog_severity_code"),
            Some(&EventValue::Integer(2))
        );
        assert_eq!(
            out[0].get("syslog_facility"),
            Some(&EventValue::String("security/authorization".into()))
        );
        assert_eq!(
            out[0].get("syslog_severity"),
            Some(&EventValue::String("critical".into()))
        );
    }

    #[tokio::test]
    async fn test_decode_pri_string_value() {
        // PRI delivered as a string (e.g. from a grok capture) still decodes.
        let f = SyslogPriFilter::from_config(&serde_json::json!({}), None).expect("config");
        let mut event = Event::new("x");
        event.set("syslog_pri", EventValue::String("191".into()));
        let out = f.filter(event).await.expect("filter");
        // 191 → facility 23 (local7), severity 7 (debug).
        assert_eq!(
            out[0].get("syslog_facility_code"),
            Some(&EventValue::Integer(23))
        );
        assert_eq!(
            out[0].get("syslog_severity_code"),
            Some(&EventValue::Integer(7))
        );
        assert_eq!(
            out[0].get("syslog_facility"),
            Some(&EventValue::String("local7".into()))
        );
        assert_eq!(
            out[0].get("syslog_severity"),
            Some(&EventValue::String("debug".into()))
        );
    }

    #[tokio::test]
    async fn test_default_pri_when_absent() {
        // No syslog_pri field → default 13 → facility 1 (user-level), severity 5 (notice).
        let f = SyslogPriFilter::from_config(&serde_json::json!({}), None).expect("config");
        let out = f.filter(Event::new("x")).await.expect("filter");
        assert_eq!(
            out[0].get("syslog_facility_code"),
            Some(&EventValue::Integer(1))
        );
        assert_eq!(
            out[0].get("syslog_severity_code"),
            Some(&EventValue::Integer(5))
        );
        assert_eq!(
            out[0].get("syslog_facility"),
            Some(&EventValue::String("user-level".into()))
        );
        assert_eq!(
            out[0].get("syslog_severity"),
            Some(&EventValue::String("notice".into()))
        );
    }

    #[tokio::test]
    async fn test_unparseable_pri_uses_default() {
        let f = SyslogPriFilter::from_config(&serde_json::json!({}), None).expect("config");
        let mut event = Event::new("x");
        event.set("syslog_pri", EventValue::String("garbage".into()));
        let out = f.filter(event).await.expect("filter");
        assert_eq!(
            out[0].get("syslog_facility_code"),
            Some(&EventValue::Integer(1))
        );
        assert_eq!(
            out[0].get("syslog_severity_code"),
            Some(&EventValue::Integer(5))
        );
    }

    #[tokio::test]
    async fn test_use_labels_false_skips_label_fields() {
        let f = SyslogPriFilter::from_config(&serde_json::json!({ "use_labels": false }), None)
            .expect("config");
        let mut event = Event::new("x");
        event.set("syslog_pri", EventValue::Integer(34));
        let out = f.filter(event).await.expect("filter");
        assert_eq!(
            out[0].get("syslog_facility_code"),
            Some(&EventValue::Integer(4))
        );
        assert!(out[0].get("syslog_facility").is_none());
        assert!(out[0].get("syslog_severity").is_none());
    }

    #[tokio::test]
    async fn test_custom_field_name_and_label_override() {
        let f = SyslogPriFilter::from_config(
            &serde_json::json!({
                "syslog_pri_field_name": "pri",
                "severity_labels": ["s0","s1","s2","s3","s4","s5","s6","s7"]
            }),
            None,
        )
        .expect("config");
        let mut event = Event::new("x");
        event.set("pri", EventValue::Integer(34));
        let out = f.filter(event).await.expect("filter");
        assert_eq!(
            out[0].get("syslog_severity"),
            Some(&EventValue::String("s2".into()))
        );
    }
}
