// SPDX-License-Identifier: Apache-2.0
//! User-Agent filter — parse User-Agent strings into structured fields.

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::FilterPlugin;
use indexmap::IndexMap;

#[derive(Debug)]
pub struct UseragentFilter {
    source: String,
    target: String,
    prefix: String,
    condition: Option<Condition>,
    patterns: Vec<UaPattern>,
    os_patterns: Vec<OsPattern>,
}

#[derive(Debug)]
struct UaPattern {
    regex: regex::Regex,
    name: &'static str,
}

#[derive(Debug)]
struct OsPattern {
    regex: regex::Regex,
    os_name: &'static str,
}

impl UseragentFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let source = settings
            .get("source")
            .and_then(|v| v.as_str())
            .unwrap_or("message")
            .to_string();

        let target = settings
            .get("target")
            .and_then(|v| v.as_str())
            .unwrap_or("ua")
            .to_string();

        let prefix = settings
            .get("prefix")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let patterns = build_browser_patterns();
        let os_patterns = build_os_patterns();

        Ok(Self {
            source,
            target,
            prefix,
            condition,
            patterns,
            os_patterns,
        })
    }
}

fn build_browser_patterns() -> Vec<UaPattern> {
    let defs: Vec<(&str, &str)> = vec![
        (r"(?i)Edg[eA]?/(\d+)\.(\d+)\.(\d+)", "Edge"),
        (r"(?i)OPR/(\d+)\.(\d+)\.(\d+)", "Opera"),
        (r"(?i)Vivaldi/(\d+)\.(\d+)\.(\d+)", "Vivaldi"),
        (r"(?i)Firefox/(\d+)\.(\d+)(?:\.(\d+))?", "Firefox"),
        (r"(?i)Chrome/(\d+)\.(\d+)\.(\d+)", "Chrome"),
        (r"(?i)Version/(\d+)\.(\d+)(?:\.(\d+))?\s+Safari/", "Safari"),
        (r"(?i)Safari/(\d+)\.(\d+)(?:\.(\d+))?", "Safari"),
        (r"(?i)MSIE\s+(\d+)\.(\d+)", "IE"),
        (r"(?i)Trident/.*?rv:(\d+)\.(\d+)", "IE"),
        (r"(?i)curl/(\d+)\.(\d+)\.(\d+)", "curl"),
    ];
    defs.into_iter()
        .filter_map(|(pat, name)| {
            regex::Regex::new(pat).ok().map(|r| UaPattern {
                regex: r,
                name: Box::leak(name.to_string().into_boxed_str()),
            })
        })
        .collect()
}

fn build_os_patterns() -> Vec<OsPattern> {
    let defs: Vec<(&str, &str)> = vec![
        (r"(?i)Windows NT (\d+)\.(\d+)", "Windows"),
        (r"(?i)Mac OS X (\d+)[_.](\d+)(?:[_.](\d+))?", "Mac OS X"),
        (r"(?i)iPhone OS (\d+)_(\d+)(?:_(\d+))?", "iOS"),
        (r"(?i)iPad.*OS (\d+)_(\d+)", "iOS"),
        // Android must come before Linux since Android UA strings contain "Linux"
        (r"(?i)Android (\d+)(?:\.(\d+)(?:\.(\d+))?)?", "Android"),
        (r"(?i)CrOS", "Chrome OS"),
        (r"(?i)Linux", "Linux"),
    ];
    defs.into_iter()
        .filter_map(|(pat, name)| {
            regex::Regex::new(pat).ok().map(|r| OsPattern {
                regex: r,
                os_name: Box::leak(name.to_string().into_boxed_str()),
            })
        })
        .collect()
}

#[async_trait]
impl FilterPlugin for UseragentFilter {
    fn name(&self) -> &'static str {
        "useragent"
    }

    async fn filter(&self, mut event: Event) -> Result<Vec<Event>> {
        let ua_string = match event.get(&self.source) {
            Some(v) => v.to_string_lossy(),
            None => return Ok(vec![event]),
        };

        let mut ua_obj = IndexMap::new();

        // Parse browser
        let mut browser_name = "Other";
        let mut major = String::new();
        let mut minor = String::new();
        let mut patch = String::new();

        for pattern in &self.patterns {
            if let Some(caps) = pattern.regex.captures(&ua_string) {
                browser_name = pattern.name;
                if let Some(m) = caps.get(1) {
                    major = m.as_str().to_string();
                }
                if let Some(m) = caps.get(2) {
                    minor = m.as_str().to_string();
                }
                if let Some(m) = caps.get(3) {
                    patch = m.as_str().to_string();
                }
                break;
            }
        }

        // Parse OS
        let mut os_name = "Other";
        let mut os_major = String::new();

        for pattern in &self.os_patterns {
            if let Some(caps) = pattern.regex.captures(&ua_string) {
                os_name = pattern.os_name;
                if let Some(m) = caps.get(1) {
                    os_major = m.as_str().to_string();
                }
                break;
            }
        }

        // Parse device
        let device = if ua_string.contains("Mobile") || ua_string.contains("Android") {
            "Mobile"
        } else if ua_string.contains("Tablet") || ua_string.contains("iPad") {
            "Tablet"
        } else if ua_string.contains("Bot")
            || ua_string.contains("bot")
            || ua_string.contains("Crawler")
            || ua_string.contains("Spider")
        {
            "Spider"
        } else {
            "PC"
        };

        let pfx = &self.prefix;

        ua_obj.insert(
            format!("{pfx}name"),
            EventValue::String(browser_name.to_string()),
        );
        ua_obj.insert(
            format!("{pfx}os"),
            EventValue::String(format!("{os_name} {os_major}").trim().to_string()),
        );
        ua_obj.insert(
            format!("{pfx}os_name"),
            EventValue::String(os_name.to_string()),
        );
        ua_obj.insert(format!("{pfx}os_major"), EventValue::String(os_major));
        ua_obj.insert(
            format!("{pfx}device"),
            EventValue::String(device.to_string()),
        );
        ua_obj.insert(format!("{pfx}major"), EventValue::String(major));
        ua_obj.insert(format!("{pfx}minor"), EventValue::String(minor));
        ua_obj.insert(format!("{pfx}patch"), EventValue::String(patch));

        event.set(self.target.clone(), EventValue::Object(ua_obj));

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
    async fn test_useragent_chrome() {
        let settings = serde_json::json!({
            "source": "agent",
            "target": "ua"
        });
        let filter = UseragentFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set(
            "agent",
            EventValue::String(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.130 Safari/537.36".into(),
            ),
        );
        let result = filter.filter(event).await.expect("filter");
        let ua = result[0].get("ua").expect("ua field");
        let obj = ua.as_object().expect("object");
        assert_eq!(obj.get("name"), Some(&EventValue::String("Chrome".into())));
        assert_eq!(obj.get("major"), Some(&EventValue::String("120".into())));
        assert_eq!(
            obj.get("os_name"),
            Some(&EventValue::String("Windows".into()))
        );
    }

    #[tokio::test]
    async fn test_useragent_firefox() {
        let settings = serde_json::json!({
            "source": "agent",
            "target": "ua"
        });
        let filter = UseragentFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set(
            "agent",
            EventValue::String(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Gecko/20100101 Firefox/121.0"
                    .into(),
            ),
        );
        let result = filter.filter(event).await.expect("filter");
        let ua = result[0].get("ua").expect("ua field");
        let obj = ua.as_object().expect("object");
        assert_eq!(obj.get("name"), Some(&EventValue::String("Firefox".into())));
        assert_eq!(
            obj.get("os_name"),
            Some(&EventValue::String("Mac OS X".into()))
        );
    }

    #[tokio::test]
    async fn test_useragent_mobile() {
        let settings = serde_json::json!({
            "source": "agent",
            "target": "ua"
        });
        let filter = UseragentFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("test");
        event.set(
            "agent",
            EventValue::String(
                "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 Chrome/120.0.6099.43 Mobile Safari/537.36".into(),
            ),
        );
        let result = filter.filter(event).await.expect("filter");
        let ua = result[0].get("ua").expect("ua field");
        let obj = ua.as_object().expect("object");
        assert_eq!(
            obj.get("device"),
            Some(&EventValue::String("Mobile".into()))
        );
        assert_eq!(
            obj.get("os_name"),
            Some(&EventValue::String("Android".into()))
        );
    }

    #[tokio::test]
    async fn test_useragent_missing_source() {
        let settings = serde_json::json!({ "source": "nonexistent" });
        let filter = UseragentFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].get("ua").is_none());
    }

    #[test]
    fn test_useragent_name() {
        let settings = serde_json::json!({});
        let filter = UseragentFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.name(), "useragent");
    }
}
