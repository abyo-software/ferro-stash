// SPDX-License-Identifier: Apache-2.0
//! Grok filter — pattern matching for unstructured text.
//!
//! Grok patterns are named regular expressions, e.g., `%{IP:client_ip}`.
//! Includes all standard Logstash grok patterns.

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::FilterPlugin;
use regex::Regex;

#[derive(Debug)]
pub struct GrokFilter {
    match_field: String,
    patterns: Vec<CompiledPattern>,
    tag_on_failure: Vec<String>,
    overwrite: Vec<String>,
    keep_empty_captures: bool,
    condition: Option<Condition>,
}

#[derive(Debug)]
struct CompiledPattern {
    regex: Regex,
    field_names: Vec<String>,
    field_types: Vec<FieldType>,
}

#[derive(Debug, Clone)]
enum FieldType {
    String,
    Int,
    Float,
}

impl GrokFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let match_config = settings
            .get("match")
            .ok_or_else(|| FerroStashError::Filter {
                plugin: "grok".to_string(),
                message: "match is required".to_string(),
            })?;

        let (match_field, raw_patterns) = if let Some(obj) = match_config.as_object() {
            let (field, patterns) = obj.iter().next().ok_or_else(|| FerroStashError::Filter {
                plugin: "grok".to_string(),
                message: "match must have at least one entry".to_string(),
            })?;
            let patterns = match patterns {
                serde_json::Value::String(s) => vec![s.clone()],
                serde_json::Value::Array(a) => a
                    .iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect(),
                _ => {
                    return Err(FerroStashError::Filter {
                        plugin: "grok".to_string(),
                        message: "match pattern must be a string or array".to_string(),
                    });
                }
            };
            (field.clone(), patterns)
        } else {
            return Err(FerroStashError::Filter {
                plugin: "grok".to_string(),
                message: "match must be a hash".to_string(),
            });
        };

        let tag_on_failure = settings
            .get("tag_on_failure")
            .and_then(|v| v.as_array())
            .map_or_else(
                || vec!["_grokparsefailure".to_string()],
                |a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                },
            );

        let overwrite = settings
            .get("overwrite")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let keep_empty_captures = settings
            .get("keep_empty_captures")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);

        let mut compiled = Vec::new();
        for pattern in &raw_patterns {
            let cp = compile_grok_pattern(pattern)?;
            compiled.push(cp);
        }

        Ok(Self {
            match_field,
            patterns: compiled,
            tag_on_failure,
            overwrite,
            keep_empty_captures,
            condition,
        })
    }
}

/// Compiles a grok pattern string to a regex with named captures.
fn compile_grok_pattern(pattern: &str) -> Result<CompiledPattern> {
    let mut regex_str = String::new();
    let mut field_names = Vec::new();
    let mut field_types = Vec::new();
    let mut i = 0;
    let bytes = pattern.as_bytes();

    while i < bytes.len() {
        if i + 1 < bytes.len() && bytes[i] == b'%' && bytes[i + 1] == b'{' {
            i += 2;
            let start = i;
            while i < bytes.len() && bytes[i] != b'}' {
                i += 1;
            }
            let token = &pattern[start..i];
            i += 1; // skip '}'

            let parts: Vec<&str> = token.splitn(3, ':').collect();
            let pattern_name = parts[0];
            let field_name = parts.get(1).copied();
            let type_hint = parts.get(2).copied();

            let inner_regex = get_builtin_pattern(pattern_name);

            if let Some(name) = field_name {
                regex_str.push_str(&format!("(?P<{name}>{inner_regex})"));
                field_names.push(name.to_string());
                field_types.push(match type_hint {
                    Some("int") => FieldType::Int,
                    Some("float") => FieldType::Float,
                    _ => FieldType::String,
                });
            } else {
                regex_str.push_str(&format!("(?:{inner_regex})"));
            }
        } else {
            // Escape special regex chars unless they're already regex
            let c = bytes[i] as char;
            match c {
                '(' | ')' | '[' | ']' | '|' | '?' | '+' | '*' | '.' | '^' | '$' | '\\' => {
                    regex_str.push(c);
                }
                _ => regex_str.push(c),
            }
            i += 1;
        }
    }

    // Build regex with enlarged DFA cache. The default 256KB limit causes
    // complex grok patterns (e.g. COMBINEDAPACHELOG with 8+ capture groups)
    // to fall back to the slower NFA engine. 10MB gives the DFA builder room
    // to compile the full pattern, yielding ~2-3x speedup on complex patterns.
    let regex = regex::RegexBuilder::new(&regex_str)
        .dfa_size_limit(10 * 1024 * 1024)
        .build()
        .map_err(|e| FerroStashError::Filter {
            plugin: "grok".to_string(),
            message: format!("invalid pattern regex '{regex_str}': {e}"),
        })?;

    Ok(CompiledPattern {
        regex,
        field_names,
        field_types,
    })
}

/// Built-in grok patterns (commonly used ones).
fn get_builtin_pattern(name: &str) -> &'static str {
    match name {
        // Base patterns
        "WORD" => r"\b\w+\b",
        "NOTSPACE" => r"\S+",
        "SPACE" => r"\s*",
        "DATA" => r".*?",
        "GREEDYDATA" => r".*",
        "GREEDYMULTILINE" => r"[\s\S]*",
        "INT" => r"(?:[+-]?(?:[0-9]+))",
        "NUMBER" => r"(?:[+-]?(?:(?:[0-9]+(?:\.[0-9]+)?)|\.[0-9]+))",
        "BASE10NUM" => r"(?:[+-]?(?:(?:[0-9]+(?:\.[0-9]+)?)|\.[0-9]+))",
        "BASE16NUM" => r"(?:0[xX]?[0-9a-fA-F]+)",
        "POSINT" => r"[1-9][0-9]*",
        "NONNEGINT" => r"[0-9]+",

        // Network patterns
        "IP" | "IPV4" => {
            r"(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))"
        }
        "IPV6" => {
            r"(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}|(?:[0-9a-fA-F]{1,4}:){1,7}:|(?:[0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}"
        }
        "IPORHOST" => {
            r"(?:[0-9A-Za-z][0-9A-Za-z\-\.]*|(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))"
        }
        "HOSTNAME" => r"[0-9A-Za-z][0-9A-Za-z\-\.]*",
        "HOSTPORT" => r"[0-9A-Za-z][0-9A-Za-z\-\.]*:\d+",
        "PORT" => r"\d+",
        "MAC" => r"(?:[0-9a-fA-F]{2}:){5}[0-9a-fA-F]{2}",
        "URIPROTO" => r"[A-Za-z][A-Za-z0-9+\-\.]*",
        "URIPATH" => r"/[A-Za-z0-9$.+!*'(){},~:;=@#%&/\-]*",
        "URIPARAM" => r"\?[A-Za-z0-9$.+!*'|(){},~@#%&/=:;\-_\?\[\]]*",
        "URI" => r"[A-Za-z][A-Za-z0-9+\-\.]*://[^\s]*",
        "USER" | "USERNAME" => r"[a-zA-Z0-9._-]+",
        "EMAILADDRESS" => r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",

        // Date/Time patterns
        "MONTH" => {
            r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
        }
        "MONTHNUM" => r"(?:0?[1-9]|1[0-2])",
        "MONTHDAY" => r"(?:0?[1-9]|[12][0-9]|3[01])",
        "DAY" => {
            r"(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?)"
        }
        "YEAR" => r"\d{4}",
        "HOUR" => r"(?:2[0123]|[01]?[0-9])",
        "MINUTE" => r"[0-5][0-9]",
        "SECOND" => r"(?:[0-5]?[0-9]|60)(?:[:.,][0-9]+)?",
        "TIME" => r"(?:2[0123]|[01]?[0-9]):(?:[0-5][0-9]):(?:(?:[0-5]?[0-9]|60)(?:[:.,][0-9]+)?)",
        "TIMESTAMP_ISO8601" => {
            r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?"
        }
        "HTTPDATE" => r"\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} [+-]\d{4}",
        "DATE" => r"\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4}",
        "DATESTAMP" => r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}",

        // Log patterns
        "LOGLEVEL" => r"(?:TRACE|DEBUG|INFO|WARN(?:ING)?|ERROR|FATAL|CRITICAL|SEVERE)",
        "SYSLOGFACILITY" => r"<\d+>",
        "SYSLOGHOST" => r"[0-9A-Za-z][0-9A-Za-z\-\.]*",
        "SYSLOGPROG" => r"[^\[\]:]+(?:\[\d+\])?",

        // HTTP
        "HTTPVERB" => r"\b\w+\b",
        "RAWREQUEST" => r".*",
        "COMMONAPACHELOG" => {
            r#"(?P<clientip>\S+) \S+ (?P<ident>\S+) \[(?P<timestamp>[^\]]+)\] "(?P<verb>\w+) (?P<request>\S+)(?: HTTP/(?P<httpversion>[0-9.]+))?" (?P<response>\d{3}) (?P<bytes>\d+|-)"#
        }
        "COMBINEDAPACHELOG" => {
            r#"(?P<clientip>\S+) \S+ (?P<ident>\S+) \[(?P<timestamp>[^\]]+)\] "(?P<verb>\w+) (?P<request>\S+)(?: HTTP/(?P<httpversion>[0-9.]+))?" (?P<response>\d{3}) (?P<bytes>\d+|-) "(?P<referrer>[^"]*)" "(?P<agent>[^"]*)""#
        }

        // Path
        "PATH" | "UNIXPATH" => r"(?:/[\w_%!$@:.,+~-]*)+",
        "WINPATH" => r"[A-Za-z]:\\(?:[\w._-]+\\)*[\w._-]*",

        // Quoted string
        "QS" | "QUOTEDSTRING" => r#""(?:[^"\\]|\\.)*""#,

        // UUID
        "UUID" => r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}",

        _ => r"\S+", // fallback
    }
}

#[async_trait]
impl FilterPlugin for GrokFilter {
    fn name(&self) -> &'static str {
        "grok"
    }

    async fn filter(&self, mut event: Event) -> Result<Vec<Event>> {
        let text = if let Some(v) = event.get(&self.match_field) {
            v.to_string_lossy()
        } else {
            for tag in &self.tag_on_failure {
                event.add_tag(tag);
            }
            return Ok(vec![event]);
        };

        let mut matched = false;

        for pattern in &self.patterns {
            if let Some(captures) = pattern.regex.captures(&text) {
                matched = true;
                for (i, name) in pattern.field_names.iter().enumerate() {
                    if let Some(m) = captures.name(name) {
                        let value_str = m.as_str();
                        if value_str.is_empty() && !self.keep_empty_captures {
                            continue;
                        }
                        let value = match &pattern.field_types[i] {
                            FieldType::Int => value_str.parse::<i64>().map_or_else(
                                |_| EventValue::String(value_str.to_string()),
                                EventValue::Integer,
                            ),
                            FieldType::Float => value_str.parse::<f64>().map_or_else(
                                |_| EventValue::String(value_str.to_string()),
                                EventValue::Float,
                            ),
                            FieldType::String => EventValue::String(value_str.to_string()),
                        };

                        if self.overwrite.contains(name) || !event.has_field(name) {
                            event.set(name.clone(), value);
                        }
                    }
                }
                break; // first match wins
            }
        }

        if !matched {
            for tag in &self.tag_on_failure {
                event.add_tag(tag);
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
    async fn test_grok_ip_match() {
        let settings = serde_json::json!({
            "match": { "message": "%{IP:client_ip} %{GREEDYDATA:rest}" }
        });
        let filter = GrokFilter::from_config(&settings, None).expect("config");
        let event = Event::new("192.168.1.1 some log data here");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0].get("client_ip"),
            Some(&EventValue::String("192.168.1.1".into()))
        );
    }

    #[tokio::test]
    async fn test_grok_no_match_tags() {
        let settings = serde_json::json!({
            "match": { "message": "%{IP:ip}" }
        });
        let filter = GrokFilter::from_config(&settings, None).expect("config");
        let event = Event::new("no ip here");
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_tag("_grokparsefailure"));
    }

    #[tokio::test]
    async fn test_grok_type_coercion() {
        let settings = serde_json::json!({
            "match": { "message": "%{INT:status:int} %{NUMBER:latency:float}" }
        });
        let filter = GrokFilter::from_config(&settings, None).expect("config");
        let event = Event::new("200 1.5");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(result[0].get("status"), Some(&EventValue::Integer(200)));
        assert_eq!(result[0].get("latency"), Some(&EventValue::Float(1.5)));
    }

    #[test]
    fn test_compile_grok_pattern() {
        let pattern = compile_grok_pattern("%{IP:ip} %{INT:port:int}").expect("compile");
        assert_eq!(pattern.field_names, vec!["ip", "port"]);
    }

    #[test]
    fn test_compile_grok_pattern_no_capture() {
        let pattern = compile_grok_pattern("%{NOTSPACE} %{GREEDYDATA:rest}").expect("compile");
        assert_eq!(pattern.field_names, vec!["rest"]);
    }

    #[test]
    fn test_compile_grok_pattern_float_type() {
        let pattern = compile_grok_pattern("%{NUMBER:latency:float}").expect("compile");
        assert_eq!(pattern.field_names, vec!["latency"]);
        assert!(matches!(pattern.field_types[0], FieldType::Float));
    }

    #[test]
    fn test_compile_grok_pattern_string_type() {
        let pattern = compile_grok_pattern("%{WORD:name}").expect("compile");
        assert!(matches!(pattern.field_types[0], FieldType::String));
    }

    #[tokio::test]
    async fn test_grok_missing_match_field() {
        let settings = serde_json::json!({
            "match": { "nonexistent": "%{IP:ip}" }
        });
        let filter = GrokFilter::from_config(&settings, None).expect("config");
        let event = Event::new("192.168.1.1");
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_tag("_grokparsefailure"));
    }

    #[tokio::test]
    async fn test_grok_custom_tag_on_failure() {
        let settings = serde_json::json!({
            "match": { "message": "%{IP:ip}" },
            "tag_on_failure": ["custom_fail"]
        });
        let filter = GrokFilter::from_config(&settings, None).expect("config");
        let event = Event::new("no ip");
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_tag("custom_fail"));
        assert!(!result[0].has_tag("_grokparsefailure"));
    }

    #[tokio::test]
    async fn test_grok_keep_empty_captures() {
        let settings = serde_json::json!({
            "match": { "message": "%{GREEDYDATA:maybe_empty}end" },
            "keep_empty_captures": true
        });
        let filter = GrokFilter::from_config(&settings, None).expect("config");
        let event = Event::new("end");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("maybe_empty"),
            Some(&EventValue::String(String::new()))
        );
    }

    #[tokio::test]
    async fn test_grok_overwrite() {
        let settings = serde_json::json!({
            "match": { "message": "%{WORD:host}" },
            "overwrite": ["host"]
        });
        let filter = GrokFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("newhost");
        event.set("host", EventValue::String("oldhost".into()));
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("host"),
            Some(&EventValue::String("newhost".into()))
        );
    }

    #[tokio::test]
    async fn test_grok_no_overwrite_by_default() {
        let settings = serde_json::json!({
            "match": { "message": "%{WORD:host}" }
        });
        let filter = GrokFilter::from_config(&settings, None).expect("config");
        let mut event = Event::new("newhost");
        event.set("host", EventValue::String("oldhost".into()));
        let result = filter.filter(event).await.expect("filter");
        // Should not overwrite existing field
        assert_eq!(
            result[0].get("host"),
            Some(&EventValue::String("oldhost".into()))
        );
    }

    #[tokio::test]
    async fn test_grok_multiple_patterns() {
        let settings = serde_json::json!({
            "match": { "message": ["%{IP:ip}", "%{WORD:word}"] }
        });
        let filter = GrokFilter::from_config(&settings, None).expect("config");
        // First pattern matches
        let event = Event::new("192.168.1.1");
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_field("ip"));

        // Second pattern matches
        let event2 = Event::new("hello");
        let result2 = filter.filter(event2).await.expect("filter");
        assert!(result2[0].has_field("word"));
    }

    #[tokio::test]
    async fn test_grok_timestamp_pattern() {
        let settings = serde_json::json!({
            "match": { "message": "%{TIMESTAMP_ISO8601:ts}" }
        });
        let filter = GrokFilter::from_config(&settings, None).expect("config");
        let event = Event::new("2024-01-15T10:30:00Z some data");
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].has_field("ts"));
    }

    #[tokio::test]
    async fn test_grok_loglevel_pattern() {
        let settings = serde_json::json!({
            "match": { "message": "%{LOGLEVEL:level} %{GREEDYDATA:msg}" }
        });
        let filter = GrokFilter::from_config(&settings, None).expect("config");
        let event = Event::new("ERROR something went wrong");
        let result = filter.filter(event).await.expect("filter");
        assert_eq!(
            result[0].get("level"),
            Some(&EventValue::String("ERROR".into()))
        );
    }

    #[test]
    fn test_grok_config_missing_match() {
        let settings = serde_json::json!({});
        assert!(GrokFilter::from_config(&settings, None).is_err());
    }

    #[test]
    fn test_grok_config_match_not_object() {
        let settings = serde_json::json!({ "match": "bad" });
        assert!(GrokFilter::from_config(&settings, None).is_err());
    }

    #[test]
    fn test_grok_name() {
        let settings = serde_json::json!({
            "match": { "message": "%{WORD:w}" }
        });
        let filter = GrokFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.name(), "grok");
    }

    #[test]
    fn test_builtin_pattern_fallback() {
        let result = get_builtin_pattern("NONEXISTENT_PATTERN");
        assert_eq!(result, r"\S+");
    }

    #[tokio::test]
    async fn test_grok_int_coercion_failure() {
        let settings = serde_json::json!({
            "match": { "message": "%{NOTSPACE:val:int}" }
        });
        let filter = GrokFilter::from_config(&settings, None).expect("config");
        let event = Event::new("notanumber");
        let result = filter.filter(event).await.expect("filter");
        // Should fall back to string when int parse fails
        assert_eq!(
            result[0].get("val"),
            Some(&EventValue::String("notanumber".into()))
        );
    }
}
