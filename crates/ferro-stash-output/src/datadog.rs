// SPDX-License-Identifier: Apache-2.0
//! Datadog output plugin — sends events to the Datadog Log Intake API.
//!
//! Production-shaped stub: config parsing, Datadog JSON format, batching, and the
//! `OutputPlugin` trait are fully wired. The HTTP POST is stubbed. For production,
//! enable the actual HTTP calls (reqwest is already a dependency via the http output).

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::Event;
use ferro_stash_core::plugin::OutputPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use tracing::{info, warn};

/// Datadog output configuration — mirrors the Logstash datadog output settings.
#[derive(Debug, Clone)]
pub struct DatadogOutputConfig {
    pub api_key: String,
    pub host: String,
    pub codec: String,
    pub batch_size: usize,
    pub use_ssl: bool,
    pub source: String,
    pub service: String,
    pub tags: Vec<String>,
}

#[derive(Debug)]
pub struct DatadogOutput {
    config: DatadogOutputConfig,
    condition: Option<Condition>,
}

impl DatadogOutput {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let api_key = settings
            .get_string("api_key")
            .ok_or_else(|| FerroStashError::Output {
                plugin: "datadog".to_string(),
                message: "api_key is required".to_string(),
            })?;

        let host = settings
            .get_string("host")
            .unwrap_or_else(|| "http-intake.logs.datadoghq.com".to_string());

        let codec = settings
            .get_string("codec")
            .unwrap_or_else(|| "json".to_string());
        let batch_size = settings.get_u64("batch_size").unwrap_or(50) as usize;
        let use_ssl = settings.get_bool("use_ssl").unwrap_or(true);
        let source = settings
            .get_string("source")
            .unwrap_or_else(|| "ferro-stash".to_string());
        let service = settings.get_string("service").unwrap_or_default();

        let tags: Vec<String> = settings
            .get("tags")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        Ok(Self {
            config: DatadogOutputConfig {
                api_key,
                host,
                codec,
                batch_size,
                use_ssl,
                source,
                service,
                tags,
            },
            condition,
        })
    }

    /// Format events into the Datadog Log Intake JSON format.
    ///
    /// Returns a JSON array of log entries, each with: message, ddsource, service,
    /// ddtags, hostname, and any additional fields.
    fn format_datadog_payload(&self, events: &[Event]) -> String {
        let entries: Vec<serde_json::Value> = events
            .iter()
            .map(|event| {
                let message = event.message().unwrap_or("").to_string();
                let hostname = event
                    .get("host")
                    .map(|v| v.to_string_lossy())
                    .unwrap_or_default();

                let mut entry = serde_json::json!({
                    "message": message,
                    "ddsource": self.config.source,
                    "hostname": hostname,
                });

                if !self.config.service.is_empty() {
                    entry["service"] = serde_json::Value::String(self.config.service.clone());
                }

                if !self.config.tags.is_empty() {
                    entry["ddtags"] = serde_json::Value::String(self.config.tags.join(","));
                }

                // Include extra fields from the event.
                for (key, value) in event.fields() {
                    if key != "message" && key != "host" && key != "@timestamp" && key != "@version"
                    {
                        entry[key] = serde_json::Value::String(value.to_string_lossy());
                    }
                }

                entry
            })
            .collect();

        serde_json::to_string(&entries).unwrap_or_else(|_| "[]".to_string())
    }
}

#[async_trait]
impl OutputPlugin for DatadogOutput {
    fn name(&self) -> &'static str {
        "datadog"
    }

    async fn output(&self, events: Vec<Event>) -> Result<()> {
        warn!("Datadog output plugin: using stub implementation — configure real Datadog connection for production");

        let scheme = if self.config.use_ssl { "https" } else { "http" };
        let url = format!("{}://{}/api/v2/logs", scheme, self.config.host);

        // Process in batches.
        for chunk in events.chunks(self.config.batch_size) {
            let payload = self.format_datadog_payload(chunk);

            info!(
                url = %url,
                event_count = chunk.len(),
                payload_bytes = payload.len(),
                "Datadog output: would POST {} events to {}",
                chunk.len(),
                url,
            );

            // Production implementation:
            // reqwest::Client::new()
            //     .post(&url)
            //     .header("DD-API-KEY", &self.config.api_key)
            //     .header("Content-Type", "application/json")
            //     .body(payload)
            //     .send()
            //     .await?;
        }

        Ok(())
    }

    fn condition(&self) -> Option<&Condition> {
        self.condition.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datadog_config_defaults() {
        let settings = serde_json::json!({ "api_key": "abc123" });
        let output = DatadogOutput::from_config(&settings, None).expect("config");
        assert_eq!(output.config.api_key, "abc123");
        assert_eq!(output.config.host, "http-intake.logs.datadoghq.com");
        assert_eq!(output.config.batch_size, 50);
        assert!(output.config.use_ssl);
        assert_eq!(output.config.source, "ferro-stash");
        assert_eq!(output.name(), "datadog");
    }

    #[test]
    fn test_datadog_config_full() {
        let settings = serde_json::json!({
            "api_key": "mykey",
            "host": "http-intake.logs.datadoghq.eu",
            "codec": "json",
            "batch_size": 100,
            "use_ssl": false,
            "source": "myapp",
            "service": "backend",
            "tags": ["env:prod", "team:platform"]
        });
        let output = DatadogOutput::from_config(&settings, None).expect("config");
        assert_eq!(output.config.host, "http-intake.logs.datadoghq.eu");
        assert_eq!(output.config.batch_size, 100);
        assert!(!output.config.use_ssl);
        assert_eq!(output.config.service, "backend");
        assert_eq!(output.config.tags, vec!["env:prod", "team:platform"]);
    }

    #[test]
    fn test_datadog_missing_api_key() {
        let settings = serde_json::json!({});
        assert!(DatadogOutput::from_config(&settings, None).is_err());
    }

    #[test]
    fn test_datadog_payload_format() {
        let settings = serde_json::json!({
            "api_key": "key",
            "source": "myapp",
            "service": "web",
            "tags": ["env:test"]
        });
        let output = DatadogOutput::from_config(&settings, None).expect("config");

        let events = vec![Event::new("hello world")];
        let payload = output.format_datadog_payload(&events);
        let parsed: Vec<serde_json::Value> =
            serde_json::from_str(&payload).expect("valid JSON array");

        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0]["message"], "hello world");
        assert_eq!(parsed[0]["ddsource"], "myapp");
        assert_eq!(parsed[0]["service"], "web");
        assert_eq!(parsed[0]["ddtags"], "env:test");
    }

    #[tokio::test]
    async fn test_datadog_output_stub_succeeds() {
        let settings = serde_json::json!({ "api_key": "key" });
        let output = DatadogOutput::from_config(&settings, None).expect("config");
        let result = output.output(vec![Event::new("test")]).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_datadog_output_batching() {
        let settings = serde_json::json!({
            "api_key": "key",
            "batch_size": 2
        });
        let output = DatadogOutput::from_config(&settings, None).expect("config");
        let events = vec![Event::new("e1"), Event::new("e2"), Event::new("e3")];
        // Should process 2 batches (2+1) without error
        let result = output.output(events).await;
        assert!(result.is_ok());
    }
}
