// SPDX-License-Identifier: Apache-2.0
//! Graphite output plugin — sends metrics to a Carbon endpoint over TCP.
//!
//! For each event the output connects to Carbon and writes one or more
//! plaintext lines of the form `metric value timestamp\n`, where the timestamp
//! is the event's `@timestamp` as Unix epoch seconds.
//!
//! Metrics are selected one of two ways:
//!
//! - `metrics` — a map of `metric-name => field-name`, where both the name and
//!   the value support `%{field}` sprintf interpolation. The key produces the
//!   metric path; the value produces the sample (typically `%{some_field}`).
//! - `fields_are_metrics` — when `true`, every numeric event field (integer or
//!   float) is emitted as `field_name value timestamp`.
//!
//! At least one of `metrics` / `fields_are_metrics` must be configured;
//! otherwise the output would silently send nothing, which is rejected at
//! config time.
//!
//! Config keys (Logstash-compatible): `host` (required), `port` (default
//! `2003`), `metrics`, `fields_are_metrics`.

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::OutputPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

#[derive(Debug)]
pub struct GraphiteOutput {
    host: String,
    port: u16,
    /// `(metric-name template, value template)` pairs; both `%{field}`-aware.
    metrics: Vec<(String, String)>,
    /// When set, every numeric event field is emitted as a metric.
    fields_are_metrics: bool,
    condition: Option<Condition>,
}

impl GraphiteOutput {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let host = settings
            .get("host")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| FerroStashError::Output {
                plugin: "graphite".to_string(),
                message: "host is required".to_string(),
            })?
            .to_string();

        let port = settings
            .get_port("port", 2003)
            .map_err(|message| FerroStashError::Output {
                plugin: "graphite".to_string(),
                message,
            })?;

        let metrics: Vec<(String, String)> = settings
            .get("metrics")
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let fields_are_metrics = settings.get_bool("fields_are_metrics").unwrap_or(false);

        // A config with neither `metrics` nor `fields_are_metrics` would
        // connect to Carbon and send nothing for every event — a silent no-op.
        // Reject it loudly so a misconfiguration is caught at load time.
        if metrics.is_empty() && !fields_are_metrics {
            return Err(FerroStashError::Output {
                plugin: "graphite".to_string(),
                message: "one of `metrics` (a metric=>field map) or `fields_are_metrics => true` \
                          must be configured; otherwise no metrics would be sent"
                    .to_string(),
            });
        }

        Ok(Self {
            host,
            port,
            metrics,
            fields_are_metrics,
            condition,
        })
    }

    /// Render every Carbon line for one event (`metric value timestamp\n`).
    fn render_event(&self, event: &Event) -> String {
        let ts = event.timestamp.timestamp();
        let mut out = String::new();

        if self.fields_are_metrics {
            for (name, value) in event.fields() {
                if matches!(value, EventValue::Integer(_) | EventValue::Float(_)) {
                    out.push_str(&format!("{name} {} {ts}\n", value.to_string_lossy()));
                }
            }
        }

        for (metric_tpl, value_tpl) in &self.metrics {
            let name = event.sprintf(metric_tpl);
            let value = event.sprintf(value_tpl);
            out.push_str(&format!("{name} {value} {ts}\n"));
        }

        out
    }
}

#[async_trait]
impl OutputPlugin for GraphiteOutput {
    fn name(&self) -> &'static str {
        "graphite"
    }

    async fn output(&self, events: Vec<Event>) -> Result<()> {
        let payload: String = events.iter().map(|e| self.render_event(e)).collect();
        if payload.is_empty() {
            return Ok(());
        }

        let addr = format!("{}:{}", self.host, self.port);
        let mut stream = TcpStream::connect(&addr)
            .await
            .map_err(|e| FerroStashError::Output {
                plugin: "graphite".to_string(),
                message: format!("connect error to {addr}: {e}"),
            })?;

        stream
            .write_all(payload.as_bytes())
            .await
            .map_err(|e| FerroStashError::Output {
                plugin: "graphite".to_string(),
                message: format!("write error: {e}"),
            })?;
        stream.flush().await.map_err(|e| FerroStashError::Output {
            plugin: "graphite".to_string(),
            message: format!("flush error: {e}"),
        })?;

        Ok(())
    }

    fn condition(&self) -> Option<&Condition> {
        self.condition.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    // ----- Config tests -----

    #[test]
    fn test_graphite_output_requires_host() {
        let settings = serde_json::json!({ "fields_are_metrics": true });
        let err = GraphiteOutput::from_config(&settings, None).expect_err("host is required");
        assert!(err.to_string().contains("host"), "got: {err}");
    }

    #[test]
    fn test_graphite_output_requires_metrics_or_fields() {
        let settings = serde_json::json!({ "host": "localhost" });
        let err =
            GraphiteOutput::from_config(&settings, None).expect_err("must require metrics/fields");
        assert!(err.to_string().contains("metrics"), "got: {err}");
    }

    #[test]
    fn test_graphite_output_defaults_and_metrics_parse() {
        let settings = serde_json::json!({
            "host": "carbon.local",
            "metrics": { "server.load": "%{load}" }
        });
        let output = GraphiteOutput::from_config(&settings, None).expect("config");
        assert_eq!(output.host, "carbon.local");
        assert_eq!(output.port, 2003);
        assert_eq!(output.metrics.len(), 1);
        assert_eq!(output.name(), "graphite");
    }

    #[test]
    fn test_graphite_output_fields_are_metrics_parse() {
        let settings = serde_json::json!({ "host": "h", "port": 2004, "fields_are_metrics": true });
        let output = GraphiteOutput::from_config(&settings, None).expect("config");
        assert!(output.fields_are_metrics);
        assert_eq!(output.port, 2004);
    }

    #[test]
    fn test_graphite_output_out_of_range_port_rejected() {
        let settings =
            serde_json::json!({ "host": "h", "port": 70000, "fields_are_metrics": true });
        let err = GraphiteOutput::from_config(&settings, None).expect_err("port 70000 rejected");
        assert!(err.to_string().contains("70000"), "got: {err}");
    }

    // ----- Render tests -----

    #[test]
    fn test_render_event_metrics_map_interpolates() {
        let settings = serde_json::json!({
            "host": "h",
            "metrics": { "server.%{host}.load": "%{load}" }
        });
        let output = GraphiteOutput::from_config(&settings, None).expect("config");
        let mut event = Event::new("ignored");
        event.set("host", EventValue::String("web1".into()));
        event.set("load", EventValue::Integer(42));
        let rendered = output.render_event(&event);
        // metric name and value both come from sprintf; ends with a timestamp.
        assert!(
            rendered.starts_with("server.web1.load 42 "),
            "got: {rendered}"
        );
        let tokens: Vec<&str> = rendered.trim_end().split(' ').collect();
        assert_eq!(tokens.len(), 3, "metric value timestamp; got: {rendered}");
        assert!(
            tokens[2].parse::<i64>().is_ok(),
            "third token is epoch: {rendered}"
        );
    }

    #[test]
    fn test_render_event_fields_are_metrics_only_numeric() {
        let settings = serde_json::json!({ "host": "h", "fields_are_metrics": true });
        let output = GraphiteOutput::from_config(&settings, None).expect("config");
        let mut event = Event::new("a string message");
        event.set("cpu", EventValue::Float(0.5));
        event.set("conns", EventValue::Integer(10));
        let rendered = output.render_event(&event);
        assert!(rendered.contains("cpu 0.5 "), "got: {rendered}");
        assert!(rendered.contains("conns 10 "), "got: {rendered}");
        // The non-numeric `message` field is not emitted as a metric.
        assert!(!rendered.contains("message "), "got: {rendered}");
    }

    // ----- Behaviour test over a loopback TCP socket -----

    #[tokio::test]
    async fn test_graphite_output_sends_line_to_carbon() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let (tx, rx) = tokio::sync::oneshot::channel::<String>();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut received = String::new();
                let _ = stream.read_to_string(&mut received).await;
                let _ = tx.send(received);
            }
        });

        let settings = serde_json::json!({
            "host": addr.ip().to_string(),
            "port": addr.port(),
            "metrics": { "server.load": "%{load}" }
        });
        let output = GraphiteOutput::from_config(&settings, None).expect("config");
        let mut event = Event::new("ignored");
        event.set("load", EventValue::Integer(42));
        output.output(vec![event]).await.expect("output");

        let received = tokio::time::timeout(std::time::Duration::from_secs(2), rx)
            .await
            .expect("recv timeout")
            .expect("received");
        assert!(received.starts_with("server.load 42 "), "got: {received}");
        assert!(
            received.ends_with('\n'),
            "carbon line must end with newline: {received:?}"
        );
    }
}
