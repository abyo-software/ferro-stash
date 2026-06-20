// SPDX-License-Identifier: Apache-2.0
//! CloudWatch input — polls Amazon CloudWatch metric statistics on an interval
//! and emits one event per datapoint. Mirrors Logstash's `cloudwatch` input for
//! the common metric-statistics case.
//!
//! ```logstash
//! input {
//!   cloudwatch {
//!     namespace    => "AWS/EC2"          # required
//!     metric_names => ["CPUUtilization"] # (alias: metrics)
//!     region       => "us-east-1"
//!     interval     => 900                # poll period (seconds)
//!     period       => 300                # statistic granularity (seconds)
//!     statistics   => ["Average", "Maximum"]
//!   }
//! }
//! ```
//!
//! Each poll issues one `GetMetricStatistics` call per metric name over the
//! window `[now - interval, now]` and emits an event per returned datapoint with
//! `metric`, `namespace`, the requested statistic values, and `unit`. Credentials
//! /endpoint follow the s3/sqs inputs.
//!
//! Scope note: dimension filtering and `GetMetricData`-style metric discovery are
//! not implemented — this polls explicit metric names via `GetMetricStatistics`.

use async_trait::async_trait;
use aws_sdk_cloudwatch::primitives::DateTime as CwDateTime;
use aws_sdk_cloudwatch::types::{Datapoint, Statistic};
use aws_sdk_cloudwatch::Client;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::InputPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use ferro_stash_core::shutdown::ShutdownSignal;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, warn};

/// CloudWatch input configuration — mirrors the Logstash cloudwatch input.
///
/// `Debug` is implemented manually so the `secret_access_key` secret is never
/// rendered in logs/diagnostics.
#[derive(Clone)]
pub struct CloudwatchInput {
    namespace: String,
    metric_names: Vec<String>,
    region: String,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    endpoint: Option<String>,
    interval: u64,
    period: i32,
    statistics: Vec<String>,
}

impl std::fmt::Debug for CloudwatchInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CloudwatchInput")
            .field("namespace", &self.namespace)
            .field("metric_names", &self.metric_names)
            .field("region", &self.region)
            .field("access_key_id", &self.access_key_id)
            .field(
                "secret_access_key",
                &self.secret_access_key.as_ref().map(|_| "***"),
            )
            .field("endpoint", &self.endpoint)
            .field("interval", &self.interval)
            .field("period", &self.period)
            .field("statistics", &self.statistics)
            .finish()
    }
}

fn parse_string_array(v: Option<&serde_json::Value>) -> Vec<String> {
    v.and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

/// Pull one statistic value out of a datapoint by name (case-insensitive).
fn datapoint_stat(dp: &Datapoint, stat: &str) -> Option<f64> {
    match stat.to_ascii_lowercase().as_str() {
        "average" => dp.average(),
        "sum" => dp.sum(),
        "minimum" => dp.minimum(),
        "maximum" => dp.maximum(),
        "samplecount" => dp.sample_count(),
        _ => None,
    }
}

impl CloudwatchInput {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let err = |m: &str| FerroStashError::Input {
            plugin: "cloudwatch".to_string(),
            message: m.to_string(),
        };
        let namespace = settings
            .get_string("namespace")
            .ok_or_else(|| err("cloudwatch input requires `namespace`"))?;

        // Accept both `metric_names` and the `metrics` alias.
        let metric_names = {
            let m = parse_string_array(settings.get("metric_names"));
            if m.is_empty() {
                parse_string_array(settings.get("metrics"))
            } else {
                m
            }
        };

        let statistics = {
            let s = parse_string_array(settings.get("statistics"));
            if s.is_empty() {
                vec!["Average".to_string()]
            } else {
                s
            }
        };

        // period is bounded to i32 (CloudWatch's wire type) and must be >= 1.
        let period = settings
            .get_u64("period")
            .unwrap_or(300)
            .clamp(1, i32::MAX as u64) as i32;

        Ok(Self {
            namespace,
            metric_names,
            region: settings
                .get_string("region")
                .unwrap_or_else(|| "us-east-1".to_string()),
            access_key_id: settings.get_string("access_key_id"),
            secret_access_key: settings.get_string("secret_access_key"),
            endpoint: settings.get_string("endpoint"),
            interval: settings.get_u64("interval").unwrap_or(900).max(1),
            period,
            statistics,
        })
    }

    async fn build_client(&self) -> Client {
        let region = aws_sdk_cloudwatch::config::Region::new(self.region.clone());
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest()).region(region);
        if let (Some(ak), Some(sk)) = (&self.access_key_id, &self.secret_access_key) {
            loader = loader.credentials_provider(aws_sdk_cloudwatch::config::Credentials::new(
                ak,
                sk,
                None,
                None,
                "ferro-stash-cloudwatch-input",
            ));
        }
        let sdk_config = loader.load().await;
        let mut cfg = aws_sdk_cloudwatch::config::Builder::from(&sdk_config);
        if let Some(ep) = &self.endpoint {
            cfg = cfg.endpoint_url(ep);
        }
        Client::from_conf(cfg.build())
    }

    /// Convert a CloudWatch datapoint into an event (one event per datapoint).
    fn datapoint_to_event(&self, metric: &str, dp: &Datapoint) -> Event {
        let mut ev = Event::empty();
        ev.set("metric", EventValue::String(metric.to_string()));
        ev.set("namespace", EventValue::String(self.namespace.clone()));
        if let Some(ts) = dp.timestamp() {
            if let Some(dt) = chrono::DateTime::from_timestamp(ts.secs(), 0) {
                ev.timestamp = dt;
            }
        }
        for stat in &self.statistics {
            if let Some(v) = datapoint_stat(dp, stat) {
                ev.set(stat.to_ascii_lowercase(), EventValue::Float(v));
            }
        }
        if let Some(unit) = dp.unit() {
            ev.set("unit", EventValue::String(unit.as_str().to_string()));
        }
        ev
    }

    /// Run one poll cycle: GetMetricStatistics for every metric name, emitting an
    /// event per datapoint. Returns `Err` only on a downstream-closed channel.
    async fn poll_once(
        &self,
        client: &Client,
        stats: &[Statistic],
        sender: &mpsc::Sender<Event>,
    ) -> std::result::Result<(), ()> {
        let end = chrono::Utc::now().timestamp();
        let start = end - self.interval as i64;
        for metric in &self.metric_names {
            let resp = client
                .get_metric_statistics()
                .namespace(&self.namespace)
                .metric_name(metric)
                .start_time(CwDateTime::from_secs(start))
                .end_time(CwDateTime::from_secs(end))
                .period(self.period)
                .set_statistics(Some(stats.to_vec()))
                .send()
                .await;
            match resp {
                Ok(out) => {
                    for dp in out.datapoints() {
                        let ev = self.datapoint_to_event(metric, dp);
                        if sender.send(ev).await.is_err() {
                            info!("cloudwatch input: downstream closed, stopping");
                            return Err(());
                        }
                    }
                }
                Err(e) => warn!(
                    metric = %metric,
                    error = %aws_sdk_cloudwatch::error::DisplayErrorContext(&e),
                    "cloudwatch GetMetricStatistics failed"
                ),
            }
        }
        Ok(())
    }
}

#[async_trait]
impl InputPlugin for CloudwatchInput {
    fn name(&self) -> &str {
        "cloudwatch"
    }

    async fn run(
        &mut self,
        sender: mpsc::Sender<Event>,
        mut shutdown: ShutdownSignal,
    ) -> Result<()> {
        let client = self.build_client().await;
        let stats: Vec<Statistic> = self
            .statistics
            .iter()
            .map(|s| Statistic::from(s.as_str()))
            .collect();
        info!(namespace = %self.namespace, metrics = self.metric_names.len(), "cloudwatch input starting");

        loop {
            if self.poll_once(&client, &stats, &sender).await.is_err() {
                return Ok(());
            }
            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(self.interval)) => {}
                () = shutdown.wait() => {
                    info!("cloudwatch input shutting down");
                    break;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requires_namespace() {
        assert!(CloudwatchInput::from_config(&serde_json::json!({})).is_err());
        assert!(
            CloudwatchInput::from_config(&serde_json::json!({ "namespace": "AWS/EC2" })).is_ok()
        );
    }

    #[test]
    fn defaults() {
        let i = CloudwatchInput::from_config(&serde_json::json!({ "namespace": "AWS/EC2" }))
            .expect("config");
        assert_eq!(i.region, "us-east-1");
        assert_eq!(i.interval, 900);
        assert_eq!(i.period, 300);
        assert_eq!(i.statistics, vec!["Average".to_string()]);
        assert_eq!(i.name(), "cloudwatch");
    }

    #[test]
    fn metric_names_and_alias() {
        let i = CloudwatchInput::from_config(&serde_json::json!({
            "namespace": "AWS/EC2", "metric_names": ["CPUUtilization", "NetworkIn"]
        }))
        .expect("config");
        assert_eq!(i.metric_names, vec!["CPUUtilization", "NetworkIn"]);

        // `metrics` alias also accepted.
        let i2 = CloudwatchInput::from_config(&serde_json::json!({
            "namespace": "AWS/EC2", "metrics": ["DiskReadOps"]
        }))
        .expect("config");
        assert_eq!(i2.metric_names, vec!["DiskReadOps"]);
    }

    #[test]
    fn statistics_and_period_overrides() {
        let i = CloudwatchInput::from_config(&serde_json::json!({
            "namespace": "NS", "statistics": ["Sum", "Maximum"],
            "period": 60, "interval": 120
        }))
        .expect("config");
        assert_eq!(i.statistics, vec!["Sum", "Maximum"]);
        assert_eq!(i.period, 60);
        assert_eq!(i.interval, 120);
    }

    #[test]
    fn debug_redacts_secret() {
        let i = CloudwatchInput::from_config(&serde_json::json!({
            "namespace": "NS", "access_key_id": "AKIA", "secret_access_key": "super-secret-sak"
        }))
        .expect("config");
        let dbg = format!("{i:?}");
        assert!(!dbg.contains("super-secret-sak"), "leaked: {dbg}");
        assert!(dbg.contains("***"));
    }

    #[test]
    fn datapoint_to_event_maps_fields() {
        let i = CloudwatchInput::from_config(&serde_json::json!({
            "namespace": "AWS/EC2", "statistics": ["Average", "Maximum"]
        }))
        .expect("config");
        let dp = Datapoint::builder()
            .average(3.5)
            .maximum(9.0)
            .unit(aws_sdk_cloudwatch::types::StandardUnit::Percent)
            .timestamp(CwDateTime::from_secs(1_700_000_000))
            .build();
        let ev = i.datapoint_to_event("CPUUtilization", &dp);
        assert_eq!(
            ev.get("metric"),
            Some(&EventValue::String("CPUUtilization".into()))
        );
        assert_eq!(
            ev.get("namespace"),
            Some(&EventValue::String("AWS/EC2".into()))
        );
        assert_eq!(ev.get("average"), Some(&EventValue::Float(3.5)));
        assert_eq!(ev.get("maximum"), Some(&EventValue::Float(9.0)));
        assert_eq!(ev.get("unit"), Some(&EventValue::String("Percent".into())));
        assert_eq!(ev.timestamp.timestamp(), 1_700_000_000);
    }

    #[test]
    fn datapoint_stat_lookup() {
        let dp = Datapoint::builder().sum(7.0).sample_count(2.0).build();
        assert_eq!(datapoint_stat(&dp, "Sum"), Some(7.0));
        assert_eq!(datapoint_stat(&dp, "samplecount"), Some(2.0));
        assert_eq!(datapoint_stat(&dp, "average"), None);
        assert_eq!(datapoint_stat(&dp, "bogus"), None);
    }

    /// Live smoke (real CloudWatch or LocalStack): set `AWS_REGION` + credentials
    /// (and optional `CLOUDWATCH_ENDPOINT`). Polls `AWS/Logs IncomingLogEvents` (or
    /// a configured `CLOUDWATCH_NAMESPACE`/`CLOUDWATCH_METRIC`). Run with:
    ///   AWS_REGION=us-east-1 AWS_ACCESS_KEY_ID=… AWS_SECRET_ACCESS_KEY=… \
    ///     cargo test -p ferro-stash-input -- --ignored cloudwatch_live
    #[tokio::test]
    #[ignore = "live: set AWS creds (+ CLOUDWATCH_ENDPOINT for LocalStack)"]
    async fn cloudwatch_live_polls() {
        use ferro_stash_core::shutdown::ShutdownController;
        if std::env::var("AWS_ACCESS_KEY_ID").is_err()
            && std::env::var("CLOUDWATCH_ENDPOINT").is_err()
        {
            eprintln!("SKIPPED: set AWS creds or CLOUDWATCH_ENDPOINT");
            return;
        }
        let mut cfg = serde_json::json!({
            "namespace": std::env::var("CLOUDWATCH_NAMESPACE").unwrap_or_else(|_| "AWS/EC2".to_string()),
            "metric_names": [std::env::var("CLOUDWATCH_METRIC").unwrap_or_else(|_| "CPUUtilization".to_string())],
            "region": std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
            "interval": 3600,
            "period": 300,
            "statistics": ["Average"],
        });
        if let Ok(ep) = std::env::var("CLOUDWATCH_ENDPOINT") {
            cfg["endpoint"] = serde_json::Value::String(ep);
        }
        let mut input = CloudwatchInput::from_config(&cfg).expect("config");
        let (tx, mut rx) = mpsc::channel(64);
        let (controller, signal) = ShutdownController::new();
        let handle = tokio::spawn(async move { input.run(tx, signal).await });
        // Just prove the poll loop runs without panicking; datapoints may be empty.
        let _ = tokio::time::timeout(Duration::from_secs(10), rx.recv()).await;
        controller.shutdown();
        let _ = handle.await;
    }
}
