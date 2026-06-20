// SPDX-License-Identifier: Apache-2.0
//! CloudWatch output — derives a metric from each event and sends it to Amazon
//! CloudWatch via `PutMetricData`. Mirrors Logstash's `cloudwatch` output for the
//! common case.
//!
//! ```logstash
//! output {
//!   cloudwatch {
//!     namespace  => "MyApp"
//!     region     => "us-east-1"
//!     metricname => "%{metric}"          # metric name (or a literal), %{}-aware
//!     value      => "%{value}"           # numeric value, %{}-aware
//!     unit       => "Count"              # CloudWatch StandardUnit, %{}-aware
//!     dimensions => { "host" => "%{host}" }  # name => %{}-aware value
//!   }
//! }
//! ```
//!
//! Credentials/endpoint follow the s3/sqs outputs (static creds when both set,
//! else the default AWS provider chain; `endpoint` for LocalStack). Events are
//! sent in batches of up to 20 `MetricDatum` per `PutMetricData` call. Events
//! whose metric name resolves empty/unresolved or whose value does not parse as a
//! number are skipped with a warning (rather than failing the whole batch).

use async_trait::async_trait;
use aws_sdk_cloudwatch::types::{Dimension, MetricDatum, StandardUnit};
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::Event;
use ferro_stash_core::plugin::OutputPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use tokio::sync::OnceCell;
use tracing::warn;

/// CloudWatch `PutMetricData` accepts at most 1000 metrics per call; we batch
/// conservatively at 20 to stay well within historical/throttling limits.
const CW_MAX_BATCH: usize = 20;

/// CloudWatch output configuration — mirrors the Logstash cloudwatch output.
///
/// `Debug` is implemented manually so the `secret_access_key` secret is never
/// rendered in logs/diagnostics (`{:?}` prints `Some("***")` / `None`).
#[derive(Clone)]
pub struct CloudwatchOutputConfig {
    pub namespace: String,
    pub region: String,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub endpoint: Option<String>,
    pub metricname: String,
    pub value: String,
    pub unit: String,
    /// `(dimension_name, value_template)` pairs (value is `%{field}`-aware).
    pub dimensions: Vec<(String, String)>,
}

impl std::fmt::Debug for CloudwatchOutputConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let secret_access_key = self.secret_access_key.as_ref().map(|_| "***");
        f.debug_struct("CloudwatchOutputConfig")
            .field("namespace", &self.namespace)
            .field("region", &self.region)
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &secret_access_key)
            .field("endpoint", &self.endpoint)
            .field("metricname", &self.metricname)
            .field("value", &self.value)
            .field("unit", &self.unit)
            .field("dimensions", &self.dimensions)
            .finish()
    }
}

#[derive(Debug)]
pub struct CloudwatchOutput {
    config: CloudwatchOutputConfig,
    condition: Option<Condition>,
    client: OnceCell<aws_sdk_cloudwatch::Client>,
}

fn parse_dimensions(v: Option<&serde_json::Value>) -> Vec<(String, String)> {
    v.and_then(|v| v.as_object())
        .map(|o| {
            o.iter()
                .filter_map(|(k, val)| val.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

impl CloudwatchOutput {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        Ok(Self {
            config: CloudwatchOutputConfig {
                namespace: settings
                    .get_string("namespace")
                    .unwrap_or_else(|| "FerroStash".to_string()),
                region: settings
                    .get_string("region")
                    .unwrap_or_else(|| "us-east-1".to_string()),
                access_key_id: settings.get_string("access_key_id"),
                secret_access_key: settings.get_string("secret_access_key"),
                endpoint: settings.get_string("endpoint"),
                metricname: settings
                    .get_string("metricname")
                    .unwrap_or_else(|| "%{metric}".to_string()),
                value: settings
                    .get_string("value")
                    .unwrap_or_else(|| "%{value}".to_string()),
                unit: settings
                    .get_string("unit")
                    .unwrap_or_else(|| "Count".to_string()),
                dimensions: parse_dimensions(settings.get("dimensions")),
            },
            condition,
            client: OnceCell::new(),
        })
    }

    async fn client(&self) -> &aws_sdk_cloudwatch::Client {
        self.client
            .get_or_init(|| async {
                let region = aws_sdk_cloudwatch::config::Region::new(self.config.region.clone());
                let mut loader =
                    aws_config::defaults(aws_config::BehaviorVersion::latest()).region(region);
                if let (Some(ak), Some(sk)) =
                    (&self.config.access_key_id, &self.config.secret_access_key)
                {
                    loader =
                        loader.credentials_provider(aws_sdk_cloudwatch::config::Credentials::new(
                            ak,
                            sk,
                            None,
                            None,
                            "ferro-stash-cloudwatch-output",
                        ));
                }
                let sdk_config = loader.load().await;
                let mut cfg = aws_sdk_cloudwatch::config::Builder::from(&sdk_config);
                if let Some(ep) = &self.config.endpoint {
                    cfg = cfg.endpoint_url(ep);
                }
                aws_sdk_cloudwatch::Client::from_conf(cfg.build())
            })
            .await
    }

    /// Build a `MetricDatum` from one event, or `None` if the event lacks a usable
    /// metric name / numeric value.
    fn build_datum(&self, event: &Event) -> Option<MetricDatum> {
        let name = event.sprintf(&self.config.metricname);
        if name.is_empty() || name.contains("%{") {
            warn!(metricname = %name, "cloudwatch output: skipping event with empty/unresolved metric name");
            return None;
        }
        let value_str = event.sprintf(&self.config.value);
        let value = match value_str.trim().parse::<f64>() {
            Ok(v) => v,
            Err(_) => {
                warn!(value = %value_str, "cloudwatch output: skipping event with non-numeric value");
                return None;
            }
        };
        let unit = event.sprintf(&self.config.unit);

        let dimensions: Vec<Dimension> = self
            .config
            .dimensions
            .iter()
            .filter_map(|(dname, dval_tmpl)| {
                let dval = event.sprintf(dval_tmpl);
                if dval.is_empty() || dval.contains("%{") {
                    None
                } else {
                    Some(Dimension::builder().name(dname).value(dval).build())
                }
            })
            .collect();

        let mut builder = MetricDatum::builder()
            .metric_name(name)
            .value(value)
            .unit(StandardUnit::from(unit.as_str()));
        if !dimensions.is_empty() {
            builder = builder.set_dimensions(Some(dimensions));
        }
        Some(builder.build())
    }
}

#[async_trait]
impl OutputPlugin for CloudwatchOutput {
    fn name(&self) -> &str {
        "cloudwatch"
    }

    async fn output(&self, events: Vec<Event>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let data: Vec<MetricDatum> = events.iter().filter_map(|e| self.build_datum(e)).collect();
        if data.is_empty() {
            return Ok(());
        }
        let client = self.client().await;
        for chunk in data.chunks(CW_MAX_BATCH) {
            let mut req = client.put_metric_data().namespace(&self.config.namespace);
            for datum in chunk {
                req = req.metric_data(datum.clone());
            }
            req.send().await.map_err(|e| FerroStashError::Output {
                plugin: "cloudwatch".to_string(),
                message: format!(
                    "PutMetricData failed: {}",
                    aws_sdk_cloudwatch::error::DisplayErrorContext(&e)
                ),
            })?;
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
    use ferro_stash_core::event::EventValue;

    #[test]
    fn defaults() {
        let o = CloudwatchOutput::from_config(&serde_json::json!({}), None).expect("config");
        assert_eq!(o.config.namespace, "FerroStash");
        assert_eq!(o.config.region, "us-east-1");
        assert_eq!(o.config.metricname, "%{metric}");
        assert_eq!(o.config.value, "%{value}");
        assert_eq!(o.config.unit, "Count");
        assert_eq!(o.name(), "cloudwatch");
    }

    #[test]
    fn full_config() {
        let o = CloudwatchOutput::from_config(
            &serde_json::json!({
                "namespace": "MyApp", "region": "eu-west-1",
                "metricname": "requests", "value": "%{count}", "unit": "Bytes",
                "dimensions": { "host": "%{host}", "env": "prod" },
            }),
            None,
        )
        .expect("config");
        assert_eq!(o.config.namespace, "MyApp");
        assert_eq!(o.config.region, "eu-west-1");
        assert_eq!(o.config.unit, "Bytes");
        assert_eq!(o.config.dimensions.len(), 2);
    }

    #[test]
    fn debug_redacts_secret() {
        let o = CloudwatchOutput::from_config(
            &serde_json::json!({
                "access_key_id": "AKIA", "secret_access_key": "super-secret-sak",
                "namespace": "NS",
            }),
            None,
        )
        .expect("config");
        let cfg_dbg = format!("{:?}", o.config);
        assert!(!cfg_dbg.contains("super-secret-sak"), "leaked: {cfg_dbg}");
        assert!(cfg_dbg.contains("***"));
        assert!(cfg_dbg.contains("NS"));
        let out_dbg = format!("{o:?}");
        assert!(
            !out_dbg.contains("super-secret-sak"),
            "wrapper leaked: {out_dbg}"
        );
    }

    #[test]
    fn build_datum_derives_name_value_unit_dimensions() {
        let o = CloudwatchOutput::from_config(
            &serde_json::json!({
                "metricname": "%{metric}", "value": "%{value}", "unit": "%{unit}",
                "dimensions": { "host": "%{host}" },
            }),
            None,
        )
        .expect("config");
        let mut ev = Event::new("x");
        ev.set("metric", EventValue::String("Latency".into()));
        ev.set("value", EventValue::Float(12.5));
        ev.set("unit", EventValue::String("Milliseconds".into()));
        ev.set("host", EventValue::String("h1".into()));
        let datum = o.build_datum(&ev).expect("datum built");
        assert_eq!(datum.metric_name(), Some("Latency"));
        assert_eq!(datum.value(), Some(12.5));
        assert_eq!(datum.unit(), Some(&StandardUnit::Milliseconds));
        let dims = datum.dimensions();
        assert_eq!(dims.len(), 1);
        assert_eq!(dims[0].name(), Some("host"));
        assert_eq!(dims[0].value(), Some("h1"));
    }

    #[test]
    fn build_datum_skips_unresolved_name_and_bad_value() {
        let o = CloudwatchOutput::from_config(
            &serde_json::json!({ "metricname": "%{metric}", "value": "%{value}" }),
            None,
        )
        .expect("config");
        // Missing `metric` field → unresolved name → skipped.
        let mut ev = Event::new("x");
        ev.set("value", EventValue::Integer(1));
        assert!(o.build_datum(&ev).is_none());

        // Present name but non-numeric value → skipped.
        let mut ev2 = Event::new("x");
        ev2.set("metric", EventValue::String("M".into()));
        ev2.set("value", EventValue::String("not-a-number".into()));
        assert!(o.build_datum(&ev2).is_none());
    }

    #[tokio::test]
    async fn empty_is_ok() {
        let o = CloudwatchOutput::from_config(&serde_json::json!({}), None).expect("config");
        assert!(o.output(vec![]).await.is_ok());
    }

    /// Live smoke (real CloudWatch or LocalStack): set `AWS_REGION` + credentials
    /// (and optional `CLOUDWATCH_ENDPOINT` for LocalStack). Run with:
    ///   CLOUDWATCH_ENDPOINT=http://localhost:4566 AWS_REGION=us-east-1 \
    ///   AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
    ///     cargo test -p ferro-stash-output -- --ignored cloudwatch_live
    #[tokio::test]
    #[ignore = "live: set AWS creds (+ CLOUDWATCH_ENDPOINT for LocalStack)"]
    async fn cloudwatch_live_put() {
        if std::env::var("AWS_ACCESS_KEY_ID").is_err()
            && std::env::var("CLOUDWATCH_ENDPOINT").is_err()
        {
            eprintln!("SKIPPED: set AWS creds or CLOUDWATCH_ENDPOINT");
            return;
        }
        let mut cfg = serde_json::json!({
            "namespace": "FerroStashLiveSmoke",
            "region": std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
            "metricname": "smoke",
            "value": "%{value}",
            "unit": "Count",
        });
        if let Ok(ep) = std::env::var("CLOUDWATCH_ENDPOINT") {
            cfg["endpoint"] = serde_json::Value::String(ep);
        }
        if let (Ok(ak), Ok(sk)) = (
            std::env::var("AWS_ACCESS_KEY_ID"),
            std::env::var("AWS_SECRET_ACCESS_KEY"),
        ) {
            cfg["access_key_id"] = serde_json::Value::String(ak);
            cfg["secret_access_key"] = serde_json::Value::String(sk);
        }
        let output = CloudwatchOutput::from_config(&cfg, None).expect("config");
        let mut ev = Event::new("x");
        ev.set("value", EventValue::Integer(42));
        output
            .output(vec![ev])
            .await
            .expect("live PutMetricData should succeed");
    }
}
