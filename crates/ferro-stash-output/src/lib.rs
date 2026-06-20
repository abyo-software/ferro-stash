// SPDX-License-Identifier: Apache-2.0
//! Output plugins for `FerroStash`.

pub mod cloudwatch;
pub mod csv;
pub mod datadog;
pub mod elasticsearch;
pub mod email;
pub mod file;
pub mod graphite;
pub mod http;
pub mod jdbc;
pub mod kafka;
pub mod null;
pub mod pipeline;
pub mod rabbitmq;
pub mod redis;
pub mod s3;
pub mod sns;
pub mod sqs;
pub mod stdout;
pub mod tcp;
pub mod udp;

use std::sync::Arc;

use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::multi_pipeline::PipelineBus;
use ferro_stash_core::plugin::OutputPlugin;
use tokio::sync::RwLock;

/// Maximum number of bytes of an HTTP error-response body that is read into a
/// log line or error message on a diagnostic/failure path.
///
/// Bounds the body via [`ferro_stash_core::bounded_snippet`] so a
/// misconfigured/hostile/proxy-fronted endpoint returning a huge error body
/// cannot amplify logs or pressure memory per request/retry.
pub(crate) const ERROR_BODY_SNIPPET_LIMIT: usize = 512;

/// Creates an output plugin by name.
pub fn create_output(
    name: &str,
    settings: &serde_json::Value,
    condition: Option<Condition>,
) -> Result<Box<dyn OutputPlugin>> {
    create_output_with_bus(name, settings, condition, None)
}

/// Creates an output plugin by name, optionally with a pipeline bus.
pub fn create_output_with_bus(
    name: &str,
    settings: &serde_json::Value,
    condition: Option<Condition>,
    bus: Option<Arc<RwLock<PipelineBus>>>,
) -> Result<Box<dyn OutputPlugin>> {
    match name {
        "stdout" => Ok(Box::new(stdout::StdoutOutput::from_config(
            settings, condition,
        )?)),
        "elasticsearch" | "ferrosearch" | "opensearch" => Ok(Box::new(
            elasticsearch::ElasticsearchOutput::from_config(settings, condition)?,
        )),
        "file" => Ok(Box::new(file::FileOutput::from_config(
            settings, condition,
        )?)),
        "http" => Ok(Box::new(http::HttpOutput::from_config(
            settings, condition,
        )?)),
        "tcp" => Ok(Box::new(tcp::TcpOutput::from_config(settings, condition)?)),
        "udp" => Ok(Box::new(udp::UdpOutput::from_config(settings, condition)?)),
        "csv" => Ok(Box::new(csv::CsvOutput::from_config(settings, condition)?)),
        "jdbc" => Ok(Box::new(jdbc::JdbcOutput::from_config(
            settings, condition,
        )?)),
        "null" => Ok(Box::new(null::NullOutput::from_config(
            settings, condition,
        )?)),
        "kafka" => Ok(Box::new(kafka::KafkaOutput::from_config(
            settings, condition,
        )?)),
        "redis" => Ok(Box::new(redis::RedisOutput::from_config(
            settings, condition,
        )?)),
        "s3" => Ok(Box::new(s3::S3Output::from_config(settings, condition)?)),
        "sqs" => Ok(Box::new(sqs::SqsOutput::from_config(settings, condition)?)),
        "sns" => Ok(Box::new(sns::SnsOutput::from_config(settings, condition)?)),
        "rabbitmq" => Ok(Box::new(rabbitmq::RabbitmqOutput::from_config(
            settings, condition,
        )?)),
        "email" => Ok(Box::new(email::EmailOutput::from_config(
            settings, condition,
        )?)),
        "cloudwatch" => Ok(Box::new(cloudwatch::CloudwatchOutput::from_config(
            settings, condition,
        )?)),
        "datadog" => Ok(Box::new(datadog::DatadogOutput::from_config(
            settings, condition,
        )?)),
        "graphite" => Ok(Box::new(graphite::GraphiteOutput::from_config(
            settings, condition,
        )?)),
        "pipeline" => {
            let bus = bus.ok_or_else(|| FerroStashError::Output {
                plugin: "pipeline".to_string(),
                message: "pipeline output requires a pipeline bus (multi-pipeline mode)"
                    .to_string(),
            })?;
            Ok(Box::new(pipeline::PipelineOutput::from_config(
                settings, bus, condition,
            )?))
        }
        _ => Err(FerroStashError::Output {
            plugin: name.to_string(),
            message: format!("unknown output plugin: {name}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_stdout_output() {
        let settings = serde_json::json!({});
        let output = create_output("stdout", &settings, None);
        assert!(output.is_ok());
        assert_eq!(output.expect("stdout output").name(), "stdout");
    }

    #[test]
    fn test_create_null_output() {
        let settings = serde_json::json!({});
        let output = create_output("null", &settings, None);
        assert!(output.is_ok());
    }

    #[test]
    fn test_create_elasticsearch_output() {
        let settings = serde_json::json!({});
        let output = create_output("elasticsearch", &settings, None);
        assert!(output.is_ok());
    }

    #[test]
    fn test_create_ferrosearch_output() {
        let settings = serde_json::json!({});
        let output = create_output("ferrosearch", &settings, None);
        assert!(output.is_ok());
    }

    #[test]
    fn test_create_opensearch_output() {
        let settings = serde_json::json!({});
        let output = create_output("opensearch", &settings, None);
        assert!(output.is_ok());
    }

    #[test]
    fn test_create_file_output() {
        let settings = serde_json::json!({ "path": "/tmp/out.log" });
        let output = create_output("file", &settings, None);
        assert!(output.is_ok());
    }

    #[test]
    fn test_create_http_output() {
        let settings = serde_json::json!({ "url": "http://example.com" });
        let output = create_output("http", &settings, None);
        assert!(output.is_ok());
    }

    #[test]
    fn test_create_tcp_output() {
        let settings = serde_json::json!({ "host": "localhost", "port": 5000 });
        let output = create_output("tcp", &settings, None);
        assert!(output.is_ok());
    }

    #[test]
    fn test_create_udp_output() {
        let settings = serde_json::json!({ "host": "127.0.0.1", "port": 9000 });
        let output = create_output("udp", &settings, None);
        assert!(output.is_ok());
        assert_eq!(output.expect("udp output").name(), "udp");
    }

    #[test]
    fn test_create_csv_output() {
        let settings = serde_json::json!({ "path": "/tmp/out.csv", "fields": ["message"] });
        let output = create_output("csv", &settings, None);
        assert!(output.is_ok());
        assert_eq!(output.expect("csv output").name(), "csv");
    }

    #[test]
    fn test_create_graphite_output() {
        let settings = serde_json::json!({ "host": "localhost", "fields_are_metrics": true });
        let output = create_output("graphite", &settings, None);
        assert!(output.is_ok());
        assert_eq!(output.expect("graphite output").name(), "graphite");
    }

    #[test]
    fn test_create_rabbitmq_output() {
        let settings = serde_json::json!({ "exchange": "ex", "key": "rk" });
        let output = create_output("rabbitmq", &settings, None);
        assert!(output.is_ok());
        assert_eq!(output.expect("rabbitmq output").name(), "rabbitmq");
    }

    #[test]
    fn test_create_email_output() {
        let settings = serde_json::json!({ "to": "ops@example.com" });
        let output = create_output("email", &settings, None);
        assert!(output.is_ok());
        assert_eq!(output.expect("email output").name(), "email");
    }

    #[test]
    fn test_create_cloudwatch_output() {
        let settings = serde_json::json!({ "namespace": "MyApp" });
        let output = create_output("cloudwatch", &settings, None);
        assert!(output.is_ok());
        assert_eq!(output.expect("cloudwatch output").name(), "cloudwatch");
    }

    #[test]
    fn test_create_unknown_output() {
        let settings = serde_json::json!({});
        let output = create_output("nonexistent", &settings, None);
        assert!(output.is_err());
    }
}
