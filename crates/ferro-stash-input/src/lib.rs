// SPDX-License-Identifier: Apache-2.0
//! Input plugins for `FerroStash`.

pub mod beats;
pub mod dead_letter_queue;
pub mod elasticsearch;
pub mod file;
pub mod generator;
pub mod heartbeat;
pub mod http;
pub mod http_poller;
pub mod jdbc;
pub mod kafka;
pub mod pipeline;
pub mod redis;
pub mod s3;
pub mod sqs;
pub mod stdin;
pub mod syslog;
pub mod tcp;
pub mod udp;

use std::sync::Arc;

use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::multi_pipeline::PipelineBus;
use ferro_stash_core::plugin::InputPlugin;
use tokio::sync::RwLock;

/// Creates an input plugin by name.
pub fn create_input(name: &str, settings: &serde_json::Value) -> Result<Box<dyn InputPlugin>> {
    create_input_with_bus(name, settings, None)
}

/// Creates an input plugin by name, optionally with a pipeline bus for inter-pipeline communication.
pub fn create_input_with_bus(
    name: &str,
    settings: &serde_json::Value,
    bus: Option<Arc<RwLock<PipelineBus>>>,
) -> Result<Box<dyn InputPlugin>> {
    match name {
        "stdin" => Ok(Box::new(stdin::StdinInput::from_config(settings)?)),
        "file" => Ok(Box::new(file::FileInput::from_config(settings)?)),
        "tcp" => Ok(Box::new(tcp::TcpInput::from_config(settings)?)),
        "udp" => Ok(Box::new(udp::UdpInput::from_config(settings)?)),
        "http" => Ok(Box::new(http::HttpInput::from_config(settings)?)),
        "http_poller" => Ok(Box::new(http_poller::HttpPollerInput::from_config(settings)?)),
        "jdbc" => Ok(Box::new(jdbc::JdbcInput::from_config(settings)?)),
        "syslog" => Ok(Box::new(syslog::SyslogInput::from_config(settings)?)),
        "generator" => Ok(Box::new(generator::GeneratorInput::from_config(settings)?)),
        "heartbeat" => Ok(Box::new(heartbeat::HeartbeatInput::from_config(settings)?)),
        "beats" => Ok(Box::new(beats::BeatsInput::from_config(settings)?)),
        "elasticsearch" => Ok(Box::new(elasticsearch::ElasticsearchInput::from_config(
            settings,
        )?)),
        "kafka" => Ok(Box::new(kafka::KafkaInput::from_config(settings)?)),
        "redis" => Ok(Box::new(redis::RedisInput::from_config(settings)?)),
        "s3" => Ok(Box::new(s3::S3Input::from_config(settings)?)),
        "sqs" => Ok(Box::new(sqs::SqsInput::from_config(settings)?)),
        "dead_letter_queue" => Ok(Box::new(dead_letter_queue::DlqInput::from_config(
            settings,
        )?)),
        "pipeline" => {
            let bus = bus.ok_or_else(|| FerroStashError::Input {
                plugin: "pipeline".to_string(),
                message: "pipeline input requires a pipeline bus (multi-pipeline mode)".to_string(),
            })?;
            Ok(Box::new(pipeline::PipelineInput::from_config(
                settings, bus,
            )?))
        }
        _ => Err(FerroStashError::Input {
            plugin: name.to_string(),
            message: format!("unknown input plugin: {name}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_stdin_input() {
        let settings = serde_json::json!({});
        let input = create_input("stdin", &settings);
        assert!(input.is_ok());
        assert_eq!(input.expect("stdin input").name(), "stdin");
    }

    #[test]
    fn test_create_generator_input() {
        let settings = serde_json::json!({});
        let input = create_input("generator", &settings);
        assert!(input.is_ok());
    }

    #[test]
    fn test_create_tcp_input() {
        let settings = serde_json::json!({ "port": 5000 });
        let input = create_input("tcp", &settings);
        assert!(input.is_ok());
    }

    #[test]
    fn test_create_udp_input() {
        let settings = serde_json::json!({ "port": 5000 });
        let input = create_input("udp", &settings);
        assert!(input.is_ok());
    }

    #[test]
    fn test_create_http_input() {
        let settings = serde_json::json!({});
        let input = create_input("http", &settings);
        assert!(input.is_ok());
    }

    #[test]
    fn test_create_syslog_input() {
        let settings = serde_json::json!({});
        let input = create_input("syslog", &settings);
        assert!(input.is_ok());
    }

    #[test]
    fn test_create_heartbeat_input() {
        let settings = serde_json::json!({});
        let input = create_input("heartbeat", &settings);
        assert!(input.is_ok());
    }

    #[test]
    fn test_create_beats_input() {
        let settings = serde_json::json!({});
        let input = create_input("beats", &settings);
        assert!(input.is_ok());
    }

    #[test]
    fn test_create_file_input() {
        let settings = serde_json::json!({ "path": "/tmp/*.log" });
        let input = create_input("file", &settings);
        assert!(input.is_ok());
    }

    #[test]
    fn test_create_elasticsearch_input() {
        let settings = serde_json::json!({});
        let input = create_input("elasticsearch", &settings);
        assert!(input.is_ok());
    }

    #[test]
    fn test_create_unknown_input() {
        let settings = serde_json::json!({});
        let input = create_input("nonexistent", &settings);
        assert!(input.is_err());
    }
}
