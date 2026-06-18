// SPDX-License-Identifier: Apache-2.0
//! Redis output plugin — pushes events to Redis lists or publishes to Pub/Sub channels.
//!
//! Production-shaped stub: config parsing, list-push and pub-sub structure, and the
//! `OutputPlugin` trait are fully wired. The actual Redis connection is stubbed.

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::Event;
use ferro_stash_core::plugin::OutputPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use tracing::{info, warn};

/// Redis data type for output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedisOutputDataType {
    /// RPUSH to a list.
    List,
    /// PUBLISH to a channel.
    Channel,
}

impl RedisOutputDataType {
    fn from_str_config(s: &str) -> Self {
        match s {
            "channel" | "publish" => Self::Channel,
            _ => Self::List,
        }
    }
}

/// Redis output configuration — mirrors the Logstash redis output settings.
#[derive(Debug, Clone)]
pub struct RedisOutputConfig {
    pub host: String,
    pub port: u16,
    pub key: String,
    pub data_type: RedisOutputDataType,
    pub db: u32,
    pub password: Option<String>,
    pub codec: String,
    pub batch: bool,
    pub batch_events: usize,
    pub congestion_interval: u64,
    pub congestion_threshold: usize,
}

#[derive(Debug)]
pub struct RedisOutput {
    config: RedisOutputConfig,
    condition: Option<Condition>,
}

impl RedisOutput {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let host = settings
            .get_string("host")
            .unwrap_or_else(|| "127.0.0.1".to_string());
        let port = settings.get_u64("port").unwrap_or(6379) as u16;

        let key = settings.get_string("key").ok_or_else(|| {
            ferro_stash_core::error::FerroStashError::Output {
                plugin: "redis".to_string(),
                message: "key is required".to_string(),
            }
        })?;

        let data_type = settings.get("data_type").and_then(|v| v.as_str()).map_or(
            RedisOutputDataType::List,
            RedisOutputDataType::from_str_config,
        );

        let db = settings.get_u64("db").unwrap_or(0) as u32;
        let password = settings.get_string("password");
        let codec = settings
            .get_string("codec")
            .unwrap_or_else(|| "json".to_string());
        let batch = settings.get_bool("batch").unwrap_or(false);
        let batch_events = settings.get_u64("batch_events").unwrap_or(50) as usize;
        let congestion_interval = settings.get_u64("congestion_interval").unwrap_or(1);
        let congestion_threshold = settings.get_u64("congestion_threshold").unwrap_or(0) as usize;

        Ok(Self {
            config: RedisOutputConfig {
                host,
                port,
                key,
                data_type,
                db,
                password,
                codec,
                batch,
                batch_events,
                congestion_interval,
                congestion_threshold,
            },
            condition,
        })
    }
}

#[async_trait]
impl OutputPlugin for RedisOutput {
    fn name(&self) -> &'static str {
        "redis"
    }

    async fn output(&self, events: Vec<Event>) -> Result<()> {
        warn!("Redis output plugin: using stub implementation — configure real Redis connection for production");

        match self.config.data_type {
            RedisOutputDataType::List => {
                // Production: RPUSH key serialized_event (or LPUSH for batch via pipeline)
                info!(
                    host = %self.config.host,
                    port = self.config.port,
                    key = %self.config.key,
                    event_count = events.len(),
                    "Redis output: would RPUSH {} events to key '{}'",
                    events.len(),
                    self.config.key,
                );
            }
            RedisOutputDataType::Channel => {
                // Production: PUBLISH key serialized_event
                info!(
                    host = %self.config.host,
                    port = self.config.port,
                    key = %self.config.key,
                    event_count = events.len(),
                    "Redis output: would PUBLISH {} events to channel '{}'",
                    events.len(),
                    self.config.key,
                );
            }
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
    fn test_redis_output_config_defaults() {
        let settings = serde_json::json!({ "key": "logstash" });
        let output = RedisOutput::from_config(&settings, None).expect("config");
        assert_eq!(output.config.host, "127.0.0.1");
        assert_eq!(output.config.port, 6379);
        assert_eq!(output.config.key, "logstash");
        assert_eq!(output.config.data_type, RedisOutputDataType::List);
        assert_eq!(output.config.db, 0);
        assert!(output.config.password.is_none());
        assert_eq!(output.name(), "redis");
    }

    #[test]
    fn test_redis_output_config_full() {
        let settings = serde_json::json!({
            "host": "redis.prod",
            "port": 6380,
            "key": "events",
            "data_type": "channel",
            "db": 3,
            "password": "secret",
            "codec": "plain",
            "batch": true,
            "batch_events": 100
        });
        let output = RedisOutput::from_config(&settings, None).expect("config");
        assert_eq!(output.config.host, "redis.prod");
        assert_eq!(output.config.port, 6380);
        assert_eq!(output.config.data_type, RedisOutputDataType::Channel);
        assert_eq!(output.config.db, 3);
        assert!(output.config.batch);
        assert_eq!(output.config.batch_events, 100);
    }

    #[test]
    fn test_redis_output_missing_key() {
        let settings = serde_json::json!({});
        assert!(RedisOutput::from_config(&settings, None).is_err());
    }

    #[tokio::test]
    async fn test_redis_output_stub_list() {
        let settings = serde_json::json!({ "key": "test" });
        let output = RedisOutput::from_config(&settings, None).expect("config");
        let result = output.output(vec![Event::new("hello")]).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_redis_output_stub_channel() {
        let settings = serde_json::json!({ "key": "events", "data_type": "channel" });
        let output = RedisOutput::from_config(&settings, None).expect("config");
        let result = output.output(vec![Event::new("pub")]).await;
        assert!(result.is_ok());
    }
}
