// SPDX-License-Identifier: Apache-2.0
//! Redis input plugin — reads events from Redis lists (BLPOP) or Pub/Sub channels.
//!
//! Production-shaped stub: full config parsing, list-pop and pub-sub loop skeletons,
//! and the `InputPlugin` trait are wired. The actual Redis connection is stubbed.
//! Replace `connect_stub` with a real Redis client (e.g., `redis-rs`) for production.

use async_trait::async_trait;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::InputPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use ferro_stash_core::shutdown::ShutdownSignal;
use tokio::sync::mpsc;
use tracing::{info, warn};

/// Redis data type determines the consumption pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedisDataType {
    /// BLPOP / BRPOP from a list.
    List,
    /// SUBSCRIBE to a channel.
    Channel,
    /// PSUBSCRIBE to a pattern.
    PatternChannel,
}

impl RedisDataType {
    fn from_str_config(s: &str) -> Self {
        match s {
            "channel" | "subscribe" => Self::Channel,
            "pattern_channel" | "psubscribe" => Self::PatternChannel,
            _ => Self::List,
        }
    }
}

/// Redis input configuration — mirrors the Logstash redis input settings.
#[derive(Debug, Clone)]
pub struct RedisInputConfig {
    pub host: String,
    pub port: u16,
    pub key: String,
    pub data_type: RedisDataType,
    pub db: u32,
    pub password: Option<String>,
    pub batch_count: usize,
    pub codec: String,
    pub timeout: u64,
}

#[derive(Debug)]
pub struct RedisInput {
    config: RedisInputConfig,
    /// Channel-based test data injection.
    test_receiver: Option<mpsc::Receiver<String>>,
}

impl RedisInput {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let host = settings
            .get_string("host")
            .unwrap_or_else(|| "127.0.0.1".to_string());
        let port = settings.get_u64("port").unwrap_or(6379) as u16;

        let key = settings
            .get_string("key")
            .ok_or_else(|| FerroStashError::Input {
                plugin: "redis".to_string(),
                message: "key is required".to_string(),
            })?;

        let data_type = settings
            .get("data_type")
            .and_then(|v| v.as_str())
            .map_or(RedisDataType::List, RedisDataType::from_str_config);

        let db = settings.get_u64("db").unwrap_or(0) as u32;
        let password = settings.get_string("password");
        let batch_count = settings.get_u64("batch_count").unwrap_or(125) as usize;
        let codec = settings
            .get_string("codec")
            .unwrap_or_else(|| "json".to_string());
        let timeout = settings.get_u64("timeout").unwrap_or(5);

        Ok(Self {
            config: RedisInputConfig {
                host,
                port,
                key,
                data_type,
                db,
                password,
                batch_count,
                codec,
                timeout,
            },
            test_receiver: None,
        })
    }

    /// Inject a channel receiver for testing.
    pub fn with_test_receiver(mut self, rx: mpsc::Receiver<String>) -> Self {
        self.test_receiver = Some(rx);
        self
    }
}

#[async_trait]
impl InputPlugin for RedisInput {
    fn name(&self) -> &'static str {
        "redis"
    }

    async fn run(
        &mut self,
        sender: mpsc::Sender<Event>,
        mut shutdown: ShutdownSignal,
    ) -> Result<()> {
        info!(
            host = %self.config.host,
            port = self.config.port,
            key = %self.config.key,
            data_type = ?self.config.data_type,
            db = self.config.db,
            "Redis input starting"
        );

        // Test mode: drain injected channel.
        if let Some(ref mut rx) = self.test_receiver {
            loop {
                tokio::select! {
                    msg = rx.recv() => {
                        match msg {
                            Some(payload) => {
                                let mut event = Event::new(&payload);
                                event.set(
                                    "[@metadata][redis][key]",
                                    EventValue::String(self.config.key.clone()),
                                );
                                if sender.send(event).await.is_err() {
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                    () = shutdown.wait() => {
                        info!("Redis input shutting down (test mode)");
                        break;
                    }
                }
            }
            return Ok(());
        }

        // --- Stub: real Redis consumer ---
        warn!("Redis input plugin: using stub implementation — configure real Redis connection for production");

        match self.config.data_type {
            RedisDataType::List => {
                // Production: BLPOP loop with batch_count
                // while !shutdown { BLPOP key timeout -> emit event }
            }
            RedisDataType::Channel => {
                // Production: SUBSCRIBE key -> message callback -> emit event
            }
            RedisDataType::PatternChannel => {
                // Production: PSUBSCRIBE key -> message callback -> emit event
            }
        }

        shutdown.wait().await;
        info!("Redis input shutting down");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redis_config_defaults() {
        let settings = serde_json::json!({ "key": "logstash" });
        let input = RedisInput::from_config(&settings).expect("config");
        assert_eq!(input.config.host, "127.0.0.1");
        assert_eq!(input.config.port, 6379);
        assert_eq!(input.config.key, "logstash");
        assert_eq!(input.config.data_type, RedisDataType::List);
        assert_eq!(input.config.db, 0);
        assert!(input.config.password.is_none());
        assert_eq!(input.config.batch_count, 125);
        assert_eq!(input.name(), "redis");
    }

    #[test]
    fn test_redis_config_full() {
        let settings = serde_json::json!({
            "host": "redis.example.com",
            "port": 6380,
            "key": "events",
            "data_type": "channel",
            "db": 2,
            "password": "secret",
            "batch_count": 50,
            "codec": "plain"
        });
        let input = RedisInput::from_config(&settings).expect("config");
        assert_eq!(input.config.host, "redis.example.com");
        assert_eq!(input.config.port, 6380);
        assert_eq!(input.config.data_type, RedisDataType::Channel);
        assert_eq!(input.config.db, 2);
        assert_eq!(input.config.password.as_deref(), Some("secret"));
    }

    #[test]
    fn test_redis_config_missing_key() {
        let settings = serde_json::json!({});
        assert!(RedisInput::from_config(&settings).is_err());
    }

    #[test]
    fn test_redis_config_pattern_channel() {
        let settings = serde_json::json!({
            "key": "events.*",
            "data_type": "pattern_channel"
        });
        let input = RedisInput::from_config(&settings).expect("config");
        assert_eq!(input.config.data_type, RedisDataType::PatternChannel);
    }

    #[tokio::test]
    async fn test_redis_input_with_test_channel() {
        let settings = serde_json::json!({ "key": "test-key" });
        let mut input = RedisInput::from_config(&settings).expect("config");

        let (test_tx, test_rx) = mpsc::channel(10);
        input = input.with_test_receiver(test_rx);

        let (tx, mut rx) = mpsc::channel(100);
        let (ctrl, sig) = ferro_stash_core::shutdown::ShutdownController::new();

        let handle = tokio::spawn(async move { input.run(tx, sig).await });

        test_tx.send("msg1".to_string()).await.expect("send");
        drop(test_tx);

        let event = rx.recv().await.expect("event");
        assert_eq!(event.message(), Some("msg1"));

        ctrl.shutdown();
        let result = handle.await.expect("join");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_redis_stub_shutdown() {
        let settings = serde_json::json!({ "key": "k" });
        let mut input = RedisInput::from_config(&settings).expect("config");
        let (tx, _rx) = mpsc::channel(100);
        let (ctrl, sig) = ferro_stash_core::shutdown::ShutdownController::new();

        let handle = tokio::spawn(async move { input.run(tx, sig).await });
        ctrl.shutdown();
        let result = handle.await.expect("join");
        assert!(result.is_ok());
    }
}
