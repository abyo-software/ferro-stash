// SPDX-License-Identifier: Apache-2.0
//! Redis output plugin — pushes events to Redis lists or publishes to Pub/Sub channels.
//!
//! Uses the async `redis` crate with a `ConnectionManager` for automatic
//! reconnection. List mode pipelines a batch of `RPUSH`es; channel mode issues
//! `PUBLISH` per event. Serialization is driven by the configured codec.

use async_trait::async_trait;
use ferro_stash_codec::{create_codec_from_settings, resolve_codec, Codec};
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::Event;
use ferro_stash_core::plugin::OutputPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use std::time::Duration;

use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use tokio::sync::OnceCell;
use tracing::{debug, info};

/// Per-attempt connection timeout so a dead Redis fails fast instead of hanging.
const REDIS_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Per-command response timeout.
const REDIS_RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);
/// Number of reconnection retries before surfacing an error.
const REDIS_CONNECT_RETRIES: usize = 3;
/// Maximum backoff (ms) between reconnection attempts.
const REDIS_MAX_RECONNECT_DELAY_MS: u64 = 2000;
/// Hard cap on total connection-establishment time (initial connect + retries).
const REDIS_ESTABLISH_TIMEOUT: Duration = Duration::from_secs(15);

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
///
/// `Debug` is implemented manually so the `password` secret is never rendered in
/// logs/diagnostics (`{:?}` prints `Some("***")` / `None`, not the plaintext).
#[derive(Clone)]
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

impl std::fmt::Debug for RedisOutputConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Redact the password so neither this struct nor any wrapper (e.g.
        // `RedisOutput`'s Debug) can leak the secret via `{:?}`.
        let password = self.password.as_ref().map(|_| "***");
        f.debug_struct("RedisOutputConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("key", &self.key)
            .field("data_type", &self.data_type)
            .field("db", &self.db)
            .field("password", &password)
            .field("codec", &self.codec)
            .field("batch", &self.batch)
            .field("batch_events", &self.batch_events)
            .field("congestion_interval", &self.congestion_interval)
            .field("congestion_threshold", &self.congestion_threshold)
            .finish()
    }
}

pub struct RedisOutput {
    config: RedisOutputConfig,
    condition: Option<Condition>,
    /// Codec used to serialize each event before pushing/publishing.
    codec: Box<dyn Codec>,
    /// Lazily-established, auto-reconnecting connection manager.
    connection: OnceCell<ConnectionManager>,
}

impl std::fmt::Debug for RedisOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisOutput")
            .field("config", &self.config)
            .field("condition", &self.condition)
            .field("codec", &self.codec)
            .field("connected", &self.connection.get().is_some())
            .finish()
    }
}

impl RedisOutput {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let host = settings
            .get_string("host")
            .unwrap_or_else(|| "127.0.0.1".to_string());
        let port = settings
            .get_port("port", 6379)
            .map_err(|message| FerroStashError::Output {
                plugin: "redis".to_string(),
                message,
            })?;

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

        // Validate the DB index instead of truncating via `as u32` — a value like
        // 4294967296 would silently wrap to 0 and write to the WRONG Redis DB.
        let db = settings
            .get_u32("db", 0)
            .map_err(|message| FerroStashError::Output {
                plugin: "redis".to_string(),
                message,
            })?;
        let password = settings.get_string("password");
        // Resolve the codec name from both DSL forms so the recorded name matches
        // the codec that is actually built below.
        let (codec, _) = resolve_codec(settings, "json");
        let batch = settings.get_bool("batch").unwrap_or(false);
        let batch_events = settings.get_u64("batch_events").unwrap_or(50) as usize;
        let congestion_interval = settings.get_u64("congestion_interval").unwrap_or(1);
        let congestion_threshold = settings.get_u64("congestion_threshold").unwrap_or(0) as usize;

        // Build the codec used to serialize events (config error => fail loudly).
        // `create_codec_from_settings` handles both the string form
        // (`codec => json`) and the descriptor form (`codec => json { ... }`);
        // `get_string("codec")` cannot see the descriptor form, so without this it
        // would silently fall back to the default codec and drop its sub-settings.
        let codec_impl = create_codec_from_settings(settings, "json")?;

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
            codec: codec_impl,
            connection: OnceCell::new(),
        })
    }

    /// Returns the auto-reconnecting connection manager, establishing it on first use.
    async fn connection(&self) -> Result<ConnectionManager> {
        let manager = self
            .connection
            .get_or_try_init(|| async {
                let conn_info = redis::ConnectionInfo {
                    addr: redis::ConnectionAddr::Tcp(self.config.host.clone(), self.config.port),
                    redis: redis::RedisConnectionInfo {
                        db: i64::from(self.config.db),
                        username: None,
                        password: self.config.password.clone(),
                        protocol: redis::ProtocolVersion::default(),
                    },
                };
                let client =
                    redis::Client::open(conn_info).map_err(|e| FerroStashError::Output {
                        plugin: "redis".to_string(),
                        message: format!("redis client error: {e}"),
                    })?;
                let cfg = ConnectionManagerConfig::new()
                    .set_connection_timeout(REDIS_CONNECT_TIMEOUT)
                    .set_response_timeout(REDIS_RESPONSE_TIMEOUT)
                    .set_number_of_retries(REDIS_CONNECT_RETRIES)
                    // Cap the exponential backoff between reconnection attempts.
                    .set_max_delay(REDIS_MAX_RECONNECT_DELAY_MS);
                // Bound the total establishment time so a dead Redis surfaces an
                // error promptly rather than blocking the pipeline indefinitely.
                let established = tokio::time::timeout(
                    REDIS_ESTABLISH_TIMEOUT,
                    ConnectionManager::new_with_config(client, cfg),
                )
                .await
                .map_err(|_| FerroStashError::Output {
                    plugin: "redis".to_string(),
                    message: format!(
                        "redis connection timed out after {}s",
                        REDIS_ESTABLISH_TIMEOUT.as_secs()
                    ),
                })?;
                established.map_err(|e| FerroStashError::Output {
                    plugin: "redis".to_string(),
                    message: format!("redis connection error: {e}"),
                })
            })
            .await?;
        // ConnectionManager is cheaply cloneable (Arc-backed) and clones share the
        // same underlying multiplexed connection.
        Ok(manager.clone())
    }

    /// Serialize an event to bytes via the configured codec.
    fn encode(&self, event: &Event) -> Result<Vec<u8>> {
        self.codec
            .encode(event)
            .map_err(|e| FerroStashError::Output {
                plugin: "redis".to_string(),
                message: format!("codec encode error: {e}"),
            })
    }
}

#[async_trait]
impl OutputPlugin for RedisOutput {
    fn name(&self) -> &'static str {
        "redis"
    }

    async fn output(&self, events: Vec<Event>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        // Serialize all events up-front so codec errors fail before touching Redis.
        let payloads: Vec<Vec<u8>> = events
            .iter()
            .map(|e| self.encode(e))
            .collect::<Result<Vec<_>>>()?;

        let mut conn = self.connection().await?;

        match self.config.data_type {
            RedisOutputDataType::List => {
                // RPUSH key v1 v2 ... — a single multi-value RPUSH per batch.
                let mut cmd = redis::cmd("RPUSH");
                cmd.arg(&self.config.key);
                for payload in &payloads {
                    cmd.arg(payload.as_slice());
                }
                let len: i64 =
                    cmd.query_async(&mut conn)
                        .await
                        .map_err(|e| FerroStashError::Output {
                            plugin: "redis".to_string(),
                            message: format!("RPUSH failed: {e}"),
                        })?;
                debug!(
                    key = %self.config.key,
                    pushed = payloads.len(),
                    list_len = len,
                    "Redis output: RPUSH complete"
                );
                info!(
                    key = %self.config.key,
                    event_count = payloads.len(),
                    "Redis output: RPUSHed events to list"
                );
            }
            RedisOutputDataType::Channel => {
                // PUBLISH key msg — pipeline all events in the batch.
                let mut pipe = redis::pipe();
                for payload in &payloads {
                    pipe.cmd("PUBLISH")
                        .arg(&self.config.key)
                        .arg(payload.as_slice());
                }
                let _receivers: Vec<i64> =
                    pipe.query_async(&mut conn)
                        .await
                        .map_err(|e| FerroStashError::Output {
                            plugin: "redis".to_string(),
                            message: format!("PUBLISH failed: {e}"),
                        })?;
                info!(
                    key = %self.config.key,
                    event_count = payloads.len(),
                    "Redis output: PUBLISHed events to channel"
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

    #[test]
    fn test_redis_output_port_out_of_range_rejected() {
        // An out-of-range port (e.g. 70000) must fail loudly at config time
        // rather than silently truncating (70000 as u16 == 4464) and
        // connecting to the WRONG endpoint.
        let settings = serde_json::json!({ "key": "k", "port": 70000 });
        let err = RedisOutput::from_config(&settings, None)
            .expect_err("out-of-range port must be rejected");
        let msg = format!("{err}");
        assert!(
            msg.contains("70000"),
            "error should mention the bad port: {msg}"
        );
    }

    #[test]
    fn test_redis_output_db_out_of_range_rejected() {
        // An out-of-range db index (> u32::MAX, e.g. 4294967296) must fail loudly
        // at config time rather than silently truncating (4294967296 as u32 == 0)
        // and writing events to the WRONG Redis DB (silent data contamination).
        let settings = serde_json::json!({ "key": "k", "db": 4_294_967_296u64 });
        let err = RedisOutput::from_config(&settings, None)
            .expect_err("out-of-range db must be rejected");
        let msg = format!("{err}");
        assert!(
            msg.contains("db"),
            "error should mention the db setting: {msg}"
        );
    }

    #[test]
    fn test_redis_output_codec_built() {
        // Unknown codec must fail loudly at config time.
        let settings = serde_json::json!({ "key": "k", "codec": "definitely-not-a-codec" });
        assert!(RedisOutput::from_config(&settings, None).is_err());
    }

    #[test]
    fn test_redis_descriptor_form_codec_honored() {
        // The DSL descriptor form `codec => plain { ... }` (object with `_plugin`)
        // must build the NAMED codec, not the default `json`. `get_string("codec")`
        // returns None for it, which used to silently fall back to the default.
        let settings = serde_json::json!({
            "key": "k",
            "codec": { "_plugin": "plain" },
        });
        let output = RedisOutput::from_config(&settings, None).expect("config");
        assert_eq!(
            output.config.codec, "plain",
            "descriptor codec name resolved"
        );

        // The built codec serializes as plain text (no JSON keys), proving the
        // descriptor form was honored end-to-end rather than defaulting to json.
        let bytes = output.encode(&Event::new("hi")).expect("encode");
        let text = String::from_utf8_lossy(&bytes);
        assert!(!text.contains("\"message\""), "plain codec body: {text}");

        // An unknown codec inside the descriptor form still fails loudly.
        let bad = serde_json::json!({
            "key": "k",
            "codec": { "_plugin": "no-such-codec" },
        });
        assert!(RedisOutput::from_config(&bad, None).is_err());
    }

    #[test]
    fn test_redis_config_debug_redacts_password() {
        // The password secret must never appear in Debug output (config or wrapper).
        let settings = serde_json::json!({
            "key": "k",
            "host": "redis.prod",
            "password": "super-secret-pw",
        });
        let output = RedisOutput::from_config(&settings, None).expect("config");

        let config_dbg = format!("{:?}", output.config);
        assert!(
            !config_dbg.contains("super-secret-pw"),
            "config Debug leaked the password: {config_dbg}"
        );
        assert!(
            config_dbg.contains("***"),
            "config Debug must mark redaction"
        );
        // Non-secret fields are still visible for diagnostics.
        assert!(
            config_dbg.contains("redis.prod"),
            "host should remain visible"
        );

        // The wrapper's Debug (which prints the config) must also not leak it.
        let output_dbg = format!("{output:?}");
        assert!(
            !output_dbg.contains("super-secret-pw"),
            "output Debug leaked the password: {output_dbg}"
        );
    }

    #[tokio::test]
    async fn test_redis_output_empty_is_ok() {
        // Empty batches must not even attempt a connection.
        let settings = serde_json::json!({ "key": "test", "host": "127.0.0.1", "port": 1 });
        let output = RedisOutput::from_config(&settings, None).expect("config");
        let result = output.output(vec![]).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_redis_output_connection_error_propagates() {
        // Port 1 is closed; connecting must error rather than panic.
        let settings = serde_json::json!({ "key": "test", "host": "127.0.0.1", "port": 1 });
        let output = RedisOutput::from_config(&settings, None).expect("config");
        let result = output.output(vec![Event::new("hello")]).await;
        assert!(result.is_err(), "expected connection error");
    }

    /// Live smoke test against a real Redis instance.
    /// Gated behind `REDIS_URL` (e.g. `redis://127.0.0.1:6379`); run with
    /// `cargo test -p ferro-stash-output -- --ignored redis_live`.
    #[tokio::test]
    #[ignore = "requires a running Redis (REDIS_URL env var)"]
    async fn redis_live_smoke() {
        let url = std::env::var("REDIS_URL").expect("REDIS_URL");
        // Parse host/port/db/password out of the URL.
        let info = url
            .parse::<redis::ConnectionInfo>()
            .expect("valid REDIS_URL");
        let (host, port) = match info.addr {
            redis::ConnectionAddr::Tcp(h, p)
            | redis::ConnectionAddr::TcpTls {
                host: h, port: p, ..
            } => (h, p),
            redis::ConnectionAddr::Unix(_) => panic!("unix sockets not supported by this test"),
        };
        let mut settings = serde_json::json!({
            "key": "ferro-stash-live-test",
            "host": host,
            "port": port,
            "db": info.redis.db,
            "codec": "json",
        });
        if let Some(pw) = info.redis.password {
            settings["password"] = serde_json::Value::String(pw);
        }
        let output = RedisOutput::from_config(&settings, None).expect("config");
        output
            .output(vec![Event::new("redis live smoke")])
            .await
            .expect("live RPUSH should succeed");
    }
}
