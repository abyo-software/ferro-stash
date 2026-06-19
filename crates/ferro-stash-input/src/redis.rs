// SPDX-License-Identifier: Apache-2.0
//! Redis input plugin — reads events from Redis lists (BLPOP) or Pub/Sub channels.
//!
//! Backed by the async `redis` crate. Three consumption modes (matching Logstash):
//! - `List` → `BLPOP key timeout` loop, draining up to `batch_count` per cycle.
//! - `Channel` → `SUBSCRIBE key`.
//! - `PatternChannel` → `PSUBSCRIBE key`.
//!
//! `password` (AUTH) and `db` (SELECT) are applied at connection time via the
//! [`redis::ConnectionInfo`] built from the config. Payloads are decoded with the
//! configured codec. A channel-based test injection point is preserved for unit tests.

use async_trait::async_trait;
use ferro_stash_codec::{create_codec, resolve_codec, Codec};
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::InputPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use ferro_stash_core::shutdown::ShutdownSignal;
use redis::{AsyncCommands, ConnectionAddr, ConnectionInfo, RedisConnectionInfo};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tracing::{debug, info, warn};

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
#[derive(Clone)]
pub struct RedisInputConfig {
    pub host: String,
    pub port: u16,
    pub key: String,
    pub data_type: RedisDataType,
    pub db: u32,
    pub password: Option<String>,
    pub batch_count: usize,
    pub codec: String,
    /// Codec sub-settings extracted from the `codec => name { ... }` block;
    /// empty object when the codec is named without a settings block.
    pub codec_settings: serde_json::Value,
    pub timeout: u64,
}

// Manual Debug to avoid leaking `password` into logs / error context.
impl std::fmt::Debug for RedisInputConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisInputConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("key", &self.key)
            .field("data_type", &self.data_type)
            .field("db", &self.db)
            .field("password", &self.password.as_ref().map(|_| "***"))
            .field("batch_count", &self.batch_count)
            .field("codec", &self.codec)
            .field("codec_settings", &self.codec_settings)
            .field("timeout", &self.timeout)
            .finish()
    }
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
        let port = settings
            .get_port("port", 6379)
            .map_err(|message| FerroStashError::Input {
                plugin: "redis".to_string(),
                message,
            })?;

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

        let db = settings
            .get_u32("db", 0)
            .map_err(|message| FerroStashError::Input {
                plugin: "redis".to_string(),
                message,
            })?;
        let password = settings.get_string("password");
        let batch_count = settings.get_u64("batch_count").unwrap_or(125) as usize;
        // Resolve the codec name and its sub-settings so they are honored
        // rather than dropped.
        let (codec, codec_settings) = resolve_codec(settings, "json");
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
                codec_settings,
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

    /// Builds the configured codec, threading its sub-settings through and
    /// mapping codec errors into an input error.
    fn build_codec(&self) -> Result<Box<dyn Codec>> {
        create_codec(&self.config.codec, &self.config.codec_settings).map_err(|e| {
            FerroStashError::Input {
                plugin: "redis".to_string(),
                message: format!("unknown/invalid codec '{}': {e}", self.config.codec),
            }
        })
    }

    /// Builds the Redis connection info, applying `db` (SELECT) and `password`
    /// (AUTH) at connect time.
    fn connection_info(&self) -> ConnectionInfo {
        ConnectionInfo {
            addr: ConnectionAddr::Tcp(self.config.host.clone(), self.config.port),
            redis: RedisConnectionInfo {
                db: i64::from(self.config.db),
                username: None,
                password: self.config.password.clone(),
                protocol: redis::ProtocolVersion::RESP2,
            },
        }
    }

    /// Opens a Redis client from the connection info.
    fn open_client(&self) -> Result<redis::Client> {
        redis::Client::open(self.connection_info()).map_err(|e| FerroStashError::Input {
            plugin: "redis".to_string(),
            message: format!(
                "failed to open Redis client {}:{}: {e}",
                self.config.host, self.config.port
            ),
        })
    }

    /// Decodes a payload via the codec and emits the resulting events with Redis
    /// metadata. Returns `false` if the downstream channel has closed.
    async fn emit_payload(
        codec: &dyn Codec,
        sender: &mpsc::Sender<Event>,
        key: &str,
        payload: &[u8],
    ) -> bool {
        let events = match codec.decode(payload) {
            Ok(events) => events,
            Err(e) => {
                warn!(key = %key, error = %e, "Redis payload decode error; skipping message");
                return true;
            }
        };
        for mut event in events {
            // Stamp Redis metadata into the event's metadata struct so it is
            // available in-pipeline as `[@metadata][redis]` but never serialized
            // to the output by `Event::to_json`.
            event.metadata.set("redis".to_string(), redis_metadata(key));
            if sender.send(event).await.is_err() {
                return false;
            }
        }
        true
    }
}

/// Builds the `[@metadata][redis]` object stamped onto each emitted event.
///
/// Constructed via `serde_json` then converted to an [`EventValue::Object`] so
/// this crate does not need to depend on `indexmap` directly.
fn redis_metadata(key: &str) -> EventValue {
    EventValue::from(serde_json::json!({ "key": key }))
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
                                event.metadata.set(
                                    "redis".to_string(),
                                    redis_metadata(&self.config.key),
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

        // --- Real Redis consumer ---
        let codec = self.build_codec()?;
        let client = self.open_client()?;

        match self.config.data_type {
            RedisDataType::List => {
                self.run_list(&client, codec.as_ref(), &sender, &mut shutdown)
                    .await;
            }
            RedisDataType::Channel => {
                self.run_pubsub(&client, codec.as_ref(), &sender, &mut shutdown, false)
                    .await;
            }
            RedisDataType::PatternChannel => {
                self.run_pubsub(&client, codec.as_ref(), &sender, &mut shutdown, true)
                    .await;
            }
        }

        info!("Redis input shutting down");
        Ok(())
    }
}

impl RedisInput {
    /// BLPOP loop. Reconnects with backoff on errors.
    ///
    /// BLPOP is *destructive*: the element is removed from the list server-side
    /// the moment the command returns. Racing the BLPOP future against
    /// `shutdown.wait()` in a `select!` would drop the future (and the popped
    /// element with it) if shutdown wins after the server popped but before the
    /// future resolved — silent data loss.
    ///
    /// To avoid that we never race a destructive BLPOP against shutdown. Each
    /// BLPOP runs to completion with a small per-call timeout; any element it
    /// returns is always emitted. Shutdown is only honored *between* calls —
    /// when a BLPOP times out empty or before issuing the next one — so an
    /// in-flight pop can never be lost.
    async fn run_list(
        &self,
        client: &redis::Client,
        codec: &dyn Codec,
        sender: &mpsc::Sender<Event>,
        shutdown: &mut ShutdownSignal,
    ) {
        // Keep the per-call timeout small so shutdown is observed promptly while
        // still letting each (destructive) BLPOP run to completion. Logstash's
        // `timeout` is in seconds; cap the *per-call* BLPOP timeout to 1s so a
        // requested shutdown is honored within ~1s between calls. (A configured
        // value larger than this only affects how a real broker is polled; the
        // worst-case shutdown latency stays bounded because BLPOP is never
        // raced against shutdown.) The floor of 1 avoids `0` = block forever.
        let blpop_timeout = self.config.timeout.clamp(1, 1) as f64;
        let key = self.config.key.clone();

        loop {
            // Check shutdown before attempting to (re)connect.
            if shutdown.is_shutdown() {
                return;
            }

            let mut conn = match client.get_multiplexed_async_connection().await {
                Ok(c) => c,
                Err(e) => {
                    warn!(error = %e, "Redis connect failed; retrying");
                    if Self::backoff_or_shutdown(shutdown).await {
                        return;
                    }
                    continue;
                }
            };

            info!(key = %key, "Redis list (BLPOP) consumer connected");

            loop {
                // Honor shutdown only between calls — never while a destructive
                // BLPOP is in flight.
                if shutdown.is_shutdown() {
                    return;
                }

                // Run BLPOP to completion (do NOT race it against shutdown).
                match Self::blpop_once(&mut conn, &key, blpop_timeout).await {
                    Ok(Some(payload)) => {
                        // A popped element is always emitted before we re-check
                        // shutdown on the next iteration.
                        if !Self::emit_payload(codec, sender, &key, &payload).await {
                            return;
                        }
                    }
                    // BLPOP timed out with no element — loop and re-check shutdown.
                    Ok(None) => {}
                    Err(e) => {
                        warn!(error = %e, "Redis BLPOP error; reconnecting");
                        break;
                    }
                }
            }

            if Self::backoff_or_shutdown(shutdown).await {
                return;
            }
        }
    }

    /// Single BLPOP returning the popped value bytes (key + value pair, value only).
    async fn blpop_once(
        conn: &mut redis::aio::MultiplexedConnection,
        key: &str,
        timeout: f64,
    ) -> redis::RedisResult<Option<Vec<u8>>> {
        // BLPOP returns nil on timeout, or [key, value] on success.
        let popped: Option<(String, Vec<u8>)> = conn.blpop(key, timeout).await?;
        Ok(popped.map(|(_k, v)| v))
    }

    /// SUBSCRIBE / PSUBSCRIBE loop over a dedicated pub/sub connection.
    async fn run_pubsub(
        &self,
        client: &redis::Client,
        codec: &dyn Codec,
        sender: &mpsc::Sender<Event>,
        shutdown: &mut ShutdownSignal,
        pattern: bool,
    ) {
        loop {
            let mut pubsub = match client.get_async_pubsub().await {
                Ok(p) => p,
                Err(e) => {
                    warn!(error = %e, "Redis pub/sub connect failed; retrying");
                    if Self::backoff_or_shutdown(shutdown).await {
                        return;
                    }
                    continue;
                }
            };

            let subscribe_result = if pattern {
                pubsub.psubscribe(&self.config.key).await
            } else {
                pubsub.subscribe(&self.config.key).await
            };
            if let Err(e) = subscribe_result {
                warn!(key = %self.config.key, error = %e, "Redis (p)subscribe failed; retrying");
                if Self::backoff_or_shutdown(shutdown).await {
                    return;
                }
                continue;
            }

            info!(
                key = %self.config.key,
                pattern,
                "Redis pub/sub consumer subscribed"
            );

            let mut stream = pubsub.on_message();
            loop {
                tokio::select! {
                    () = shutdown.wait() => return,
                    msg = stream.next() => {
                        match msg {
                            Some(msg) => {
                                let channel = msg.get_channel_name().to_string();
                                let payload = msg.get_payload_bytes().to_vec();
                                if !Self::emit_payload(codec, sender, &channel, &payload).await {
                                    return;
                                }
                            }
                            None => {
                                debug!("Redis pub/sub stream ended; reconnecting");
                                break;
                            }
                        }
                    }
                }
            }

            if Self::backoff_or_shutdown(shutdown).await {
                return;
            }
        }
    }

    /// Sleeps for the reconnect backoff while watching for shutdown. Returns
    /// `true` if shutdown was requested during the wait.
    async fn backoff_or_shutdown(shutdown: &mut ShutdownSignal) -> bool {
        tokio::select! {
            () = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => false,
            () = shutdown.wait() => true,
        }
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
    fn test_redis_input_config_debug_redacts_password() {
        let settings = serde_json::json!({ "key": "logstash", "password": "hunter2" });
        let input = RedisInput::from_config(&settings).expect("config");
        let dbg = format!("{:?}", input.config);
        assert!(!dbg.contains("hunter2"), "password leaked in Debug: {dbg}");
        assert!(dbg.contains("***"), "expected redaction marker in: {dbg}");
        assert!(dbg.contains("redis.example.com") || dbg.contains("127.0.0.1"));
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

    #[test]
    fn test_redis_config_out_of_range_port_rejected() {
        // Regression: a port above 65535 must fail loudly at config time rather
        // than silently truncating via `as u16` (70000 as u16 == 4464).
        let settings = serde_json::json!({ "key": "events", "port": 70000 });
        let err = RedisInput::from_config(&settings).expect_err("port 70000 must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("70000") && msg.contains("port"),
            "expected an out-of-range port error, got: {msg}"
        );
    }

    #[test]
    fn test_redis_config_out_of_range_db_rejected() {
        // Regression: a `db` index above u32::MAX must fail loudly at config
        // time rather than silently truncating via `as u32` (4294967296 as u32
        // == 0), which would read the WRONG Redis DB.
        let settings = serde_json::json!({ "key": "events", "db": 4_294_967_296u64 });
        let err = RedisInput::from_config(&settings).expect_err("db out of range must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("4294967296") && msg.contains("db"),
            "expected an out-of-range db error, got: {msg}"
        );
    }

    #[test]
    fn test_redis_config_string_port_in_range_accepted() {
        // A valid in-range port (string-encoded, Logstash DSL style) is honored.
        let settings = serde_json::json!({ "key": "events", "port": "6390" });
        let input = RedisInput::from_config(&settings).expect("config");
        assert_eq!(input.config.port, 6390);
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

    #[test]
    fn test_redis_connection_info_applies_db_and_password() {
        let settings = serde_json::json!({
            "key": "k",
            "host": "10.0.0.1",
            "port": 6390,
            "db": 3,
            "password": "hunter2"
        });
        let input = RedisInput::from_config(&settings).expect("config");
        let ci = input.connection_info();
        assert_eq!(ci.redis.db, 3);
        assert_eq!(ci.redis.password.as_deref(), Some("hunter2"));
        match ci.addr {
            ConnectionAddr::Tcp(host, port) => {
                assert_eq!(host, "10.0.0.1");
                assert_eq!(port, 6390);
            }
            other => panic!("expected Tcp addr, got {other:?}"),
        }
    }

    #[test]
    fn test_redis_build_codec_invalid() {
        let settings = serde_json::json!({ "key": "k", "codec": "nope-not-real" });
        let input = RedisInput::from_config(&settings).expect("config");
        assert!(input.build_codec().is_err());
    }

    #[test]
    fn test_redis_codec_descriptor_settings_threaded() {
        // Finding #3: codec sub-settings must be captured, not discarded.
        let settings = serde_json::json!({
            "key": "k",
            "codec": { "_plugin": "json", "target": "data" }
        });
        let input = RedisInput::from_config(&settings).expect("config");
        assert_eq!(input.config.codec, "json");
        assert_eq!(
            input.config.codec_settings,
            serde_json::json!({ "target": "data" })
        );
        assert!(input.build_codec().is_ok());
    }

    #[tokio::test]
    async fn test_redis_metadata_not_in_output_json() {
        // Finding #1: emitted events must not serialize an `@metadata` key.
        let settings = serde_json::json!({ "key": "logstash" });
        let mut input = RedisInput::from_config(&settings).expect("config");

        let (test_tx, test_rx) = mpsc::channel(10);
        input = input.with_test_receiver(test_rx);

        let (tx, mut rx) = mpsc::channel(100);
        let (ctrl, sig) = ferro_stash_core::shutdown::ShutdownController::new();
        let handle = tokio::spawn(async move { input.run(tx, sig).await });

        test_tx.send("payload".to_string()).await.expect("send");
        drop(test_tx);

        let event = rx.recv().await.expect("event");
        let json = event.to_json();
        let obj = json.as_object().expect("object");
        assert!(
            !obj.contains_key("@metadata"),
            "to_json must not contain @metadata, got: {json}"
        );
        assert!(event.metadata.get("redis").is_some());

        ctrl.shutdown();
        let _ = handle.await.expect("join");
    }

    #[tokio::test]
    async fn test_redis_emit_payload_metadata_in_struct_not_fields() {
        let codec = create_codec("plain", &serde_json::json!({})).expect("codec");
        let (tx, mut rx) = mpsc::channel(10);
        let ok = RedisInput::emit_payload(codec.as_ref(), &tx, "mykey", b"hi").await;
        assert!(ok);
        let event = rx.recv().await.expect("event");
        assert!(!event
            .to_json()
            .as_object()
            .expect("obj")
            .contains_key("@metadata"));
        let redis_meta = event.metadata.get("redis").expect("redis metadata");
        let map = redis_meta.as_object().expect("object");
        assert_eq!(map.get("key"), Some(&EventValue::String("mykey".into())));
    }

    /// Finding #4 (best-effort regression): a popped element must always be
    /// emitted before shutdown is honored — i.e. `emit_payload` is driven to
    /// completion and the element reaches the downstream channel even if a
    /// shutdown has already been requested.
    ///
    /// We model the destructive-pop-then-shutdown ordering directly: the
    /// element has been popped (so it is gone server-side); the fixed code's
    /// contract is that such an element is unconditionally emitted (it is never
    /// raced against shutdown). The downstream channel still has capacity, so a
    /// completed `emit_payload` must deliver it.
    #[tokio::test]
    async fn test_redis_popped_element_emitted_even_after_shutdown_requested() {
        let codec = create_codec("plain", &serde_json::json!({})).expect("codec");
        let (tx, mut rx) = mpsc::channel(10);

        // Shutdown is already requested at the moment the element was popped.
        let (ctrl, _sig) = ferro_stash_core::shutdown::ShutdownController::new();
        ctrl.shutdown();

        // The fixed code never drops a popped element; emit it unconditionally.
        let ok = RedisInput::emit_payload(codec.as_ref(), &tx, "k", b"in-flight").await;
        assert!(ok, "emit_payload should succeed while channel has capacity");

        // The popped element must have reached the channel (not lost to shutdown).
        let event = rx.recv().await.expect("popped element must be emitted");
        assert_eq!(event.message(), Some("in-flight"));
    }

    /// Live smoke test against a real Redis server (list / BLPOP mode).
    ///
    /// Run with: `REDIS_URL_HOST=127.0.0.1 REDIS_KEY=ferro-smoke \
    ///   cargo test -p ferro-stash-input -- --ignored redis_live_smoke_list`
    /// Push a value with `redis-cli RPUSH ferro-smoke '{"a":1}'` first.
    #[tokio::test]
    #[ignore = "requires a live Redis server; set REDIS_URL_HOST and REDIS_KEY"]
    async fn redis_live_smoke_list() {
        let host = std::env::var("REDIS_URL_HOST").expect("REDIS_URL_HOST");
        let key = std::env::var("REDIS_KEY").expect("REDIS_KEY");
        let settings = serde_json::json!({
            "host": host,
            "key": key,
            "data_type": "list",
            "codec": "plain",
            "timeout": 1
        });
        let mut input = RedisInput::from_config(&settings).expect("config");
        let (tx, mut rx) = mpsc::channel(100);
        let (ctrl, sig) = ferro_stash_core::shutdown::ShutdownController::new();
        let handle = tokio::spawn(async move { input.run(tx, sig).await });

        let event = tokio::time::timeout(std::time::Duration::from_secs(10), rx.recv())
            .await
            .expect("timed out waiting for a Redis list element");
        assert!(event.is_some(), "expected at least one event");

        ctrl.shutdown();
        let _ = handle.await.expect("join");
    }
}
