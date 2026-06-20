// SPDX-License-Identifier: Apache-2.0
//! RabbitMQ input — consumes messages from an AMQP queue, decodes each delivery
//! body with the configured codec, emits the events, and (by default)
//! acknowledges the delivery. Mirrors Logstash's `rabbitmq` input for the common
//! case. Backed by the `lapin` AMQP client (rustls TLS stance).
//!
//! ```logstash
//! input {
//!   rabbitmq {
//!     host     => "localhost"
//!     port     => 5672
//!     vhost    => "/"
//!     user     => "guest"
//!     password => "guest"
//!     queue    => "logs"
//!     exchange => "amq.topic"   # optional: bind the queue to this exchange
//!     key      => "app.#"       # optional: routing key for the bind
//!     durable  => true
//!     ack      => true
//!     codec    => "json"
//!   }
//! }
//! ```

use async_trait::async_trait;
use ferro_stash_codec::{create_codec, resolve_codec, Codec};
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::Event;
use ferro_stash_core::plugin::InputPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use ferro_stash_core::shutdown::ShutdownSignal;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicNackOptions, QueueBindOptions, QueueDeclareOptions,
};
use lapin::types::FieldTable;
use lapin::uri::{AMQPAuthority, AMQPScheme, AMQPUri, AMQPUserInfo};
use lapin::{Connection, ConnectionProperties};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tracing::{info, warn};

/// RabbitMQ input configuration — mirrors the Logstash rabbitmq input settings.
///
/// `Debug` is implemented manually so the `password` secret is never rendered in
/// logs/diagnostics (`{:?}` prints `"***"`, not the plaintext).
#[derive(Clone)]
pub struct RabbitmqInput {
    host: String,
    port: u16,
    vhost: String,
    user: String,
    password: String,
    queue: String,
    exchange: Option<String>,
    key: Option<String>,
    durable: bool,
    ack: bool,
    codec: String,
    codec_settings: serde_json::Value,
}

impl std::fmt::Debug for RabbitmqInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RabbitmqInput")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("vhost", &self.vhost)
            .field("user", &self.user)
            .field("password", &"***")
            .field("queue", &self.queue)
            .field("exchange", &self.exchange)
            .field("key", &self.key)
            .field("durable", &self.durable)
            .field("ack", &self.ack)
            .field("codec", &self.codec)
            .finish()
    }
}

/// What to do with a consumed delivery after attempting decode + send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeliveryAction {
    /// Decode + send succeeded and acking is enabled: ack the delivery.
    Ack,
    /// Decode failed (with acking enabled): nack **without requeue** so the
    /// broker routes it to the queue's dead-letter exchange (or drops it),
    /// instead of silently acking and dropping the data.
    NackToDlx,
    /// Acking is disabled in config: leave the delivery to the broker's policy.
    Leave,
}

/// Decide what to do with a delivery. A decode failure must never ack (which
/// would acknowledge — and so drop — a message we failed to process).
fn delivery_action(ack_enabled: bool, decode_ok: bool) -> DeliveryAction {
    match (ack_enabled, decode_ok) {
        (true, true) => DeliveryAction::Ack,
        (true, false) => DeliveryAction::NackToDlx,
        (false, _) => DeliveryAction::Leave,
    }
}

impl RabbitmqInput {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let err = |m: &str| FerroStashError::Input {
            plugin: "rabbitmq".to_string(),
            message: m.to_string(),
        };
        let queue = settings
            .get_string("queue")
            .ok_or_else(|| err("rabbitmq input requires `queue`"))?;
        let port = settings
            .get_port("port", 5672)
            .map_err(|message| FerroStashError::Input {
                plugin: "rabbitmq".to_string(),
                message,
            })?;
        let (codec, codec_settings) = resolve_codec(settings, "json");
        Ok(Self {
            host: settings
                .get_string("host")
                .unwrap_or_else(|| "localhost".to_string()),
            port,
            vhost: settings
                .get_string("vhost")
                .unwrap_or_else(|| "/".to_string()),
            user: settings
                .get_string("user")
                .unwrap_or_else(|| "guest".to_string()),
            password: settings
                .get_string("password")
                .unwrap_or_else(|| "guest".to_string()),
            queue,
            exchange: settings.get_string("exchange"),
            key: settings.get_string("key"),
            durable: settings.get_bool("durable").unwrap_or(true),
            ack: settings.get_bool("ack").unwrap_or(true),
            codec,
            codec_settings,
        })
    }

    fn build_codec(&self) -> Result<Box<dyn Codec>> {
        create_codec(&self.codec, &self.codec_settings).map_err(|e| FerroStashError::Input {
            plugin: "rabbitmq".to_string(),
            message: format!("unknown/invalid codec '{}': {e}", self.codec),
        })
    }

    /// Build the AMQP URI from the configured parts (avoids string parsing /
    /// percent-encoding pitfalls — the vhost/user/password are taken verbatim).
    fn build_uri(&self) -> AMQPUri {
        AMQPUri {
            scheme: AMQPScheme::AMQP,
            authority: AMQPAuthority {
                userinfo: AMQPUserInfo {
                    username: self.user.clone(),
                    password: self.password.clone(),
                },
                host: self.host.clone(),
                port: self.port,
            },
            vhost: self.vhost.clone(),
            ..AMQPUri::default()
        }
    }
}

#[async_trait]
impl InputPlugin for RabbitmqInput {
    fn name(&self) -> &str {
        "rabbitmq"
    }

    async fn run(
        &mut self,
        sender: mpsc::Sender<Event>,
        mut shutdown: ShutdownSignal,
    ) -> Result<()> {
        let codec = self.build_codec()?;
        let conn = Connection::connect_uri(self.build_uri(), ConnectionProperties::default())
            .await
            .map_err(|e| FerroStashError::Input {
                plugin: "rabbitmq".to_string(),
                message: format!("connect failed: {e}"),
            })?;
        let channel = conn
            .create_channel()
            .await
            .map_err(|e| FerroStashError::Input {
                plugin: "rabbitmq".to_string(),
                message: format!("create_channel failed: {e}"),
            })?;

        let qopts = QueueDeclareOptions {
            durable: self.durable,
            ..QueueDeclareOptions::default()
        };
        channel
            .queue_declare(&self.queue, qopts, FieldTable::default())
            .await
            .map_err(|e| FerroStashError::Input {
                plugin: "rabbitmq".to_string(),
                message: format!("queue_declare({}) failed: {e}", self.queue),
            })?;

        if let Some(exchange) = &self.exchange {
            let key = self.key.clone().unwrap_or_default();
            channel
                .queue_bind(
                    &self.queue,
                    exchange,
                    &key,
                    QueueBindOptions::default(),
                    FieldTable::default(),
                )
                .await
                .map_err(|e| FerroStashError::Input {
                    plugin: "rabbitmq".to_string(),
                    message: format!("queue_bind({}->{exchange}) failed: {e}", self.queue),
                })?;
        }

        let consumer_tag = format!("ferro-stash-{}", std::process::id());
        let mut consumer = channel
            .basic_consume(
                &self.queue,
                &consumer_tag,
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| FerroStashError::Input {
                plugin: "rabbitmq".to_string(),
                message: format!("basic_consume({}) failed: {e}", self.queue),
            })?;
        info!(queue = %self.queue, "rabbitmq input consuming");

        loop {
            tokio::select! {
                maybe = consumer.next() => {
                    match maybe {
                        Some(Ok(delivery)) => {
                            // Only acknowledge after a successful decode + send.
                            let decode_ok = match codec.decode(&delivery.data) {
                                Ok(events) => {
                                    for ev in events {
                                        if sender.send(ev).await.is_err() {
                                            info!("rabbitmq input: downstream closed, stopping");
                                            let _ = conn.close(200, "shutdown").await;
                                            return Ok(());
                                        }
                                    }
                                    true
                                }
                                Err(e) => {
                                    warn!(error = %e, "rabbitmq decode error; not acking (nack→DLX / leave unacked)");
                                    false
                                }
                            };
                            match delivery_action(self.ack, decode_ok) {
                                DeliveryAction::Ack => {
                                    if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
                                        warn!(error = %e, "rabbitmq ack failed (will redeliver)");
                                    }
                                }
                                DeliveryAction::NackToDlx => {
                                    let opts = BasicNackOptions {
                                        requeue: false,
                                        ..BasicNackOptions::default()
                                    };
                                    if let Err(e) = delivery.nack(opts).await {
                                        warn!(error = %e, "rabbitmq nack failed (will redeliver)");
                                    }
                                }
                                DeliveryAction::Leave => {}
                            }
                        }
                        Some(Err(e)) => {
                            warn!(error = %e, "rabbitmq consume error, backing off");
                            tokio::select! {
                                () = tokio::time::sleep(Duration::from_secs(5)) => {}
                                () = shutdown.wait() => break,
                            }
                        }
                        None => {
                            info!("rabbitmq consumer stream ended");
                            break;
                        }
                    }
                }
                () = shutdown.wait() => {
                    info!("rabbitmq input shutting down");
                    break;
                }
            }
        }
        let _ = conn.close(200, "shutdown").await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requires_queue() {
        assert!(RabbitmqInput::from_config(&serde_json::json!({})).is_err());
        assert!(RabbitmqInput::from_config(&serde_json::json!({ "queue": "q" })).is_ok());
    }

    #[test]
    fn defaults() {
        let i =
            RabbitmqInput::from_config(&serde_json::json!({ "queue": "logs" })).expect("config");
        assert_eq!(i.host, "localhost");
        assert_eq!(i.port, 5672);
        assert_eq!(i.vhost, "/");
        assert_eq!(i.user, "guest");
        assert_eq!(i.password, "guest");
        assert!(i.durable);
        assert!(i.ack);
        assert_eq!(i.codec, "json");
        assert_eq!(i.name(), "rabbitmq");
    }

    #[test]
    fn full_config_and_port_validation() {
        let i = RabbitmqInput::from_config(&serde_json::json!({
            "host": "rabbit.prod", "port": 5673, "vhost": "/app",
            "user": "u", "password": "p", "queue": "q",
            "exchange": "ex", "key": "rk", "durable": false, "ack": false,
            "codec": "plain"
        }))
        .expect("config");
        assert_eq!(i.host, "rabbit.prod");
        assert_eq!(i.port, 5673);
        assert_eq!(i.vhost, "/app");
        assert_eq!(i.exchange.as_deref(), Some("ex"));
        assert_eq!(i.key.as_deref(), Some("rk"));
        assert!(!i.durable);
        assert!(!i.ack);
        assert_eq!(i.codec, "plain");

        // Out-of-range port must fail loudly (not silently truncate).
        assert!(
            RabbitmqInput::from_config(&serde_json::json!({ "queue": "q", "port": 70000 }))
                .is_err()
        );
    }

    #[test]
    fn delivery_action_decision() {
        // Success + ack enabled → ack.
        assert_eq!(delivery_action(true, true), DeliveryAction::Ack);
        // Decode failure + ack enabled → nack to DLX (never silently ack/drop).
        assert_eq!(delivery_action(true, false), DeliveryAction::NackToDlx);
        // Ack disabled → leave the delivery alone regardless of decode outcome.
        assert_eq!(delivery_action(false, true), DeliveryAction::Leave);
        assert_eq!(delivery_action(false, false), DeliveryAction::Leave);
    }

    #[test]
    fn debug_redacts_password() {
        let i = RabbitmqInput::from_config(
            &serde_json::json!({ "queue": "q", "password": "super-secret-pw" }),
        )
        .expect("config");
        let dbg = format!("{i:?}");
        assert!(!dbg.contains("super-secret-pw"), "password leaked: {dbg}");
        assert!(dbg.contains("***"));
    }

    #[test]
    fn build_uri_carries_parts() {
        let i = RabbitmqInput::from_config(&serde_json::json!({
            "queue": "q", "host": "h", "port": 5673, "vhost": "/v",
            "user": "u", "password": "p"
        }))
        .expect("config");
        let uri = i.build_uri();
        assert_eq!(uri.authority.host, "h");
        assert_eq!(uri.authority.port, 5673);
        assert_eq!(uri.authority.userinfo.username, "u");
        assert_eq!(uri.vhost, "/v");
    }

    /// Live smoke (real RabbitMQ): set `RABBITMQ_URL` (e.g.
    /// `amqp://guest:guest@localhost:5672/%2f`) and publish a JSON message to the
    /// `RABBITMQ_QUEUE` (default `ferro-stash-live`) queue, then assert the input
    /// emits it. Run with a broker up:
    ///   RABBITMQ_URL=amqp://guest:guest@localhost:5672/ \
    ///     cargo test -p ferro-stash-input -- --ignored rabbitmq_live
    #[tokio::test]
    #[ignore = "live: set RABBITMQ_URL (running RabbitMQ broker)"]
    async fn rabbitmq_live_emits() {
        use ferro_stash_core::shutdown::ShutdownController;
        let Ok(url) = std::env::var("RABBITMQ_URL") else {
            eprintln!("SKIPPED: set RABBITMQ_URL");
            return;
        };
        let parsed = url::Url::parse(&url).expect("valid RABBITMQ_URL");
        let queue =
            std::env::var("RABBITMQ_QUEUE").unwrap_or_else(|_| "ferro-stash-live".to_string());
        let host = parsed.host_str().unwrap_or("localhost").to_string();
        let port = parsed.port().unwrap_or(5672);
        let vhost = {
            let p = parsed.path().trim_start_matches('/');
            if p.is_empty() {
                "/".to_string()
            } else {
                p.to_string()
            }
        };
        let cfg = serde_json::json!({
            "host": host, "port": port, "vhost": vhost,
            "user": if parsed.username().is_empty() { "guest" } else { parsed.username() },
            "password": parsed.password().unwrap_or("guest"),
            "queue": queue, "codec": "json",
        });

        // Publish one message so there is something to consume.
        let mut producer = RabbitmqInput::from_config(&cfg).expect("config");
        let conn = Connection::connect_uri(producer.build_uri(), ConnectionProperties::default())
            .await
            .expect("connect");
        let ch = conn.create_channel().await.expect("channel");
        ch.queue_declare(
            &producer.queue,
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("declare");
        ch.basic_publish(
            "",
            &producer.queue,
            lapin::options::BasicPublishOptions::default(),
            br#"{"message":"rabbitmq live smoke"}"#,
            lapin::BasicProperties::default(),
        )
        .await
        .expect("publish")
        .await
        .expect("confirm");

        let (tx, mut rx) = mpsc::channel(64);
        let (controller, signal) = ShutdownController::new();
        let handle = tokio::spawn(async move { producer.run(tx, signal).await });
        let ev = tokio::time::timeout(Duration::from_secs(15), rx.recv())
            .await
            .expect("timed out")
            .expect("channel closed");
        assert!(ev.get("message").is_some());
        controller.shutdown();
        let _ = handle.await;
        let _ = conn.close(200, "done").await;
    }
}
