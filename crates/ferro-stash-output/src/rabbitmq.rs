// SPDX-License-Identifier: Apache-2.0
//! RabbitMQ output — encodes each event with the configured codec and publishes
//! it to an AMQP exchange via `basic_publish`. Mirrors Logstash's `rabbitmq`
//! output for the common case. Backed by the `lapin` AMQP client (rustls stance).
//!
//! ```logstash
//! output {
//!   rabbitmq {
//!     host       => "localhost"
//!     port       => 5672
//!     vhost      => "/"
//!     user       => "guest"
//!     password   => "guest"
//!     exchange   => "amq.topic"
//!     key        => "app.%{level}"   # routing key, `%{field}`-aware
//!     codec      => "json"
//!     persistent => true
//!   }
//! }
//! ```
//!
//! The connection/channel are established lazily on first publish (via a
//! `OnceCell`) and reused for the lifetime of the output.

use async_trait::async_trait;
use ferro_stash_codec::{create_codec_from_settings, resolve_codec, Codec};
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::Event;
use ferro_stash_core::plugin::OutputPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use lapin::options::BasicPublishOptions;
use lapin::uri::{AMQPAuthority, AMQPScheme, AMQPUri, AMQPUserInfo};
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties};
use tokio::sync::OnceCell;

/// RabbitMQ output configuration — mirrors the Logstash rabbitmq output settings.
///
/// `Debug` is implemented manually so the `password` secret is never rendered in
/// logs/diagnostics (`{:?}` prints `"***"`, not the plaintext).
#[derive(Clone)]
pub struct RabbitmqOutputConfig {
    pub host: String,
    pub port: u16,
    pub vhost: String,
    pub user: String,
    pub password: String,
    pub exchange: String,
    pub key: String,
    pub codec: String,
    pub persistent: bool,
}

impl std::fmt::Debug for RabbitmqOutputConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RabbitmqOutputConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("vhost", &self.vhost)
            .field("user", &self.user)
            .field("password", &"***")
            .field("exchange", &self.exchange)
            .field("key", &self.key)
            .field("codec", &self.codec)
            .field("persistent", &self.persistent)
            .finish()
    }
}

pub struct RabbitmqOutput {
    config: RabbitmqOutputConfig,
    condition: Option<Condition>,
    codec: Box<dyn Codec>,
    /// Lazily-established connection (kept alive so the channel stays open).
    connection: OnceCell<Connection>,
    channel: OnceCell<Channel>,
}

impl std::fmt::Debug for RabbitmqOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RabbitmqOutput")
            .field("config", &self.config)
            .field("condition", &self.condition)
            .field("codec", &self.codec)
            .field("connected", &self.connection.get().is_some())
            .finish()
    }
}

impl RabbitmqOutput {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let port = settings
            .get_port("port", 5672)
            .map_err(|message| FerroStashError::Output {
                plugin: "rabbitmq".to_string(),
                message,
            })?;
        let (codec, _) = resolve_codec(settings, "json");
        // Build the codec (config error => fail loudly), honoring both DSL forms.
        let codec_impl = create_codec_from_settings(settings, "json")?;
        Ok(Self {
            config: RabbitmqOutputConfig {
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
                exchange: settings.get_string("exchange").unwrap_or_default(),
                key: settings.get_string("key").unwrap_or_default(),
                codec,
                persistent: settings.get_bool("persistent").unwrap_or(true),
            },
            condition,
            codec: codec_impl,
            connection: OnceCell::new(),
            channel: OnceCell::new(),
        })
    }

    fn build_uri(&self) -> AMQPUri {
        AMQPUri {
            scheme: AMQPScheme::AMQP,
            authority: AMQPAuthority {
                userinfo: AMQPUserInfo {
                    username: self.config.user.clone(),
                    password: self.config.password.clone(),
                },
                host: self.config.host.clone(),
                port: self.config.port,
            },
            vhost: self.config.vhost.clone(),
            ..AMQPUri::default()
        }
    }

    /// Lazily establish the connection + channel and return the channel.
    async fn channel(&self) -> Result<&Channel> {
        let conn = self
            .connection
            .get_or_try_init(|| async {
                Connection::connect_uri(self.build_uri(), ConnectionProperties::default())
                    .await
                    .map_err(|e| FerroStashError::Output {
                        plugin: "rabbitmq".to_string(),
                        message: format!("connect failed: {e}"),
                    })
            })
            .await?;
        self.channel
            .get_or_try_init(|| async {
                conn.create_channel()
                    .await
                    .map_err(|e| FerroStashError::Output {
                        plugin: "rabbitmq".to_string(),
                        message: format!("create_channel failed: {e}"),
                    })
            })
            .await
    }
}

#[async_trait]
impl OutputPlugin for RabbitmqOutput {
    fn name(&self) -> &str {
        "rabbitmq"
    }

    async fn output(&self, events: Vec<Event>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        // Encode up-front so a codec error fails before touching the broker.
        let mut messages: Vec<(String, Vec<u8>)> = Vec::with_capacity(events.len());
        for event in &events {
            let bytes = self
                .codec
                .encode(event)
                .map_err(|e| FerroStashError::Output {
                    plugin: "rabbitmq".to_string(),
                    message: format!("codec encode error: {e}"),
                })?;
            // The routing key is `%{field}`-aware (per-event).
            let key = event.sprintf(&self.config.key);
            messages.push((key, bytes));
        }

        let channel = self.channel().await?;
        // delivery_mode 2 = persistent, 1 = transient.
        let props = BasicProperties::default().with_delivery_mode(if self.config.persistent {
            2
        } else {
            1
        });

        for (key, bytes) in &messages {
            channel
                .basic_publish(
                    &self.config.exchange,
                    key,
                    BasicPublishOptions::default(),
                    bytes,
                    props.clone(),
                )
                .await
                .map_err(|e| FerroStashError::Output {
                    plugin: "rabbitmq".to_string(),
                    message: format!("basic_publish failed: {e}"),
                })?
                .await
                .map_err(|e| FerroStashError::Output {
                    plugin: "rabbitmq".to_string(),
                    message: format!("publish confirm failed: {e}"),
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

    #[test]
    fn defaults() {
        let o = RabbitmqOutput::from_config(&serde_json::json!({}), None).expect("config");
        assert_eq!(o.config.host, "localhost");
        assert_eq!(o.config.port, 5672);
        assert_eq!(o.config.vhost, "/");
        assert_eq!(o.config.user, "guest");
        assert_eq!(o.config.exchange, "");
        assert_eq!(o.config.key, "");
        assert!(o.config.persistent);
        assert_eq!(o.config.codec, "json");
        assert_eq!(o.name(), "rabbitmq");
    }

    #[test]
    fn full_config_and_port_validation() {
        let o = RabbitmqOutput::from_config(
            &serde_json::json!({
                "host": "rabbit.prod", "port": 5673, "vhost": "/app",
                "user": "u", "password": "p",
                "exchange": "ex", "key": "rk.%{level}",
                "codec": "plain", "persistent": false
            }),
            None,
        )
        .expect("config");
        assert_eq!(o.config.exchange, "ex");
        assert_eq!(o.config.key, "rk.%{level}");
        assert!(!o.config.persistent);
        assert_eq!(o.config.codec, "plain");

        assert!(RabbitmqOutput::from_config(&serde_json::json!({ "port": 70000 }), None).is_err());
    }

    #[test]
    fn unknown_codec_rejected() {
        assert!(RabbitmqOutput::from_config(
            &serde_json::json!({ "codec": "no-such-codec" }),
            None
        )
        .is_err());
    }

    #[test]
    fn routing_key_sprintf() {
        // The routing key honors `%{field}` interpolation per event.
        let o = RabbitmqOutput::from_config(&serde_json::json!({ "key": "app.%{level}" }), None)
            .expect("config");
        let mut ev = Event::new("x");
        ev.set(
            "level",
            ferro_stash_core::event::EventValue::String("error".into()),
        );
        assert_eq!(ev.sprintf(&o.config.key), "app.error");
    }

    #[test]
    fn debug_redacts_password() {
        let o = RabbitmqOutput::from_config(
            &serde_json::json!({ "password": "super-secret-pw", "host": "rabbit.prod" }),
            None,
        )
        .expect("config");
        let cfg_dbg = format!("{:?}", o.config);
        assert!(!cfg_dbg.contains("super-secret-pw"), "leaked: {cfg_dbg}");
        assert!(cfg_dbg.contains("***"));
        assert!(cfg_dbg.contains("rabbit.prod"));
        let out_dbg = format!("{o:?}");
        assert!(
            !out_dbg.contains("super-secret-pw"),
            "wrapper leaked: {out_dbg}"
        );
    }

    #[tokio::test]
    async fn empty_is_ok() {
        let o = RabbitmqOutput::from_config(&serde_json::json!({}), None).expect("config");
        assert!(o.output(vec![]).await.is_ok());
    }

    /// Live smoke (real RabbitMQ): set `RABBITMQ_URL`; publishes one JSON event to
    /// the default exchange with routing key = `RABBITMQ_QUEUE` (default
    /// `ferro-stash-live`). Run with a broker up:
    ///   RABBITMQ_URL=amqp://guest:guest@localhost:5672/ \
    ///     cargo test -p ferro-stash-output -- --ignored rabbitmq_live
    #[tokio::test]
    #[ignore = "live: set RABBITMQ_URL (running RabbitMQ broker)"]
    async fn rabbitmq_live_publishes() {
        let Ok(url) = std::env::var("RABBITMQ_URL") else {
            eprintln!("SKIPPED: set RABBITMQ_URL");
            return;
        };
        let parsed = url::Url::parse(&url).expect("valid RABBITMQ_URL");
        let queue =
            std::env::var("RABBITMQ_QUEUE").unwrap_or_else(|_| "ferro-stash-live".to_string());
        let vhost = {
            let p = parsed.path().trim_start_matches('/');
            if p.is_empty() {
                "/".to_string()
            } else {
                p.to_string()
            }
        };
        let cfg = serde_json::json!({
            "host": parsed.host_str().unwrap_or("localhost"),
            "port": parsed.port().unwrap_or(5672),
            "vhost": vhost,
            "user": if parsed.username().is_empty() { "guest" } else { parsed.username() },
            "password": parsed.password().unwrap_or("guest"),
            "exchange": "",
            "key": queue,
            "codec": "json",
        });
        let output = RabbitmqOutput::from_config(&cfg, None).expect("config");
        output
            .output(vec![Event::new("rabbitmq live smoke")])
            .await
            .expect("live publish should succeed");
    }
}
