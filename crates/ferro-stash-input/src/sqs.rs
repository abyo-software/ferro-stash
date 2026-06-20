// SPDX-License-Identifier: Apache-2.0
//! Amazon SQS input — long-polls an SQS queue, decodes each message body with
//! the configured codec, emits the events, and (by default) deletes the message
//! to acknowledge it. Mirrors Logstash's `sqs` input for the common case.
//!
//! ```logstash
//! input {
//!   sqs {
//!     queue            => "my-queue"           # name (resolved via GetQueueUrl)
//!     # or: queue_url   => "https://sqs.us-east-1.amazonaws.com/123/my-queue"
//!     region           => "us-east-1"
//!     codec            => "json"
//!     wait_time_seconds => 10                   # long-poll
//!     delete_after_read => true                 # ack by deleting
//!   }
//! }
//! ```
//!
//! Credentials follow the s3 input: static `access_key_id`/`secret_access_key`
//! when both are set, otherwise the default AWS provider chain. `endpoint` lets
//! you point at LocalStack / an SQS-compatible service for testing.

use async_trait::async_trait;
use aws_sdk_sqs::config::{Credentials, Region};
use aws_sdk_sqs::Client;
use ferro_stash_codec::{create_codec, resolve_codec, Codec};
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::Event;
use ferro_stash_core::plugin::InputPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use ferro_stash_core::shutdown::ShutdownSignal;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// SQS input configuration.
///
/// `Debug` is implemented manually so the `secret_access_key` secret is never
/// rendered in logs/diagnostics (`{:?}` prints `Some("***")` / `None`).
#[derive(Clone)]
pub struct SqsInput {
    queue: Option<String>,
    queue_url: Option<String>,
    region: String,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    endpoint: Option<String>,
    codec: String,
    codec_settings: serde_json::Value,
    max_messages: i32,
    wait_time_seconds: i32,
    delete_after_read: bool,
}

impl std::fmt::Debug for SqsInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqsInput")
            .field("queue", &self.queue)
            .field("queue_url", &self.queue_url)
            .field("region", &self.region)
            .field("access_key_id", &self.access_key_id)
            .field(
                "secret_access_key",
                &self.secret_access_key.as_ref().map(|_| "***"),
            )
            .field("endpoint", &self.endpoint)
            .field("codec", &self.codec)
            .field("codec_settings", &self.codec_settings)
            .field("max_messages", &self.max_messages)
            .field("wait_time_seconds", &self.wait_time_seconds)
            .field("delete_after_read", &self.delete_after_read)
            .finish()
    }
}

/// Whether an SQS message may be deleted (acknowledged) after processing.
///
/// A message is deleted only when `delete_after_read` is enabled **and** its body
/// decoded successfully. A decode failure must NOT delete the message: leaving it
/// undeleted lets SQS redeliver it (and ultimately route it to the queue's
/// configured DLQ via the redrive policy) instead of silently dropping the data.
fn should_delete(delete_after_read: bool, decode_ok: bool) -> bool {
    delete_after_read && decode_ok
}

impl SqsInput {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let err = |m: &str| FerroStashError::Input {
            plugin: "sqs".to_string(),
            message: m.to_string(),
        };
        let queue = settings.get_string("queue");
        let queue_url = settings.get_string("queue_url");
        if queue.is_none() && queue_url.is_none() {
            return Err(err("sqs input requires `queue` (name) or `queue_url`"));
        }
        let (codec, codec_settings) = resolve_codec(settings, "plain");
        Ok(Self {
            queue,
            queue_url,
            region: settings
                .get_string("region")
                .unwrap_or_else(|| "us-east-1".to_string()),
            access_key_id: settings.get_string("access_key_id"),
            secret_access_key: settings.get_string("secret_access_key"),
            endpoint: settings.get_string("endpoint"),
            codec,
            codec_settings,
            // SQS allows 1..=10 messages per ReceiveMessage.
            max_messages: settings.get_u64("max_messages").unwrap_or(10).clamp(1, 10) as i32,
            // Long-poll wait, 0..=20s.
            wait_time_seconds: settings.get_u64("wait_time_seconds").unwrap_or(10).min(20) as i32,
            delete_after_read: settings.get_bool("delete_after_read").unwrap_or(true),
        })
    }

    fn build_codec(&self) -> Result<Box<dyn Codec>> {
        create_codec(&self.codec, &self.codec_settings).map_err(|e| FerroStashError::Input {
            plugin: "sqs".to_string(),
            message: format!("unknown/invalid codec '{}': {e}", self.codec),
        })
    }

    async fn build_client(&self) -> Client {
        let region = Region::new(self.region.clone());
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest()).region(region);
        if let (Some(ak), Some(sk)) = (&self.access_key_id, &self.secret_access_key) {
            loader = loader.credentials_provider(Credentials::new(
                ak,
                sk,
                None,
                None,
                "ferro-stash-sqs-input",
            ));
        }
        let sdk_config = loader.load().await;
        let mut cfg = aws_sdk_sqs::config::Builder::from(&sdk_config);
        if let Some(ep) = &self.endpoint {
            cfg = cfg.endpoint_url(ep);
        }
        Client::from_conf(cfg.build())
    }

    /// Resolve the queue URL: prefer the explicit `queue_url`, else `GetQueueUrl`
    /// from the queue name.
    async fn resolve_queue_url(&self, client: &Client) -> Result<String> {
        if let Some(url) = &self.queue_url {
            return Ok(url.clone());
        }
        let name = self.queue.as_ref().expect("queue or queue_url validated");
        let out = client
            .get_queue_url()
            .queue_name(name)
            .send()
            .await
            .map_err(|e| FerroStashError::Input {
                plugin: "sqs".to_string(),
                message: format!("GetQueueUrl({name}) failed: {e}"),
            })?;
        out.queue_url()
            .map(String::from)
            .ok_or_else(|| FerroStashError::Input {
                plugin: "sqs".to_string(),
                message: format!("GetQueueUrl({name}) returned no URL"),
            })
    }
}

#[async_trait]
impl InputPlugin for SqsInput {
    fn name(&self) -> &str {
        "sqs"
    }

    async fn run(
        &mut self,
        sender: mpsc::Sender<Event>,
        mut shutdown: ShutdownSignal,
    ) -> Result<()> {
        let codec = self.build_codec()?;
        let client = self.build_client().await;
        let queue_url = self.resolve_queue_url(&client).await?;
        info!(queue_url = %queue_url, "sqs input starting");

        loop {
            let recv = client
                .receive_message()
                .queue_url(&queue_url)
                .max_number_of_messages(self.max_messages)
                .wait_time_seconds(self.wait_time_seconds);

            tokio::select! {
                result = recv.send() => {
                    match result {
                        Ok(out) => {
                            for msg in out.messages.unwrap_or_default() {
                                // Decode (and deliver) before deciding whether to
                                // delete. A decode failure must leave the message
                                // on the queue so SQS can redeliver / DLQ it.
                                let decode_ok = match msg.body() {
                                    Some(body) => match codec.decode(body.as_bytes()) {
                                        Ok(events) => {
                                            for ev in events {
                                                if sender.send(ev).await.is_err() {
                                                    info!("sqs input: downstream closed, stopping");
                                                    return Ok(());
                                                }
                                            }
                                            true
                                        }
                                        Err(e) => {
                                            warn!(error = %e, "sqs decode error; leaving message on queue (redeliver / DLQ)");
                                            false
                                        }
                                    },
                                    // No body to decode: nothing to lose, treat as
                                    // ackable so it does not loop forever.
                                    None => true,
                                };
                                if should_delete(self.delete_after_read, decode_ok) {
                                    if let Some(rh) = msg.receipt_handle() {
                                        if let Err(e) = client.delete_message()
                                            .queue_url(&queue_url).receipt_handle(rh).send().await {
                                            warn!(error = %e, "sqs delete_message failed (will redeliver)");
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "sqs receive_message failed, backing off");
                            tokio::select! {
                                () = tokio::time::sleep(Duration::from_secs(5)) => {}
                                () = shutdown.wait() => break,
                            }
                        }
                    }
                }
                () = shutdown.wait() => {
                    info!("sqs input shutting down");
                    break;
                }
            }
            debug!("sqs poll cycle complete");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requires_queue_or_url() {
        assert!(SqsInput::from_config(&serde_json::json!({})).is_err());
        assert!(SqsInput::from_config(&serde_json::json!({ "queue": "q" })).is_ok());
        assert!(SqsInput::from_config(&serde_json::json!({ "queue_url": "http://x/q" })).is_ok());
    }

    #[test]
    fn decode_failure_does_not_delete() {
        // A decode failure must NOT delete the message (let SQS redeliver / DLQ),
        // even when delete_after_read is enabled — otherwise data is silently lost.
        assert!(!should_delete(true, false));
        // A successful decode with delete_after_read deletes (acks) the message.
        assert!(should_delete(true, true));
        // delete_after_read disabled never deletes, regardless of decode outcome.
        assert!(!should_delete(false, true));
        assert!(!should_delete(false, false));
    }

    #[test]
    fn debug_redacts_secret() {
        let i = SqsInput::from_config(&serde_json::json!({
            "queue": "q", "access_key_id": "AKIAEXAMPLE", "secret_access_key": "super-secret-sak"
        }))
        .expect("config");
        let dbg = format!("{i:?}");
        assert!(!dbg.contains("super-secret-sak"), "secret leaked: {dbg}");
        assert!(dbg.contains("***"));
        // Non-secret fields stay visible.
        assert!(dbg.contains("AKIAEXAMPLE"));
    }

    #[test]
    fn clamps_and_defaults() {
        let i = SqsInput::from_config(&serde_json::json!({
            "queue": "q", "max_messages": 99, "wait_time_seconds": 99
        }))
        .expect("config");
        assert_eq!(i.max_messages, 10); // clamped to SQS max
        assert_eq!(i.wait_time_seconds, 20); // clamped
        assert!(i.delete_after_read); // default true
        assert_eq!(i.region, "us-east-1");
    }

    /// Live smoke (LocalStack): create a queue, send a JSON message, assert the
    /// input emits it. Run with LocalStack up:
    ///   SQS_TEST_QUEUE_URL=http://localhost:4566/000000000000/t \
    ///   AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
    ///     cargo test -p ferro-stash-input -- --ignored sqs_live
    #[tokio::test]
    #[ignore = "live: set SQS_TEST_QUEUE_URL (+ AWS creds / endpoint)"]
    async fn sqs_live_emits() {
        use ferro_stash_core::shutdown::ShutdownController;
        let Ok(url) = std::env::var("SQS_TEST_QUEUE_URL") else {
            eprintln!("SKIPPED: set SQS_TEST_QUEUE_URL");
            return;
        };
        let endpoint = std::env::var("SQS_TEST_ENDPOINT").ok();
        let mut cfg =
            serde_json::json!({ "queue_url": url, "codec": "json", "wait_time_seconds": 2 });
        if let Some(ep) = endpoint {
            cfg["endpoint"] = serde_json::Value::String(ep);
        }
        let mut input = SqsInput::from_config(&cfg).expect("config");
        let (tx, mut rx) = mpsc::channel(64);
        let (controller, signal) = ShutdownController::new();
        let handle = tokio::spawn(async move { input.run(tx, signal).await });
        let ev = tokio::time::timeout(Duration::from_secs(15), rx.recv())
            .await
            .expect("timed out")
            .expect("channel closed");
        assert!(ev.get("message").is_some() || !ev.fields().is_empty());
        controller.shutdown();
        let _ = handle.await;
    }
}
