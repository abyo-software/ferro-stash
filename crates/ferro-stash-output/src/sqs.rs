// SPDX-License-Identifier: Apache-2.0
//! Amazon SQS output — encodes each event with the configured codec and sends
//! it to an SQS queue via `SendMessage`. Mirrors Logstash's `sqs` output.
//!
//! ```logstash
//! output {
//!   sqs {
//!     queue      => "my-queue"          # name (resolved via GetQueueUrl)
//!     # or: queue_url => "https://sqs.us-east-1.amazonaws.com/123/my-queue"
//!     region     => "us-east-1"
//!     codec      => "json"
//!   }
//! }
//! ```
//!
//! Credentials/endpoint follow the s3 output (static creds when both set, else
//! the default AWS provider chain; `endpoint` for LocalStack). One `SendMessage`
//! per event (no batching yet — documented residual).

use async_trait::async_trait;
use ferro_stash_codec::{create_codec, resolve_codec, Codec};
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::Event;
use ferro_stash_core::plugin::OutputPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use tokio::sync::OnceCell;

/// SQS output configuration.
///
/// `Debug` is implemented manually so the `secret_access_key` secret is never
/// rendered in logs/diagnostics (`{:?}` prints `Some("***")` / `None`).
pub struct SqsOutput {
    queue: Option<String>,
    queue_url: Option<String>,
    region: String,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    endpoint: Option<String>,
    codec: Box<dyn Codec>,
    condition: Option<Condition>,
    client: OnceCell<aws_sdk_sqs::Client>,
    resolved_url: OnceCell<String>,
}

impl std::fmt::Debug for SqsOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqsOutput")
            .field("queue", &self.queue)
            .field("queue_url", &self.queue_url)
            .field("region", &self.region)
            .field("access_key_id", &self.access_key_id)
            .field(
                "secret_access_key",
                &self.secret_access_key.as_ref().map(|_| "***"),
            )
            .field("endpoint", &self.endpoint)
            .field("condition", &self.condition)
            .finish_non_exhaustive()
    }
}

impl SqsOutput {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let err = |m: &str| FerroStashError::Output {
            plugin: "sqs".to_string(),
            message: m.to_string(),
        };
        let queue = settings.get_string("queue");
        let queue_url = settings.get_string("queue_url");
        if queue.is_none() && queue_url.is_none() {
            return Err(err("sqs output requires `queue` (name) or `queue_url`"));
        }
        let (codec_name, codec_settings) = resolve_codec(settings, "json");
        let codec =
            create_codec(&codec_name, &codec_settings).map_err(|e| FerroStashError::Output {
                plugin: "sqs".to_string(),
                message: format!("unknown/invalid codec '{codec_name}': {e}"),
            })?;
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
            condition,
            client: OnceCell::new(),
            resolved_url: OnceCell::new(),
        })
    }

    async fn client(&self) -> &aws_sdk_sqs::Client {
        self.client
            .get_or_init(|| async {
                let region = aws_sdk_sqs::config::Region::new(self.region.clone());
                let mut loader =
                    aws_config::defaults(aws_config::BehaviorVersion::latest()).region(region);
                if let (Some(ak), Some(sk)) = (&self.access_key_id, &self.secret_access_key) {
                    loader = loader.credentials_provider(aws_sdk_sqs::config::Credentials::new(
                        ak,
                        sk,
                        None,
                        None,
                        "ferro-stash-sqs-output",
                    ));
                }
                let sdk_config = loader.load().await;
                let mut cfg = aws_sdk_sqs::config::Builder::from(&sdk_config);
                if let Some(ep) = &self.endpoint {
                    cfg = cfg.endpoint_url(ep);
                }
                aws_sdk_sqs::Client::from_conf(cfg.build())
            })
            .await
    }

    async fn queue_url(&self) -> Result<&String> {
        self.resolved_url
            .get_or_try_init(|| async {
                if let Some(url) = &self.queue_url {
                    return Ok(url.clone());
                }
                let name = self.queue.as_ref().expect("queue or queue_url validated");
                let out = self
                    .client()
                    .await
                    .get_queue_url()
                    .queue_name(name)
                    .send()
                    .await
                    .map_err(|e| FerroStashError::Output {
                        plugin: "sqs".to_string(),
                        message: format!("GetQueueUrl({name}) failed: {e}"),
                    })?;
                out.queue_url()
                    .map(String::from)
                    .ok_or_else(|| FerroStashError::Output {
                        plugin: "sqs".to_string(),
                        message: format!("GetQueueUrl({name}) returned no URL"),
                    })
            })
            .await
    }
}

#[async_trait]
impl OutputPlugin for SqsOutput {
    fn name(&self) -> &str {
        "sqs"
    }

    async fn output(&self, events: Vec<Event>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let url = self.queue_url().await?.clone();
        let client = self.client().await;
        for event in events {
            let bytes = self
                .codec
                .encode(&event)
                .map_err(|e| FerroStashError::Output {
                    plugin: "sqs".to_string(),
                    message: format!("codec encode error: {e}"),
                })?;
            let body = String::from_utf8_lossy(&bytes).to_string();
            client
                .send_message()
                .queue_url(&url)
                .message_body(body)
                .send()
                .await
                .map_err(|e| FerroStashError::Output {
                    plugin: "sqs".to_string(),
                    message: format!("SendMessage failed: {e}"),
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
    fn requires_queue_or_url() {
        assert!(SqsOutput::from_config(&serde_json::json!({}), None).is_err());
        assert!(SqsOutput::from_config(&serde_json::json!({ "queue": "q" }), None).is_ok());
        assert!(
            SqsOutput::from_config(&serde_json::json!({ "queue_url": "http://x/q" }), None).is_ok()
        );
    }

    #[test]
    fn name_and_region_default() {
        let o = SqsOutput::from_config(&serde_json::json!({ "queue": "q" }), None).expect("config");
        assert_eq!(o.name(), "sqs");
        assert_eq!(o.region, "us-east-1");
    }

    #[test]
    fn debug_redacts_secret() {
        let o = SqsOutput::from_config(
            &serde_json::json!({
                "queue": "q", "access_key_id": "AKIAEXAMPLE",
                "secret_access_key": "super-secret-sak"
            }),
            None,
        )
        .expect("config");
        let dbg = format!("{o:?}");
        assert!(!dbg.contains("super-secret-sak"), "secret leaked: {dbg}");
        assert!(dbg.contains("***"));
        assert!(dbg.contains("AKIAEXAMPLE"));
    }
}
