// SPDX-License-Identifier: Apache-2.0
//! Amazon SNS output — encodes each event with the configured codec and
//! publishes it to an SNS topic via `Publish`. Mirrors Logstash's `sns` output
//! for the common case.
//!
//! ```logstash
//! output {
//!   sns {
//!     topic_arn => "arn:aws:sns:us-east-1:123456789012:my-topic"
//!     region    => "us-east-1"
//!     codec     => "json"
//!   }
//! }
//! ```
//!
//! Credentials/endpoint follow the s3 output (static creds when both set, else
//! the default AWS provider chain; `endpoint` for LocalStack). One `Publish`
//! per event.

use async_trait::async_trait;
use ferro_stash_codec::{create_codec, resolve_codec, Codec};
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::Event;
use ferro_stash_core::plugin::OutputPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use tokio::sync::OnceCell;

#[derive(Debug)]
pub struct SnsOutput {
    topic_arn: String,
    region: String,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    endpoint: Option<String>,
    /// Optional `Subject` for the SNS message (email-style notifications).
    subject: Option<String>,
    codec: Box<dyn Codec>,
    condition: Option<Condition>,
    client: OnceCell<aws_sdk_sns::Client>,
}

impl SnsOutput {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let topic_arn =
            settings
                .get_string("topic_arn")
                .ok_or_else(|| FerroStashError::Output {
                    plugin: "sns".to_string(),
                    message: "sns output requires `topic_arn`".to_string(),
                })?;
        let (codec_name, codec_settings) = resolve_codec(settings, "json");
        let codec =
            create_codec(&codec_name, &codec_settings).map_err(|e| FerroStashError::Output {
                plugin: "sns".to_string(),
                message: format!("unknown/invalid codec '{codec_name}': {e}"),
            })?;
        Ok(Self {
            topic_arn,
            region: settings
                .get_string("region")
                .unwrap_or_else(|| "us-east-1".to_string()),
            access_key_id: settings.get_string("access_key_id"),
            secret_access_key: settings.get_string("secret_access_key"),
            endpoint: settings.get_string("endpoint"),
            subject: settings.get_string("subject"),
            codec,
            condition,
            client: OnceCell::new(),
        })
    }

    async fn client(&self) -> &aws_sdk_sns::Client {
        self.client
            .get_or_init(|| async {
                let region = aws_sdk_sns::config::Region::new(self.region.clone());
                let mut loader =
                    aws_config::defaults(aws_config::BehaviorVersion::latest()).region(region);
                if let (Some(ak), Some(sk)) = (&self.access_key_id, &self.secret_access_key) {
                    loader = loader.credentials_provider(aws_sdk_sns::config::Credentials::new(
                        ak,
                        sk,
                        None,
                        None,
                        "ferro-stash-sns-output",
                    ));
                }
                let sdk_config = loader.load().await;
                let mut cfg = aws_sdk_sns::config::Builder::from(&sdk_config);
                if let Some(ep) = &self.endpoint {
                    cfg = cfg.endpoint_url(ep);
                }
                aws_sdk_sns::Client::from_conf(cfg.build())
            })
            .await
    }
}

#[async_trait]
impl OutputPlugin for SnsOutput {
    fn name(&self) -> &str {
        "sns"
    }

    async fn output(&self, events: Vec<Event>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let client = self.client().await;
        for event in events {
            let bytes = self
                .codec
                .encode(&event)
                .map_err(|e| FerroStashError::Output {
                    plugin: "sns".to_string(),
                    message: format!("codec encode error: {e}"),
                })?;
            let message = String::from_utf8_lossy(&bytes).to_string();
            let mut req = client.publish().topic_arn(&self.topic_arn).message(message);
            if let Some(subject) = &self.subject {
                req = req.subject(subject);
            }
            req.send().await.map_err(|e| FerroStashError::Output {
                plugin: "sns".to_string(),
                message: format!("Publish failed: {e}"),
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
    fn requires_topic_arn() {
        assert!(SnsOutput::from_config(&serde_json::json!({}), None).is_err());
        assert!(SnsOutput::from_config(
            &serde_json::json!({ "topic_arn": "arn:aws:sns:us-east-1:1:t" }),
            None
        )
        .is_ok());
    }

    #[test]
    fn name_and_region_default() {
        let o = SnsOutput::from_config(
            &serde_json::json!({ "topic_arn": "arn:aws:sns:us-east-1:1:t" }),
            None,
        )
        .expect("config");
        assert_eq!(o.name(), "sns");
        assert_eq!(o.region, "us-east-1");
    }
}
