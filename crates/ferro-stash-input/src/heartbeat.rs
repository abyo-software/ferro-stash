// SPDX-License-Identifier: Apache-2.0
//! Heartbeat input plugin — generates periodic heartbeat events.

use async_trait::async_trait;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::InputPlugin;
use ferro_stash_core::shutdown::ShutdownSignal;
use tokio::sync::mpsc;
use tokio::time::{self, Duration};

#[derive(Debug)]
pub struct HeartbeatInput {
    message: String,
    interval_secs: u64,
    count: Option<u64>,
    tags: Vec<String>,
}

impl HeartbeatInput {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let message = settings
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("ok")
            .to_string();
        // Clamp the interval to a minimum of 1s. A DSL-accepted `interval => 0`
        // would feed `Duration::from_secs(0)` to `tokio::time::interval`, which
        // PANICS ("`period` must be non-zero") and aborts the heartbeat task.
        // Sibling plugins already guard this (s3 floors at MIN_POLL_INTERVAL_SECS,
        // generator gates `interval_ms > 0`, datadog `.max(1)`); heartbeat was
        // missed. (DD round-20 Finding #1.)
        let interval_secs = settings
            .get("interval")
            .and_then(ferro_stash_core::settings_helpers::as_u64_flexible)
            .unwrap_or(60)
            .max(1);
        let count = settings
            .get("count")
            .and_then(ferro_stash_core::settings_helpers::as_u64_flexible);
        let tags = settings
            .get("tags")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        Ok(Self {
            message,
            interval_secs,
            count,
            tags,
        })
    }
}

#[async_trait]
impl InputPlugin for HeartbeatInput {
    fn name(&self) -> &'static str {
        "heartbeat"
    }

    async fn run(
        &mut self,
        sender: mpsc::Sender<Event>,
        mut shutdown: ShutdownSignal,
    ) -> Result<()> {
        let mut interval = time::interval(Duration::from_secs(self.interval_secs));
        let mut sequence = 0u64;

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Some(max) = self.count {
                        if sequence >= max {
                            break;
                        }
                    }

                    let mut event = Event::new(&self.message);
                    event.set("sequence", EventValue::Integer(sequence as i64));
                    event.set("type", EventValue::String("heartbeat".to_string()));
                    event.set(
                        "host",
                        EventValue::String(
                            hostname::get().map_or_else(|_| "unknown".to_string(), |h| h.to_string_lossy().to_string()),
                        ),
                    );
                    for tag in &self.tags {
                        event.add_tag(tag);
                    }

                    if sender.send(event).await.is_err() {
                        break;
                    }

                    sequence += 1;
                }
                () = shutdown.wait() => break,
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_heartbeat_config_defaults() {
        let settings = serde_json::json!({});
        let input = HeartbeatInput::from_config(&settings).expect("config");
        assert_eq!(input.message, "ok");
        assert_eq!(input.interval_secs, 60);
        assert!(input.count.is_none());
    }

    #[test]
    fn test_heartbeat_config_custom() {
        let settings = serde_json::json!({
            "message": "alive",
            "interval": 10,
            "count": 5,
            "tags": ["heartbeat"]
        });
        let input = HeartbeatInput::from_config(&settings).expect("config");
        assert_eq!(input.message, "alive");
        assert_eq!(input.interval_secs, 10);
        assert_eq!(input.count, Some(5));
        assert_eq!(input.tags, vec!["heartbeat"]);
    }

    #[test]
    fn test_heartbeat_name() {
        let settings = serde_json::json!({});
        let input = HeartbeatInput::from_config(&settings).expect("config");
        assert_eq!(input.name(), "heartbeat");
    }

    #[test]
    fn test_heartbeat_zero_interval_clamped() {
        // A DSL-accepted `interval => 0` must be clamped to >=1 so the timer
        // is never built with a zero period.
        let settings = serde_json::json!({ "interval": 0 });
        let input = HeartbeatInput::from_config(&settings).expect("config");
        assert_eq!(input.interval_secs, 1);
    }

    #[tokio::test]
    async fn test_heartbeat_zero_interval_does_not_panic() {
        // Regression: `interval => 0` previously panicked `tokio::time::interval`
        // ("`period` must be non-zero") and aborted the task. With the clamp the
        // timer is built with >=1s and `run()` produces events normally.
        let settings = serde_json::json!({
            "message": "beat",
            "interval": 0,
            "count": 1
        });
        let mut input = HeartbeatInput::from_config(&settings).expect("config");
        assert_eq!(input.interval_secs, 1);
        let (tx, mut rx) = mpsc::channel(10);
        let (_ctrl, sig) = ferro_stash_core::shutdown::ShutdownController::new();

        // Bounded so the test cannot hang even if the clamp regresses.
        let handle = tokio::spawn(async move { input.run(tx, sig).await });
        let res = tokio::time::timeout(Duration::from_secs(5), handle).await;
        let join = res.expect("run() should complete (no panic / no zero-period stall)");
        // The task itself must not have panicked.
        assert!(join.is_ok(), "heartbeat task panicked: {join:?}");

        let mut count = 0;
        while rx.try_recv().is_ok() {
            count += 1;
        }
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_heartbeat_with_count() {
        let settings = serde_json::json!({
            "message": "beat",
            "interval": 1,
            "count": 2
        });
        let mut input = HeartbeatInput::from_config(&settings).expect("config");
        let (tx, mut rx) = mpsc::channel(100);
        let (_ctrl, sig) = ferro_stash_core::shutdown::ShutdownController::new();

        let handle = tokio::spawn(async move { input.run(tx, sig).await });

        let _ = handle.await;

        let mut count = 0;
        while rx.try_recv().is_ok() {
            count += 1;
        }
        assert_eq!(count, 2);
    }
}
