// SPDX-License-Identifier: Apache-2.0
//! Generator input plugin — generates test events.

use async_trait::async_trait;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::InputPlugin;
use ferro_stash_core::shutdown::ShutdownSignal;
use tokio::sync::mpsc;
use tokio::time::{self, Duration};

#[allow(dead_code)]
#[derive(Debug)]
pub struct GeneratorInput {
    message: String,
    count: Option<u64>,
    threads: usize,
    tags: Vec<String>,
    interval_ms: u64,
}

impl GeneratorInput {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let message = settings
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("Hello World!")
            .to_string();
        let count = settings
            .get("count")
            .and_then(ferro_stash_core::settings_helpers::as_u64_flexible);
        let threads = settings
            .get("threads")
            .and_then(ferro_stash_core::settings_helpers::as_u64_flexible)
            .unwrap_or(1) as usize;
        let tags = settings
            .get("tags")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let interval_ms = settings
            .get("interval")
            .and_then(ferro_stash_core::settings_helpers::as_u64_flexible)
            .unwrap_or(0);

        Ok(Self {
            message,
            count,
            threads,
            tags,
            interval_ms,
        })
    }
}

#[async_trait]
impl InputPlugin for GeneratorInput {
    fn name(&self) -> &'static str {
        "generator"
    }

    async fn run(
        &mut self,
        sender: mpsc::Sender<Event>,
        mut shutdown: ShutdownSignal,
    ) -> Result<()> {
        let mut sequence = 0u64;

        let interval = if self.interval_ms > 0 {
            Some(Duration::from_millis(self.interval_ms))
        } else {
            None
        };

        loop {
            if let Some(max) = self.count {
                if sequence >= max {
                    break;
                }
            }

            let mut event = Event::new(&self.message);
            event.set("sequence", EventValue::Integer(sequence as i64));
            for tag in &self.tags {
                event.add_tag(tag);
            }

            if sender.send(event).await.is_err() {
                break;
            }

            sequence += 1;

            if let Some(wait) = interval {
                tokio::select! {
                    () = time::sleep(wait) => {}
                    () = shutdown.wait() => break,
                }
            } else if shutdown.is_shutdown() {
                break;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_generator_count() {
        let settings = serde_json::json!({
            "message": "test",
            "count": 5
        });
        let mut gen = GeneratorInput::from_config(&settings).expect("config");
        let (tx, mut rx) = mpsc::channel(100);
        let (_ctrl, sig) = ferro_stash_core::shutdown::ShutdownController::new();

        gen.run(tx, sig).await.expect("run");

        let mut count = 0;
        while rx.try_recv().is_ok() {
            count += 1;
        }
        assert_eq!(count, 5);
    }

    #[tokio::test]
    async fn test_generator_custom_message() {
        let settings = serde_json::json!({
            "message": "custom event",
            "count": 1
        });
        let mut gen = GeneratorInput::from_config(&settings).expect("config");
        let (tx, mut rx) = mpsc::channel(100);
        let (_ctrl, sig) = ferro_stash_core::shutdown::ShutdownController::new();
        gen.run(tx, sig).await.expect("run");
        let event = rx.try_recv().expect("event");
        assert_eq!(event.message(), Some("custom event"));
    }

    #[tokio::test]
    async fn test_generator_sequence() {
        let settings = serde_json::json!({ "message": "test", "count": 3 });
        let mut gen = GeneratorInput::from_config(&settings).expect("config");
        let (tx, mut rx) = mpsc::channel(100);
        let (_ctrl, sig) = ferro_stash_core::shutdown::ShutdownController::new();
        gen.run(tx, sig).await.expect("run");
        let e0 = rx.try_recv().expect("e0");
        let e1 = rx.try_recv().expect("e1");
        let e2 = rx.try_recv().expect("e2");
        assert_eq!(e0.get("sequence"), Some(&EventValue::Integer(0)));
        assert_eq!(e1.get("sequence"), Some(&EventValue::Integer(1)));
        assert_eq!(e2.get("sequence"), Some(&EventValue::Integer(2)));
    }

    #[tokio::test]
    async fn test_generator_with_tags() {
        let settings = serde_json::json!({
            "message": "test",
            "count": 1,
            "tags": ["generated"]
        });
        let mut gen = GeneratorInput::from_config(&settings).expect("config");
        let (tx, mut rx) = mpsc::channel(100);
        let (_ctrl, sig) = ferro_stash_core::shutdown::ShutdownController::new();
        gen.run(tx, sig).await.expect("run");
        let event = rx.try_recv().expect("event");
        assert!(event.has_tag("generated"));
    }

    #[test]
    fn test_generator_default_message() {
        let settings = serde_json::json!({});
        let gen = GeneratorInput::from_config(&settings).expect("config");
        assert_eq!(gen.message, "Hello World!");
    }

    #[test]
    fn test_generator_name() {
        let settings = serde_json::json!({});
        let gen = GeneratorInput::from_config(&settings).expect("config");
        assert_eq!(gen.name(), "generator");
    }

    #[tokio::test]
    async fn test_generator_shutdown() {
        let settings = serde_json::json!({ "message": "test" }); // no count = infinite
        let mut gen = GeneratorInput::from_config(&settings).expect("config");
        let (tx, _rx) = mpsc::channel(100);
        let (ctrl, sig) = ferro_stash_core::shutdown::ShutdownController::new();
        ctrl.shutdown();
        let result = gen.run(tx, sig).await;
        assert!(result.is_ok());
    }
}
