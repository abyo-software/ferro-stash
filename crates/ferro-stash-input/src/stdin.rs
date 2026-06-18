// SPDX-License-Identifier: Apache-2.0
//! Stdin input plugin — reads lines from standard input.

use async_trait::async_trait;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::InputPlugin;
use ferro_stash_core::shutdown::ShutdownSignal;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct StdinInput {
    add_field: Vec<(String, String)>,
    tags: Vec<String>,
}

impl StdinInput {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let add_field = settings
            .get("add_field")
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let tags = settings
            .get("tags")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        Ok(Self { add_field, tags })
    }
}

#[async_trait]
impl InputPlugin for StdinInput {
    fn name(&self) -> &'static str {
        "stdin"
    }

    async fn run(
        &mut self,
        sender: mpsc::Sender<Event>,
        mut shutdown: ShutdownSignal,
    ) -> Result<()> {
        let stdin = tokio::io::stdin();
        let mut reader = BufReader::new(stdin);
        let mut line = String::new();

        loop {
            line.clear();
            tokio::select! {
                result = reader.read_line(&mut line) => {
                    match result {
                        Ok(0) => break, // EOF
                        Ok(_) => {
                            let trimmed = line.trim_end_matches('\n').trim_end_matches('\r');
                            if trimmed.is_empty() {
                                continue;
                            }
                            let mut event = Event::new(trimmed);
                            for (k, v) in &self.add_field {
                                event.set(k.clone(), EventValue::String(v.clone()));
                            }
                            for tag in &self.tags {
                                event.add_tag(tag);
                            }
                            if sender.send(event).await.is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "stdin read error");
                            break;
                        }
                    }
                }
                () = shutdown.wait() => {
                    break;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stdin_config_defaults() {
        let settings = serde_json::json!({});
        let input = StdinInput::from_config(&settings).expect("config");
        assert!(input.add_field.is_empty());
        assert!(input.tags.is_empty());
    }

    #[test]
    fn test_stdin_config_with_fields() {
        let settings = serde_json::json!({
            "add_field": { "source": "stdin" },
            "tags": ["interactive"]
        });
        let input = StdinInput::from_config(&settings).expect("config");
        assert_eq!(input.add_field.len(), 1);
        assert_eq!(input.tags, vec!["interactive"]);
    }

    #[test]
    fn test_stdin_name() {
        let settings = serde_json::json!({});
        let input = StdinInput::from_config(&settings).expect("config");
        assert_eq!(input.name(), "stdin");
    }
}
