// SPDX-License-Identifier: Apache-2.0
//! Stdout output plugin — prints events to standard output.

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::Event;
use ferro_stash_core::plugin::OutputPlugin;

#[derive(Debug)]
pub struct StdoutOutput {
    codec: OutputCodec,
    condition: Option<Condition>,
}

#[derive(Debug)]
enum OutputCodec {
    Json,
    JsonPretty,
    Rubydebug,
    Line(Option<String>),
    Dots,
}

impl StdoutOutput {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let codec = match settings
            .get("codec")
            .and_then(|v| v.as_str())
            .unwrap_or("rubydebug")
        {
            "json" | "json_lines" => OutputCodec::Json,
            "json_pretty" => OutputCodec::JsonPretty,
            "rubydebug" => OutputCodec::Rubydebug,
            "dots" => OutputCodec::Dots,
            "line" => {
                let format = settings
                    .get("format")
                    .and_then(|v| v.as_str())
                    .map(String::from);
                OutputCodec::Line(format)
            }
            _ => OutputCodec::Rubydebug,
        };

        Ok(Self { codec, condition })
    }
}

#[async_trait]
impl OutputPlugin for StdoutOutput {
    fn name(&self) -> &'static str {
        "stdout"
    }

    async fn output(&self, events: Vec<Event>) -> Result<()> {
        for event in &events {
            match &self.codec {
                OutputCodec::Json => {
                    println!("{}", event.to_json_string());
                }
                OutputCodec::JsonPretty | OutputCodec::Rubydebug => {
                    println!("{}", event.to_json_string_pretty());
                }
                OutputCodec::Line(format) => {
                    if let Some(fmt) = format {
                        println!("{}", event.sprintf(fmt));
                    } else {
                        println!("{}", event.message().unwrap_or(""));
                    }
                }
                OutputCodec::Dots => {
                    print!(".");
                }
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
    fn test_stdout_config_default() {
        let settings = serde_json::json!({});
        let output = StdoutOutput::from_config(&settings, None).expect("config");
        assert_eq!(output.name(), "stdout");
    }

    #[test]
    fn test_stdout_config_json() {
        let settings = serde_json::json!({ "codec": "json" });
        let output = StdoutOutput::from_config(&settings, None).expect("config");
        assert!(matches!(output.codec, OutputCodec::Json));
    }

    #[test]
    fn test_stdout_config_json_pretty() {
        let settings = serde_json::json!({ "codec": "json_pretty" });
        let output = StdoutOutput::from_config(&settings, None).expect("config");
        assert!(matches!(output.codec, OutputCodec::JsonPretty));
    }

    #[test]
    fn test_stdout_config_rubydebug() {
        let settings = serde_json::json!({ "codec": "rubydebug" });
        let output = StdoutOutput::from_config(&settings, None).expect("config");
        assert!(matches!(output.codec, OutputCodec::Rubydebug));
    }

    #[test]
    fn test_stdout_config_dots() {
        let settings = serde_json::json!({ "codec": "dots" });
        let output = StdoutOutput::from_config(&settings, None).expect("config");
        assert!(matches!(output.codec, OutputCodec::Dots));
    }

    #[test]
    fn test_stdout_config_line() {
        let settings = serde_json::json!({ "codec": "line", "format": "%{message}" });
        let output = StdoutOutput::from_config(&settings, None).expect("config");
        assert!(matches!(output.codec, OutputCodec::Line(_)));
    }

    #[tokio::test]
    async fn test_stdout_output_runs() {
        let settings = serde_json::json!({ "codec": "dots" });
        let output = StdoutOutput::from_config(&settings, None).expect("config");
        let events = vec![Event::new("test")];
        let result = output.output(events).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_stdout_no_condition() {
        let settings = serde_json::json!({});
        let output = StdoutOutput::from_config(&settings, None).expect("config");
        assert!(output.condition().is_none());
    }
}
