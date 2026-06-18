// SPDX-License-Identifier: Apache-2.0
//! YAML configuration parser.
//!
//! Example YAML config:
//! ```yaml
//! pipeline:
//!   workers: 4
//!   batch_size: 500
//!
//! input:
//!   - type: file
//!     path: /var/log/*.log
//!     start_position: beginning
//!
//! filter:
//!   - type: grok
//!     match:
//!       message: "%{COMBINEDAPACHELOG}"
//!
//! output:
//!   - type: elasticsearch
//!     hosts:
//!       - http://localhost:9200
//!     index: "logs-%{+YYYY.MM.dd}"
//! ```

use ferro_stash_core::error::{FerroStashError, Result};

use crate::model::{Config, FilterConfig, InputConfig, OutputConfig};

/// Parses a YAML configuration string.
pub fn parse(input: &str) -> Result<Config> {
    let yaml: serde_yaml::Value = serde_yaml::from_str(input)
        .map_err(|e| FerroStashError::Config(format!("YAML parse error: {e}")))?;

    let mapping = yaml
        .as_mapping()
        .ok_or_else(|| FerroStashError::Config("expected YAML mapping at root".to_string()))?;

    let mut config = Config::default();

    // Parse pipeline settings
    if let Some(pipeline) = mapping.get(serde_yaml::Value::String("pipeline".into())) {
        let json_str = serde_json::to_string(&yaml_to_json(pipeline))
            .map_err(|e| FerroStashError::Config(format!("pipeline settings error: {e}")))?;
        config.pipeline = serde_json::from_str(&json_str)
            .map_err(|e| FerroStashError::Config(format!("pipeline settings error: {e}")))?;
    }

    // Parse inputs
    if let Some(inputs) = mapping.get(serde_yaml::Value::String("input".into())) {
        config.inputs = parse_inputs(inputs)?;
    }

    // Parse filters
    if let Some(filters) = mapping.get(serde_yaml::Value::String("filter".into())) {
        config.filters = parse_filters(filters)?;
    }

    // Parse outputs
    if let Some(outputs) = mapping.get(serde_yaml::Value::String("output".into())) {
        config.outputs = parse_outputs(outputs)?;
    }

    // Parse queue configuration
    if let Some(queue) = mapping.get(serde_yaml::Value::String("queue".into())) {
        let json_str = serde_json::to_string(&yaml_to_json(queue))
            .map_err(|e| FerroStashError::Config(format!("queue settings error: {e}")))?;
        config.queue = serde_json::from_str(&json_str)
            .map_err(|e| FerroStashError::Config(format!("queue settings error: {e}")))?;
    }

    // Parse dead_letter_queue configuration
    if let Some(dlq) = mapping.get(serde_yaml::Value::String("dead_letter_queue".into())) {
        let json_str = serde_json::to_string(&yaml_to_json(dlq))
            .map_err(|e| FerroStashError::Config(format!("DLQ settings error: {e}")))?;
        config.dead_letter_queue = serde_json::from_str(&json_str)
            .map_err(|e| FerroStashError::Config(format!("DLQ settings error: {e}")))?;
    }

    Ok(config)
}

fn parse_inputs(value: &serde_yaml::Value) -> Result<Vec<InputConfig>> {
    let arr = value
        .as_sequence()
        .ok_or_else(|| FerroStashError::Config("input must be a list".to_string()))?;

    let mut inputs = Vec::new();
    for item in arr {
        let map = item
            .as_mapping()
            .ok_or_else(|| FerroStashError::Config("input item must be a mapping".to_string()))?;

        let plugin_type = map
            .get(serde_yaml::Value::String("type".into()))
            .and_then(|v| v.as_str())
            .ok_or_else(|| FerroStashError::Config("input requires 'type' field".to_string()))?
            .to_string();

        let codec = map
            .get(serde_yaml::Value::String("codec".into()))
            .and_then(|v| v.as_str())
            .map(String::from);

        let tags: Vec<String> = map
            .get(serde_yaml::Value::String("tags".into()))
            .and_then(|v| v.as_sequence())
            .map(|seq| {
                seq.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let event_type = map
            .get(serde_yaml::Value::String("event_type".into()))
            .and_then(|v| v.as_str())
            .map(String::from);

        let settings = yaml_to_json(item);

        inputs.push(InputConfig {
            plugin_type,
            settings,
            codec,
            codec_settings: serde_json::Value::Object(serde_json::Map::new()),
            tags,
            event_type,
        });
    }

    Ok(inputs)
}

fn parse_filters(value: &serde_yaml::Value) -> Result<Vec<FilterConfig>> {
    let arr = value
        .as_sequence()
        .ok_or_else(|| FerroStashError::Config("filter must be a list".to_string()))?;

    let mut filters = Vec::new();
    for item in arr {
        let map = item
            .as_mapping()
            .ok_or_else(|| FerroStashError::Config("filter item must be a mapping".to_string()))?;

        let plugin_type = map
            .get(serde_yaml::Value::String("type".into()))
            .and_then(|v| v.as_str())
            .ok_or_else(|| FerroStashError::Config("filter requires 'type' field".to_string()))?
            .to_string();

        let settings = yaml_to_json(item);

        filters.push(FilterConfig {
            plugin_type,
            settings,
            condition: None,
        });
    }

    Ok(filters)
}

fn parse_outputs(value: &serde_yaml::Value) -> Result<Vec<OutputConfig>> {
    let arr = value
        .as_sequence()
        .ok_or_else(|| FerroStashError::Config("output must be a list".to_string()))?;

    let mut outputs = Vec::new();
    for item in arr {
        let map = item
            .as_mapping()
            .ok_or_else(|| FerroStashError::Config("output item must be a mapping".to_string()))?;

        let plugin_type = map
            .get(serde_yaml::Value::String("type".into()))
            .and_then(|v| v.as_str())
            .ok_or_else(|| FerroStashError::Config("output requires 'type' field".to_string()))?
            .to_string();

        let codec = map
            .get(serde_yaml::Value::String("codec".into()))
            .and_then(|v| v.as_str())
            .map(String::from);

        let settings = yaml_to_json(item);

        outputs.push(OutputConfig {
            plugin_type,
            settings,
            codec,
            codec_settings: serde_json::Value::Object(serde_json::Map::new()),
            condition: None,
        });
    }

    Ok(outputs)
}

/// Converts a YAML value to a JSON value.
fn yaml_to_json(yaml: &serde_yaml::Value) -> serde_json::Value {
    match yaml {
        serde_yaml::Value::Null => serde_json::Value::Null,
        serde_yaml::Value::Bool(b) => serde_json::Value::Bool(*b),
        serde_yaml::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                serde_json::Value::Number(serde_json::Number::from(i))
            } else if let Some(f) = n.as_f64() {
                serde_json::Value::Number(
                    serde_json::Number::from_f64(f).unwrap_or_else(|| serde_json::Number::from(0)),
                )
            } else {
                serde_json::Value::Null
            }
        }
        serde_yaml::Value::String(s) => serde_json::Value::String(s.clone()),
        serde_yaml::Value::Sequence(seq) => {
            serde_json::Value::Array(seq.iter().map(yaml_to_json).collect())
        }
        serde_yaml::Value::Mapping(map) => {
            let mut json_map = serde_json::Map::new();
            for (k, v) in map {
                let key = match k {
                    serde_yaml::Value::String(s) => s.clone(),
                    _ => format!("{k:?}"),
                };
                json_map.insert(key, yaml_to_json(v));
            }
            serde_json::Value::Object(json_map)
        }
        serde_yaml::Value::Tagged(tagged) => yaml_to_json(&tagged.value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_yaml_simple() {
        let yaml = r"
input:
  - type: stdin

output:
  - type: stdout
";
        let config = parse(yaml).expect("parse failed");
        assert_eq!(config.inputs.len(), 1);
        assert_eq!(config.inputs[0].plugin_type, "stdin");
        assert_eq!(config.outputs.len(), 1);
        assert_eq!(config.outputs[0].plugin_type, "stdout");
    }

    #[test]
    fn test_parse_yaml_with_settings() {
        let yaml = r#"
pipeline:
  workers: 4
  batch_size: 1000

input:
  - type: file
    path: /var/log/*.log
    start_position: beginning

filter:
  - type: grok
    match:
      message: "%{COMBINEDAPACHELOG}"

output:
  - type: elasticsearch
    hosts:
      - http://localhost:9200
    index: logs
"#;
        let config = parse(yaml).expect("parse failed");
        assert_eq!(config.pipeline.workers, 4);
        assert_eq!(config.pipeline.batch_size, 1000);
        assert_eq!(config.inputs[0].plugin_type, "file");
        assert_eq!(config.filters[0].plugin_type, "grok");
        assert_eq!(config.outputs[0].plugin_type, "elasticsearch");
    }

    #[test]
    fn test_parse_yaml_with_tags() {
        let yaml = r"
input:
  - type: file
    path: /var/log/syslog
    tags:
      - syslog
      - linux

output:
  - type: stdout
";
        let config = parse(yaml).expect("parse failed");
        assert_eq!(config.inputs[0].tags, vec!["syslog", "linux"]);
    }

    #[test]
    fn test_parse_yaml_invalid() {
        let result = parse("not: [valid: yaml: {{{");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_yaml_with_queue() {
        let yaml = r"
input:
  - type: stdin

output:
  - type: stdout

queue:
  type: persisted
  path: /var/lib/ferro-stash/queue
  max_bytes: 1073741824
";
        let config = parse(yaml).expect("parse failed");
        assert_eq!(config.queue.queue_type, "persisted");
        assert_eq!(config.queue.path, "/var/lib/ferro-stash/queue");
        assert_eq!(config.queue.max_bytes, 1_073_741_824);
    }

    #[test]
    fn test_parse_yaml_with_dlq() {
        let yaml = r"
input:
  - type: stdin

output:
  - type: stdout

dead_letter_queue:
  enable: true
  path: /var/lib/ferro-stash/dlq
  max_bytes: 104857600
";
        let config = parse(yaml).expect("parse failed");
        assert!(config.dead_letter_queue.enable);
        assert_eq!(config.dead_letter_queue.path, "/var/lib/ferro-stash/dlq");
        assert_eq!(config.dead_letter_queue.max_bytes, 104_857_600);
    }

    #[test]
    fn test_parse_yaml_queue_defaults() {
        let yaml = r"
input:
  - type: stdin

output:
  - type: stdout
";
        let config = parse(yaml).expect("parse failed");
        assert_eq!(config.queue.queue_type, "memory");
        assert!(!config.dead_letter_queue.enable);
    }
}
