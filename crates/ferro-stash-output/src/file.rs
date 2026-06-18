// SPDX-License-Identifier: Apache-2.0
//! File output plugin — writes events to files.

use std::io::Write;
use std::sync::Mutex;

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::Event;
use ferro_stash_core::plugin::OutputPlugin;

#[allow(dead_code)]
#[derive(Debug)]
pub struct FileOutput {
    path: String,
    codec: FileCodec,
    flush_interval: u64,
    writer: Mutex<Option<std::io::BufWriter<std::fs::File>>>,
    create_if_deleted: bool,
    condition: Option<Condition>,
}

#[derive(Debug)]
enum FileCodec {
    JsonLines,
    Line(Option<String>),
}

impl FileOutput {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let path = settings
            .get("path")
            .and_then(|v| v.as_str())
            .ok_or_else(|| FerroStashError::Output {
                plugin: "file".to_string(),
                message: "path is required".to_string(),
            })?
            .to_string();

        // Codec can be a string ("line") or a plugin block: `codec => line { format => "..." }`
        let codec_name;
        let codec_settings: &serde_json::Value;
        if let Some(obj) = settings.get("codec").and_then(|v| v.as_object()) {
            codec_name = obj
                .get("_plugin")
                .and_then(|v| v.as_str())
                .unwrap_or("json_lines")
                .to_string();
            codec_settings = settings.get("codec").expect("checked above");
        } else {
            codec_name = settings
                .get("codec")
                .and_then(|v| v.as_str())
                .unwrap_or("json_lines")
                .to_string();
            codec_settings = settings;
        }

        let codec = match codec_name.as_str() {
            "line" => {
                let format = codec_settings
                    .get("format")
                    .and_then(|v| v.as_str())
                    .map(String::from);
                FileCodec::Line(format)
            }
            _ => FileCodec::JsonLines,
        };

        let flush_interval = settings
            .get("flush_interval")
            .and_then(ferro_stash_core::settings_helpers::as_u64_flexible)
            .unwrap_or(2);
        let create_if_deleted = settings
            .get("create_if_deleted")
            .and_then(ferro_stash_core::settings_helpers::as_bool_flexible)
            .unwrap_or(true);

        Ok(Self {
            path,
            codec,
            flush_interval,
            writer: Mutex::new(None),
            create_if_deleted,
            condition,
        })
    }

    fn ensure_writer(&self) -> Result<()> {
        let mut guard = self.writer.lock().map_err(|e| FerroStashError::Output {
            plugin: "file".to_string(),
            message: format!("lock error: {e}"),
        })?;

        if guard.is_none() || (self.create_if_deleted && !std::path::Path::new(&self.path).exists())
        {
            // Create parent directories
            if let Some(parent) = std::path::Path::new(&self.path).parent() {
                std::fs::create_dir_all(parent).map_err(|e| FerroStashError::Output {
                    plugin: "file".to_string(),
                    message: format!("cannot create directory: {e}"),
                })?;
            }
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)
                .map_err(|e| FerroStashError::Output {
                    plugin: "file".to_string(),
                    message: format!("cannot open {}: {e}", self.path),
                })?;
            *guard = Some(std::io::BufWriter::new(file));
        }

        Ok(())
    }
}

#[async_trait]
impl OutputPlugin for FileOutput {
    fn name(&self) -> &'static str {
        "file"
    }

    async fn output(&self, events: Vec<Event>) -> Result<()> {
        self.ensure_writer()?;
        let mut guard = self.writer.lock().map_err(|e| FerroStashError::Output {
            plugin: "file".to_string(),
            message: format!("lock error: {e}"),
        })?;

        if let Some(ref mut writer) = *guard {
            for event in &events {
                let line = match &self.codec {
                    FileCodec::JsonLines => event.to_json_string(),
                    FileCodec::Line(Some(fmt)) => event.sprintf(fmt),
                    FileCodec::Line(None) => event.message().unwrap_or("").to_string(),
                };
                writeln!(writer, "{line}").map_err(|e| FerroStashError::Output {
                    plugin: "file".to_string(),
                    message: format!("write error: {e}"),
                })?;
            }
            writer.flush().map_err(|e| FerroStashError::Output {
                plugin: "file".to_string(),
                message: format!("flush error: {e}"),
            })?;
        }

        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        let mut guard = self.writer.lock().map_err(|e| FerroStashError::Output {
            plugin: "file".to_string(),
            message: format!("lock error: {e}"),
        })?;
        if let Some(ref mut writer) = *guard {
            writer.flush().map_err(|e| FerroStashError::Output {
                plugin: "file".to_string(),
                message: format!("flush error: {e}"),
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
    fn test_file_output_config() {
        let settings = serde_json::json!({ "path": "/tmp/output.log" });
        let output = FileOutput::from_config(&settings, None).expect("config");
        assert_eq!(output.path, "/tmp/output.log");
        assert_eq!(output.name(), "file");
    }

    #[test]
    fn test_file_output_missing_path() {
        let settings = serde_json::json!({});
        assert!(FileOutput::from_config(&settings, None).is_err());
    }

    #[test]
    fn test_file_output_config_line_codec() {
        let settings = serde_json::json!({
            "path": "/tmp/output.log",
            "codec": "line",
            "format": "%{message}"
        });
        let output = FileOutput::from_config(&settings, None).expect("config");
        assert!(matches!(output.codec, FileCodec::Line(Some(_))));
    }

    #[test]
    fn test_file_output_config_json_lines() {
        let settings = serde_json::json!({
            "path": "/tmp/output.log",
            "codec": "json_lines"
        });
        let output = FileOutput::from_config(&settings, None).expect("config");
        assert!(matches!(output.codec, FileCodec::JsonLines));
    }

    #[tokio::test]
    async fn test_file_output_write() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("test_output.log");
        let settings = serde_json::json!({
            "path": path.to_string_lossy().to_string()
        });
        let output = FileOutput::from_config(&settings, None).expect("config");
        let events = vec![Event::new("hello"), Event::new("world")];
        output.output(events).await.expect("output");

        let content = std::fs::read_to_string(&path).expect("read");
        assert!(content.contains("hello"));
        assert!(content.contains("world"));
        assert_eq!(content.lines().count(), 2);
    }

    #[tokio::test]
    async fn test_file_output_flush() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("flush_test.log");
        let settings = serde_json::json!({
            "path": path.to_string_lossy().to_string()
        });
        let output = FileOutput::from_config(&settings, None).expect("config");
        output
            .output(vec![Event::new("test")])
            .await
            .expect("output");
        let result = output.flush().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_file_output_append() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("append_test.log");
        let settings = serde_json::json!({
            "path": path.to_string_lossy().to_string()
        });
        let output = FileOutput::from_config(&settings, None).expect("config");
        output
            .output(vec![Event::new("line1")])
            .await
            .expect("output1");
        output
            .output(vec![Event::new("line2")])
            .await
            .expect("output2");

        let content = std::fs::read_to_string(&path).expect("read");
        assert_eq!(content.lines().count(), 2);
    }

    #[tokio::test]
    async fn test_file_output_line_codec() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("line_test.log");
        let settings = serde_json::json!({
            "path": path.to_string_lossy().to_string(),
            "codec": "line"
        });
        let output = FileOutput::from_config(&settings, None).expect("config");
        output
            .output(vec![Event::new("plain text")])
            .await
            .expect("output");

        let content = std::fs::read_to_string(&path).expect("read");
        assert_eq!(content.trim(), "plain text");
    }
}
