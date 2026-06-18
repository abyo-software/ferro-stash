// SPDX-License-Identifier: Apache-2.0
//! Multiline codec — merges multiple lines into a single event based on patterns.

use regex::Regex;

use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::Event;

use crate::Codec;

/// How to handle pattern matches.
#[derive(Debug, Clone, PartialEq)]
pub enum MultilineMode {
    /// Lines matching the pattern are the START of a new event.
    Previous,
    /// Lines matching the pattern are the END of the current event.
    Next,
}

/// Negate the pattern match.
#[derive(Debug, Clone)]
pub struct MultilineCodec {
    pattern: Regex,
    mode: MultilineMode,
    negate: bool,
    max_lines: usize,
    max_bytes: usize,
}

impl Default for MultilineCodec {
    fn default() -> Self {
        Self {
            pattern: Regex::new("^\\s").unwrap_or_else(|_| Regex::new(".").expect("infallible")),
            mode: MultilineMode::Previous,
            negate: false,
            max_lines: 500,
            max_bytes: 1_048_576, // 1MB
        }
    }
}

impl MultilineCodec {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let pattern_str = settings
            .get("pattern")
            .and_then(|v| v.as_str())
            .unwrap_or("^\\s");
        let pattern = Regex::new(pattern_str)
            .map_err(|e| FerroStashError::Codec(format!("invalid multiline pattern: {e}")))?;
        let mode = match settings
            .get("what")
            .and_then(|v| v.as_str())
            .unwrap_or("previous")
        {
            "next" => MultilineMode::Next,
            _ => MultilineMode::Previous,
        };
        let negate = settings
            .get("negate")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        let max_lines = settings
            .get("max_lines")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(500) as usize;
        let max_bytes = settings
            .get("max_bytes")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(1_048_576) as usize;

        Ok(Self {
            pattern,
            mode,
            negate,
            max_lines,
            max_bytes,
        })
    }

    /// Determines if a line should be merged with the current buffer.
    pub fn should_merge(&self, line: &str) -> bool {
        let matches = self.pattern.is_match(line);
        if self.negate {
            !matches
        } else {
            matches
        }
    }

    /// Returns the mode.
    pub fn mode(&self) -> &MultilineMode {
        &self.mode
    }

    /// Returns the max lines setting.
    pub fn max_lines(&self) -> usize {
        self.max_lines
    }

    /// Returns the max bytes setting.
    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }
}

impl Codec for MultilineCodec {
    fn name(&self) -> &'static str {
        "multiline"
    }

    fn decode(&self, data: &[u8]) -> Result<Vec<Event>> {
        // For single-line decode, just create an event.
        // The actual multiline merging is done by the input plugin's reader.
        let text = String::from_utf8_lossy(data);
        Ok(vec![Event::new(text.trim_end())])
    }

    fn is_stateful(&self) -> bool {
        true
    }

    fn encode(&self, event: &Event) -> Result<Vec<u8>> {
        let msg = event.message().unwrap_or("");
        Ok(format!("{msg}\n").into_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_multiline_should_merge() {
        let codec = MultilineCodec::default(); // pattern: ^\\s
        assert!(codec.should_merge("  continuation line"));
        assert!(!codec.should_merge("new line start"));
    }

    #[test]
    fn test_multiline_negate() {
        let codec = MultilineCodec {
            negate: true,
            ..Default::default()
        };
        // With negate, lines NOT matching ^\\s should merge
        assert!(!codec.should_merge("  continuation line"));
        assert!(codec.should_merge("new line start"));
    }

    #[test]
    fn test_multiline_from_config() {
        let settings = serde_json::json!({
            "pattern": "^\\[",
            "what": "previous",
            "negate": true,
            "max_lines": 100
        });
        let codec = MultilineCodec::from_config(&settings).expect("config");
        assert_eq!(codec.max_lines(), 100);
        assert!(codec.negate);
    }
}
