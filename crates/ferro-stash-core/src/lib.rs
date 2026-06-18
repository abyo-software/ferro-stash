// SPDX-License-Identifier: Apache-2.0
//! `FerroStash` Core — Event model, pipeline engine, and plugin traits.

pub mod buffer;
pub mod condition;
pub mod dead_letter_queue;
pub mod error;
pub mod event;
pub mod field_ref;
pub mod metrics;
pub mod monitoring;
pub mod multi_pipeline;
pub mod persistent_queue;
pub mod pipeline;
pub mod plugin;
pub mod settings_helpers;
pub mod shutdown;

pub use error::FerroStashError;
pub use event::{Event, EventValue, Metadata};
pub use pipeline::{Pipeline, PipelineConfig};
pub use plugin::{FilterPlugin, InputPlugin, OutputPlugin};
pub use shutdown::ShutdownSignal;

/// Truncate `body` to at most `limit` bytes (on a UTF-8 char boundary),
/// appending a `… (N bytes total)` marker when truncation occurs.
///
/// Used to bound HTTP error-response bodies before they are logged or placed
/// into error messages, so a misconfigured/hostile endpoint returning a huge
/// body cannot amplify logs or pressure memory on the diagnostic path.
#[must_use]
pub fn bounded_snippet(body: &str, limit: usize) -> String {
    if body.len() <= limit {
        return body.to_string();
    }
    let mut end = limit;
    while end > 0 && !body.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}… ({} bytes total)", &body[..end], body.len())
}

#[cfg(test)]
mod bounded_snippet_tests {
    use super::bounded_snippet;

    #[test]
    fn short_body_unchanged() {
        assert_eq!(bounded_snippet("short", 512), "short");
    }

    #[test]
    fn long_body_truncated_with_marker() {
        let body = "x".repeat(1000);
        let s = bounded_snippet(&body, 512);
        assert!(s.starts_with(&"x".repeat(512)));
        assert!(s.contains("1000 bytes total"));
    }

    #[test]
    fn respects_char_boundary() {
        let s = bounded_snippet("ああ", 4);
        assert!(s.starts_with('あ'));
        assert!(s.contains("bytes total"));
    }
}
