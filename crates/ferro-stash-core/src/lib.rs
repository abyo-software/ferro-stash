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

/// Read at most `limit + 1` bytes from a byte-chunk stream (e.g.
/// `reqwest::Response::bytes_stream()`), then format a bounded UTF-8 snippet
/// via [`bounded_snippet`].
///
/// Unlike `Response::text()`, this never buffers the whole body, so a
/// misconfigured/hostile endpoint returning a huge error body cannot OOM the
/// process on a diagnostic path. Stream errors stop the read with whatever was
/// collected so far. The stream must be `Unpin` — wrap with `Box::pin(...)`.
pub async fn read_bounded_body_stream<S, E>(mut stream: S, limit: usize) -> String
where
    S: tokio_stream::Stream<Item = Result<bytes::Bytes, E>> + Unpin,
{
    use tokio_stream::StreamExt;
    let cap = limit.saturating_add(1);
    let mut buf: Vec<u8> = Vec::new();
    while buf.len() < cap {
        match stream.next().await {
            Some(Ok(chunk)) => {
                let take = (cap - buf.len()).min(chunk.len());
                buf.extend_from_slice(&chunk[..take]);
            }
            Some(Err(_)) | None => break,
        }
    }
    bounded_snippet(&String::from_utf8_lossy(&buf), limit)
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

    #[tokio::test]
    async fn stream_reader_stops_after_limit() {
        // A stream of many 1 KiB chunks; only ~limit+1 bytes should be read.
        let chunks: Vec<Result<bytes::Bytes, std::convert::Infallible>> =
            (0..10_000).map(|_| Ok(bytes::Bytes::from(vec![b'x'; 1024]))).collect();
        let stream = tokio_stream::iter(chunks);
        let s = super::read_bounded_body_stream(stream, 512).await;
        // Output is the bounded snippet (512 bytes + marker), never the ~10 MB.
        assert!(s.starts_with(&"x".repeat(512)));
        assert!(s.contains("bytes total"));
        assert!(s.len() < 600, "snippet must be bounded, got {}", s.len());
    }

    #[tokio::test]
    async fn stream_reader_short_body_unchanged() {
        let stream = tokio_stream::iter(vec![Ok::<_, std::convert::Infallible>(
            bytes::Bytes::from_static(b"oops"),
        )]);
        assert_eq!(super::read_bounded_body_stream(stream, 512).await, "oops");
    }
}
