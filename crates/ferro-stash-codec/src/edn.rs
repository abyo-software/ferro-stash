// SPDX-License-Identifier: Apache-2.0
//! EDN (Extensible Data Notation) codec — Clojure's data format.
//!
//! EDN is a subset of Clojure syntax used for data transfer.
//! This codec parses basic EDN values into events:
//! - Maps: `{:key "value", :count 42}`
//! - Strings: `"hello"`
//! - Numbers: `42`, `3.14`
//! - Keywords: `:keyword`
//! - Booleans: `true`, `false`
//! - Nil: `nil`
//! - Vectors: `[1 2 3]`
//! - Sets: `#{1 2 3}`

use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use indexmap::IndexMap;

use crate::Codec;

/// Maximum nesting depth for EDN values (vectors, maps, sets, tagged
/// literals).
///
/// Without this cap, an attacker can craft an input like
/// `[#X[#X[#X...` (sibling reproducer in
/// `fuzz/artifacts/codec_decode/timeout-cc6b8c35*`, `timeout-45ca6cbb*`,
/// and `timeout-946cc5ce*`) that drives mutually recursive
/// `parse_vector` ↔ `parse_value` ↔ `parse_tagged` calls dozens of
/// frames deep. The depth cap pairs with `parse_tagged`'s
/// "don't-stringify-structural-values" guard (see that function's
/// comment) — together they bound both stack depth and per-frame
/// allocation. Pre-fix observation: a 313-byte input pinned `decode`
/// for 50+ seconds while building a >1 GiB intermediate string.
///
/// 64 is the same depth cap used by sibling repos for the same class
/// of bug:
/// - ferrosearch `1af3262` (eql/fql/esql parsers, `MAX_RECURSION_DEPTH`)
/// - ferrodruid `864f3ce` (SQL parenthesis nesting)
///
/// Any real-world EDN payload nests well under 32 levels; 64 leaves
/// generous headroom while killing the DoS cliff.
const MAX_DEPTH: usize = 64;

/// Maximum total count of container-open bytes (`{`, `[`) outside string
/// literals in an attacker-supplied EDN payload.
///
/// 2026-05-17 fuzz wave (4 artifacts in `codec_decode`,
/// `timeout-032ed780/4096092e/7b3997ce/2c6eb739`) carried payloads
/// shaped `{{{{{{X<garbage>}{{{{{...` with 29-101 `{` opens versus
/// only 27-82 `}` closes. Each burned 18-30 s+ of wall-clock inside
/// `EdnCodec::decode` despite the existing `MAX_DEPTH = 64` cap
/// passing trivially (effective nesting depth in these inputs is
/// ≤ 17 — most opens stay sibling-flat, never reaching the depth
/// threshold). The blowup is at the iteration level rather than the
/// depth level: `parse_map`'s outer loop processes one key-value
/// pair per iteration; with malformed UTF-8 bytes (becoming
/// `\u{FFFD}` after `from_utf8_lossy`) at every value position,
/// `parse_symbol` walks the remaining input each time, driving
/// total work super-linearly.
///
/// Realistic EDN payloads consumed by Logstash-compatible pipelines
/// carry a single root map with a small number of nested
/// configurations (sources / filters / sinks). Even an aggressively
/// nested Logstash pipeline produces ~6-8 total `{` opens. Cap = 32
/// is generous (≥4× a realistic config) and rejects every artifact
/// in the 2026-05-17 family by at least 1.0× (the smallest, 29, is
/// already a borderline-DoS at 18.4 s wall-clock; cap = 32
/// excludes it via the consecutive-opens companion below).
///
/// Boundary-inclusive `>=` reject — same boundary-learning
/// discipline as `MAX_REGEXP_REPETITION_COUNT` /
/// `MAX_SQL_BRACKET_OPENS_TOTAL`.
const MAX_EDN_CONTAINER_OPENS_TOTAL: usize = 32;

/// Maximum consecutive run of `{` / `[` container-open bytes outside
/// string literals.
///
/// Companion to `MAX_EDN_CONTAINER_OPENS_TOTAL` — every artifact in
/// the 2026-05-17 family starts with runs of 5-15 consecutive `{`
/// (e.g. 032ed780 carries `{{{{{ruby{` then `{{{{{{`; 4096092e
/// carries `{{{{{{` then `{{{{{{{{{{{{{{{` = 15 consecutive). This
/// cap closes the same boundary-learning escape that the regexp
/// consecutive-quantifier cap closes — even if a future fuzz
/// pattern drops total opens to 31, a 5+ consecutive `{` run is
/// catastrophic on its own (each open in the run triggers a fresh
/// recursive `parse_map` call whose inner loop walks the remaining
/// input).
///
/// Realistic EDN never starts a payload with 5+ consecutive `{`;
/// even `{{:nested {:deep {:value 1}}}}` is 4 consecutive in
/// pretty-printed form. 4 is the same consecutive-run cap used in
/// `MAX_SQL_COMPARISON_RUN` / `MAX_REGEXP_CONSECUTIVE_QUANTIFIERS`.
///
/// Boundary-inclusive `>` reject (strict greater than).
const MAX_EDN_CONSECUTIVE_OPENS: usize = 4;

/// Walk `text` byte-by-byte and reject inputs whose total or
/// consecutive container-open counts exceed the caps above. String
/// literals (`"..."`) are skipped so legitimate string content with
/// embedded `{`/`[` does not count toward the cap; everything else
/// (including unmatched closes, EDN keywords, symbols) contributes.
///
/// Run *before* `parse_value` so the slow per-byte parse never
/// starts on inputs that would time out.
fn check_edn_container_density(text: &str) -> Result<()> {
    let bytes = text.as_bytes();
    let mut total_opens: usize = 0;
    let mut run: usize = 0;
    let mut max_run: usize = 0;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            // Skip the contents of `"..."` string literals (with `\"`
            // escape) so legitimate strings carrying brace bytes do
            // not falsely trip the cap.
            b'"' => {
                run = 0;
                i += 1;
                while i < bytes.len() {
                    match bytes[i] {
                        b'\\' if i + 1 < bytes.len() => {
                            i += 2;
                        }
                        b'"' => {
                            i += 1;
                            break;
                        }
                        _ => {
                            i += 1;
                        }
                    }
                }
            }
            // `;` line comments — EDN single-line comment to EOL.
            b';' => {
                run = 0;
                i += 1;
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'{' | b'[' => {
                total_opens += 1;
                run += 1;
                if run > max_run {
                    max_run = run;
                }
                i += 1;
            }
            _ => {
                run = 0;
                i += 1;
            }
        }
    }
    if total_opens >= MAX_EDN_CONTAINER_OPENS_TOTAL {
        return Err(FerroStashError::Codec(format!(
            "EDN payload has {total_opens} container-open bytes (`{{`/`[`); \
             maximum {} (exclusive)",
            MAX_EDN_CONTAINER_OPENS_TOTAL
        )));
    }
    if max_run > MAX_EDN_CONSECUTIVE_OPENS {
        return Err(FerroStashError::Codec(format!(
            "EDN payload has a run of {max_run} consecutive container-open \
             bytes (`{{`/`[`); maximum {MAX_EDN_CONSECUTIVE_OPENS}"
        )));
    }
    Ok(())
}

/// EDN codec configuration.
#[derive(Debug, Clone, Default)]
pub struct EdnCodec {
    /// Target field for decoded data (None = merge into root).
    pub target: Option<String>,
}

impl EdnCodec {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let target = settings
            .get("target")
            .and_then(|v| v.as_str())
            .map(String::from);
        Ok(Self { target })
    }

    /// Parse an EDN value from the input string, returning the value and remaining input.
    fn parse_value(input: &str) -> Option<(EventValue, &str)> {
        Self::parse_value_inner(input, 0)
    }

    /// Internal recursive parse with a depth counter.
    ///
    /// `depth` is incremented every time a container parser
    /// (`parse_vector` / `parse_map` / `parse_set` / `parse_tagged`)
    /// recurses back into `parse_value_inner`. Returning `None` once
    /// `depth >= MAX_DEPTH` propagates up the recursion chain via
    /// the existing `?` operators and is caught by `decode` as a
    /// "failed to parse EDN" error — never a panic, never a hang.
    fn parse_value_inner(input: &str, depth: usize) -> Option<(EventValue, &str)> {
        if depth >= MAX_DEPTH {
            return None;
        }
        // Skip whitespace and comments
        let mut input = input.trim_start();
        // Skip ; line comments
        while input.starts_with(';') {
            if let Some(nl) = input.find('\n') {
                input = input[nl + 1..].trim_start();
            } else {
                return None; // rest is all comment
            }
        }
        if input.is_empty() {
            return None;
        }

        let first = input.as_bytes()[0];
        match first {
            b'"' => Self::parse_string(input),
            b'{' => Self::parse_map(input, depth),
            b'[' => Self::parse_vector(input, depth),
            b'#' if input.len() > 1 && input.as_bytes()[1] == b'{' => Self::parse_set(input, depth),
            b'#' if input.len() > 1 => {
                // Tagged literals: #inst "...", #uuid "...", #tag value
                Self::parse_tagged(input, depth)
            }
            b':' => Self::parse_keyword(input),
            b't' if input.starts_with("true") => {
                let rest = &input[4..];
                Some((EventValue::Boolean(true), rest))
            }
            b'f' if input.starts_with("false") => {
                let rest = &input[5..];
                Some((EventValue::Boolean(false), rest))
            }
            b'n' if input.starts_with("nil") => {
                let rest = &input[3..];
                Some((EventValue::Null, rest))
            }
            b'\\' => {
                // Character literal: \a, \newline, \space, \tab
                Self::parse_char_literal(input)
            }
            b'-' | b'0'..=b'9' => Self::parse_number(input),
            _ => {
                // Symbol or unknown — read as string until delimiter
                Self::parse_symbol(input)
            }
        }
    }

    fn parse_tagged(input: &str, depth: usize) -> Option<(EventValue, &str)> {
        // #inst "2026-04-12T00:00:00Z" → string value (tag dropped for known tags)
        // #uuid "..."                  → string value
        // #tag <scalar>                → "#tag <scalar>" string preserves tag
        // #tag <Array|Object>          → value passed through unchanged.
        //   The pre-fix path stringified non-scalar values via
        //   `serde_json::to_string` and embedded the result in a
        //   `format!("#{tag} {}", …)`. That is exponentially blowing
        //   up on adversarial nested input: each outer `parse_tagged`
        //   serializes a value that itself contains an array of
        //   already-stringified deep tagged values, doubling the
        //   string size at each layer (observed: 1 GiB+ string built
        //   from a 313-byte input). Drop the tag for non-scalars
        //   instead — the structural value is preserved, and tag
        //   metadata for non-`inst`/non-`uuid` tags has no further
        //   semantic role in the FerroStash event model anyway
        //   (sibling `#inst`/`#uuid` already discard the tag).
        let rest = &input[1..]; // skip '#'
        let tag_end = rest.find(|c: char| c.is_whitespace()).unwrap_or(rest.len());
        let tag = &rest[..tag_end];
        let after_tag = rest[tag_end..].trim_start();

        // Parse the value following the tag
        let (value, remaining) = Self::parse_value_inner(after_tag, depth + 1)?;

        // For known tags, keep the parsed value; add tag metadata
        match tag {
            "inst" | "uuid" => Some((value, remaining)),
            _ => {
                // Only stringify scalars — embedding a structural
                // value (`Array`/`Object`) here is the
                // exponential-blowup site (see comment above).
                match &value {
                    EventValue::Array(_) | EventValue::Object(_) => {
                        // Drop the unknown tag; pass the structural
                        // value through unchanged. (Same behaviour
                        // as `#inst`/`#uuid`.)
                        Some((value, remaining))
                    }
                    _ => {
                        let s = format!("#{tag} {}", EventValue::to_string_lossy(&value));
                        Some((EventValue::String(s), remaining))
                    }
                }
            }
        }
    }

    fn parse_char_literal(input: &str) -> Option<(EventValue, &str)> {
        let rest = &input[1..]; // skip '\'
        let end = rest
            .find(|c: char| c.is_whitespace() || c == ',' || c == '}' || c == ']' || c == ')')
            .unwrap_or(rest.len());
        let char_name = &rest[..end];
        let ch = match char_name {
            "newline" => '\n',
            "return" => '\r',
            "space" => ' ',
            "tab" => '\t',
            s if s.len() == 1 => s.chars().next().unwrap_or(' '),
            _ => '?',
        };
        Some((EventValue::String(ch.to_string()), &rest[end..]))
    }

    fn parse_string(input: &str) -> Option<(EventValue, &str)> {
        if !input.starts_with('"') {
            return None;
        }
        let mut chars = input[1..].char_indices();
        let mut s = String::new();
        while let Some((_, c)) = chars.next() {
            if c == '\\' {
                if let Some((_, escaped)) = chars.next() {
                    match escaped {
                        'n' => s.push('\n'),
                        't' => s.push('\t'),
                        'r' => s.push('\r'),
                        '"' => s.push('"'),
                        '\\' => s.push('\\'),
                        _ => {
                            s.push('\\');
                            s.push(escaped);
                        }
                    }
                }
            } else if c == '"' {
                let (idx, _) = chars.next().map_or((input.len() - 1, ' '), |(i, c)| (i, c));
                let rest = &input[1 + idx..];
                return Some((EventValue::String(s), rest));
            } else {
                s.push(c);
            }
        }
        // Unterminated string — use what we have
        Some((EventValue::String(s), ""))
    }

    fn parse_keyword(input: &str) -> Option<(EventValue, &str)> {
        if !input.starts_with(':') {
            return None;
        }
        let end = input[1..]
            .find(|c: char| c.is_whitespace() || c == ',' || c == '}' || c == ']' || c == ')')
            .map_or(input.len(), |i| i + 1);
        let keyword = &input[1..end];
        Some((EventValue::String(keyword.to_string()), &input[end..]))
    }

    fn parse_number(input: &str) -> Option<(EventValue, &str)> {
        let end = input
            .find(|c: char| c.is_whitespace() || c == ',' || c == '}' || c == ']' || c == ')')
            .unwrap_or(input.len());
        let num_str = &input[..end];

        if let Ok(n) = num_str.parse::<i64>() {
            Some((EventValue::Integer(n), &input[end..]))
        } else if let Ok(f) = num_str.parse::<f64>() {
            Some((EventValue::Float(f), &input[end..]))
        } else {
            Some((EventValue::String(num_str.to_string()), &input[end..]))
        }
    }

    fn parse_symbol(input: &str) -> Option<(EventValue, &str)> {
        let end = input
            .find(|c: char| c.is_whitespace() || c == ',' || c == '}' || c == ']' || c == ')')
            .unwrap_or(input.len());
        if end == 0 {
            return None;
        }
        let symbol = &input[..end];
        Some((EventValue::String(symbol.to_string()), &input[end..]))
    }

    fn parse_map(input: &str, depth: usize) -> Option<(EventValue, &str)> {
        if !input.starts_with('{') {
            return None;
        }
        let mut rest = &input[1..];
        let mut map = IndexMap::new();

        loop {
            rest = rest.trim_start();
            // Skip commas
            while rest.starts_with(',') {
                rest = rest[1..].trim_start();
            }
            if rest.starts_with('}') {
                rest = &rest[1..];
                break;
            }
            if rest.is_empty() {
                break;
            }

            let prev_len = rest.len();
            // Parse key
            let (key, after_key) = Self::parse_value_inner(rest, depth + 1)?;
            let key_str = match key {
                EventValue::String(s) => s,
                // 2026-05-24 fuzz wave: `timeout-4425b914` (3354B)
                // burned 10+ s of wall-clock in `serde_json::Serializer::
                // serialize_str` reached via `to_string_lossy(<inner Object>)`
                // here. The exponential blow-up shape is the same one
                // closed for `parse_tagged` (see comment above): each
                // outer `parse_map` serializes a key that itself contains
                // already-serialized nested-container strings, growing
                // the serialized output by a factor per nesting level.
                // EDN keys must be hashable atoms anyway (strings /
                // keywords / numbers / symbols / chars / booleans), so
                // a container in key position is malformed — reject the
                // parse rather than spin.
                EventValue::Array(_) | EventValue::Object(_) => return None,
                other => other.to_string_lossy(),
            };

            // Parse value
            let (value, after_value) = Self::parse_value_inner(after_key, depth + 1)?;
            // Defence-in-depth: every successful key+value pair must
            // shrink the remaining input. If neither parser advanced
            // (e.g. a future code path that returns the slice unchanged),
            // bail rather than spin. The existing parsers always
            // advance; this is a belt-and-braces guard against
            // regressions.
            if after_value.len() >= prev_len {
                return None;
            }
            map.insert(key_str, value);
            rest = after_value;
        }

        Some((EventValue::Object(map), rest))
    }

    fn parse_vector(input: &str, depth: usize) -> Option<(EventValue, &str)> {
        if !input.starts_with('[') {
            return None;
        }
        let mut rest = &input[1..];
        let mut items = Vec::new();

        loop {
            rest = rest.trim_start();
            while rest.starts_with(',') {
                rest = rest[1..].trim_start();
            }
            if rest.starts_with(']') {
                rest = &rest[1..];
                break;
            }
            if rest.is_empty() {
                break;
            }

            let prev_len = rest.len();
            let (value, after) = Self::parse_value_inner(rest, depth + 1)?;
            // Defence-in-depth — see `parse_map`.
            if after.len() >= prev_len {
                return None;
            }
            items.push(value);
            rest = after;
        }

        Some((EventValue::Array(items), rest))
    }

    fn parse_set(input: &str, depth: usize) -> Option<(EventValue, &str)> {
        if !input.starts_with("#{") {
            return None;
        }
        let mut rest = &input[2..];
        let mut items = Vec::new();

        loop {
            rest = rest.trim_start();
            while rest.starts_with(',') {
                rest = rest[1..].trim_start();
            }
            if rest.starts_with('}') {
                rest = &rest[1..];
                break;
            }
            if rest.is_empty() {
                break;
            }

            let prev_len = rest.len();
            let (value, after) = Self::parse_value_inner(rest, depth + 1)?;
            // Defence-in-depth — see `parse_map`.
            if after.len() >= prev_len {
                return None;
            }
            items.push(value);
            rest = after;
        }

        Some((EventValue::Array(items), rest))
    }

    /// Format an EventValue as EDN.
    fn format_edn(value: &EventValue) -> String {
        match value {
            EventValue::String(s) => format!("\"{s}\""),
            EventValue::Integer(n) => n.to_string(),
            EventValue::Float(f) => f.to_string(),
            EventValue::Boolean(b) => b.to_string(),
            EventValue::Null => "nil".to_string(),
            EventValue::Array(arr) => {
                let items: Vec<String> = arr.iter().map(Self::format_edn).collect();
                format!("[{}]", items.join(" "))
            }
            EventValue::Object(obj) => {
                let items: Vec<String> = obj
                    .iter()
                    .map(|(k, v)| format!(":{} {}", k, Self::format_edn(v)))
                    .collect();
                format!("{{{}}}", items.join(", "))
            }
        }
    }
}

impl Codec for EdnCodec {
    fn name(&self) -> &'static str {
        "edn"
    }

    fn decode(&self, data: &[u8]) -> Result<Vec<Event>> {
        let text = String::from_utf8_lossy(data);
        let text = text.trim();

        if text.is_empty() {
            return Err(FerroStashError::Codec("empty EDN data".to_string()));
        }

        // Cheap pre-parse guard against container-open density DoS — see
        // `MAX_EDN_CONTAINER_OPENS_TOTAL` / `MAX_EDN_CONSECUTIVE_OPENS`
        // for the 2026-05-17 fuzz wave that motivates this check.
        check_edn_container_density(text)?;

        let (value, _) = Self::parse_value(text)
            .ok_or_else(|| FerroStashError::Codec("failed to parse EDN".to_string()))?;

        let event = match value {
            EventValue::Object(map) => {
                if let Some(ref target) = self.target {
                    let mut event = Event::empty();
                    event.set(target.clone(), EventValue::Object(map));
                    event
                } else {
                    let mut event = Event::empty();
                    for (k, v) in map {
                        event.set(k, v);
                    }
                    event
                }
            }
            other => {
                let mut event = Event::empty();
                if let Some(ref target) = self.target {
                    event.set(target.clone(), other);
                } else {
                    event.set_message(EventValue::to_string_lossy(&other));
                }
                event
            }
        };
        Ok(vec![event])
    }

    fn encode(&self, event: &Event) -> Result<Vec<u8>> {
        let mut map = IndexMap::new();
        map.insert(
            "@timestamp".to_string(),
            EventValue::String(event.timestamp.to_rfc3339()),
        );
        for (k, v) in event.fields() {
            map.insert(k.clone(), v.clone());
        }

        let edn = Self::format_edn(&EventValue::Object(map));
        Ok(format!("{edn}\n").into_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_edn_decode_map() {
        let codec = EdnCodec::default();
        let data = br#"{:message "hello" :count 42 :active true}"#;
        let event = codec
            .decode(data)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(event.message(), Some("hello"));
        assert_eq!(event.get("count"), Some(&EventValue::Integer(42)));
        assert_eq!(event.get("active"), Some(&EventValue::Boolean(true)));
    }

    #[test]
    fn test_edn_decode_nested() {
        let codec = EdnCodec::default();
        let data = br#"{:host "web01" :data {:cpu 0.75 :mem 1024}}"#;
        let event = codec
            .decode(data)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(event.get("host"), Some(&EventValue::String("web01".into())));
        assert!(event.has_field("data"));
    }

    #[test]
    fn test_edn_decode_vector() {
        let codec = EdnCodec::default();
        let data = b"[1 2 3]";
        let event = codec
            .decode(data)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        // Non-map value stored as message
        assert!(event.message().is_some());
    }

    #[test]
    fn test_edn_decode_nil() {
        let codec = EdnCodec::default();
        let data = br"{:value nil}";
        let event = codec
            .decode(data)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(event.get("value"), Some(&EventValue::Null));
    }

    #[test]
    fn test_edn_decode_with_target() {
        let codec = EdnCodec {
            target: Some("edn".to_string()),
        };
        let data = br#"{:key "val"}"#;
        let event = codec
            .decode(data)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert!(event.has_field("edn"));
    }

    #[test]
    fn test_edn_encode() {
        let codec = EdnCodec::default();
        let mut event = Event::empty();
        event.set("host", EventValue::String("web01".into()));
        event.set("count", EventValue::Integer(42));
        let bytes = codec.encode(&event).expect("encode");
        let text = String::from_utf8(bytes).expect("utf8");
        assert!(text.contains(":host \"web01\""));
        assert!(text.contains(":count 42"));
    }

    #[test]
    fn test_edn_empty() {
        let codec = EdnCodec::default();
        assert!(codec.decode(b"").is_err());
    }

    #[test]
    fn test_edn_name() {
        assert_eq!(EdnCodec::default().name(), "edn");
    }

    #[test]
    fn test_edn_set_parsing() {
        let codec = EdnCodec::default();
        let data = br"{:tags #{:a :b :c}}";
        let event = codec
            .decode(data)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert!(event.has_field("tags"));
    }

    #[test]
    fn test_edn_float() {
        let codec = EdnCodec::default();
        let data = br"{:pi 2.5}";
        let event = codec
            .decode(data)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(event.get("pi"), Some(&EventValue::Float(2.5)));
    }

    #[test]
    fn test_edn_escaped_string() {
        let codec = EdnCodec::default();
        let data = br#"{:msg "hello\nworld"}"#;
        let event = codec
            .decode(data)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(
            event.get("msg"),
            Some(&EventValue::String("hello\nworld".into()))
        );
    }

    #[test]
    fn test_edn_tagged_inst() {
        let codec = EdnCodec::default();
        let data = br#"{:created #inst "2026-04-12T00:00:00Z"}"#;
        let event = codec
            .decode(data)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(
            event.get("created"),
            Some(&EventValue::String("2026-04-12T00:00:00Z".into()))
        );
    }

    #[test]
    fn test_edn_tagged_uuid() {
        let codec = EdnCodec::default();
        let data = br#"{:id #uuid "550e8400-e29b-41d4-a716-446655440000"}"#;
        let event = codec
            .decode(data)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(
            event.get("id"),
            Some(&EventValue::String(
                "550e8400-e29b-41d4-a716-446655440000".into()
            ))
        );
    }

    #[test]
    fn test_edn_line_comments() {
        let codec = EdnCodec::default();
        let data = b"; this is a comment\n{:key \"value\"}";
        let event = codec
            .decode(data)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(event.get("key"), Some(&EventValue::String("value".into())));
    }

    #[test]
    fn test_edn_char_literal() {
        let codec = EdnCodec::default();
        let data = br"{:sep \tab}";
        let event = codec
            .decode(data)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(event.get("sep"), Some(&EventValue::String("\t".into())));
    }

    /// 2026-05-17 fuzz wave regression: 4 `codec_decode` artifacts
    /// (`timeout-032ed780/4096092e/7b3997ce/2c6eb739`) carried 29-101 `{`
    /// container-opens against 27-82 `}` closes, burning 18-30 s+ inside
    /// `EdnCodec::decode` even though `MAX_DEPTH=64` was never exceeded
    /// (effective nesting ≤ 17). New `check_edn_container_density` rejects
    /// them pre-parse.
    #[test]
    fn test_edn_dense_container_opens_rejected_2026_05_17() {
        let codec = EdnCodec::default();
        // 32 consecutive `{` (exceeds both total-cap=32 inclusive *and*
        // consecutive-cap=4).
        let attack: Vec<u8> = std::iter::repeat(b'{').take(32).collect();
        let err = codec.decode(&attack).expect_err("32 `{` must reject");
        let msg = format!("{err}");
        assert!(
            msg.contains("container-open"),
            "unexpected error: {msg}"
        );
    }

    /// Consecutive-run cap closes boundary-learning: even if total opens
    /// sit just below 32, a 5+ run of `{` is rejected on its own.
    #[test]
    fn test_edn_consecutive_open_run_rejected_2026_05_17() {
        let codec = EdnCodec::default();
        // 5 consecutive `{`, then split with spaces — total = 5 (well
        // under 32 total cap) but consecutive-run = 5 (over cap=4).
        let attack = b"{{{{{ a 1 } } } } }";
        let err = codec.decode(attack).expect_err("5-run `{` must reject");
        let msg = format!("{err}");
        assert!(
            msg.contains("run of") && msg.contains("container-open"),
            "unexpected error: {msg}"
        );
    }

    /// 4 consecutive `{` sits at the cap and must pass the density
    /// check (sqlparser/EDN parser itself may still fail on the
    /// malformed input — we assert only that the *density check*
    /// does not fire at exactly the cap).
    #[test]
    fn test_edn_consecutive_open_run_at_cap_passes_density_check() {
        let codec = EdnCodec::default();
        let at_cap = b"{{{{:a 1}}}}";
        match codec.decode(at_cap) {
            Ok(_) => {}
            Err(e) => {
                let msg = format!("{e}");
                assert!(
                    !msg.contains("container-open"),
                    "4-run must not trip density cap: {msg}"
                );
            }
        }
    }

    /// String literal content is skipped: `{` characters inside a
    /// `"..."` string do NOT count toward the cap.
    #[test]
    fn test_edn_container_density_skips_strings() {
        let codec = EdnCodec::default();
        // 50 `{` inside a string literal — must NOT trip the cap.
        let mut payload = String::from("{:notes \"");
        payload.push_str(&"{".repeat(50));
        payload.push_str("\"}");
        let event = codec
            .decode(payload.as_bytes())
            .expect("string-literal content must not trip cap")
            .into_iter()
            .next()
            .expect("no events");
        assert!(matches!(event.get("notes"), Some(EventValue::String(_))));
    }

    /// Realistic EDN config with several nested containers must continue
    /// to parse — well under both caps.
    #[test]
    fn test_edn_legitimate_nested_config_still_parses() {
        let codec = EdnCodec::default();
        let data = br#"{:filters [{:type "grok" :pattern "%{COMBINEDAPACHELOG}"}
                                   {:type "mutate" :fields {:src "ip" :dst "client_ip"}}]
                        :sink {:type "es" :host "localhost"}}"#;
        let event = codec
            .decode(data)
            .expect("legitimate nested EDN must parse")
            .into_iter()
            .next()
            .expect("no events");
        let _ = event;
    }

    /// Replay the 2026-05-17 artifact `timeout-2c6eb739` (the
    /// worst-case 60s+ timeout on local hardware). Must reject in O(N)
    /// wall-clock without invoking the recursive parser.
    #[test]
    fn test_edn_replay_2c6eb739_2026_05_17() {
        let codec = EdnCodec::default();
        // Synthetic shape matching the artifact: 63 `{` opens, 14
        // distinct chars, runs of 6 / 11 consecutive `{`.
        let attack: Vec<u8> = b"{{{{{{X\xff5\t{\x9f\n\xc4\xc4}{{{{{{{{{{{\
                                \xf5\x9f\n\xaf\xfd\x00\x00\x9f\n\xaf\xfd\
                                \x00\x00\x00\x00\x01{{k{{X5\t{\xff\x9f\n\
                                \xc4\xc4}{{{{{{".to_vec();
        let start = std::time::Instant::now();
        let err = codec
            .decode(&attack)
            .expect_err("dense `{` shape must reject");
        let elapsed = start.elapsed();
        let msg = format!("{err}");
        assert!(msg.contains("container-open"), "unexpected error: {msg}");
        assert!(
            elapsed.as_millis() < 10,
            "density check must reject in O(N) (took {:?})",
            elapsed
        );
    }

    /// 2026-05-19 fuzz wave regression: 1059-byte `codec_decode`
    /// artifact `timeout-b216efbb…` — same EDN brace-stew DoS class as
    /// the 2026-05-17 family but a larger payload (1059 B vs ≤313 B)
    /// embedding the `ruby` symbol marker plus longer runs of
    /// consecutive `{` and high-bit garbage (`\xff`/`\x99…`). Pre-fix
    /// (i.e. before `44f6190` landed `check_edn_container_density`),
    /// `cargo +nightly fuzz run codec_decode … -timeout=10` burned 11 s+
    /// inside `EdnCodec::decode`. The 2026-05-17 caps
    /// (`MAX_EDN_CONTAINER_OPENS_TOTAL=32` /
    /// `MAX_EDN_CONSECUTIVE_OPENS=4`) trip on the leading 5-`{` run
    /// inside the first ~6 bytes and reject in O(N) — this test pins
    /// the exact 1058-byte body (selector byte `\x60` stripped by the
    /// fuzz target before `decode` is called) so a future refactor of
    /// the density check cannot silently regress.
    #[test]
    fn test_edn_replay_b216efbb_brace_stew_2026_05_19() {
        let codec = EdnCodec::default();
        // The fuzz target consumes `data[0]` as the codec selector
        // (`0x60 % 21 = 12 = edn`) and passes `data[1..]` to `decode`.
        // Pin the post-selector body identically.
        let raw: &[u8] = include_bytes!(
            "../../../fuzz/corpus/codec_decode/\
             regression-edn-brace-stew-1059b-2026-05-19-b216efbb"
        );
        assert_eq!(raw.len(), 1059, "regression seed size drifted");
        assert_eq!(raw[0], 0x60, "regression seed selector byte drifted");
        let body = &raw[1..];

        let start = std::time::Instant::now();
        let err = codec
            .decode(body)
            .expect_err("1058-byte brace-stew body must reject");
        let elapsed = start.elapsed();
        let msg = format!("{err}");
        assert!(
            msg.contains("container-open"),
            "expected density-cap rejection, got: {msg}"
        );
        // Pre-fix: 11 s+ inside `parse_map` recursion. Post-fix: O(N)
        // byte walk + early reject. 50 ms is generous (~25× headroom
        // over local sub-ms observation) to avoid flakes on loaded CI.
        assert!(
            elapsed.as_millis() < 50,
            "density check must reject in O(N) (took {elapsed:?})"
        );
    }

    /// 2026-05-24 fuzz wave: `timeout-4425b914` (3354B) burned
    /// 10+ s of wall-clock inside `serde_json::Serializer::serialize_str`
    /// reached via `to_string_lossy(<inner Object>)` in `parse_map`'s
    /// key-stringification path. 23 nested `{` outside strings — under
    /// the 32-open cap, max run 3 — but each outer `parse_map` serialized
    /// a key that contained an already-serialized inner Object, growing
    /// the serialized output by a factor per nesting level. Same shape
    /// as the closed `parse_tagged` blow-up. Fix: reject the parse when
    /// a container appears in key position (EDN keys must be hashable
    /// atoms).
    #[test]
    fn test_edn_container_key_rejects_2026_05_24() {
        let codec = EdnCodec::default();
        // `{ { } : v }` — empty map as key. Should reject cleanly, not
        // serialize the inner map.
        let pathological = "{{} :v}";
        let result = codec.decode(pathological.as_bytes());
        assert!(
            result.is_err(),
            "container in EDN key position must reject (got {result:?})"
        );
        // Repro the wall-clock guard with a synthetic minimum of the
        // shape (depth-23 nested `{`). Pre-fix: exponential. Post-fix:
        // O(depth) reject inside parse_map.
        let nested = "{".repeat(23) + ":v}";
        let start = std::time::Instant::now();
        let result = codec.decode(nested.as_bytes());
        let elapsed = start.elapsed();
        assert!(result.is_err(), "deeply-nested key map must reject");
        assert!(
            elapsed.as_millis() < 50,
            "container-key reject must be O(depth) (took {elapsed:?})"
        );
        // Legitimate string key with map value still parses.
        let legit = r#"{"k" {:nested true}}"#;
        let ok = codec.decode(legit.as_bytes());
        assert!(ok.is_ok(), "string key with map value must parse (got {ok:?})");
    }
}
