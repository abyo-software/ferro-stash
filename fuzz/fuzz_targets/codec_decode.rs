// SPDX-License-Identifier: Apache-2.0
//! Multiplexed fuzz target for every Logstash-compatible codec exposed by
//! `ferro-stash-codec::create_codec`.
//!
//! ROI: ferro-stash-codec hosts 22 hand-rolled binary/text decoders that
//! all sit at trust-boundaries — they parse bytes that arrive over TCP/UDP
//! from external Beats/Fluent/NetFlow/collectd agents (no Logstash agent
//! pre-validation) and from Elasticsearch _bulk NDJSON, S3 CloudTrail/
//! CloudFront log files, etc. Sibling repos (ferrostream/ferroconnect/
//! ferrosearch) have already yielded production DoS bugs in equivalent
//! wire decoders (`Vec::with_capacity(<varint>)` upfront alloc OOMs,
//! `unwrap_or(0)` swallowing varint errors → spin loops, recursion-cap
//! gaps, etc), so fuzzing the analogous parsers here is high-yield.
//!
//! Body layout: `[selector, body...]` — `selector % N` picks the codec.
//! Binary-protocol codecs (netflow, collectd, msgpack, fluent, avro,
//! protobuf) get the lion's share of slots since their hand-rolled TLV /
//! varint logic is the highest-risk surface.
//!
//! Watch points:
//! - panic / abort from any decoder (must return `Err`, never panic)
//! - OOM from `Vec::with_capacity(<attacker-controlled length>)`
//! - infinite loops where a parser fails to advance the cursor on
//!   malformed input
//! - integer overflow / arithmetic underflow on length math
//! - unbounded recursion in nested structures (avro/protobuf/edn)
//!
//! NOT covered here (out of scope for this target):
//! - `multiline` is stateful across calls; fuzzing it via single-shot
//!   `decode` only exercises the line-buffer cold path. A dedicated
//!   target with `Arbitrary`-driven repeated calls would be follow-up.

#![no_main]

use libfuzzer_sys::fuzz_target;

use ferro_stash_codec::create_codec;

// Order chosen so binary protocols (highest DoS surface) sit at low
// selector values — libFuzzer biases toward small-byte mutations early
// and a low selector hits more often per unit corpus volume.
const CODEC_NAMES: &[&str] = &[
    "netflow",     // 0 — NetFlow v5/v9/IPFIX, template-cache stateful
    "collectd",    // 1 — TLV binary protocol
    "msgpack",     // 2 — rmp-serde wrapper
    "fluent",      // 3 — MessagePack Forward Protocol
    "avro",        // 4 — OCF magic detection + JSON fallback
    "protobuf",    // 5 — hand-rolled wire decoder
    "cef",         // 6 — Common Event Format (SIEM)
    "es_bulk",     // 7 — NDJSON action+source pairs
    "graphite",    // 8 — line protocol
    "nmap",        // 9 — XML scan results
    "cloudtrail",  // 10 — JSON array of events
    "cloudfront",  // 11 — TSV access logs
    "edn",         // 12 — Extensible Data Notation
    "json",        // 13 — serde_json passthrough
    "json_lines",  // 14 — same Codec, different name
    "csv",         // 15 — csv crate wrapper
    "rubydebug",   // 16 — Ruby pp parser
    "plain",       // 17 — UTF-8 lossy
    "line",        // 18 — alias of plain
    "dots",        // 19 — throughput monitor codec
    "bytes",       // 20 — raw passthrough
];

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }
    let selector = data[0] as usize % CODEC_NAMES.len();
    let body = &data[1..];

    // Build with default settings — `from_config` itself is fuzzed
    // statistically through any codec that reads optional knobs from
    // `serde_json::Value::Null`. We're targeting the `decode` path.
    let Ok(codec) = create_codec(CODEC_NAMES[selector], &serde_json::Value::Null) else {
        return;
    };

    // The contract is: `decode` may return `Err` on malformed input but
    // must never panic, abort, or OOM. libFuzzer's sanitizer + 2GB rss
    // cap (configured by the runner) will catch all three.
    let _ = codec.decode(body);
});
