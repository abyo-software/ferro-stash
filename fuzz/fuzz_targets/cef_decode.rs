// SPDX-License-Identifier: Apache-2.0
//! Focused fuzz target for the CEF (Common Event Format) codec.
//!
//! Companion to `codec_decode` — that target multiplexes 21 codecs
//! through `selector % 21`, so libFuzzer hits each branch ~5% of
//! corpus mutations. CEF is an ArcSight/SIEM standard log format
//! routinely received over UDP/TCP/syslog from third-party security
//! appliances; the parser's surface is wide enough to deserve its
//! own focused target:
//!
//! - **External wire**: CEF arrives via syslog/UDP from firewalls,
//!   IDS appliances, EDR agents, and other third-party gear with no
//!   pre-validation step.
//! - **Pipe-escape state machine** (`cef.rs:172-189`): hand-rolled
//!   `peekable` char loop that accumulates `\\<ch>` into a
//!   `current` String and switches on `|` only when fewer than 7
//!   header fields have been consumed. The loop is the highest-risk
//!   surface — sibling EDN parser (`edn.rs`) had an analogous
//!   structural-tag hot-loop bug fixed at `ok6934553`.
//! - **Extension key=value parser** (`cef.rs:99-140`): splits the
//!   trailing extension string on every `=`, then guesses key
//!   boundaries by `rfind(' ')`. Adversarial inputs can drive the
//!   `rfind` into pathological splits or empty-key states.
//! - **`unescape_field`** (`cef.rs:143-145`): two passes of
//!   `String::replace` on attacker-controlled bytes; not obviously
//!   panic-prone but worth fuzzing for memory growth on
//!   `\|\\\|\\\\` rich inputs.
//!
//! Watch points:
//! - panic / abort from any code path (must return `Err`)
//! - OOM / runaway allocation from the field-accumulator `String`
//! - infinite loops in the extension parser when no `=` separates
//!   tokens or every token is `=`
//! - char-boundary panics on multibyte UTF-8 in `text[..cef_start]`
//!   slice (line 163) — `cef_start` is a byte index from `find`,
//!   but the UTF-8-lossy upstream means the boundary is safe; still
//!   worth fuzzing
//! - integer overflow on header field index when fields >> 7
//!
//! NOT covered here (out of scope):
//! - `encode` direction — separate target if needed.
//! - `from_config` settings parsing — `serde_json::Value::Null` is
//!   used to avoid bias.

#![no_main]

use libfuzzer_sys::fuzz_target;

use ferro_stash_codec::Codec;
use ferro_stash_codec::cef::CefCodec;

fuzz_target!(|data: &[u8]| {
    // Stateless decoder, but instantiate per-iteration to mirror the
    // multiplex target's contract (CefCodec::default is cheap — no
    // shared mutex / template cache). If this changes, hoist into a
    // OnceLock the way netflow_decode does.
    let codec = CefCodec::default();
    let _ = codec.decode(data);
});
