// SPDX-License-Identifier: Apache-2.0
//! Regression: `AvroCodec::decode_ocf` block-data slicing must not panic
//! on an attacker-controlled signed `block_size` varint.
//!
//! The original sibling fix (`d1103a1`, "fix(codec,config): three
//! production DoS panics surfaced by 60s fuzz") cleaned up the
//! `*offset + len` overflow in `read_string` / `read_bytes` but missed
//! a third site in the same file — `decode_ocf` line ~186 computed:
//!
//! ```ignore
//! let end = (offset as i64 + size).min(data.len() as i64) as usize;
//! ```
//!
//! `size` is a zig-zag-decoded i64 that can be either large positive
//! (overflow) or negative (the `(small i64).min(positive i64) as usize`
//! chain wraps to ~`u64::MAX`, then `&data[offset..end]` panics with
//! `range end index 18446744062033484611 out of range for slice of
//! length 161`).
//!
//! Reproducer: 162-byte fuzz seed
//! `fuzz/artifacts/codec_decode/crash-31bdc0ac49d6dc07c6fe6d714aa748edb023d65b`.
//! The first byte (`0x2e`) is the multiplexed-target selector
//! (`0x2e % 21 == 4` → avro); the remaining 161 bytes are the body
//! handed directly to `AvroCodec::decode`.

use ferro_stash_codec::{avro::AvroCodec, Codec};

/// Verbatim 161-byte body extracted from
/// `fuzz/artifacts/codec_decode/crash-31bdc0ac49d6dc07c6fe6d714aa748edb023d65b`
/// (selector byte 0x2e stripped). Byte-for-byte reproducer; do not
/// reformat.
const CRASH_BODY: [u8; 161] = [
    0x4f, 0x62, 0x6a, 0x01, 0x30, 0x2e, 0x36, 0x36, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x95, 0x95,
    0x95, 0x95, 0x95, 0x95, 0x5d, 0x95, 0x95, 0x95, 0x97, 0x95, 0x95, 0x95, 0xff, 0x56, 0x3e, 0x00,
    0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0x07, 0x00, 0x00, 0x95, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x95, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff,
    0xff, 0xff, 0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x95, 0x95, 0x17, 0x95, 0x95, 0x95, 0xff, 0x56, 0xff, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00,
];

/// The original crash: pre-fix this body panicked with
/// `range end index 18446744062033484611 out of range for slice of
/// length 161` while computing `&data[offset..end]` in
/// `decode_ocf`. Post-fix it must return without panic — `Ok` (with
/// best-effort metadata) or `Err` are both acceptable; we are
/// exclusively guarding against an abort.
#[test]
fn avro_decode_ocf_block_size_underflow_does_not_panic() {
    let codec = AvroCodec::default();
    let _ = codec.decode(&CRASH_BODY);
}

/// Synthetic minimal trigger: a valid OCF magic + empty header + 16
/// sync bytes + a positive `block_count` varint + a `block_size`
/// varint that decodes to a *negative* i64 (zig-zag of an odd
/// pre-decoded value). Exercises the explicit `i64 → usize`
/// conversion guard rather than re-running the 161-byte corpus seed.
#[test]
fn avro_decode_ocf_negative_block_size_does_not_panic() {
    let codec = AvroCodec::default();
    let mut data = Vec::new();
    data.extend_from_slice(b"Obj\x01");
    // Empty header map: zero block count.
    data.push(0);
    // 16-byte sync marker.
    data.extend_from_slice(&[0u8; 16]);
    // Need >= 4 bytes remaining for the block read branch to fire.
    // block_count = zig-zag(2) = 0x04 (positive, harmless).
    data.push(0x04);
    // block_size = zig-zag-decoded large negative number. A 10-byte
    // varint with all-but-last bytes 0xFF and a final 0x7F decodes to
    // u64 ~MAX, then zig-zag → i64::MIN-ish: triggers the
    // `(offset as i64 + size)` underflow → wrapped `as usize`
    // → out-of-range slice.
    data.extend_from_slice(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f]);
    // A few trailing payload bytes so the slice attempt has data to
    // run past.
    data.extend_from_slice(&[0x00, 0x01, 0x02, 0x03]);

    let _ = codec.decode(&data);
}
