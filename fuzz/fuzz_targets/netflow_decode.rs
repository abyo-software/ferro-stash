// SPDX-License-Identifier: Apache-2.0
//! Focused fuzz target for the NetFlow codec (v5 / v9 / IPFIX).
//!
//! Companion to `codec_decode` — that target multiplexes 21 codecs
//! through `selector % 21`, so libFuzzer hits each branch ~5% of
//! corpus mutations. NetFlow is by far the most complex and the
//! highest-risk binary parser in `ferro-stash-codec`:
//!
//! - **External UDP wire**: NetFlow v5/v9/IPFIX is the standard
//!   flow-export protocol from routers/switches/firewalls. Anyone on
//!   the same L3 path as the collector can spoof datagrams; there is
//!   no Logstash-agent pre-validation.
//! - **Stateful template cache**: v9/IPFIX maintain a per-source
//!   template cache (`Arc<Mutex<HashMap<u16, Vec<TemplateField>>>>`)
//!   built from earlier-seen template records. A malicious template
//!   record persists across decode calls, so the wire format can
//!   plant a corrupt template once and exploit it on every
//!   subsequent data record. We exploit that by reusing the same
//!   `NetflowCodec` instance across many fuzz iterations via
//!   `OnceLock`.
//! - **TLV length math**: header counts × per-record sizes are the
//!   classical OOM-via-`Vec::with_capacity` shape the
//!   ferrostream/ferroconnect audits already turned up
//!   (find_coordinator / alter_partition_reassignments,
//!   2026-05-01 Session B).
//! - **Variable-length fields**: v9 templates declare per-field
//!   lengths that the data record must respect — an attacker
//!   declaring a 65535-byte field then sending a 4-byte payload is
//!   a likely panic shape.
//!
//! Watch points:
//! - panic on truncated header / template
//! - OOM from header `count` field × record size pre-allocation
//! - integer underflow on `length - header_size`
//! - infinite loop on zero-length template fields
//! - poisoned `Mutex` from a panic during template parse leaking
//!   into the next call

#![no_main]

use std::sync::OnceLock;

use libfuzzer_sys::fuzz_target;

use ferro_stash_codec::Codec;
use ferro_stash_codec::netflow::NetflowCodec;

/// Single shared codec instance so fuzz iterations share the
/// stateful template cache. This deliberately mirrors how the
/// production UDP listener reuses one codec across all incoming
/// datagrams from a given source — corrupted-template-then-data
/// attacks need exactly this state continuity to reproduce.
fn codec() -> &'static NetflowCodec {
    static CODEC: OnceLock<NetflowCodec> = OnceLock::new();
    CODEC.get_or_init(NetflowCodec::default)
}

fuzz_target!(|data: &[u8]| {
    // The first byte of a real NetFlow datagram is the high byte of
    // the version u16. Real values are 5/9/10. We pass the raw bytes
    // through unchanged so libFuzzer learns the magic.
    let _ = codec().decode(data);
});
