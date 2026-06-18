// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 abyo software 合同会社 (abyo software LLC)
//
//! Ruby interpreter bridge for `FerroStash`.
//!
//! Embeds the Artichoke (mruby-based) Ruby interpreter to provide full
//! Logstash-compatible Ruby filter execution. The interpreter is pre-loaded
//! with a `LogStash::Event` class that mirrors the real Logstash event API.

mod event_bridge;
mod runtime;

pub use runtime::{RubyRuntime, RubyRuntimeError};
