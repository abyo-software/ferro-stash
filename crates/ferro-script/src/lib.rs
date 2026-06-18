// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024 FerroSearch Authors

// The workspace denies `unsafe_code` by default. This crate opts in for a
// single line: the Cranelift JIT trampoline (`jit.rs:156`) transmutes the
// finalized function pointer to a typed `fn`. This is the standard and
// unavoidable pattern for calling Cranelift-emitted code. The SAFETY
// invariant is that the function signature we compile against matches
// `ScriptScoreFn`, which is enforced at the builder level above.
#![allow(unsafe_code)]

//! Painless-compatible scripting engine for `FerroSearch`.
//!
//! Provides a subset of the Elasticsearch Painless scripting language,
//! including field access, arithmetic, string methods, and control flow.

pub mod error;
pub mod evaluator;
pub mod jit;
pub mod parser;
pub mod types;

pub use error::{FerroError, FerroResult};
pub use evaluator::{ScriptContext, evaluate, set_nested_value};
