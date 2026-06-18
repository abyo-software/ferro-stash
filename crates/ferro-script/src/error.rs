// SPDX-License-Identifier: Apache-2.0
//! Error types for the script engine.

#[derive(Debug, thiserror::Error)]
pub enum FerroError {
    #[error("script parse error: {0}")]
    QueryParseError(String),
    #[error("script runtime error: {0}")]
    Internal(String),
}

pub type FerroResult<T> = Result<T, FerroError>;
