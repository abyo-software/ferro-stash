// SPDX-License-Identifier: Apache-2.0
//! Error types for `FerroStash`.

use thiserror::Error;

#[derive(Error, Debug)]
pub enum FerroStashError {
    #[error("configuration error: {0}")]
    Config(String),

    #[error("pipeline error: {0}")]
    Pipeline(String),

    #[error("input plugin error: {plugin}: {message}")]
    Input { plugin: String, message: String },

    #[error("filter plugin error: {plugin}: {message}")]
    Filter { plugin: String, message: String },

    #[error("output plugin error: {plugin}: {message}")]
    Output { plugin: String, message: String },

    #[error("codec error: {0}")]
    Codec(String),

    #[error("field reference error: {0}")]
    FieldRef(String),

    #[error("condition evaluation error: {0}")]
    Condition(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("shutdown requested")]
    Shutdown,
}

pub type Result<T> = std::result::Result<T, FerroStashError>;
