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
