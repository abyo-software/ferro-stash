// SPDX-License-Identifier: Apache-2.0
//! Buffering strategies for pipeline stages.
//!
//! Provides backpressure-aware buffering between pipeline components.

use std::time::Duration;

use tokio::sync::mpsc;

use crate::event::Event;

/// Buffer configuration.
#[derive(Debug, Clone)]
pub struct BufferConfig {
    /// Maximum number of events in the buffer.
    pub max_events: usize,
    /// Maximum time to wait before flushing a batch.
    pub flush_interval: Duration,
    /// Batch size for output plugins.
    pub batch_size: usize,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            max_events: 10_000,
            flush_interval: Duration::from_secs(5),
            batch_size: 500,
        }
    }
}

/// A bounded event buffer with backpressure.
pub struct EventBuffer {
    sender: mpsc::Sender<Event>,
    receiver: mpsc::Receiver<Event>,
    config: BufferConfig,
}

impl EventBuffer {
    pub fn new(config: BufferConfig) -> Self {
        // `max_events` is an unvalidated `usize` from config (`pipeline.buffer_size`).
        // `tokio::sync::mpsc::channel` asserts `buffer > 0` and PANICS on a zero
        // capacity, so a `buffer_size: 0` config would panic at construction.
        // Clamp the divisor to >=1 (same zero-config class as the interval/modulo
        // clamps), preserving the requested capacity for all legitimate values.
        let (sender, receiver) = mpsc::channel(config.max_events.max(1));
        Self {
            sender,
            receiver,
            config,
        }
    }

    /// Returns a sender handle that can be cloned across tasks.
    pub fn sender(&self) -> mpsc::Sender<Event> {
        self.sender.clone()
    }

    /// Returns the receiver (can only be owned by one consumer).
    pub fn into_receiver(self) -> mpsc::Receiver<Event> {
        self.receiver
    }

    /// Split into sender and receiver.
    pub fn split(self) -> (mpsc::Sender<Event>, mpsc::Receiver<Event>) {
        (self.sender, self.receiver)
    }

    /// Returns the buffer configuration.
    pub fn config(&self) -> &BufferConfig {
        &self.config
    }
}

/// Collects events into batches based on size and timeout.
pub struct BatchCollector {
    batch: Vec<Event>,
    config: BufferConfig,
}

impl BatchCollector {
    pub fn new(config: BufferConfig) -> Self {
        Self {
            batch: Vec::with_capacity(config.batch_size),
            config,
        }
    }

    /// Adds an event. Returns `Some(batch)` if the batch is full.
    pub fn add(&mut self, event: Event) -> Option<Vec<Event>> {
        self.batch.push(event);
        if self.batch.len() >= self.config.batch_size {
            Some(self.flush())
        } else {
            None
        }
    }

    /// Flushes the current batch regardless of size.
    pub fn flush(&mut self) -> Vec<Event> {
        let mut batch = Vec::with_capacity(self.config.batch_size);
        std::mem::swap(&mut batch, &mut self.batch);
        batch
    }

    /// Returns true if the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.batch.is_empty()
    }

    /// Returns the number of events in the current batch.
    pub fn len(&self) -> usize {
        self.batch.len()
    }

    /// Returns the flush interval from the config.
    pub fn flush_interval(&self) -> Duration {
        self.config.flush_interval
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_collector() {
        let config = BufferConfig {
            batch_size: 3,
            ..Default::default()
        };
        let mut collector = BatchCollector::new(config);

        assert!(collector.add(Event::new("1")).is_none());
        assert!(collector.add(Event::new("2")).is_none());
        let batch = collector.add(Event::new("3"));
        assert!(batch.is_some());
        assert_eq!(batch.as_ref().map(Vec::len), Some(3));
        assert!(collector.is_empty());
    }

    #[test]
    fn test_batch_flush() {
        let config = BufferConfig {
            batch_size: 100,
            ..Default::default()
        };
        let mut collector = BatchCollector::new(config);
        collector.add(Event::new("1"));
        collector.add(Event::new("2"));
        let batch = collector.flush();
        assert_eq!(batch.len(), 2);
        assert!(collector.is_empty());
    }

    #[tokio::test]
    async fn test_event_buffer() {
        let config = BufferConfig {
            max_events: 100,
            ..Default::default()
        };
        let buffer = EventBuffer::new(config);
        let sender = buffer.sender();
        let mut receiver = buffer.into_receiver();

        sender.send(Event::new("hello")).await.ok();
        let event = receiver.recv().await;
        assert!(event.is_some());
        assert_eq!(event.as_ref().and_then(|e| e.message()), Some("hello"));
    }

    #[test]
    fn test_buffer_config_default() {
        let config = BufferConfig::default();
        assert_eq!(config.max_events, 10_000);
        assert_eq!(config.batch_size, 500);
        assert_eq!(config.flush_interval, Duration::from_secs(5));
    }

    #[test]
    fn test_batch_collector_len() {
        let config = BufferConfig {
            batch_size: 100,
            ..Default::default()
        };
        let mut collector = BatchCollector::new(config);
        assert_eq!(collector.len(), 0);
        assert!(collector.is_empty());
        collector.add(Event::new("1"));
        assert_eq!(collector.len(), 1);
        assert!(!collector.is_empty());
    }

    #[test]
    fn test_batch_collector_flush_interval() {
        let config = BufferConfig {
            flush_interval: Duration::from_secs(10),
            ..Default::default()
        };
        let collector = BatchCollector::new(config);
        assert_eq!(collector.flush_interval(), Duration::from_secs(10));
    }

    #[test]
    fn test_event_buffer_split() {
        let config = BufferConfig::default();
        let buffer = EventBuffer::new(config);
        let (sender, _receiver) = buffer.split();
        assert!(!sender.is_closed());
    }

    #[test]
    fn test_event_buffer_config() {
        let config = BufferConfig {
            max_events: 500,
            batch_size: 50,
            flush_interval: Duration::from_secs(1),
        };
        let buffer = EventBuffer::new(config);
        assert_eq!(buffer.config().max_events, 500);
        assert_eq!(buffer.config().batch_size, 50);
    }

    #[tokio::test]
    async fn test_event_buffer_zero_max_events_does_not_panic() {
        // A `pipeline.buffer_size: 0` config yields `max_events: 0`, which would
        // make `tokio::sync::mpsc::channel(0)` panic (it asserts buffer > 0).
        // The clamp must floor the capacity to 1 so construction succeeds.
        let config = BufferConfig {
            max_events: 0,
            ..Default::default()
        };
        // Must not panic at construction.
        let buffer = EventBuffer::new(config);
        let sender = buffer.sender();
        let mut receiver = buffer.into_receiver();

        // The clamped capacity-1 channel is fully functional.
        sender.send(Event::new("z")).await.ok();
        let event = receiver.recv().await;
        assert!(event.is_some());
        assert_eq!(event.as_ref().and_then(|e| e.message()), Some("z"));
    }

    #[test]
    fn test_batch_collector_multiple_batches() {
        let config = BufferConfig {
            batch_size: 2,
            ..Default::default()
        };
        let mut collector = BatchCollector::new(config);

        assert!(collector.add(Event::new("1")).is_none());
        let batch1 = collector.add(Event::new("2")).expect("batch1");
        assert_eq!(batch1.len(), 2);

        assert!(collector.add(Event::new("3")).is_none());
        let batch2 = collector.add(Event::new("4")).expect("batch2");
        assert_eq!(batch2.len(), 2);
    }
}
