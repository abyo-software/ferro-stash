// SPDX-License-Identifier: Apache-2.0
//! Metrics filter — generate metric events from pipeline events.
//!
//! Tracks meters (counts) and timers for specified fields, and enriches events
//! with metric data. In Logstash, this filter periodically flushes accumulated
//! metrics as separate events; here we attach metric metadata to each event
//! for simplicity while maintaining the same configuration interface.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::FilterPlugin;
use indexmap::IndexMap;

/// Internal state for accumulated metrics.
#[derive(Debug, Default)]
struct MetricsState {
    /// Meter counters: field_name -> count.
    meters: HashMap<String, u64>,
    /// Timer accumulators: field_name -> list of durations.
    timers: HashMap<String, Vec<f64>>,
    /// Total events seen.
    event_count: u64,
}

#[derive(Debug)]
pub struct MetricsFilter {
    /// Field names to meter (count occurrences).
    meter: Vec<String>,
    /// Field names to time (accumulate numeric values as durations).
    timer: Vec<String>,
    /// Flush interval in seconds (for future periodic flush support).
    #[allow(dead_code)]
    flush_interval: u64,
    /// Rates to calculate (e.g., 1, 5, 15 minute rates).
    #[allow(dead_code)]
    rates: Vec<u64>,
    /// Percentiles to calculate for timers.
    percentiles: Vec<f64>,
    /// Shared metrics state.
    state: Arc<Mutex<MetricsState>>,
    condition: Option<Condition>,
}

impl MetricsFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let meter = settings
            .get("meter")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let timer = settings
            .get("timer")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let flush_interval = settings
            .get("flush_interval")
            .and_then(|v| v.as_u64())
            .unwrap_or(5);

        let rates = settings
            .get("rates")
            .and_then(|v| v.as_array())
            .map_or_else(
                || vec![1, 5, 15],
                |a| a.iter().filter_map(|v| v.as_u64()).collect(),
            );

        let percentiles = settings
            .get("percentiles")
            .and_then(|v| v.as_array())
            .map_or_else(
                || vec![1.0, 5.0, 25.0, 50.0, 75.0, 95.0, 99.0],
                |a| a.iter().filter_map(|v| v.as_f64()).collect(),
            );

        Ok(Self {
            meter,
            timer,
            flush_interval,
            rates,
            percentiles,
            state: Arc::new(Mutex::new(MetricsState::default())),
            condition,
        })
    }

    /// Calculate the percentile of a sorted slice.
    fn percentile(sorted: &[f64], p: f64) -> f64 {
        if sorted.is_empty() {
            return 0.0;
        }
        if sorted.len() == 1 {
            return sorted[0];
        }
        let rank = (p / 100.0) * (sorted.len() - 1) as f64;
        let lower = rank.floor() as usize;
        let upper = rank.ceil() as usize;
        if lower == upper || upper >= sorted.len() {
            sorted[lower.min(sorted.len() - 1)]
        } else {
            let frac = rank - lower as f64;
            sorted[lower] * (1.0 - frac) + sorted[upper] * frac
        }
    }
}

#[async_trait]
impl FilterPlugin for MetricsFilter {
    fn name(&self) -> &'static str {
        "metrics"
    }

    async fn filter(&self, mut event: Event) -> Result<Vec<Event>> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.event_count += 1;

        // Update meters
        for field in &self.meter {
            if event.has_field(field) {
                let counter = state.meters.entry(field.clone()).or_insert(0);
                *counter += 1;
            }
        }

        // Update timers
        for field in &self.timer {
            if let Some(val) = event.get(field) {
                let duration = match val {
                    EventValue::Float(f) => Some(*f),
                    EventValue::Integer(i) => Some(*i as f64),
                    EventValue::String(s) => s.parse::<f64>().ok(),
                    _ => None,
                };
                if let Some(d) = duration {
                    state.timers.entry(field.clone()).or_default().push(d);
                }
            }
        }

        // Attach current metrics summary to event
        let mut metrics = IndexMap::new();
        metrics.insert(
            "event_count".to_string(),
            EventValue::Integer(state.event_count as i64),
        );

        // Meter counts
        if !self.meter.is_empty() {
            let mut meter_map = IndexMap::new();
            for field in &self.meter {
                let count = state.meters.get(field).copied().unwrap_or(0);
                meter_map.insert(field.clone(), EventValue::Integer(count as i64));
            }
            metrics.insert("meters".to_string(), EventValue::Object(meter_map));
        }

        // Timer statistics
        if !self.timer.is_empty() {
            let mut timer_map = IndexMap::new();
            for field in &self.timer {
                if let Some(values) = state.timers.get(field) {
                    let mut sorted = values.clone();
                    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

                    let mut stats = IndexMap::new();
                    stats.insert(
                        "count".to_string(),
                        EventValue::Integer(sorted.len() as i64),
                    );
                    if !sorted.is_empty() {
                        let sum: f64 = sorted.iter().sum();
                        stats.insert("min".to_string(), EventValue::Float(sorted[0]));
                        stats.insert(
                            "max".to_string(),
                            EventValue::Float(sorted[sorted.len() - 1]),
                        );
                        stats.insert(
                            "mean".to_string(),
                            EventValue::Float(sum / sorted.len() as f64),
                        );

                        // Percentiles
                        let mut pct_map = IndexMap::new();
                        for p in &self.percentiles {
                            let val = Self::percentile(&sorted, *p);
                            pct_map.insert(format!("p{p}"), EventValue::Float(val));
                        }
                        stats.insert("percentiles".to_string(), EventValue::Object(pct_map));
                    }
                    timer_map.insert(field.clone(), EventValue::Object(stats));
                }
            }
            metrics.insert("timers".to_string(), EventValue::Object(timer_map));
        }

        event.set("_metrics", EventValue::Object(metrics));

        Ok(vec![event])
    }

    fn condition(&self) -> Option<&Condition> {
        self.condition.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_metrics_meter() {
        let settings = serde_json::json!({
            "meter": ["status"]
        });
        let filter = MetricsFilter::from_config(&settings, None).expect("config");

        let mut event1 = Event::new("test");
        event1.set("status", EventValue::String("200".into()));
        let r1 = filter.filter(event1).await.expect("filter");
        let m1 = r1[0].get("_metrics").expect("metrics");
        let meters = m1.as_object().expect("obj").get("meters").expect("meters");
        let status_count = meters
            .as_object()
            .expect("obj")
            .get("status")
            .expect("status");
        assert_eq!(status_count, &EventValue::Integer(1));

        let mut event2 = Event::new("test2");
        event2.set("status", EventValue::String("200".into()));
        let r2 = filter.filter(event2).await.expect("filter");
        let m2 = r2[0].get("_metrics").expect("metrics");
        let meters2 = m2.as_object().expect("obj").get("meters").expect("meters");
        let status_count2 = meters2
            .as_object()
            .expect("obj")
            .get("status")
            .expect("status");
        assert_eq!(status_count2, &EventValue::Integer(2));
    }

    #[tokio::test]
    async fn test_metrics_timer() {
        let settings = serde_json::json!({
            "timer": ["duration"],
            "percentiles": [50.0, 99.0]
        });
        let filter = MetricsFilter::from_config(&settings, None).expect("config");

        for val in &[1.0, 2.0, 3.0, 4.0, 5.0] {
            let mut event = Event::new("test");
            event.set("duration", EventValue::Float(*val));
            let _r = filter.filter(event).await.expect("filter");
        }

        // Check the last event's metrics
        let mut event = Event::new("test");
        event.set("duration", EventValue::Float(6.0));
        let result = filter.filter(event).await.expect("filter");
        let metrics = result[0].get("_metrics").expect("metrics");
        let timers = metrics
            .as_object()
            .expect("obj")
            .get("timers")
            .expect("timers");
        let dur = timers
            .as_object()
            .expect("obj")
            .get("duration")
            .expect("duration");
        let dur_obj = dur.as_object().expect("obj");
        assert_eq!(dur_obj.get("count"), Some(&EventValue::Integer(6)));
        assert_eq!(dur_obj.get("min"), Some(&EventValue::Float(1.0)));
        assert_eq!(dur_obj.get("max"), Some(&EventValue::Float(6.0)));
    }

    #[tokio::test]
    async fn test_metrics_event_count() {
        let settings = serde_json::json!({});
        let filter = MetricsFilter::from_config(&settings, None).expect("config");

        let event1 = Event::new("a");
        let r1 = filter.filter(event1).await.expect("filter");
        let m1 = r1[0].get("_metrics").expect("metrics");
        assert_eq!(
            m1.as_object().expect("obj").get("event_count"),
            Some(&EventValue::Integer(1))
        );

        let event2 = Event::new("b");
        let r2 = filter.filter(event2).await.expect("filter");
        let m2 = r2[0].get("_metrics").expect("metrics");
        assert_eq!(
            m2.as_object().expect("obj").get("event_count"),
            Some(&EventValue::Integer(2))
        );
    }

    #[test]
    fn test_metrics_percentile_calc() {
        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let p50 = MetricsFilter::percentile(&data, 50.0);
        assert!((p50 - 3.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_metrics_name() {
        let settings = serde_json::json!({});
        let filter = MetricsFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.name(), "metrics");
    }

    #[tokio::test]
    async fn test_metrics_no_matching_fields() {
        let settings = serde_json::json!({
            "meter": ["nonexistent"],
            "timer": ["also_nonexistent"]
        });
        let filter = MetricsFilter::from_config(&settings, None).expect("config");
        let event = Event::new("test");
        let result = filter.filter(event).await.expect("filter");
        // Should still have _metrics with event_count
        assert!(result[0].has_field("_metrics"));
    }
}
