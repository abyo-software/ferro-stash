// SPDX-License-Identifier: Apache-2.0
//! Logstash-compatible monitoring HTTP API (port 9600).
//!
//! Endpoints:
//! - `GET /`                       — Root info (version, host, status)
//! - `GET /_node/stats`            — Pipeline statistics
//! - `GET /_node/stats/pipelines`  — Per-pipeline stats
//! - `GET /_node`                  — Node info
//! - `GET /_node/hot_threads`      — Hot threads (stub)

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tracing::{error, info};

use crate::metrics::PipelineMetrics;

/// Default monitoring API port (Logstash-compatible).
pub const DEFAULT_PORT: u16 = 9600;

/// Monitoring configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitoringConfig {
    /// Whether the monitoring API is enabled.
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Port to listen on.
    #[serde(default = "default_port")]
    pub port: u16,
    /// Bind address.
    #[serde(default = "default_bind")]
    pub bind: String,
}

fn default_enabled() -> bool {
    true
}
fn default_port() -> u16 {
    DEFAULT_PORT
}
fn default_bind() -> String {
    "0.0.0.0".to_string()
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            port: default_port(),
            bind: default_bind(),
        }
    }
}

/// Per-pipeline state tracked by the monitoring server.
#[derive(Debug, Clone)]
pub struct PipelineInfo {
    /// Pipeline ID.
    pub id: String,
    /// Number of pipeline workers.
    pub workers: usize,
    /// Batch size.
    pub batch_size: usize,
    /// Metrics handle.
    pub metrics: Arc<PipelineMetrics>,
    /// Input plugin names.
    pub input_plugins: Vec<String>,
    /// Filter plugin names.
    pub filter_plugins: Vec<String>,
    /// Output plugin names.
    pub output_plugins: Vec<String>,
}

/// Shared state for the monitoring server.
#[derive(Debug, Clone)]
pub struct MonitoringState {
    /// Instance-wide (cumulative) metrics.
    pub instance_metrics: Arc<PipelineMetrics>,
    /// Per-pipeline info.
    pub pipelines: Arc<std::sync::RwLock<HashMap<String, PipelineInfo>>>,
    /// Host name.
    pub hostname: String,
    /// Version string.
    pub version: String,
}

impl MonitoringState {
    /// Create a new monitoring state with the given instance metrics.
    pub fn new(instance_metrics: Arc<PipelineMetrics>) -> Self {
        let hostname = hostname::get()
            .ok()
            .and_then(|h| h.into_string().ok())
            .unwrap_or_else(|| "unknown".to_string());
        Self {
            instance_metrics,
            pipelines: Arc::new(std::sync::RwLock::new(HashMap::new())),
            hostname,
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    /// Register a pipeline for monitoring.
    pub fn register_pipeline(&self, info: PipelineInfo) {
        if let Ok(mut map) = self.pipelines.write() {
            map.insert(info.id.clone(), info);
        }
    }

    /// Remove a pipeline from monitoring.
    pub fn unregister_pipeline(&self, id: &str) {
        if let Ok(mut map) = self.pipelines.write() {
            map.remove(id);
        }
    }
}

/// The monitoring HTTP server.
pub struct MonitoringServer {
    config: MonitoringConfig,
    state: MonitoringState,
}

impl MonitoringServer {
    /// Create a new monitoring server.
    pub fn new(config: MonitoringConfig, state: MonitoringState) -> Self {
        Self { config, state }
    }

    /// Build the axum router.
    pub fn router(state: MonitoringState) -> Router {
        Router::new()
            .route("/", get(root_handler))
            .route("/_node", get(node_handler))
            .route("/_node/stats", get(node_stats_handler))
            .route("/_node/stats/pipelines", get(pipelines_stats_handler))
            .route("/_node/hot_threads", get(hot_threads_handler))
            .with_state(state)
    }

    /// Run the monitoring server until the provided shutdown future completes.
    pub async fn run(self, shutdown: crate::shutdown::ShutdownSignal) -> crate::error::Result<()> {
        if !self.config.enabled {
            info!("monitoring API disabled");
            return Ok(());
        }

        let addr: SocketAddr = format!("{}:{}", self.config.bind, self.config.port)
            .parse()
            .map_err(|e| {
                crate::error::FerroStashError::Config(format!("invalid monitoring address: {e}"))
            })?;

        let router = Self::router(self.state);

        info!(addr = %addr, "starting monitoring API");

        let listener = TcpListener::bind(addr).await.map_err(|e| {
            crate::error::FerroStashError::Config(format!("cannot bind monitoring port: {e}"))
        })?;

        let mut shutdown_signal = shutdown;
        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                shutdown_signal.wait().await;
            })
            .await
            .map_err(|e| {
                error!(error = %e, "monitoring server error");
                crate::error::FerroStashError::Config(format!("monitoring server error: {e}"))
            })?;

        info!("monitoring API stopped");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// `GET /` — Root info.
async fn root_handler(State(state): State<MonitoringState>) -> impl IntoResponse {
    let pipelines = state.pipelines.read().ok();
    let (workers, batch_size) = pipelines
        .as_ref()
        .and_then(|map| map.values().next())
        .map_or((num_cpus::get(), 500), |p| (p.workers, p.batch_size));

    let body = serde_json::json!({
        "host": state.hostname,
        "version": state.version,
        "status": "green",
        "pipeline": {
            "workers": workers,
            "batch_size": batch_size,
        }
    });

    (StatusCode::OK, axum::Json(body))
}

/// `GET /_node` — Node info.
async fn node_handler(State(state): State<MonitoringState>) -> impl IntoResponse {
    let pipelines = state.pipelines.read().ok();
    let (workers, batch_size) = pipelines
        .as_ref()
        .and_then(|map| map.values().next())
        .map_or((num_cpus::get(), 500), |p| (p.workers, p.batch_size));

    let body = serde_json::json!({
        "host": state.hostname,
        "version": state.version,
        "os": {
            "name": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
        },
        "pipeline": {
            "workers": workers,
            "batch_size": batch_size,
        }
    });

    (StatusCode::OK, axum::Json(body))
}

/// `GET /_node/stats` — Pipeline statistics (aggregate).
async fn node_stats_handler(State(state): State<MonitoringState>) -> impl IntoResponse {
    let snap = state.instance_metrics.snapshot();
    let pid = std::process::id();

    let plugins = build_plugins_json(&state);

    let body = serde_json::json!({
        "pipeline": {
            "events": {
                "in": snap.events_in,
                "filtered": snap.events_filtered,
                "out": snap.events_out,
                "duration_in_millis": snap.uptime_millis,
            },
            "plugins": plugins,
            "reloads": {
                "successes": snap.reload_successes,
                "failures": snap.reload_failures,
                "last_success_timestamp": snap.last_reload_success,
                "last_failure_timestamp": snap.last_reload_failure,
            }
        },
        "jvm": null,
        "process": {
            "pid": pid,
            "mem": {
                "total_virtual_in_bytes": 0,
            }
        },
        "events": {
            "in": snap.events_in,
            "filtered": snap.events_filtered,
            "out": snap.events_out,
        }
    });

    (StatusCode::OK, axum::Json(body))
}

/// `GET /_node/stats/pipelines` — Per-pipeline stats.
async fn pipelines_stats_handler(State(state): State<MonitoringState>) -> impl IntoResponse {
    let mut pipelines_json = serde_json::Map::new();

    if let Ok(map) = state.pipelines.read() {
        for (id, info) in map.iter() {
            let snap = info.metrics.snapshot();
            let pipeline_obj = serde_json::json!({
                "events": {
                    "in": snap.events_in,
                    "filtered": snap.events_filtered,
                    "out": snap.events_out,
                    "duration_in_millis": snap.uptime_millis,
                },
                "plugins": {
                    "inputs": info.input_plugins.iter()
                        .map(|n| serde_json::json!({"name": n}))
                        .collect::<Vec<_>>(),
                    "filters": info.filter_plugins.iter()
                        .map(|n| serde_json::json!({"name": n}))
                        .collect::<Vec<_>>(),
                    "outputs": info.output_plugins.iter()
                        .map(|n| serde_json::json!({"name": n}))
                        .collect::<Vec<_>>(),
                },
                "reloads": {
                    "successes": snap.reload_successes,
                    "failures": snap.reload_failures,
                },
                "queue": {
                    "type": "memory",
                }
            });
            pipelines_json.insert(id.clone(), pipeline_obj);
        }
    }

    let body = serde_json::json!({
        "pipelines": pipelines_json,
    });

    (StatusCode::OK, axum::Json(body))
}

/// `GET /_node/hot_threads` — Hot threads stub.
async fn hot_threads_handler(State(_state): State<MonitoringState>) -> impl IntoResponse {
    let pid = std::process::id();
    let body = serde_json::json!({
        "hot_threads": {
            "time": chrono::Utc::now().to_rfc3339(),
            "busiest_threads": 0,
            "threads": [],
        },
        "process": {
            "pid": pid,
        }
    });

    (StatusCode::OK, axum::Json(body))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn build_plugins_json(state: &MonitoringState) -> serde_json::Value {
    let mut inputs = Vec::new();
    let mut filters = Vec::new();
    let mut outputs = Vec::new();

    if let Ok(map) = state.pipelines.read() {
        for info in map.values() {
            for name in &info.input_plugins {
                inputs.push(serde_json::json!({"name": name}));
            }
            for name in &info.filter_plugins {
                filters.push(serde_json::json!({"name": name}));
            }
            for name in &info.output_plugins {
                outputs.push(serde_json::json!({"name": name}));
            }
        }
    }

    serde_json::json!({
        "inputs": inputs,
        "filters": filters,
        "outputs": outputs,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::util::ServiceExt;

    fn test_state() -> MonitoringState {
        let metrics = PipelineMetrics::new();
        metrics.record_in(100, 5000);
        metrics.record_filtered(95);
        metrics.record_out(95, 4750);

        let state = MonitoringState {
            instance_metrics: metrics,
            pipelines: Arc::new(std::sync::RwLock::new(HashMap::new())),
            hostname: "test-host".to_string(),
            version: "0.1.0".to_string(),
        };

        let info = PipelineInfo {
            id: "main".to_string(),
            workers: 4,
            batch_size: 500,
            metrics: PipelineMetrics::new(),
            input_plugins: vec!["stdin".to_string()],
            filter_plugins: vec!["grok".to_string(), "mutate".to_string()],
            output_plugins: vec!["elasticsearch".to_string()],
        };
        state.register_pipeline(info);
        state
    }

    async fn get_json(app: &Router, path: &str) -> serde_json::Value {
        let req = Request::builder()
            .uri(path)
            .body(Body::empty())
            .expect("build request");
        let resp = app.clone().oneshot(req).await.expect("response");
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1_000_000)
            .await
            .expect("read body");
        serde_json::from_slice(&body).expect("parse json")
    }

    #[tokio::test]
    async fn test_root_endpoint() {
        let state = test_state();
        let app = MonitoringServer::router(state);
        let json = get_json(&app, "/").await;

        assert_eq!(json["host"], "test-host");
        assert_eq!(json["version"], "0.1.0");
        assert_eq!(json["status"], "green");
        assert_eq!(json["pipeline"]["workers"], 4);
        assert_eq!(json["pipeline"]["batch_size"], 500);
    }

    #[tokio::test]
    async fn test_node_endpoint() {
        let state = test_state();
        let app = MonitoringServer::router(state);
        let json = get_json(&app, "/_node").await;

        assert_eq!(json["host"], "test-host");
        assert_eq!(json["version"], "0.1.0");
        assert!(!json["os"]["name"].is_null());
        assert!(!json["os"]["arch"].is_null());
        assert_eq!(json["pipeline"]["workers"], 4);
    }

    #[tokio::test]
    async fn test_node_stats_endpoint() {
        let state = test_state();
        let app = MonitoringServer::router(state);
        let json = get_json(&app, "/_node/stats").await;

        assert_eq!(json["pipeline"]["events"]["in"], 100);
        assert_eq!(json["pipeline"]["events"]["filtered"], 95);
        assert_eq!(json["pipeline"]["events"]["out"], 95);
        assert!(json["pipeline"]["events"]["duration_in_millis"].is_number());
        assert!(json["jvm"].is_null());
        assert!(json["process"]["pid"].is_number());
        assert_eq!(json["process"]["mem"]["total_virtual_in_bytes"], 0);

        // plugins
        let plugins = &json["pipeline"]["plugins"];
        assert!(plugins["inputs"].is_array());
        assert!(plugins["filters"].is_array());
        assert!(plugins["outputs"].is_array());

        // reloads
        assert_eq!(json["pipeline"]["reloads"]["successes"], 0);
        assert_eq!(json["pipeline"]["reloads"]["failures"], 0);

        // top-level events
        assert_eq!(json["events"]["in"], 100);
        assert_eq!(json["events"]["out"], 95);
    }

    #[tokio::test]
    async fn test_pipelines_stats_endpoint() {
        let state = test_state();
        // Add some per-pipeline metrics
        if let Ok(map) = state.pipelines.read() {
            if let Some(info) = map.get("main") {
                info.metrics.record_in(50, 2500);
                info.metrics.record_out(48, 2400);
                info.metrics.record_filtered(50);
            }
        }

        let app = MonitoringServer::router(state);
        let json = get_json(&app, "/_node/stats/pipelines").await;

        let main = &json["pipelines"]["main"];
        assert_eq!(main["events"]["in"], 50);
        assert_eq!(main["events"]["out"], 48);
        assert_eq!(main["events"]["filtered"], 50);

        let plugins = &main["plugins"];
        assert_eq!(plugins["inputs"][0]["name"], "stdin");
        assert_eq!(plugins["filters"][0]["name"], "grok");
        assert_eq!(plugins["filters"][1]["name"], "mutate");
        assert_eq!(plugins["outputs"][0]["name"], "elasticsearch");
    }

    #[tokio::test]
    async fn test_hot_threads_endpoint() {
        let state = test_state();
        let app = MonitoringServer::router(state);
        let json = get_json(&app, "/_node/hot_threads").await;

        assert!(json["hot_threads"]["time"].is_string());
        assert_eq!(json["hot_threads"]["busiest_threads"], 0);
        assert!(json["hot_threads"]["threads"].is_array());
        assert!(json["process"]["pid"].is_number());
    }

    #[tokio::test]
    async fn test_root_no_pipelines() {
        let metrics = PipelineMetrics::new();
        let state = MonitoringState {
            instance_metrics: metrics,
            pipelines: Arc::new(std::sync::RwLock::new(HashMap::new())),
            hostname: "empty-host".to_string(),
            version: "0.1.0".to_string(),
        };

        let app = MonitoringServer::router(state);
        let json = get_json(&app, "/").await;

        assert_eq!(json["host"], "empty-host");
        assert_eq!(json["status"], "green");
        // Falls back to defaults when no pipelines registered
        assert!(json["pipeline"]["workers"].is_number());
        assert_eq!(json["pipeline"]["batch_size"], 500);
    }

    #[tokio::test]
    async fn test_monitoring_state_new() {
        let metrics = PipelineMetrics::new();
        let state = MonitoringState::new(metrics);
        assert!(!state.hostname.is_empty());
        assert_eq!(state.version, env!("CARGO_PKG_VERSION"));
    }

    #[tokio::test]
    async fn test_register_unregister_pipeline() {
        let metrics = PipelineMetrics::new();
        let state = MonitoringState::new(metrics);

        let info = PipelineInfo {
            id: "test".to_string(),
            workers: 2,
            batch_size: 250,
            metrics: PipelineMetrics::new(),
            input_plugins: vec!["file".to_string()],
            filter_plugins: vec![],
            output_plugins: vec!["stdout".to_string()],
        };

        state.register_pipeline(info);
        {
            let map = state.pipelines.read().expect("read lock");
            assert!(map.contains_key("test"));
        }

        state.unregister_pipeline("test");
        {
            let map = state.pipelines.read().expect("read lock");
            assert!(!map.contains_key("test"));
        }
    }

    #[tokio::test]
    async fn test_multiple_pipelines_stats() {
        let metrics = PipelineMetrics::new();
        let state = MonitoringState::new(metrics);

        let info1 = PipelineInfo {
            id: "pipe-a".to_string(),
            workers: 2,
            batch_size: 100,
            metrics: PipelineMetrics::new(),
            input_plugins: vec!["kafka".to_string()],
            filter_plugins: vec!["json".to_string()],
            output_plugins: vec!["elasticsearch".to_string()],
        };
        let info2 = PipelineInfo {
            id: "pipe-b".to_string(),
            workers: 1,
            batch_size: 50,
            metrics: PipelineMetrics::new(),
            input_plugins: vec!["tcp".to_string()],
            filter_plugins: vec![],
            output_plugins: vec!["file".to_string()],
        };

        state.register_pipeline(info1);
        state.register_pipeline(info2);

        let app = MonitoringServer::router(state);
        let json = get_json(&app, "/_node/stats/pipelines").await;

        assert!(json["pipelines"]["pipe-a"].is_object());
        assert!(json["pipelines"]["pipe-b"].is_object());
        assert_eq!(
            json["pipelines"]["pipe-b"]["plugins"]["inputs"][0]["name"],
            "tcp"
        );
    }

    #[tokio::test]
    async fn test_monitoring_config_default() {
        let config = MonitoringConfig::default();
        assert!(config.enabled);
        assert_eq!(config.port, 9600);
        assert_eq!(config.bind, "0.0.0.0");
    }

    #[tokio::test]
    async fn test_monitoring_config_deserialize() {
        let yaml = r"
enabled: false
port: 9601
bind: 127.0.0.1
";
        let config: MonitoringConfig = serde_yaml::from_str(yaml).expect("parse");
        assert!(!config.enabled);
        assert_eq!(config.port, 9601);
        assert_eq!(config.bind, "127.0.0.1");
    }

    #[tokio::test]
    async fn test_monitoring_server_disabled() {
        let config = MonitoringConfig {
            enabled: false,
            port: 0,
            bind: "127.0.0.1".to_string(),
        };
        let state = MonitoringState::new(PipelineMetrics::new());
        let server = MonitoringServer::new(config, state);
        let shutdown = crate::shutdown::ShutdownController::new();
        // Should return immediately when disabled
        let result = server.run(shutdown.1).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_monitoring_server_disabled_returns_ok() {
        let config = MonitoringConfig {
            enabled: false,
            port: 0,
            bind: "127.0.0.1".to_string(),
        };
        let state = MonitoringState::new(PipelineMetrics::new());
        let server = MonitoringServer::new(config, state);
        let (_, signal) = crate::shutdown::ShutdownController::new();
        let result = server.run(signal).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_monitoring_server_bind_and_shutdown() {
        let state = MonitoringState::new(PipelineMetrics::new());
        let info = PipelineInfo {
            id: "main".to_string(),
            workers: 4,
            batch_size: 500,
            metrics: PipelineMetrics::new(),
            input_plugins: vec!["stdin".to_string()],
            filter_plugins: vec![],
            output_plugins: vec!["stdout".to_string()],
        };
        state.register_pipeline(info);

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let _addr = listener.local_addr().expect("local addr");
        let router = MonitoringServer::router(state);
        let (controller, signal) = crate::shutdown::ShutdownController::new();

        let handle = tokio::spawn(async move {
            let mut shutdown_signal = signal;
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    shutdown_signal.wait().await;
                })
                .await
        });

        // Shutdown immediately — verifies the server starts and stops cleanly
        controller.shutdown();
        let result = handle.await.expect("join");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_stats_aggregate_plugins() {
        let metrics = PipelineMetrics::new();
        let state = MonitoringState::new(metrics);

        // Register two pipelines with overlapping plugin names
        let info1 = PipelineInfo {
            id: "a".to_string(),
            workers: 2,
            batch_size: 100,
            metrics: PipelineMetrics::new(),
            input_plugins: vec!["beats".to_string()],
            filter_plugins: vec!["grok".to_string()],
            output_plugins: vec!["elasticsearch".to_string()],
        };
        let info2 = PipelineInfo {
            id: "b".to_string(),
            workers: 1,
            batch_size: 50,
            metrics: PipelineMetrics::new(),
            input_plugins: vec!["tcp".to_string()],
            filter_plugins: vec![],
            output_plugins: vec!["file".to_string()],
        };
        state.register_pipeline(info1);
        state.register_pipeline(info2);

        let app = MonitoringServer::router(state);
        let json = get_json(&app, "/_node/stats").await;

        let plugins = &json["pipeline"]["plugins"];
        let inputs = plugins["inputs"].as_array().expect("inputs array");
        let outputs = plugins["outputs"].as_array().expect("outputs array");
        // Two pipelines = two input plugins, two output plugins
        assert_eq!(inputs.len(), 2);
        assert_eq!(outputs.len(), 2);
    }
}
