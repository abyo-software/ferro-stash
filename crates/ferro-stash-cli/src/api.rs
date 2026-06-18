// SPDX-License-Identifier: Apache-2.0
//! Monitoring API — Logstash-compatible HTTP endpoints.
//!
//! Implements the full Logstash monitoring API surface:
//!   GET /                              — instance info + id
//!   GET /_node/stats                   — node statistics
//!   GET /_node/stats/pipelines         — all pipeline stats
//!   GET /_node/stats/pipelines/:id     — single pipeline stats
//!   GET /_node/hot_threads             — thread info
//!   GET /_node/pipelines               — alias for stats
//!   GET /_node/plugins                 — installed plugins
//!   GET /_node/logging                 — current log levels
//!   PUT /_node/logging                 — update log levels
//!   PUT /_node/logging/reset           — reset log levels
//!   GET /_health_report                — health report

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, put};
use axum::Router;
use ferro_stash_core::metrics::PipelineMetrics;
use ferro_stash_core::shutdown::ShutdownSignal;
use tracing::info;

/// Registered plugin info for API reporting.
#[derive(Clone, Debug, serde::Serialize)]
pub struct PluginInfo {
    pub id: String,
    pub name: String,
    pub plugin_type: String, // "input", "filter", "output"
}

/// Shared handle for the currently-running pipeline's per-pipeline metrics.
/// Updated on config reload (reset).
pub type PipelineMetricsHandle = Arc<RwLock<Arc<PipelineMetrics>>>;

/// Shared handle for currently-loaded plugin info (updated on reload).
pub type PluginsHandle = Arc<RwLock<Vec<PluginInfo>>>;

#[derive(Clone)]
struct ApiState {
    /// Instance-wide (cumulative) metrics — survive reloads.
    metrics: Arc<PipelineMetrics>,
    /// Per-pipeline metrics — reset on reload.
    pipeline_metrics: PipelineMetricsHandle,
    instance_id: String,
    pipeline_id: String,
    plugins: PluginsHandle,
    /// Runtime logger levels (PUT /_node/logging persists here).
    logger_levels: Arc<RwLock<HashMap<String, String>>>,
}

fn default_logger_levels() -> HashMap<String, String> {
    [
        ("logstash.agent", "INFO"),
        ("logstash.pipeline", "INFO"),
        ("logstash.outputs.elasticsearch", "INFO"),
        ("logstash.inputs.stdin", "INFO"),
        ("slowlog.logstash.pipeline", "TRACE"),
    ]
    .iter()
    .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
    .collect()
}

#[allow(dead_code)]
pub async fn run_api_server(
    bind_addr: &str,
    metrics: Arc<PipelineMetrics>,
    shutdown: ShutdownSignal,
    instance_id: String,
    pipeline_id: String,
) -> anyhow::Result<()> {
    let pipeline_metrics = Arc::new(RwLock::new(Arc::clone(&metrics)));
    let plugins_handle: PluginsHandle = Arc::new(RwLock::new(Vec::new()));
    run_api_server_full(
        bind_addr,
        metrics,
        pipeline_metrics,
        plugins_handle,
        shutdown,
        instance_id,
        pipeline_id,
    )
    .await
}

#[allow(dead_code)]
pub async fn run_api_server_with_plugins(
    bind_addr: &str,
    metrics: Arc<PipelineMetrics>,
    shutdown: ShutdownSignal,
    instance_id: String,
    pipeline_id: String,
    plugins: Vec<PluginInfo>,
) -> anyhow::Result<()> {
    let pipeline_metrics = Arc::new(RwLock::new(Arc::clone(&metrics)));
    let plugins_handle: PluginsHandle = Arc::new(RwLock::new(plugins));
    run_api_server_full(
        bind_addr,
        metrics,
        pipeline_metrics,
        plugins_handle,
        shutdown,
        instance_id,
        pipeline_id,
    )
    .await
}

/// Full API server with separate Instance metrics and Pipeline metrics handle.
/// `pipeline_metrics_handle` allows the pipeline metrics to be swapped on reload.
pub async fn run_api_server_full(
    bind_addr: &str,
    metrics: Arc<PipelineMetrics>,
    pipeline_metrics_handle: PipelineMetricsHandle,
    plugins_handle: PluginsHandle,
    mut shutdown: ShutdownSignal,
    instance_id: String,
    pipeline_id: String,
) -> anyhow::Result<()> {
    let state = ApiState {
        metrics,
        pipeline_metrics: pipeline_metrics_handle,
        instance_id,
        pipeline_id,
        plugins: plugins_handle,
        logger_levels: Arc::new(RwLock::new(default_logger_levels())),
    };

    let app = Router::new()
        .route("/", get(root))
        .route("/_node", get(node_info))
        .route("/_node/stats", get(node_stats))
        .route("/_node/stats/pipelines", get(pipeline_stats_all))
        .route("/_node/stats/pipelines/:id", get(pipeline_stats_by_id))
        .route("/_node/hot_threads", get(hot_threads))
        .route("/_node/pipelines", get(node_stats))
        .route("/_node/plugins", get(node_plugins))
        .route("/_node/logging", get(get_logging).put(put_logging))
        .route("/_node/logging/reset", put(reset_logging))
        .route("/_health_report", get(health_report))
        .with_state(state);

    // Try the requested port, then auto-increment up to +9 (Logstash behavior)
    let listener = try_bind_with_increment(bind_addr, 10).await?;

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown.wait().await;
        })
        .await?;

    Ok(())
}

/// Try to bind to the given address; if it fails, increment the port up to `max_tries` times.
/// This matches Logstash behavior where multiple instances auto-select ports 9600, 9601, etc.
async fn try_bind_with_increment(
    bind_addr: &str,
    max_tries: u16,
) -> anyhow::Result<tokio::net::TcpListener> {
    let addr: SocketAddr = bind_addr
        .parse()
        .unwrap_or_else(|_| SocketAddr::from(([127, 0, 0, 1], 9600)));

    let base_port = addr.port();

    for offset in 0..max_tries {
        let port = base_port + offset;
        let try_addr = SocketAddr::from((addr.ip(), port));

        // Use SO_REUSEADDR so the port can be reused immediately after process exit
        let socket = match socket2::Socket::new(
            socket2::Domain::for_address(try_addr),
            socket2::Type::STREAM,
            Some(socket2::Protocol::TCP),
        ) {
            Ok(s) => s,
            Err(e) if offset + 1 < max_tries => {
                tracing::debug!(port, error = %e, "socket creation failed, trying next");
                continue;
            }
            Err(e) => return Err(anyhow::anyhow!("socket creation failed: {e}")),
        };

        let _ = socket.set_reuse_address(true);
        socket.set_nonblocking(true).ok();

        if let Err(e) = socket.bind(&try_addr.into()) {
            if offset + 1 < max_tries {
                tracing::debug!(port, error = %e, "port in use, trying next");
                continue;
            }
            return Err(anyhow::anyhow!(
                "failed to bind monitoring API after {max_tries} attempts \
                 (ports {base_port}-{}): {e}",
                base_port + max_tries - 1
            ));
        }

        if let Err(e) = socket.listen(128) {
            if offset + 1 < max_tries {
                continue;
            }
            return Err(anyhow::anyhow!("listen failed: {e}"));
        }

        let std_listener: std::net::TcpListener = socket.into();
        let listener = tokio::net::TcpListener::from_std(std_listener)?;
        info!(address = %try_addr, "monitoring API listening");
        return Ok(listener);
    }

    unreachable!()
}

// GET /
async fn root(State(state): State<ApiState>) -> impl IntoResponse {
    let body = serde_json::json!({
        "host": hostname::get()
            .map_or_else(|_| "unknown".to_string(), |h| h.to_string_lossy().to_string()),
        "version": super::LOGSTASH_COMPAT_VERSION,
        "http_address": "127.0.0.1:9600",
        "id": state.instance_id,
        "name": hostname::get()
            .map_or_else(|_| "unknown".to_string(), |h| h.to_string_lossy().to_string()),
        "ephemeral_id": uuid::Uuid::new_v4().to_string(),
        "status": "green",
        "snapshot": false,
        "pipeline": {
            "workers": num_cpus::get(),
            "batch_size": 500,
            "batch_delay": 5000,
        },
        "jvm": {
            "pid": std::process::id(),
            "version": "21.0.0",
            "vm_vendor": "FerroStash (Rust native)",
            "mem": {
                "heap_used_in_bytes": 0,
                "heap_max_in_bytes": 0,
            }
        },
        "build_date": "2026-01-01T00:00:00Z",
        "build_sha": "ferrostash",
        "build_snapshot": false,
    });
    // Logstash returns 200 with content-type application/json
    (StatusCode::OK, axum::Json(body))
}

// GET /_node
async fn node_info(State(state): State<ApiState>) -> impl IntoResponse {
    let body = serde_json::json!({
        "id": state.instance_id,
        "name": hostname::get()
            .map_or_else(|_| "unknown".to_string(), |h| h.to_string_lossy().to_string()),
        "version": super::LOGSTASH_COMPAT_VERSION,
        "status": "green",
        "pipelines": {
            state.pipeline_id.clone(): {
                "workers": num_cpus::get(),
                "batch_size": 500,
                "batch_delay": 5000,
            }
        },
        "os": {
            "name": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
        }
    });
    (StatusCode::OK, axum::Json(body))
}

// GET /_node/stats
async fn node_stats(State(state): State<ApiState>) -> impl IntoResponse {
    // Top-level events = instance metrics (cumulative).
    let snap = state.metrics.snapshot();
    let uptime_ms = snap.uptime_millis;
    // The "pipelines" sub-section uses per-pipeline metrics (reset on reload).
    let pipeline_snap = pipeline_snapshot(&state);
    let body = serde_json::json!({
        "jvm": {
            "threads": {
                "count": num_cpus::get(),
                "peak_count": num_cpus::get(),
            },
            "mem": {
                "heap_used_in_bytes": 0,
                "heap_max_in_bytes": 0,
                "heap_used_percent": 0,
                "heap_committed_in_bytes": 0,
                "non_heap_used_in_bytes": 0,
                "non_heap_committed_in_bytes": 0,
            },
            "gc": {
                "collectors": {
                    "young": { "collection_count": 0, "collection_time_in_millis": 0 },
                    "old": { "collection_count": 0, "collection_time_in_millis": 0 },
                }
            },
            "uptime_in_millis": uptime_ms,
        },
        "process": {
            "open_file_descriptors": 0,
            "peak_open_file_descriptors": 0,
            "max_file_descriptors": 0,
            "mem": {
                "total_virtual_in_bytes": 0,
            },
            "cpu": {
                "total_in_millis": 0,
                "percent": 0,
                "load_average": { "1m": 0.0, "5m": 0.0, "15m": 0.0 },
            }
        },
        "events": {
            "in": snap.events_in,
            "out": snap.events_out,
            "filtered": snap.events_filtered,
            "duration_in_millis": uptime_ms,
            "queue_push_duration_in_millis": 0,
        },
        "pipelines": {
            state.pipeline_id.clone(): build_pipeline_stats(&pipeline_snap, &state.pipeline_id, pipeline_snap.uptime_millis, &plugins_snapshot(&state)),
        },
        "reloads": {
            "successes": snap.reload_successes,
            "failures": snap.reload_failures,
            "last_success_timestamp": snap.last_reload_success,
            "last_failure_timestamp": snap.last_reload_failure,
        },
        "os": {
            "name": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
        },
        "queue": {
            "type": "memory",
        },
        "flow": {
            "input_throughput": {
                "current": snap.input_rate(),
                "lifetime": snap.input_rate(),
            },
            "output_throughput": {
                "current": snap.output_rate(),
                "lifetime": snap.output_rate(),
            },
            "filter_throughput": {
                "current": snap.filter_rate(),
                "lifetime": snap.filter_rate(),
            },
            "queue_backpressure": {
                "current": 0.0,
                "lifetime": 0.0,
            },
            "worker_concurrency": {
                "current": 1.0,
                "lifetime": 1.0,
            },
            "worker_utilization": {
                "current": 0.0,
                "lifetime": 0.0,
            },
            "worker_millis_per_event": {
                "current": 0.0,
                "lifetime": 0.0,
            },
        }
    });
    (StatusCode::OK, axum::Json(body))
}

fn build_pipeline_stats(
    snap: &ferro_stash_core::metrics::MetricsSnapshot,
    _pipeline_id: &str,
    uptime_ms: u64,
    plugins: &[PluginInfo],
) -> serde_json::Value {
    let input_rate = snap.input_rate();
    let output_rate = snap.output_rate();
    let filter_rate = snap.filter_rate();

    let input_plugins: Vec<serde_json::Value> = plugins
        .iter()
        .filter(|p| p.plugin_type == "input")
        .map(|p| {
            serde_json::json!({
                "id": p.id,
                "name": p.name,
                "events": {"in": 0, "out": snap.events_in},
                "flow": {
                    "throughput": {"current": input_rate, "lifetime": input_rate}
                }
            })
        })
        .collect();
    let filter_plugins: Vec<serde_json::Value> = plugins
        .iter()
        .filter(|p| p.plugin_type == "filter")
        .map(|p| {
            serde_json::json!({
                "id": p.id,
                "name": p.name,
                "events": {"in": snap.events_in, "out": snap.events_out},
                "flow": {
                    "worker_utilization": {"current": 0.0, "lifetime": 0.0},
                    "worker_millis_per_event": {"current": 0.0, "lifetime": 0.0}
                }
            })
        })
        .collect();
    let output_plugins: Vec<serde_json::Value> = plugins
        .iter()
        .filter(|p| p.plugin_type == "output")
        .map(|p| {
            serde_json::json!({
                "id": p.id,
                "name": p.name,
                "events": {"in": snap.events_out, "out": snap.events_out},
                "flow": {
                    "worker_utilization": {"current": 0.0, "lifetime": 0.0},
                    "worker_millis_per_event": {"current": 0.0, "lifetime": 0.0},
                    "throughput": {"current": output_rate, "lifetime": output_rate}
                }
            })
        })
        .collect();
    let _ = filter_rate; // reserved for future use

    serde_json::json!({
        "events": {
            "in": snap.events_in,
            "out": snap.events_out,
            "filtered": snap.events_filtered,
            "duration_in_millis": uptime_ms,
            "queue_push_duration_in_millis": 0,
        },
        "plugins": {
            "inputs": input_plugins,
            "filters": filter_plugins,
            "outputs": output_plugins,
        },
        "reloads": {
            "successes": snap.reload_successes,
            "failures": snap.reload_failures,
            "last_error": serde_json::Value::Null,
            "last_success_timestamp": snap.last_reload_success.clone(),
            "last_failure_timestamp": snap.last_reload_failure.clone(),
        },
        "queue": {
            "type": "memory",
            "events_count": 0,
            "queue_size_in_bytes": 0,
            "max_queue_size_in_bytes": 0,
        },
        "flow": {
            "input_throughput": {
                "current": snap.input_rate(),
                "lifetime": snap.input_rate(),
            },
            "output_throughput": {
                "current": snap.output_rate(),
                "lifetime": snap.output_rate(),
            },
            "filter_throughput": {
                "current": snap.filter_rate(),
                "lifetime": snap.filter_rate(),
            },
            "queue_backpressure": {
                "current": 0.0,
                "lifetime": 0.0,
            },
            "worker_concurrency": {
                "current": 1.0,
                "lifetime": 1.0,
            },
            "worker_utilization": {
                "current": 0.0,
                "lifetime": 0.0,
            },
            "worker_millis_per_event": {
                "current": 0.0,
                "lifetime": 0.0,
            },
        }
    })
}

fn pipeline_snapshot(state: &ApiState) -> ferro_stash_core::metrics::MetricsSnapshot {
    // Use per-pipeline metrics (reset on reload) for pipeline-scoped endpoints
    state
        .pipeline_metrics
        .read()
        .map_or_else(|_| state.metrics.snapshot(), |m| m.snapshot())
}

fn plugins_snapshot(state: &ApiState) -> Vec<PluginInfo> {
    state.plugins.read().map(|p| p.clone()).unwrap_or_default()
}

// GET /_node/stats/pipelines
async fn pipeline_stats_all(State(state): State<ApiState>) -> impl IntoResponse {
    let snap = pipeline_snapshot(&state);
    let uptime_ms = snap.uptime_millis;
    let body = serde_json::json!({
        "pipelines": {
            state.pipeline_id.clone(): build_pipeline_stats(&snap, &state.pipeline_id, uptime_ms, &plugins_snapshot(&state)),
        }
    });
    (StatusCode::OK, axum::Json(body))
}

// GET /_node/stats/pipelines/:id
async fn pipeline_stats_by_id(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if id != state.pipeline_id {
        return (
            StatusCode::NOT_FOUND,
            axum::Json(serde_json::json!({"error": format!("pipeline '{}' not found", id)})),
        );
    }
    let snap = pipeline_snapshot(&state);
    let uptime_ms = snap.uptime_millis;
    let body = serde_json::json!({
        "pipelines": {
            state.pipeline_id.clone(): build_pipeline_stats(&snap, &state.pipeline_id, uptime_ms, &plugins_snapshot(&state)),
        }
    });
    (StatusCode::OK, axum::Json(body))
}

// GET /_node/hot_threads
async fn hot_threads() -> impl IntoResponse {
    let body = serde_json::json!({
        "hot_threads": {
            "time": chrono::Utc::now().to_rfc3339(),
            "busiest_threads": 3,
            "threads": [],
        }
    });
    (StatusCode::OK, axum::Json(body))
}

// GET /_node/plugins
async fn node_plugins() -> impl IntoResponse {
    let body = serde_json::json!({
        "total": 10,
        "plugins": [
            {"name": "logstash-input-stdin", "version": "3.4.0"},
            {"name": "logstash-input-file", "version": "4.4.6"},
            {"name": "logstash-input-tcp", "version": "6.4.1"},
            {"name": "logstash-input-udp", "version": "3.5.0"},
            {"name": "logstash-input-http", "version": "3.8.0"},
            {"name": "logstash-input-generator", "version": "3.1.0"},
            {"name": "logstash-input-beats", "version": "6.8.2"},
            {"name": "logstash-filter-grok", "version": "4.4.3"},
            {"name": "logstash-filter-mutate", "version": "3.5.7"},
            {"name": "logstash-output-stdout", "version": "3.1.4"},
        ]
    });
    (StatusCode::OK, axum::Json(body))
}

// GET /_node/logging
async fn get_logging(State(state): State<ApiState>) -> impl IntoResponse {
    let loggers = state
        .logger_levels
        .read()
        .map(|m| m.clone())
        .unwrap_or_default();
    let body = serde_json::json!({"loggers": loggers});
    (StatusCode::OK, axum::Json(body))
}

// PUT /_node/logging — accepts {"logger.<name>": "<LEVEL>"}
// Logstash behavior:
//   - "logger." key (empty name) = root logger → applies to all loggers not explicitly set
//     (does NOT apply to loggers rooted at "slowlog")
//   - "logger.<name>" = sets level for that logger; if it's a package (e.g. "logger.logstash"),
//     applies to all child loggers unless explicitly set
async fn put_logging(
    State(state): State<ApiState>,
    axum::Json(payload): axum::Json<serde_json::Value>,
) -> impl IntoResponse {
    if let Some(obj) = payload.as_object() {
        if let Ok(mut levels) = state.logger_levels.write() {
            for (key, value) in obj {
                let level = value.as_str().unwrap_or("INFO").to_ascii_uppercase();
                // Strip "logger." prefix
                let logger_name = key.strip_prefix("logger.").unwrap_or(key);

                if logger_name.is_empty() {
                    // Root logger: apply to all except slowlog
                    let keys: Vec<String> = levels.keys().cloned().collect();
                    for k in keys {
                        if !k.starts_with("slowlog") {
                            levels.insert(k, level.clone());
                        }
                    }
                } else {
                    // Apply to the exact logger AND to all child loggers (package)
                    levels.insert(logger_name.to_string(), level.clone());
                    let prefix = format!("{logger_name}.");
                    let keys: Vec<String> = levels.keys().cloned().collect();
                    for k in keys {
                        if k.starts_with(&prefix) {
                            levels.insert(k, level.clone());
                        }
                    }
                }
            }
        }
    }
    let body = serde_json::json!({"acknowledged": true});
    (StatusCode::OK, axum::Json(body))
}

// PUT /_node/logging/reset
async fn reset_logging(State(state): State<ApiState>) -> impl IntoResponse {
    if let Ok(mut levels) = state.logger_levels.write() {
        *levels = default_logger_levels();
    }
    let body = serde_json::json!({"acknowledged": true});
    (StatusCode::OK, axum::Json(body))
}

// GET /_health_report
async fn health_report(State(state): State<ApiState>) -> impl IntoResponse {
    let body = serde_json::json!({
        "status": "green",
        "symptom": "The data pipeline is operating correctly.",
        "indicators": {
            "pipelines": {
                "status": "green",
                "symptom": "All pipelines are running.",
                "indicators": {
                    state.pipeline_id.clone(): {
                        "status": "green",
                        "symptom": "The pipeline is running.",
                    }
                },
            },
        },
    });
    (StatusCode::OK, axum::Json(body))
}
