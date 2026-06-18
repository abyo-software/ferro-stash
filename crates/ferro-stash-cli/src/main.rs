// SPDX-License-Identifier: Apache-2.0
//! Logstash-compatible data pipeline.
//!
//! A drop-in Logstash replacement written in Rust.
//! Supports Logstash-compatible configuration files, CLI flags, monitoring API,
//! and automatic configuration reloading.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::parser::ValueSource;
use clap::{CommandFactory, FromArgMatches, Parser};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use ferro_stash_config::{parse_config_file, Config};
use ferro_stash_core::buffer::BufferConfig;
use ferro_stash_core::pipeline::{Pipeline, PipelineConfig};
use ferro_stash_core::shutdown::ShutdownController;

mod api;

/// Logstash version we are compatible with.
/// Can be overridden at build time via LOGSTASH_COMPAT_VERSION env var.
const LOGSTASH_COMPAT_VERSION: &str = match option_env!("LOGSTASH_COMPAT_VERSION") {
    Some(v) => v,
    None => "9.3.2",
};

// ---------------------------------------------------------------------------
// CLI definition — Logstash-compatible flags
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(
    name = "logstash",
    about = "bin/logstash [OPTIONS]",
    long_about = None,
    disable_version_flag = true,
    disable_help_flag = true,
    subcommand_negates_reqs = true,
)]
struct Cli {
    /// Show help and exit
    #[arg(long = "help", short = 'h')]
    help: bool,

    /// Show version and exit
    #[arg(long = "version", short = 'V')]
    version: bool,

    /// Path to pipeline configuration file(s)
    #[arg(short = 'f', long = "path.config", alias = "config")]
    config: Option<String>,

    /// Configuration string (inline pipeline definition)
    #[arg(short = 'e', long = "config.string", alias = "config-string")]
    config_string: Option<String>,

    /// Log level: trace, debug, info, warn, error
    #[arg(
        short = 'l',
        long = "log.level",
        alias = "log-level",
        default_value = "info"
    )]
    log_level: String,

    /// Number of pipeline workers
    #[arg(short = 'w', long = "pipeline.workers", alias = "workers")]
    workers: Option<usize>,

    /// Pipeline batch size
    #[arg(short = 'b', long = "pipeline.batch.size", alias = "batch-size")]
    batch_size: Option<usize>,

    /// Pipeline batch delay in milliseconds
    #[arg(long = "pipeline.batch.delay", alias = "batch-delay")]
    batch_delay: Option<u64>,

    /// Pipeline identifier
    #[arg(
        long = "pipeline.id",
        alias = "pipeline-id",
        default_value = "main",
        value_name = "ID"
    )]
    pipeline_id: String,

    /// Enable monitoring API (default: true)
    #[arg(long = "api.enabled", alias = "api-enabled", default_value = "true", num_args = 0..=1, default_missing_value = "true")]
    api_enabled: String,

    /// Monitoring API bind address
    #[arg(
        long = "api.http.host",
        alias = "api-bind",
        default_value = "127.0.0.1:9600"
    )]
    api_bind: String,

    /// Path to directory containing logstash.yml settings
    #[arg(long = "path.settings")]
    path_settings: Option<String>,

    /// Path to data directory (must be unique per instance)
    #[arg(long = "path.data")]
    path_data: Option<String>,

    /// Path to log directory
    #[arg(long = "path.logs")]
    path_logs: Option<String>,

    /// Validate configuration and exit
    #[arg(long = "config.test_and_exit", alias = "config-test-and-exit")]
    config_test_and_exit: bool,

    /// Exit automatically when all inputs complete (useful for batch/benchmark)
    #[arg(long = "auto-exit")]
    auto_exit: bool,

    /// Enable automatic configuration reloading
    #[arg(long = "config.reload.automatic", alias = "config-reload-automatic", num_args = 0..=1, default_missing_value = "true")]
    config_reload_automatic: Option<String>,

    /// Configuration reload check interval in seconds
    #[arg(long = "config.reload.interval", default_value = "3")]
    config_reload_interval: u64,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(clap::Subcommand)]
enum Commands {
    /// Validate configuration and exit
    #[command(name = "configtest")]
    ConfigTest {
        /// Path to configuration file
        #[arg(short = 'f', long = "config")]
        config: String,
    },

    /// Show version information
    Version,
}

// ---------------------------------------------------------------------------
// Instance identity — persistent UUID stored in path.data
// ---------------------------------------------------------------------------

fn get_or_create_instance_id(path_data: &str) -> String {
    let id_path = PathBuf::from(path_data).join("uuid");
    if let Ok(existing) = std::fs::read_to_string(&id_path) {
        let trimmed = existing.trim().to_string();
        if !trimmed.is_empty() {
            return trimmed;
        }
    }
    let id = uuid::Uuid::new_v4().to_string();
    let _ = std::fs::create_dir_all(path_data);
    let _ = std::fs::write(&id_path, &id);
    id
}

/// Acquire an exclusive lock on the data directory.
fn acquire_data_lock(path_data: &str) -> Result<std::fs::File> {
    std::fs::create_dir_all(path_data)
        .with_context(|| format!("cannot create data directory: {path_data}"))?;
    let lock_path = PathBuf::from(path_data).join(".lock");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&lock_path)
        .with_context(|| format!("cannot open lock file: {}", lock_path.display()))?;

    // Try exclusive non-blocking lock (works on both Unix and Windows)
    use fs2::FileExt;
    if file.try_lock_exclusive().is_err() {
        anyhow::bail!(
            "Logstash could not be started because another instance is already using \
             the configured data directory. path.data: {path_data}"
        );
    }

    Ok(file)
}

// ---------------------------------------------------------------------------
// Pipeline construction from Config
// ---------------------------------------------------------------------------

#[allow(dead_code)]
fn build_pipeline(cli: &Cli, config: &Config) -> Result<Pipeline> {
    build_pipeline_with_bus(cli, config, None, None)
}

fn build_pipeline_with_bus(
    cli: &Cli,
    config: &Config,
    bus: Option<Arc<tokio::sync::RwLock<ferro_stash_core::multi_pipeline::PipelineBus>>>,
    shared_metrics: Option<Arc<ferro_stash_core::metrics::PipelineMetrics>>,
) -> Result<Pipeline> {
    // Auto-exit when all inputs complete: enabled for -e (inline config)
    // or explicit --auto-exit flag, matching Logstash behavior.
    let auto_exit = cli.config_string.is_some() || cli.auto_exit;
    build_pipeline_internal(cli, config, bus, shared_metrics, auto_exit)
}

fn build_pipeline_internal(
    cli: &Cli,
    config: &Config,
    bus: Option<Arc<tokio::sync::RwLock<ferro_stash_core::multi_pipeline::PipelineBus>>>,
    shared_metrics: Option<Arc<ferro_stash_core::metrics::PipelineMetrics>>,
    auto_exit_on_inputs_done: bool,
) -> Result<Pipeline> {
    // Build queue type from config
    let queue = if config.queue.queue_type == "persisted" {
        ferro_stash_core::pipeline::QueueType::Persisted(
            ferro_stash_core::persistent_queue::PqConfig {
                path: config.queue.path.clone(),
                max_bytes: config.queue.max_bytes,
                ..ferro_stash_core::persistent_queue::PqConfig::default()
            },
        )
    } else {
        ferro_stash_core::pipeline::QueueType::Memory
    };

    // Build DLQ settings from config
    let dead_letter_queue = ferro_stash_core::pipeline::DlqSettings {
        enable: config.dead_letter_queue.enable,
        config: if config.dead_letter_queue.enable {
            Some(ferro_stash_core::dead_letter_queue::DlqConfig {
                path: config.dead_letter_queue.path.clone(),
                max_bytes: config.dead_letter_queue.max_bytes,
                ..ferro_stash_core::dead_letter_queue::DlqConfig::default()
            })
        } else {
            None
        },
    };

    let pipeline_config = PipelineConfig {
        workers: cli.workers.unwrap_or(config.pipeline.workers),
        buffer: BufferConfig {
            max_events: config.pipeline.buffer_size,
            flush_interval: Duration::from_millis(
                cli.batch_delay.unwrap_or(config.pipeline.batch_delay_ms),
            ),
            batch_size: cli.batch_size.unwrap_or(config.pipeline.batch_size),
        },
        id: cli.pipeline_id.clone(),
        auto_exit_on_inputs_done,
        queue,
        dead_letter_queue,
    };

    let mut pipeline = if let Some(m) = shared_metrics {
        // Instance metrics accumulate across pipeline reloads;
        // pipeline-local metrics (inside Pipeline) reset on each reload.
        Pipeline::new_with_instance_metrics(pipeline_config, m)
    } else {
        Pipeline::new(pipeline_config)
    };

    for input_config in &config.inputs {
        let input = ferro_stash_input::create_input_with_bus(
            &input_config.plugin_type,
            &input_config.settings,
            bus.clone(),
        )
        .with_context(|| format!("failed to create input: {}", input_config.plugin_type))?;
        pipeline.add_input(input);
    }

    for filter_config in &config.filters {
        let filter = ferro_stash_filter::create_filter(
            &filter_config.plugin_type,
            &filter_config.settings,
            filter_config.condition.clone(),
        )
        .with_context(|| format!("failed to create filter: {}", filter_config.plugin_type))?;
        pipeline.add_filter(filter);
    }

    for output_config in &config.outputs {
        let output = ferro_stash_output::create_output_with_bus(
            &output_config.plugin_type,
            &output_config.settings,
            output_config.condition.clone(),
            bus.clone(),
        )
        .with_context(|| format!("failed to create output: {}", output_config.plugin_type))?;
        pipeline.add_output(output);
    }

    Ok(pipeline)
}

// ---------------------------------------------------------------------------
// Multi-pipeline mode (pipelines.yml)
// ---------------------------------------------------------------------------

async fn run_multi_pipeline(cli: &Cli, pipelines_yml: &std::path::Path) -> Result<()> {
    use ferro_stash_core::multi_pipeline::{parse_pipelines_config, PipelineBus};
    use std::io::Write;

    // Read file with Logstash-compatible error messages
    let content = match std::fs::read_to_string(pipelines_yml) {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "Failed to read pipelines yaml file at {}: {e}",
                pipelines_yml.display()
            );
            if e.kind() == std::io::ErrorKind::PermissionDenied {
                eprintln!("Permission denied");
            }
            let _ = std::io::stderr().flush();
            let _ = std::io::stdout().flush();
            std::process::exit(1);
        }
    };

    // Check if effectively empty (only comments/whitespace)
    let effective = content
        .lines()
        .filter(|line| {
            let t = line.trim();
            !t.is_empty() && !t.starts_with('#')
        })
        .collect::<Vec<_>>();

    if effective.is_empty() {
        eprintln!(
            "Pipelines YAML file is empty. Location: {}",
            pipelines_yml.display()
        );
        let _ = std::io::stderr().flush();
        let _ = std::io::stdout().flush();
        std::process::exit(1);
    }

    let pipelines_config = match parse_pipelines_config(&content) {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "Failed to parse contents of pipelines yaml file at {}: SyntaxError: {e}",
                pipelines_yml.display()
            );
            let _ = std::io::stderr().flush();
            let _ = std::io::stdout().flush();
            std::process::exit(1);
        }
    };

    if pipelines_config.pipelines.is_empty() {
        eprintln!(
            "Pipelines YAML file is empty. Location: {}",
            pipelines_yml.display()
        );
        let _ = std::io::stderr().flush();
        let _ = std::io::stdout().flush();
        std::process::exit(1);
    }

    info!(
        count = pipelines_config.pipelines.len(),
        "multi-pipeline mode: loading pipelines"
    );

    // Shared bus for inter-pipeline communication
    let bus = Arc::new(tokio::sync::RwLock::new(PipelineBus::new()));

    let (shutdown_controller, _) = ShutdownController::new();
    let shutdown_controller = Arc::new(shutdown_controller);

    register_signal_handlers(&shutdown_controller);

    let mut handles = Vec::new();

    for entry in &pipelines_config.pipelines {
        // Load each pipeline's config from path.config or config.string
        let config = if let Some(ref config_str) = entry.config_string {
            ferro_stash_config::parse_config(config_str, ferro_stash_config::ConfigFormat::Auto)
                .with_context(|| format!("failed to parse pipeline '{}' inline config", entry.id))?
        } else if let Some(ref config_path) = entry.config_path {
            load_config(config_path).with_context(|| {
                format!(
                    "failed to load pipeline '{}' config: {}",
                    entry.id, config_path
                )
            })?
        } else {
            anyhow::bail!(
                "pipeline '{}' has neither path.config nor config.string",
                entry.id
            );
        };

        // Build a Cli-like override for this pipeline
        let pipeline_cli = Cli {
            help: false,
            version: false,
            config: entry.config_path.clone(),
            config_string: entry.config_string.clone(),
            log_level: cli.log_level.clone(),
            workers: entry.workers.or(cli.workers),
            batch_size: entry.batch_size.or(cli.batch_size),
            batch_delay: cli.batch_delay,
            pipeline_id: entry.id.clone(),
            api_enabled: "false".to_string(), // API already started
            api_bind: cli.api_bind.clone(),
            path_settings: cli.path_settings.clone(),
            path_data: cli.path_data.clone(),
            path_logs: cli.path_logs.clone(),
            config_test_and_exit: false,
            auto_exit: true,
            config_reload_automatic: None,
            config_reload_interval: cli.config_reload_interval,
            command: None,
        };

        // In multi-pipeline mode, each pipeline auto-exits when all its inputs complete
        // (Logstash behavior — finite inputs like `generator { count => N }` terminate naturally).
        let pipeline =
            build_pipeline_internal(&pipeline_cli, &config, Some(Arc::clone(&bus)), None, true)?;
        let metrics = pipeline.metrics();
        let signal = shutdown_controller.signal();
        let id = entry.id.clone();

        info!(pipeline = %id, inputs = config.inputs.len(), "starting pipeline");

        let handle = tokio::spawn(async move {
            if let Err(e) = pipeline.run(signal).await {
                error!(pipeline = %id, error = %e, "pipeline error");
            }
            let snap = metrics.snapshot();
            info!(
                pipeline = %id,
                events_in = snap.events_in,
                events_out = snap.events_out,
                "pipeline finished"
            );
        });
        handles.push(handle);
    }

    // Wait for all pipelines to complete
    for handle in handles {
        let _ = handle.await;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Config file watcher for --config.reload.automatic
// ---------------------------------------------------------------------------

/// Watches a config file and sends a message when it changes.
fn spawn_config_watcher(config_path: &str, _interval_secs: u64) -> tokio::sync::mpsc::Receiver<()> {
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let path = config_path.to_string();
    // Sub-second polling — tests call wait_for_port(new_port, 60) after cp, so
    // we must detect within that window. 500ms is quick enough without CPU waste.
    let poll_interval = std::time::Duration::from_millis(500);

    std::thread::spawn(move || {
        fn snapshot(paths: &[PathBuf]) -> Vec<Option<(std::time::SystemTime, u64)>> {
            paths
                .iter()
                .map(|p| {
                    std::fs::metadata(p)
                        .and_then(|m| m.modified().map(|t| (t, m.len())))
                        .ok()
                })
                .collect()
        }

        fn resolve_paths(path: &str) -> Vec<PathBuf> {
            if path.contains('*') || path.contains('?') {
                glob::glob(path).map_or_else(
                    |_| vec![PathBuf::from(path)],
                    |entries| entries.filter_map(Result::ok).collect(),
                )
            } else {
                vec![PathBuf::from(path)]
            }
        }

        info!(path = %path, interval_ms = poll_interval.as_millis() as u64, "config file watcher started (stat poll)");

        let mut last = snapshot(&resolve_paths(&path));

        loop {
            std::thread::sleep(poll_interval);
            let current = snapshot(&resolve_paths(&path));
            if current != last {
                last = current;
                info!("config file change detected, reloading...");
                if tx.blocking_send(()).is_err() {
                    break;
                }
            }
        }
    });

    rx
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    // Parse CLI via ArgMatches so we can distinguish clap's default value for
    // `--pipeline.id` from an explicit user override (e.g. `--pipeline.id main`).
    let matches = Cli::command().get_matches();
    let pipeline_id_from_cli = matches
        .value_source("pipeline_id")
        .is_some_and(|src| src == ValueSource::CommandLine);
    let mut cli =
        Cli::from_arg_matches(&matches).map_err(|e| anyhow::anyhow!("failed to parse CLI: {e}"))?;

    // Apply `pipeline.id` override from logstash.yml settings file (if present)
    // only when user didn't explicitly set --pipeline.id on the command line.
    // Using clap's ValueSource correctly distinguishes the default from an
    // explicit `--pipeline.id main`.
    if !pipeline_id_from_cli {
        if let Some(id) = read_pipeline_id_from_settings(cli.path_settings.as_ref()) {
            cli.pipeline_id = id;
        }
    }

    // --help (custom Logstash-compatible format — no angle brackets around value names)
    if cli.help {
        print_logstash_help();
        return Ok(());
    }

    // --version
    if cli.version {
        println!("logstash {LOGSTASH_COMPAT_VERSION}");
        return Ok(());
    }

    // Subcommands (legacy)
    if let Some(command) = &cli.command {
        match command {
            Commands::ConfigTest { config } => {
                return run_config_test(config);
            }
            Commands::Version => {
                println!("logstash {LOGSTASH_COMPAT_VERSION}");
                return Ok(());
            }
        }
    }

    // Mutually exclusive -f and -e
    if cli.config.is_some() && cli.config_string.is_some() {
        eprintln!(
            "ERROR: Settings 'path.config' (-f) and 'config.string' (-e) \
             can't be used simultaneously."
        );
        std::process::exit(1);
    }

    // Resolve path.data
    let path_data = cli.path_data.clone().unwrap_or_else(|| "data".to_string());

    // Acquire data directory lock
    let _lock = acquire_data_lock(&path_data)?;

    // Instance ID
    let instance_id = get_or_create_instance_id(&path_data);

    // Logstash-compatible startup messages
    println!("Using bundled JDK:");

    // Initialize logging
    let filter = EnvFilter::try_new(&cli.log_level).unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();

    let log_dir = cli.path_logs.clone().unwrap_or_else(|| {
        PathBuf::from(&path_data)
            .join("logs")
            .to_string_lossy()
            .to_string()
    });
    let _ = std::fs::create_dir_all(&log_dir);
    println!("Sending Logstash logs to {log_dir}/logstash-plain.log");

    // --config.test_and_exit
    if cli.config_test_and_exit {
        if let Some(ref config_path) = cli.config {
            return run_config_test(config_path);
        } else if let Some(ref config_str) = cli.config_string {
            let config = ferro_stash_config::parse_config(
                config_str,
                ferro_stash_config::ConfigFormat::Auto,
            )
            .context("failed to parse inline configuration")?;
            println!("Configuration OK");
            println!("  Inputs:  {}", config.inputs.len());
            println!("  Filters: {}", config.filters.len());
            println!("  Outputs: {}", config.outputs.len());
            return Ok(());
        }
        eprintln!("ERROR: No configuration specified for config.test_and_exit");
        std::process::exit(1);
    }

    info!(version = LOGSTASH_COMPAT_VERSION, "Logstash starting");

    // Check for multi-pipeline mode (pipelines.yml in path.settings)
    let pipelines_yml = cli
        .path_settings
        .as_ref()
        .map(|dir| PathBuf::from(dir).join("pipelines.yml"));
    let multi_pipeline = pipelines_yml.as_ref().is_some_and(|p| p.exists());

    // Start metrics API (runs for the lifetime of the process)
    let shared_metrics = ferro_stash_core::metrics::PipelineMetrics::new();
    // Pipeline metrics handle — swapped on reload so that /_node/stats/pipelines/<id>
    // reflects per-pipeline (reset on reload) counters.
    let pipeline_metrics_handle: api::PipelineMetricsHandle =
        Arc::new(std::sync::RwLock::new(Arc::clone(&shared_metrics)));
    // Plugins handle — updated with plugin id/type/name on each (re)load.
    let plugins_handle: api::PluginsHandle = Arc::new(std::sync::RwLock::new(Vec::new()));
    let api_enabled = cli.api_enabled == "true" || cli.api_enabled == "1";
    if api_enabled {
        let api_metrics = Arc::clone(&shared_metrics);
        let api_pipeline_metrics = Arc::clone(&pipeline_metrics_handle);
        let api_plugins = Arc::clone(&plugins_handle);
        let api_bind = cli.api_bind.clone();
        let api_instance_id = instance_id.clone();
        let api_pipeline_id = cli.pipeline_id.clone();
        let (api_ctrl, api_signal) = ShutdownController::new();
        let api_ctrl = Arc::new(api_ctrl);

        let api_ctrl_for_sigint = Arc::clone(&api_ctrl);
        tokio::spawn(async move {
            if let Err(e) = api::run_api_server_full(
                &api_bind,
                api_metrics,
                api_pipeline_metrics,
                api_plugins,
                api_signal,
                api_instance_id,
                api_pipeline_id,
            )
            .await
            {
                error!(error = %e, "metrics API error");
            }
        });

        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            api_ctrl_for_sigint.shutdown();
        });
    }

    // Multi-pipeline mode
    if multi_pipeline {
        let yml_path = pipelines_yml.expect("checked above");
        return run_multi_pipeline(&cli, &yml_path).await;
    }

    // Single pipeline mode
    let config = load_config_from_cli(&cli)?;
    validate_config(&config)?;

    info!(
        inputs = config.inputs.len(),
        filters = config.filters.len(),
        outputs = config.outputs.len(),
        workers = config.pipeline.workers,
        "configuration loaded"
    );

    let reload_automatic = cli
        .config_reload_automatic
        .as_deref()
        .is_some_and(|v| v == "true" || v == "1");
    if reload_automatic && cli.config.is_some() {
        run_with_reload(
            &cli,
            config,
            &shared_metrics,
            &pipeline_metrics_handle,
            &plugins_handle,
        )
        .await
    } else {
        run_once(
            &cli,
            config,
            &shared_metrics,
            &pipeline_metrics_handle,
            &plugins_handle,
        )
        .await
    }
}

// ---------------------------------------------------------------------------
// Run once (no reload)
// ---------------------------------------------------------------------------

async fn run_once(
    cli: &Cli,
    config: Config,
    shared_metrics: &Arc<ferro_stash_core::metrics::PipelineMetrics>,
    pipeline_metrics_handle: &api::PipelineMetricsHandle,
    plugins_handle: &api::PluginsHandle,
) -> Result<()> {
    let (shutdown_controller, shutdown_signal) = ShutdownController::new();
    let shutdown_controller = Arc::new(shutdown_controller);

    // Publish plugin info to API
    if let Ok(mut p) = plugins_handle.write() {
        *p = extract_plugins(&config);
    }

    let pipeline = build_pipeline_with_bus(cli, &config, None, Some(Arc::clone(shared_metrics)))?;
    let metrics = pipeline.metrics();

    // Publish pipeline metrics for the API
    if let Ok(mut handle) = pipeline_metrics_handle.write() {
        *handle = Arc::clone(&metrics);
    }

    register_signal_handlers(&shutdown_controller);

    info!(pipeline = %cli.pipeline_id, "starting pipeline");
    let start = std::time::Instant::now();

    pipeline
        .run(shutdown_signal)
        .await
        .context("pipeline error")?;

    log_pipeline_finished(&metrics, start);
    Ok(())
}

// ---------------------------------------------------------------------------
// Run with config reload
// ---------------------------------------------------------------------------

async fn run_with_reload(
    cli: &Cli,
    initial_config: Config,
    shared_metrics: &Arc<ferro_stash_core::metrics::PipelineMetrics>,
    pipeline_metrics_handle: &api::PipelineMetricsHandle,
    plugins_handle: &api::PluginsHandle,
) -> Result<()> {
    let config_path = cli.config.as_deref().expect("reload requires -f");

    let mut reload_rx = spawn_config_watcher(config_path, cli.config_reload_interval);

    // Global shutdown (SIGINT/SIGTERM)
    let (global_ctrl, _global_signal) = ShutdownController::new();
    let global_ctrl = Arc::new(global_ctrl);

    // SIGINT handler
    let ctrl_for_sigint = Arc::clone(&global_ctrl);
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("received SIGINT, shutting down...");
        ctrl_for_sigint.shutdown();
    });

    #[cfg(unix)]
    {
        let ctrl_for_sigterm = Arc::clone(&global_ctrl);
        tokio::spawn(async move {
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("SIGTERM handler");
            sigterm.recv().await;
            info!("received SIGTERM, shutting down...");
            ctrl_for_sigterm.shutdown();
        });
    }

    let mut current_config = initial_config;
    let mut is_reload = false;

    loop {
        let (pipeline_ctrl, pipeline_signal) = ShutdownController::new();
        let pipeline_ctrl = Arc::new(pipeline_ctrl);

        // Publish plugin info for the API
        if let Ok(mut p) = plugins_handle.write() {
            *p = extract_plugins(&current_config);
        }

        let pipeline =
            build_pipeline_with_bus(cli, &current_config, None, Some(Arc::clone(shared_metrics)))?;
        let metrics = pipeline.metrics();

        // Publish the new pipeline's per-pipeline metrics to the API so that
        // /_node/stats/pipelines/<id> reflects the freshly-reset counters.
        if let Ok(mut handle) = pipeline_metrics_handle.write() {
            *handle = Arc::clone(&metrics);
        }

        // If this iteration was triggered by a reload, record success on the
        // (fresh) pipeline metrics so that /_node/stats/pipelines/<id>/reloads.successes
        // reports it after the reload.
        if is_reload {
            metrics.record_reload_success();
            is_reload = false;
        }

        info!(pipeline = %cli.pipeline_id, "starting pipeline");
        let start = std::time::Instant::now();

        // Run pipeline in a task so we can select on reload/shutdown
        let mut pipeline_handle = tokio::spawn(async move { pipeline.run(pipeline_signal).await });

        let mut global_shutdown_signal = global_ctrl.signal();

        // Wait for: global shutdown, config reload, or pipeline exit
        let reason = tokio::select! {
            () = global_shutdown_signal.wait() => "shutdown",
            Some(()) = reload_rx.recv() => "reload",
            result = &mut pipeline_handle => {
                if let Ok(Err(e)) = result {
                    error!(error = %e, "pipeline error");
                }
                "exit"
            }
        };

        match reason {
            "shutdown" => {
                info!("global shutdown, stopping pipeline");
                pipeline_ctrl.shutdown();
                await_pipeline_with_timeout(&mut pipeline_handle, "shutdown").await;
                log_pipeline_finished(&metrics, start);
                break;
            }
            "reload" => {
                info!("reloading pipeline configuration");
                match load_config(config_path) {
                    Ok(new_config) => {
                        if let Err(e) = validate_config(&new_config) {
                            warn!(error = %e, "new config invalid, keeping current config");
                            shared_metrics.record_reload_failure();
                            continue;
                        }
                        // Shutdown old pipeline, bounded so a stuck plugin
                        // can't leave a zombie pipeline task behind.
                        pipeline_ctrl.shutdown();
                        await_pipeline_with_timeout(&mut pipeline_handle, "reload").await;
                        log_pipeline_finished(&metrics, start);

                        // shared_metrics is the INSTANCE metrics (cumulative);
                        // pipeline metrics reset naturally because we create a new
                        // Pipeline with fresh pipeline_metrics on each iteration.
                        shared_metrics.record_reload_success();
                        // Also reflect this success in the NEXT pipeline metrics (post-reload)
                        is_reload = true;

                        info!(
                            inputs = new_config.inputs.len(),
                            filters = new_config.filters.len(),
                            outputs = new_config.outputs.len(),
                            "new configuration loaded"
                        );
                        current_config = new_config;
                        // Loop continues — will start new pipeline
                    }
                    Err(e) => {
                        warn!(error = %e, "failed to parse new config, keeping current config");
                        shared_metrics.record_reload_failure();
                        // Don't stop the pipeline — keep running with old config
                    }
                }
            }
            "exit" => {
                log_pipeline_finished(&metrics, start);
                break;
            }
            _ => break,
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn register_signal_handlers(controller: &Arc<ShutdownController>) {
    let ctrl = Arc::clone(controller);
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("received SIGINT, shutting down...");
        ctrl.shutdown();
    });

    #[cfg(unix)]
    {
        let ctrl = Arc::clone(controller);
        tokio::spawn(async move {
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("SIGTERM handler");
            sigterm.recv().await;
            info!("received SIGTERM, shutting down...");
            ctrl.shutdown();
        });
    }
}

/// Extract plugin id/type/name from a parsed Config for the monitoring API.
/// Uses DSL `id => 'xxx'` when present, otherwise auto-generates `<plugin>_<index>`.
fn extract_plugins(config: &Config) -> Vec<api::PluginInfo> {
    let mut out = Vec::new();
    for (i, input) in config.inputs.iter().enumerate() {
        let id = input
            .settings
            .get("id")
            .and_then(|v| v.as_str())
            .map_or_else(|| format!("{}_{}", input.plugin_type, i), String::from);
        out.push(api::PluginInfo {
            id,
            name: input.plugin_type.clone(),
            plugin_type: "input".to_string(),
        });
    }
    for (i, filter) in config.filters.iter().enumerate() {
        let id = filter
            .settings
            .get("id")
            .and_then(|v| v.as_str())
            .map_or_else(|| format!("{}_{}", filter.plugin_type, i), String::from);
        out.push(api::PluginInfo {
            id,
            name: filter.plugin_type.clone(),
            plugin_type: "filter".to_string(),
        });
    }
    for (i, output) in config.outputs.iter().enumerate() {
        let id = output
            .settings
            .get("id")
            .and_then(|v| v.as_str())
            .map_or_else(|| format!("{}_{}", output.plugin_type, i), String::from);
        out.push(api::PluginInfo {
            id,
            name: output.plugin_type.clone(),
            plugin_type: "output".to_string(),
        });
    }
    out
}

fn validate_config(config: &Config) -> Result<()> {
    if config.inputs.is_empty() {
        anyhow::bail!("no inputs configured");
    }
    // Logstash allows empty output blocks — events are silently discarded
    if config.outputs.is_empty() {
        warn!("no outputs configured — events will be discarded");
    }
    Ok(())
}

/// Maximum time we wait for a pipeline task to finish after we have already
/// signalled it to shut down. If this elapses we assume a plugin is stuck
/// (blocked I/O, runaway loop, etc.) and abort the tokio task to avoid
/// leaking a zombie pipeline across reload/shutdown.
const PIPELINE_STOP_TIMEOUT: Duration = Duration::from_secs(30);

/// Await a pipeline task with a bounded timeout. On timeout, aborts the
/// task handle and logs a warning. `cause` is used for log context
/// ("shutdown" or "reload").
async fn await_pipeline_with_timeout<E: std::fmt::Display>(
    pipeline_handle: &mut tokio::task::JoinHandle<std::result::Result<(), E>>,
    cause: &'static str,
) {
    await_pipeline_with_timeout_inner(pipeline_handle, cause, PIPELINE_STOP_TIMEOUT).await;
}

async fn await_pipeline_with_timeout_inner<E: std::fmt::Display>(
    pipeline_handle: &mut tokio::task::JoinHandle<std::result::Result<(), E>>,
    cause: &'static str,
    stop_timeout: Duration,
) {
    match tokio::time::timeout(stop_timeout, &mut *pipeline_handle).await {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(e))) => {
            warn!(cause, error = %e, "pipeline finished with error during stop");
        }
        Ok(Err(e)) => {
            warn!(cause, error = %e, "pipeline task join error during stop");
        }
        Err(_) => {
            warn!(
                cause,
                timeout_secs = stop_timeout.as_secs(),
                "pipeline did not stop in time, aborting task (a plugin is likely blocked)"
            );
            pipeline_handle.abort();
            // Give the abort a brief moment to take effect so we don't
            // return while the task is still technically running.
            let _ = tokio::time::timeout(Duration::from_secs(1), &mut *pipeline_handle).await;
        }
    }
}

fn log_pipeline_finished(
    metrics: &Arc<ferro_stash_core::metrics::PipelineMetrics>,
    start: std::time::Instant,
) {
    let elapsed = start.elapsed();
    let snap = metrics.snapshot();
    info!(
        events_in = snap.events_in,
        events_out = snap.events_out,
        events_filtered = snap.events_filtered,
        duration_secs = elapsed.as_secs(),
        "pipeline finished"
    );
}

/// Parse logstash.yml and extract `config.string` or `path.config` if present.
/// Returns (Some(config_string), None), (None, Some(config_path)), or (None, None).
fn read_settings_yml(settings_yml: &std::path::Path) -> (Option<String>, Option<String>) {
    let Ok(content) = std::fs::read_to_string(settings_yml) else {
        return (None, None);
    };
    let Ok(v) = serde_yaml::from_str::<serde_yaml::Value>(&content) else {
        return (None, None);
    };
    let map = v.as_mapping();
    let get = |key: &str| -> Option<String> {
        map.and_then(|m| m.get(serde_yaml::Value::String(key.to_string())))
            .and_then(|v| v.as_str().map(String::from))
    };
    (get("config.string"), get("path.config"))
}

/// Read `pipeline.id` from a settings YAML file. Checks path.settings/logstash.yml if explicit,
/// else install-dir/config/logstash.yml. Returns None if not found.
fn read_pipeline_id_from_settings(path_settings: Option<&String>) -> Option<String> {
    let candidates: Vec<PathBuf> = if let Some(dir) = path_settings {
        vec![PathBuf::from(dir).join("logstash.yml")]
    } else {
        let mut v = Vec::new();
        if let Ok(exe) = std::env::current_exe() {
            if let Some(parent) = exe.parent().and_then(|p| p.parent()) {
                v.push(parent.join("config").join("logstash.yml"));
            }
        }
        v.push(PathBuf::from("config/logstash.yml"));
        v
    };
    for yml in candidates {
        let Ok(content) = std::fs::read_to_string(&yml) else {
            continue;
        };
        let Ok(v) = serde_yaml::from_str::<serde_yaml::Value>(&content) else {
            continue;
        };
        if let Some(m) = v.as_mapping() {
            if let Some(val) = m.get(serde_yaml::Value::String("pipeline.id".to_string())) {
                if let Some(s) = val.as_str() {
                    return Some(s.to_string());
                }
            }
        }
    }
    None
}

fn load_config_from_cli(cli: &Cli) -> Result<Config> {
    if let Some(ref config_string) = cli.config_string {
        return ferro_stash_config::parse_config(
            config_string,
            ferro_stash_config::ConfigFormat::Auto,
        )
        .context("failed to parse inline configuration");
    }

    if let Some(ref config_path) = cli.config {
        return load_config(config_path);
    }

    // --path.settings (or default) logstash.yml に config.string/path.config があれば使う
    let mut settings_candidates: Vec<PathBuf> = Vec::new();
    if let Some(dir) = cli.path_settings.as_ref() {
        settings_candidates.push(PathBuf::from(dir).join("logstash.yml"));
    } else {
        if let Ok(exe) = std::env::current_exe() {
            if let Some(parent) = exe.parent().and_then(|p| p.parent()) {
                settings_candidates.push(parent.join("config").join("logstash.yml"));
            }
        }
        settings_candidates.push(PathBuf::from("config/logstash.yml"));
    }
    for yml in settings_candidates.iter().filter(|p| p.exists()) {
        let (config_str, config_path) = read_settings_yml(yml);
        if let Some(s) = config_str {
            return ferro_stash_config::parse_config(&s, ferro_stash_config::ConfigFormat::Auto)
                .context("failed to parse logstash.yml config.string");
        }
        if let Some(p) = config_path {
            return load_config(&p);
        }
    }

    let default_paths = [
        "config/pipelines.yml",
        "pipeline/logstash.conf",
        "logstash.conf",
        "config/logstash.conf",
        "ferro-stash.conf",
        "ferro-stash.yml",
        "config/ferro-stash.conf",
        "config/ferro-stash.yml",
        "/etc/ferro-stash/ferro-stash.conf",
    ];
    for path in &default_paths {
        if std::path::Path::new(path).exists() {
            return load_config(path);
        }
    }

    error!("no configuration file specified. Use -f <config> or -e '<config string>'");
    std::process::exit(1);
}

fn load_config(path: &str) -> Result<Config> {
    if path.contains('*') || path.contains('?') {
        let mut merged = Config::default();
        let entries: Vec<_> = glob::glob(path)
            .context("invalid config glob pattern")?
            .collect();
        if entries.is_empty() {
            anyhow::bail!("no config files matching pattern: {path}");
        }
        for entry in entries {
            let entry = entry.context("glob error")?;
            let config = parse_config_file(&entry.to_string_lossy())
                .with_context(|| format!("failed to parse {}", entry.display()))?;
            merged.inputs.extend(config.inputs);
            merged.filters.extend(config.filters);
            merged.outputs.extend(config.outputs);
        }
        Ok(merged)
    } else {
        parse_config_file(path).with_context(|| format!("failed to parse config: {path}"))
    }
}

fn print_logstash_help() {
    println!("bin/logstash [OPTIONS]");
    println!();
    println!("Options:");
    println!("  -f, --path.config CONFIG          Path to pipeline configuration file(s)");
    println!(
        "  -e, --config.string CONFIG_STRING  Configuration string (inline pipeline definition)"
    );
    println!("  -w, --pipeline.workers WORKERS     Number of pipeline workers");
    println!("  -b, --pipeline.batch.size BATCH_SIZE Pipeline batch size");
    println!("      --pipeline.batch.delay BATCH_DELAY Pipeline batch delay in milliseconds");
    println!("      --pipeline.id ID               Pipeline identifier [default: main]");
    println!("  -l, --log.level LOG_LEVEL          Log level: trace, debug, info, warn, error [default: info]");
    println!(
        "      --path.settings PATH_SETTINGS  Path to directory containing logstash.yml settings"
    );
    println!(
        "      --path.data PATH_DATA          Path to data directory (must be unique per instance)"
    );
    println!("      --path.logs PATH_LOGS          Path to log directory");
    println!("      --config.test_and_exit         Validate configuration and exit");
    println!("      --config.reload.automatic      Enable automatic configuration reloading");
    println!("      --config.reload.interval INTERVAL Configuration reload check interval in seconds [default: 3]");
    println!("      --api.enabled API_ENABLED      Enable monitoring API (default: true)");
    println!("      --api.http.host API_BIND       Monitoring API bind address [default: 127.0.0.1:9600]");
    println!("  -V, --version                      Show version and exit");
    println!("  -h, --help                         Print help");
}

fn run_config_test(path: &str) -> Result<()> {
    let config = load_config(path)?;
    println!("Configuration OK");
    println!("  Inputs:  {}", config.inputs.len());
    println!("  Filters: {}", config.filters.len());
    println!("  Outputs: {}", config.outputs.len());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression test: `value_source("pipeline_id")` must correctly
    /// distinguish the clap default from an explicit override, even when
    /// the user passes `--pipeline.id main` (identical to the default).
    #[test]
    fn pipeline_id_value_source_detects_explicit_override() {
        // No CLI override → source is DefaultValue
        let m = Cli::command().get_matches_from(["ferro-stash"]);
        assert_eq!(
            m.value_source("pipeline_id"),
            Some(ValueSource::DefaultValue)
        );

        // Explicit override with a non-default value
        let m = Cli::command().get_matches_from(["ferro-stash", "--pipeline.id", "custom"]);
        assert_eq!(
            m.value_source("pipeline_id"),
            Some(ValueSource::CommandLine)
        );

        // Explicit override that matches the default literal ("main").
        // This is the core bug the Medium-1 fix addresses: the previous
        // `cli.pipeline_id == "main"` check would misclassify this as
        // "not overridden" and let logstash.yml clobber it.
        let m = Cli::command().get_matches_from(["ferro-stash", "--pipeline.id", "main"]);
        assert_eq!(
            m.value_source("pipeline_id"),
            Some(ValueSource::CommandLine)
        );
    }

    /// Regression test for Medium 2: `await_pipeline_with_timeout_inner`
    /// must not block forever when a pipeline task refuses to exit — it
    /// should abort the handle after the timeout elapses.
    #[tokio::test]
    async fn await_pipeline_with_timeout_aborts_stuck_task() {
        let mut handle: tokio::task::JoinHandle<std::result::Result<(), std::io::Error>> =
            tokio::spawn(async {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                Ok(())
            });

        let started = std::time::Instant::now();
        await_pipeline_with_timeout_inner(&mut handle, "shutdown", Duration::from_millis(50)).await;
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "helper should return promptly after timeout, took {:?}",
            started.elapsed()
        );
        assert!(
            handle.is_finished(),
            "pipeline task should be finished after abort"
        );
    }

    /// Happy path: if the pipeline finishes before the timeout,
    /// the helper returns cleanly without aborting anything.
    #[tokio::test]
    async fn await_pipeline_with_timeout_returns_on_clean_exit() {
        let mut handle: tokio::task::JoinHandle<std::result::Result<(), std::io::Error>> =
            tokio::spawn(async { Ok(()) });
        await_pipeline_with_timeout_inner(&mut handle, "shutdown", Duration::from_secs(5)).await;
    }
}
