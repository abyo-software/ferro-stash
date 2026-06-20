// SPDX-License-Identifier: Apache-2.0
//! JDBC input — polls a relational database with a SQL `statement` on an
//! interval, maps every row to an event, and (optionally) tracks an incrementing
//! column for incremental loads. Mirrors Logstash's `jdbc` input for the common
//! case.
//!
//! Unlike Logstash (which loads a Java JDBC driver), this uses native Rust
//! drivers via [`sqlx`]'s `Any` backend: one code path serves PostgreSQL, MySQL
//! and SQLite, selected by the connection-string scheme. JDBC URLs are accepted
//! and translated (`jdbc:postgresql:` → `postgres:`, `jdbc:mysql:` → `mysql:`,
//! `jdbc:sqlite:` → `sqlite:`).
//!
//! ```logstash
//! input {
//!   jdbc {
//!     jdbc_connection_string => "jdbc:postgresql://localhost:5432/app"
//!     statement              => "SELECT id, name, updated_at FROM users WHERE id > :sql_last_value"
//!     tracking_column        => "id"
//!     use_column_value       => true
//!     interval               => 30          # seconds (also: schedule => { every => "30s" })
//!     last_run_metadata_path => "/var/lib/ferro-stash/.jdbc_last_run"
//!   }
//! }
//! ```
//!
//! Residuals (honest limitations):
//! - **No Java JDBC drivers / driver libraries.** Only the schemes sqlx's `Any`
//!   backend supports (postgres / mysql / sqlite) work; `jdbc_driver_library`
//!   and `jdbc_driver_class` are accepted but ignored.
//! - **`:sql_last_value` is substituted textually**, not as a bound parameter, so
//!   it is best suited to numeric tracking columns. For string/date columns,
//!   quote it in your SQL (`... > ':sql_last_value'`).
//! - **Scheduling is interval-only** — a `schedule => { every => "10s" }` is
//!   parsed to a fixed interval; full cron expressions are not supported.
//! - **Column type-mapping is best-effort**: each value is decoded as the first
//!   of i64 / f64 / bool / String that succeeds, else Null (blobs become Null).
//! - **Paging** (`jdbc_paging_enabled`) wraps the statement in
//!   `SELECT * FROM (<statement>) LIMIT/OFFSET` and **streams each page to the
//!   pipeline before fetching the next**, so a large result set is never fully
//!   buffered in memory. Paging **requires a stable `ORDER BY`** in your
//!   `statement`: LIMIT/OFFSET over an unordered query may return rows in a
//!   different order between pages, silently **duplicating or skipping** rows. A
//!   paged statement without an `ORDER BY` is logged with a warning at startup.

use async_trait::async_trait;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::InputPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use ferro_stash_core::shutdown::ShutdownSignal;
use sqlx::any::{AnyPoolOptions, AnyRow};
use sqlx::{Column, Row};
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// Installs sqlx's default `Any` drivers exactly once per process.
fn install_drivers_once() {
    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(sqlx::any::install_default_drivers);
}

/// JDBC input configuration.
///
/// `Debug` is implemented manually so the `connection_string` (which embeds DB
/// credentials, e.g. `postgres://user:pass@host/db`) is never rendered in
/// logs/diagnostics.
#[derive(Clone)]
pub struct JdbcInput {
    connection_string: String,
    statement: String,
    interval: u64,
    tracking_column: Option<String>,
    use_column_value: bool,
    last_run_metadata_path: PathBuf,
    clean_run: bool,
    lowercase_column_names: bool,
    paging_enabled: bool,
    page_size: u64,
}

impl std::fmt::Debug for JdbcInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JdbcInput")
            .field("connection_string", &"***")
            .field("statement", &self.statement)
            .field("interval", &self.interval)
            .field("tracking_column", &self.tracking_column)
            .field("use_column_value", &self.use_column_value)
            .field("last_run_metadata_path", &self.last_run_metadata_path)
            .field("clean_run", &self.clean_run)
            .field("lowercase_column_names", &self.lowercase_column_names)
            .field("paging_enabled", &self.paging_enabled)
            .field("page_size", &self.page_size)
            .finish()
    }
}

/// Outcome of emitting one batch of rows downstream.
enum EmitOutcome {
    /// All rows were sent.
    Sent,
    /// The downstream channel closed; the input should stop.
    DownstreamClosed,
}

impl JdbcInput {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let connection_string = resolve_connection_string(settings)?;

        // `statement` (inline SQL) or `statement_filepath` (read from disk).
        let statement = match settings.get_string("statement") {
            Some(s) => s,
            None => match settings.get_string("statement_filepath") {
                Some(path) => std::fs::read_to_string(&path).map_err(|e| {
                    input_err(format!("could not read statement_filepath '{path}': {e}"))
                })?,
                None => {
                    return Err(input_err(
                        "jdbc input requires `statement` (SQL) or `statement_filepath`".to_string(),
                    ))
                }
            },
        };

        // Scheduling: prefer `schedule => { every => "10s" }`, else `interval`
        // seconds; floored to >= 1s.
        let scheduled = settings
            .get("schedule")
            .and_then(|s| s.get("every"))
            .and_then(serde_json::Value::as_str)
            .and_then(parse_every);
        let interval = scheduled
            .or_else(|| settings.get_u64("interval"))
            .unwrap_or(60)
            .max(1);

        let last_run_metadata_path = settings
            .get_string("last_run_metadata_path")
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::temp_dir().join(".ferro_stash_jdbc_last_run"));

        let paging_enabled = settings.get_bool("jdbc_paging_enabled").unwrap_or(false);
        // LIMIT/OFFSET paging over an unordered query can return rows in a
        // different order between pages, silently duplicating or skipping rows.
        // Warn loudly so the operator adds a stable ORDER BY.
        if paging_enabled && !statement.to_lowercase().contains("order by") {
            warn!(
                "jdbc input: `jdbc_paging_enabled` is set but the statement has no `ORDER BY`; \
                 paging requires a stable ORDER BY to avoid duplicate/skipped rows"
            );
        }

        Ok(Self {
            connection_string,
            statement,
            interval,
            tracking_column: settings.get_string("tracking_column"),
            use_column_value: settings.get_bool("use_column_value").unwrap_or(false),
            last_run_metadata_path,
            clean_run: settings.get_bool("clean_run").unwrap_or(false),
            lowercase_column_names: settings.get_bool("lowercase_column_names").unwrap_or(true),
            paging_enabled,
            page_size: settings.get_u64("jdbc_page_size").unwrap_or(100_000).max(1),
        })
    }

    /// Whether incremental tracking is active (a tracking column + use the value).
    fn tracking_active(&self) -> bool {
        self.use_column_value && self.tracking_column.is_some()
    }

    /// Reads the persisted `sql_last_value` (or the `0` floor on first run /
    /// when `clean_run` is set).
    fn load_last_value(&self) -> String {
        if self.clean_run {
            return "0".to_string();
        }
        std::fs::read_to_string(&self.last_run_metadata_path)
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "0".to_string())
    }

    fn persist_last_value(&self, value: &str) {
        if let Some(parent) = self.last_run_metadata_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if let Err(e) = std::fs::write(&self.last_run_metadata_path, value) {
            warn!(error = %e, path = ?self.last_run_metadata_path, "jdbc: could not persist sql_last_value");
        }
    }

    /// Emits one batch of rows to `sender`, mapping each row to an event and
    /// tracking the greatest tracking-column value seen across batches. Returns
    /// whether the downstream channel closed (so the caller can stop).
    async fn emit_rows(
        &self,
        rows: Vec<AnyRow>,
        sender: &mpsc::Sender<Event>,
        max_tracked: &mut Option<EventValue>,
    ) -> EmitOutcome {
        let tracking = self.tracking_column.clone();
        for row in rows {
            let mut event = Event::empty();
            for col in row.columns() {
                let raw_name = col.name();
                let value = decode_any_value(&row, col.ordinal());
                if self.tracking_active() {
                    if let Some(tc) = &tracking {
                        if raw_name.eq_ignore_ascii_case(tc) {
                            let greater = max_tracked
                                .as_ref()
                                .map_or(true, |cur| value_gt(&value, cur));
                            if greater {
                                *max_tracked = Some(value.clone());
                            }
                        }
                    }
                }
                let key = if self.lowercase_column_names {
                    raw_name.to_lowercase()
                } else {
                    raw_name.to_string()
                };
                event.set(key, value);
            }
            if sender.send(event).await.is_err() {
                info!("jdbc input: downstream closed, stopping");
                return EmitOutcome::DownstreamClosed;
            }
        }
        EmitOutcome::Sent
    }
}

#[async_trait]
impl InputPlugin for JdbcInput {
    fn name(&self) -> &str {
        "jdbc"
    }

    async fn run(
        &mut self,
        sender: mpsc::Sender<Event>,
        mut shutdown: ShutdownSignal,
    ) -> Result<()> {
        install_drivers_once();
        let pool = AnyPoolOptions::new()
            .max_connections(2)
            .connect(&self.connection_string)
            .await
            .map_err(|e| input_err(format!("connect failed: {e}")))?;
        info!("jdbc input starting (interval {}s)", self.interval);

        let mut sql_last_value = self.load_last_value();

        loop {
            let statement = self.statement.replace(":sql_last_value", &sql_last_value);
            let mut max_tracked: Option<EventValue> = None;

            if !self.paging_enabled {
                match sqlx::query(&statement).fetch_all(&pool).await {
                    Ok(rows) => {
                        if let EmitOutcome::DownstreamClosed =
                            self.emit_rows(rows, &sender, &mut max_tracked).await
                        {
                            return Ok(());
                        }
                    }
                    Err(e) => warn!(error = %e, "jdbc query failed, will retry next interval"),
                }
            } else {
                // Paged: stream each page downstream BEFORE fetching the next so a
                // large result set is never fully buffered in memory.
                let base = statement.trim().trim_end_matches(';');
                let mut offset: u64 = 0;
                loop {
                    let paged = format!(
                        "SELECT * FROM ({base}) AS ferro_stash_paged LIMIT {} OFFSET {offset}",
                        self.page_size
                    );
                    match sqlx::query(&paged).fetch_all(&pool).await {
                        Ok(rows) => {
                            let n = rows.len() as u64;
                            if let EmitOutcome::DownstreamClosed =
                                self.emit_rows(rows, &sender, &mut max_tracked).await
                            {
                                return Ok(());
                            }
                            if n < self.page_size {
                                break;
                            }
                            offset += self.page_size;
                        }
                        Err(e) => {
                            warn!(error = %e, "jdbc paged query failed, will retry next interval");
                            break;
                        }
                    }
                }
            }

            if let Some(max) = max_tracked {
                sql_last_value = max.to_string_lossy();
                self.persist_last_value(&sql_last_value);
            }

            debug!("jdbc poll cycle complete");
            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(self.interval)) => {}
                () = shutdown.wait() => {
                    info!("jdbc input shutting down");
                    break;
                }
            }
        }
        Ok(())
    }
}

fn input_err(message: String) -> FerroStashError {
    FerroStashError::Input {
        plugin: "jdbc".to_string(),
        message,
    }
}

/// Resolves and translates the connection string. Accepts `jdbc_connection_string`
/// (JDBC-style URL, translated) or a plain `connection_string` (sqlx URL).
fn resolve_connection_string(settings: &serde_json::Value) -> Result<String> {
    let raw = settings
        .get_string("jdbc_connection_string")
        .or_else(|| settings.get_string("connection_string"))
        .ok_or_else(|| {
            input_err(
                "jdbc input requires `jdbc_connection_string` or `connection_string`".to_string(),
            )
        })?;
    Ok(jdbc_to_sqlx_url(&raw))
}

/// Translates a JDBC URL to a sqlx URL by stripping the leading `jdbc:` and
/// normalising the scheme. A URL that is already a sqlx URL is returned as-is.
fn jdbc_to_sqlx_url(s: &str) -> String {
    if let Some(rest) = s.strip_prefix("jdbc:postgresql:") {
        format!("postgres:{rest}")
    } else if let Some(rest) = s.strip_prefix("jdbc:postgres:") {
        format!("postgres:{rest}")
    } else if let Some(rest) = s.strip_prefix("jdbc:mysql:") {
        format!("mysql:{rest}")
    } else if let Some(rest) = s.strip_prefix("jdbc:mariadb:") {
        format!("mysql:{rest}")
    } else if let Some(rest) = s.strip_prefix("jdbc:sqlite:") {
        format!("sqlite:{rest}")
    } else if let Some(rest) = s.strip_prefix("jdbc:") {
        rest.to_string()
    } else {
        s.to_string()
    }
}

/// Best-effort decode of a column value into an [`EventValue`]: try i64, then
/// f64, then bool, then String; fall back to Null (e.g. blobs / unknown types).
fn decode_any_value(row: &AnyRow, idx: usize) -> EventValue {
    if let Ok(v) = row.try_get::<i64, _>(idx) {
        return EventValue::Integer(v);
    }
    if let Ok(v) = row.try_get::<f64, _>(idx) {
        return EventValue::Float(v);
    }
    if let Ok(v) = row.try_get::<bool, _>(idx) {
        return EventValue::Boolean(v);
    }
    if let Ok(v) = row.try_get::<String, _>(idx) {
        return EventValue::String(v);
    }
    EventValue::Null
}

/// Orders two tracked values: numeric when both parse as integers/floats, else
/// lexicographic on their string form.
fn value_gt(a: &EventValue, b: &EventValue) -> bool {
    if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) {
        return x > y;
    }
    if let (Some(x), Some(y)) = (a.as_f64(), b.as_f64()) {
        return x > y;
    }
    a.to_string_lossy() > b.to_string_lossy()
}

/// Parses a `schedule => { every => "10s" }` duration into seconds. Accepts a
/// bare number (seconds) or a `<n><unit>` form (s/m/h/d).
fn parse_every(s: &str) -> Option<u64> {
    let s = s.trim();
    if let Ok(n) = s.parse::<u64>() {
        return Some(n);
    }
    let split = s.find(|c: char| c.is_alphabetic())?;
    let (num, unit) = s.split_at(split);
    let n: u64 = num.trim().parse().ok()?;
    let mult = match unit.trim().to_ascii_lowercase().as_str() {
        "s" | "sec" | "secs" | "second" | "seconds" => 1,
        "m" | "min" | "mins" | "minute" | "minutes" => 60,
        "h" | "hour" | "hours" => 3600,
        "d" | "day" | "days" => 86400,
        _ => return None,
    };
    Some(n.saturating_mul(mult))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn translates_jdbc_urls() {
        assert_eq!(
            jdbc_to_sqlx_url("jdbc:postgresql://h:5432/db"),
            "postgres://h:5432/db"
        );
        assert_eq!(jdbc_to_sqlx_url("jdbc:mysql://h/db"), "mysql://h/db");
        assert_eq!(
            jdbc_to_sqlx_url("jdbc:sqlite:/tmp/x.db"),
            "sqlite:/tmp/x.db"
        );
        // Already a sqlx URL: unchanged.
        assert_eq!(jdbc_to_sqlx_url("postgres://h/db"), "postgres://h/db");
        // Unknown jdbc subprotocol: strip the leading jdbc:.
        assert_eq!(jdbc_to_sqlx_url("jdbc:oracle:thin:@h"), "oracle:thin:@h");
    }

    #[test]
    fn from_config_translates_and_requires_fields() {
        let cfg = serde_json::json!({
            "jdbc_connection_string": "jdbc:postgresql://h/db",
            "statement": "SELECT 1"
        });
        let i = JdbcInput::from_config(&cfg).expect("config");
        assert_eq!(i.connection_string, "postgres://h/db");
        assert_eq!(i.name(), "jdbc");

        // Missing connection string.
        assert!(JdbcInput::from_config(&serde_json::json!({ "statement": "SELECT 1" })).is_err());
        // Missing statement.
        assert!(JdbcInput::from_config(
            &serde_json::json!({ "connection_string": "sqlite::memory:" })
        )
        .is_err());
    }

    #[test]
    fn debug_redacts_connection_string() {
        // The connection string embeds DB credentials and must not leak via `{:?}`.
        let i = JdbcInput::from_config(&serde_json::json!({
            "connection_string": "postgres://user:super-secret-pw@h/db",
            "statement": "SELECT 1"
        }))
        .expect("config");
        let dbg = format!("{i:?}");
        assert!(
            !dbg.contains("super-secret-pw"),
            "connection string leaked: {dbg}"
        );
        assert!(!dbg.contains("user:"), "credentials leaked: {dbg}");
        assert!(dbg.contains("***"));
    }

    #[test]
    fn plain_connection_string_accepted() {
        let i = JdbcInput::from_config(&serde_json::json!({
            "connection_string": "mysql://h/db",
            "statement": "SELECT 1"
        }))
        .expect("config");
        assert_eq!(i.connection_string, "mysql://h/db");
    }

    #[test]
    fn defaults_and_floors() {
        let i = JdbcInput::from_config(&serde_json::json!({
            "connection_string": "sqlite::memory:",
            "statement": "SELECT 1"
        }))
        .expect("config");
        assert_eq!(i.interval, 60);
        assert!(i.lowercase_column_names);
        assert!(!i.use_column_value);
        assert!(!i.clean_run);
        assert!(!i.paging_enabled);
        assert_eq!(i.page_size, 100_000);

        // interval floored to >= 1.
        let i = JdbcInput::from_config(&serde_json::json!({
            "connection_string": "sqlite::memory:",
            "statement": "SELECT 1",
            "interval": 0
        }))
        .expect("config");
        assert_eq!(i.interval, 1);
    }

    #[test]
    fn schedule_every_parsed_to_interval() {
        let i = JdbcInput::from_config(&serde_json::json!({
            "connection_string": "sqlite::memory:",
            "statement": "SELECT 1",
            "schedule": { "every": "10s" }
        }))
        .expect("config");
        assert_eq!(i.interval, 10);

        let i = JdbcInput::from_config(&serde_json::json!({
            "connection_string": "sqlite::memory:",
            "statement": "SELECT 1",
            "schedule": { "every": "5m" }
        }))
        .expect("config");
        assert_eq!(i.interval, 300);
    }

    #[test]
    fn parse_every_units() {
        assert_eq!(parse_every("30"), Some(30));
        assert_eq!(parse_every("15s"), Some(15));
        assert_eq!(parse_every("2m"), Some(120));
        assert_eq!(parse_every("1h"), Some(3600));
        assert_eq!(parse_every("1d"), Some(86400));
        assert_eq!(parse_every("nonsense"), None);
    }

    #[test]
    fn value_ordering() {
        // Numeric ordering, not lexicographic ("10" > "9").
        assert!(value_gt(&EventValue::Integer(10), &EventValue::Integer(9)));
        assert!(!value_gt(&EventValue::Integer(9), &EventValue::Integer(10)));
        assert!(value_gt(&EventValue::Float(2.5), &EventValue::Float(1.5)));
        assert!(value_gt(
            &EventValue::String("b".into()),
            &EventValue::String("a".into())
        ));
    }

    /// Live SQLite smoke (no external infra). Creates a temp SQLite DB + table,
    /// runs the input, and asserts the first row is emitted with mapped fields.
    /// Runs only under `--ignored`:
    ///   cargo test -p ferro-stash-input -- --ignored jdbc_input_sqlite
    #[tokio::test]
    #[ignore = "live: creates a temp SQLite DB and runs the jdbc input"]
    async fn jdbc_input_sqlite_smoke() {
        use ferro_stash_core::shutdown::ShutdownController;

        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("input.db");
        let url = format!("sqlite://{}?mode=rwc", db_path.display());

        install_drivers_once();
        let setup = AnyPoolOptions::new()
            .connect(&url)
            .await
            .expect("connect setup");
        sqlx::query(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, score REAL, active BOOLEAN)",
        )
        .execute(&setup)
        .await
        .expect("create table");
        sqlx::query(
            "INSERT INTO items (id, name, score, active) VALUES \
             (1, 'alpha', 1.5, 1), (2, 'beta', 2.5, 0)",
        )
        .execute(&setup)
        .await
        .expect("insert rows");
        setup.close().await;

        let cfg = serde_json::json!({
            "connection_string": url,
            "statement": "SELECT id, name, score FROM items ORDER BY id",
            "interval": 1
        });
        let mut input = JdbcInput::from_config(&cfg).expect("config");
        let (tx, mut rx) = mpsc::channel(16);
        let (controller, signal) = ShutdownController::new();
        let handle = tokio::spawn(async move { input.run(tx, signal).await });

        let ev = tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .expect("timed out waiting for event")
            .expect("channel closed");
        assert_eq!(ev.get("id").and_then(EventValue::as_i64), Some(1));
        assert_eq!(ev.get("name").and_then(EventValue::as_str), Some("alpha"));
        assert!(ev.get("score").and_then(EventValue::as_f64).is_some());

        controller.shutdown();
        let _ = handle.await;
    }
}
