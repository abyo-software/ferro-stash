// SPDX-License-Identifier: Apache-2.0
//! `jdbc_static` filter — loads reference data into memory once at startup (and
//! optionally refreshes it on an interval), then enriches events via in-memory
//! key lookups. A faithful **subset** of Logstash's `jdbc_static` filter.
//!
//! Mirrors the Logstash plugin's core value: avoid a per-event database round
//! trip by pre-loading lookup tables. Like the jdbc input/output, it uses native
//! Rust drivers via [`sqlx`]'s `Any` backend (PostgreSQL / MySQL / SQLite).
//!
//! ```logstash
//! filter {
//!   jdbc_static {
//!     connection_string => "jdbc:postgresql://localhost/app"
//!     loaders => [
//!       { id => "users", query => "SELECT id, name, dept FROM users" }
//!     ]
//!     local_lookups => [
//!       { id => "find_user", loader => "users", key_column => "id",
//!         query => "%{user_id}", target => "user" }
//!     ]
//!     refresh_interval => 300   # optional; re-runs loaders when this elapses
//!   }
//! }
//! ```
//!
//! Per loader the `query` is run once and its rows are held in memory; for each
//! `local_lookups` entry an index is built keyed by `key_column` (default: the
//! loader's first column). For each event the lookup's `query` (a `%{}` template,
//! or a bare field name) is resolved to a key and the matched rows are written to
//! `target` as an array of objects.
//!
//! Residuals (honest limitations — the parts of Logstash `jdbc_static` NOT
//! implemented):
//! - **No local in-memory SQL database** (Logstash stages loaders into a local
//!   Derby/Apache-Calcite DB and runs arbitrary SQL `local_lookups` against it,
//!   including joins). Here a `local_lookups` entry is a single keyed lookup
//!   against one loader's rows — no SQL, no joins, no `local_db_objects`.
//! - **One key per lookup** (`key_column` + a single resolved key value); multi
//!   -column composite keys and SQL `parameters` arrays are not supported.
//! - **`refresh_interval` is lazy** — loaders are reloaded on the first event
//!   after the interval elapses, not by a background timer.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::FilterPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use sqlx::any::AnyPoolOptions;
use sqlx::AnyPool;
use tokio::sync::{OnceCell, RwLock};
use tracing::warn;

use crate::jdbc_streaming::{install_drivers_once, resolve_connection_string, row_to_object};

/// Default cap on the number of rows a single loader query may return. The whole
/// reference table is held in memory, so an unbounded loader (a typo'd query
/// against a huge table) could OOM the process. A loader that exceeds this fails
/// loud (a load error → `_jdbcstaticfailure`) rather than exhausting memory.
const DEFAULT_MAX_LOADER_ROWS: usize = 1_000_000;

/// A startup loader: a SQL `query` whose rows are held in memory under `id`.
#[derive(Debug, Clone)]
struct Loader {
    id: String,
    query: String,
}

/// An in-memory keyed lookup against one loader's rows.
#[derive(Debug, Clone)]
struct Lookup {
    /// Which loader's rows to search.
    loader_id: String,
    /// Column whose value is the lookup key (default: the loader's first column).
    key_column: Option<String>,
    /// Event key source: a `%{}` template, or a bare field name.
    key_template: String,
    /// Event field the matched rows are written to.
    target: String,
}

/// Loaded reference data: one key→rows index per configured lookup (aligned by
/// position with `JdbcStaticFilter::lookups`).
struct LoadedState {
    indexes: Vec<HashMap<String, Vec<EventValue>>>,
    loaded_at: Instant,
}

pub struct JdbcStaticFilter {
    connection_string: String,
    loaders: Vec<Loader>,
    lookups: Vec<Lookup>,
    refresh_interval: Option<Duration>,
    /// Max rows a single loader query may return before the load fails loud.
    max_loader_rows: usize,
    state: RwLock<Option<LoadedState>>,
    pool: OnceCell<AnyPool>,
    condition: Option<Condition>,
}

/// Manual `Debug` that redacts the connection string (it may embed a password).
impl std::fmt::Debug for JdbcStaticFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JdbcStaticFilter")
            .field("connection_string", &"***")
            .field("loaders", &self.loaders)
            .field("lookups", &self.lookups)
            .field("refresh_interval", &self.refresh_interval)
            .field("condition", &self.condition)
            .finish()
    }
}

impl JdbcStaticFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let connection_string = resolve_connection_string(settings, "jdbc_static")?;

        let loaders = parse_loaders(settings)?;
        if loaders.is_empty() {
            return Err(filter_err(
                "jdbc_static filter requires at least one `loaders` entry".to_string(),
            ));
        }
        let lookups = parse_lookups(settings, &loaders)?;
        if lookups.is_empty() {
            return Err(filter_err(
                "jdbc_static filter requires at least one `local_lookups` entry".to_string(),
            ));
        }

        let refresh_interval = settings
            .get_u64("refresh_interval")
            .filter(|s| *s > 0)
            .map(Duration::from_secs);

        // Optional per-loader row cap (additive to the Logstash keys); defaults to
        // a safe bound so a runaway loader query fails loud instead of OOMing.
        let max_loader_rows = settings
            .get_u64("loader_max_rows")
            .filter(|n| *n > 0)
            .map(|n| n as usize)
            .unwrap_or(DEFAULT_MAX_LOADER_ROWS);

        Ok(Self {
            connection_string,
            loaders,
            lookups,
            refresh_interval,
            max_loader_rows,
            state: RwLock::new(None),
            pool: OnceCell::new(),
            condition,
        })
    }

    async fn pool(&self) -> Result<&AnyPool> {
        self.pool
            .get_or_try_init(|| async {
                install_drivers_once();
                AnyPoolOptions::new()
                    .max_connections(2)
                    .connect(&self.connection_string)
                    .await
                    .map_err(|e| filter_err(format!("connect failed: {e}")))
            })
            .await
    }

    /// True when `state` is present and still within the refresh window.
    fn is_fresh(&self, state: Option<&LoadedState>) -> bool {
        match state {
            None => false,
            Some(s) => self
                .refresh_interval
                .map_or(true, |ttl| s.loaded_at.elapsed() <= ttl),
        }
    }

    /// Ensures reference data is loaded (and refreshed if stale).
    async fn ensure_loaded(&self) -> Result<()> {
        if self.is_fresh(self.state.read().await.as_ref()) {
            return Ok(());
        }
        let mut guard = self.state.write().await;
        // Re-check under the write lock (another task may have just loaded).
        if self.is_fresh(guard.as_ref()) {
            return Ok(());
        }
        let loaded = self.load().await?;
        *guard = Some(loaded);
        Ok(())
    }

    /// Runs all loader queries and builds the per-lookup key→rows indexes.
    async fn load(&self) -> Result<LoadedState> {
        let pool = self.pool().await?;

        let mut loader_rows: HashMap<String, Vec<EventValue>> = HashMap::new();
        for loader in &self.loaders {
            // Cap the fetch at `max_loader_rows + 1` rows at the database level so
            // a runaway loader never materializes more than the cap in memory.
            let base = loader.query.trim().trim_end_matches(';');
            let capped = format!(
                "SELECT * FROM ({base}) AS ferro_stash_loader LIMIT {}",
                self.max_loader_rows.saturating_add(1)
            );
            let rows = sqlx::query(&capped)
                .fetch_all(pool)
                .await
                .map_err(|e| filter_err(format!("loader '{}' query failed: {e}", loader.id)))?;
            if rows.len() > self.max_loader_rows {
                return Err(filter_err(format!(
                    "loader '{}' returned more than {} rows (the cap); refusing to load to \
                     avoid OOM — add a WHERE/LIMIT to the loader query or raise `loader_max_rows`",
                    loader.id, self.max_loader_rows
                )));
            }
            let mapped: Vec<EventValue> = rows.iter().map(row_to_object).collect();
            loader_rows.insert(loader.id.clone(), mapped);
        }

        let mut indexes = Vec::with_capacity(self.lookups.len());
        for lookup in &self.lookups {
            let rows = loader_rows
                .get(&lookup.loader_id)
                .cloned()
                .unwrap_or_default();
            let mut index: HashMap<String, Vec<EventValue>> = HashMap::new();
            for row in rows {
                if let EventValue::Object(obj) = &row {
                    let key_value = match &lookup.key_column {
                        Some(col) => obj.get(col),
                        None => obj.values().next(),
                    };
                    if let Some(kv) = key_value {
                        index
                            .entry(kv.to_string_lossy())
                            .or_default()
                            .push(row.clone());
                    }
                }
            }
            indexes.push(index);
        }

        Ok(LoadedState {
            indexes,
            loaded_at: Instant::now(),
        })
    }
}

#[async_trait]
impl FilterPlugin for JdbcStaticFilter {
    fn name(&self) -> &'static str {
        "jdbc_static"
    }

    async fn filter(&self, mut event: Event) -> Result<Vec<Event>> {
        if let Err(e) = self.ensure_loaded().await {
            warn!(error = %e, "jdbc_static: load failed");
            event.add_tag("_jdbcstaticfailure");
            return Ok(vec![event]);
        }

        let guard = self.state.read().await;
        let Some(state) = guard.as_ref() else {
            event.add_tag("_jdbcstaticfailure");
            return Ok(vec![event]);
        };

        for (i, lookup) in self.lookups.iter().enumerate() {
            let key = resolve_key(&lookup.key_template, &event);
            let matched = state
                .indexes
                .get(i)
                .and_then(|idx| idx.get(&key))
                .cloned()
                .unwrap_or_default();
            event.set(lookup.target.clone(), EventValue::Array(matched));
        }

        Ok(vec![event])
    }

    fn condition(&self) -> Option<&Condition> {
        self.condition.as_ref()
    }
}

/// Resolves a lookup key from the event: a `%{}` template is interpolated; a
/// bare field name reads that field; otherwise the literal is used.
fn resolve_key(template: &str, event: &Event) -> String {
    if template.contains("%{") {
        event.sprintf(template)
    } else if let Some(v) = event.get(template) {
        v.to_string_lossy()
    } else {
        template.to_string()
    }
}

fn parse_loaders(settings: &serde_json::Value) -> Result<Vec<Loader>> {
    let arr = settings
        .get("loaders")
        .and_then(|v| v.as_array())
        .ok_or_else(|| filter_err("jdbc_static filter requires a `loaders` array".to_string()))?;
    arr.iter()
        .map(|entry| {
            let id = entry
                .get("id")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .ok_or_else(|| filter_err("each `loaders` entry requires an `id`".to_string()))?
                .to_string();
            let query = entry
                .get("query")
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| filter_err(format!("loader '{id}' requires a non-empty `query`")))?
                .to_string();
            Ok(Loader { id, query })
        })
        .collect()
}

fn parse_lookups(settings: &serde_json::Value, loaders: &[Loader]) -> Result<Vec<Lookup>> {
    let arr = settings
        .get("local_lookups")
        .and_then(|v| v.as_array())
        .ok_or_else(|| {
            filter_err("jdbc_static filter requires a `local_lookups` array".to_string())
        })?;
    arr.iter()
        .map(|entry| {
            let id = entry.get("id").and_then(|v| v.as_str()).map(String::from);
            // Which loader's rows to search: `loader` / `local_table`, else the
            // single loader when only one is configured.
            let loader_id = entry
                .get("loader")
                .or_else(|| entry.get("local_table"))
                .and_then(|v| v.as_str())
                .map(String::from)
                .or_else(|| {
                    if loaders.len() == 1 {
                        Some(loaders[0].id.clone())
                    } else {
                        None
                    }
                })
                .ok_or_else(|| {
                    filter_err(
                        "each `local_lookups` entry requires a `loader` (which loader's rows \
                         to search) when more than one loader is configured"
                            .to_string(),
                    )
                })?;
            if !loaders.iter().any(|l| l.id == loader_id) {
                return Err(filter_err(format!(
                    "local_lookups references unknown loader '{loader_id}'"
                )));
            }
            // Event key source.
            let key_template = entry
                .get("query")
                .or_else(|| entry.get("source"))
                .or_else(|| entry.get("field"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    filter_err(
                        "each `local_lookups` entry requires a `query` (key %{}-template or \
                         field name)"
                            .to_string(),
                    )
                })?
                .to_string();
            let key_column = entry
                .get("key_column")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(String::from);
            // Target defaults to the lookup id, else the loader id.
            let target = entry
                .get("target")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(String::from)
                .or_else(|| id.clone())
                .unwrap_or_else(|| loader_id.clone());
            Ok(Lookup {
                loader_id,
                key_column,
                key_template,
                target,
            })
        })
        .collect()
}

fn filter_err(message: String) -> FerroStashError {
    FerroStashError::Filter {
        plugin: "jdbc_static".to_string(),
        message,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_config_requires_loaders_and_lookups() {
        // No loaders.
        assert!(JdbcStaticFilter::from_config(
            &serde_json::json!({ "connection_string": "sqlite::memory:" }),
            None
        )
        .is_err());
        // Loaders but no lookups.
        assert!(JdbcStaticFilter::from_config(
            &serde_json::json!({
                "connection_string": "sqlite::memory:",
                "loaders": [{ "id": "a", "query": "SELECT 1" }]
            }),
            None
        )
        .is_err());
        // Valid: single loader → lookup loader defaults to it.
        let f = JdbcStaticFilter::from_config(
            &serde_json::json!({
                "connection_string": "sqlite::memory:",
                "loaders": [{ "id": "users", "query": "SELECT id, name FROM users" }],
                "local_lookups": [{ "id": "u", "query": "%{uid}", "target": "user" }]
            }),
            None,
        )
        .expect("config");
        assert_eq!(f.loaders.len(), 1);
        assert_eq!(f.lookups.len(), 1);
        assert_eq!(f.lookups[0].loader_id, "users");
        assert_eq!(f.lookups[0].target, "user");
        assert_eq!(f.name(), "jdbc_static");
        // Debug must not leak the connection string.
        assert!(!format!("{f:?}").contains("sqlite"));
    }

    #[test]
    fn unknown_loader_reference_rejected() {
        assert!(JdbcStaticFilter::from_config(
            &serde_json::json!({
                "connection_string": "sqlite::memory:",
                "loaders": [
                    { "id": "a", "query": "SELECT 1" },
                    { "id": "b", "query": "SELECT 1" }
                ],
                "local_lookups": [{ "loader": "ghost", "query": "%{x}", "target": "t" }]
            }),
            None
        )
        .is_err());
        // Multiple loaders without an explicit loader ref is ambiguous → error.
        assert!(JdbcStaticFilter::from_config(
            &serde_json::json!({
                "connection_string": "sqlite::memory:",
                "loaders": [
                    { "id": "a", "query": "SELECT 1" },
                    { "id": "b", "query": "SELECT 1" }
                ],
                "local_lookups": [{ "query": "%{x}", "target": "t" }]
            }),
            None
        )
        .is_err());
    }

    /// Real SQLite enrichment (no external infra): load a reference table at
    /// startup, then enrich an event via an in-memory keyed lookup.
    #[tokio::test]
    async fn jdbc_static_enriches_from_loaded_table() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("static.db");
        let url = format!("sqlite://{}?mode=rwc", db_path.display());

        install_drivers_once();
        let setup = AnyPoolOptions::new()
            .connect(&url)
            .await
            .expect("connect setup");
        sqlx::query("CREATE TABLE depts (id INTEGER PRIMARY KEY, name TEXT, floor INTEGER)")
            .execute(&setup)
            .await
            .expect("create");
        sqlx::query("INSERT INTO depts (id, name, floor) VALUES (1, 'eng', 3), (2, 'sales', 1)")
            .execute(&setup)
            .await
            .expect("insert");
        setup.close().await;

        let filter = JdbcStaticFilter::from_config(
            &serde_json::json!({
                "connection_string": url,
                "loaders": [{ "id": "depts", "query": "SELECT id, name, floor FROM depts" }],
                "local_lookups": [{
                    "id": "find_dept",
                    "loader": "depts",
                    "key_column": "id",
                    "query": "%{dept_id}",
                    "target": "dept"
                }]
            }),
            None,
        )
        .expect("config");

        let mut event = Event::new("test");
        event.set("dept_id", EventValue::Integer(1));
        let out = filter.filter(event).await.expect("filter");
        assert!(!out[0].has_tag("_jdbcstaticfailure"));
        let rows = out[0]
            .get("dept")
            .and_then(EventValue::as_array)
            .expect("array");
        assert_eq!(rows.len(), 1);
        if let EventValue::Object(obj) = &rows[0] {
            assert_eq!(obj.get("name"), Some(&EventValue::String("eng".into())));
            assert_eq!(obj.get("floor"), Some(&EventValue::Integer(3)));
        } else {
            panic!("expected object row, got {:?}", rows[0]);
        }

        // A miss writes an empty array (and no failure tag).
        let mut miss = Event::new("test");
        miss.set("dept_id", EventValue::Integer(99));
        let out = filter.filter(miss).await.expect("filter");
        assert!(!out[0].has_tag("_jdbcstaticfailure"));
        assert_eq!(
            out[0]
                .get("dept")
                .and_then(EventValue::as_array)
                .map(Vec::len),
            Some(0)
        );
    }

    #[tokio::test]
    async fn jdbc_static_loader_row_cap_fails_loud() {
        // A loader returning more rows than `loader_max_rows` must fail loud (a
        // load error → `_jdbcstaticfailure`) rather than load unbounded into RAM.
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("cap.db");
        let url = format!("sqlite://{}?mode=rwc", db_path.display());

        install_drivers_once();
        let setup = AnyPoolOptions::new()
            .connect(&url)
            .await
            .expect("connect setup");
        sqlx::query("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .execute(&setup)
            .await
            .expect("create");
        sqlx::query("INSERT INTO t (id) VALUES (1), (2), (3), (4), (5)")
            .execute(&setup)
            .await
            .expect("insert");
        setup.close().await;

        let filter = JdbcStaticFilter::from_config(
            &serde_json::json!({
                "connection_string": url,
                "loaders": [{ "id": "t", "query": "SELECT id FROM t" }],
                "local_lookups": [{ "query": "%{k}", "target": "out" }],
                "loader_max_rows": 2
            }),
            None,
        )
        .expect("config");
        let out = filter.filter(Event::new("x")).await.expect("filter");
        assert!(
            out[0].has_tag("_jdbcstaticfailure"),
            "over-cap loader must fail loud"
        );
    }

    #[tokio::test]
    async fn jdbc_static_bad_loader_tags_failure() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("bad.db");
        let url = format!("sqlite://{}?mode=rwc", db_path.display());
        let filter = JdbcStaticFilter::from_config(
            &serde_json::json!({
                "connection_string": url,
                "loaders": [{ "id": "x", "query": "SELECT * FROM does_not_exist" }],
                "local_lookups": [{ "query": "%{k}", "target": "t" }]
            }),
            None,
        )
        .expect("config");
        let out = filter.filter(Event::new("test")).await.expect("filter");
        assert!(out[0].has_tag("_jdbcstaticfailure"));
    }
}
