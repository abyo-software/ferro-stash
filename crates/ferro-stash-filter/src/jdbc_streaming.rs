// SPDX-License-Identifier: Apache-2.0
//! `jdbc_streaming` filter — runs a parameterised SQL query **per event** to
//! enrich it with the matching rows, with a small bounded result cache.
//!
//! Mirrors Logstash's `jdbc_streaming` filter for the common lookup case. Like
//! the jdbc input/output, it uses native Rust drivers via [`sqlx`]'s `Any`
//! backend (PostgreSQL / MySQL / SQLite), not a Java JDBC driver; JDBC URLs are
//! accepted and translated (`jdbc:postgresql:` → `postgres:`, etc.).
//!
//! ```logstash
//! filter {
//!   jdbc_streaming {
//!     jdbc_connection_string => "jdbc:postgresql://localhost/app"
//!     statement  => "SELECT name, email FROM users WHERE id = :id"
//!     parameters => { "id" => "%{user_id}" }
//!     target     => "user"
//!     cache_size => 500
//!     cache_expiration => 5
//!   }
//! }
//! ```
//!
//! The `:param` placeholders in `statement` are rewritten to positional binds
//! and the resolved parameter values are **bound** (never string-spliced into
//! the SQL), so event data cannot inject query structure. The matched rows are
//! stored as an array of objects in `target`. On query error the event is tagged
//! `_jdbcstreamingfailure` and otherwise left unchanged.
//!
//! Residuals (honest limitations):
//! - **No Java JDBC drivers** — only sqlx `Any` schemes (postgres / mysql /
//!   sqlite) work; `jdbc_driver_library` / `jdbc_driver_class` are ignored.
//! - **`default_hash` / `tag_on_default_use`** are not implemented; when a query
//!   matches no rows, `target` is set to an empty array.
//! - **Positional binds use `?`** (sqlite / mysql syntax). Strict positional
//!   backends (Postgres `$1`) are reached via the sqlx `Any` layer; bind values
//!   are typed best-effort (int / float / bool / string / NULL).

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::FilterPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use indexmap::IndexMap;
use sqlx::any::{AnyArguments, AnyPoolOptions, AnyRow};
use sqlx::query::Query;
use sqlx::{Any, AnyPool, Column, Row};
use tokio::sync::OnceCell;
use tracing::warn;

// ----- Shared JDBC helpers (reused by the jdbc_static filter) -----

/// Installs sqlx's default `Any` drivers exactly once per process.
pub(crate) fn install_drivers_once() {
    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(sqlx::any::install_default_drivers);
}

/// Translates a JDBC URL to a sqlx URL by stripping the leading `jdbc:` and
/// normalising the scheme. A URL that is already a sqlx URL is returned as-is.
/// Mirrors the translation in the jdbc input/output plugins.
pub(crate) fn jdbc_to_sqlx_url(s: &str) -> String {
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

/// Resolves and translates the connection string. Accepts `jdbc_connection_string`
/// (JDBC-style URL, translated) or a plain `connection_string` (sqlx URL).
pub(crate) fn resolve_connection_string(
    settings: &serde_json::Value,
    plugin: &str,
) -> Result<String> {
    let raw = settings
        .get_string("jdbc_connection_string")
        .or_else(|| settings.get_string("connection_string"))
        .ok_or_else(|| FerroStashError::Filter {
            plugin: plugin.to_string(),
            message: format!(
                "{plugin} filter requires `jdbc_connection_string` or `connection_string`"
            ),
        })?;
    Ok(jdbc_to_sqlx_url(&raw))
}

/// Best-effort decode of a column value into an [`EventValue`]: try i64, then
/// f64, then bool, then String; fall back to Null. Mirrors the jdbc input.
pub(crate) fn decode_any_value(row: &AnyRow, idx: usize) -> EventValue {
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

/// Maps a whole row into an ordered [`EventValue::Object`] keyed by column name.
pub(crate) fn row_to_object(row: &AnyRow) -> EventValue {
    let mut map: IndexMap<String, EventValue> = IndexMap::new();
    for col in row.columns() {
        map.insert(col.name().to_string(), decode_any_value(row, col.ordinal()));
    }
    EventValue::Object(map)
}

/// Binds one resolved parameter value to the next positional placeholder,
/// best-effort by type.
pub(crate) fn bind_value<'q>(
    query: Query<'q, Any, AnyArguments<'q>>,
    value: &EventValue,
) -> Query<'q, Any, AnyArguments<'q>> {
    match value {
        EventValue::Integer(i) => query.bind(*i),
        EventValue::Float(f) => query.bind(*f),
        EventValue::Boolean(b) => query.bind(*b),
        EventValue::Null => query.bind(Option::<String>::None),
        EventValue::String(s) => query.bind(s.clone()),
        other => query.bind(other.to_string_lossy()),
    }
}

/// Rewrites `:name` placeholders to positional `?` binds, returning the rewritten
/// SQL and the parameter names in order of appearance. A `::` (e.g. a Postgres
/// type cast) is preserved literally and never treated as a parameter.
pub(crate) fn rewrite_named_params(sql: &str) -> (String, Vec<String>) {
    let mut out = String::with_capacity(sql.len());
    let mut params = Vec::new();
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i] as char;
        if c == ':' {
            // Preserve `::` casts verbatim.
            if i + 1 < bytes.len() && bytes[i + 1] == b':' {
                out.push_str("::");
                i += 2;
                continue;
            }
            // A named parameter must start with an ASCII letter or '_'.
            if i + 1 < bytes.len() && (bytes[i + 1].is_ascii_alphabetic() || bytes[i + 1] == b'_') {
                let start = i + 1;
                let mut j = start;
                while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
                    j += 1;
                }
                params.push(sql[start..j].to_string());
                out.push('?');
                i = j;
                continue;
            }
        }
        out.push(c);
        i += 1;
    }
    (out, params)
}

// ----- jdbc_streaming filter -----

/// A cached result set with its insertion time (for expiry).
struct CacheEntry {
    rows: Vec<EventValue>,
    inserted: Instant,
}

/// FIFO-bounded, time-expiring result cache.
struct ResultCache {
    map: HashMap<String, CacheEntry>,
    order: VecDeque<String>,
    capacity: usize,
    expiration: Option<Duration>,
}

impl ResultCache {
    fn new(capacity: usize, expiration: Option<Duration>) -> Self {
        Self {
            map: HashMap::new(),
            order: VecDeque::new(),
            capacity,
            expiration,
        }
    }

    fn get(&mut self, key: &str) -> Option<Vec<EventValue>> {
        if self.capacity == 0 {
            return None;
        }
        let expired = self
            .map
            .get(key)
            .map(|e| {
                self.expiration
                    .is_some_and(|ttl| e.inserted.elapsed() > ttl)
            })
            .unwrap_or(false);
        if expired {
            self.map.remove(key);
            self.order.retain(|k| k != key);
            return None;
        }
        self.map.get(key).map(|e| e.rows.clone())
    }

    fn insert(&mut self, key: String, rows: Vec<EventValue>) {
        if self.capacity == 0 {
            return;
        }
        if !self.map.contains_key(&key) {
            while self.order.len() >= self.capacity {
                if let Some(old) = self.order.pop_front() {
                    self.map.remove(&old);
                } else {
                    break;
                }
            }
            self.order.push_back(key.clone());
        }
        self.map.insert(
            key,
            CacheEntry {
                rows,
                inserted: Instant::now(),
            },
        );
    }
}

pub struct JdbcStreamingFilter {
    connection_string: String,
    /// SQL with `:name` rewritten to positional `?` binds.
    sql: String,
    /// Parameter names in bind order (as they appear in the statement).
    param_order: Vec<String>,
    /// Config map: param name → event-field name or `%{}` template.
    parameters: HashMap<String, String>,
    target: String,
    cache: Mutex<ResultCache>,
    pool: OnceCell<AnyPool>,
    condition: Option<Condition>,
}

/// Manual `Debug` that redacts the connection string (it may embed a password).
impl std::fmt::Debug for JdbcStreamingFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JdbcStreamingFilter")
            .field("connection_string", &"***")
            .field("sql", &self.sql)
            .field("param_order", &self.param_order)
            .field("parameters", &self.parameters)
            .field("target", &self.target)
            .field("condition", &self.condition)
            .finish()
    }
}

impl JdbcStreamingFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let connection_string = resolve_connection_string(settings, "jdbc_streaming")?;
        let statement = settings.get_string("statement").ok_or_else(|| {
            filter_err("jdbc_streaming filter requires a `statement`".to_string())
        })?;
        let (sql, param_order) = rewrite_named_params(&statement);

        let parameters = settings
            .get("parameters")
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();

        let target = settings
            .get_string("target")
            .unwrap_or_else(|| "[jdbc_streaming]".to_string());

        let cache_size = settings.get_u64("cache_size").unwrap_or(500) as usize;
        let cache_expiration = settings
            .get_f64("cache_expiration")
            .filter(|s| *s > 0.0)
            .map(Duration::from_secs_f64);

        Ok(Self {
            connection_string,
            sql,
            param_order,
            parameters,
            target,
            cache: Mutex::new(ResultCache::new(cache_size, cache_expiration)),
            pool: OnceCell::new(),
            condition,
        })
    }

    async fn pool(&self) -> Result<&AnyPool> {
        self.pool
            .get_or_try_init(|| async {
                install_drivers_once();
                AnyPoolOptions::new()
                    .max_connections(4)
                    .connect(&self.connection_string)
                    .await
                    .map_err(|e| filter_err(format!("connect failed: {e}")))
            })
            .await
    }

    /// Resolves a single parameter's value against the event.
    fn resolve_param(&self, name: &str, event: &Event) -> EventValue {
        match self.parameters.get(name) {
            Some(raw) if raw.contains("%{") => EventValue::String(event.sprintf(raw)),
            Some(raw) => event
                .get(raw)
                .cloned()
                .unwrap_or_else(|| EventValue::String(raw.clone())),
            // No mapping configured for this placeholder: fall back to a same-named
            // event field, else bind NULL.
            None => event.get(name).cloned().unwrap_or(EventValue::Null),
        }
    }

    fn resolve_all(&self, event: &Event) -> Vec<EventValue> {
        self.param_order
            .iter()
            .map(|name| self.resolve_param(name, event))
            .collect()
    }
}

#[async_trait]
impl FilterPlugin for JdbcStreamingFilter {
    fn name(&self) -> &'static str {
        "jdbc_streaming"
    }

    async fn filter(&self, mut event: Event) -> Result<Vec<Event>> {
        let values = self.resolve_all(&event);
        let cache_key: String = values
            .iter()
            .map(EventValue::to_string_lossy)
            .collect::<Vec<_>>()
            .join("\u{1}");

        // Cache lookup (guard dropped before any await).
        if let Some(rows) = cache_lock(&self.cache).get(&cache_key) {
            event.set(self.target.clone(), EventValue::Array(rows));
            return Ok(vec![event]);
        }

        let pool = match self.pool().await {
            Ok(p) => p,
            Err(e) => {
                warn!(error = %e, "jdbc_streaming: connection failed");
                event.add_tag("_jdbcstreamingfailure");
                return Ok(vec![event]);
            }
        };

        let mut query = sqlx::query::<Any>(&self.sql);
        for value in &values {
            query = bind_value(query, value);
        }

        match query.fetch_all(pool).await {
            Ok(rows) => {
                let mapped: Vec<EventValue> = rows.iter().map(row_to_object).collect();
                cache_lock(&self.cache).insert(cache_key, mapped.clone());
                event.set(self.target.clone(), EventValue::Array(mapped));
            }
            Err(e) => {
                warn!(error = %e, "jdbc_streaming: query failed");
                event.add_tag("_jdbcstreamingfailure");
            }
        }

        Ok(vec![event])
    }

    fn condition(&self) -> Option<&Condition> {
        self.condition.as_ref()
    }
}

/// Locks the cache, recovering from a poisoned mutex rather than panicking.
fn cache_lock(cache: &Mutex<ResultCache>) -> std::sync::MutexGuard<'_, ResultCache> {
    cache
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn filter_err(message: String) -> FerroStashError {
    FerroStashError::Filter {
        plugin: "jdbc_streaming".to_string(),
        message,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrites_named_params_in_order() {
        let (sql, params) =
            rewrite_named_params("SELECT * FROM t WHERE a = :a AND b = :b OR a = :a");
        assert_eq!(sql, "SELECT * FROM t WHERE a = ? AND b = ? OR a = ?");
        assert_eq!(params, vec!["a", "b", "a"]);
    }

    #[test]
    fn rewrite_preserves_casts() {
        let (sql, params) = rewrite_named_params("SELECT x::int FROM t WHERE id = :id");
        assert_eq!(sql, "SELECT x::int FROM t WHERE id = ?");
        assert_eq!(params, vec!["id"]);
    }

    #[test]
    fn translates_jdbc_urls() {
        assert_eq!(
            jdbc_to_sqlx_url("jdbc:postgresql://h/db"),
            "postgres://h/db"
        );
        assert_eq!(jdbc_to_sqlx_url("sqlite::memory:"), "sqlite::memory:");
    }

    #[test]
    fn from_config_requires_fields() {
        // Missing connection string.
        assert!(JdbcStreamingFilter::from_config(
            &serde_json::json!({ "statement": "SELECT 1" }),
            None
        )
        .is_err());
        // Missing statement.
        assert!(JdbcStreamingFilter::from_config(
            &serde_json::json!({ "connection_string": "sqlite::memory:" }),
            None
        )
        .is_err());
        let f = JdbcStreamingFilter::from_config(
            &serde_json::json!({
                "connection_string": "sqlite::memory:",
                "statement": "SELECT name FROM users WHERE id = :id",
                "parameters": { "id": "%{uid}" }
            }),
            None,
        )
        .expect("config");
        assert_eq!(f.sql, "SELECT name FROM users WHERE id = ?");
        assert_eq!(f.param_order, vec!["id"]);
        assert_eq!(f.target, "[jdbc_streaming]");
        assert_eq!(f.name(), "jdbc_streaming");
        // Debug must not leak the connection string.
        assert!(!format!("{f:?}").contains("sqlite"));
    }

    fn cache_only(capacity: usize) -> ResultCache {
        ResultCache::new(capacity, None)
    }

    #[test]
    fn cache_evicts_fifo_and_respects_zero_capacity() {
        let mut c = cache_only(2);
        c.insert("a".into(), vec![EventValue::Integer(1)]);
        c.insert("b".into(), vec![EventValue::Integer(2)]);
        c.insert("c".into(), vec![EventValue::Integer(3)]); // evicts "a"
        assert!(c.get("a").is_none());
        assert!(c.get("b").is_some());
        assert!(c.get("c").is_some());

        let mut z = cache_only(0);
        z.insert("k".into(), vec![EventValue::Integer(1)]);
        assert!(z.get("k").is_none(), "zero-capacity cache stores nothing");
    }

    /// Real SQLite enrichment (no external infra): create a table + rows, then
    /// run the filter and assert the matched row lands in `target`.
    #[tokio::test]
    async fn jdbc_streaming_enriches_from_sqlite() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("stream.db");
        let url = format!("sqlite://{}?mode=rwc", db_path.display());

        install_drivers_once();
        let setup = AnyPoolOptions::new()
            .connect(&url)
            .await
            .expect("connect setup");
        sqlx::query("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
            .execute(&setup)
            .await
            .expect("create");
        sqlx::query(
            "INSERT INTO users (id, name, email) VALUES (1, 'alice', 'a@x'), (2, 'bob', 'b@x')",
        )
        .execute(&setup)
        .await
        .expect("insert");
        setup.close().await;

        let filter = JdbcStreamingFilter::from_config(
            &serde_json::json!({
                "connection_string": url,
                "statement": "SELECT name, email FROM users WHERE id = :id",
                "parameters": { "id": "uid" },
                "target": "user"
            }),
            None,
        )
        .expect("config");

        let mut event = Event::new("test");
        event.set("uid", EventValue::Integer(1));
        let out = filter.filter(event).await.expect("filter");
        assert!(!out[0].has_tag("_jdbcstreamingfailure"));
        let rows = out[0]
            .get("user")
            .and_then(EventValue::as_array)
            .expect("array");
        assert_eq!(rows.len(), 1);
        if let EventValue::Object(obj) = &rows[0] {
            assert_eq!(obj.get("name"), Some(&EventValue::String("alice".into())));
            assert_eq!(obj.get("email"), Some(&EventValue::String("a@x".into())));
        } else {
            panic!("expected object row, got {:?}", rows[0]);
        }

        // A miss returns an empty array (query succeeded, no rows) and no failure tag.
        let mut miss = Event::new("test");
        miss.set("uid", EventValue::Integer(999));
        let out = filter.filter(miss).await.expect("filter");
        assert!(!out[0].has_tag("_jdbcstreamingfailure"));
        assert_eq!(
            out[0]
                .get("user")
                .and_then(EventValue::as_array)
                .map(Vec::len),
            Some(0)
        );
    }

    #[tokio::test]
    async fn jdbc_streaming_bad_connection_tags_failure() {
        let filter = JdbcStreamingFilter::from_config(
            &serde_json::json!({
                "connection_string": "sqlite:///nonexistent/dir/nope.db?mode=rw",
                "statement": "SELECT 1 WHERE 1 = :n",
                "parameters": { "n": "%{n}" }
            }),
            None,
        )
        .expect("config");
        let mut event = Event::new("test");
        event.set("n", EventValue::Integer(1));
        let out = filter.filter(event).await.expect("filter");
        assert!(out[0].has_tag("_jdbcstreamingfailure"));
    }
}
