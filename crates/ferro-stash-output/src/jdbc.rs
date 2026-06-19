// SPDX-License-Identifier: Apache-2.0
//! JDBC output — executes a parameterised SQL `statement` once per event,
//! binding event fields to the `?` placeholders. Mirrors Logstash's `jdbc`
//! output for the common INSERT/UPDATE case.
//!
//! Like the jdbc input, this uses native Rust drivers via [`sqlx`]'s `Any`
//! backend (PostgreSQL / MySQL / SQLite), not a Java JDBC driver. JDBC URLs are
//! accepted and translated (`jdbc:postgresql:` → `postgres:`, etc.).
//!
//! The `statement` is a JSON array: element 0 is the SQL with `?` placeholders,
//! the remaining elements are event field names bound (in order) to those
//! placeholders.
//!
//! ```logstash
//! output {
//!   jdbc {
//!     connection_string => "postgresql://localhost:5432/app"
//!     statement => [ "INSERT INTO events (host, level, msg) VALUES (?, ?, ?)",
//!                    "host", "level", "message" ]
//!   }
//! }
//! ```
//!
//! Residuals (honest limitations):
//! - **No Java JDBC drivers.** Only sqlx `Any` schemes (postgres / mysql /
//!   sqlite) work; `jdbc_driver_library` / `jdbc_driver_class` are ignored.
//! - **One execute per event** (no multi-row batching / prepared-statement
//!   reuse across the batch yet).
//! - **Best-effort bind types**: int / float / bool / string; a missing or null
//!   field binds SQL NULL; arrays/objects bind their JSON-string form.

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::OutputPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use sqlx::any::{AnyArguments, AnyPoolOptions};
use sqlx::query::Query;
use sqlx::{Any, AnyPool};
use tokio::sync::OnceCell;

/// Installs sqlx's default `Any` drivers exactly once per process.
fn install_drivers_once() {
    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(sqlx::any::install_default_drivers);
}

#[derive(Debug)]
pub struct JdbcOutput {
    connection_string: String,
    sql: String,
    /// Event field names bound, in order, to the `?` placeholders in `sql`.
    fields: Vec<String>,
    condition: Option<Condition>,
    pool: OnceCell<AnyPool>,
}

impl JdbcOutput {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let connection_string = resolve_connection_string(settings)?;
        let (sql, fields) = parse_statement(settings)?;
        Ok(Self {
            connection_string,
            sql,
            fields,
            condition,
            pool: OnceCell::new(),
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
                    .map_err(|e| output_err(format!("connect failed: {e}")))
            })
            .await
    }
}

#[async_trait]
impl OutputPlugin for JdbcOutput {
    fn name(&self) -> &str {
        "jdbc"
    }

    async fn output(&self, events: Vec<Event>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let pool = self.pool().await?;
        for event in events {
            let mut query = sqlx::query::<Any>(&self.sql);
            for field in &self.fields {
                query = bind_field(query, event.get(field));
            }
            query
                .execute(pool)
                .await
                .map_err(|e| output_err(format!("execute failed: {e}")))?;
        }
        Ok(())
    }

    fn condition(&self) -> Option<&Condition> {
        self.condition.as_ref()
    }
}

fn output_err(message: String) -> FerroStashError {
    FerroStashError::Output {
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
            output_err(
                "jdbc output requires `jdbc_connection_string` or `connection_string`".to_string(),
            )
        })?;
    Ok(jdbc_to_sqlx_url(&raw))
}

/// Parses the `statement` into the SQL string and the ordered bind-field list.
/// Accepts a JSON array `["SQL", "field_a", "field_b"]` or a bare SQL string
/// (no binds).
fn parse_statement(settings: &serde_json::Value) -> Result<(String, Vec<String>)> {
    match settings.get("statement") {
        Some(serde_json::Value::Array(arr)) => {
            let mut it = arr.iter();
            let sql = it
                .next()
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    output_err("jdbc output `statement` array element 0 must be the SQL string".to_string())
                })?
                .to_string();
            let fields = it
                .map(|v| {
                    v.as_str().map(String::from).ok_or_else(|| {
                        output_err("jdbc output `statement` field names must be strings".to_string())
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok((sql, fields))
        }
        Some(serde_json::Value::String(s)) => Ok((s.clone(), Vec::new())),
        _ => Err(output_err(
            "jdbc output requires `statement` (a [SQL, field, ...] array or a SQL string)"
                .to_string(),
        )),
    }
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

/// Binds a single event field to the next `?` placeholder, best-effort by type.
fn bind_field<'q>(
    query: Query<'q, Any, AnyArguments<'q>>,
    value: Option<&EventValue>,
) -> Query<'q, Any, AnyArguments<'q>> {
    match value {
        Some(EventValue::Integer(i)) => query.bind(*i),
        Some(EventValue::Float(f)) => query.bind(*f),
        Some(EventValue::Boolean(b)) => query.bind(*b),
        Some(EventValue::String(s)) => query.bind(s.clone()),
        None | Some(EventValue::Null) => query.bind(Option::<String>::None),
        // Arrays/objects: bind their JSON-string representation.
        Some(other) => query.bind(other.to_string_lossy()),
    }
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
        assert_eq!(jdbc_to_sqlx_url("jdbc:sqlite:/tmp/x.db"), "sqlite:/tmp/x.db");
        assert_eq!(jdbc_to_sqlx_url("mysql://h/db"), "mysql://h/db");
    }

    #[test]
    fn from_config_parses_statement_array() {
        let cfg = serde_json::json!({
            "jdbc_connection_string": "jdbc:mysql://h/db",
            "statement": ["INSERT INTO t (a, b) VALUES (?, ?)", "field_a", "field_b"]
        });
        let o = JdbcOutput::from_config(&cfg, None).expect("config");
        assert_eq!(o.connection_string, "mysql://h/db");
        assert_eq!(o.sql, "INSERT INTO t (a, b) VALUES (?, ?)");
        assert_eq!(o.fields, vec!["field_a".to_string(), "field_b".to_string()]);
        assert_eq!(o.name(), "jdbc");
    }

    #[test]
    fn from_config_accepts_bare_sql_string() {
        let o = JdbcOutput::from_config(
            &serde_json::json!({
                "connection_string": "sqlite::memory:",
                "statement": "DELETE FROM t"
            }),
            None,
        )
        .expect("config");
        assert_eq!(o.sql, "DELETE FROM t");
        assert!(o.fields.is_empty());
    }

    #[test]
    fn from_config_requires_fields() {
        // Missing connection string.
        assert!(JdbcOutput::from_config(
            &serde_json::json!({ "statement": ["INSERT", "a"] }),
            None
        )
        .is_err());
        // Missing statement.
        assert!(JdbcOutput::from_config(
            &serde_json::json!({ "connection_string": "sqlite::memory:" }),
            None
        )
        .is_err());
        // Non-string SQL in array element 0.
        assert!(JdbcOutput::from_config(
            &serde_json::json!({ "connection_string": "sqlite::memory:", "statement": [123, "a"] }),
            None
        )
        .is_err());
    }

    /// Live SQLite smoke (no external infra). Creates a temp SQLite DB + table,
    /// runs the output, then reads the row back. Runs only under `--ignored`:
    ///   cargo test -p ferro-stash-output -- --ignored jdbc_output_sqlite
    #[tokio::test]
    #[ignore = "live: creates a temp SQLite DB and runs the jdbc output"]
    async fn jdbc_output_sqlite_smoke() {
        use sqlx::Row;

        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("output.db");
        let url = format!("sqlite://{}?mode=rwc", db_path.display());

        install_drivers_once();
        let setup = AnyPoolOptions::new().connect(&url).await.expect("connect setup");
        sqlx::query("CREATE TABLE logs (msg TEXT, level INTEGER, ratio REAL)")
            .execute(&setup)
            .await
            .expect("create table");

        let cfg = serde_json::json!({
            "connection_string": url,
            "statement": ["INSERT INTO logs (msg, level, ratio) VALUES (?, ?, ?)",
                          "message", "level", "ratio"]
        });
        let out = JdbcOutput::from_config(&cfg, None).expect("config");

        let mut ev = Event::new("hello jdbc");
        ev.set("level", EventValue::Integer(3));
        ev.set("ratio", EventValue::Float(0.5));
        out.output(vec![ev]).await.expect("output");

        let row = sqlx::query("SELECT msg, level, ratio FROM logs")
            .fetch_one(&setup)
            .await
            .expect("read back");
        let msg: String = row.try_get("msg").expect("msg");
        let level: i64 = row.try_get("level").expect("level");
        let ratio: f64 = row.try_get("ratio").expect("ratio");
        assert_eq!(msg, "hello jdbc");
        assert_eq!(level, 3);
        assert!((ratio - 0.5).abs() < f64::EPSILON);

        setup.close().await;
    }
}
