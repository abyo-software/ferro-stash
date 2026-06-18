// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024 FerroSearch Authors

//! Script value types for the Painless-compatible scripting engine.

use serde_json;
use std::collections::HashMap;
use std::fmt;

/// A dynamically-typed value used throughout script evaluation.
#[derive(Debug, Clone, PartialEq)]
pub enum ScriptValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Regex(String),
    Array(Vec<ScriptValue>),
    Map(HashMap<String, ScriptValue>),
}

impl fmt::Display for ScriptValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScriptValue::Null => write!(f, "null"),
            ScriptValue::Bool(b) => write!(f, "{b}"),
            ScriptValue::Int(i) => write!(f, "{i}"),
            ScriptValue::Float(v) => write!(f, "{v}"),
            ScriptValue::Str(s) => write!(f, "{s}"),
            ScriptValue::Regex(pattern) => write!(f, "/{pattern}/"),
            ScriptValue::Array(arr) => {
                write!(f, "[")?;
                for (i, v) in arr.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{v}")?;
                }
                write!(f, "]")
            }
            ScriptValue::Map(map) => {
                write!(f, "{{")?;
                for (i, (k, v)) in map.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{k}: {v}")?;
                }
                write!(f, "}}")
            }
        }
    }
}

impl From<serde_json::Value> for ScriptValue {
    fn from(v: serde_json::Value) -> Self {
        match v {
            serde_json::Value::Null => ScriptValue::Null,
            serde_json::Value::Bool(b) => ScriptValue::Bool(b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    ScriptValue::Int(i)
                } else {
                    ScriptValue::Float(n.as_f64().unwrap_or(0.0))
                }
            }
            serde_json::Value::String(s) => ScriptValue::Str(s),
            serde_json::Value::Array(arr) => {
                ScriptValue::Array(arr.into_iter().map(ScriptValue::from).collect())
            }
            serde_json::Value::Object(map) => {
                let m = map
                    .into_iter()
                    .map(|(k, v)| (k, ScriptValue::from(v)))
                    .collect();
                ScriptValue::Map(m)
            }
        }
    }
}

impl From<ScriptValue> for serde_json::Value {
    fn from(v: ScriptValue) -> Self {
        match v {
            ScriptValue::Null => serde_json::Value::Null,
            ScriptValue::Bool(b) => serde_json::Value::Bool(b),
            ScriptValue::Int(i) => serde_json::json!(i),
            ScriptValue::Float(f) => serde_json::json!(f),
            ScriptValue::Str(s) => serde_json::Value::String(s),
            ScriptValue::Regex(pattern) => serde_json::Value::String(pattern),
            ScriptValue::Array(arr) => {
                serde_json::Value::Array(arr.into_iter().map(serde_json::Value::from).collect())
            }
            ScriptValue::Map(map) => {
                let m: serde_json::Map<String, serde_json::Value> =
                    map.into_iter().map(|(k, v)| (k, v.into())).collect();
                serde_json::Value::Object(m)
            }
        }
    }
}

impl ScriptValue {
    /// Returns true if this value is truthy.
    pub fn is_truthy(&self) -> bool {
        match self {
            ScriptValue::Null => false,
            ScriptValue::Bool(b) => *b,
            ScriptValue::Int(i) => *i != 0,
            ScriptValue::Float(f) => *f != 0.0,
            ScriptValue::Str(s) => !s.is_empty(),
            ScriptValue::Regex(pattern) => !pattern.is_empty(),
            ScriptValue::Array(a) => !a.is_empty(),
            ScriptValue::Map(m) => !m.is_empty(),
        }
    }

    /// Attempt to coerce to f64 for arithmetic.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            ScriptValue::Int(i) => Some(*i as f64),
            ScriptValue::Float(f) => Some(*f),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_roundtrip() {
        let json = serde_json::json!({"name": "test", "value": 42, "active": true});
        let sv = ScriptValue::from(json.clone());
        let back: serde_json::Value = sv.into();
        assert_eq!(json, back);
    }

    #[test]
    fn test_truthiness() {
        assert!(!ScriptValue::Null.is_truthy());
        assert!(!ScriptValue::Bool(false).is_truthy());
        assert!(ScriptValue::Bool(true).is_truthy());
        assert!(!ScriptValue::Int(0).is_truthy());
        assert!(ScriptValue::Int(1).is_truthy());
        assert!(ScriptValue::Str("hello".into()).is_truthy());
        assert!(!ScriptValue::Str(String::new()).is_truthy());
    }

    #[test]
    fn test_as_f64() {
        assert_eq!(ScriptValue::Int(5).as_f64(), Some(5.0));
        assert_eq!(ScriptValue::Float(3.125).as_f64(), Some(3.125));
        assert_eq!(ScriptValue::Str("x".into()).as_f64(), None);
    }
}
