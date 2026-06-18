// SPDX-License-Identifier: Apache-2.0
//! Conditional expressions for filter/output blocks.
//!
//! Supports Logstash-compatible condition syntax:
//! - `[field] == "value"`
//! - `[field] =~ /pattern/`
//! - `[field] in ["a", "b"]`
//! - `"tag" in [tags]`
//! - `[field]` (existence check)
//! - `!condition`
//! - `condition and condition`
//! - `condition or condition`

use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::event::{Event, EventValue};

/// A parsed condition expression.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum Condition {
    /// Always true.
    True,
    /// Always false.
    False,
    /// Field existence check: `[field]`.
    Exists(String),
    /// Equality: `[field] == "value"`.
    Equals(String, ConditionValue),
    /// Inequality: `[field] != "value"`.
    NotEquals(String, ConditionValue),
    /// Greater than: `[field] > value`.
    GreaterThan(String, ConditionValue),
    /// Greater or equal: `[field] >= value`.
    GreaterOrEqual(String, ConditionValue),
    /// Less than: `[field] < value`.
    LessThan(String, ConditionValue),
    /// Less or equal: `[field] <= value`.
    LessOrEqual(String, ConditionValue),
    /// Regex match: `[field] =~ /pattern/`.
    RegexMatch(String, String),
    /// Negated regex: `[field] !~ /pattern/`.
    RegexNotMatch(String, String),
    /// In list: `[field] in ["a", "b"]`.
    InList(String, Vec<ConditionValue>),
    /// Not in list: `[field] not in ["a", "b"]`.
    NotInList(String, Vec<ConditionValue>),
    /// Tag check: `"tag" in [tags]`.
    HasTag(String),
    /// Logical NOT.
    Not(Box<Condition>),
    /// Logical AND.
    And(Box<Condition>, Box<Condition>),
    /// Logical OR.
    Or(Box<Condition>, Box<Condition>),
}

/// A literal value used in conditions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum ConditionValue {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
}

impl ConditionValue {
    fn matches_event_value(&self, ev: &EventValue) -> bool {
        match (self, ev) {
            (Self::String(a), EventValue::String(b)) => a == b,
            (Self::Integer(a), EventValue::Integer(b)) => a == b,
            (Self::Float(a), EventValue::Float(b)) => (a - b).abs() < f64::EPSILON,
            (Self::Boolean(a), EventValue::Boolean(b)) => a == b,
            (Self::Integer(a), EventValue::Float(b)) => (*a as f64 - b).abs() < f64::EPSILON,
            (Self::Float(a), EventValue::Integer(b)) => (a - *b as f64).abs() < f64::EPSILON,
            _ => false,
        }
    }

    fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Integer(n) => Some(*n as f64),
            Self::Float(f) => Some(*f),
            _ => None,
        }
    }
}

impl Condition {
    /// Evaluates this condition against an event.
    pub fn evaluate(&self, event: &Event) -> bool {
        match self {
            Self::True => true,
            Self::False => false,
            Self::Exists(field) => event.has_field(field),
            Self::Equals(field, value) => event
                .get(field)
                .is_some_and(|ev| value.matches_event_value(ev)),
            Self::NotEquals(field, value) => event
                .get(field)
                .map_or(true, |ev| !value.matches_event_value(ev)),
            Self::GreaterThan(field, value) => compare_field(event, field, value, |a, b| a > b),
            Self::GreaterOrEqual(field, value) => compare_field(event, field, value, |a, b| a >= b),
            Self::LessThan(field, value) => compare_field(event, field, value, |a, b| a < b),
            Self::LessOrEqual(field, value) => compare_field(event, field, value, |a, b| a <= b),
            Self::RegexMatch(field, pattern) => {
                if let Some(ev) = event.get(field) {
                    let text = ev.to_string_lossy();
                    Regex::new(pattern).is_ok_and(|re| re.is_match(&text))
                } else {
                    false
                }
            }
            Self::RegexNotMatch(field, pattern) => {
                if let Some(ev) = event.get(field) {
                    let text = ev.to_string_lossy();
                    Regex::new(pattern).map_or(true, |re| !re.is_match(&text))
                } else {
                    true
                }
            }
            Self::InList(field, values) => event
                .get(field)
                .is_some_and(|ev| values.iter().any(|v| v.matches_event_value(ev))),
            Self::NotInList(field, values) => event
                .get(field)
                .map_or(true, |ev| !values.iter().any(|v| v.matches_event_value(ev))),
            Self::HasTag(tag) => event.has_tag(tag),
            Self::Not(inner) => !inner.evaluate(event),
            Self::And(left, right) => left.evaluate(event) && right.evaluate(event),
            Self::Or(left, right) => left.evaluate(event) || right.evaluate(event),
        }
    }
}

fn compare_field(
    event: &Event,
    field: &str,
    value: &ConditionValue,
    cmp: impl Fn(f64, f64) -> bool,
) -> bool {
    let ev = match event.get(field) {
        Some(ev) => ev,
        None => return false,
    };
    let ev_f64 = match ev {
        EventValue::Integer(n) => *n as f64,
        EventValue::Float(f) => *f,
        _ => return false,
    };
    let val_f64 = match value.as_f64() {
        Some(f) => f,
        None => return false,
    };
    cmp(ev_f64, val_f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_event() -> Event {
        let mut e = Event::new("hello world");
        e.set("status", EventValue::Integer(200));
        e.set("host", EventValue::String("server01".into()));
        e.set("latency", EventValue::Float(1.5));
        e.add_tag("web");
        e
    }

    #[test]
    fn test_exists() {
        let e = test_event();
        assert!(Condition::Exists("host".into()).evaluate(&e));
        assert!(!Condition::Exists("missing".into()).evaluate(&e));
    }

    #[test]
    fn test_equals() {
        let e = test_event();
        assert!(Condition::Equals("status".into(), ConditionValue::Integer(200)).evaluate(&e));
        assert!(!Condition::Equals("status".into(), ConditionValue::Integer(404)).evaluate(&e));
    }

    #[test]
    fn test_comparison() {
        let e = test_event();
        assert!(Condition::GreaterThan("status".into(), ConditionValue::Integer(100)).evaluate(&e));
        assert!(!Condition::LessThan("status".into(), ConditionValue::Integer(100)).evaluate(&e));
    }

    #[test]
    fn test_regex_match() {
        let e = test_event();
        assert!(Condition::RegexMatch("host".into(), "server\\d+".into()).evaluate(&e));
        assert!(!Condition::RegexMatch("host".into(), "^client".into()).evaluate(&e));
    }

    #[test]
    fn test_in_list() {
        let e = test_event();
        assert!(Condition::InList(
            "status".into(),
            vec![ConditionValue::Integer(200), ConditionValue::Integer(201)]
        )
        .evaluate(&e));
    }

    #[test]
    fn test_has_tag() {
        let e = test_event();
        assert!(Condition::HasTag("web".into()).evaluate(&e));
        assert!(!Condition::HasTag("db".into()).evaluate(&e));
    }

    #[test]
    fn test_logical_operators() {
        let e = test_event();
        let cond = Condition::And(
            Box::new(Condition::Exists("host".into())),
            Box::new(Condition::HasTag("web".into())),
        );
        assert!(cond.evaluate(&e));

        let cond = Condition::Or(
            Box::new(Condition::HasTag("db".into())),
            Box::new(Condition::HasTag("web".into())),
        );
        assert!(cond.evaluate(&e));

        let cond = Condition::Not(Box::new(Condition::HasTag("db".into())));
        assert!(cond.evaluate(&e));
    }

    #[test]
    fn test_true_false() {
        let e = test_event();
        assert!(Condition::True.evaluate(&e));
        assert!(!Condition::False.evaluate(&e));
    }

    #[test]
    fn test_not_equals() {
        let e = test_event();
        assert!(Condition::NotEquals("status".into(), ConditionValue::Integer(404)).evaluate(&e));
        assert!(!Condition::NotEquals("status".into(), ConditionValue::Integer(200)).evaluate(&e));
    }

    #[test]
    fn test_not_equals_missing_field() {
        let e = test_event();
        assert!(
            Condition::NotEquals("nonexistent".into(), ConditionValue::Integer(1)).evaluate(&e)
        );
    }

    #[test]
    fn test_greater_or_equal() {
        let e = test_event();
        assert!(
            Condition::GreaterOrEqual("status".into(), ConditionValue::Integer(200)).evaluate(&e)
        );
        assert!(
            !Condition::GreaterOrEqual("status".into(), ConditionValue::Integer(201)).evaluate(&e)
        );
    }

    #[test]
    fn test_less_or_equal() {
        let e = test_event();
        assert!(Condition::LessOrEqual("status".into(), ConditionValue::Integer(200)).evaluate(&e));
        assert!(
            !Condition::LessOrEqual("status".into(), ConditionValue::Integer(199)).evaluate(&e)
        );
    }

    #[test]
    fn test_less_than() {
        let e = test_event();
        assert!(Condition::LessThan("status".into(), ConditionValue::Integer(300)).evaluate(&e));
    }

    #[test]
    fn test_regex_not_match() {
        let e = test_event();
        assert!(Condition::RegexNotMatch("host".into(), "^client".into()).evaluate(&e));
        assert!(!Condition::RegexNotMatch("host".into(), "server\\d+".into()).evaluate(&e));
    }

    #[test]
    fn test_regex_match_missing_field() {
        let e = test_event();
        assert!(!Condition::RegexMatch("missing".into(), ".*".into()).evaluate(&e));
    }

    #[test]
    fn test_regex_not_match_missing_field() {
        let e = test_event();
        assert!(Condition::RegexNotMatch("missing".into(), ".*".into()).evaluate(&e));
    }

    #[test]
    fn test_not_in_list() {
        let e = test_event();
        assert!(Condition::NotInList(
            "status".into(),
            vec![ConditionValue::Integer(404), ConditionValue::Integer(500)]
        )
        .evaluate(&e));
        assert!(
            !Condition::NotInList("status".into(), vec![ConditionValue::Integer(200)]).evaluate(&e)
        );
    }

    #[test]
    fn test_in_list_missing_field() {
        let e = test_event();
        assert!(
            !Condition::InList("missing".into(), vec![ConditionValue::Integer(1)]).evaluate(&e)
        );
    }

    #[test]
    fn test_not_in_list_missing_field() {
        let e = test_event();
        assert!(
            Condition::NotInList("missing".into(), vec![ConditionValue::Integer(1)]).evaluate(&e)
        );
    }

    #[test]
    fn test_comparison_with_float() {
        let e = test_event();
        assert!(Condition::GreaterThan("latency".into(), ConditionValue::Float(1.0)).evaluate(&e));
        assert!(!Condition::GreaterThan("latency".into(), ConditionValue::Float(2.0)).evaluate(&e));
    }

    #[test]
    fn test_comparison_missing_field() {
        let e = test_event();
        assert!(!Condition::GreaterThan("missing".into(), ConditionValue::Integer(0)).evaluate(&e));
    }

    #[test]
    fn test_comparison_string_field() {
        let e = test_event();
        assert!(!Condition::GreaterThan("host".into(), ConditionValue::Integer(0)).evaluate(&e));
    }

    #[test]
    fn test_condition_value_matches_cross_type() {
        // Integer vs Float matching
        assert!(ConditionValue::Integer(42).matches_event_value(&EventValue::Float(42.0)));
        assert!(ConditionValue::Float(42.0).matches_event_value(&EventValue::Integer(42)));
    }

    #[test]
    fn test_condition_value_as_f64() {
        assert_eq!(ConditionValue::Integer(42).as_f64(), Some(42.0));
        assert_eq!(ConditionValue::Float(2.5).as_f64(), Some(2.5));
        assert_eq!(ConditionValue::String("x".into()).as_f64(), None);
        assert_eq!(ConditionValue::Boolean(true).as_f64(), None);
    }

    #[test]
    fn test_nested_and_or() {
        let e = test_event();
        // (exists("host") AND status==200) OR tag("db")
        let cond = Condition::Or(
            Box::new(Condition::And(
                Box::new(Condition::Exists("host".into())),
                Box::new(Condition::Equals(
                    "status".into(),
                    ConditionValue::Integer(200),
                )),
            )),
            Box::new(Condition::HasTag("db".into())),
        );
        assert!(cond.evaluate(&e));
    }

    #[test]
    fn test_double_not() {
        let e = test_event();
        let cond = Condition::Not(Box::new(Condition::Not(Box::new(Condition::True))));
        assert!(cond.evaluate(&e));
    }
}
