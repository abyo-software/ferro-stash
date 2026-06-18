// SPDX-License-Identifier: Apache-2.0
//! Logstash DSL parser — parses Logstash-compatible configuration files.
//!
//! Supports the standard Logstash config syntax:
//! ```text
//! input {
//!   stdin { }
//!   file {
//!     path => "/var/log/*.log"
//!     start_position => "beginning"
//!   }
//! }
//!
//! filter {
//!   grok {
//!     match => { "message" => "%{COMBINEDAPACHELOG}" }
//!   }
//!   if [status] == "200" {
//!     mutate { add_tag => ["ok"] }
//!   }
//! }
//!
//! output {
//!   elasticsearch {
//!     hosts => ["http://localhost:9200"]
//!     index => "logs-%{+YYYY.MM.dd}"
//!   }
//! }
//! ```

use ferro_stash_core::condition::{Condition, ConditionValue};
use ferro_stash_core::error::{FerroStashError, Result};

use crate::model::{Config, FilterConfig, InputConfig, OutputConfig};

/// One `(plugin_type, settings)` pair as parsed inside a conditional
/// branch body. The condition is attached after the chain is fully
/// parsed (else-if/else need cross-branch context).
type ConditionalBlockEntry = (String, serde_json::Value);
/// One branch of an `if`/`else if`/`else` chain: an optional condition
/// (None = bare `else`) and the parsed body of that branch.
type ConditionalBranch = (Option<Condition>, Vec<ConditionalBlockEntry>);

/// Parses a Logstash DSL configuration string.
pub fn parse(input: &str) -> Result<Config> {
    let mut parser = DslParser::new(input);
    parser.parse()
}

struct DslParser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> DslParser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn parse(&mut self) -> Result<Config> {
        let mut config = Config::default();

        self.skip_whitespace_and_comments();
        while self.pos < self.input.len() {
            let section = self.read_identifier()?;
            self.skip_whitespace_and_comments();
            self.expect_char('{')?;

            match section.as_str() {
                "input" => {
                    config.inputs = self.parse_input_section()?;
                }
                "filter" => {
                    config.filters = self.parse_filter_section()?;
                }
                "output" => {
                    config.outputs = self.parse_output_section()?;
                }
                other => {
                    return Err(FerroStashError::Config(format!("unknown section: {other}")));
                }
            }

            self.expect_char('}')?;
            self.skip_whitespace_and_comments();
        }

        Ok(config)
    }

    fn parse_input_section(&mut self) -> Result<Vec<InputConfig>> {
        let mut inputs = Vec::new();
        self.skip_whitespace_and_comments();

        while self.peek_char() != Some('}') {
            let plugin_type = self.read_identifier()?;
            self.skip_whitespace_and_comments();
            self.expect_char('{')?;
            let settings = self.parse_plugin_body()?;
            self.expect_char('}')?;
            self.skip_whitespace_and_comments();

            let codec = settings
                .get("codec")
                .and_then(|v| v.as_str())
                .map(String::from);
            let tags = settings
                .get("tags")
                .and_then(|v| v.as_array())
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            let event_type = settings
                .get("type")
                .and_then(|v| v.as_str())
                .map(String::from);

            inputs.push(InputConfig {
                plugin_type,
                settings: serde_json::Value::Object(
                    settings.as_object().cloned().unwrap_or_default(),
                ),
                codec,
                codec_settings: serde_json::Value::Object(serde_json::Map::new()),
                tags,
                event_type,
            });
        }

        Ok(inputs)
    }

    fn parse_filter_section(&mut self) -> Result<Vec<FilterConfig>> {
        let mut filters = Vec::new();
        self.skip_whitespace_and_comments();

        while self.peek_char() != Some('}') {
            // Check for conditional
            if self.peek_word() == "if" {
                let conditional_filters = self.parse_conditional_filter()?;
                filters.extend(conditional_filters);
            } else {
                let plugin_type = self.read_identifier()?;
                self.skip_whitespace_and_comments();
                self.expect_char('{')?;
                let settings = self.parse_plugin_body()?;
                self.expect_char('}')?;
                self.skip_whitespace_and_comments();

                filters.push(FilterConfig {
                    plugin_type,
                    settings,
                    condition: None,
                });
            }
        }

        Ok(filters)
    }

    fn parse_output_section(&mut self) -> Result<Vec<OutputConfig>> {
        let mut outputs = Vec::new();
        self.skip_whitespace_and_comments();

        while self.peek_char() != Some('}') {
            if self.peek_word() == "if" {
                let conditional_outputs = self.parse_conditional_output()?;
                outputs.extend(conditional_outputs);
            } else {
                let plugin_type = self.read_identifier()?;
                self.skip_whitespace_and_comments();
                self.expect_char('{')?;
                let settings = self.parse_plugin_body()?;
                self.expect_char('}')?;
                self.skip_whitespace_and_comments();

                let codec = settings
                    .get("codec")
                    .and_then(|v| v.as_str())
                    .map(String::from);

                outputs.push(OutputConfig {
                    plugin_type,
                    settings,
                    codec,
                    codec_settings: serde_json::Value::Object(serde_json::Map::new()),
                    condition: None,
                });
            }
        }

        Ok(outputs)
    }

    fn parse_filter_block_body(&mut self) -> Result<Vec<ConditionalBlockEntry>> {
        self.expect_char('{')?;
        let mut plugins = Vec::new();
        self.skip_whitespace_and_comments();
        while self.peek_char() != Some('}') {
            let plugin_type = self.read_identifier()?;
            self.skip_whitespace_and_comments();
            self.expect_char('{')?;
            let settings = self.parse_plugin_body()?;
            self.expect_char('}')?;
            self.skip_whitespace_and_comments();
            plugins.push((plugin_type, settings));
        }
        self.expect_char('}')?;
        self.skip_whitespace_and_comments();
        Ok(plugins)
    }

    fn parse_output_block_body(&mut self) -> Result<Vec<ConditionalBlockEntry>> {
        // Outputs and filters share the same `name { k => v ... }` shape
        // for parsing purposes; the codec extraction is done by the caller.
        self.parse_filter_block_body()
    }

    /// Parse a chained `if (cond) { ... } else if (cond) { ... } else { ... }`.
    /// Each branch's filters are emitted with an effective condition that
    /// guards against earlier branches firing — Logstash's `if/elsif/else`
    /// semantics are mutually exclusive.
    fn parse_conditional_filter(&mut self) -> Result<Vec<FilterConfig>> {
        self.read_identifier()?; // consume "if"
        self.skip_whitespace_and_comments();
        let first_cond = self.parse_condition()?;
        let first_body = self.parse_filter_block_body()?;

        let mut branches: Vec<ConditionalBranch> = vec![(Some(first_cond), first_body)];

        while self.peek_word() == "else" {
            self.read_identifier()?; // consume "else"
            self.skip_whitespace_and_comments();
            if self.peek_word() == "if" {
                self.read_identifier()?; // consume "if"
                self.skip_whitespace_and_comments();
                let cond = self.parse_condition()?;
                let body = self.parse_filter_block_body()?;
                branches.push((Some(cond), body));
            } else {
                // bare `else { ... }`
                let body = self.parse_filter_block_body()?;
                branches.push((None, body));
                break;
            }
        }

        let mut emitted = Vec::new();
        let mut earlier: Option<Condition> = None;
        for (branch_cond, body) in branches {
            let effective = match (&earlier, &branch_cond) {
                (None, Some(c)) => Some(c.clone()),
                (Some(prev), Some(c)) => Some(Condition::And(
                    Box::new(c.clone()),
                    Box::new(Condition::Not(Box::new(prev.clone()))),
                )),
                (Some(prev), None) => Some(Condition::Not(Box::new(prev.clone()))),
                (None, None) => None,
            };
            for (plugin_type, settings) in body {
                emitted.push(FilterConfig {
                    plugin_type,
                    settings,
                    condition: effective.clone(),
                });
            }
            earlier = match (earlier, branch_cond) {
                (None, Some(c)) => Some(c),
                (Some(prev), Some(c)) => Some(Condition::Or(Box::new(prev), Box::new(c))),
                (Some(prev), None) => Some(prev),
                (None, None) => None,
            };
        }

        Ok(emitted)
    }

    fn parse_conditional_output(&mut self) -> Result<Vec<OutputConfig>> {
        self.read_identifier()?; // consume "if"
        self.skip_whitespace_and_comments();
        let first_cond = self.parse_condition()?;
        let first_body = self.parse_output_block_body()?;

        let mut branches: Vec<ConditionalBranch> = vec![(Some(first_cond), first_body)];

        while self.peek_word() == "else" {
            self.read_identifier()?;
            self.skip_whitespace_and_comments();
            if self.peek_word() == "if" {
                self.read_identifier()?;
                self.skip_whitespace_and_comments();
                let cond = self.parse_condition()?;
                let body = self.parse_output_block_body()?;
                branches.push((Some(cond), body));
            } else {
                let body = self.parse_output_block_body()?;
                branches.push((None, body));
                break;
            }
        }

        let mut emitted = Vec::new();
        let mut earlier: Option<Condition> = None;
        for (branch_cond, body) in branches {
            let effective = match (&earlier, &branch_cond) {
                (None, Some(c)) => Some(c.clone()),
                (Some(prev), Some(c)) => Some(Condition::And(
                    Box::new(c.clone()),
                    Box::new(Condition::Not(Box::new(prev.clone()))),
                )),
                (Some(prev), None) => Some(Condition::Not(Box::new(prev.clone()))),
                (None, None) => None,
            };
            for (plugin_type, settings) in body {
                let codec = settings
                    .get("codec")
                    .and_then(|v| v.as_str())
                    .map(String::from);
                emitted.push(OutputConfig {
                    plugin_type,
                    settings,
                    codec,
                    codec_settings: serde_json::Value::Object(serde_json::Map::new()),
                    condition: effective.clone(),
                });
            }
            earlier = match (earlier, branch_cond) {
                (None, Some(c)) => Some(c),
                (Some(prev), Some(c)) => Some(Condition::Or(Box::new(prev), Box::new(c))),
                (Some(prev), None) => Some(prev),
                (None, None) => None,
            };
        }

        Ok(emitted)
    }

    fn parse_condition(&mut self) -> Result<Condition> {
        let cond = self.parse_or_condition()?;
        Ok(cond)
    }

    fn parse_or_condition(&mut self) -> Result<Condition> {
        let mut left = self.parse_and_condition()?;
        self.skip_whitespace_and_comments();

        while self.peek_word() == "or" {
            self.read_identifier()?; // consume "or"
            self.skip_whitespace_and_comments();
            let right = self.parse_and_condition()?;
            left = Condition::Or(Box::new(left), Box::new(right));
            self.skip_whitespace_and_comments();
        }

        Ok(left)
    }

    fn parse_and_condition(&mut self) -> Result<Condition> {
        let mut left = self.parse_unary_condition()?;
        self.skip_whitespace_and_comments();

        while self.peek_word() == "and" {
            self.read_identifier()?; // consume "and"
            self.skip_whitespace_and_comments();
            let right = self.parse_unary_condition()?;
            left = Condition::And(Box::new(left), Box::new(right));
            self.skip_whitespace_and_comments();
        }

        Ok(left)
    }

    fn parse_unary_condition(&mut self) -> Result<Condition> {
        self.skip_whitespace_and_comments();

        if self.peek_char() == Some('!') {
            self.pos += 1;
            self.skip_whitespace_and_comments();
            let inner = self.parse_unary_condition()?;
            return Ok(Condition::Not(Box::new(inner)));
        }

        if self.peek_char() == Some('(') {
            self.pos += 1;
            self.skip_whitespace_and_comments();
            let cond = self.parse_or_condition()?;
            self.skip_whitespace_and_comments();
            self.expect_char(')')?;
            return Ok(cond);
        }

        self.parse_comparison()
    }

    fn parse_comparison(&mut self) -> Result<Condition> {
        self.skip_whitespace_and_comments();

        // Check for string literal first (e.g., "tag" in [tags])
        if matches!(self.peek_char(), Some('"' | '\'')) {
            let value = self.read_string()?;
            self.skip_whitespace_and_comments();
            if self.peek_word() == "in" {
                self.read_identifier()?;
                self.skip_whitespace_and_comments();
                // Expect [tags]
                if self.peek_char() == Some('[') {
                    let field = self.read_bracket_field()?;
                    if field == "tags" {
                        return Ok(Condition::HasTag(value));
                    }
                    return Ok(Condition::InList(
                        field,
                        vec![ConditionValue::String(value)],
                    ));
                }
            }
            return Err(FerroStashError::Config(
                "unexpected string in condition".to_string(),
            ));
        }

        // Field reference
        let field = if self.peek_char() == Some('[') {
            self.read_bracket_field()?
        } else {
            self.read_identifier()?
        };

        self.skip_whitespace_and_comments();

        // Check for operator
        let op = self.read_operator()?;
        self.skip_whitespace_and_comments();

        if op.is_empty() {
            // Just a field existence check
            return Ok(Condition::Exists(field));
        }

        let value = self.read_condition_value()?;

        match op.as_str() {
            "==" => Ok(Condition::Equals(field, value)),
            "!=" => Ok(Condition::NotEquals(field, value)),
            ">" => Ok(Condition::GreaterThan(field, value)),
            ">=" => Ok(Condition::GreaterOrEqual(field, value)),
            "<" => Ok(Condition::LessThan(field, value)),
            "<=" => Ok(Condition::LessOrEqual(field, value)),
            "=~" => {
                if let ConditionValue::String(pattern) = value {
                    Ok(Condition::RegexMatch(field, pattern))
                } else {
                    Err(FerroStashError::Config(
                        "regex match requires a string pattern".to_string(),
                    ))
                }
            }
            "!~" => {
                if let ConditionValue::String(pattern) = value {
                    Ok(Condition::RegexNotMatch(field, pattern))
                } else {
                    Err(FerroStashError::Config(
                        "regex not-match requires a string pattern".to_string(),
                    ))
                }
            }
            "in" => {
                // value should be a list — but we parsed a single value
                Ok(Condition::InList(field, vec![value]))
            }
            "not in" => Ok(Condition::NotInList(field, vec![value])),
            _ => Err(FerroStashError::Config(format!("unknown operator: {op}"))),
        }
    }

    fn parse_plugin_body(&mut self) -> Result<serde_json::Value> {
        let mut map = serde_json::Map::new();
        self.skip_whitespace_and_comments();

        while self.peek_char() != Some('}') && self.pos < self.input.len() {
            let key = self.read_identifier()?;
            self.skip_whitespace_and_comments();
            self.expect_str("=>")?;
            self.skip_whitespace_and_comments();
            let value = self.read_value()?;
            map.insert(key, value);
            self.skip_whitespace_and_comments();
        }

        Ok(serde_json::Value::Object(map))
    }

    fn read_value(&mut self) -> Result<serde_json::Value> {
        self.skip_whitespace_and_comments();
        match self.peek_char() {
            Some('"' | '\'') => {
                let s = self.read_string()?;
                Ok(serde_json::Value::String(s))
            }
            Some('[') => self.read_array(),
            Some('{') => self.read_hash(),
            Some(c) if c.is_ascii_digit() || c == '-' => {
                let num = self.read_number()?;
                Ok(num)
            }
            Some(c) if c.is_alphabetic() || c == '_' => {
                // Read bare identifier (true/false/null or plugin name or enum value)
                let word = self.read_identifier()?;
                match word.as_str() {
                    "true" => Ok(serde_json::Value::Bool(true)),
                    "false" => Ok(serde_json::Value::Bool(false)),
                    "nil" | "null" => Ok(serde_json::Value::Null),
                    _ => {
                        // Check if followed by plugin block: `codec => line { format => ... }`
                        self.skip_whitespace_and_comments();
                        if self.peek_char() == Some('{') {
                            // Parse the plugin body and return as plugin descriptor:
                            //   { "_plugin": "<name>", ...settings }
                            self.expect_char('{')?;
                            let mut map = match self.parse_plugin_body()? {
                                serde_json::Value::Object(m) => m,
                                _ => serde_json::Map::new(),
                            };
                            self.expect_char('}')?;
                            map.insert("_plugin".to_string(), serde_json::Value::String(word));
                            Ok(serde_json::Value::Object(map))
                        } else {
                            Ok(serde_json::Value::String(word))
                        }
                    }
                }
            }
            _ => Err(FerroStashError::Config(format!(
                "unexpected character at position {}: {:?}",
                self.pos,
                self.peek_char()
            ))),
        }
    }

    fn read_array(&mut self) -> Result<serde_json::Value> {
        self.expect_char('[')?;
        let mut items = Vec::new();
        self.skip_whitespace_and_comments();

        while self.peek_char() != Some(']') {
            let value = self.read_value()?;
            items.push(value);
            self.skip_whitespace_and_comments();
            if self.peek_char() == Some(',') {
                self.pos += 1;
                self.skip_whitespace_and_comments();
            }
        }

        self.expect_char(']')?;
        Ok(serde_json::Value::Array(items))
    }

    fn read_hash(&mut self) -> Result<serde_json::Value> {
        self.expect_char('{')?;
        let mut map = serde_json::Map::new();
        self.skip_whitespace_and_comments();

        while self.peek_char() != Some('}') {
            let key = if matches!(self.peek_char(), Some('"' | '\'')) {
                self.read_string()?
            } else {
                self.read_identifier()?
            };
            self.skip_whitespace_and_comments();
            self.expect_str("=>")?;
            self.skip_whitespace_and_comments();
            let value = self.read_value()?;
            map.insert(key, value);
            self.skip_whitespace_and_comments();
            if self.peek_char() == Some(',') {
                self.pos += 1;
                self.skip_whitespace_and_comments();
            }
        }

        self.expect_char('}')?;
        Ok(serde_json::Value::Object(map))
    }

    fn read_string(&mut self) -> Result<String> {
        // Logstash supports both single and double-quoted strings
        let quote = match self.peek_char() {
            Some('"') => '"',
            Some('\'') => '\'',
            _ => {
                return Err(FerroStashError::Config(format!(
                    "expected '\"' or '\\'' at position {}, got {:?}",
                    self.pos,
                    self.peek_char()
                )));
            }
        };
        self.pos += 1; // consume opening quote

        let mut result = String::new();
        while self.pos < self.input.len() {
            // UTF-8 aware: consume a full codepoint, not a raw byte.
            // Treating multi-byte UTF-8 as Latin-1 chars mojibakes non-ASCII
            // string literals (e.g. `id => '입력'`).
            let c = self.input[self.pos..]
                .chars()
                .next()
                .expect("non-empty by loop condition");
            self.pos += c.len_utf8();
            if c == quote {
                return Ok(result);
            }
            if c == '\\' && self.pos < self.input.len() {
                // Match Logstash's `grammar.treetop` string semantics
                // exactly. The Ruby AST (`LogStash::Config::AST::String`)
                // unescapes ONLY `\\` and the matching quote (`\"` in
                // double-quoted, `\'` in single-quoted) — every other
                // `\X` keeps the backslash as a literal byte. This is
                // load-bearing for grok pipelines that use `\|`, `\.`
                // etc. in patterns. ferro-stash previously had a C-style
                // table that turned `\n`/`\t`/`\r` into newline/tab/CR
                // and dropped backslashes from other unknowns; both
                // diverged from Logstash. See Wave 5.3 divergence #2.
                let escaped = self.input[self.pos..]
                    .chars()
                    .next()
                    .expect("non-empty by surrounding check");
                self.pos += escaped.len_utf8();
                if escaped == '\\' || escaped == quote {
                    result.push(escaped);
                } else {
                    result.push('\\');
                    result.push(escaped);
                }
            } else {
                result.push(c);
            }
        }
        Err(FerroStashError::Config("unterminated string".to_string()))
    }

    fn read_number(&mut self) -> Result<serde_json::Value> {
        let start = self.pos;
        let mut has_dot = false;
        if self.peek_char() == Some('-') {
            self.pos += 1;
        }
        // ASCII-only: digits / '.' are single-byte, so byte-indexing is
        // safe here. We still gate on `is_ascii_digit` so multi-byte
        // codepoints (e.g. fullwidth '１') are not mistaken for digits.
        while self.pos < self.input.len() {
            let b = self.input.as_bytes()[self.pos];
            if b == b'.' && !has_dot {
                has_dot = true;
                self.pos += 1;
            } else if b.is_ascii_digit() {
                self.pos += 1;
            } else {
                break;
            }
        }
        let num_str = &self.input[start..self.pos];
        if has_dot {
            let f: f64 = num_str
                .parse()
                .map_err(|e| FerroStashError::Config(format!("invalid number: {e}")))?;
            Ok(serde_json::Value::Number(
                serde_json::Number::from_f64(f)
                    .ok_or_else(|| FerroStashError::Config("invalid float".to_string()))?,
            ))
        } else {
            let n: i64 = num_str
                .parse()
                .map_err(|e| FerroStashError::Config(format!("invalid number: {e}")))?;
            Ok(serde_json::Value::Number(serde_json::Number::from(n)))
        }
    }

    fn read_identifier(&mut self) -> Result<String> {
        self.skip_whitespace_and_comments();
        let start = self.pos;
        // UTF-8 aware: advance by `c.len_utf8()` so multi-byte
        // codepoints (CJK / Cyrillic / etc) don't leave `pos` inside
        // a codepoint and panic the next time we slice `input`.
        // Discovered by 60s smoke fuzz on `logstash_dsl_parse`
        // (2026-05-02); see
        // `tests::test_parse_multibyte_utf8_no_char_boundary_panic`.
        while self.pos < self.input.len() {
            let Some(c) = self.input[self.pos..].chars().next() else {
                break;
            };
            if c.is_alphanumeric() || c == '_' || c == '-' || c == '.' {
                self.pos += c.len_utf8();
            } else {
                break;
            }
        }
        if self.pos == start {
            return Err(FerroStashError::Config(format!(
                "expected identifier at position {}",
                self.pos
            )));
        }
        Ok(self.input[start..self.pos].to_string())
    }

    fn read_bracket_field(&mut self) -> Result<String> {
        let mut segments = Vec::new();
        while self.peek_char() == Some('[') {
            self.pos += 1; // consume '[' (ASCII, single byte)
            let start = self.pos;
            // UTF-8 aware: advance by full codepoint width. The
            // sentinel `]` is ASCII so byte-comparison is safe, but
            // any non-`]` codepoint may be multi-byte.
            while self.pos < self.input.len() {
                let Some(c) = self.input[self.pos..].chars().next() else {
                    break;
                };
                if c == ']' {
                    break;
                }
                self.pos += c.len_utf8();
            }
            segments.push(self.input[start..self.pos].to_string());
            if self.pos < self.input.len() {
                self.pos += 1; // consume ']' (ASCII, single byte)
            }
        }
        Ok(segments.join("."))
    }

    fn read_operator(&mut self) -> Result<String> {
        // All real operators are ASCII (`==`, `!=`, `>=`, `<=`, `=~`,
        // `!~`, `>`, `<`, `in`, `not`). Multi-byte codepoints must not
        // slice into a 2-byte window — that would panic on a char
        // boundary error. Use `as_bytes()` for the ASCII look-ahead so
        // we never construct a `&str` slice across a codepoint.
        let bytes = self.input.as_bytes();
        if self.pos + 1 < bytes.len() {
            let two = [bytes[self.pos], bytes[self.pos + 1]];
            let op2: Option<&'static str> = match &two {
                b"==" => Some("=="),
                b"!=" => Some("!="),
                b">=" => Some(">="),
                b"<=" => Some("<="),
                b"=~" => Some("=~"),
                b"!~" => Some("!~"),
                _ => None,
            };
            if let Some(op) = op2 {
                self.pos += 2;
                return Ok(op.to_string());
            }
        }
        if self.pos < bytes.len() {
            let b = bytes[self.pos];
            if b == b'>' || b == b'<' {
                self.pos += 1;
                return Ok((b as char).to_string());
            }
        }
        // Check for keyword operators
        let word = self.peek_word();
        if word == "in" || word == "not" {
            return Ok(String::new()); // handled by caller for 'in' keyword
        }
        Ok(String::new())
    }

    fn read_condition_value(&mut self) -> Result<ConditionValue> {
        self.skip_whitespace_and_comments();
        match self.peek_char() {
            Some('"' | '\'') => {
                let s = self.read_string()?;
                Ok(ConditionValue::String(s))
            }
            Some('/') => {
                // Regex pattern. Sentinels `/` and `\` are ASCII, but
                // the body itself may contain multi-byte codepoints
                // (Unicode regex literals), so advance by full
                // codepoint width.
                self.pos += 1; // consume opening '/' (ASCII)
                let start = self.pos;
                while self.pos < self.input.len() {
                    let Some(c) = self.input[self.pos..].chars().next() else {
                        break;
                    };
                    if c == '/' {
                        break;
                    }
                    if c == '\\' {
                        // Skip the backslash and whatever codepoint
                        // follows (escape pair). Both must advance by
                        // full codepoint width.
                        self.pos += c.len_utf8();
                        if let Some(esc) = self.input[self.pos..].chars().next() {
                            self.pos += esc.len_utf8();
                        }
                    } else {
                        self.pos += c.len_utf8();
                    }
                }
                let pattern = self.input[start..self.pos].to_string();
                if self.pos < self.input.len() {
                    self.pos += 1; // consume closing '/' (ASCII)
                }
                Ok(ConditionValue::String(pattern))
            }
            Some(c) if c.is_ascii_digit() || c == '-' => {
                let num = self.read_number()?;
                if let Some(n) = num.as_i64() {
                    Ok(ConditionValue::Integer(n))
                } else if let Some(f) = num.as_f64() {
                    Ok(ConditionValue::Float(f))
                } else {
                    Err(FerroStashError::Config(
                        "invalid number in condition".to_string(),
                    ))
                }
            }
            Some('t' | 'f') => {
                let word = self.read_identifier()?;
                match word.as_str() {
                    "true" => Ok(ConditionValue::Boolean(true)),
                    "false" => Ok(ConditionValue::Boolean(false)),
                    _ => Ok(ConditionValue::String(word)),
                }
            }
            _ => Err(FerroStashError::Config(format!(
                "unexpected character in condition value at position {}",
                self.pos
            ))),
        }
    }

    fn peek_char(&self) -> Option<char> {
        // UTF-8 aware. Returning the byte cast as `char` would lose
        // the high bits of multi-byte codepoints and, worse, callers
        // that advance with `pos += 1` after this would land inside
        // a codepoint and panic on the next slice.
        self.input[self.pos..].chars().next()
    }

    fn peek_word(&self) -> &str {
        let mut pos = self.pos;
        // Skip whitespace (UTF-8 aware: NBSP / ideographic space etc
        // are multi-byte but `char::is_whitespace` recognises them).
        while pos < self.input.len() {
            let Some(c) = self.input[pos..].chars().next() else {
                break;
            };
            if c.is_whitespace() {
                pos += c.len_utf8();
            } else {
                break;
            }
        }
        let start = pos;
        while pos < self.input.len() {
            let Some(c) = self.input[pos..].chars().next() else {
                break;
            };
            if c.is_alphanumeric() || c == '_' {
                pos += c.len_utf8();
            } else {
                break;
            }
        }
        &self.input[start..pos]
    }

    fn expect_char(&mut self, expected: char) -> Result<()> {
        self.skip_whitespace_and_comments();
        if self.peek_char() == Some(expected) {
            self.pos += 1;
            Ok(())
        } else {
            Err(FerroStashError::Config(format!(
                "expected '{}' at position {}, got {:?}",
                expected,
                self.pos,
                self.peek_char()
            )))
        }
    }

    fn expect_str(&mut self, expected: &str) -> Result<()> {
        self.skip_whitespace_and_comments();
        if self.input[self.pos..].starts_with(expected) {
            self.pos += expected.len();
            Ok(())
        } else {
            Err(FerroStashError::Config(format!(
                "expected '{}' at position {}",
                expected, self.pos
            )))
        }
    }

    fn skip_whitespace_and_comments(&mut self) {
        // UTF-8 aware. Comment lines may contain multi-byte
        // codepoints (e.g. CJK), and Unicode whitespace (NBSP, ideo
        // space) is multi-byte too, so we must advance by full
        // codepoint width or we'll leave `pos` mid-codepoint.
        while self.pos < self.input.len() {
            let Some(c) = self.input[self.pos..].chars().next() else {
                break;
            };
            if c.is_whitespace() {
                self.pos += c.len_utf8();
            } else if c == '#' {
                // Skip to end of line
                while self.pos < self.input.len() {
                    let Some(cc) = self.input[self.pos..].chars().next() else {
                        break;
                    };
                    if cc == '\n' {
                        break;
                    }
                    self.pos += cc.len_utf8();
                }
            } else {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_config() {
        let config_str = r"
input {
  stdin { }
}

output {
  stdout { }
}
";
        let config = parse(config_str).expect("parse failed");
        assert_eq!(config.inputs.len(), 1);
        assert_eq!(config.inputs[0].plugin_type, "stdin");
        assert_eq!(config.outputs.len(), 1);
        assert_eq!(config.outputs[0].plugin_type, "stdout");
    }

    #[test]
    fn test_parse_with_settings() {
        let config_str = r#"
input {
  file {
    path => "/var/log/syslog"
    start_position => "beginning"
    sincedb_path => "/dev/null"
  }
}

output {
  elasticsearch {
    hosts => ["http://localhost:9200"]
    index => "test-index"
  }
}
"#;
        let config = parse(config_str).expect("parse failed");
        assert_eq!(config.inputs[0].plugin_type, "file");
        assert_eq!(
            config.inputs[0].settings["path"],
            serde_json::Value::String("/var/log/syslog".into())
        );
        assert_eq!(config.outputs[0].plugin_type, "elasticsearch");
    }

    #[test]
    fn test_parse_with_comments() {
        let config_str = r"
# This is a comment
input {
  stdin { } # inline comment
}

output {
  stdout { }
}
";
        let config = parse(config_str).expect("parse failed");
        assert_eq!(config.inputs.len(), 1);
    }

    #[test]
    fn test_parse_filter_with_condition() {
        let config_str = r#"
input {
  stdin { }
}

filter {
  grok {
    match => { "message" => "%{COMBINEDAPACHELOG}" }
  }
  if [status] == "200" {
    mutate {
      add_tag => ["ok"]
    }
  }
}

output {
  stdout { }
}
"#;
        let config = parse(config_str).expect("parse failed");
        assert_eq!(config.filters.len(), 2);
        assert_eq!(config.filters[0].plugin_type, "grok");
        assert!(config.filters[0].condition.is_none());
        assert_eq!(config.filters[1].plugin_type, "mutate");
        assert!(config.filters[1].condition.is_some());
    }

    #[test]
    fn test_parse_hash_value() {
        let config_str = r#"
input {
  stdin { }
}

filter {
  grok {
    match => { "message" => "test pattern" }
  }
}

output {
  stdout { }
}
"#;
        let config = parse(config_str).expect("parse failed");
        assert_eq!(config.filters[0].plugin_type, "grok");
        let match_val = &config.filters[0].settings["match"];
        assert!(match_val.is_object());
    }

    #[test]
    fn test_parse_single_quoted_strings() {
        let config_str = r"
input {
  tcp {
    port => '9999'
    host => 'localhost'
  }
}

output {
  file {
    path => '/tmp/out.log'
  }
}
";
        let config = parse(config_str).expect("single quotes should parse");
        assert_eq!(config.inputs[0].plugin_type, "tcp");
        assert_eq!(
            config.inputs[0].settings["port"],
            serde_json::Value::String("9999".into())
        );
        assert_eq!(
            config.inputs[0].settings["host"],
            serde_json::Value::String("localhost".into())
        );
    }

    #[test]
    fn test_parse_mixed_quotes() {
        let config_str = r#"
input {
  tcp {
    port => '9999'
    host => "localhost"
  }
}

output { stdout { codec => 'rubydebug' } }
"#;
        let config = parse(config_str).expect("mixed quotes should parse");
        assert_eq!(
            config.inputs[0].settings["port"],
            serde_json::Value::String("9999".into())
        );
        assert_eq!(
            config.inputs[0].settings["host"],
            serde_json::Value::String("localhost".into())
        );
        assert_eq!(config.outputs[0].codec, Some("rubydebug".to_string()));
    }

    /// Regression: parsing a config whose first non-whitespace bytes are
    /// a multi-byte UTF-8 codepoint must not panic with
    /// `byte index N is not a char boundary`. Discovered by 60s smoke
    /// fuzz on `logstash_dsl_parse` (2026-05-02). Corpus byte-for-byte
    /// matches `fuzz/corpus/logstash_dsl_parse/regression-dsl-char-boundary-2026-05-02`.
    ///
    /// Trigger: `read_identifier` (and several sibling lexer methods)
    /// were reading bytes via `as_bytes()[pos] as char`, treating each
    /// byte as Latin-1, so multi-byte UTF-8 advanced `pos` only one byte
    /// at a time. The next slice (`&self.input[start..self.pos]`) then
    /// landed inside a multi-byte codepoint and panicked.
    #[test]
    fn test_parse_multibyte_utf8_no_char_boundary_panic() {
        // bytes: f6 d3 bb — f6 is invalid UTF-8 start byte by itself,
        // but d3 bb is the valid 2-byte sequence for U+04FB ('ӻ').
        // serde / parse_config rejects non-UTF-8, but the DSL parser
        // is also reachable through legitimately UTF-8-valid inputs;
        // a 2-byte CJK / Cyrillic character at the head of an
        // identifier hits the same byte-indexing bug.
        let utf8_input = "ӻ"; // U+04FB, 2 bytes (0xd3 0xbb)
        let _ = parse(utf8_input);

        // Also exercise the original 3-byte fuzz corpus through any
        // entry point that happens to be UTF-8 valid. The first byte
        // 0xf6 alone is invalid, so the public `parse` may early-out
        // on `from_utf8`, but if reachable the parser must still not
        // panic. Construct a string that triggers each entry point
        // (read_identifier, read_bracket_field, read_operator) with
        // multi-byte input.
        let _ = parse("ӻ { }");
        let _ = parse("filter { if [ӻ] == \"x\" { } }");
        let _ = parse("filter { if [a] ӻ \"x\" { } }");
    }

    /// Wave 5.3 divergence #2 — match Logstash's
    /// `LogStash::Config::AST::String#value` semantics: only `\\` and the
    /// matching quote are unescaped; every other `\X` keeps the
    /// backslash literally. The previous C-style escape table mapped
    /// `\n`/`\t`/`\r` to whitespace and dropped backslashes from
    /// unknowns, both of which diverged from Logstash and (load-bearing)
    /// broke grok pipelines that rely on `\|`/`\.` etc. surviving the
    /// DSL parse intact.
    #[test]
    fn test_parse_string_escape_logstash_semantics() {
        // \" inside a double-quoted string -> embedded "
        let cfg = parse(
            r#"
filter {
  mutate { add_field => { "raw" => "left \"mid\" right" } }
}
"#,
        )
        .expect("parse");
        assert_eq!(
            cfg.filters[0].settings["add_field"]["raw"],
            serde_json::Value::String(r#"left "mid" right"#.into())
        );

        // \\ -> \
        let cfg = parse(
            r#"
filter {
  mutate { add_field => { "raw" => "a\\b" } }
}
"#,
        )
        .expect("parse");
        assert_eq!(
            cfg.filters[0].settings["add_field"]["raw"],
            serde_json::Value::String(r"a\b".into())
        );

        // \n -> literal backslash + `n` (NOT a newline). This is the
        // load-bearing change from the C-style table, and the reason
        // grok patterns like `%{NOTSPACE:user}\|%{GREEDYDATA:body}`
        // round-trip correctly.
        let cfg = parse(
            r#"
filter {
  mutate { add_field => { "raw" => "x\ny" } }
}
"#,
        )
        .expect("parse");
        assert_eq!(
            cfg.filters[0].settings["add_field"]["raw"],
            serde_json::Value::String(r"x\ny".into())
        );

        // Unknown escape -> backslash retained, char emitted (Logstash
        // does NOT drop the backslash for unknown escapes).
        let cfg = parse(
            r#"
filter {
  mutate { add_field => { "raw" => "x\zy" } }
}
"#,
        )
        .expect("parse");
        assert_eq!(
            cfg.filters[0].settings["add_field"]["raw"],
            serde_json::Value::String(r"x\zy".into())
        );

        // Single-quoted: only \\ and \' are unescaped; \" keeps the
        // backslash because " is not the matching quote.
        let cfg = parse(
            r"
filter {
  mutate { add_field => { 'raw' => 'a\'b\\c\nd' } }
}
",
        )
        .expect("parse");
        assert_eq!(
            cfg.filters[0].settings["add_field"]["raw"],
            serde_json::Value::String(r"a'b\c\nd".into())
        );
    }

    /// Wave 5.3 divergence #3 — `else if` / `else` chains. Each branch's
    /// emitted filter must carry an effective condition that is mutually
    /// exclusive with the earlier branches', exactly like Logstash.
    #[test]
    fn test_parse_else_if_else_chain() {
        use ferro_stash_core::condition::ConditionValue;
        use ferro_stash_core::event::{Event, EventValue};

        let cfg = parse(
            r#"
filter {
  if [level] == "ERROR" {
    mutate { add_field => { "severity" => "high" } }
  } else if [level] == "WARN" {
    mutate { add_field => { "severity" => "medium" } }
  } else {
    mutate { add_field => { "severity" => "low" } }
  }
}
"#,
        )
        .expect("parse");

        assert_eq!(cfg.filters.len(), 3);
        // Each branch should still resolve to a `mutate`.
        for f in &cfg.filters {
            assert_eq!(f.plugin_type, "mutate");
            assert!(
                f.condition.is_some(),
                "every branch (incl. else) must carry a guard condition"
            );
        }

        // Mutual exclusion: an event with level=ERROR must satisfy
        // branch 0's condition only.
        let mut error_evt = Event::new("");
        error_evt.set("level", EventValue::String("ERROR".into()));
        assert!(cfg.filters[0]
            .condition
            .as_ref()
            .unwrap()
            .evaluate(&error_evt));
        assert!(!cfg.filters[1]
            .condition
            .as_ref()
            .unwrap()
            .evaluate(&error_evt));
        assert!(!cfg.filters[2]
            .condition
            .as_ref()
            .unwrap()
            .evaluate(&error_evt));

        let mut warn_evt = Event::new("");
        warn_evt.set("level", EventValue::String("WARN".into()));
        assert!(!cfg.filters[0]
            .condition
            .as_ref()
            .unwrap()
            .evaluate(&warn_evt));
        assert!(cfg.filters[1]
            .condition
            .as_ref()
            .unwrap()
            .evaluate(&warn_evt));
        assert!(!cfg.filters[2]
            .condition
            .as_ref()
            .unwrap()
            .evaluate(&warn_evt));

        let mut info_evt = Event::new("");
        info_evt.set("level", EventValue::String("INFO".into()));
        assert!(!cfg.filters[0]
            .condition
            .as_ref()
            .unwrap()
            .evaluate(&info_evt));
        assert!(!cfg.filters[1]
            .condition
            .as_ref()
            .unwrap()
            .evaluate(&info_evt));
        assert!(cfg.filters[2]
            .condition
            .as_ref()
            .unwrap()
            .evaluate(&info_evt));

        // Sanity-check no warning about unused import.
        let _ = ConditionValue::Boolean(true);
    }

    #[test]
    fn test_parse_else_if_chain_no_trailing_else() {
        use ferro_stash_core::event::{Event, EventValue};

        let cfg = parse(
            r#"
filter {
  if [code] == 1 {
    mutate { add_tag => ["one"] }
  } else if [code] == 2 {
    mutate { add_tag => ["two"] }
  }
}
"#,
        )
        .expect("parse");
        assert_eq!(cfg.filters.len(), 2);

        let mut e1 = Event::new("");
        e1.set("code", EventValue::Integer(1));
        assert!(cfg.filters[0].condition.as_ref().unwrap().evaluate(&e1));
        assert!(!cfg.filters[1].condition.as_ref().unwrap().evaluate(&e1));

        let mut e2 = Event::new("");
        e2.set("code", EventValue::Integer(2));
        assert!(!cfg.filters[0].condition.as_ref().unwrap().evaluate(&e2));
        assert!(cfg.filters[1].condition.as_ref().unwrap().evaluate(&e2));

        let mut e3 = Event::new("");
        e3.set("code", EventValue::Integer(3));
        assert!(!cfg.filters[0].condition.as_ref().unwrap().evaluate(&e3));
        assert!(!cfg.filters[1].condition.as_ref().unwrap().evaluate(&e3));
    }

    #[test]
    fn test_parse_else_if_else_in_output_section() {
        let cfg = parse(
            r#"
output {
  if [type] == "metric" {
    file { path => "/tmp/m.log" }
  } else if [type] == "log" {
    file { path => "/tmp/l.log" }
  } else {
    stdout { }
  }
}
"#,
        )
        .expect("parse");
        assert_eq!(cfg.outputs.len(), 3);
        assert!(cfg.outputs[0].condition.is_some());
        assert!(cfg.outputs[1].condition.is_some());
        assert!(cfg.outputs[2].condition.is_some());
    }

    #[test]
    fn test_parse_multiple_inputs() {
        let config_str = r#"
input {
  stdin { }
  file {
    path => "/tmp/test.log"
  }
}

output {
  stdout { }
}
"#;
        let config = parse(config_str).expect("parse failed");
        assert_eq!(config.inputs.len(), 2);
        assert_eq!(config.inputs[0].plugin_type, "stdin");
        assert_eq!(config.inputs[1].plugin_type, "file");
    }
}
