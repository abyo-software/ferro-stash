// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024 FerroSearch Authors

#![allow(
    clippy::unreadable_literal, // epoch-ms / Hinnant date-algorithm constants are clearer in canonical form
    clippy::too_many_lines, // eval_expr is a long match arm, factoring would obscure flow
    clippy::manual_let_else, // pre-existing match-Err return idiom; refactor is tangential
    clippy::cast_possible_truncation, // f64 -> i64 / usize narrowing is intentional in script value coercion
    clippy::cast_precision_loss, // i64 -> f64 is intentional in script-value math
    clippy::cast_sign_loss,
    clippy::cast_lossless,
    clippy::cast_possible_wrap,
    clippy::needless_pass_by_value,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::module_name_repetitions
)]

//! Tree-walking evaluator for the Painless scripting language subset.

use std::collections::HashMap;

use crate::error::{FerroError, FerroResult};
use serde_json;

use crate::parser::{self, BinOp, Expr, Stmt, UnaryOp};
use crate::types::ScriptValue;

/// Execution context for a script, providing access to document fields,
/// source, and user-supplied parameters.
pub struct ScriptContext {
    /// Document field values (as returned by `doc['field'].value`).
    pub doc: HashMap<String, serde_json::Value>,
    /// The `_source` document (mutable for update scripts).
    pub source: serde_json::Value,
    /// User-supplied parameters.
    pub params: HashMap<String, serde_json::Value>,
    /// Local variables.
    pub locals: HashMap<String, ScriptValue>,
    /// Compiled regexes keyed by pattern to avoid recompiling on each eval.
    pub regex_cache: HashMap<String, regex::Regex>,
    /// Metadata fields accessible via `ctx._id`, `ctx._index`, `ctx._routing`, etc.
    pub metadata: HashMap<String, String>,
}

impl ScriptContext {
    pub fn new() -> Self {
        Self {
            doc: HashMap::new(),
            source: serde_json::Value::Object(serde_json::Map::new()),
            params: HashMap::new(),
            locals: HashMap::new(),
            regex_cache: HashMap::new(),
            metadata: HashMap::new(),
        }
    }
}

impl Default for ScriptContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse and evaluate a Painless script, returning the result as a JSON value.
pub fn evaluate(script: &str, ctx: &mut ScriptContext) -> FerroResult<serde_json::Value> {
    let stmts = parser::parse(script)?;
    let result = eval_stmts(&stmts, ctx)?;
    Ok(result.into())
}

/// Evaluate a **pre-parsed** program. Callers that run the same script against
/// many inputs (e.g. the `script` filter, once per event) should `parse` once
/// and reuse the cached AST here, instead of calling [`evaluate`] which
/// re-parses the source on every call.
pub fn evaluate_parsed(stmts: &[Stmt], ctx: &mut ScriptContext) -> FerroResult<serde_json::Value> {
    let result = eval_stmts(stmts, ctx)?;
    Ok(result.into())
}

/// Sentinel value to signal an early return from a loop or block.
#[derive(Debug)]
enum StmtResult {
    Value(ScriptValue),
    Return(ScriptValue),
}

fn eval_stmts(stmts: &[Stmt], ctx: &mut ScriptContext) -> FerroResult<ScriptValue> {
    match eval_stmts_inner(stmts, ctx)? {
        StmtResult::Value(v) | StmtResult::Return(v) => Ok(v),
    }
}

fn eval_stmts_inner(stmts: &[Stmt], ctx: &mut ScriptContext) -> FerroResult<StmtResult> {
    let mut last = ScriptValue::Null;
    for stmt in stmts {
        match stmt {
            Stmt::Return(expr) => {
                let val = eval_expr(expr, ctx)?;
                return Ok(StmtResult::Return(val));
            }
            Stmt::Expr(expr) => {
                last = eval_expr(expr, ctx)?;
            }
            Stmt::VarDecl(name, expr) => {
                let val = eval_expr(expr, ctx)?;
                ctx.locals.insert(name.clone(), val.clone());
                last = val;
            }
            Stmt::TryCatch(try_body, exception_var, catch_body) => {
                match eval_stmts_inner(try_body, ctx) {
                    Ok(StmtResult::Return(v)) => return Ok(StmtResult::Return(v)),
                    Ok(StmtResult::Value(v)) => last = v,
                    Err(e) => {
                        ctx.locals
                            .insert(exception_var.clone(), ScriptValue::Str(e.to_string()));
                        match eval_stmts_inner(catch_body, ctx) {
                            Ok(StmtResult::Return(v)) => {
                                return Ok(StmtResult::Return(v));
                            }
                            Ok(StmtResult::Value(v)) => last = v,
                            Err(catch_err) => return Err(catch_err),
                        }
                    }
                }
            }
        }
    }
    Ok(StmtResult::Value(last))
}

fn eval_expr(expr: &Expr, ctx: &mut ScriptContext) -> FerroResult<ScriptValue> {
    match expr {
        Expr::IntLit(n) => Ok(ScriptValue::Int(*n)),
        Expr::FloatLit(f) => Ok(ScriptValue::Float(*f)),
        Expr::StringLit(s) => Ok(ScriptValue::Str(s.clone())),
        Expr::BoolLit(b) => Ok(ScriptValue::Bool(*b)),
        Expr::NullLit => Ok(ScriptValue::Null),

        Expr::DocField(field) => {
            let val = ctx
                .doc
                .get(field)
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            Ok(ScriptValue::from(val))
        }

        Expr::SourceField(field) => {
            // Check metadata fields first (_id, _index, _routing, etc.)
            if field.starts_with('_')
                && let Some(meta_val) = ctx.metadata.get(field)
            {
                return Ok(ScriptValue::Str(meta_val.clone()));
            }
            let val = get_nested_value(&ctx.source, field);
            Ok(ScriptValue::from(val))
        }

        Expr::ParamField(field) => {
            let val = ctx
                .params
                .get(field)
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            Ok(ScriptValue::from(val))
        }

        Expr::Var(name) => {
            if name == "_source" {
                Ok(ScriptValue::from(ctx.source.clone()))
            } else if let Some(val) = ctx.locals.get(name) {
                Ok(val.clone())
            } else {
                Ok(ScriptValue::Null)
            }
        }

        Expr::Assign(field, value_expr) => {
            let value = eval_expr(value_expr, ctx)?;
            let json_value: serde_json::Value = value.clone().into();
            set_nested_value(&mut ctx.source, field, json_value);
            Ok(value)
        }

        Expr::CompoundAssign(field, op, value_expr) => {
            let current = ScriptValue::from(get_nested_value(&ctx.source, field));
            let value = eval_expr(value_expr, ctx)?;
            let updated = eval_binop(&current, op, &value)?;
            let json_value: serde_json::Value = updated.clone().into();
            set_nested_value(&mut ctx.source, field, json_value);
            Ok(updated)
        }

        Expr::BinOp(left, op, right) => {
            let lval = eval_expr(left, ctx)?;
            let rval = eval_expr(right, ctx)?;
            eval_binop(&lval, op, &rval)
        }

        Expr::UnaryOp(op, expr) => {
            let val = eval_expr(expr, ctx)?;
            eval_unary(op, &val)
        }

        Expr::Ternary(cond, then_expr, else_expr) => {
            let cond_val = eval_expr(cond, ctx)?;
            if cond_val.is_truthy() {
                eval_expr(then_expr, ctx)
            } else {
                eval_expr(else_expr, ctx)
            }
        }

        Expr::MethodCall(target, method, args) => {
            let target_val = eval_expr(target, ctx)?;
            let mut arg_vals = Vec::new();
            for a in args {
                arg_vals.push(eval_expr(a, ctx)?);
            }
            eval_method(&target_val, method, &arg_vals)
        }

        Expr::MathCall(func, args) => {
            let mut arg_vals = Vec::new();
            for a in args {
                arg_vals.push(eval_expr(a, ctx)?);
            }
            eval_math(func, &arg_vals)
        }

        Expr::FuncCall(func, args) => {
            let mut arg_vals = Vec::new();
            for a in args {
                arg_vals.push(eval_expr(a, ctx)?);
            }
            eval_func_call(func, &arg_vals, ctx)
        }

        Expr::IfElse(cond, then_body, else_body) => {
            let cond_val = eval_expr(cond, ctx)?;
            if cond_val.is_truthy() {
                eval_stmts(then_body, ctx)
            } else if let Some(else_stmts) = else_body {
                eval_stmts(else_stmts, ctx)
            } else {
                Ok(ScriptValue::Null)
            }
        }

        Expr::For(init, cond, incr, body) => {
            // Execute init
            match init.as_ref() {
                Stmt::VarDecl(name, expr) => {
                    let val = eval_expr(expr, ctx)?;
                    ctx.locals.insert(name.clone(), val);
                }
                Stmt::Expr(expr) => {
                    eval_expr(expr, ctx)?;
                }
                Stmt::Return(expr) => {
                    return eval_expr(expr, ctx);
                }
                Stmt::TryCatch(..) => {}
            }
            let mut last = ScriptValue::Null;
            let mut iteration_count = 0u32;
            loop {
                iteration_count += 1;
                if iteration_count > 100_000 {
                    return Err(FerroError::Internal(
                        "for loop iteration limit exceeded".into(),
                    ));
                }
                let cond_val = eval_expr(cond, ctx)?;
                if !cond_val.is_truthy() {
                    break;
                }
                match eval_stmts_inner(body, ctx)? {
                    StmtResult::Return(v) => return Ok(v),
                    StmtResult::Value(v) => last = v,
                }
                // Execute increment
                match incr.as_ref() {
                    Stmt::VarDecl(name, expr) => {
                        let val = eval_expr(expr, ctx)?;
                        ctx.locals.insert(name.clone(), val);
                    }
                    Stmt::Expr(expr) => {
                        eval_expr(expr, ctx)?;
                    }
                    Stmt::Return(_) | Stmt::TryCatch(..) => {}
                }
            }
            Ok(last)
        }

        Expr::ForEach(var_name, iterable, body) => {
            let iter_val = eval_expr(iterable, ctx)?;
            let items = match iter_val {
                ScriptValue::Array(arr) => arr,
                _ => {
                    return Err(FerroError::QueryParseError(
                        "for-each requires an iterable (array)".into(),
                    ));
                }
            };
            let mut last = ScriptValue::Null;
            for item in items {
                ctx.locals.insert(var_name.clone(), item);
                match eval_stmts_inner(body, ctx)? {
                    StmtResult::Return(v) => return Ok(v),
                    StmtResult::Value(v) => last = v,
                }
            }
            Ok(last)
        }

        Expr::RegexLit(pattern) => Ok(ScriptValue::Regex(pattern.clone())),

        Expr::RegexFind(text_expr, pattern_expr) => {
            let text_val = eval_expr(text_expr, ctx)?;
            let pattern_val = eval_expr(pattern_expr, ctx)?;
            let text = match &text_val {
                ScriptValue::Str(s) => s.as_str(),
                _ => {
                    return Err(FerroError::QueryParseError(
                        "regex find requires string operand".into(),
                    ));
                }
            };
            let pattern = extract_regex_pattern(&pattern_val)?;
            let re = get_or_compile_regex(ctx, &pattern)?;
            Ok(ScriptValue::Bool(re.is_match(text)))
        }

        Expr::RegexMatchOp(text_expr, pattern_expr) => {
            let text_val = eval_expr(text_expr, ctx)?;
            let pattern_val = eval_expr(pattern_expr, ctx)?;
            let text = match &text_val {
                ScriptValue::Str(s) => s.as_str(),
                _ => {
                    return Err(FerroError::QueryParseError(
                        "regex match requires string operand".into(),
                    ));
                }
            };
            let pattern = extract_regex_pattern(&pattern_val)?;
            // ==~ is full match, so anchor the pattern
            let anchored = format!("^(?:{pattern})$");
            let re = get_or_compile_regex(ctx, &anchored)?;
            Ok(ScriptValue::Bool(re.is_match(text)))
        }

        Expr::TypeCast(type_name, expr) => {
            let val = eval_expr(expr, ctx)?;
            match type_name.as_str() {
                "int" | "long" => match &val {
                    ScriptValue::Int(i) => Ok(ScriptValue::Int(*i)),
                    ScriptValue::Float(f) => Ok(ScriptValue::Int(*f as i64)),
                    ScriptValue::Str(s) => {
                        let i: i64 = s.parse().map_err(|_| {
                            FerroError::QueryParseError(format!("cannot cast '{s}' to int"))
                        })?;
                        Ok(ScriptValue::Int(i))
                    }
                    ScriptValue::Bool(b) => Ok(ScriptValue::Int(i64::from(*b))),
                    _ => Ok(ScriptValue::Int(0)),
                },
                "float" | "double" => match &val {
                    ScriptValue::Int(i) => Ok(ScriptValue::Float(*i as f64)),
                    ScriptValue::Float(f) => Ok(ScriptValue::Float(*f)),
                    ScriptValue::Str(s) => {
                        let f: f64 = s.parse().map_err(|_| {
                            FerroError::QueryParseError(format!("cannot cast '{s}' to double"))
                        })?;
                        Ok(ScriptValue::Float(f))
                    }
                    _ => Ok(ScriptValue::Float(0.0)),
                },
                "String" => Ok(ScriptValue::Str(format!("{val}"))),
                "boolean" => Ok(ScriptValue::Bool(val.is_truthy())),
                _ => Ok(val),
            }
        }

        Expr::StaticCall(class, method, args) => {
            let mut arg_vals = Vec::new();
            for a in args {
                arg_vals.push(eval_expr(a, ctx)?);
            }
            eval_static_call(class, method, &arg_vals)
        }

        Expr::Lambda(_params, _body) => {
            // Lambda expressions are not directly evaluable as values.
            // They are used in .stream().map() contexts (future work).
            Ok(ScriptValue::Null)
        }

        Expr::ArrayLit(elements) => {
            let mut vals = Vec::new();
            for e in elements {
                vals.push(eval_expr(e, ctx)?);
            }
            Ok(ScriptValue::Array(vals))
        }

        Expr::VarDecl(name, expr) => {
            let val = eval_expr(expr, ctx)?;
            ctx.locals.insert(name.clone(), val.clone());
            Ok(val)
        }

        Expr::VarCompoundAssign(name, op, expr) => {
            let current = ctx.locals.get(name).cloned().unwrap_or(ScriptValue::Null);
            let value = eval_expr(expr, ctx)?;
            let updated = eval_binop(&current, op, &value)?;
            ctx.locals.insert(name.clone(), updated.clone());
            Ok(updated)
        }

        Expr::IndexAccess(target, index) => {
            let target_val = eval_expr(target, ctx)?;
            let index_val = eval_expr(index, ctx)?;
            match (&target_val, &index_val) {
                (ScriptValue::Array(arr), ScriptValue::Int(i)) => {
                    if *i < 0 {
                        return Ok(ScriptValue::Null);
                    }
                    let idx = *i as usize;
                    Ok(arr.get(idx).cloned().unwrap_or(ScriptValue::Null))
                }
                (ScriptValue::Map(map), ScriptValue::Str(key)) => {
                    Ok(map.get(key).cloned().unwrap_or(ScriptValue::Null))
                }
                _ => Err(FerroError::QueryParseError("invalid index access".into())),
            }
        }
    }
}

/// Get a value from a nested dotted path like "task.ownerId".
fn get_nested_value(source: &serde_json::Value, path: &str) -> serde_json::Value {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = source;
    for part in &parts {
        match current.get(*part) {
            Some(v) => current = v,
            None => return serde_json::Value::Null,
        }
    }
    current.clone()
}

/// Set a value at a nested dotted path like "task.ownerId", creating intermediate objects.
pub fn set_nested_value(source: &mut serde_json::Value, path: &str, value: serde_json::Value) {
    let parts: Vec<&str> = path.split('.').collect();
    if parts.len() == 1 {
        if let serde_json::Value::Object(map) = source {
            map.insert(path.to_string(), value);
        }
    } else {
        // Navigate/create intermediate objects
        let mut current = source;
        for part in &parts[..parts.len() - 1] {
            // Ensure current is an Object; bail if it's a non-Object leaf
            if !current.is_object() {
                return;
            }
            // If intermediate key is missing or not an object, insert an empty object
            if !current.get(*part).is_some_and(serde_json::Value::is_object)
                && let serde_json::Value::Object(map) = &mut *current
            {
                map.insert(
                    part.to_string(),
                    serde_json::Value::Object(serde_json::Map::new()),
                );
            }
            match current.get_mut(*part) {
                Some(next) => current = next,
                None => return,
            }
        }
        if let serde_json::Value::Object(map) = current {
            map.insert(parts[parts.len() - 1].to_string(), value);
        }
    }
}

fn eval_binop(left: &ScriptValue, op: &BinOp, right: &ScriptValue) -> FerroResult<ScriptValue> {
    // String concatenation with +
    if matches!(op, BinOp::Add) {
        if let (ScriptValue::Str(a), ScriptValue::Str(b)) = (left, right) {
            return Ok(ScriptValue::Str(format!("{a}{b}")));
        }
        // String + non-string or non-string + string
        if matches!(left, ScriptValue::Str(_)) || matches!(right, ScriptValue::Str(_)) {
            return Ok(ScriptValue::Str(format!("{left}{right}")));
        }
    }

    // Equality checks work on all types
    match op {
        BinOp::Eq => return Ok(ScriptValue::Bool(left == right)),
        BinOp::Neq => return Ok(ScriptValue::Bool(left != right)),
        _ => {}
    }

    // Logical operators
    match op {
        BinOp::And => return Ok(ScriptValue::Bool(left.is_truthy() && right.is_truthy())),
        BinOp::Or => return Ok(ScriptValue::Bool(left.is_truthy() || right.is_truthy())),
        _ => {}
    }

    // Numeric operations
    let lf = left
        .as_f64()
        .ok_or_else(|| FerroError::QueryParseError(format!("cannot use {left:?} in arithmetic")))?;
    let rf = right.as_f64().ok_or_else(|| {
        FerroError::QueryParseError(format!("cannot use {right:?} in arithmetic"))
    })?;

    // If both are integers and the op produces an integer result, keep as int
    let both_int = matches!(left, ScriptValue::Int(_)) && matches!(right, ScriptValue::Int(_));

    match op {
        BinOp::Add => {
            if both_int {
                Ok(ScriptValue::Int(lf as i64 + rf as i64))
            } else {
                Ok(ScriptValue::Float(lf + rf))
            }
        }
        BinOp::Sub => {
            if both_int {
                Ok(ScriptValue::Int(lf as i64 - rf as i64))
            } else {
                Ok(ScriptValue::Float(lf - rf))
            }
        }
        BinOp::Mul => {
            if both_int {
                Ok(ScriptValue::Int(lf as i64 * rf as i64))
            } else {
                Ok(ScriptValue::Float(lf * rf))
            }
        }
        BinOp::Div => {
            if rf == 0.0 {
                return Err(FerroError::QueryParseError("division by zero".into()));
            }
            if both_int {
                Ok(ScriptValue::Int(lf as i64 / rf as i64))
            } else {
                Ok(ScriptValue::Float(lf / rf))
            }
        }
        BinOp::Mod => {
            if rf == 0.0 {
                return Err(FerroError::QueryParseError("modulo by zero".into()));
            }
            if both_int {
                Ok(ScriptValue::Int(lf as i64 % rf as i64))
            } else {
                Ok(ScriptValue::Float(lf % rf))
            }
        }
        BinOp::Gt => Ok(ScriptValue::Bool(lf > rf)),
        BinOp::Lt => Ok(ScriptValue::Bool(lf < rf)),
        BinOp::Gte => Ok(ScriptValue::Bool(lf >= rf)),
        BinOp::Lte => Ok(ScriptValue::Bool(lf <= rf)),
        BinOp::Eq | BinOp::Neq | BinOp::And | BinOp::Or => unreachable!(),
    }
}

fn eval_unary(op: &UnaryOp, val: &ScriptValue) -> FerroResult<ScriptValue> {
    match op {
        UnaryOp::Not => Ok(ScriptValue::Bool(!val.is_truthy())),
        UnaryOp::Neg => match val {
            ScriptValue::Int(i) => Ok(ScriptValue::Int(-i)),
            ScriptValue::Float(f) => Ok(ScriptValue::Float(-f)),
            _ => Err(FerroError::QueryParseError(format!(
                "cannot negate {val:?}"
            ))),
        },
    }
}

/// Extract the regex pattern string from a `ScriptValue` (regex literal or plain string).
fn extract_regex_pattern(val: &ScriptValue) -> FerroResult<String> {
    match val {
        ScriptValue::Regex(pattern) => Ok(pattern.clone()),
        ScriptValue::Str(s) => Ok(s.clone()),
        _ => Err(FerroError::QueryParseError(
            "regex pattern must be a string or regex literal".into(),
        )),
    }
}

fn get_or_compile_regex<'a>(
    ctx: &'a mut ScriptContext,
    pattern: &str,
) -> FerroResult<&'a regex::Regex> {
    if !ctx.regex_cache.contains_key(pattern) {
        let compiled = regex::Regex::new(pattern)
            .map_err(|e| FerroError::QueryParseError(format!("invalid regex: {e}")))?;
        ctx.regex_cache.insert(pattern.to_string(), compiled);
    }
    Ok(ctx
        .regex_cache
        .get(pattern)
        .expect("regex cache must contain compiled pattern"))
}

fn eval_method(
    target: &ScriptValue,
    method: &str,
    args: &[ScriptValue],
) -> FerroResult<ScriptValue> {
    match (target, method) {
        // String methods
        (ScriptValue::Str(s), "toUpperCase") => Ok(ScriptValue::Str(s.to_uppercase())),
        (ScriptValue::Str(s), "toLowerCase") => Ok(ScriptValue::Str(s.to_lowercase())),
        (ScriptValue::Str(s), "length") => Ok(ScriptValue::Int(s.len() as i64)),
        (ScriptValue::Str(s), "contains") => {
            if let Some(ScriptValue::Str(sub)) = args.first() {
                Ok(ScriptValue::Bool(s.contains(sub.as_str())))
            } else {
                Err(FerroError::QueryParseError(
                    "contains() requires a string argument".into(),
                ))
            }
        }
        (ScriptValue::Str(s), "substring") => {
            let start = match args.first() {
                Some(ScriptValue::Int(i)) => *i as usize,
                _ => {
                    return Err(FerroError::QueryParseError(
                        "substring() requires integer arguments".into(),
                    ));
                }
            };
            let end = match args.get(1) {
                Some(ScriptValue::Int(i)) => *i as usize,
                None => s.len(),
                _ => {
                    return Err(FerroError::QueryParseError(
                        "substring() requires integer arguments".into(),
                    ));
                }
            };
            let start = start.min(s.len());
            let end = end.min(s.len());
            // `s.get` returns None when start > end or when either index falls
            // inside a multibyte character — never panic on attacker-supplied
            // event data (a raw `s[start..end]` slice would).
            match s.get(start..end) {
                Some(sub) => Ok(ScriptValue::Str(sub.to_string())),
                None => Err(FerroError::QueryParseError(
                    "substring() indices out of range or not on a character boundary".into(),
                )),
            }
        }
        (ScriptValue::Str(s), "trim") => Ok(ScriptValue::Str(s.trim().to_string())),
        (ScriptValue::Str(s), "startsWith") => {
            if let Some(ScriptValue::Str(prefix)) = args.first() {
                Ok(ScriptValue::Bool(s.starts_with(prefix.as_str())))
            } else {
                Err(FerroError::QueryParseError(
                    "startsWith() requires a string argument".into(),
                ))
            }
        }
        (ScriptValue::Str(s), "endsWith") => {
            if let Some(ScriptValue::Str(suffix)) = args.first() {
                Ok(ScriptValue::Bool(s.ends_with(suffix.as_str())))
            } else {
                Err(FerroError::QueryParseError(
                    "endsWith() requires a string argument".into(),
                ))
            }
        }
        (ScriptValue::Str(s), "replace") => {
            if let (Some(ScriptValue::Str(from)), Some(ScriptValue::Str(to))) =
                (args.first(), args.get(1))
            {
                Ok(ScriptValue::Str(s.replace(from.as_str(), to.as_str())))
            } else {
                Err(FerroError::QueryParseError(
                    "replace() requires two string arguments".into(),
                ))
            }
        }
        (ScriptValue::Str(s), "split") => {
            if let Some(ScriptValue::Str(sep)) = args.first() {
                let parts: Vec<ScriptValue> = s
                    .split(sep.as_str())
                    .map(|p| ScriptValue::Str(p.to_string()))
                    .collect();
                Ok(ScriptValue::Array(parts))
            } else {
                Err(FerroError::QueryParseError(
                    "split() requires a string argument".into(),
                ))
            }
        }
        (ScriptValue::Str(s), "indexOf") => {
            if let Some(ScriptValue::Str(sub)) = args.first() {
                Ok(ScriptValue::Int(
                    s.find(sub.as_str()).map_or(-1, |i| i as i64),
                ))
            } else {
                Err(FerroError::QueryParseError(
                    "indexOf() requires a string argument".into(),
                ))
            }
        }
        (ScriptValue::Str(s), "charAt") => {
            if let Some(ScriptValue::Int(i)) = args.first() {
                if *i < 0 {
                    return Ok(ScriptValue::Null);
                }
                let idx = *i as usize;
                if let Some(ch) = s.chars().nth(idx) {
                    Ok(ScriptValue::Str(ch.to_string()))
                } else {
                    Ok(ScriptValue::Null)
                }
            } else {
                Err(FerroError::QueryParseError(
                    "charAt() requires an integer argument".into(),
                ))
            }
        }
        // Regex matcher method on regex literal
        (ScriptValue::Regex(pattern), "matcher") => {
            if let Some(ScriptValue::Str(text)) = args.first() {
                // Return a "matcher" value encoded as a special string
                Ok(ScriptValue::Str(format!("__matcher:{pattern}:{text}")))
            } else {
                Err(FerroError::QueryParseError(
                    "matcher() requires a string argument".into(),
                ))
            }
        }
        // Matcher find/matches
        (ScriptValue::Str(s), "find") if s.starts_with("__matcher:") => {
            let rest = &s["__matcher:".len()..];
            if let Some(colon_pos) = rest.find(':') {
                let pattern = &rest[..colon_pos];
                let text = &rest[colon_pos + 1..];
                let re = regex::Regex::new(pattern)
                    .map_err(|e| FerroError::QueryParseError(format!("invalid regex: {e}")))?;
                Ok(ScriptValue::Bool(re.is_match(text)))
            } else {
                Ok(ScriptValue::Bool(false))
            }
        }
        (ScriptValue::Str(s), "matches") if s.starts_with("__matcher:") => {
            let rest = &s["__matcher:".len()..];
            if let Some(colon_pos) = rest.find(':') {
                let pattern = &rest[..colon_pos];
                let text = &rest[colon_pos + 1..];
                let anchored = format!("^(?:{pattern})$");
                let re = regex::Regex::new(&anchored)
                    .map_err(|e| FerroError::QueryParseError(format!("invalid regex: {e}")))?;
                Ok(ScriptValue::Bool(re.is_match(text)))
            } else {
                Ok(ScriptValue::Bool(false))
            }
        }

        // Array/List methods
        (ScriptValue::Array(arr), "size") => Ok(ScriptValue::Int(arr.len() as i64)),
        (ScriptValue::Array(arr), "length") => Ok(ScriptValue::Int(arr.len() as i64)),
        (ScriptValue::Array(arr), "contains") => {
            if let Some(val) = args.first() {
                Ok(ScriptValue::Bool(arr.contains(val)))
            } else {
                Err(FerroError::QueryParseError(
                    "contains() requires an argument".into(),
                ))
            }
        }
        (ScriptValue::Array(arr), "indexOf") => {
            if let Some(val) = args.first() {
                Ok(ScriptValue::Int(
                    arr.iter().position(|v| v == val).map_or(-1, |i| i as i64),
                ))
            } else {
                Err(FerroError::QueryParseError(
                    "indexOf() requires an argument".into(),
                ))
            }
        }
        (ScriptValue::Array(arr), "add") => {
            if let Some(val) = args.first() {
                let mut new_arr = arr.clone();
                new_arr.push(val.clone());
                Ok(ScriptValue::Array(new_arr))
            } else {
                Err(FerroError::QueryParseError(
                    "add() requires an argument".into(),
                ))
            }
        }
        (ScriptValue::Array(arr), "remove") => {
            if let Some(ScriptValue::Int(i)) = args.first() {
                let mut new_arr = arr.clone();
                let idx = *i as usize;
                if idx < new_arr.len() {
                    new_arr.remove(idx);
                }
                Ok(ScriptValue::Array(new_arr))
            } else {
                Err(FerroError::QueryParseError(
                    "remove() requires an integer argument".into(),
                ))
            }
        }
        (ScriptValue::Array(arr), "isEmpty") => Ok(ScriptValue::Bool(arr.is_empty())),
        (ScriptValue::Array(arr), "get") => {
            if let Some(ScriptValue::Int(i)) = args.first() {
                let idx = *i as usize;
                Ok(arr.get(idx).cloned().unwrap_or(ScriptValue::Null))
            } else {
                Err(FerroError::QueryParseError(
                    "get() requires an integer argument".into(),
                ))
            }
        }
        (ScriptValue::Array(arr), "join") => {
            let sep = match args.first() {
                Some(ScriptValue::Str(s)) => s.as_str(),
                _ => ",",
            };
            let joined: String = arr
                .iter()
                .map(|v| format!("{v}"))
                .collect::<Vec<_>>()
                .join(sep);
            Ok(ScriptValue::Str(joined))
        }

        // Map methods
        (ScriptValue::Map(map), "containsKey") => {
            if let Some(ScriptValue::Str(key)) = args.first() {
                Ok(ScriptValue::Bool(map.contains_key(key)))
            } else {
                Err(FerroError::QueryParseError(
                    "containsKey() requires a string argument".into(),
                ))
            }
        }
        (ScriptValue::Map(map), "get") => {
            if let Some(ScriptValue::Str(key)) = args.first() {
                Ok(map.get(key).cloned().unwrap_or(ScriptValue::Null))
            } else {
                Err(FerroError::QueryParseError(
                    "get() requires a string argument".into(),
                ))
            }
        }
        (ScriptValue::Map(map), "put") => {
            if let (Some(ScriptValue::Str(key)), Some(val)) = (args.first(), args.get(1)) {
                let mut new_map = map.clone();
                new_map.insert(key.clone(), val.clone());
                Ok(ScriptValue::Map(new_map))
            } else {
                Err(FerroError::QueryParseError(
                    "put() requires a string key and a value".into(),
                ))
            }
        }
        (ScriptValue::Map(map), "remove") => {
            if let Some(ScriptValue::Str(key)) = args.first() {
                let mut new_map = map.clone();
                new_map.remove(key);
                Ok(ScriptValue::Map(new_map))
            } else {
                Err(FerroError::QueryParseError(
                    "remove() requires a string argument".into(),
                ))
            }
        }
        (ScriptValue::Map(map), "keySet") => {
            let keys: Vec<ScriptValue> = map.keys().map(|k| ScriptValue::Str(k.clone())).collect();
            Ok(ScriptValue::Array(keys))
        }
        (ScriptValue::Map(map), "values") => {
            let vals: Vec<ScriptValue> = map.values().cloned().collect();
            Ok(ScriptValue::Array(vals))
        }
        (ScriptValue::Map(map), "size") => Ok(ScriptValue::Int(map.len() as i64)),
        (ScriptValue::Map(map), "isEmpty") => Ok(ScriptValue::Bool(map.is_empty())),
        (ScriptValue::Map(map), "containsValue") => {
            if let Some(val) = args.first() {
                Ok(ScriptValue::Bool(map.values().any(|v| v == val)))
            } else {
                Err(FerroError::QueryParseError(
                    "containsValue() requires an argument".into(),
                ))
            }
        }

        // .matches() on a matcher Map
        (ScriptValue::Map(map), "matches") => Ok(ScriptValue::Bool(
            map.get("_matched")
                .is_some_and(|v| matches!(v, ScriptValue::Bool(true))),
        )),
        // .group(N) on a matcher Map
        (ScriptValue::Map(map), "group") => {
            let idx = match args.first() {
                Some(ScriptValue::Int(i)) => i.to_string(),
                _ => "0".to_string(),
            };
            Ok(map.get(&idx).cloned().unwrap_or(ScriptValue::Null))
        }
        // .value is a no-op (used in doc['field'].value access)
        (val, "value") => Ok(val.clone()),

        // ── Date/Time methods (ZonedDateTime-compatible) ──
        // Works on ISO 8601 date strings and epoch millis (Int).
        // ES Painless: doc['date_field'].value.getYear(), .getMonthValue(), etc.
        (
            val,
            method_name @ ("getYear" | "getMonthValue" | "getDayOfMonth" | "getHour" | "getMinute"
            | "getSecond" | "getNano" | "getDayOfWeek" | "getDayOfYear"
            | "getMillis" | "toEpochMilli" | "toInstant"),
        ) => {
            let (year, month, day, hour, minute, second, epoch_ms) = parse_date_components(val)?;
            match method_name {
                "getYear" => Ok(ScriptValue::Int(year)),
                "getMonthValue" => Ok(ScriptValue::Int(month)),
                "getDayOfMonth" => Ok(ScriptValue::Int(day)),
                "getHour" => Ok(ScriptValue::Int(hour)),
                "getMinute" => Ok(ScriptValue::Int(minute)),
                "getSecond" => Ok(ScriptValue::Int(second)),
                "getNano" => Ok(ScriptValue::Int(0)), // sub-second not tracked
                "getDayOfWeek" => {
                    // ISO day of week: Monday=1, Sunday=7
                    // Use Zeller-like calculation from epoch_ms
                    let days = epoch_ms / 86_400_000;
                    // 1970-01-01 was Thursday (4)
                    let dow = (days % 7 + 4 - 1) % 7 + 1;
                    Ok(ScriptValue::Int(dow))
                }
                "getDayOfYear" => {
                    // `month` comes from a parsed (possibly attacker-supplied)
                    // date string and may be out of 1..=12 (e.g. "2025-99-15");
                    // validate before indexing so `[..(month-1)]` can't go
                    // out of bounds or underflow.
                    if !(1..=12).contains(&month) {
                        return Err(FerroError::QueryParseError(format!(
                            "getDayOfYear(): invalid month {month} in date"
                        )));
                    }
                    let is_leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
                    let days_in_months: &[i64] = if is_leap {
                        &[31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
                    } else {
                        &[31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
                    };
                    let doy: i64 = days_in_months[..(month as usize - 1)].iter().sum::<i64>() + day;
                    Ok(ScriptValue::Int(doy))
                }
                "getMillis" | "toEpochMilli" | "toInstant" => Ok(ScriptValue::Int(epoch_ms)),
                _ => unreachable!(),
            }
        }
        // plusDays / minusDays / plusHours / minusHours — return epoch millis
        (
            val,
            method_name @ ("plusDays" | "minusDays" | "plusHours" | "minusHours" | "plusMinutes"
            | "minusMinutes" | "plusSeconds" | "minusSeconds"),
        ) => {
            let (_, _, _, _, _, _, epoch_ms) = parse_date_components(val)?;
            let amount = args
                .first()
                .and_then(super::types::ScriptValue::as_f64)
                .unwrap_or(0.0) as i64;
            let delta_ms = match method_name {
                "plusDays" => amount * 86_400_000,
                "minusDays" => -amount * 86_400_000,
                "plusHours" => amount * 3_600_000,
                "minusHours" => -amount * 3_600_000,
                "plusMinutes" => amount * 60_000,
                "minusMinutes" => -amount * 60_000,
                "plusSeconds" => amount * 1_000,
                "minusSeconds" => -amount * 1_000,
                _ => unreachable!(),
            };
            Ok(ScriptValue::Int(epoch_ms + delta_ms))
        }

        _ => Err(FerroError::QueryParseError(format!(
            "unknown method: {method}"
        ))),
    }
}

/// Parse date components from a `ScriptValue` (ISO 8601 string or epoch millis).
fn parse_date_components(val: &ScriptValue) -> FerroResult<(i64, i64, i64, i64, i64, i64, i64)> {
    match val {
        ScriptValue::Int(epoch_ms) => {
            // Epoch millis → components
            let secs = epoch_ms / 1000;
            let days = secs / 86400;
            let time_of_day = secs % 86400;
            let hour = time_of_day / 3600;
            let minute = (time_of_day % 3600) / 60;
            let second = time_of_day % 60;

            // Civil date from days since 1970-01-01 (Howard Hinnant algorithm).
            // Magic numbers (719468 epoch shift, 146097 days/era, 146096 prev-era,
            // 36524 days/century, 1460 days/leap, 365 days/yr) come straight from
            // the Hinnant paper: http://howardhinnant.github.io/date_algorithms.html
            let z = days + 719468;
            let era = if z >= 0 { z } else { z - 146096 } / 146097;
            let doe = z - era * 146097;
            let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
            let y = yoe + era * 400;
            let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
            let mp = (5 * doy + 2) / 153;
            let d = doy - (153 * mp + 2) / 5 + 1;
            let m = if mp < 10 { mp + 3 } else { mp - 9 };
            let y = if m <= 2 { y + 1 } else { y };
            Ok((y, m, d, hour, minute, second, *epoch_ms))
        }
        ScriptValue::Str(s) => {
            // Parse ISO 8601: "2025-03-15T10:30:00Z" or "2025-03-15"
            let s = s.trim();
            // ISO 8601 timestamps are pure ASCII. Require ASCII (and length)
            // up front so the byte-index slicing below can never split a
            // multibyte character — a raw `s[0..4]` on event data like "€€€€"
            // would panic and wedge the filter worker.
            if s.len() < 10 || !s.is_ascii() {
                return Err(FerroError::QueryParseError(format!(
                    "cannot parse date: {s}"
                )));
            }
            let year: i64 = s[0..4].parse().unwrap_or(1970);
            let month: i64 = s[5..7].parse().unwrap_or(1);
            let day: i64 = s[8..10].parse().unwrap_or(1);
            let (hour, minute, second) = if s.len() >= 19 {
                (
                    s[11..13].parse().unwrap_or(0),
                    s[14..16].parse().unwrap_or(0),
                    s[17..19].parse().unwrap_or(0),
                )
            } else {
                (0i64, 0i64, 0i64)
            };
            // Compute epoch millis
            let epoch_ms = date_to_epoch_ms(year, month, day, hour, minute, second);
            Ok((year, month, day, hour, minute, second, epoch_ms))
        }
        ScriptValue::Float(f) => {
            // Treat as epoch millis
            let epoch_ms = *f as i64;
            let sv = ScriptValue::Int(epoch_ms);
            parse_date_components(&sv)
        }
        _ => Err(FerroError::QueryParseError(format!(
            "cannot interpret as date: {val}"
        ))),
    }
}

/// Convert civil date+time to epoch milliseconds (UTC).
fn date_to_epoch_ms(year: i64, month: i64, day: i64, hour: i64, minute: i64, second: i64) -> i64 {
    // Howard Hinnant days_from_civil
    let y = if month <= 2 { year - 1 } else { year };
    let m = if month <= 2 { month + 9 } else { month - 3 };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * m + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;
    days * 86_400_000 + hour * 3_600_000 + minute * 60_000 + second * 1_000
}

fn eval_static_call(class: &str, method: &str, args: &[ScriptValue]) -> FerroResult<ScriptValue> {
    match (class, method) {
        ("Integer" | "Long", "parseInt" | "parseLong") => {
            if let Some(ScriptValue::Str(s)) = args.first() {
                let i: i64 = s.trim().parse().map_err(|_| {
                    FerroError::QueryParseError(format!("cannot parse '{s}' as integer"))
                })?;
                Ok(ScriptValue::Int(i))
            } else if let Some(ScriptValue::Int(i)) = args.first() {
                Ok(ScriptValue::Int(*i))
            } else {
                Err(FerroError::QueryParseError(format!(
                    "{class}.{method}() requires a string argument"
                )))
            }
        }
        ("Float" | "Double", "parseFloat" | "parseDouble") => {
            if let Some(ScriptValue::Str(s)) = args.first() {
                let f: f64 = s.trim().parse().map_err(|_| {
                    FerroError::QueryParseError(format!("cannot parse '{s}' as float"))
                })?;
                Ok(ScriptValue::Float(f))
            } else if let Some(ScriptValue::Float(f)) = args.first() {
                Ok(ScriptValue::Float(*f))
            } else {
                Err(FerroError::QueryParseError(format!(
                    "{class}.{method}() requires a string argument"
                )))
            }
        }
        ("String", "valueOf") => {
            if let Some(val) = args.first() {
                Ok(ScriptValue::Str(format!("{val}")))
            } else {
                Ok(ScriptValue::Str(String::new()))
            }
        }
        ("Integer" | "Long", "valueOf") => {
            if let Some(ScriptValue::Str(s)) = args.first() {
                let i: i64 = s.trim().parse().map_err(|_| {
                    FerroError::QueryParseError(format!("cannot parse '{s}' as integer"))
                })?;
                Ok(ScriptValue::Int(i))
            } else if let Some(ScriptValue::Int(i)) = args.first() {
                Ok(ScriptValue::Int(*i))
            } else {
                Err(FerroError::QueryParseError(format!(
                    "{class}.valueOf() requires an argument"
                )))
            }
        }
        ("Boolean", "parseBoolean" | "valueOf") => {
            if let Some(ScriptValue::Str(s)) = args.first() {
                Ok(ScriptValue::Bool(s.eq_ignore_ascii_case("true")))
            } else if let Some(ScriptValue::Bool(b)) = args.first() {
                Ok(ScriptValue::Bool(*b))
            } else {
                Ok(ScriptValue::Bool(false))
            }
        }
        _ => Err(FerroError::QueryParseError(format!(
            "unknown static method: {class}.{method}"
        ))),
    }
}

fn eval_math(func: &str, args: &[ScriptValue]) -> FerroResult<ScriptValue> {
    match func {
        "max" => {
            let a = args
                .first()
                .and_then(super::types::ScriptValue::as_f64)
                .ok_or_else(|| FerroError::QueryParseError("Math.max requires numbers".into()))?;
            let b = args
                .get(1)
                .and_then(super::types::ScriptValue::as_f64)
                .ok_or_else(|| FerroError::QueryParseError("Math.max requires numbers".into()))?;
            let both_int =
                matches!(args[0], ScriptValue::Int(_)) && matches!(args[1], ScriptValue::Int(_));
            if both_int {
                Ok(ScriptValue::Int(a.max(b) as i64))
            } else {
                Ok(ScriptValue::Float(a.max(b)))
            }
        }
        "min" => {
            let a = args
                .first()
                .and_then(super::types::ScriptValue::as_f64)
                .ok_or_else(|| FerroError::QueryParseError("Math.min requires numbers".into()))?;
            let b = args
                .get(1)
                .and_then(super::types::ScriptValue::as_f64)
                .ok_or_else(|| FerroError::QueryParseError("Math.min requires numbers".into()))?;
            let both_int =
                matches!(args[0], ScriptValue::Int(_)) && matches!(args[1], ScriptValue::Int(_));
            if both_int {
                Ok(ScriptValue::Int(a.min(b) as i64))
            } else {
                Ok(ScriptValue::Float(a.min(b)))
            }
        }
        "abs" => {
            let a = args.first().ok_or_else(|| {
                FerroError::QueryParseError("Math.abs requires an argument".into())
            })?;
            match a {
                ScriptValue::Int(i) => Ok(ScriptValue::Int(i.abs())),
                ScriptValue::Float(f) => Ok(ScriptValue::Float(f.abs())),
                _ => Err(FerroError::QueryParseError(
                    "Math.abs requires a number".into(),
                )),
            }
        }
        // ── Single-argument f64 functions ──
        "log" | "log10" | "log2" | "exp" | "sqrt" | "cbrt" | "ceil" | "floor" | "round"
        | "rint" | "sin" | "cos" | "tan" | "asin" | "acos" | "atan" | "sinh" | "cosh" | "tanh"
        | "signum" | "toRadians" | "toDegrees" => {
            let a = args
                .first()
                .and_then(super::types::ScriptValue::as_f64)
                .ok_or_else(|| {
                    FerroError::QueryParseError(format!("Math.{func} requires a number"))
                })?;
            let result = match func {
                "log" => a.ln(),
                "log10" => a.log10(),
                "log2" => a.log2(),
                "exp" => a.exp(),
                "sqrt" => a.sqrt(),
                "cbrt" => a.cbrt(),
                "ceil" => a.ceil(),
                "floor" => a.floor(),
                "round" | "rint" => a.round(),
                "sin" => a.sin(),
                "cos" => a.cos(),
                "tan" => a.tan(),
                "asin" => a.asin(),
                "acos" => a.acos(),
                "atan" => a.atan(),
                "sinh" => a.sinh(),
                "cosh" => a.cosh(),
                "tanh" => a.tanh(),
                "signum" => {
                    if a > 0.0 {
                        1.0
                    } else if a < 0.0 {
                        -1.0
                    } else {
                        0.0
                    }
                }
                "toRadians" => a.to_radians(),
                "toDegrees" => a.to_degrees(),
                _ => unreachable!(),
            };
            Ok(ScriptValue::Float(result))
        }
        // ── Two-argument functions ──
        "pow" => {
            let a = args
                .first()
                .and_then(super::types::ScriptValue::as_f64)
                .ok_or_else(|| {
                    FerroError::QueryParseError("Math.pow requires two numbers".into())
                })?;
            let b = args
                .get(1)
                .and_then(super::types::ScriptValue::as_f64)
                .ok_or_else(|| {
                    FerroError::QueryParseError("Math.pow requires two numbers".into())
                })?;
            Ok(ScriptValue::Float(a.powf(b)))
        }
        "atan2" => {
            let a = args
                .first()
                .and_then(super::types::ScriptValue::as_f64)
                .ok_or_else(|| {
                    FerroError::QueryParseError("Math.atan2 requires two numbers".into())
                })?;
            let b = args
                .get(1)
                .and_then(super::types::ScriptValue::as_f64)
                .ok_or_else(|| {
                    FerroError::QueryParseError("Math.atan2 requires two numbers".into())
                })?;
            Ok(ScriptValue::Float(a.atan2(b)))
        }
        // ── Zero-argument ──
        "random" => Ok(ScriptValue::Float(rand_f64())),
        _ => Err(FerroError::QueryParseError(format!(
            "unknown Math function: {func}"
        ))),
    }
}

/// Coerce a `ScriptValue` into a `Vec<f64>` of vector components.
/// Accepts arrays of numbers, arrays-of-arrays (taking the first sub-array,
/// mirroring how `dense_vector` arrives in `_source`), and tolerates booleans.
fn to_vector(val: &ScriptValue) -> FerroResult<Vec<f64>> {
    match val {
        ScriptValue::Array(arr) => {
            // Some _source representations wrap the vector inside an outer array
            // (e.g. multi-valued field). Unwrap one level if needed.
            if arr.len() == 1 && matches!(arr[0], ScriptValue::Array(_)) {
                return to_vector(&arr[0]);
            }
            let mut out = Vec::with_capacity(arr.len());
            for v in arr {
                let f = v.as_f64().ok_or_else(|| {
                    FerroError::QueryParseError(
                        "vector element must be numeric for similarity function".into(),
                    )
                })?;
                out.push(f);
            }
            Ok(out)
        }
        ScriptValue::Null => Ok(Vec::new()),
        _ => Err(FerroError::QueryParseError(
            "expected an array for vector similarity argument".into(),
        )),
    }
}

/// Resolve the second argument of `dotProduct(query, field)` /
/// `cosineSimilarity(query, field)` / `l1norm`, `l2norm` etc. into the
/// stored document vector. The argument is a string field name (the same
/// behaviour as Painless: the field is read from doc-values / `_source`).
fn resolve_doc_vector(arg: &ScriptValue, ctx: &ScriptContext) -> FerroResult<Vec<f64>> {
    match arg {
        ScriptValue::Str(field) => {
            // Prefer doc[field] (set by the host before evaluation),
            // fall back to _source for nested objects.
            if let Some(v) = ctx.doc.get(field) {
                return to_vector(&ScriptValue::from(v.clone()));
            }
            let v = get_nested_value(&ctx.source, field);
            to_vector(&ScriptValue::from(v))
        }
        // Allow the caller to pass a literal vector instead of a field name.
        ScriptValue::Array(_) => to_vector(arg),
        _ => Err(FerroError::QueryParseError(
            "vector similarity field argument must be a string".into(),
        )),
    }
}

/// Built-in free function dispatch (vector similarity helpers, etc.).
fn eval_func_call(
    func: &str,
    args: &[ScriptValue],
    ctx: &ScriptContext,
) -> FerroResult<ScriptValue> {
    match func {
        "dotProduct" | "cosineSimilarity" | "l1norm" | "l2norm" | "hamming" => {
            if args.len() != 2 {
                return Err(FerroError::QueryParseError(format!(
                    "{func} requires (queryVector, field)"
                )));
            }
            let query = to_vector(&args[0])?;
            let stored = resolve_doc_vector(&args[1], ctx)?;
            if query.len() != stored.len() {
                return Err(FerroError::QueryParseError(format!(
                    "{func}: vector dimension mismatch ({} vs {})",
                    query.len(),
                    stored.len()
                )));
            }
            if query.is_empty() {
                return Ok(ScriptValue::Float(0.0));
            }
            let result = match func {
                "dotProduct" => query.iter().zip(&stored).map(|(a, b)| a * b).sum::<f64>(),
                "cosineSimilarity" => {
                    let dot: f64 = query.iter().zip(&stored).map(|(a, b)| a * b).sum();
                    let nq: f64 = query.iter().map(|a| a * a).sum::<f64>().sqrt();
                    let ns: f64 = stored.iter().map(|a| a * a).sum::<f64>().sqrt();
                    let denom = nq * ns;
                    if denom == 0.0 { 0.0 } else { dot / denom }
                }
                "l1norm" => query
                    .iter()
                    .zip(&stored)
                    .map(|(a, b)| (a - b).abs())
                    .sum::<f64>(),
                "l2norm" => query
                    .iter()
                    .zip(&stored)
                    .map(|(a, b)| (a - b).powi(2))
                    .sum::<f64>()
                    .sqrt(),
                "hamming" => query
                    .iter()
                    .zip(&stored)
                    .filter(|(a, b)| (**a - **b).abs() > f64::EPSILON)
                    .count() as f64,
                _ => unreachable!(),
            };
            Ok(ScriptValue::Float(result))
        }
        // Painless `decayDateLinear`/`decayNumericGauss` etc. are not yet
        // implemented; surface a clear error rather than silently returning 0.
        _ => Err(FerroError::QueryParseError(format!(
            "unknown function: {func}"
        ))),
    }
}

/// Simple pseudo-random f64 in [0, 1) using thread-local state.
fn rand_f64() -> f64 {
    use std::cell::Cell;
    thread_local! {
        static STATE: Cell<u64> = const { Cell::new(0x12345678_9abcdef0) };
    }
    STATE.with(|s| {
        let mut x = s.get();
        // xorshift64
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        s.set(x);
        (x as f64) / (u64::MAX as f64)
    })
}

#[cfg(test)]
#[allow(clippy::float_cmp)]
mod tests {
    use super::*;

    fn eval(script: &str) -> serde_json::Value {
        let mut ctx = ScriptContext::new();
        evaluate(script, &mut ctx).unwrap()
    }

    fn eval_with_ctx(script: &str, ctx: &mut ScriptContext) -> serde_json::Value {
        evaluate(script, ctx).unwrap()
    }

    #[test]
    fn test_integer_arithmetic() {
        assert_eq!(eval("1 + 2"), serde_json::json!(3));
        assert_eq!(eval("10 - 3"), serde_json::json!(7));
        assert_eq!(eval("4 * 5"), serde_json::json!(20));
        assert_eq!(eval("10 / 3"), serde_json::json!(3));
        assert_eq!(eval("10 % 3"), serde_json::json!(1));
    }

    #[test]
    fn test_float_arithmetic() {
        assert_eq!(eval("1.5 + 2.5"), serde_json::json!(4.0));
        assert_eq!(eval("3.0 * 2.0"), serde_json::json!(6.0));
    }

    #[test]
    fn test_comparison() {
        assert_eq!(eval("5 > 3"), serde_json::json!(true));
        assert_eq!(eval("3 > 5"), serde_json::json!(false));
        assert_eq!(eval("3 >= 3"), serde_json::json!(true));
        assert_eq!(eval("2 < 5"), serde_json::json!(true));
        assert_eq!(eval("5 <= 5"), serde_json::json!(true));
        assert_eq!(eval("5 == 5"), serde_json::json!(true));
        assert_eq!(eval("5 != 3"), serde_json::json!(true));
    }

    #[test]
    fn test_logical_operators() {
        assert_eq!(eval("true && true"), serde_json::json!(true));
        assert_eq!(eval("true && false"), serde_json::json!(false));
        assert_eq!(eval("false || true"), serde_json::json!(true));
        assert_eq!(eval("false || false"), serde_json::json!(false));
        assert_eq!(eval("!false"), serde_json::json!(true));
    }

    #[test]
    fn test_string_methods() {
        assert_eq!(eval("'hello'.toUpperCase()"), serde_json::json!("HELLO"));
        assert_eq!(eval("'HELLO'.toLowerCase()"), serde_json::json!("hello"));
        assert_eq!(eval("'hello'.length()"), serde_json::json!(5));
        assert_eq!(
            eval("'hello world'.contains('world')"),
            serde_json::json!(true)
        );
        assert_eq!(
            eval("'hello world'.contains('xyz')"),
            serde_json::json!(false)
        );
        assert_eq!(eval("'hello'.substring(1, 3)"), serde_json::json!("el"));
    }

    #[test]
    fn test_string_concatenation() {
        assert_eq!(
            eval("'hello' + ' ' + 'world'"),
            serde_json::json!("hello world")
        );
        assert_eq!(eval("'count: ' + 42"), serde_json::json!("count: 42"));
    }

    #[test]
    fn test_doc_field_access() {
        let mut ctx = ScriptContext::new();
        ctx.doc.insert("price".into(), serde_json::json!(99.99));
        let result = eval_with_ctx("doc['price'].value", &mut ctx);
        assert_eq!(result, serde_json::json!(99.99));
    }

    #[test]
    fn test_source_field_access() {
        let mut ctx = ScriptContext::new();
        ctx.source = serde_json::json!({"name": "test", "count": 5});
        let result = eval_with_ctx("ctx._source.name", &mut ctx);
        assert_eq!(result, serde_json::json!("test"));
    }

    #[test]
    fn test_source_bracket_access() {
        let mut ctx = ScriptContext::new();
        ctx.source = serde_json::json!({"name": "test"});
        let result = eval_with_ctx("ctx._source['name']", &mut ctx);
        assert_eq!(result, serde_json::json!("test"));
    }

    #[test]
    fn test_params_access() {
        let mut ctx = ScriptContext::new();
        ctx.params.insert("factor".into(), serde_json::json!(2));
        let result = eval_with_ctx("params.factor", &mut ctx);
        assert_eq!(result, serde_json::json!(2));
    }

    #[test]
    fn test_assignment() {
        let mut ctx = ScriptContext::new();
        ctx.source = serde_json::json!({"count": 1});
        eval_with_ctx("ctx._source.count = 42", &mut ctx);
        assert_eq!(ctx.source["count"], serde_json::json!(42));
    }

    #[test]
    fn test_ternary() {
        assert_eq!(eval("true ? 'yes' : 'no'"), serde_json::json!("yes"));
        assert_eq!(eval("false ? 'yes' : 'no'"), serde_json::json!("no"));
    }

    #[test]
    fn test_if_else() {
        assert_eq!(
            eval("if (true) { return 'a'; } else { return 'b'; }"),
            serde_json::json!("a")
        );
        assert_eq!(
            eval("if (false) { return 'a'; } else { return 'b'; }"),
            serde_json::json!("b")
        );
    }

    #[test]
    fn test_null_handling() {
        assert_eq!(eval("null"), serde_json::Value::Null);
        assert_eq!(eval("null == null"), serde_json::json!(true));
        assert_eq!(eval("null != 1"), serde_json::json!(true));
    }

    #[test]
    fn test_math_functions() {
        assert_eq!(eval("Math.max(3, 7)"), serde_json::json!(7));
        assert_eq!(eval("Math.min(3, 7)"), serde_json::json!(3));
        assert_eq!(eval("Math.abs(-5)"), serde_json::json!(5));
        assert_eq!(eval("Math.abs(5)"), serde_json::json!(5));
    }

    #[test]
    fn test_unary_negation() {
        assert_eq!(eval("-5"), serde_json::json!(-5));
        assert_eq!(eval("-3.125"), serde_json::json!(-3.125));
    }

    #[test]
    fn test_complex_expression() {
        let mut ctx = ScriptContext::new();
        ctx.doc.insert("price".into(), serde_json::json!(100));
        ctx.params.insert("discount".into(), serde_json::json!(20));
        let result = eval_with_ctx("doc['price'].value - params.discount", &mut ctx);
        assert_eq!(result, serde_json::json!(80));
    }

    #[test]
    fn test_division_by_zero() {
        let mut ctx = ScriptContext::new();
        let result = evaluate("1 / 0", &mut ctx);
        assert!(result.is_err());
    }

    // ---- For loop tests ----

    #[test]
    fn test_for_loop_sum() {
        let mut ctx = ScriptContext::new();
        let result = eval_with_ctx(
            "def sum = 0; for (int i = 0; i < 5; i = i + 1) { sum = sum + i; }; return sum;",
            &mut ctx,
        );
        assert_eq!(result, serde_json::json!(10));
    }

    #[test]
    fn test_foreach_loop() {
        let mut ctx = ScriptContext::new();
        ctx.source = serde_json::json!({"items": [1, 2, 3]});
        let result = eval_with_ctx(
            "def sum = 0; for (def item : ctx._source.items) { sum = sum + item; }; return sum;",
            &mut ctx,
        );
        assert_eq!(result, serde_json::json!(6));
    }

    // ---- Array/List operation tests ----

    #[test]
    fn test_array_add() {
        let mut ctx = ScriptContext::new();
        let result = eval_with_ctx(
            "def arr = [1, 2, 3]; arr = arr.add(4); return arr.size()",
            &mut ctx,
        );
        assert_eq!(result, serde_json::json!(4));
    }

    #[test]
    fn test_array_remove() {
        let mut ctx = ScriptContext::new();
        let result = eval_with_ctx(
            "def arr = [1, 2, 3]; arr = arr.remove(0); return arr.size()",
            &mut ctx,
        );
        assert_eq!(result, serde_json::json!(2));
    }

    #[test]
    fn test_array_contains() {
        assert_eq!(eval("[1, 2, 3].contains(2)"), serde_json::json!(true));
        assert_eq!(eval("[1, 2, 3].contains(5)"), serde_json::json!(false));
    }

    #[test]
    fn test_array_indexof() {
        assert_eq!(eval("['a', 'b', 'c'].indexOf('b')"), serde_json::json!(1));
        assert_eq!(eval("['a', 'b', 'c'].indexOf('z')"), serde_json::json!(-1));
    }

    #[test]
    fn test_array_index_access() {
        assert_eq!(eval("[10, 20, 30][1]"), serde_json::json!(20));
    }

    // ---- Map operation tests ----

    #[test]
    fn test_map_contains_key() {
        let mut ctx = ScriptContext::new();
        ctx.source = serde_json::json!({"name": "test", "count": 5});
        let result = eval_with_ctx("ctx._source.containsKey('name')", &mut ctx);
        // ctx._source is a SourceField, not a Map directly, so we use doc approach
        // For map operations, use directly created maps
        assert_eq!(result, serde_json::json!(true));
    }

    #[test]
    fn test_map_key_set() {
        let mut ctx = ScriptContext::new();
        ctx.source = serde_json::json!({"a": 1, "b": 2});
        let result = eval_with_ctx("ctx._source.keySet().size()", &mut ctx);
        assert_eq!(result, serde_json::json!(2));
    }

    // ---- Regex tests ----

    #[test]
    fn test_regex_find() {
        assert_eq!(eval("'hello world' =~ /world/"), serde_json::json!(true));
        assert_eq!(eval("'hello world' =~ /xyz/"), serde_json::json!(false));
    }

    #[test]
    fn test_regex_match() {
        assert_eq!(eval("'hello' ==~ /hello/"), serde_json::json!(true));
        assert_eq!(eval("'hello world' ==~ /hello/"), serde_json::json!(false));
        assert_eq!(eval("'hello world' ==~ /hello.*/"), serde_json::json!(true));
    }

    #[test]
    fn test_regex_matcher() {
        assert_eq!(
            eval("/\\d+/.matcher('abc123').find()"),
            serde_json::json!(true)
        );
    }

    // ---- Type cast tests ----

    #[test]
    fn test_type_cast_int() {
        assert_eq!(eval("(int) 3.14"), serde_json::json!(3));
        assert_eq!(eval("(int) 42"), serde_json::json!(42));
    }

    #[test]
    fn test_type_cast_double() {
        assert_eq!(eval("(double) 42"), serde_json::json!(42.0));
    }

    // ---- Static call tests ----

    #[test]
    fn test_integer_parse_int() {
        assert_eq!(eval("Integer.parseInt('42')"), serde_json::json!(42));
    }

    #[test]
    fn test_string_value_of() {
        assert_eq!(eval("String.valueOf(42)"), serde_json::json!("42"));
    }

    // ---- Variable tests ----

    #[test]
    fn test_variable_declaration() {
        let mut ctx = ScriptContext::new();
        let result = eval_with_ctx("def x = 10; return x;", &mut ctx);
        assert_eq!(result, serde_json::json!(10));
    }

    #[test]
    fn test_variable_reassignment() {
        let mut ctx = ScriptContext::new();
        let result = eval_with_ctx("def x = 10; x = 20; return x;", &mut ctx);
        assert_eq!(result, serde_json::json!(20));
    }

    #[test]
    fn test_nested_source_field_read() {
        let mut ctx = ScriptContext::new();
        ctx.source = serde_json::json!({"task": {"status": "idle", "ownerId": null}});
        let result = eval_with_ctx("ctx._source.task.status", &mut ctx);
        assert_eq!(result, serde_json::json!("idle"));
    }

    #[test]
    fn test_nested_source_field_assign() {
        let mut ctx = ScriptContext::new();
        ctx.source = serde_json::json!({"task": {"status": "idle", "ownerId": null}});
        eval_with_ctx("ctx._source.task.status = 'claiming'", &mut ctx);
        assert_eq!(ctx.source["task"]["status"], serde_json::json!("claiming"));
        // ownerId should be unchanged
        assert_eq!(ctx.source["task"]["ownerId"], serde_json::Value::Null);
    }

    #[test]
    fn test_nested_assign_with_params() {
        let mut ctx = ScriptContext::new();
        ctx.source = serde_json::json!({"task": {"status": "idle", "ownerId": null}});
        ctx.params
            .insert("status".into(), serde_json::json!("claiming"));
        ctx.params
            .insert("ownerId".into(), serde_json::json!("node-1"));
        eval_with_ctx(
            "ctx._source.task.status = params.status; ctx._source.task.ownerId = params.ownerId",
            &mut ctx,
        );
        assert_eq!(ctx.source["task"]["status"], serde_json::json!("claiming"));
        assert_eq!(ctx.source["task"]["ownerId"], serde_json::json!("node-1"));
    }

    #[test]
    fn test_nested_assign_creates_intermediate() {
        let mut ctx = ScriptContext::new();
        ctx.source = serde_json::json!({});
        eval_with_ctx("ctx._source.task.status = 'new'", &mut ctx);
        assert_eq!(ctx.source["task"]["status"], serde_json::json!("new"));
    }

    // ---- Additional coverage tests ----

    // String methods: startsWith, endsWith, replace, trim, split, indexOf, charAt
    #[test]
    fn test_string_starts_with() {
        assert_eq!(
            eval("'hello world'.startsWith('hello')"),
            serde_json::json!(true)
        );
        assert_eq!(
            eval("'hello world'.startsWith('world')"),
            serde_json::json!(false)
        );
    }

    #[test]
    fn test_string_ends_with() {
        assert_eq!(
            eval("'hello world'.endsWith('world')"),
            serde_json::json!(true)
        );
        assert_eq!(
            eval("'hello world'.endsWith('hello')"),
            serde_json::json!(false)
        );
    }

    #[test]
    fn test_string_replace() {
        assert_eq!(
            eval("'hello world'.replace('world', 'rust')"),
            serde_json::json!("hello rust")
        );
    }

    #[test]
    fn test_string_trim() {
        assert_eq!(eval("'  hello  '.trim()"), serde_json::json!("hello"));
    }

    #[test]
    fn test_string_split() {
        assert_eq!(
            eval("'a,b,c'.split(',')"),
            serde_json::json!(["a", "b", "c"])
        );
    }

    #[test]
    fn test_string_indexof() {
        assert_eq!(eval("'hello'.indexOf('ll')"), serde_json::json!(2));
        assert_eq!(eval("'hello'.indexOf('xyz')"), serde_json::json!(-1));
    }

    #[test]
    fn test_string_charat() {
        assert_eq!(eval("'hello'.charAt(1)"), serde_json::json!("e"));
    }

    #[test]
    fn test_string_charat_out_of_bounds() {
        assert_eq!(eval("'hello'.charAt(99)"), serde_json::Value::Null);
    }

    #[test]
    fn test_string_substring_one_arg() {
        assert_eq!(eval("'hello'.substring(2)"), serde_json::json!("llo"));
    }

    // Math with float args
    #[test]
    fn test_math_max_float() {
        assert_eq!(eval("Math.max(3.5, 7.2)"), serde_json::json!(7.2));
    }

    #[test]
    fn test_math_min_float() {
        assert_eq!(eval("Math.min(3.5, 7.2)"), serde_json::json!(3.5));
    }

    #[test]
    fn test_math_abs_float() {
        assert_eq!(eval("Math.abs(-3.5)"), serde_json::json!(3.5));
    }

    // Modulo by zero
    #[test]
    fn test_modulo_by_zero() {
        let mut ctx = ScriptContext::new();
        let result = evaluate("5 % 0", &mut ctx);
        assert!(result.is_err());
    }

    // Mixed int/float arithmetic
    #[test]
    fn test_mixed_int_float_arithmetic() {
        assert_eq!(eval("3 + 1.5"), serde_json::json!(4.5));
        assert_eq!(eval("10 - 2.5"), serde_json::json!(7.5));
        assert_eq!(eval("4 * 2.5"), serde_json::json!(10.0));
        assert_eq!(eval("7 / 2.0"), serde_json::json!(3.5));
        assert_eq!(eval("7.0 % 3"), serde_json::json!(1.0));
    }

    // String + non-string concatenation (non-string + string path)
    #[test]
    fn test_string_concat_non_string_left() {
        assert_eq!(eval("42 + ' items'"), serde_json::json!("42 items"));
    }

    // Comparison operators with floats
    #[test]
    fn test_comparison_float() {
        assert_eq!(eval("3.5 > 2.1"), serde_json::json!(true));
        assert_eq!(eval("1.0 < 2.0"), serde_json::json!(true));
        assert_eq!(eval("2.5 >= 2.5"), serde_json::json!(true));
        assert_eq!(eval("3.0 <= 2.0"), serde_json::json!(false));
    }

    // Equality on different types
    #[test]
    fn test_equality_strings() {
        assert_eq!(eval("'abc' == 'abc'"), serde_json::json!(true));
        assert_eq!(eval("'abc' != 'def'"), serde_json::json!(true));
        assert_eq!(eval("'abc' == 'def'"), serde_json::json!(false));
    }

    // Logical operators with non-bool operands
    #[test]
    fn test_logical_with_non_bool() {
        assert_eq!(eval("1 && 1"), serde_json::json!(true));
        assert_eq!(eval("0 || 1"), serde_json::json!(true));
        assert_eq!(eval("0 && 1"), serde_json::json!(false));
        assert_eq!(eval("0 || 0"), serde_json::json!(false));
    }

    // Unary not on non-bool
    #[test]
    fn test_unary_not_non_bool() {
        assert_eq!(eval("!0"), serde_json::json!(true));
        assert_eq!(eval("!1"), serde_json::json!(false));
        assert_eq!(eval("!''"), serde_json::json!(true));
        assert_eq!(eval("!'hello'"), serde_json::json!(false));
    }

    // Ternary with non-bool conditions
    #[test]
    fn test_ternary_truthy_conditions() {
        assert_eq!(eval("1 ? 'yes' : 'no'"), serde_json::json!("yes"));
        assert_eq!(eval("0 ? 'yes' : 'no'"), serde_json::json!("no"));
        assert_eq!(eval("'' ? 'yes' : 'no'"), serde_json::json!("no"));
        assert_eq!(eval("'x' ? 'yes' : 'no'"), serde_json::json!("yes"));
    }

    // If without else
    #[test]
    fn test_if_without_else_true() {
        assert_eq!(eval("if (true) { return 'a'; }"), serde_json::json!("a"));
    }

    #[test]
    fn test_if_without_else_false() {
        assert_eq!(eval("if (false) { return 'a'; }"), serde_json::Value::Null);
    }

    // Variable: undefined var returns null
    #[test]
    fn test_undefined_var_is_null() {
        let mut ctx = ScriptContext::new();
        let result = eval_with_ctx("return undefined_var;", &mut ctx);
        assert_eq!(result, serde_json::Value::Null);
    }

    // _source as variable
    #[test]
    fn test_source_var_access() {
        let mut ctx = ScriptContext::new();
        ctx.source = serde_json::json!({"name": "test"});
        let result = eval_with_ctx("_source", &mut ctx);
        assert_eq!(result, serde_json::json!({"name": "test"}));
    }

    // Doc field missing returns null
    #[test]
    fn test_doc_missing_field() {
        let mut ctx = ScriptContext::new();
        let result = eval_with_ctx("doc['nonexistent'].value", &mut ctx);
        assert_eq!(result, serde_json::Value::Null);
    }

    // Params missing field returns null
    #[test]
    fn test_params_missing_field() {
        let mut ctx = ScriptContext::new();
        let result = eval_with_ctx("params.nonexistent", &mut ctx);
        assert_eq!(result, serde_json::Value::Null);
    }

    // Nested source missing field
    #[test]
    fn test_nested_source_missing_field() {
        let mut ctx = ScriptContext::new();
        ctx.source = serde_json::json!({"a": 1});
        let result = eval_with_ctx("ctx._source.b.c", &mut ctx);
        assert_eq!(result, serde_json::Value::Null);
    }

    // set_nested_value single level
    #[test]
    fn test_set_nested_value_single_level() {
        let mut source = serde_json::json!({"a": 1});
        set_nested_value(&mut source, "b", serde_json::json!(2));
        assert_eq!(source, serde_json::json!({"a": 1, "b": 2}));
    }

    // Array methods: isEmpty, get, join, length
    #[test]
    fn test_array_is_empty() {
        let mut ctx = ScriptContext::new();
        let result = eval_with_ctx("def arr = []; return arr.isEmpty()", &mut ctx);
        assert_eq!(result, serde_json::json!(true));
        let result2 = eval_with_ctx("def arr = [1]; return arr.isEmpty()", &mut ctx);
        assert_eq!(result2, serde_json::json!(false));
    }

    #[test]
    fn test_array_get() {
        let mut ctx = ScriptContext::new();
        let result = eval_with_ctx("def arr = [10, 20, 30]; return arr.get(1)", &mut ctx);
        assert_eq!(result, serde_json::json!(20));
    }

    #[test]
    fn test_array_get_out_of_bounds() {
        let mut ctx = ScriptContext::new();
        let result = eval_with_ctx("def arr = [10]; return arr.get(5)", &mut ctx);
        assert_eq!(result, serde_json::Value::Null);
    }

    #[test]
    fn test_array_join() {
        let mut ctx = ScriptContext::new();
        let result = eval_with_ctx("def arr = ['a', 'b', 'c']; return arr.join('-')", &mut ctx);
        assert_eq!(result, serde_json::json!("a-b-c"));
    }

    #[test]
    fn test_array_join_default_sep() {
        let mut ctx = ScriptContext::new();
        let result = eval_with_ctx("def arr = ['a', 'b', 'c']; return arr.join()", &mut ctx);
        assert_eq!(result, serde_json::json!("a,b,c"));
    }

    #[test]
    fn test_array_length_method() {
        assert_eq!(eval("[1, 2, 3].length()"), serde_json::json!(3));
    }

    // Map methods
    #[test]
    fn test_map_get() {
        let mut ctx = ScriptContext::new();
        ctx.source = serde_json::json!({"name": "alice"});
        let result = eval_with_ctx("ctx._source.get('name')", &mut ctx);
        assert_eq!(result, serde_json::json!("alice"));
    }

    #[test]
    fn test_map_get_missing() {
        let mut ctx = ScriptContext::new();
        ctx.source = serde_json::json!({"name": "alice"});
        let result = eval_with_ctx("ctx._source.get('missing')", &mut ctx);
        assert_eq!(result, serde_json::Value::Null);
    }

    #[test]
    fn test_map_put() {
        let mut ctx = ScriptContext::new();
        ctx.source = serde_json::json!({"a": 1});
        let result = eval_with_ctx("ctx._source.put('b', 2)", &mut ctx);
        let map: serde_json::Value = result;
        assert_eq!(map["b"], serde_json::json!(2));
    }

    #[test]
    fn test_map_remove() {
        let mut ctx = ScriptContext::new();
        ctx.source = serde_json::json!({"a": 1, "b": 2});
        let result = eval_with_ctx("ctx._source.remove('a')", &mut ctx);
        let map: serde_json::Value = result;
        assert_eq!(map.get("a"), None);
    }

    #[test]
    fn test_map_values() {
        let mut ctx = ScriptContext::new();
        ctx.source = serde_json::json!({"a": 1});
        let result = eval_with_ctx("ctx._source.values().size()", &mut ctx);
        assert_eq!(result, serde_json::json!(1));
    }

    #[test]
    fn test_map_size() {
        let mut ctx = ScriptContext::new();
        ctx.source = serde_json::json!({"a": 1, "b": 2});
        let result = eval_with_ctx("ctx._source.size()", &mut ctx);
        assert_eq!(result, serde_json::json!(2));
    }

    #[test]
    fn test_map_is_empty() {
        let mut ctx = ScriptContext::new();
        ctx.source = serde_json::json!({});
        let result = eval_with_ctx("ctx._source.isEmpty()", &mut ctx);
        assert_eq!(result, serde_json::json!(true));
    }

    #[test]
    fn test_map_contains_value() {
        let mut ctx = ScriptContext::new();
        ctx.source = serde_json::json!({"a": 1, "b": 2});
        let result = eval_with_ctx("ctx._source.containsValue(1)", &mut ctx);
        assert_eq!(result, serde_json::json!(true));
        let result2 = eval_with_ctx("ctx._source.containsValue(99)", &mut ctx);
        assert_eq!(result2, serde_json::json!(false));
    }

    // Map index access (bracket on map)
    #[test]
    fn test_map_index_access() {
        let mut ctx = ScriptContext::new();
        ctx.source = serde_json::json!({"name": "bob"});
        let result = eval_with_ctx("ctx._source['name']", &mut ctx);
        assert_eq!(result, serde_json::json!("bob"));
    }

    // Array index out of bounds returns null
    #[test]
    fn test_array_index_out_of_bounds() {
        assert_eq!(eval("[1, 2][99]"), serde_json::Value::Null);
    }

    // Type cast tests
    #[test]
    fn test_type_cast_int_from_string() {
        assert_eq!(eval("(int) '42'"), serde_json::json!(42));
    }

    #[test]
    fn test_type_cast_int_from_bool() {
        assert_eq!(eval("(int) true"), serde_json::json!(1));
        assert_eq!(eval("(int) false"), serde_json::json!(0));
    }

    #[test]
    fn test_type_cast_int_from_null() {
        assert_eq!(eval("(int) null"), serde_json::json!(0));
    }

    #[test]
    fn test_type_cast_double_from_string() {
        assert_eq!(eval("(double) '4.56'"), serde_json::json!(4.56));
    }

    #[test]
    fn test_type_cast_double_from_double() {
        assert_eq!(eval("(double) 2.5"), serde_json::json!(2.5));
    }

    #[test]
    fn test_type_cast_double_from_null() {
        assert_eq!(eval("(double) null"), serde_json::json!(0.0));
    }

    #[test]
    fn test_type_cast_string() {
        assert_eq!(eval("(String) 42"), serde_json::json!("42"));
        assert_eq!(eval("(String) true"), serde_json::json!("true"));
    }

    #[test]
    fn test_type_cast_boolean() {
        assert_eq!(eval("(boolean) 1"), serde_json::json!(true));
        assert_eq!(eval("(boolean) 0"), serde_json::json!(false));
        assert_eq!(eval("(boolean) 'hello'"), serde_json::json!(true));
        assert_eq!(eval("(boolean) ''"), serde_json::json!(false));
    }

    #[test]
    fn test_type_cast_unknown_type() {
        // Unknown type cast returns value unchanged
        assert_eq!(eval("(UnknownType) 42"), serde_json::json!(42));
    }

    #[test]
    fn test_type_cast_long() {
        assert_eq!(eval("(long) 3.9"), serde_json::json!(3));
    }

    #[test]
    fn test_type_cast_float() {
        assert_eq!(eval("(float) 42"), serde_json::json!(42.0));
    }

    // Static calls
    #[test]
    fn test_long_parse_long() {
        assert_eq!(eval("Long.parseLong('100')"), serde_json::json!(100));
    }

    #[test]
    fn test_integer_parse_int_passthrough() {
        assert_eq!(eval("Integer.parseInt(42)"), serde_json::json!(42));
    }

    #[test]
    fn test_double_parse_double() {
        assert_eq!(eval("Double.parseDouble('4.56')"), serde_json::json!(4.56));
    }

    #[test]
    fn test_float_parse_float() {
        assert_eq!(eval("Float.parseFloat('2.5')"), serde_json::json!(2.5));
    }

    #[test]
    fn test_double_parse_double_passthrough() {
        assert_eq!(eval("Double.parseDouble(2.5)"), serde_json::json!(2.5));
    }

    #[test]
    fn test_integer_value_of_string() {
        assert_eq!(eval("Integer.valueOf('99')"), serde_json::json!(99));
    }

    #[test]
    fn test_integer_value_of_int() {
        assert_eq!(eval("Integer.valueOf(99)"), serde_json::json!(99));
    }

    #[test]
    fn test_long_value_of() {
        assert_eq!(eval("Long.valueOf('55')"), serde_json::json!(55));
    }

    #[test]
    fn test_boolean_parse_boolean_true() {
        assert_eq!(
            eval("Boolean.parseBoolean('true')"),
            serde_json::json!(true)
        );
        assert_eq!(
            eval("Boolean.parseBoolean('TRUE')"),
            serde_json::json!(true)
        );
    }

    #[test]
    fn test_boolean_parse_boolean_false() {
        assert_eq!(
            eval("Boolean.parseBoolean('false')"),
            serde_json::json!(false)
        );
        assert_eq!(
            eval("Boolean.parseBoolean('xyz')"),
            serde_json::json!(false)
        );
    }

    #[test]
    fn test_boolean_value_of_bool() {
        assert_eq!(eval("Boolean.valueOf(true)"), serde_json::json!(true));
        assert_eq!(eval("Boolean.valueOf(false)"), serde_json::json!(false));
    }

    #[test]
    fn test_boolean_value_of_default() {
        assert_eq!(eval("Boolean.valueOf(42)"), serde_json::json!(false));
    }

    #[test]
    fn test_string_value_of_no_args() {
        assert_eq!(eval("String.valueOf()"), serde_json::json!(""));
    }

    // Static call error cases
    #[test]
    fn test_unknown_static_method() {
        let mut ctx = ScriptContext::new();
        let result = evaluate("Unknown.method()", &mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_integer_parse_int_bad_arg() {
        let mut ctx = ScriptContext::new();
        let result = evaluate("Integer.parseInt(true)", &mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_double_parse_double_bad_arg() {
        let mut ctx = ScriptContext::new();
        let result = evaluate("Double.parseDouble(true)", &mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_integer_value_of_bad_arg() {
        let mut ctx = ScriptContext::new();
        let result = evaluate("Integer.valueOf(true)", &mut ctx);
        assert!(result.is_err());
    }

    // Unknown math function
    #[test]
    fn test_unknown_math_function() {
        let mut ctx = ScriptContext::new();
        let result = evaluate("Math.nonexistent(1.0)", &mut ctx);
        assert!(result.is_err());
    }

    // Math functions (26 functions + 2 constants)
    #[test]
    fn test_math_trig_functions() {
        let mut ctx = ScriptContext::new();
        // sin/cos/tan
        let r = evaluate("Math.sin(0.0)", &mut ctx).unwrap();
        assert_eq!(r.as_f64().unwrap(), 0.0);
        let r = evaluate("Math.cos(0.0)", &mut ctx).unwrap();
        assert_eq!(r.as_f64().unwrap(), 1.0);
        let r = evaluate("Math.tan(0.0)", &mut ctx).unwrap();
        assert_eq!(r.as_f64().unwrap(), 0.0);
        // asin/acos/atan
        let r = evaluate("Math.asin(0.0)", &mut ctx).unwrap();
        assert_eq!(r.as_f64().unwrap(), 0.0);
        let r = evaluate("Math.acos(1.0)", &mut ctx).unwrap();
        assert_eq!(r.as_f64().unwrap(), 0.0);
        let r = evaluate("Math.atan(0.0)", &mut ctx).unwrap();
        assert_eq!(r.as_f64().unwrap(), 0.0);
        // sinh/cosh/tanh
        let r = evaluate("Math.sinh(0.0)", &mut ctx).unwrap();
        assert_eq!(r.as_f64().unwrap(), 0.0);
        let r = evaluate("Math.cosh(0.0)", &mut ctx).unwrap();
        assert_eq!(r.as_f64().unwrap(), 1.0);
        let r = evaluate("Math.tanh(0.0)", &mut ctx).unwrap();
        assert_eq!(r.as_f64().unwrap(), 0.0);
    }

    #[test]
    fn test_math_log_exp_functions() {
        let mut ctx = ScriptContext::new();
        let r = evaluate("Math.log(1.0)", &mut ctx).unwrap();
        assert_eq!(r.as_f64().unwrap(), 0.0); // ln(1) = 0
        let r = evaluate("Math.log10(100.0)", &mut ctx).unwrap();
        assert_eq!(r.as_f64().unwrap(), 2.0);
        let r = evaluate("Math.log2(8.0)", &mut ctx).unwrap();
        assert_eq!(r.as_f64().unwrap(), 3.0);
        let r = evaluate("Math.exp(0.0)", &mut ctx).unwrap();
        assert_eq!(r.as_f64().unwrap(), 1.0);
    }

    #[test]
    fn test_math_power_root_functions() {
        let mut ctx = ScriptContext::new();
        let r = evaluate("Math.pow(2.0, 10.0)", &mut ctx).unwrap();
        assert_eq!(r.as_f64().unwrap(), 1024.0);
        let r = evaluate("Math.sqrt(9.0)", &mut ctx).unwrap();
        assert_eq!(r.as_f64().unwrap(), 3.0);
        let r = evaluate("Math.cbrt(27.0)", &mut ctx).unwrap();
        assert_eq!(r.as_f64().unwrap(), 3.0);
    }

    #[test]
    fn test_math_rounding_functions() {
        let mut ctx = ScriptContext::new();
        let r = evaluate("Math.ceil(2.3)", &mut ctx).unwrap();
        assert_eq!(r.as_f64().unwrap(), 3.0);
        let r = evaluate("Math.floor(2.7)", &mut ctx).unwrap();
        assert_eq!(r.as_f64().unwrap(), 2.0);
        let r = evaluate("Math.round(2.5)", &mut ctx).unwrap();
        assert_eq!(r.as_f64().unwrap(), 3.0);
        let r = evaluate("Math.rint(2.5)", &mut ctx).unwrap();
        assert_eq!(r.as_f64().unwrap(), 3.0);
    }

    #[test]
    fn test_math_misc_functions() {
        let mut ctx = ScriptContext::new();
        let r = evaluate("Math.signum(-5.0)", &mut ctx).unwrap();
        assert_eq!(r.as_f64().unwrap(), -1.0);
        let r = evaluate("Math.signum(5.0)", &mut ctx).unwrap();
        assert_eq!(r.as_f64().unwrap(), 1.0);
        let r = evaluate("Math.signum(0.0)", &mut ctx).unwrap();
        assert_eq!(r.as_f64().unwrap(), 0.0);
        let r = evaluate("Math.toRadians(180.0)", &mut ctx).unwrap();
        assert!((r.as_f64().unwrap() - std::f64::consts::PI).abs() < 1e-10);
        let r = evaluate("Math.toDegrees(3.141592653589793)", &mut ctx).unwrap();
        assert!((r.as_f64().unwrap() - 180.0).abs() < 1e-10);
        let r = evaluate("Math.atan2(1.0, 1.0)", &mut ctx).unwrap();
        assert!((r.as_f64().unwrap() - std::f64::consts::FRAC_PI_4).abs() < 1e-10);
        // random returns a value in [0, 1)
        let r = evaluate("Math.random()", &mut ctx).unwrap();
        let v = r.as_f64().unwrap();
        assert!((0.0..1.0).contains(&v));
    }

    #[test]
    fn test_math_constants() {
        let mut ctx = ScriptContext::new();
        let r = evaluate("Math.PI", &mut ctx).unwrap();
        assert!((r.as_f64().unwrap() - std::f64::consts::PI).abs() < 1e-10);
        let r = evaluate("Math.E", &mut ctx).unwrap();
        assert!((r.as_f64().unwrap() - std::f64::consts::E).abs() < 1e-10);
    }

    #[test]
    fn test_math_in_expression() {
        let mut ctx = ScriptContext::new();
        // Math.log(1 + x) * _score — typical script_score pattern
        ctx.locals
            .insert("_score".to_string(), crate::types::ScriptValue::Float(2.0));
        let r = evaluate("Math.log(1 + 99) * _score", &mut ctx).unwrap();
        let v = r.as_f64().unwrap();
        assert!((v - 2.0 * (100.0_f64).ln()).abs() < 1e-10);
    }

    // ── Date/Time methods ──

    #[test]
    fn test_date_getters_from_string() {
        let mut ctx = ScriptContext::new();
        ctx.doc.insert(
            "date".to_string(),
            serde_json::json!("2025-03-15T10:30:45Z"),
        );
        let r = evaluate("doc['date'].value.getYear()", &mut ctx).unwrap();
        assert_eq!(r, serde_json::json!(2025));
        let r = evaluate("doc['date'].value.getMonthValue()", &mut ctx).unwrap();
        assert_eq!(r, serde_json::json!(3));
        let r = evaluate("doc['date'].value.getDayOfMonth()", &mut ctx).unwrap();
        assert_eq!(r, serde_json::json!(15));
        let r = evaluate("doc['date'].value.getHour()", &mut ctx).unwrap();
        assert_eq!(r, serde_json::json!(10));
        let r = evaluate("doc['date'].value.getMinute()", &mut ctx).unwrap();
        assert_eq!(r, serde_json::json!(30));
        let r = evaluate("doc['date'].value.getSecond()", &mut ctx).unwrap();
        assert_eq!(r, serde_json::json!(45));
    }

    #[test]
    fn test_date_epoch_millis() {
        let mut ctx = ScriptContext::new();
        // 2025-01-01T00:00:00Z = 1735689600000
        ctx.doc
            .insert("ts".to_string(), serde_json::json!(1735689600000_i64));
        let r = evaluate("doc['ts'].value.getYear()", &mut ctx).unwrap();
        assert_eq!(r, serde_json::json!(2025));
        let r = evaluate("doc['ts'].value.getMonthValue()", &mut ctx).unwrap();
        assert_eq!(r, serde_json::json!(1));
        let r = evaluate("doc['ts'].value.getDayOfMonth()", &mut ctx).unwrap();
        assert_eq!(r, serde_json::json!(1));
        let r = evaluate("doc['ts'].value.toEpochMilli()", &mut ctx).unwrap();
        assert_eq!(r, serde_json::json!(1735689600000_i64));
    }

    // ── Panic-safety on hostile event data (round-5 review fixes) ──
    // These previously panicked (unwinding past the script filter and aborting
    // the filter worker = remote DoS); they must now return Err, never panic.

    #[test]
    fn substring_multibyte_does_not_panic() {
        // Byte index 2 falls inside the 3-byte '€' — a raw slice would panic.
        let mut ctx = ScriptContext::new();
        ctx.doc.insert("f".to_string(), serde_json::json!("€abc"));
        let r = evaluate("doc['f'].value.substring(0, 2)", &mut ctx);
        assert!(
            r.is_err(),
            "multibyte substring should error, not panic: {r:?}"
        );
    }

    #[test]
    fn substring_reversed_args_does_not_panic() {
        let mut ctx = ScriptContext::new();
        ctx.doc.insert("f".to_string(), serde_json::json!("hello"));
        let r = evaluate("doc['f'].value.substring(3, 1)", &mut ctx);
        assert!(
            r.is_err(),
            "start>end substring should error, not panic: {r:?}"
        );
    }

    #[test]
    fn substring_ascii_still_works() {
        let mut ctx = ScriptContext::new();
        ctx.doc.insert("f".to_string(), serde_json::json!("hello"));
        let r = evaluate("doc['f'].value.substring(1, 3)", &mut ctx).unwrap();
        assert_eq!(r, serde_json::json!("el"));
    }

    #[test]
    fn date_method_multibyte_string_does_not_panic() {
        // Non-ASCII value ≥10 bytes: byte slices like s[0..4] would split a char.
        let mut ctx = ScriptContext::new();
        ctx.doc
            .insert("d".to_string(), serde_json::json!("€€€€abcdef"));
        let r = evaluate("doc['d'].value.getYear()", &mut ctx);
        assert!(r.is_err(), "multibyte date should error, not panic: {r:?}");
    }

    #[test]
    fn get_day_of_year_invalid_month_does_not_panic() {
        // month=99 → days_in_months[..98] would be out of bounds.
        let mut ctx = ScriptContext::new();
        ctx.doc
            .insert("d".to_string(), serde_json::json!("2025-99-15T00:00:00Z"));
        let r = evaluate("doc['d'].value.getDayOfYear()", &mut ctx);
        assert!(r.is_err(), "invalid month should error, not panic: {r:?}");
    }

    #[test]
    fn get_day_of_year_zero_month_does_not_panic() {
        // month=00 → (month as usize - 1) would underflow.
        let mut ctx = ScriptContext::new();
        ctx.doc
            .insert("d".to_string(), serde_json::json!("2025-00-15T00:00:00Z"));
        let r = evaluate("doc['d'].value.getDayOfYear()", &mut ctx);
        assert!(r.is_err(), "zero month should error, not panic: {r:?}");
    }

    #[test]
    fn get_day_of_year_valid_still_works() {
        let mut ctx = ScriptContext::new();
        ctx.doc
            .insert("d".to_string(), serde_json::json!("2025-03-15T00:00:00Z"));
        let r = evaluate("doc['d'].value.getDayOfYear()", &mut ctx).unwrap();
        assert_eq!(r, serde_json::json!(74)); // Jan(31)+Feb(28)+15
    }

    #[test]
    fn test_date_arithmetic() {
        let mut ctx = ScriptContext::new();
        ctx.doc.insert(
            "ts".to_string(),
            serde_json::json!(1735689600000_i64), // 2025-01-01
        );
        let r = evaluate("doc['ts'].value.plusDays(1)", &mut ctx).unwrap();
        assert_eq!(r, serde_json::json!(1735689600000_i64 + 86_400_000));
        let r = evaluate("doc['ts'].value.minusHours(2)", &mut ctx).unwrap();
        assert_eq!(r, serde_json::json!(1735689600000_i64 - 7_200_000));
    }

    // Regex matcher .matches()
    #[test]
    fn test_regex_matcher_matches() {
        assert_eq!(
            eval("/^hello$/.matcher('hello').matches()"),
            serde_json::json!(true)
        );
        assert_eq!(
            eval("/^hello$/.matcher('hello world').matches()"),
            serde_json::json!(false)
        );
    }

    // ForEach on non-array errors
    #[test]
    fn test_foreach_non_array_error() {
        let mut ctx = ScriptContext::new();
        ctx.source = serde_json::json!({"items": "not_an_array"});
        let result = evaluate(
            "for (def item : ctx._source.items) { return item; }",
            &mut ctx,
        );
        assert!(result.is_err());
    }

    // .value method is no-op
    #[test]
    fn test_value_method_noop() {
        let mut ctx = ScriptContext::new();
        ctx.doc.insert("x".into(), serde_json::json!(42));
        let result = eval_with_ctx("doc['x'].value", &mut ctx);
        assert_eq!(result, serde_json::json!(42));
    }

    // Unknown method error
    #[test]
    fn test_unknown_method_error() {
        let mut ctx = ScriptContext::new();
        let result = evaluate("'hello'.unknownMethod()", &mut ctx);
        assert!(result.is_err());
    }

    // Unary negation error on non-number
    #[test]
    fn test_unary_neg_non_number_error() {
        let mut ctx = ScriptContext::new();
        let result = evaluate("-'hello'", &mut ctx);
        assert!(result.is_err());
    }

    // Arithmetic on non-numeric types errors
    #[test]
    fn test_arithmetic_non_numeric_error() {
        let mut ctx = ScriptContext::new();
        let result = evaluate("true + false", &mut ctx);
        assert!(result.is_err());
    }

    // Array literal
    #[test]
    fn test_array_literal() {
        assert_eq!(eval("[1, 2, 3]"), serde_json::json!([1, 2, 3]));
        assert_eq!(eval("[]"), serde_json::json!([]));
    }

    // Return statement
    #[test]
    fn test_return_statement() {
        assert_eq!(eval("return 42;"), serde_json::json!(42));
        assert_eq!(
            eval("return 'early'; return 'late';"),
            serde_json::json!("early")
        );
    }

    // VarDecl expression
    #[test]
    fn test_var_decl_expression() {
        let mut ctx = ScriptContext::new();
        let result = eval_with_ctx("def x = 5; def y = 10; return x + y;", &mut ctx);
        assert_eq!(result, serde_json::json!(15));
    }

    // Boolean literal
    #[test]
    fn test_bool_literal() {
        assert_eq!(eval("true"), serde_json::json!(true));
        assert_eq!(eval("false"), serde_json::json!(false));
    }

    // String method error cases
    #[test]
    fn test_contains_non_string_arg_error() {
        let mut ctx = ScriptContext::new();
        let result = evaluate("'hello'.contains(42)", &mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_starts_with_non_string_arg_error() {
        let mut ctx = ScriptContext::new();
        let result = evaluate("'hello'.startsWith(42)", &mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_ends_with_non_string_arg_error() {
        let mut ctx = ScriptContext::new();
        let result = evaluate("'hello'.endsWith(42)", &mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_replace_non_string_arg_error() {
        let mut ctx = ScriptContext::new();
        let result = evaluate("'hello'.replace(42, 'x')", &mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_split_non_string_arg_error() {
        let mut ctx = ScriptContext::new();
        let result = evaluate("'hello'.split(42)", &mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_indexof_non_string_arg_error() {
        let mut ctx = ScriptContext::new();
        let result = evaluate("'hello'.indexOf(42)", &mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_charat_non_int_arg_error() {
        let mut ctx = ScriptContext::new();
        let result = evaluate("'hello'.charAt('x')", &mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_substring_non_int_arg_error() {
        let mut ctx = ScriptContext::new();
        let result = evaluate("'hello'.substring('x')", &mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_substring_non_int_second_arg_error() {
        let mut ctx = ScriptContext::new();
        let result = evaluate("'hello'.substring(0, 'x')", &mut ctx);
        assert!(result.is_err());
    }

    // Array method error cases
    #[test]
    fn test_array_contains_no_arg_error() {
        let mut ctx = ScriptContext::new();
        let result = evaluate("[1,2].contains()", &mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_array_indexof_no_arg_error() {
        let mut ctx = ScriptContext::new();
        let result = evaluate("[1,2].indexOf()", &mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_array_add_no_arg_error() {
        let mut ctx = ScriptContext::new();
        let result = evaluate("[1,2].add()", &mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_array_remove_non_int_error() {
        let mut ctx = ScriptContext::new();
        let result = evaluate("[1,2].remove('x')", &mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_array_get_non_int_error() {
        let mut ctx = ScriptContext::new();
        let result = evaluate("[1,2].get('x')", &mut ctx);
        assert!(result.is_err());
    }

    // Map method error cases
    #[test]
    fn test_map_contains_key_non_string_error() {
        let mut ctx = ScriptContext::new();
        ctx.source = serde_json::json!({"a": 1});
        let result = evaluate("ctx._source.containsKey(42)", &mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_map_get_non_string_error() {
        let mut ctx = ScriptContext::new();
        ctx.source = serde_json::json!({"a": 1});
        let result = evaluate("ctx._source.get(42)", &mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_map_put_bad_args_error() {
        let mut ctx = ScriptContext::new();
        ctx.source = serde_json::json!({"a": 1});
        let result = evaluate("ctx._source.put(42, 'x')", &mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_map_remove_non_string_error() {
        let mut ctx = ScriptContext::new();
        ctx.source = serde_json::json!({"a": 1});
        let result = evaluate("ctx._source.remove(42)", &mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_map_contains_value_no_arg_error() {
        let mut ctx = ScriptContext::new();
        ctx.source = serde_json::json!({"a": 1});
        let result = evaluate("ctx._source.containsValue()", &mut ctx);
        assert!(result.is_err());
    }

    // Regex error cases
    #[test]
    fn test_regex_find_non_string_error() {
        let mut ctx = ScriptContext::new();
        let result = evaluate("42 =~ /pattern/", &mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_regex_match_non_string_error() {
        let mut ctx = ScriptContext::new();
        let result = evaluate("42 ==~ /pattern/", &mut ctx);
        assert!(result.is_err());
    }

    // Math.abs non-number error
    #[test]
    fn test_math_abs_non_number_error() {
        let mut ctx = ScriptContext::new();
        let result = evaluate("Math.abs('hello')", &mut ctx);
        assert!(result.is_err());
    }

    // ScriptContext default
    #[test]
    fn test_script_context_default() {
        let ctx = ScriptContext::default();
        assert!(ctx.doc.is_empty());
        assert!(ctx.params.is_empty());
        assert!(ctx.locals.is_empty());
    }

    // Integer division
    #[test]
    fn test_integer_division() {
        assert_eq!(eval("10 / 2"), serde_json::json!(5));
        assert_eq!(eval("7 / 2"), serde_json::json!(3));
    }

    // Integer modulo
    #[test]
    fn test_integer_modulo() {
        assert_eq!(eval("10 % 3"), serde_json::json!(1));
        assert_eq!(eval("9 % 3"), serde_json::json!(0));
    }

    // For loop with return inside body
    #[test]
    fn test_for_loop_early_return() {
        let mut ctx = ScriptContext::new();
        let result = eval_with_ctx(
            "def sum = 0; for (int i = 1; i < 4; i = i + 1) { sum = sum + i; }; return sum;",
            &mut ctx,
        );
        assert_eq!(result, serde_json::json!(6));
    }

    // ForEach collecting values
    #[test]
    fn test_foreach_collect() {
        let mut ctx = ScriptContext::new();
        ctx.source = serde_json::json!({"items": [10, 20, 30]});
        let result = eval_with_ctx(
            "def total = 0; for (def item : ctx._source.items) { total = total + item; }; return total;",
            &mut ctx,
        );
        assert_eq!(result, serde_json::json!(60));
    }

    // Type cast error: bad string to int
    #[test]
    fn test_type_cast_bad_string_to_int() {
        let mut ctx = ScriptContext::new();
        let result = evaluate("(int) 'not_a_number'", &mut ctx);
        assert!(result.is_err());
    }

    // Type cast error: bad string to double
    #[test]
    fn test_type_cast_bad_string_to_double() {
        let mut ctx = ScriptContext::new();
        let result = evaluate("(double) 'not_a_number'", &mut ctx);
        assert!(result.is_err());
    }

    // Integer.parseInt bad string
    #[test]
    fn test_integer_parse_int_bad_string() {
        let mut ctx = ScriptContext::new();
        let result = evaluate("Integer.parseInt('abc')", &mut ctx);
        assert!(result.is_err());
    }

    // Double.parseDouble bad string
    #[test]
    fn test_double_parse_double_bad_string() {
        let mut ctx = ScriptContext::new();
        let result = evaluate("Double.parseDouble('abc')", &mut ctx);
        assert!(result.is_err());
    }

    // Integer.valueOf bad string
    #[test]
    fn test_integer_value_of_bad_string() {
        let mut ctx = ScriptContext::new();
        let result = evaluate("Integer.valueOf('abc')", &mut ctx);
        assert!(result.is_err());
    }

    // Regex matcher on non-string arg
    #[test]
    fn test_regex_matcher_non_string_arg_error() {
        let mut ctx = ScriptContext::new();
        let result = evaluate("/\\d+/.matcher(42)", &mut ctx);
        assert!(result.is_err());
    }

    // Assignment to source and read back
    #[test]
    fn test_assign_and_read_source() {
        let mut ctx = ScriptContext::new();
        ctx.source = serde_json::json!({"count": 0});
        eval_with_ctx("ctx._source.count = 42", &mut ctx);
        let result = eval_with_ctx("ctx._source.count", &mut ctx);
        assert_eq!(result, serde_json::json!(42));
    }

    #[test]
    fn test_source_compound_assign() {
        let mut ctx = ScriptContext::new();
        ctx.source = serde_json::json!({"count": 2});
        let result = eval_with_ctx("ctx._source.count += 3", &mut ctx);
        assert_eq!(result, serde_json::json!(5));
        assert_eq!(ctx.source["count"], serde_json::json!(5));
    }

    #[test]
    fn test_var_compound_assign() {
        let mut ctx = ScriptContext::new();
        let result = eval_with_ctx("def x = 4; x *= 3; return x;", &mut ctx);
        assert_eq!(result, serde_json::json!(12));
    }

    // Complex: conditional assignment
    #[test]
    fn test_conditional_assignment() {
        let mut ctx = ScriptContext::new();
        ctx.source = serde_json::json!({"status": "pending"});
        ctx.params
            .insert("new_status".into(), serde_json::json!("done"));
        eval_with_ctx(
            "if (ctx._source.status == 'pending') { ctx._source.status = params.new_status; }",
            &mut ctx,
        );
        assert_eq!(ctx.source["status"], serde_json::json!("done"));
    }

    // Nested ternary
    #[test]
    fn test_nested_ternary() {
        assert_eq!(
            eval("true ? (false ? 'a' : 'b') : 'c'"),
            serde_json::json!("b")
        );
    }

    // Multiple statements returning last value
    #[test]
    fn test_multiple_stmts_last_value() {
        let mut ctx = ScriptContext::new();
        let result = eval_with_ctx("def x = 1; def y = 2; x + y", &mut ctx);
        assert_eq!(result, serde_json::json!(3));
    }

    // Source with array field
    #[test]
    fn test_source_array_field() {
        let mut ctx = ScriptContext::new();
        ctx.source = serde_json::json!({"tags": ["a", "b", "c"]});
        let result = eval_with_ctx("ctx._source.tags", &mut ctx);
        assert_eq!(result, serde_json::json!(["a", "b", "c"]));
    }

    // ── Vector similarity built-ins ──

    fn ctx_with_vector(field: &str, vec: serde_json::Value) -> ScriptContext {
        let mut ctx = ScriptContext::new();
        ctx.doc.insert(field.to_string(), vec.clone());
        ctx.source = serde_json::json!({ field: vec });
        ctx
    }

    fn approx(a: f64, b: f64) {
        assert!((a - b).abs() < 1e-9, "expected {b}, got {a}");
    }

    fn as_f64(v: &serde_json::Value) -> f64 {
        v.as_f64().expect("number")
    }

    #[test]
    fn test_dot_product_basic() {
        let mut ctx = ctx_with_vector("v", serde_json::json!([1.0, 2.0, 3.0]));
        ctx.params
            .insert("q".into(), serde_json::json!([4.0, 5.0, 6.0]));
        let r = eval_with_ctx("dotProduct(params.q, 'v')", &mut ctx);
        approx(as_f64(&r), 32.0);
    }

    #[test]
    fn test_dot_product_zero_vector() {
        let mut ctx = ctx_with_vector("v", serde_json::json!([0.0, 0.0, 0.0]));
        ctx.params
            .insert("q".into(), serde_json::json!([1.0, 2.0, 3.0]));
        let r = eval_with_ctx("dotProduct(params.q, 'v')", &mut ctx);
        approx(as_f64(&r), 0.0);
    }

    #[test]
    fn test_dot_product_negatives() {
        let mut ctx = ctx_with_vector("v", serde_json::json!([-1.0, -2.0, -3.0]));
        ctx.params
            .insert("q".into(), serde_json::json!([1.0, 2.0, 3.0]));
        let r = eval_with_ctx("dotProduct(params.q, 'v')", &mut ctx);
        approx(as_f64(&r), -14.0);
    }

    #[test]
    fn test_dot_product_with_score_offset() {
        let mut ctx = ctx_with_vector("v", serde_json::json!([1.0, 1.0, 1.0]));
        ctx.params
            .insert("q".into(), serde_json::json!([2.0, 2.0, 2.0]));
        let r = eval_with_ctx("dotProduct(params.q, 'v') + 1.0", &mut ctx);
        approx(as_f64(&r), 7.0);
    }

    #[test]
    fn test_dot_product_int_vector() {
        let mut ctx = ctx_with_vector("v", serde_json::json!([1, 2, 3]));
        ctx.params.insert("q".into(), serde_json::json!([1, 1, 1]));
        let r = eval_with_ctx("dotProduct(params.q, 'v')", &mut ctx);
        approx(as_f64(&r), 6.0);
    }

    #[test]
    fn test_dot_product_dimension_mismatch() {
        let mut ctx = ctx_with_vector("v", serde_json::json!([1.0, 2.0]));
        ctx.params
            .insert("q".into(), serde_json::json!([1.0, 2.0, 3.0]));
        let err = evaluate("dotProduct(params.q, 'v')", &mut ctx);
        assert!(err.is_err(), "expected dimension mismatch error");
    }

    #[test]
    fn test_cosine_similarity_identical() {
        let mut ctx = ctx_with_vector("v", serde_json::json!([1.0, 2.0, 3.0]));
        ctx.params
            .insert("q".into(), serde_json::json!([1.0, 2.0, 3.0]));
        let r = eval_with_ctx("cosineSimilarity(params.q, 'v')", &mut ctx);
        approx(as_f64(&r), 1.0);
    }

    #[test]
    fn test_cosine_similarity_orthogonal() {
        let mut ctx = ctx_with_vector("v", serde_json::json!([1.0, 0.0]));
        ctx.params.insert("q".into(), serde_json::json!([0.0, 1.0]));
        let r = eval_with_ctx("cosineSimilarity(params.q, 'v')", &mut ctx);
        approx(as_f64(&r), 0.0);
    }

    #[test]
    fn test_cosine_similarity_opposite() {
        let mut ctx = ctx_with_vector("v", serde_json::json!([1.0, 2.0, 3.0]));
        ctx.params
            .insert("q".into(), serde_json::json!([-1.0, -2.0, -3.0]));
        let r = eval_with_ctx("cosineSimilarity(params.q, 'v')", &mut ctx);
        approx(as_f64(&r), -1.0);
    }

    #[test]
    fn test_cosine_similarity_scaled_equiv() {
        let mut ctx = ctx_with_vector("v", serde_json::json!([2.0, 4.0, 6.0]));
        ctx.params
            .insert("q".into(), serde_json::json!([1.0, 2.0, 3.0]));
        let r = eval_with_ctx("cosineSimilarity(params.q, 'v')", &mut ctx);
        approx(as_f64(&r), 1.0);
    }

    #[test]
    fn test_cosine_similarity_zero_norm() {
        let mut ctx = ctx_with_vector("v", serde_json::json!([0.0, 0.0]));
        ctx.params.insert("q".into(), serde_json::json!([1.0, 2.0]));
        let r = eval_with_ctx("cosineSimilarity(params.q, 'v')", &mut ctx);
        approx(as_f64(&r), 0.0);
    }

    #[test]
    fn test_cosine_similarity_known_value() {
        // [1,0,0] vs [1,1,0] = 1 / sqrt(2)
        let mut ctx = ctx_with_vector("v", serde_json::json!([1.0, 1.0, 0.0]));
        ctx.params
            .insert("q".into(), serde_json::json!([1.0, 0.0, 0.0]));
        let r = eval_with_ctx("cosineSimilarity(params.q, 'v')", &mut ctx);
        approx(as_f64(&r), 1.0 / 2.0_f64.sqrt());
    }

    #[test]
    fn test_l1norm() {
        let mut ctx = ctx_with_vector("v", serde_json::json!([1.0, 2.0, 3.0]));
        ctx.params
            .insert("q".into(), serde_json::json!([4.0, 6.0, 8.0]));
        let r = eval_with_ctx("l1norm(params.q, 'v')", &mut ctx);
        approx(as_f64(&r), 12.0);
    }

    #[test]
    fn test_l1norm_zero() {
        let mut ctx = ctx_with_vector("v", serde_json::json!([1.0, 2.0, 3.0]));
        ctx.params
            .insert("q".into(), serde_json::json!([1.0, 2.0, 3.0]));
        let r = eval_with_ctx("l1norm(params.q, 'v')", &mut ctx);
        approx(as_f64(&r), 0.0);
    }

    #[test]
    fn test_l2norm() {
        let mut ctx = ctx_with_vector("v", serde_json::json!([0.0, 0.0]));
        ctx.params.insert("q".into(), serde_json::json!([3.0, 4.0]));
        let r = eval_with_ctx("l2norm(params.q, 'v')", &mut ctx);
        approx(as_f64(&r), 5.0);
    }

    #[test]
    fn test_hamming_distance() {
        let mut ctx = ctx_with_vector("v", serde_json::json!([1.0, 2.0, 3.0, 4.0]));
        ctx.params
            .insert("q".into(), serde_json::json!([1.0, 0.0, 3.0, 0.0]));
        let r = eval_with_ctx("hamming(params.q, 'v')", &mut ctx);
        approx(as_f64(&r), 2.0);
    }

    #[test]
    fn test_vector_function_via_source() {
        // Field present only in _source (no doc-values map) should still resolve.
        let mut ctx = ScriptContext::new();
        ctx.source = serde_json::json!({"v": [1.0, 1.0, 1.0]});
        ctx.params
            .insert("q".into(), serde_json::json!([2.0, 3.0, 4.0]));
        let r = eval_with_ctx("dotProduct(params.q, 'v')", &mut ctx);
        approx(as_f64(&r), 9.0);
    }

    #[test]
    fn test_vector_function_literal_arg() {
        // Allow passing an explicit array as the second argument.
        let mut ctx = ScriptContext::new();
        let r = eval_with_ctx("dotProduct([1.0, 2.0], [3.0, 4.0])", &mut ctx);
        approx(as_f64(&r), 11.0);
    }

    #[test]
    fn test_unknown_function_errors() {
        let mut ctx = ScriptContext::new();
        let err = evaluate("frobnicate(1, 2)", &mut ctx);
        assert!(err.is_err());
    }

    #[test]
    fn test_vector_function_wrong_arity() {
        let mut ctx = ctx_with_vector("v", serde_json::json!([1.0]));
        let err = evaluate("dotProduct(params.q)", &mut ctx);
        assert!(err.is_err());
    }

    #[test]
    fn test_cosine_in_expression() {
        // Common kNN pattern: "cosineSimilarity(...) + 1.0"
        let mut ctx = ctx_with_vector("v", serde_json::json!([1.0, 0.0]));
        ctx.params.insert("q".into(), serde_json::json!([1.0, 0.0]));
        let r = eval_with_ctx("cosineSimilarity(params.q, 'v') + 1.0", &mut ctx);
        approx(as_f64(&r), 2.0);
    }
}
