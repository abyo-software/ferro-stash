// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024 FerroSearch Authors

#![allow(clippy::similar_names)] // arg0/arg1/args are conventional names in JIT compile-call helpers

//! Cranelift JIT compiler for Painless scripts.
//!
//! Compiles frequently-executed scripts (e.g., `script_score`) to native
//! machine code for near-zero overhead execution. First invocation interprets
//! the AST; subsequent invocations call a cached function pointer.
//!
//! Supported script patterns:
//! - Arithmetic: `_score * 2`, `doc['price'].value + 10`
//! - Math functions: `Math.log(1 + x)`, `Math.sin(x)`, etc.
//! - Ternary: `x > 0 ? a : b`
//! - Field access: `doc['field'].value`, `_score`
//!
//! Scripts that use string operations, ctx._source mutation, or complex
//! control flow fall back to the AST interpreter.

use std::collections::HashMap;
use std::sync::Arc;

use cranelift::prelude::*;
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{Linkage, Module};
use dashmap::DashMap;

use crate::parser::{self, BinOp, Expr, Stmt};

/// Signature for JIT-compiled script_score functions.
/// Takes (_score: f64, field_values: *const f64, num_fields: usize) -> f64
type ScriptScoreFn = unsafe extern "C" fn(f64, *const f64, usize) -> f64;

/// Global cache of compiled scripts: source_code → native function pointer.
static JIT_CACHE: std::sync::LazyLock<DashMap<String, CompiledScript>> =
    std::sync::LazyLock::new(DashMap::new);

/// A compiled script ready for execution.
struct CompiledScript {
    /// The compiled function pointer.
    func: ScriptScoreFn,
    /// Field names in order (index matches field_values array position).
    field_order: Vec<String>,
    /// Keep the JIT module alive so code pages aren't unmapped.
    _module: Arc<JITModule>,
}

// Safety: JITModule code pages are immutable after finalization.
// The function pointer is valid for the lifetime of the module.
unsafe impl Send for CompiledScript {}
unsafe impl Sync for CompiledScript {}

/// Try to JIT-compile a script_score script.
/// Returns None if the script is too complex for JIT (falls back to interpreter).
pub fn try_jit_compile(source: &str) -> Option<()> {
    if JIT_CACHE.contains_key(source) {
        return Some(());
    }
    // Catch panics from Cranelift (e.g., PLT not supported on aarch64)
    let source_owned = source.to_string();
    std::panic::catch_unwind(|| try_jit_compile_inner(&source_owned))
        .ok()
        .flatten()
}

fn try_jit_compile_inner(source: &str) -> Option<()> {
    if JIT_CACHE.contains_key(source) {
        return Some(());
    }

    let stmts = parser::parse(source).ok()?;
    // Only compile single-expression scripts (typical for script_score)
    if stmts.len() != 1 {
        return None;
    }
    let expr = match &stmts[0] {
        Stmt::Expr(e) => e,
        _ => return None,
    };

    // Check if all operations are JIT-able (numeric only)
    let mut fields = Vec::new();
    if !is_jitable(expr, &mut fields) {
        return None;
    }

    // Build Cranelift JIT with PIC disabled (avoids PLT which is x86_64-only).
    // Math functions use call_indirect with raw pointers — no Import/PLT needed.
    let module = {
        let mut flag_builder = cranelift::prelude::settings::builder();
        // Disable PIC to avoid PLT generation (not supported on aarch64)
        flag_builder.set("is_pic", "false").unwrap();
        // Use colocated libcalls (direct calls, not via GOT/PLT)
        flag_builder.set("use_colocated_libcalls", "true").unwrap();
        let isa_builder = cranelift_native::builder().unwrap_or_else(|msg| {
            panic!("host machine is not supported: {msg}");
        });
        let isa = isa_builder
            .finish(cranelift::prelude::settings::Flags::new(flag_builder))
            .expect("ISA creation");
        let builder = JITBuilder::with_isa(isa, cranelift_module::default_libcall_names());
        JITModule::new(builder)
    };
    let mut module = module;
    let mut ctx = module.make_context();

    // Function signature: (f64 _score, *const f64 fields, usize num_fields) -> f64
    let ptr_type = module.target_config().pointer_type();
    let mut sig = module.make_signature();
    sig.params.push(AbiParam::new(types::F64)); // _score
    sig.params.push(AbiParam::new(ptr_type)); // field_values ptr
    sig.params.push(AbiParam::new(ptr_type)); // num_fields
    sig.returns.push(AbiParam::new(types::F64)); // result

    let func_id = module
        .declare_function("script_score", Linkage::Local, &sig)
        .ok()?;

    ctx.func.signature = sig;
    let mut fn_builder_ctx = FunctionBuilderContext::new();
    {
        let mut builder = FunctionBuilder::new(&mut ctx.func, &mut fn_builder_ctx);
        let entry = builder.create_block();
        builder.append_block_params_for_function_params(entry);
        builder.switch_to_block(entry);
        builder.seal_block(entry);

        let score_val = builder.block_params(entry)[0];
        let fields_ptr = builder.block_params(entry)[1];

        // Build field name → index map
        let field_indices: HashMap<String, usize> = fields
            .iter()
            .enumerate()
            .map(|(i, f)| (f.clone(), i))
            .collect();

        // Compile the expression
        let result = compile_expr(
            &mut builder,
            &mut module,
            expr,
            score_val,
            fields_ptr,
            &field_indices,
        )?;

        builder.ins().return_(&[result]);
        builder.finalize();
    }

    module.define_function(func_id, &mut ctx).ok()?;
    module.clear_context(&mut ctx);
    module.finalize_definitions().ok()?;

    let code_ptr = module.get_finalized_function(func_id);
    let func: ScriptScoreFn = unsafe { std::mem::transmute(code_ptr) };

    // SAFETY: CompiledScript manually implements Send+Sync above; JITModule
    // itself is not auto-Send/Sync, but finalized code pages are immutable and
    // we keep the Arc only to pin them for the program lifetime.
    #[allow(clippy::arc_with_non_send_sync)]
    let module = Arc::new(module);
    JIT_CACHE.insert(
        source.to_string(),
        CompiledScript {
            func,
            field_order: fields,
            _module: module,
        },
    );

    Some(())
}

/// Execute a JIT-compiled script_score.
/// Returns None if script is not compiled (caller should use interpreter).
#[allow(clippy::implicit_hasher)]
pub fn execute_jit(source: &str, score: f64, doc_fields: &HashMap<String, f64>) -> Option<f64> {
    let entry = JIT_CACHE.get(source)?;
    let compiled = entry.value();

    // Build field_values array in the expected order
    let mut field_values: Vec<f64> = Vec::with_capacity(compiled.field_order.len());
    for field_name in &compiled.field_order {
        field_values.push(*doc_fields.get(field_name).unwrap_or(&0.0));
    }

    let result = unsafe { (compiled.func)(score, field_values.as_ptr(), field_values.len()) };
    Some(result)
}

/// Check if an expression can be JIT-compiled (numeric-only operations).
fn is_jitable(expr: &Expr, fields: &mut Vec<String>) -> bool {
    match expr {
        Expr::IntLit(_) | Expr::FloatLit(_) | Expr::BoolLit(_) => true,
        Expr::Var(name) if name == "_score" => true,
        Expr::DocField(field_name) => {
            if !fields.contains(field_name) {
                fields.push(field_name.clone());
            }
            true
        }
        Expr::BinOp(left, _, right) => is_jitable(left, fields) && is_jitable(right, fields),
        Expr::UnaryOp(_, inner) => is_jitable(inner, fields),
        Expr::MathCall(_, args) => args.iter().all(|a| is_jitable(a, fields)),
        Expr::Ternary(cond, then_expr, else_expr) => {
            is_jitable(cond, fields)
                && is_jitable(then_expr, fields)
                && is_jitable(else_expr, fields)
        }
        Expr::MethodCall(target, method, _) if method == "value" => is_jitable(target, fields),
        _ => false,
    }
}

/// Compile an AST expression to Cranelift IR.
fn compile_expr(
    builder: &mut FunctionBuilder,
    module: &mut JITModule,
    expr: &Expr,
    score_val: Value,
    fields_ptr: Value,
    field_indices: &HashMap<String, usize>,
) -> Option<Value> {
    match expr {
        Expr::FloatLit(f) => Some(builder.ins().f64const(*f)),
        Expr::IntLit(i) => Some(builder.ins().f64const(*i as f64)),
        Expr::BoolLit(b) => Some(builder.ins().f64const(if *b { 1.0 } else { 0.0 })),
        Expr::Var(name) if name == "_score" => Some(score_val),
        Expr::DocField(field_name) => {
            let idx = *field_indices.get(field_name)?;
            let ptr_type = module.target_config().pointer_type();
            let offset = builder.ins().iconst(ptr_type, (idx * 8) as i64);
            let addr = builder.ins().iadd(fields_ptr, offset);
            Some(builder.ins().load(types::F64, MemFlags::trusted(), addr, 0))
        }
        Expr::MethodCall(target, method, _) if method == "value" => compile_expr(
            builder,
            module,
            target,
            score_val,
            fields_ptr,
            field_indices,
        ),
        Expr::BinOp(left, op, right) => {
            let lhs = compile_expr(builder, module, left, score_val, fields_ptr, field_indices)?;
            let rhs = compile_expr(builder, module, right, score_val, fields_ptr, field_indices)?;
            Some(match op {
                BinOp::Add => builder.ins().fadd(lhs, rhs),
                BinOp::Sub => builder.ins().fsub(lhs, rhs),
                BinOp::Mul => builder.ins().fmul(lhs, rhs),
                BinOp::Div => builder.ins().fdiv(lhs, rhs),
                BinOp::Gt | BinOp::Lt | BinOp::Gte | BinOp::Lte | BinOp::Eq | BinOp::Neq => {
                    let cc = match op {
                        BinOp::Gt => FloatCC::GreaterThan,
                        BinOp::Lt => FloatCC::LessThan,
                        BinOp::Gte => FloatCC::GreaterThanOrEqual,
                        BinOp::Lte => FloatCC::LessThanOrEqual,
                        BinOp::Eq => FloatCC::Equal,
                        BinOp::Neq => FloatCC::NotEqual,
                        _ => unreachable!(),
                    };
                    let cmp = builder.ins().fcmp(cc, lhs, rhs);
                    let one = builder.ins().f64const(1.0);
                    let zero = builder.ins().f64const(0.0);
                    builder.ins().select(cmp, one, zero)
                }
                _ => return None,
            })
        }
        Expr::UnaryOp(crate::parser::UnaryOp::Neg, inner) => {
            let val = compile_expr(builder, module, inner, score_val, fields_ptr, field_indices)?;
            Some(builder.ins().fneg(val))
        }
        Expr::MathCall(func, args) => compile_math_call(
            builder,
            module,
            func,
            args,
            score_val,
            fields_ptr,
            field_indices,
        ),
        Expr::Ternary(cond, then_expr, else_expr) => {
            let cond_val =
                compile_expr(builder, module, cond, score_val, fields_ptr, field_indices)?;
            let zero = builder.ins().f64const(0.0);
            let cmp = builder.ins().fcmp(FloatCC::NotEqual, cond_val, zero);

            let then_block = builder.create_block();
            let else_block = builder.create_block();
            let merge_block = builder.create_block();
            builder.append_block_param(merge_block, types::F64);

            builder.ins().brif(cmp, then_block, &[], else_block, &[]);

            builder.switch_to_block(then_block);
            builder.seal_block(then_block);
            let then_val = compile_expr(
                builder,
                module,
                then_expr,
                score_val,
                fields_ptr,
                field_indices,
            )?;
            builder.ins().jump(merge_block, &[then_val]);

            builder.switch_to_block(else_block);
            builder.seal_block(else_block);
            let else_val = compile_expr(
                builder,
                module,
                else_expr,
                score_val,
                fields_ptr,
                field_indices,
            )?;
            builder.ins().jump(merge_block, &[else_val]);

            builder.switch_to_block(merge_block);
            builder.seal_block(merge_block);
            Some(builder.block_params(merge_block)[0])
        }
        _ => None,
    }
}

/// Compile a Math.* function call using indirect call (avoids PLT, works on aarch64).
fn compile_math_call(
    builder: &mut FunctionBuilder,
    module: &mut JITModule,
    func_name: &str,
    args: &[Expr],
    score_val: Value,
    fields_ptr: Value,
    field_indices: &HashMap<String, usize>,
) -> Option<Value> {
    let arg0 = compile_expr(
        builder,
        module,
        args.first()?,
        score_val,
        fields_ptr,
        field_indices,
    )?;

    // Get the function pointer for the math function
    let fn_ptr: *const u8 = match func_name {
        "log" => f64::ln as *const u8,
        "log10" => f64::log10 as *const u8,
        "log2" => f64::log2 as *const u8,
        "exp" => f64::exp as *const u8,
        "sqrt" => f64::sqrt as *const u8,
        "cbrt" => f64::cbrt as *const u8,
        "sin" => f64::sin as *const u8,
        "cos" => f64::cos as *const u8,
        "tan" => f64::tan as *const u8,
        "asin" => f64::asin as *const u8,
        "acos" => f64::acos as *const u8,
        "atan" => f64::atan as *const u8,
        "sinh" => f64::sinh as *const u8,
        "cosh" => f64::cosh as *const u8,
        "tanh" => f64::tanh as *const u8,
        "ceil" => f64::ceil as *const u8,
        "floor" => f64::floor as *const u8,
        "round" => f64::round as *const u8,
        "abs" => f64::abs as *const u8,
        _ => return None,
    };

    // Indirect call: load function pointer as immediate, call via call_indirect.
    // This avoids PLT (not available on aarch64) and declare_function/Import.
    let ptr_type = module.target_config().pointer_type();
    let fn_addr = builder.ins().iconst(ptr_type, fn_ptr as i64);

    let mut sig = module.make_signature();
    sig.params.push(AbiParam::new(types::F64));
    sig.returns.push(AbiParam::new(types::F64));
    let sig_ref = builder.import_signature(sig);

    let call = builder.ins().call_indirect(sig_ref, fn_addr, &[arg0]);
    Some(builder.inst_results(call)[0])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_jit_simple_score() {
        let source = "_score * 2";
        assert!(try_jit_compile(source).is_some());

        let fields = HashMap::new();
        let result = execute_jit(source, 3.0, &fields).unwrap();
        assert!((result - 6.0).abs() < 1e-10);
    }

    #[test]
    fn test_jit_math_log() {
        let source = "Math.log(1 + doc['price'].value) * _score";
        assert!(try_jit_compile(source).is_some());

        let mut fields = HashMap::new();
        fields.insert("price".to_string(), 99.0);
        let result = execute_jit(source, 2.0, &fields).unwrap();
        let expected = (100.0_f64).ln() * 2.0;
        assert!(
            (result - expected).abs() < 1e-10,
            "got {result}, expected {expected}"
        );
    }

    #[test]
    fn test_jit_ternary() {
        let source = "doc['price'].value > 100 ? _score * 2 : _score";
        assert!(try_jit_compile(source).is_some());

        let mut fields = HashMap::new();
        fields.insert("price".to_string(), 200.0);
        assert!((execute_jit(source, 1.0, &fields).unwrap() - 2.0).abs() < 1e-10);

        fields.insert("price".to_string(), 50.0);
        assert!((execute_jit(source, 1.0, &fields).unwrap() - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_jit_complex_trig() {
        let source = "Math.sin(doc['price'].value) + Math.cos(doc['rating'].value) > 0 ? _score * 2 : _score";
        assert!(try_jit_compile(source).is_some());

        let mut fields = HashMap::new();
        fields.insert("price".to_string(), 1.0);
        fields.insert("rating".to_string(), 0.0);
        // sin(1) + cos(0) = 0.841 + 1.0 = 1.841 > 0 → _score * 2
        let result = execute_jit(source, 1.5, &fields).unwrap();
        assert!((result - 3.0).abs() < 1e-10);
    }

    #[test]
    fn test_jit_non_jitable_falls_back() {
        // String operations can't be JIT-compiled
        let source = "ctx._source.name.toLowerCase()";
        assert!(try_jit_compile(source).is_none());
    }
}
