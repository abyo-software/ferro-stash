// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024 FerroSearch Authors

//! Tokenizer and recursive descent parser for the Painless scripting language subset.

use crate::error::FerroError;

// ── Tokens ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Literals
    IntLit(i64),
    FloatLit(f64),
    StringLit(String),
    BoolLit(bool),
    Null,

    // Regex literal
    RegexLit(String),

    // Identifiers & keywords
    Ident(String),
    If,
    Else,
    Return,
    For,
    Def,
    Int,
    New,

    // Regex operators
    RegexFind,  // =~
    RegexMatch, // ==~

    // Operators
    Plus,
    PlusAssign,
    Minus,
    MinusAssign,
    Star,
    StarAssign,
    Slash,
    SlashAssign,
    Percent,
    Eq,
    Neq,
    Gt,
    Lt,
    Gte,
    Lte,
    And,
    Or,
    Not,
    Assign,

    // Delimiters
    LParen,
    RParen,
    LBrace,
    RBrace,
    LBracket,
    RBracket,
    Dot,
    Comma,
    Semicolon,
    Question,
    Colon,

    Eof,
}

// ── Tokenizer ───────────────────────────────────────────────────────────────

pub fn tokenize(input: &str) -> Result<Vec<Token>, FerroError> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        let c = chars[i];

        // Skip whitespace
        if c.is_whitespace() {
            i += 1;
            continue;
        }

        // Skip line comments
        if c == '/' && i + 1 < len && chars[i + 1] == '/' {
            while i < len && chars[i] != '\n' {
                i += 1;
            }
            continue;
        }

        // Three-character operators
        if i + 2 < len {
            let three: String = chars[i..i + 3].iter().collect();
            if three == "==~" {
                tokens.push(Token::RegexMatch);
                i += 3;
                continue;
            }
        }

        // Two-character operators
        if i + 1 < len {
            let two = format!("{}{}", c, chars[i + 1]);
            let tok = match two.as_str() {
                "==" => Some(Token::Eq),
                "!=" => Some(Token::Neq),
                ">=" => Some(Token::Gte),
                "<=" => Some(Token::Lte),
                "&&" => Some(Token::And),
                "||" => Some(Token::Or),
                "+=" => Some(Token::PlusAssign),
                "-=" => Some(Token::MinusAssign),
                "*=" => Some(Token::StarAssign),
                "/=" => Some(Token::SlashAssign),
                "=~" => Some(Token::RegexFind),
                _ => None,
            };
            if let Some(t) = tok {
                tokens.push(t);
                i += 2;
                continue;
            }
        }

        // Regex literals: /pattern/
        // Only tokenize as regex if the previous token suggests it's not division
        if c == '/' {
            let is_regex = match tokens.last() {
                None => true,
                Some(
                    Token::RParen
                    | Token::RBracket
                    | Token::IntLit(_)
                    | Token::FloatLit(_)
                    | Token::StringLit(_)
                    | Token::BoolLit(_)
                    | Token::Ident(_)
                    | Token::Null,
                ) => false,
                _ => true,
            };
            if is_regex {
                i += 1;
                let mut pattern = String::new();
                while i < len && chars[i] != '/' {
                    if chars[i] == '\\' && i + 1 < len {
                        pattern.push(chars[i]);
                        i += 1;
                        pattern.push(chars[i]);
                    } else {
                        pattern.push(chars[i]);
                    }
                    i += 1;
                }
                if i >= len {
                    return Err(FerroError::QueryParseError(
                        "unterminated regex literal".into(),
                    ));
                }
                i += 1; // skip closing /
                tokens.push(Token::RegexLit(pattern));
                continue;
            }
        }

        // Single-character tokens
        let single = match c {
            '+' => Some(Token::Plus),
            '-' => Some(Token::Minus),
            '*' => Some(Token::Star),
            '/' => Some(Token::Slash),
            '%' => Some(Token::Percent),
            '>' => Some(Token::Gt),
            '<' => Some(Token::Lt),
            '!' => Some(Token::Not),
            '=' => Some(Token::Assign),
            '(' => Some(Token::LParen),
            ')' => Some(Token::RParen),
            '{' => Some(Token::LBrace),
            '}' => Some(Token::RBrace),
            '[' => Some(Token::LBracket),
            ']' => Some(Token::RBracket),
            '.' => Some(Token::Dot),
            ',' => Some(Token::Comma),
            ';' => Some(Token::Semicolon),
            '?' => Some(Token::Question),
            ':' => Some(Token::Colon),
            _ => None,
        };
        if let Some(t) = single {
            tokens.push(t);
            i += 1;
            continue;
        }

        // String literals
        if c == '\'' || c == '"' {
            let quote = c;
            i += 1;
            let mut s = String::new();
            while i < len && chars[i] != quote {
                if chars[i] == '\\' && i + 1 < len {
                    i += 1;
                    match chars[i] {
                        'n' => s.push('\n'),
                        't' => s.push('\t'),
                        '\\' => s.push('\\'),
                        other => {
                            s.push(other);
                        }
                    }
                } else {
                    s.push(chars[i]);
                }
                i += 1;
            }
            if i >= len {
                return Err(FerroError::QueryParseError(
                    "unterminated string literal".into(),
                ));
            }
            i += 1; // skip closing quote
            tokens.push(Token::StringLit(s));
            continue;
        }

        // Number literals
        if c.is_ascii_digit() {
            let start = i;
            while i < len && chars[i].is_ascii_digit() {
                i += 1;
            }
            if i < len && chars[i] == '.' && i + 1 < len && chars[i + 1].is_ascii_digit() {
                i += 1;
                while i < len && chars[i].is_ascii_digit() {
                    i += 1;
                }
                let s: String = chars[start..i].iter().collect();
                let f: f64 = s
                    .parse()
                    .map_err(|e| FerroError::QueryParseError(format!("invalid float: {e}")))?;
                tokens.push(Token::FloatLit(f));
            } else {
                let s: String = chars[start..i].iter().collect();
                let n: i64 = s
                    .parse()
                    .map_err(|e| FerroError::QueryParseError(format!("invalid int: {e}")))?;
                tokens.push(Token::IntLit(n));
            }
            continue;
        }

        // Identifiers and keywords
        if c.is_ascii_alphabetic() || c == '_' {
            let start = i;
            while i < len && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            let tok = match word.as_str() {
                "true" => Token::BoolLit(true),
                "false" => Token::BoolLit(false),
                "null" => Token::Null,
                "if" => Token::If,
                "else" => Token::Else,
                "return" => Token::Return,
                "for" => Token::For,
                "def" => Token::Def,
                "int" => Token::Int,
                "new" => Token::New,
                _ => Token::Ident(word),
            };
            tokens.push(tok);
            continue;
        }

        return Err(FerroError::QueryParseError(format!(
            "unexpected character: '{c}'"
        )));
    }

    tokens.push(Token::Eof);
    Ok(tokens)
}

// ── AST ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    IntLit(i64),
    FloatLit(f64),
    StringLit(String),
    BoolLit(bool),
    NullLit,

    /// Binary operation: left op right
    BinOp(Box<Expr>, BinOp, Box<Expr>),

    /// Unary operation: op expr
    UnaryOp(UnaryOp, Box<Expr>),

    /// Ternary: cond ? then : else
    Ternary(Box<Expr>, Box<Expr>, Box<Expr>),

    /// Field access: `doc['field'].value`, `ctx._source.field`, `ctx._source['field']`
    DocField(String),

    /// ctx._source access (for read)
    SourceField(String),

    /// params access
    ParamField(String),

    /// Assignment: ctx._source.field = expr
    Assign(String, Box<Expr>),

    /// Compound assignment: ctx._source.field <op>= expr
    CompoundAssign(String, BinOp, Box<Expr>),

    /// Method call on a value: expr.method(args)
    MethodCall(Box<Expr>, String, Vec<Expr>),

    /// Math.fn(args)
    MathCall(String, Vec<Expr>),

    /// Bare function call: fn(args). Used for built-in vector helpers
    /// like `dotProduct(query, 'field')` and `cosineSimilarity(query, 'field')`.
    FuncCall(String, Vec<Expr>),

    /// Variable reference
    Var(String),

    /// If/else statement
    IfElse(Box<Expr>, Vec<Stmt>, Option<Vec<Stmt>>),

    /// For loop: for (init; cond; incr) { body }
    For(Box<Stmt>, Box<Expr>, Box<Stmt>, Vec<Stmt>),

    /// For-each loop: for (type var : iterable) { body }
    ForEach(String, Box<Expr>, Vec<Stmt>),

    /// Regex literal
    RegexLit(String),

    /// Regex find operator: text =~ /pattern/
    RegexFind(Box<Expr>, Box<Expr>),

    /// Regex match operator: text ==~ /pattern/
    RegexMatchOp(Box<Expr>, Box<Expr>),

    /// Type cast: (int) expr, (double) expr, etc.
    TypeCast(String, Box<Expr>),

    /// Static method call: Integer.parseInt(s), String.valueOf(n)
    StaticCall(String, String, Vec<Expr>),

    /// Lambda expression: (params) -> body
    Lambda(Vec<String>, Box<Expr>),

    /// Array/list literal: [1, 2, 3]
    ArrayLit(Vec<Expr>),

    /// Variable declaration with assignment: def x = expr, int x = expr
    VarDecl(String, Box<Expr>),

    /// Compound assignment on a local variable: x <op>= expr
    VarCompoundAssign(String, BinOp, Box<Expr>),

    /// Index access: expr[index]
    IndexAccess(Box<Expr>, Box<Expr>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    Neq,
    Gt,
    Lt,
    Gte,
    Lte,
    And,
    Or,
}

#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOp {
    Neg,
    Not,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Stmt {
    Expr(Expr),
    Return(Expr),
    VarDecl(String, Expr),
    /// try { body } catch (Exception e) { handler }
    TryCatch(Vec<Stmt>, String, Vec<Stmt>),
}

// ── Parser ──────────────────────────────────────────────────────────────────

pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    fn advance(&mut self) -> Token {
        let tok = self.tokens.get(self.pos).cloned().unwrap_or(Token::Eof);
        self.pos += 1;
        tok
    }

    fn expect(&mut self, expected: &Token) -> Result<(), FerroError> {
        let tok = self.advance();
        if tok == *expected {
            Ok(())
        } else {
            Err(FerroError::QueryParseError(format!(
                "expected {expected:?}, got {tok:?}"
            )))
        }
    }

    pub fn parse_program(&mut self) -> Result<Vec<Stmt>, FerroError> {
        let mut stmts = Vec::new();
        while *self.peek() != Token::Eof {
            stmts.push(self.parse_stmt()?);
        }
        Ok(stmts)
    }

    fn parse_stmt(&mut self) -> Result<Stmt, FerroError> {
        if *self.peek() == Token::Return {
            self.advance();
            let expr = self.parse_expr()?;
            if *self.peek() == Token::Semicolon {
                self.advance();
            }
            return Ok(Stmt::Return(expr));
        }

        // Variable declaration: def x = expr or int x = expr
        if matches!(self.peek(), Token::Def | Token::Int) {
            self.advance();
            if let Token::Ident(name) = self.advance() {
                if *self.peek() == Token::Assign {
                    self.advance();
                    let expr = self.parse_expr()?;
                    if *self.peek() == Token::Semicolon {
                        self.advance();
                    }
                    return Ok(Stmt::VarDecl(name, expr));
                }
                // Declaration without assignment
                if *self.peek() == Token::Semicolon {
                    self.advance();
                }
                return Ok(Stmt::VarDecl(name, Expr::NullLit));
            }
            return Err(FerroError::QueryParseError(
                "expected variable name after type".into(),
            ));
        }

        let expr = self.parse_expr()?;
        if *self.peek() == Token::Semicolon {
            self.advance();
        }
        Ok(Stmt::Expr(expr))
    }

    fn parse_expr(&mut self) -> Result<Expr, FerroError> {
        // Check for if/else
        if *self.peek() == Token::If {
            return self.parse_if();
        }

        // Check for for loops
        if *self.peek() == Token::For {
            return self.parse_for();
        }

        // Check for assignment: ctx._source.field = expr or ctx._source['field'] = expr
        let checkpoint = self.pos;
        if let Ok(field) = self.try_parse_source_lvalue() {
            if *self.peek() == Token::Assign {
                self.advance();
                let value = self.parse_expr()?;
                return Ok(Expr::Assign(field, Box::new(value)));
            } else if let Some(op) = compound_assign_op(self.peek()) {
                self.advance();
                let value = self.parse_expr()?;
                return Ok(Expr::CompoundAssign(field, op, Box::new(value)));
            }
            // Not an assignment, backtrack
            self.pos = checkpoint;
        }

        // Check for variable assignment: varname = expr
        let checkpoint = self.pos;
        if let Token::Ident(ref name) = self.peek().clone() {
            let var_name = name.clone();
            self.advance();
            if *self.peek() == Token::Assign {
                self.advance();
                let value = self.parse_expr()?;
                return Ok(Expr::VarDecl(var_name, Box::new(value)));
            } else if let Some(op) = compound_assign_op(self.peek()) {
                self.advance();
                let value = self.parse_expr()?;
                return Ok(Expr::VarCompoundAssign(var_name, op, Box::new(value)));
            }
            self.pos = checkpoint;
        }

        self.parse_ternary()
    }

    fn try_parse_source_lvalue(&mut self) -> Result<String, FerroError> {
        // ctx._source.field.subfield... or ctx._source['field'] or ctx.field.subfield...
        if let Token::Ident(ref name) = self.peek().clone()
            && name == "ctx"
        {
            self.advance();
            self.expect(&Token::Dot)?;
            let next = self.advance();
            match next {
                Token::Ident(ref s) if s == "_source" => {
                    if *self.peek() == Token::Dot {
                        self.advance();
                        if let Token::Ident(field) = self.advance() {
                            // Collect additional dotted segments: task.ownerId etc.
                            let mut path = field;
                            while *self.peek() == Token::Dot {
                                let checkpoint = self.pos;
                                self.advance();
                                if let Token::Ident(ref next_field) = self.peek().clone() {
                                    let next_field = next_field.clone();
                                    self.advance();
                                    path = format!("{path}.{next_field}");
                                } else {
                                    self.pos = checkpoint;
                                    break;
                                }
                            }
                            return Ok(path);
                        }
                    } else if *self.peek() == Token::LBracket {
                        self.advance();
                        if let Token::StringLit(field) = self.advance() {
                            self.expect(&Token::RBracket)?;
                            return Ok(field);
                        }
                    }
                }
                Token::Ident(field) => {
                    // Collect additional dotted segments
                    let mut path = field;
                    while *self.peek() == Token::Dot {
                        let checkpoint = self.pos;
                        self.advance();
                        if let Token::Ident(ref next_field) = self.peek().clone() {
                            let next_field = next_field.clone();
                            self.advance();
                            path = format!("{path}.{next_field}");
                        } else {
                            self.pos = checkpoint;
                            break;
                        }
                    }
                    return Ok(path);
                }
                _ => {}
            }
        }
        Err(FerroError::QueryParseError("not a source lvalue".into()))
    }

    fn parse_if(&mut self) -> Result<Expr, FerroError> {
        self.expect(&Token::If)?;
        self.expect(&Token::LParen)?;
        let cond = self.parse_expr()?;
        self.expect(&Token::RParen)?;
        let then_body = self.parse_block()?;
        let else_body = if *self.peek() == Token::Else {
            self.advance();
            if *self.peek() == Token::If {
                // else if
                let if_expr = self.parse_if()?;
                Some(vec![Stmt::Expr(if_expr)])
            } else {
                Some(self.parse_block()?)
            }
        } else {
            None
        };
        Ok(Expr::IfElse(Box::new(cond), then_body, else_body))
    }

    fn parse_for(&mut self) -> Result<Expr, FerroError> {
        self.expect(&Token::For)?;
        self.expect(&Token::LParen)?;

        // Determine if this is for-each (type var : iterable) or C-style for (init; cond; incr)
        // Peek ahead: if we see "def/int ident :" pattern, it's for-each
        let checkpoint = self.pos;
        let is_foreach = if matches!(self.peek(), Token::Def | Token::Int) {
            self.advance();
            if let Token::Ident(_) = self.peek().clone() {
                self.advance();
                *self.peek() == Token::Colon
            } else {
                false
            }
        } else {
            false
        };
        self.pos = checkpoint;

        if is_foreach {
            // for (def/int var : iterable) { body }
            self.advance(); // skip def/int
            let var_name = if let Token::Ident(name) = self.advance() {
                name
            } else {
                return Err(FerroError::QueryParseError(
                    "expected variable name in for-each".into(),
                ));
            };
            self.expect(&Token::Colon)?;
            let iterable = self.parse_expr()?;
            self.expect(&Token::RParen)?;
            let body = self.parse_block()?;
            Ok(Expr::ForEach(var_name, Box::new(iterable), body))
        } else {
            // C-style for (init; cond; incr) { body }
            let init = self.parse_stmt()?;
            // parse_stmt already consumed the semicolon
            let cond = self.parse_expr()?;
            self.expect(&Token::Semicolon)?;
            let incr_expr = self.parse_expr()?;
            let incr = Stmt::Expr(incr_expr);
            self.expect(&Token::RParen)?;
            let body = self.parse_block()?;
            Ok(Expr::For(
                Box::new(init),
                Box::new(cond),
                Box::new(incr),
                body,
            ))
        }
    }

    fn parse_block(&mut self) -> Result<Vec<Stmt>, FerroError> {
        self.expect(&Token::LBrace)?;
        let mut stmts = Vec::new();
        while *self.peek() != Token::RBrace && *self.peek() != Token::Eof {
            stmts.push(self.parse_stmt()?);
        }
        self.expect(&Token::RBrace)?;
        Ok(stmts)
    }

    fn parse_ternary(&mut self) -> Result<Expr, FerroError> {
        let cond = self.parse_or()?;
        if *self.peek() == Token::Question {
            self.advance();
            let then_expr = self.parse_expr()?;
            self.expect(&Token::Colon)?;
            let else_expr = self.parse_expr()?;
            Ok(Expr::Ternary(
                Box::new(cond),
                Box::new(then_expr),
                Box::new(else_expr),
            ))
        } else {
            Ok(cond)
        }
    }

    fn parse_or(&mut self) -> Result<Expr, FerroError> {
        let mut left = self.parse_and()?;
        while *self.peek() == Token::Or {
            self.advance();
            let right = self.parse_and()?;
            left = Expr::BinOp(Box::new(left), BinOp::Or, Box::new(right));
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr, FerroError> {
        let mut left = self.parse_equality()?;
        while *self.peek() == Token::And {
            self.advance();
            let right = self.parse_equality()?;
            left = Expr::BinOp(Box::new(left), BinOp::And, Box::new(right));
        }
        Ok(left)
    }

    fn parse_equality(&mut self) -> Result<Expr, FerroError> {
        let mut left = self.parse_comparison()?;
        loop {
            match self.peek().clone() {
                Token::Eq => {
                    self.advance();
                    let right = self.parse_comparison()?;
                    left = Expr::BinOp(Box::new(left), BinOp::Eq, Box::new(right));
                }
                Token::Neq => {
                    self.advance();
                    let right = self.parse_comparison()?;
                    left = Expr::BinOp(Box::new(left), BinOp::Neq, Box::new(right));
                }
                Token::RegexFind => {
                    self.advance();
                    let right = self.parse_comparison()?;
                    left = Expr::RegexFind(Box::new(left), Box::new(right));
                }
                Token::RegexMatch => {
                    self.advance();
                    let right = self.parse_comparison()?;
                    left = Expr::RegexMatchOp(Box::new(left), Box::new(right));
                }
                _ => break,
            }
        }
        Ok(left)
    }

    fn parse_comparison(&mut self) -> Result<Expr, FerroError> {
        let mut left = self.parse_addition()?;
        loop {
            let op = match self.peek() {
                Token::Gt => BinOp::Gt,
                Token::Lt => BinOp::Lt,
                Token::Gte => BinOp::Gte,
                Token::Lte => BinOp::Lte,
                _ => break,
            };
            self.advance();
            let right = self.parse_addition()?;
            left = Expr::BinOp(Box::new(left), op, Box::new(right));
        }
        Ok(left)
    }

    fn parse_addition(&mut self) -> Result<Expr, FerroError> {
        let mut left = self.parse_multiplication()?;
        loop {
            let op = match self.peek() {
                Token::Plus => BinOp::Add,
                Token::Minus => BinOp::Sub,
                _ => break,
            };
            self.advance();
            let right = self.parse_multiplication()?;
            left = Expr::BinOp(Box::new(left), op, Box::new(right));
        }
        Ok(left)
    }

    fn parse_multiplication(&mut self) -> Result<Expr, FerroError> {
        let mut left = self.parse_unary()?;
        loop {
            let op = match self.peek() {
                Token::Star => BinOp::Mul,
                Token::Slash => BinOp::Div,
                Token::Percent => BinOp::Mod,
                _ => break,
            };
            self.advance();
            let right = self.parse_unary()?;
            left = Expr::BinOp(Box::new(left), op, Box::new(right));
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Expr, FerroError> {
        match self.peek().clone() {
            Token::Not => {
                self.advance();
                let expr = self.parse_unary()?;
                Ok(Expr::UnaryOp(UnaryOp::Not, Box::new(expr)))
            }
            Token::Minus => {
                self.advance();
                let expr = self.parse_unary()?;
                Ok(Expr::UnaryOp(UnaryOp::Neg, Box::new(expr)))
            }
            _ => self.parse_postfix(),
        }
    }

    fn parse_postfix(&mut self) -> Result<Expr, FerroError> {
        let mut expr = self.parse_primary()?;
        loop {
            if *self.peek() == Token::Dot {
                self.advance();
                if let Token::Ident(method) = self.advance() {
                    if *self.peek() == Token::LParen {
                        self.advance();
                        let args = self.parse_args()?;
                        self.expect(&Token::RParen)?;
                        expr = Expr::MethodCall(Box::new(expr), method, args);
                    } else {
                        // Treat as field access on the value — re-wrap as method
                        // This handles things like `.value` on doc fields
                        expr = Expr::MethodCall(Box::new(expr), method, vec![]);
                    }
                } else {
                    return Err(FerroError::QueryParseError(
                        "expected identifier after '.'".into(),
                    ));
                }
            } else if *self.peek() == Token::LBracket {
                self.advance();
                let index = self.parse_expr()?;
                self.expect(&Token::RBracket)?;
                expr = Expr::IndexAccess(Box::new(expr), Box::new(index));
            } else {
                break;
            }
        }
        Ok(expr)
    }

    fn parse_args(&mut self) -> Result<Vec<Expr>, FerroError> {
        let mut args = Vec::new();
        if *self.peek() == Token::RParen {
            return Ok(args);
        }
        args.push(self.parse_expr()?);
        while *self.peek() == Token::Comma {
            self.advance();
            args.push(self.parse_expr()?);
        }
        Ok(args)
    }

    fn parse_primary(&mut self) -> Result<Expr, FerroError> {
        match self.peek().clone() {
            Token::IntLit(n) => {
                self.advance();
                Ok(Expr::IntLit(n))
            }
            Token::FloatLit(f) => {
                self.advance();
                Ok(Expr::FloatLit(f))
            }
            Token::StringLit(s) => {
                self.advance();
                Ok(Expr::StringLit(s))
            }
            Token::BoolLit(b) => {
                self.advance();
                Ok(Expr::BoolLit(b))
            }
            Token::Null => {
                self.advance();
                Ok(Expr::NullLit)
            }
            Token::RegexLit(pattern) => {
                self.advance();
                Ok(Expr::RegexLit(pattern))
            }
            Token::LBracket => {
                // Array literal: [1, 2, 3]
                self.advance();
                let mut elements = Vec::new();
                if *self.peek() != Token::RBracket {
                    elements.push(self.parse_expr()?);
                    while *self.peek() == Token::Comma {
                        self.advance();
                        elements.push(self.parse_expr()?);
                    }
                }
                self.expect(&Token::RBracket)?;
                Ok(Expr::ArrayLit(elements))
            }
            Token::LParen => {
                self.advance();
                // Check for type cast: (int) expr, (double) expr
                if let Token::Ident(ref type_name) = self.peek().clone() {
                    let tn = type_name.clone();
                    if matches!(
                        tn.as_str(),
                        "int" | "long" | "float" | "double" | "String" | "boolean"
                    ) {
                        let checkpoint = self.pos;
                        self.advance();
                        if *self.peek() == Token::RParen {
                            self.advance();
                            let expr = self.parse_unary()?;
                            return Ok(Expr::TypeCast(tn, Box::new(expr)));
                        }
                        // Not a cast, backtrack
                        self.pos = checkpoint;
                    }
                }
                // Also handle (int) from the Int token
                if *self.peek() == Token::Int {
                    let checkpoint = self.pos;
                    self.advance();
                    if *self.peek() == Token::RParen {
                        self.advance();
                        let expr = self.parse_unary()?;
                        return Ok(Expr::TypeCast("int".to_string(), Box::new(expr)));
                    }
                    self.pos = checkpoint;
                }
                let expr = self.parse_expr()?;
                self.expect(&Token::RParen)?;
                Ok(expr)
            }
            Token::Ident(name) => {
                self.advance();
                match name.as_str() {
                    "doc" => self.parse_doc_access(),
                    "ctx" => self.parse_ctx_access(),
                    "params" => self.parse_params_access(),
                    "Math" => {
                        self.expect(&Token::Dot)?;
                        if let Token::Ident(func) = self.advance() {
                            // Math.PI, Math.E — constants (no parentheses)
                            if func == "PI" {
                                Ok(Expr::FloatLit(std::f64::consts::PI))
                            } else if func == "E" {
                                Ok(Expr::FloatLit(std::f64::consts::E))
                            } else {
                                self.expect(&Token::LParen)?;
                                let args = self.parse_args()?;
                                self.expect(&Token::RParen)?;
                                Ok(Expr::MathCall(func, args))
                            }
                        } else {
                            Err(FerroError::QueryParseError(
                                "expected Math method name".into(),
                            ))
                        }
                    }
                    // Static method calls: Integer.parseInt, String.valueOf
                    "Integer" | "Long" | "Float" | "Double" | "String" | "Boolean" => {
                        if *self.peek() == Token::Dot {
                            self.advance();
                            if let Token::Ident(method) = self.advance() {
                                self.expect(&Token::LParen)?;
                                let args = self.parse_args()?;
                                self.expect(&Token::RParen)?;
                                Ok(Expr::StaticCall(name, method, args))
                            } else {
                                Err(FerroError::QueryParseError(format!(
                                    "expected method name after {name}."
                                )))
                            }
                        } else {
                            Ok(Expr::Var(name))
                        }
                    }
                    _ => {
                        // Bare function call: ident(args) — used for built-in
                        // vector helpers (dotProduct, cosineSimilarity, l1norm,
                        // l2norm, hamming) on dense_vector fields.
                        if *self.peek() == Token::LParen {
                            self.advance();
                            let args = self.parse_args()?;
                            self.expect(&Token::RParen)?;
                            Ok(Expr::FuncCall(name, args))
                        } else {
                            Ok(Expr::Var(name))
                        }
                    }
                }
            }
            tok => Err(FerroError::QueryParseError(format!(
                "unexpected token: {tok:?}"
            ))),
        }
    }

    /// Parse `doc['field'].value`
    fn parse_doc_access(&mut self) -> Result<Expr, FerroError> {
        self.expect(&Token::LBracket)?;
        let field = match self.advance() {
            Token::StringLit(s) => s,
            tok => {
                return Err(FerroError::QueryParseError(format!(
                    "expected string in doc access, got {tok:?}"
                )));
            }
        };
        self.expect(&Token::RBracket)?;
        // Optional .value
        if *self.peek() == Token::Dot {
            self.advance();
            if let Token::Ident(ref prop) = self.peek().clone()
                && prop == "value"
            {
                self.advance();
            }
        }
        Ok(Expr::DocField(field))
    }

    /// Parse `ctx._source.field.subfield...`, `ctx._source['field']`, `ctx.field.subfield...`
    fn parse_ctx_access(&mut self) -> Result<Expr, FerroError> {
        self.expect(&Token::Dot)?;
        let next = self.advance();
        match next {
            Token::Ident(ref s) if s == "_source" => {
                if *self.peek() == Token::Dot {
                    // Check if next is a method call (ident followed by '(')
                    // If so, return _source as a Var and let postfix handle it
                    let checkpoint = self.pos;
                    self.advance(); // consume dot
                    if let Token::Ident(ref field) = self.peek().clone() {
                        let field = field.clone();
                        self.advance(); // consume ident
                        if *self.peek() == Token::LParen {
                            // This is a method call on ctx._source
                            // Backtrack to before the dot so postfix can handle it
                            self.pos = checkpoint;
                            return Ok(Expr::Var("_source".into()));
                        }
                        // Not a method call, it's field access. Resume collecting path.
                        let mut path = field;
                        while *self.peek() == Token::Dot {
                            let cp = self.pos;
                            self.advance();
                            if let Token::Ident(ref next_field) = self.peek().clone() {
                                let nf = next_field.clone();
                                let nf_pos = self.pos;
                                self.advance();
                                // Check if this next field is a method call
                                if *self.peek() == Token::LParen {
                                    // Backtrack: this is a method call on the field path so far
                                    self.pos = nf_pos;
                                    // Actually we need to backtrack to before the dot
                                    self.pos = cp;
                                    break;
                                }
                                path = format!("{path}.{nf}");
                            } else {
                                self.pos = cp;
                                break;
                            }
                        }
                        Ok(Expr::SourceField(path))
                    } else {
                        self.pos = checkpoint;
                        Ok(Expr::Var("_source".into()))
                    }
                } else if *self.peek() == Token::LBracket {
                    self.advance();
                    if let Token::StringLit(field) = self.advance() {
                        self.expect(&Token::RBracket)?;
                        Ok(Expr::SourceField(field))
                    } else {
                        Err(FerroError::QueryParseError(
                            "expected string in ctx._source[] access".into(),
                        ))
                    }
                } else {
                    // ctx._source as a whole
                    Ok(Expr::Var("_source".into()))
                }
            }
            Token::Ident(field) => {
                // Collect additional dotted segments
                let mut path = field;
                while *self.peek() == Token::Dot {
                    let checkpoint = self.pos;
                    self.advance();
                    if let Token::Ident(ref next_field) = self.peek().clone() {
                        let next_field = next_field.clone();
                        self.advance();
                        path = format!("{path}.{next_field}");
                    } else {
                        self.pos = checkpoint;
                        break;
                    }
                }
                Ok(Expr::SourceField(path))
            }
            _ => Err(FerroError::QueryParseError(
                "expected field after ctx.".into(),
            )),
        }
    }

    /// Parse params.field
    fn parse_params_access(&mut self) -> Result<Expr, FerroError> {
        self.expect(&Token::Dot)?;
        if let Token::Ident(field) = self.advance() {
            Ok(Expr::ParamField(field))
        } else {
            Err(FerroError::QueryParseError(
                "expected field name after params.".into(),
            ))
        }
    }
}

fn compound_assign_op(token: &Token) -> Option<BinOp> {
    match token {
        Token::PlusAssign => Some(BinOp::Add),
        Token::MinusAssign => Some(BinOp::Sub),
        Token::StarAssign => Some(BinOp::Mul),
        Token::SlashAssign => Some(BinOp::Div),
        _ => None,
    }
}

pub fn parse(input: &str) -> Result<Vec<Stmt>, FerroError> {
    let tokens = tokenize(input)?;
    let mut parser = Parser::new(tokens);
    parser.parse_program()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize_arithmetic() {
        let tokens = tokenize("1 + 2 * 3").unwrap();
        assert_eq!(tokens.len(), 6); // 1 + 2 * 3 EOF
    }

    #[test]
    fn test_tokenize_string() {
        let tokens = tokenize("'hello'").unwrap();
        assert_eq!(tokens[0], Token::StringLit("hello".into()));
    }

    #[test]
    fn test_parse_arithmetic() {
        let stmts = parse("1 + 2").unwrap();
        assert_eq!(stmts.len(), 1);
        match &stmts[0] {
            Stmt::Expr(Expr::BinOp(_, BinOp::Add, _)) => {}
            other => panic!("expected BinOp Add, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_doc_field() {
        let stmts = parse("doc['price'].value").unwrap();
        assert_eq!(stmts.len(), 1);
        match &stmts[0] {
            Stmt::Expr(Expr::DocField(f)) => assert_eq!(f, "price"),
            other => panic!("expected DocField, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_ternary() {
        let stmts = parse("true ? 1 : 0").unwrap();
        assert_eq!(stmts.len(), 1);
        match &stmts[0] {
            Stmt::Expr(Expr::Ternary(_, _, _)) => {}
            other => panic!("expected Ternary, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_if_else() {
        let stmts = parse("if (x > 0) { return 1; } else { return 0; }").unwrap();
        assert_eq!(stmts.len(), 1);
        match &stmts[0] {
            Stmt::Expr(Expr::IfElse(_, then_body, Some(else_body))) => {
                assert_eq!(then_body.len(), 1);
                assert_eq!(else_body.len(), 1);
            }
            other => panic!("expected IfElse, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_math_call() {
        let stmts = parse("Math.max(1, 2)").unwrap();
        match &stmts[0] {
            Stmt::Expr(Expr::MathCall(name, args)) => {
                assert_eq!(name, "max");
                assert_eq!(args.len(), 2);
            }
            other => panic!("expected MathCall, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_method_call() {
        let stmts = parse("'hello'.toUpperCase()").unwrap();
        match &stmts[0] {
            Stmt::Expr(Expr::MethodCall(_, method, args)) => {
                assert_eq!(method, "toUpperCase");
                assert!(args.is_empty());
            }
            other => panic!("expected MethodCall, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_for_loop() {
        let stmts = parse("for (int i = 0; i < 10; i = i + 1) { return i; }").unwrap();
        assert_eq!(stmts.len(), 1);
        match &stmts[0] {
            Stmt::Expr(Expr::For(_, _, _, body)) => {
                assert_eq!(body.len(), 1);
            }
            other => panic!("expected For, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_foreach_loop() {
        let stmts = parse("for (def item : items) { return item; }").unwrap();
        assert_eq!(stmts.len(), 1);
        match &stmts[0] {
            Stmt::Expr(Expr::ForEach(var, _, body)) => {
                assert_eq!(var, "item");
                assert_eq!(body.len(), 1);
            }
            other => panic!("expected ForEach, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_regex_find() {
        let stmts = parse("'hello' =~ /hell/").unwrap();
        match &stmts[0] {
            Stmt::Expr(Expr::RegexFind(_, _)) => {}
            other => panic!("expected RegexFind, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_regex_match() {
        let stmts = parse("'hello' ==~ /hello/").unwrap();
        match &stmts[0] {
            Stmt::Expr(Expr::RegexMatchOp(_, _)) => {}
            other => panic!("expected RegexMatchOp, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_type_cast() {
        let stmts = parse("(int) 3.14").unwrap();
        match &stmts[0] {
            Stmt::Expr(Expr::TypeCast(t, _)) => assert_eq!(t, "int"),
            other => panic!("expected TypeCast, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_static_call() {
        let stmts = parse("Integer.parseInt('42')").unwrap();
        match &stmts[0] {
            Stmt::Expr(Expr::StaticCall(cls, method, args)) => {
                assert_eq!(cls, "Integer");
                assert_eq!(method, "parseInt");
                assert_eq!(args.len(), 1);
            }
            other => panic!("expected StaticCall, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_array_literal() {
        let stmts = parse("[1, 2, 3]").unwrap();
        match &stmts[0] {
            Stmt::Expr(Expr::ArrayLit(elements)) => {
                assert_eq!(elements.len(), 3);
            }
            other => panic!("expected ArrayLit, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_var_decl() {
        let stmts = parse("def x = 42;").unwrap();
        match &stmts[0] {
            Stmt::VarDecl(name, _) => assert_eq!(name, "x"),
            other => panic!("expected VarDecl, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_index_access() {
        let stmts = parse("items[0]").unwrap();
        match &stmts[0] {
            Stmt::Expr(Expr::IndexAccess(_, _)) => {}
            other => panic!("expected IndexAccess, got {other:?}"),
        }
    }

    // ---- tokenizer: line comments ----

    #[test]
    fn test_tokenize_line_comment() {
        let tokens = tokenize("1 + 2 // this is a comment\n* 3").unwrap();
        // Should tokenize as: 1 + 2 * 3 EOF
        assert_eq!(tokens.len(), 6);
        assert_eq!(tokens[0], Token::IntLit(1));
        assert_eq!(tokens[3], Token::Star);
    }

    // ---- tokenizer: string escape sequences ----

    #[test]
    fn test_tokenize_string_escapes() {
        let tokens = tokenize(r"'hello\nworld\t!\\'").unwrap();
        match &tokens[0] {
            Token::StringLit(s) => assert_eq!(s, "hello\nworld\t!\\"),
            other => panic!("expected StringLit, got {other:?}"),
        }
    }

    #[test]
    fn test_tokenize_double_quoted_string() {
        let tokens = tokenize(r#""hello""#).unwrap();
        assert_eq!(tokens[0], Token::StringLit("hello".into()));
    }

    // ---- tokenizer: error cases ----

    #[test]
    fn test_tokenize_unterminated_string() {
        let err = tokenize("'unterminated").unwrap_err();
        assert!(err.to_string().contains("unterminated string"));
    }

    #[test]
    fn test_tokenize_unterminated_regex() {
        let err = tokenize("=~ /unterminated").unwrap_err();
        assert!(err.to_string().contains("unterminated regex"));
    }

    #[test]
    fn test_tokenize_unexpected_char() {
        let err = tokenize("1 @ 2").unwrap_err();
        assert!(err.to_string().contains("unexpected character"));
    }

    // ---- tokenizer: float literals ----

    #[test]
    fn test_tokenize_float() {
        let tokens = tokenize("3.25").unwrap();
        assert_eq!(tokens[0], Token::FloatLit(3.25));
    }

    // ---- tokenizer: new keyword ----

    #[test]
    fn test_tokenize_new_keyword() {
        let tokens = tokenize("new ArrayList()").unwrap();
        assert_eq!(tokens[0], Token::New);
    }

    // ---- tokenizer: regex after operator ----

    #[test]
    fn test_tokenize_regex_after_operator() {
        let tokens = tokenize("=~ /pattern/").unwrap();
        assert_eq!(tokens[0], Token::RegexFind);
        assert_eq!(tokens[1], Token::RegexLit("pattern".into()));
    }

    #[test]
    fn test_tokenize_regex_escaped_slash() {
        let tokens = tokenize(r"/hello\/world/").unwrap();
        match &tokens[0] {
            Token::RegexLit(p) => assert_eq!(p, r"hello\/world"),
            other => panic!("expected RegexLit, got {other:?}"),
        }
    }

    // ---- tokenizer: division after value (not regex) ----

    #[test]
    fn test_tokenize_division_after_number() {
        let tokens = tokenize("10 / 2").unwrap();
        assert_eq!(tokens[0], Token::IntLit(10));
        assert_eq!(tokens[1], Token::Slash);
        assert_eq!(tokens[2], Token::IntLit(2));
    }

    // ---- parser: var decl without assignment ----

    #[test]
    fn test_parse_var_decl_no_assignment() {
        let stmts = parse("def x;").unwrap();
        match &stmts[0] {
            Stmt::VarDecl(name, expr) => {
                assert_eq!(name, "x");
                assert_eq!(*expr, Expr::NullLit);
            }
            other => panic!("expected VarDecl, got {other:?}"),
        }
    }

    // ---- parser: int var decl ----

    #[test]
    fn test_parse_int_var_decl() {
        let stmts = parse("int x = 10;").unwrap();
        match &stmts[0] {
            Stmt::VarDecl(name, _) => assert_eq!(name, "x"),
            other => panic!("expected VarDecl, got {other:?}"),
        }
    }

    // ---- parser: else if chain ----

    #[test]
    fn test_parse_else_if() {
        let stmts = parse("if (x > 0) { return 1; } else if (x < 0) { return -1; }").unwrap();
        match &stmts[0] {
            Stmt::Expr(Expr::IfElse(_, _, Some(else_body))) => match &else_body[0] {
                Stmt::Expr(Expr::IfElse(_, _, None)) => {}
                other => panic!("expected inner IfElse, got {other:?}"),
            },
            other => panic!("expected IfElse, got {other:?}"),
        }
    }

    // ---- parser: ctx._source bracket access ----

    #[test]
    fn test_parse_ctx_source_bracket() {
        let stmts = parse("ctx._source['field_name']").unwrap();
        match &stmts[0] {
            Stmt::Expr(Expr::SourceField(f)) => assert_eq!(f, "field_name"),
            other => panic!("expected SourceField, got {other:?}"),
        }
    }

    // ---- parser: ctx._source with method call ----

    #[test]
    fn test_parse_ctx_source_with_method() {
        let stmts = parse("ctx._source.remove('field')").unwrap();
        match &stmts[0] {
            Stmt::Expr(Expr::MethodCall(_, method, args)) => {
                assert_eq!(method, "remove");
                assert_eq!(args.len(), 1);
            }
            other => panic!("expected MethodCall, got {other:?}"),
        }
    }

    // ---- parser: ctx.field (no _source) ----

    #[test]
    fn test_parse_ctx_field_direct() {
        let stmts = parse("ctx.status").unwrap();
        match &stmts[0] {
            Stmt::Expr(Expr::SourceField(f)) => assert_eq!(f, "status"),
            other => panic!("expected SourceField, got {other:?}"),
        }
    }

    // ---- parser: ctx.field.subfield ----

    #[test]
    fn test_parse_ctx_field_nested() {
        let stmts = parse("ctx.task.ownerId").unwrap();
        match &stmts[0] {
            Stmt::Expr(Expr::SourceField(f)) => assert_eq!(f, "task.ownerId"),
            other => panic!("expected SourceField, got {other:?}"),
        }
    }

    // ---- parser: doc['field'] without .value ----

    #[test]
    fn test_parse_doc_access_no_value() {
        let stmts = parse("doc['price']").unwrap();
        match &stmts[0] {
            Stmt::Expr(Expr::DocField(f)) => assert_eq!(f, "price"),
            other => panic!("expected DocField, got {other:?}"),
        }
    }

    // ---- parser: unary neg ----

    #[test]
    fn test_parse_unary_neg() {
        let stmts = parse("-42").unwrap();
        match &stmts[0] {
            Stmt::Expr(Expr::UnaryOp(UnaryOp::Neg, _)) => {}
            other => panic!("expected UnaryOp Neg, got {other:?}"),
        }
    }

    // ---- parser: unary not ----

    #[test]
    fn test_parse_unary_not() {
        let stmts = parse("!true").unwrap();
        match &stmts[0] {
            Stmt::Expr(Expr::UnaryOp(UnaryOp::Not, _)) => {}
            other => panic!("expected UnaryOp Not, got {other:?}"),
        }
    }

    // ---- parser: type cast with Int token ----

    #[test]
    fn test_parse_type_cast_int_token() {
        let stmts = parse("(int) 3.14").unwrap();
        match &stmts[0] {
            Stmt::Expr(Expr::TypeCast(t, _)) => assert_eq!(t, "int"),
            other => panic!("expected TypeCast, got {other:?}"),
        }
    }

    // ---- parser: type cast with other types ----

    #[test]
    fn test_parse_type_cast_double() {
        let stmts = parse("(double) 42").unwrap();
        match &stmts[0] {
            Stmt::Expr(Expr::TypeCast(t, _)) => assert_eq!(t, "double"),
            other => panic!("expected TypeCast double, got {other:?}"),
        }
    }

    // ---- parser: static class as var ----

    #[test]
    fn test_parse_static_class_as_var() {
        let stmts = parse("Integer").unwrap();
        match &stmts[0] {
            Stmt::Expr(Expr::Var(name)) => assert_eq!(name, "Integer"),
            other => panic!("expected Var, got {other:?}"),
        }
    }

    // ---- parser: comparison operators ----

    #[test]
    fn test_parse_gte_lte() {
        let stmts = parse("a >= 1").unwrap();
        match &stmts[0] {
            Stmt::Expr(Expr::BinOp(_, BinOp::Gte, _)) => {}
            other => panic!("expected Gte, got {other:?}"),
        }
        let stmts = parse("a <= 1").unwrap();
        match &stmts[0] {
            Stmt::Expr(Expr::BinOp(_, BinOp::Lte, _)) => {}
            other => panic!("expected Lte, got {other:?}"),
        }
    }

    // ---- parser: multiplication ops ----

    #[test]
    fn test_parse_mul_div_mod() {
        let stmts = parse("a * b / c % d").unwrap();
        assert_eq!(stmts.len(), 1);
    }

    // ---- parser: or expression ----

    #[test]
    fn test_parse_or_expr() {
        let stmts = parse("a || b").unwrap();
        match &stmts[0] {
            Stmt::Expr(Expr::BinOp(_, BinOp::Or, _)) => {}
            other => panic!("expected Or, got {other:?}"),
        }
    }

    // ---- parser: and expression ----

    #[test]
    fn test_parse_and_expr() {
        let stmts = parse("a && b").unwrap();
        match &stmts[0] {
            Stmt::Expr(Expr::BinOp(_, BinOp::And, _)) => {}
            other => panic!("expected And, got {other:?}"),
        }
    }

    // ---- parser: neq expression ----

    #[test]
    fn test_parse_neq() {
        let stmts = parse("a != b").unwrap();
        match &stmts[0] {
            Stmt::Expr(Expr::BinOp(_, BinOp::Neq, _)) => {}
            other => panic!("expected Neq, got {other:?}"),
        }
    }

    // ---- parser: assignment to ctx._source.field ----

    #[test]
    fn test_parse_source_assign() {
        let stmts = parse("ctx._source.status = 'active'").unwrap();
        match &stmts[0] {
            Stmt::Expr(Expr::Assign(field, _)) => assert_eq!(field, "status"),
            other => panic!("expected Assign, got {other:?}"),
        }
    }

    // ---- parser: assignment to ctx._source nested field ----

    #[test]
    fn test_parse_source_assign_nested() {
        let stmts = parse("ctx._source.task.owner = 'bob'").unwrap();
        match &stmts[0] {
            Stmt::Expr(Expr::Assign(field, _)) => assert_eq!(field, "task.owner"),
            other => panic!("expected Assign, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_source_compound_assign() {
        let stmts = parse("ctx._source.count += 1").unwrap();
        match &stmts[0] {
            Stmt::Expr(Expr::CompoundAssign(field, BinOp::Add, _)) => assert_eq!(field, "count"),
            other => panic!("expected CompoundAssign, got {other:?}"),
        }
    }

    // ---- parser: variable assignment ----

    #[test]
    fn test_parse_var_assign() {
        let stmts = parse("x = 42").unwrap();
        match &stmts[0] {
            Stmt::Expr(Expr::VarDecl(name, _)) => assert_eq!(name, "x"),
            other => panic!("expected VarDecl (assign), got {other:?}"),
        }
    }

    #[test]
    fn test_parse_var_compound_assign() {
        let stmts = parse("x *= 2").unwrap();
        match &stmts[0] {
            Stmt::Expr(Expr::VarCompoundAssign(name, BinOp::Mul, _)) => assert_eq!(name, "x"),
            other => panic!("expected VarCompoundAssign, got {other:?}"),
        }
    }

    // ---- parser: postfix field access (not method call) ----

    #[test]
    fn test_parse_postfix_field_access() {
        let stmts = parse("obj.field").unwrap();
        match &stmts[0] {
            Stmt::Expr(Expr::MethodCall(_, field, args)) => {
                assert_eq!(field, "field");
                assert!(args.is_empty());
            }
            other => panic!("expected MethodCall (field access), got {other:?}"),
        }
    }

    // ---- parser: empty array literal ----

    #[test]
    fn test_parse_empty_array() {
        let stmts = parse("[]").unwrap();
        match &stmts[0] {
            Stmt::Expr(Expr::ArrayLit(elems)) => assert!(elems.is_empty()),
            other => panic!("expected ArrayLit, got {other:?}"),
        }
    }

    // ---- parser: parenthesized expression (not cast) ----

    #[test]
    fn test_parse_paren_expr() {
        let stmts = parse("(1 + 2) * 3").unwrap();
        match &stmts[0] {
            Stmt::Expr(Expr::BinOp(_, BinOp::Mul, _)) => {}
            other => panic!("expected Mul, got {other:?}"),
        }
    }

    // ---- parser: ctx._source.field with method call ----

    #[test]
    fn test_parse_ctx_source_method_call() {
        let stmts = parse("ctx._source.toLowerCase()").unwrap();
        match &stmts[0] {
            Stmt::Expr(Expr::MethodCall(_, method, _)) => {
                assert_eq!(method, "toLowerCase");
            }
            other => panic!("expected MethodCall, got {other:?}"),
        }
    }
}
