//! `PromQL` parser — builds an AST from a token stream.

use crate::diagnostics::Span;
use crate::promql::ast::{
    AggregationModifier, AggregationOp, AtModifier, BinaryOp, DurationExpr, DurationOp, Expr,
    LabelMatcher, MatchOp, RangeModifier, UnaryOp, VectorMatching, VectorMatchingCardinality,
};
use crate::promql::lexer::{Token, lex_spanned};

use std::fmt;

/// Parse error with context.
#[derive(Debug, Clone, PartialEq)]
pub struct ParseError {
    /// Error description: one sentence, with the suggestion when there is one.
    pub msg: String,
    /// Index of the token the error is at.
    pub pos: usize,
    /// The bytes of the query the error is about.
    pub span: Option<Span>,
    /// What the query probably meant: `did you mean "rate"?`.
    pub hint: Option<String>,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "parse error: {}", self.msg)
    }
}

impl std::error::Error for ParseError {}

/// Parse a `PromQL` expression string into an AST.
///
/// A failure carries the byte [`span`](ParseError::span) of the token it is
/// at, so a client can draw a caret under it.
pub fn parse(input: &str) -> Result<Expr, ParseError> {
    let (tokens, spans) = lex_spanned(input).map_err(|e| {
        let end = input[e.pos..]
            .chars()
            .next()
            .map_or(e.pos, |c| e.pos + c.len_utf8());
        ParseError {
            msg: e.msg,
            pos: 0,
            span: Some(Span { start: e.pos, end }),
            hint: None,
        }
    })?;
    let mut parser = Parser::new(tokens);
    let result = parser.parse_expr().and_then(|expr| {
        if parser.at_eof() {
            Ok(expr)
        } else {
            Err(parser.error(format!("unexpected token: {}", parser.peek())))
        }
    });
    result.map_err(|mut e| {
        if e.span.is_none() {
            e.span = spans.get(e.pos).or_else(|| spans.last()).copied();
        }
        e
    })
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
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

    fn at_eof(&self) -> bool {
        matches!(self.peek(), Token::Eof)
    }

    fn expect(&mut self, expected: &Token) -> Result<(), ParseError> {
        let tok = self.advance();
        if &tok == expected {
            Ok(())
        } else {
            // At the token found, which `advance` has already stepped past.
            let mut e = self.error(format!(
                "expected {}, found {}",
                quoted(expected),
                quoted(&tok)
            ));
            e.pos -= 1;
            Err(e)
        }
    }

    fn error(&self, msg: String) -> ParseError {
        ParseError {
            msg,
            pos: self.pos,
            span: None,
            hint: None,
        }
    }

    // ── Expression parsing (precedence climbing) ───────────────────────

    fn parse_expr(&mut self) -> Result<Expr, ParseError> {
        self.parse_binary_expr(0)
    }

    fn parse_binary_expr(&mut self, min_prec: u8) -> Result<Expr, ParseError> {
        let mut lhs = self.parse_unary()?;

        loop {
            let op = match self.peek() {
                Token::Plus => BinaryOp::Add,
                Token::Minus => BinaryOp::Sub,
                Token::Star => BinaryOp::Mul,
                Token::Slash => BinaryOp::Div,
                Token::Percent => BinaryOp::Mod,
                Token::Caret => BinaryOp::Pow,
                Token::Eql => BinaryOp::Eql,
                Token::Neq => BinaryOp::Neq,
                Token::Lss => BinaryOp::Lss,
                Token::Gtr => BinaryOp::Gtr,
                Token::Lte => BinaryOp::Lte,
                Token::Gte => BinaryOp::Gte,
                Token::And => BinaryOp::And,
                Token::Or => BinaryOp::Or,
                Token::Unless => BinaryOp::Unless,
                _ => break,
            };

            let prec = op.precedence();
            if prec < min_prec {
                break;
            }

            self.advance(); // consume the operator

            // Check for `bool` modifier on comparison operators
            let bool_mod = if op.is_comparison() && matches!(self.peek(), Token::Bool) {
                self.advance();
                true
            } else {
                false
            };

            // Check for vector matching: on/ignoring, group_left/group_right
            let matching = self.parse_vector_matching()?;

            let next_prec = if op.is_right_assoc() { prec } else { prec + 1 };
            let rhs = self.parse_binary_expr(next_prec)?;

            lhs = Expr::BinaryExpr {
                op,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
                bool_mod,
                matching,
            };
        }

        Ok(lhs)
    }

    fn parse_vector_matching(&mut self) -> Result<Option<VectorMatching>, ParseError> {
        let on = match self.peek() {
            Token::On => {
                self.advance();
                true
            }
            Token::Ignoring => {
                self.advance();
                false
            }
            _ => return Ok(None),
        };

        let labels = self.parse_label_list()?;

        let (card, include) = match self.peek() {
            Token::GroupLeft => {
                self.advance();
                // Optional include labels
                let include = if matches!(self.peek(), Token::LeftParen) {
                    self.parse_label_list()?
                } else {
                    Vec::new()
                };
                (VectorMatchingCardinality::ManyToOne, include)
            }
            Token::GroupRight => {
                self.advance();
                let include = if matches!(self.peek(), Token::LeftParen) {
                    self.parse_label_list()?
                } else {
                    Vec::new()
                };
                (VectorMatchingCardinality::OneToMany, include)
            }
            _ => (VectorMatchingCardinality::OneToOne, Vec::new()),
        };

        Ok(Some(VectorMatching {
            card,
            labels,
            on,
            include,
        }))
    }

    fn parse_label_list(&mut self) -> Result<Vec<String>, ParseError> {
        self.expect(&Token::LeftParen)?;
        let mut labels = Vec::new();
        while !matches!(self.peek(), Token::RightParen | Token::Eof) {
            match self.advance() {
                Token::Ident(name) => labels.push(name),
                other => return Err(self.error(format!("expected label name, got {other}"))),
            }
            if matches!(self.peek(), Token::Comma) {
                self.advance();
            }
        }
        self.expect(&Token::RightParen)?;
        Ok(labels)
    }

    fn parse_unary(&mut self) -> Result<Expr, ParseError> {
        if matches!(self.peek(), Token::Minus) {
            self.advance();
            let expr = self.parse_postfix()?;
            return Ok(Expr::UnaryExpr {
                op: UnaryOp::Neg,
                expr: Box::new(expr),
            });
        }
        if matches!(self.peek(), Token::Plus) {
            self.advance(); // unary plus is a no-op
        }
        self.parse_postfix()
    }

    fn parse_postfix(&mut self) -> Result<Expr, ParseError> {
        let mut expr = self.parse_primary()?;

        // Matrix selector: metric[5m] or subquery: expr[5m:1m]
        if matches!(self.peek(), Token::LeftBracket) {
            self.advance();
            let range = self.parse_duration_expr()?;

            // Check for subquery step: [5m:1m] or [5m:]
            if matches!(self.peek(), Token::Colon) {
                self.advance(); // consume ':'
                let step = if matches!(self.peek(), Token::RightBracket) {
                    None // [5m:] — use the default step
                } else {
                    Some(self.parse_duration_expr()?) // [5m:1m]
                };
                self.expect_closing_bracket()?;
                let mut sub = Expr::Subquery {
                    expr: Box::new(expr),
                    range,
                    step,
                    offset: None,
                    at: None,
                };
                self.parse_subquery_modifiers(&mut sub)?;
                return Ok(sub);
            }

            self.expect_closing_bracket()?;

            // If expr was a VectorSelector, wrap it as a MatrixSelector
            match &expr {
                Expr::VectorSelector { .. } => {
                    // The modifiers bind to the *inner* selector, which is
                    // where the evaluator reads them from — the same shape as
                    // Prometheus, whose `MatrixSelector` delegates to its
                    // `VectorSelector`'s offset and timestamp.
                    let modifier = self.parse_modifiers(&mut expr)?;
                    expr = Expr::MatrixSelector {
                        vector: Box::new(expr),
                        range,
                        modifier,
                    };
                }
                _ => {
                    // Non-vector expression with [range] but no step → subquery
                    // with default step, e.g. rate(http_requests[5m])[30m]
                    let mut sub = Expr::Subquery {
                        expr: Box::new(expr),
                        range,
                        step: None,
                        offset: None,
                        at: None,
                    };
                    self.parse_subquery_modifiers(&mut sub)?;
                    return Ok(sub);
                }
            }
            return Ok(expr);
        }

        // `offset` / `@` / `smoothed` on a bare selector.
        let pos = self.pos;
        match self.parse_modifiers(&mut expr)? {
            None => {}
            Some(RangeModifier::Smoothed) => {
                if let Expr::VectorSelector { smoothed, .. } = &mut expr {
                    *smoothed = true;
                }
            }
            Some(RangeModifier::Anchored) => {
                return Err(ParseError {
                    msg: "`anchored` applies to a range selector: write `metric[5m] anchored`"
                        .into(),
                    pos,
                    span: None,
                    hint: None,
                });
            }
        }

        Ok(expr)
    }

    /// `]`, with a pointed message when a range modifier sits inside it.
    fn expect_closing_bracket(&mut self) -> Result<(), ParseError> {
        if let Token::Ident(word) = self.peek()
            && let Some(m) = range_modifier(word)
        {
            let word = match m {
                RangeModifier::Anchored => "anchored",
                RangeModifier::Smoothed => "smoothed",
            };
            return Err(self.error(format!(
                "`{word}` goes after the closing bracket: `metric[5m] {word}`"
            )));
        }
        self.expect(&Token::RightBracket)
    }

    /// Modifiers after a subquery: `offset` and `@`, never a range modifier.
    fn parse_subquery_modifiers(&mut self, sub: &mut Expr) -> Result<(), ParseError> {
        let pos = self.pos;
        if self.parse_modifiers(sub)?.is_some() {
            return Err(ParseError {
                msg: "extended range selectors (`anchored`, `smoothed`) are not supported on \
                      subqueries"
                    .into(),
                pos,
                span: None,
                hint: None,
            });
        }
        Ok(())
    }

    /// Consume any `offset` and `@` modifiers and attach them to `expr`.
    ///
    /// Both modifiers attach to a **selector or a subquery** and nothing else;
    /// `sum(m) offset 5m` is a parse error, as it is in Prometheus. Accepting
    /// and dropping it would make `rate(m[5m]) offset 5m` run unshifted, which
    /// is a wrong answer wearing the shape of a right one.
    ///
    /// Either modifier may appear at most once, in either order. A range
    /// modifier (`anchored`, `smoothed`) is returned for the caller to place,
    /// because where it is valid depends on what it follows.
    fn parse_modifiers(&mut self, expr: &mut Expr) -> Result<Option<RangeModifier>, ParseError> {
        let mut seen_offset = false;
        let mut seen_at = false;
        let mut range_mod = None;

        loop {
            if let Token::Ident(word) = self.peek()
                && let Some(m) = range_modifier(word)
            {
                if range_mod.is_some() {
                    return Err(self.error("duplicate range modifier".into()));
                }
                range_mod = Some(m);
                self.advance();
                continue;
            }
            match self.peek() {
                Token::Offset => {
                    if seen_offset {
                        return Err(ParseError {
                            msg: "duplicate offset modifier".into(),
                            pos: self.pos,
                            span: None,
                            hint: None,
                        });
                    }
                    seen_offset = true;
                    self.advance();
                    let d = self.parse_offset_duration()?;
                    Self::attach_offset(expr, d, self.pos)?;
                }
                Token::At => {
                    if seen_at {
                        return Err(ParseError {
                            msg: "duplicate @ modifier".into(),
                            pos: self.pos,
                            span: None,
                            hint: None,
                        });
                    }
                    seen_at = true;
                    self.advance();
                    let at = self.parse_at_modifier()?;
                    Self::attach_at(expr, at, self.pos)?;
                }
                _ => return Ok(range_mod),
            }
        }
    }

    fn attach_offset(expr: &mut Expr, d: DurationExpr, pos: usize) -> Result<(), ParseError> {
        match expr {
            Expr::VectorSelector { offset, .. } | Expr::Subquery { offset, .. } => {
                *offset = Some(d);
                Ok(())
            }
            _ => Err(ParseError {
                msg: "offset modifier must be preceded by an instant vector selector or range \
                      vector selector or a subquery"
                    .into(),
                pos,
                span: None,
                hint: None,
            }),
        }
    }

    fn attach_at(expr: &mut Expr, at: AtModifier, pos: usize) -> Result<(), ParseError> {
        match expr {
            Expr::VectorSelector { at: slot, .. } | Expr::Subquery { at: slot, .. } => {
                *slot = Some(at);
                Ok(())
            }
            _ => Err(ParseError {
                msg: "@ modifier must be preceded by an instant vector selector or range vector \
                      selector or a subquery"
                    .into(),
                pos,
                span: None,
                hint: None,
            }),
        }
    }

    /// `@ <unix seconds>`, `@ start()` or `@ end()`.
    fn parse_at_modifier(&mut self) -> Result<AtModifier, ParseError> {
        match self.peek().clone() {
            Token::Number(n) => {
                self.advance();
                Ok(AtModifier::Timestamp(n))
            }
            Token::Minus => {
                self.advance();
                match self.peek().clone() {
                    Token::Number(n) => {
                        self.advance();
                        Ok(AtModifier::Timestamp(-n))
                    }
                    other => Err(ParseError {
                        msg: format!("expected a timestamp after `@ -`, found {other}"),
                        pos: self.pos,
                        span: None,
                        hint: None,
                    }),
                }
            }
            Token::LeftParen => {
                Err(self.error("duration expressions are not supported in the `@` modifier".into()))
            }
            Token::Ident(name) if name == "start" || name == "end" => {
                self.advance();
                self.expect(&Token::LeftParen)?;
                self.expect(&Token::RightParen)?;
                Ok(if name == "start" {
                    AtModifier::Start
                } else {
                    AtModifier::End
                })
            }
            other => Err(ParseError {
                msg: format!("expected a timestamp, start() or end() after `@`, found {other}"),
                pos: self.pos,
                span: None,
                hint: None,
            }),
        }
    }

    /// The duration after `offset`.
    ///
    /// Without parentheses only a signed primary is the offset — a literal, a
    /// number of seconds or a duration function — so `m offset 1h / 2` is
    /// `(m offset 1h) / 2`, as upstream parses it. A whole expression needs
    /// parentheses: `m offset (1h / 2)`.
    fn parse_offset_duration(&mut self) -> Result<DurationExpr, ParseError> {
        match self.peek() {
            Token::Minus => {
                self.advance();
                Ok(DurationExpr::Neg(Box::new(self.parse_offset_duration()?)))
            }
            Token::Plus => {
                self.advance();
                self.parse_offset_duration()
            }
            _ => self.parse_duration_primary(),
        }
    }

    /// A duration expression: arithmetic over durations, numbers (seconds),
    /// `step()`, `range()`, `min_of(a, b)` and `max_of(a, b)`.
    fn parse_duration_expr(&mut self) -> Result<DurationExpr, ParseError> {
        let mut lhs = self.parse_duration_term()?;
        loop {
            let op = match self.peek() {
                Token::Plus => DurationOp::Add,
                Token::Minus => DurationOp::Sub,
                _ => return Ok(lhs),
            };
            self.advance();
            let rhs = self.parse_duration_term()?;
            lhs = DurationExpr::Binary {
                op,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            };
        }
    }

    fn parse_duration_term(&mut self) -> Result<DurationExpr, ParseError> {
        let mut lhs = self.parse_duration_unary()?;
        loop {
            let op = match self.peek() {
                Token::Star => DurationOp::Mul,
                Token::Slash => DurationOp::Div,
                Token::Percent => DurationOp::Mod,
                _ => return Ok(lhs),
            };
            self.advance();
            let rhs = self.parse_duration_unary()?;
            lhs = DurationExpr::Binary {
                op,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            };
        }
    }

    /// Unary minus binds looser than `^`: `-2 ^ 2` is `-(2 ^ 2)`.
    fn parse_duration_unary(&mut self) -> Result<DurationExpr, ParseError> {
        match self.peek() {
            Token::Minus => {
                self.advance();
                Ok(DurationExpr::Neg(Box::new(self.parse_duration_unary()?)))
            }
            Token::Plus => {
                self.advance();
                self.parse_duration_unary()
            }
            _ => self.parse_duration_power(),
        }
    }

    /// `^` is right-associative: `2 ^ 3 ^ 2` is `2 ^ 9`.
    fn parse_duration_power(&mut self) -> Result<DurationExpr, ParseError> {
        let base = self.parse_duration_primary()?;
        if matches!(self.peek(), Token::Caret) {
            self.advance();
            let exp = self.parse_duration_unary()?;
            return Ok(DurationExpr::Binary {
                op: DurationOp::Pow,
                lhs: Box::new(base),
                rhs: Box::new(exp),
            });
        }
        Ok(base)
    }

    fn parse_duration_primary(&mut self) -> Result<DurationExpr, ParseError> {
        match self.peek().clone() {
            Token::Duration(secs) | Token::Number(secs) => {
                self.advance();
                Ok(DurationExpr::Literal(secs))
            }
            Token::LeftParen => {
                self.advance();
                let inner = self.parse_duration_expr()?;
                self.expect(&Token::RightParen)?;
                Ok(inner)
            }
            Token::Ident(name) => {
                let lower = name.to_ascii_lowercase();
                let Some(arity) = duration_function_arity(&lower) else {
                    return Err(self.error(format!(
                        "expected a duration, found `{name}`; a duration is a literal such as \
                         `5m`, a number of seconds, or `step()`, `range()`, `min_of` or `max_of`"
                    )));
                };
                self.advance();
                self.expect(&Token::LeftParen)?;
                let mut args = Vec::with_capacity(arity);
                for i in 0..arity {
                    if i > 0 {
                        self.expect(&Token::Comma)?;
                    }
                    args.push(self.parse_duration_expr()?);
                }
                self.expect(&Token::RightParen)?;
                let mut args = args.into_iter();
                let mut arg = || Box::new(args.next().unwrap_or(DurationExpr::Literal(0.0)));
                Ok(match lower.as_str() {
                    "step" => DurationExpr::Step,
                    "range" => DurationExpr::Range,
                    "min_of" => DurationExpr::MinOf(arg(), arg()),
                    _ => DurationExpr::MaxOf(arg(), arg()),
                })
            }
            other => Err(self.error(format!("expected a duration, found {other}"))),
        }
    }

    fn parse_primary(&mut self) -> Result<Expr, ParseError> {
        match self.peek().clone() {
            // A duration where a float is expected is that many seconds.
            Token::Number(n) | Token::Duration(n) => {
                self.advance();
                Ok(Expr::NumberLiteral(n))
            }
            Token::String(s) => {
                self.advance();
                Ok(Expr::StringLiteral(s))
            }
            Token::LeftParen => {
                self.advance();
                let expr = self.parse_expr()?;
                self.expect(&Token::RightParen)?;
                Ok(Expr::Paren(Box::new(expr)))
            }
            Token::LeftBrace => {
                // {label="value"} selector without metric name
                let matchers = self.parse_label_matchers()?;
                // Prometheus's rule, and it is about matchers rather than
                // about the name: at least one must not be satisfied by an
                // absent label. So `{host="a"}` is legal and reaches every
                // metric carrying that label, while `{}` and `{host=~".*"}`
                // name the whole database and are refused — here, in the
                // parser, so the client sees a 400 `bad_data` rather than a
                // 422 execution error.
                if matchers.iter().all(LabelMatcher::matches_empty) {
                    return Err(ParseError {
                        msg: "vector selector must contain at least one non-empty matcher"
                            .to_string(),
                        pos: self.pos,
                        span: None,
                        hint: None,
                    });
                }
                Ok(Expr::VectorSelector {
                    name: None,
                    matchers,
                    offset: None,
                    at: None,
                    smoothed: false,
                })
            }
            Token::Ident(name) => {
                // Check if it's an aggregation operator
                if let Some(agg_op) = AggregationOp::parse_op(&name) {
                    self.advance();
                    return self.parse_aggregation(agg_op);
                }

                self.advance();

                match self.peek() {
                    Token::LeftParen => {
                        // Function call: func(args...)
                        //
                        // An unknown name is rejected here rather than at
                        // evaluation, because that is where upstream reports
                        // it — a 400 with `errorType: bad_data`, not a 422
                        // execution error. A misspelled function is one of
                        // the most ordinary things a query editor sends, and
                        // clients branch on the difference.
                        let lower = name.to_ascii_lowercase();
                        if duration_function_arity(&lower).is_some() {
                            return Err(ParseError {
                                msg: format!(
                                    "`{lower}()` is a duration function, unsupported as an \
                                     expression; use it inside `[...]` or `offset (...)`"
                                ),
                                pos: self.pos,
                                span: None,
                                hint: None,
                            });
                        }
                        if !crate::promql::eval::function::is_known_function(&name) {
                            // Upstream's wording, so it is the sentence people
                            // search for, then what was probably meant. Every
                            // aggregation operator is a candidate too: `summ(x)`
                            // means `sum`.
                            let candidates = crate::promql::eval::function::FUNCTIONS
                                .iter()
                                .copied()
                                .chain(AggregationOp::NAMES.iter().copied());
                            let hint = crate::diagnostics::suggest(&name, candidates).hint();
                            let msg = match &hint {
                                Some(h) => format!("unknown function with name \"{name}\" — {h}"),
                                None => format!("unknown function with name \"{name}\""),
                            };
                            // The name, not the parenthesis after it.
                            return Err(ParseError {
                                msg,
                                pos: self.pos - 1,
                                span: None,
                                hint,
                            });
                        }
                        self.advance();
                        let args = self.parse_call_args()?;
                        self.expect(&Token::RightParen)?;
                        Ok(Expr::Call { func: name, args })
                    }
                    Token::LeftBrace => {
                        // metric{label="value"}
                        let matchers = self.parse_label_matchers()?;
                        Ok(Expr::VectorSelector {
                            name: Some(name),
                            matchers,
                            offset: None,
                            at: None,
                            smoothed: false,
                        })
                    }
                    _ => {
                        // Plain metric name
                        Ok(Expr::VectorSelector {
                            name: Some(name),
                            matchers: vec![],
                            offset: None,
                            at: None,
                            smoothed: false,
                        })
                    }
                }
            }
            other => Err(self.error(format!("unexpected token: {other}"))),
        }
    }

    fn parse_aggregation(&mut self, op: AggregationOp) -> Result<Expr, ParseError> {
        // Parse optional modifier before or after the expression
        let modifier_before = self.parse_aggregation_modifier()?;

        // Parse parameter for topk, bottomk, quantile, count_values
        let has_param = matches!(
            op,
            AggregationOp::Topk
                | AggregationOp::Bottomk
                | AggregationOp::Quantile
                | AggregationOp::CountValues
        );

        self.expect(&Token::LeftParen)?;

        let (param, expr) = if has_param {
            let p = self.parse_expr()?;
            self.expect(&Token::Comma)?;
            let e = self.parse_expr()?;
            (Some(Box::new(p)), Box::new(e))
        } else {
            let e = self.parse_expr()?;
            (None, Box::new(e))
        };

        self.expect(&Token::RightParen)?;

        let modifier = if modifier_before.is_some() {
            modifier_before
        } else {
            self.parse_aggregation_modifier()?
        };

        Ok(Expr::Aggregation {
            op,
            expr,
            param,
            modifier,
        })
    }

    fn parse_aggregation_modifier(&mut self) -> Result<Option<AggregationModifier>, ParseError> {
        match self.peek() {
            Token::By => {
                self.advance();
                let labels = self.parse_label_list()?;
                Ok(Some(AggregationModifier::By(labels)))
            }
            Token::Without => {
                self.advance();
                let labels = self.parse_label_list()?;
                Ok(Some(AggregationModifier::Without(labels)))
            }
            _ => Ok(None),
        }
    }

    fn parse_call_args(&mut self) -> Result<Vec<Expr>, ParseError> {
        let mut args = Vec::new();
        if matches!(self.peek(), Token::RightParen) {
            return Ok(args);
        }
        args.push(self.parse_expr()?);
        while matches!(self.peek(), Token::Comma) {
            self.advance();
            args.push(self.parse_expr()?);
        }
        Ok(args)
    }

    fn parse_label_matchers(&mut self) -> Result<Vec<LabelMatcher>, ParseError> {
        self.expect(&Token::LeftBrace)?;
        let mut matchers = Vec::new();
        while !matches!(self.peek(), Token::RightBrace | Token::Eof) {
            let name = match self.advance() {
                Token::Ident(n) => n,
                other => return Err(self.error(format!("expected label name, got {other}"))),
            };

            let op = match self.advance() {
                Token::Assign => MatchOp::Equal,
                Token::Neq => MatchOp::NotEqual,
                Token::EqlRegex => MatchOp::RegexMatch,
                Token::NeqRegex => MatchOp::RegexNotMatch,
                other => {
                    return Err(self.error(format!(
                        "expected matcher operator (= != =~ !~), got {other}"
                    )));
                }
            };

            let value = match self.advance() {
                Token::String(s) => s,
                other => return Err(self.error(format!("expected string value, got {other}"))),
            };

            matchers.push(LabelMatcher { name, op, value });

            if matches!(self.peek(), Token::Comma) {
                self.advance();
            }
        }
        self.expect(&Token::RightBrace)?;
        Ok(matchers)
    }
}

/// `anchored` or `smoothed`.
fn range_modifier(word: &str) -> Option<RangeModifier> {
    match word {
        "anchored" => Some(RangeModifier::Anchored),
        "smoothed" => Some(RangeModifier::Smoothed),
        _ => None,
    }
}

/// The duration functions valid inside a duration position, by arity.
fn duration_function_arity(lower: &str) -> Option<usize> {
    match lower {
        "step" | "range" => Some(0),
        "min_of" | "max_of" => Some(2),
        _ => None,
    }
}

/// A token as a message names it: `"]"`, and a string literal as written.
fn quoted(tok: &Token) -> String {
    match tok {
        Token::String(_) => tok.to_string(),
        Token::Eof => "the end of the query".to_string(),
        other => format!("\"{other}\""),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_metric() {
        let expr = parse("http_requests_total").unwrap();
        match expr {
            Expr::VectorSelector {
                name,
                matchers,
                offset,
                at: _,
                smoothed: _,
            } => {
                assert_eq!(name, Some("http_requests_total".into()));
                assert!(matchers.is_empty());
                assert!(offset.is_none());
            }
            _ => panic!("expected VectorSelector, got {expr:?}"),
        }
    }

    #[test]
    fn parse_metric_with_labels() {
        let expr = parse(r#"cpu_usage{host="srv1", cpu!="idle"}"#).unwrap();
        match expr {
            Expr::VectorSelector { name, matchers, .. } => {
                assert_eq!(name, Some("cpu_usage".into()));
                assert_eq!(matchers.len(), 2);
                assert_eq!(matchers[0].name, "host");
                assert_eq!(matchers[0].op, MatchOp::Equal);
                assert_eq!(matchers[0].value, "srv1");
                assert_eq!(matchers[1].name, "cpu");
                assert_eq!(matchers[1].op, MatchOp::NotEqual);
                assert_eq!(matchers[1].value, "idle");
            }
            _ => panic!("expected VectorSelector"),
        }
    }

    #[test]
    fn parse_range_vector() {
        let expr = parse("http_requests_total[5m]").unwrap();
        match expr {
            Expr::MatrixSelector { vector, range, .. } => {
                assert_eq!(range.as_secs(), 300.0);
                match *vector {
                    Expr::VectorSelector { name, .. } => {
                        assert_eq!(name, Some("http_requests_total".into()));
                    }
                    _ => panic!("expected VectorSelector inside matrix"),
                }
            }
            _ => panic!("expected MatrixSelector, got {expr:?}"),
        }
    }

    #[test]
    fn parse_function_call() {
        let expr = parse("rate(http_requests_total[5m])").unwrap();
        match expr {
            Expr::Call { func, args } => {
                assert_eq!(func, "rate");
                assert_eq!(args.len(), 1);
                assert!(matches!(&args[0], Expr::MatrixSelector { .. }));
            }
            _ => panic!("expected Call, got {expr:?}"),
        }
    }

    #[test]
    fn parse_aggregation_with_by() {
        let expr = parse("sum by (host) (rate(requests[5m]))").unwrap();
        match expr {
            Expr::Aggregation { op, modifier, .. } => {
                assert_eq!(op, AggregationOp::Sum);
                assert_eq!(modifier, Some(AggregationModifier::By(vec!["host".into()])));
            }
            _ => panic!("expected Aggregation, got {expr:?}"),
        }
    }

    #[test]
    fn parse_aggregation_without() {
        let expr = parse("avg without (instance) (cpu_usage)").unwrap();
        match expr {
            Expr::Aggregation { op, modifier, .. } => {
                assert_eq!(op, AggregationOp::Avg);
                assert_eq!(
                    modifier,
                    Some(AggregationModifier::Without(vec!["instance".into()]))
                );
            }
            _ => panic!("expected Aggregation"),
        }
    }

    #[test]
    fn parse_binary_expr() {
        let expr = parse("a + b * c").unwrap();
        // Due to precedence, this should be: a + (b * c)
        match expr {
            Expr::BinaryExpr { op, lhs, rhs, .. } => {
                assert_eq!(op, BinaryOp::Add);
                assert!(matches!(*lhs, Expr::VectorSelector { .. }));
                match *rhs {
                    Expr::BinaryExpr { op, .. } => {
                        assert_eq!(op, BinaryOp::Mul);
                    }
                    _ => panic!("expected nested BinaryExpr"),
                }
            }
            _ => panic!("expected BinaryExpr, got {expr:?}"),
        }
    }

    #[test]
    fn parse_comparison_with_bool() {
        let expr = parse("a > bool 10").unwrap();
        match expr {
            Expr::BinaryExpr { op, bool_mod, .. } => {
                assert_eq!(op, BinaryOp::Gtr);
                assert!(bool_mod);
            }
            _ => panic!("expected BinaryExpr"),
        }
    }

    #[test]
    fn parse_offset_modifier() {
        let expr = parse("http_requests_total offset 5m").unwrap();
        match expr {
            Expr::VectorSelector { offset, .. } => {
                assert_eq!(offset.unwrap().as_secs(), 300.0);
            }
            _ => panic!("expected VectorSelector"),
        }
    }

    #[test]
    fn parse_subquery_with_step() {
        // Subqueries with an explicit step.
        let expr = parse("rate(http_requests_total[5m])[30m:1m]").unwrap();
        match expr {
            Expr::Subquery {
                expr,
                range,
                step,
                offset,
                at: _,
            } => {
                assert_eq!(range.as_secs(), 1800.0); // 30m
                assert_eq!(step.unwrap().as_secs(), 60.0); // 1m
                assert!(offset.is_none());
                match *expr {
                    Expr::Call { func, .. } => assert_eq!(func, "rate"),
                    _ => panic!("expected Call inside Subquery"),
                }
            }
            _ => panic!("expected Subquery, got {:?}", expr),
        }
    }

    #[test]
    fn parse_subquery_without_step() {
        // Subqueries without a step, which take the default.
        let expr = parse("rate(http_requests_total[5m])[30m:]").unwrap();
        match expr {
            Expr::Subquery {
                expr,
                range,
                step,
                offset,
                at: _,
            } => {
                assert_eq!(range.as_secs(), 1800.0); // 30m
                assert!(step.is_none()); // default step
                assert!(offset.is_none());
                match *expr {
                    Expr::Call { func, .. } => assert_eq!(func, "rate"),
                    _ => panic!("expected Call inside Subquery"),
                }
            }
            _ => panic!("expected Subquery, got {:?}", expr),
        }
    }

    #[test]
    fn parse_negative_number() {
        let expr = parse("-42").unwrap();
        match expr {
            Expr::UnaryExpr { op, expr } => {
                assert_eq!(op, UnaryOp::Neg);
                match *expr {
                    Expr::NumberLiteral(n) => assert_eq!(n, 42.0),
                    _ => panic!("expected NumberLiteral"),
                }
            }
            _ => panic!("expected UnaryExpr"),
        }
    }

    #[test]
    fn parse_parenthesized() {
        let expr = parse("(a + b) * c").unwrap();
        match expr {
            Expr::BinaryExpr { op, lhs, .. } => {
                assert_eq!(op, BinaryOp::Mul);
                assert!(matches!(*lhs, Expr::Paren(_)));
            }
            _ => panic!("expected BinaryExpr"),
        }
    }

    #[test]
    fn parse_topk() {
        let expr = parse("topk(5, cpu_usage)").unwrap();
        match expr {
            Expr::Aggregation { op, param, .. } => {
                assert_eq!(op, AggregationOp::Topk);
                match *param.unwrap() {
                    Expr::NumberLiteral(n) => assert_eq!(n, 5.0),
                    _ => panic!("expected NumberLiteral"),
                }
            }
            _ => panic!("expected Aggregation"),
        }
    }

    #[test]
    fn parse_nested_functions() {
        let expr = parse("sum(rate(http_requests_total[5m]))").unwrap();
        match expr {
            Expr::Aggregation { op, expr, .. } => {
                assert_eq!(op, AggregationOp::Sum);
                assert!(matches!(*expr, Expr::Call { .. }));
            }
            _ => panic!("expected Aggregation"),
        }
    }

    #[test]
    fn parse_complex_query() {
        // A real-world query: rate with aggregation and binary op
        let expr =
            parse(r#"sum by (job) (rate(http_requests_total{status=~"5.."}[5m])) > 0.5"#).unwrap();
        match expr {
            Expr::BinaryExpr { op, .. } => {
                assert_eq!(op, BinaryOp::Gtr);
            }
            _ => panic!("expected BinaryExpr at top level"),
        }
    }

    #[test]
    fn parse_error_message() {
        let err = parse("sum(").unwrap_err();
        assert!(err.msg.contains("unexpected"));
    }

    #[test]
    fn parse_vector_matching() {
        let expr = parse("a / on (instance) group_left b").unwrap();
        match expr {
            Expr::BinaryExpr { matching, .. } => {
                let m = matching.unwrap();
                assert!(m.on);
                assert_eq!(m.labels, vec!["instance".to_string()]);
                assert_eq!(m.card, VectorMatchingCardinality::ManyToOne);
            }
            _ => panic!("expected BinaryExpr"),
        }
    }

    // ── Duration expressions (stable in Prometheus 3.14) ─────────────────

    /// The range of `m[<expr>]`, resolved with no step and no range.
    fn range_of(q: &str) -> f64 {
        match parse(q).unwrap() {
            Expr::Call { args, .. } => match &args[0] {
                Expr::MatrixSelector { range, .. } => range.resolve(0.0, 0.0).unwrap(),
                other => panic!("expected a range selector, got {other:?}"),
            },
            Expr::MatrixSelector { range, .. } => range.resolve(0.0, 0.0).unwrap(),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn duration_expressions_follow_arithmetic_precedence() {
        assert_eq!(range_of("rate(m[2m*(10+5)])"), 1800.0);
        assert_eq!(range_of("rate(m[1h30m % 1h])"), 1800.0);
        assert_eq!(range_of("rate(m[30m1s-30m1s % 1m])"), 1800.0);
        assert_eq!(range_of("rate(m[-5m+35m])"), 1800.0);
        // `^` is right-associative and binds tighter than unary minus.
        assert_eq!(range_of("rate(m[2^3^1 * 1s])"), 8.0);
        assert_eq!(range_of("rate(m[-2^2 + 10])"), 6.0);
    }

    #[test]
    fn duration_functions_resolve_against_the_query() {
        let Expr::Call { args, .. } = parse("rate(m[min_of(step()+1, 1h)])").unwrap() else {
            panic!()
        };
        let Expr::MatrixSelector { range, .. } = &args[0] else {
            panic!()
        };
        assert_eq!(range.resolve(5.0, 10.0).unwrap(), 6.0);
        // Case-insensitive, as upstream.
        assert!(parse("rate(m[STep()])").is_ok());
        assert!(parse("max_over_time(m[range():1s])").is_ok());
    }

    #[test]
    fn an_unparenthesised_offset_takes_one_primary() {
        // `m offset 1h / 2` is `(m offset 1h) / 2`.
        match parse("m offset 1h / 2").unwrap() {
            Expr::BinaryExpr { op, lhs, .. } => {
                assert_eq!(op, BinaryOp::Div);
                assert!(matches!(
                    *lhs,
                    Expr::VectorSelector {
                        offset: Some(_),
                        ..
                    }
                ));
            }
            other => panic!("expected a division, got {other:?}"),
        }
        assert!(parse("m offset (1h / 2)").is_ok());
        assert!(parse("m offset min_of(step(), 1s) + 8000").is_ok());
        assert!(parse("m offset 100").is_ok(), "a bare number of seconds");
    }

    #[test]
    fn duration_functions_are_refused_outside_a_duration() {
        let err = parse("step()").unwrap_err();
        assert!(err.msg.contains("duration function"), "{}", err.msg);
        let err = parse("m @ (start() + 1)").unwrap_err();
        assert!(
            err.msg.contains("not supported in the `@` modifier"),
            "{}",
            err.msg
        );
    }

    // ── Extended range selectors (stable in Prometheus 3.16) ─────────────

    #[test]
    fn range_modifiers_attach_to_the_selector_they_follow() {
        match parse("rate(m[1m] anchored)").unwrap() {
            Expr::Call { args, .. } => assert!(matches!(
                args[0],
                Expr::MatrixSelector {
                    modifier: Some(RangeModifier::Anchored),
                    ..
                }
            )),
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            parse("m smoothed").unwrap(),
            Expr::VectorSelector { smoothed: true, .. }
        ));
        assert!(parse("m offset -100 smoothed + 0").is_ok());
    }

    #[test]
    fn range_modifiers_are_refused_where_they_have_no_meaning() {
        let err = parse("rate(m[5m:1m] smoothed)").unwrap_err();
        assert!(
            err.msg.contains("not supported on subqueries"),
            "{}",
            err.msg
        );
        let err = parse("m anchored").unwrap_err();
        assert!(err.msg.contains("range selector"), "{}", err.msg);
        let err = parse("rate(m[5m smoothed])").unwrap_err();
        assert!(
            err.msg.contains("goes after the closing bracket"),
            "{}",
            err.msg
        );
    }

    #[test]
    fn modifier_words_remain_valid_metric_names() {
        assert!(parse(r#"anchored{job="x"}"#).is_ok());
        assert!(parse("smoothed").is_ok());
    }
}
