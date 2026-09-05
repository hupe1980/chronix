//! `PromQL` parser — builds an AST from a token stream.

use crate::promql::ast::{
    AggregationModifier, AggregationOp, AtModifier, BinaryOp, Duration, Expr, LabelMatcher,
    MatchOp, UnaryOp, VectorMatching, VectorMatchingCardinality,
};
use crate::promql::lexer::{lex, Token};

use std::fmt;

/// Parse error with context.
#[derive(Debug, Clone, PartialEq)]
pub struct ParseError {
    /// Error description.
    pub msg: String,
    /// Token position where the error occurred.
    pub pos: usize,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "parse error at token {}: {}", self.pos, self.msg)
    }
}

impl std::error::Error for ParseError {}

/// Parse a `PromQL` expression string into an AST.
pub fn parse(input: &str) -> Result<Expr, ParseError> {
    let tokens = lex(input).map_err(|e| ParseError {
        msg: e.msg,
        pos: e.pos,
    })?;
    let mut parser = Parser::new(tokens);
    let expr = parser.parse_expr()?;
    if !parser.at_eof() {
        return Err(parser.error(format!("unexpected token: {}", parser.peek())));
    }
    Ok(expr)
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
            Err(self.error(format!("expected {expected}, got {tok}")))
        }
    }

    fn error(&self, msg: String) -> ParseError {
        ParseError { msg, pos: self.pos }
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
            let range = self.parse_duration()?;

            // Check for subquery step: [5m:1m] or [5m:]
            if matches!(self.peek(), Token::Colon) {
                self.advance(); // consume ':'
                let step = if matches!(self.peek(), Token::RightBracket) {
                    None // [5m:] — use the default step
                } else {
                    Some(self.parse_duration()?) // [5m:1m]
                };
                self.expect(&Token::RightBracket)?;
                let mut sub = Expr::Subquery {
                    expr: Box::new(expr),
                    range,
                    step,
                    offset: None,
                    at: None,
                };
                self.parse_modifiers(&mut sub)?;
                return Ok(sub);
            }

            self.expect(&Token::RightBracket)?;

            // If expr was a VectorSelector, wrap it as a MatrixSelector
            match &expr {
                Expr::VectorSelector { .. } => {
                    // The modifiers bind to the *inner* selector, which is
                    // where the evaluator reads them from — the same shape as
                    // Prometheus, whose `MatrixSelector` delegates to its
                    // `VectorSelector`'s offset and timestamp.
                    self.parse_modifiers(&mut expr)?;
                    expr = Expr::MatrixSelector {
                        vector: Box::new(expr),
                        range,
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
                    self.parse_modifiers(&mut sub)?;
                    return Ok(sub);
                }
            }
        }

        // `offset` / `@` on a bare selector.
        self.parse_modifiers(&mut expr)?;

        Ok(expr)
    }

    /// Consume any `offset` and `@` modifiers and attach them to `expr`.
    ///
    /// Both modifiers attach to a **selector or a subquery** and nothing else;
    /// `sum(m) offset 5m` is a parse error, as it is in Prometheus. Accepting
    /// and dropping it would make `rate(m[5m]) offset 5m` run unshifted, which
    /// is a wrong answer wearing the shape of a right one.
    ///
    /// Either modifier may appear at most once, in either order.
    fn parse_modifiers(&mut self, expr: &mut Expr) -> Result<(), ParseError> {
        let mut seen_offset = false;
        let mut seen_at = false;

        loop {
            match self.peek() {
                Token::Offset => {
                    if seen_offset {
                        return Err(ParseError {
                            msg: "duplicate offset modifier".into(),
                            pos: self.pos,
                        });
                    }
                    seen_offset = true;
                    self.advance();
                    let d = self.parse_signed_duration()?;
                    Self::attach_offset(expr, d, self.pos)?;
                }
                Token::At => {
                    if seen_at {
                        return Err(ParseError {
                            msg: "duplicate @ modifier".into(),
                            pos: self.pos,
                        });
                    }
                    seen_at = true;
                    self.advance();
                    let at = self.parse_at_modifier()?;
                    Self::attach_at(expr, at, self.pos)?;
                }
                _ => return Ok(()),
            }
        }
    }

    fn attach_offset(expr: &mut Expr, d: Duration, pos: usize) -> Result<(), ParseError> {
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
                    }),
                }
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
            }),
        }
    }

    /// A duration that may be negative.
    ///
    /// `offset -5m` shifts the window **forward**, which is how a query
    /// compares a value against one from the future of its own evaluation
    /// time — used by recording rules that backfill, and unconditional in
    /// Prometheus 3.x.
    fn parse_signed_duration(&mut self) -> Result<Duration, ParseError> {
        if matches!(self.peek(), Token::Minus) {
            self.advance();
            let d = self.parse_duration()?;
            return Ok(Duration(-d.0));
        }
        self.parse_duration()
    }

    fn parse_primary(&mut self) -> Result<Expr, ParseError> {
        match self.peek().clone() {
            Token::Number(n) => {
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
                    });
                }
                Ok(Expr::VectorSelector {
                    name: None,
                    matchers,
                    offset: None,
                    at: None,
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
                        if !crate::promql::eval::function::is_known_function(&name) {
                            return Err(ParseError {
                                msg: format!("unknown function with name \"{name}\""),
                                pos: self.pos,
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
                        })
                    }
                    _ => {
                        // Plain metric name
                        Ok(Expr::VectorSelector {
                            name: Some(name),
                            matchers: vec![],
                            offset: None,
                            at: None,
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

    /// Parse a `PromQL` duration like `5m`, `1h30m`, `2d`.
    fn parse_duration(&mut self) -> Result<Duration, ParseError> {
        // A duration is a sequence of <number><unit> pairs
        let mut total_secs = 0.0;
        let mut parsed_any = false;

        while let Token::Number(n) = self.peek() {
            let n = *n;
            self.advance();
            // Next should be an ident that's a duration unit
            if let Token::Ident(unit) = self.peek() {
                let unit = unit.clone();
                let mult = duration_unit_to_secs(&unit)
                    .ok_or_else(|| self.error(format!("invalid duration unit: {unit}")))?;
                total_secs += n * mult;
                self.advance();
                parsed_any = true;
            } else {
                if parsed_any {
                    // The number isn't part of the duration (put it back by decrementing pos)
                    self.pos -= 1;
                    break;
                }
                return Err(self.error("expected duration unit after number".to_string()));
            }
        }

        if !parsed_any {
            // Maybe the duration is a single ident like "5m" already split into Number(5) + Ident("m")
            // That's handled above. If we're here, it's an error.
            return Err(self.error("expected duration".to_string()));
        }

        Ok(Duration(total_secs))
    }
}

/// Convert a duration unit string to seconds multiplier.
fn duration_unit_to_secs(unit: &str) -> Option<f64> {
    match unit {
        "ms" => Some(0.001),
        "s" => Some(1.0),
        "m" => Some(60.0),
        "h" => Some(3600.0),
        "d" => Some(86400.0),
        "w" => Some(604800.0),
        "y" => Some(31536000.0),
        _ => None,
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
            Expr::MatrixSelector { vector, range } => {
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
}
