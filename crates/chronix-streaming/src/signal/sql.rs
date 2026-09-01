//! Signal SQL extensions — `CREATE`, `SHOW`, `DROP`, `ALTER` TRIGGER
//! SQL grammar, parser, executor, and catalog persistence.
//!
//! ## Supported SQL
//!
//! ```sql
//! CREATE TRIGGER alert_cpu ON cpu_usage
//!   WHEN anomaly_score > 3.0
//!   DELIVER webhook('https://...')
//!   COOLDOWN INTERVAL '5m';
//!
//! SHOW TRIGGERS;
//!
//! DROP TRIGGER alert_cpu;
//!
//! ALTER TRIGGER alert_cpu DISABLE;
//! ALTER TRIGGER alert_cpu ENABLE;
//! ```
//!
//! ## Compound conditions
//!
//! The `WHEN` clause supports compound conditions with `AND` / `OR`
//! and parenthesised grouping.  `AND` binds tighter than `OR`.
//!
//! ```sql
//! CREATE TRIGGER compound ON cpu
//!   WHEN value > 90.0 AND rate < 5.0
//!   DELIVER log;
//!
//! CREATE TRIGGER grouped ON cpu
//!   WHEN (value > 100 AND rate < 5) OR anomaly_score > 3.0
//!   DELIVER log;
//! ```

use std::collections::HashMap;
use std::net::IpAddr;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::signal::engine::TriggerEngine;
use crate::signal::error::{Result, SignalError};
use crate::signal::model::{EventTrigger, ThresholdOp, TriggerCondition};

// ── SQL AST ─────────────────────────────────────────────────────────

/// A parsed trigger SQL statement.
#[derive(Debug, Clone, PartialEq)]
pub enum TriggerStatement {
    /// `CREATE TRIGGER <name> ON <measurement> WHEN <condition> [DELIVER <channels>] [COOLDOWN INTERVAL '<dur>']`
    Create(CreateTrigger),
    /// `SHOW TRIGGERS`
    Show,
    /// `DROP TRIGGER <name>`
    Drop(String),
    /// `ALTER TRIGGER <name> ENABLE|DISABLE`
    Alter(AlterTrigger),
}

/// CREATE TRIGGER details.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTrigger {
    /// Trigger name / ID.
    pub name: String,
    /// Measurement to watch.
    pub measurement: String,
    /// Trigger condition.
    pub condition: ParsedCondition,
    /// Delivery channels.
    pub deliver: Vec<DeliveryTarget>,
    /// Optional cooldown.
    pub cooldown: Option<Duration>,
}

/// Parsed condition from the WHEN clause.
#[derive(Debug, Clone, PartialEq)]
pub enum ParsedCondition {
    /// `anomaly_score <op> <value>`
    AnomalyScore {
        /// Comparison operator.
        op: ThresholdOp,
        /// Threshold value.
        value: f64,
    },
    /// `forecast_deviation <op> <value>`
    ForecastDeviation {
        /// Comparison operator.
        op: ThresholdOp,
        /// Deviation value.
        value: f64,
    },
    /// `<field> <op> <value>`
    FieldThreshold {
        /// Field name.
        field: String,
        /// Comparison operator.
        op: ThresholdOp,
        /// Threshold value.
        value: f64,
    },
    /// Conjunction: both conditions must hold.
    And(Box<ParsedCondition>, Box<ParsedCondition>),
    /// Disjunction: at least one condition must hold.
    Or(Box<ParsedCondition>, Box<ParsedCondition>),
}

/// A delivery target from the DELIVER clause.
#[derive(Debug, Clone, PartialEq)]
pub enum DeliveryTarget {
    /// `webhook('url')`
    Webhook(String),
    /// `nats('subject')`
    Nats(String),
    /// `mqtt('topic')`
    Mqtt(String),
    /// `log`
    Log,
}

/// ALTER TRIGGER sub-command.
#[derive(Debug, Clone, PartialEq)]
pub enum AlterTrigger {
    /// Enable a trigger.
    Enable(String),
    /// Disable a trigger.
    Disable(String),
}

// ── TriggerInfo ─────────────────────────────────────────────────────

/// Information about a registered trigger (for SHOW TRIGGERS).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerInfo {
    /// Trigger ID.
    pub id: String,
    /// Trigger name (display).
    pub name: String,
    /// Measurement.
    pub measurement: String,
    /// Whether enabled.
    pub enabled: bool,
    /// Condition description.
    pub condition: String,
    /// Delivery targets.
    pub delivery: Vec<String>,
    /// Cooldown duration.
    pub cooldown: Option<String>,
}

// ── Parser ──────────────────────────────────────────────────────────

/// Parse a trigger SQL statement.
///
/// # Errors
///
/// Returns an error if the SQL is malformed.
pub fn parse_trigger_sql(sql: &str) -> Result<TriggerStatement> {
    let sql = sql.trim();
    let tokens = tokenize(sql);

    if tokens.is_empty() {
        return Err(SignalError::InvalidConfig("Empty SQL".into()));
    }

    match tokens[0].to_uppercase().as_str() {
        "CREATE" => parse_create(&tokens),
        "SHOW" => parse_show(&tokens),
        "DROP" => parse_drop(&tokens),
        "ALTER" => parse_alter(&tokens),
        other => Err(SignalError::InvalidConfig(format!(
            "Unexpected keyword: {other}"
        ))),
    }
}

/// Tokenize a trigger SQL statement into a list of string tokens.
///
/// Double-quoted (`"my-trigger"`) and backtick-quoted (`` `my-trigger` ``)
/// identifiers are prefixed with a `\x01` sentinel byte so that downstream
/// parsers can distinguish quoted identifiers from bare keywords.  Use
/// [`strip_quote_marker`] to remove the sentinel and recover the clean name.
fn tokenize(sql: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut chars = sql.chars().peekable();

    while let Some(&ch) = chars.peek() {
        if ch.is_whitespace() {
            chars.next();
            continue;
        }
        // Single-quoted strings (values)
        if ch == '\'' {
            chars.next(); // skip opening quote
            let mut s = String::new();
            while let Some(&c) = chars.peek() {
                if c == '\'' {
                    chars.next();
                    break;
                }
                s.push(c);
                chars.next();
            }
            tokens.push(format!("'{s}'"));
            continue;
        }
        // Double-quoted identifiers (e.g. "my-trigger", "cpu.usage")
        // Prefixed with \x01 so downstream parsers can detect quoting.
        if ch == '"' {
            chars.next(); // skip opening quote
            let mut s = String::from('\x01');
            while let Some(&c) = chars.peek() {
                if c == '"' {
                    chars.next();
                    // Check for escaped double-quote ("")
                    if chars.peek() == Some(&'"') {
                        s.push('"');
                        chars.next();
                    } else {
                        break;
                    }
                } else {
                    s.push(c);
                    chars.next();
                }
            }
            tokens.push(s);
            continue;
        }
        // Backtick-quoted identifiers (e.g. `my-trigger`, `cpu.usage`)
        // Prefixed with \x01 so downstream parsers can detect quoting.
        if ch == '`' {
            chars.next(); // skip opening backtick
            let mut s = String::from('\x01');
            while let Some(&c) = chars.peek() {
                if c == '`' {
                    chars.next();
                    break;
                }
                s.push(c);
                chars.next();
            }
            tokens.push(s);
            continue;
        }
        if ch == '(' || ch == ')' || ch == ',' || ch == ';' {
            tokens.push(ch.to_string());
            chars.next();
            continue;
        }
        if ch == '>' || ch == '<' || ch == '=' || ch == '!' {
            let mut op = String::new();
            op.push(ch);
            chars.next();
            if let Some(&next) = chars.peek() {
                if next == '=' {
                    op.push(next);
                    chars.next();
                }
            }
            tokens.push(op);
            continue;
        }
        // Negative number: '-' followed by a digit or '.'
        if ch == '-' {
            let prev_is_operator = tokens.last().is_none_or(|t| {
                matches!(
                    t.as_str(),
                    ">" | ">=" | "<" | "<=" | "==" | "=" | "!=" | "(" | ","
                )
            });
            chars.next();
            if prev_is_operator {
                if let Some(&next) = chars.peek() {
                    if next.is_ascii_digit() || next == '.' {
                        let mut word = String::from('-');
                        while let Some(&c) = chars.peek() {
                            if c.is_whitespace() || c == '(' || c == ')' || c == ',' || c == ';' {
                                break;
                            }
                            word.push(c);
                            chars.next();
                        }
                        tokens.push(word);
                        continue;
                    }
                }
            }
            tokens.push("-".to_string());
            continue;
        }
        // Identifier or number
        let mut word = String::new();
        while let Some(&c) = chars.peek() {
            if c.is_whitespace()
                || c == '('
                || c == ')'
                || c == ','
                || c == ';'
                || c == '\''
                || c == '>'
                || c == '<'
                || c == '='
                || c == '!'
            {
                break;
            }
            word.push(c);
            chars.next();
        }
        if !word.is_empty() {
            tokens.push(word);
        }
    }

    tokens
}

/// Returns `true` if `s` is a valid SQL-style identifier: non-empty,
/// starts with an ASCII letter or underscore, and contains only
/// ASCII alphanumeric characters and underscores.
fn is_valid_identifier(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Strip the `\x01` quoting sentinel prepended by [`tokenize`] for
/// double-quoted and backtick-quoted identifiers.
///
/// Returns `(clean_identifier, was_quoted)`.  Unquoted tokens are
/// returned unchanged with `was_quoted = false`.
fn strip_quote_marker(s: &str) -> (String, bool) {
    if let Some(rest) = s.strip_prefix('\x01') {
        (rest.to_string(), true)
    } else {
        (s.to_string(), false)
    }
}

fn parse_create(tokens: &[String]) -> Result<TriggerStatement> {
    // CREATE TRIGGER <name> ON <measurement> WHEN <condition> [DELIVER ...] [COOLDOWN ...]
    expect_keyword(tokens, 0, "CREATE")?;
    expect_keyword(tokens, 1, "TRIGGER")?;

    let raw_name = get_token(tokens, 2)?;
    let (name, name_quoted) = strip_quote_marker(&raw_name);
    if name.trim().is_empty() || name.chars().all(char::is_whitespace) {
        return Err(SignalError::InvalidConfig(
            "Trigger name must not be empty or whitespace".into(),
        ));
    }
    if !name_quoted && !is_valid_identifier(&name) {
        return Err(SignalError::InvalidConfig(format!(
            "Invalid trigger name '{name}': must start with a letter or underscore \
             and contain only ASCII alphanumeric characters and underscores"
        )));
    }
    expect_keyword(tokens, 3, "ON")?;
    let raw_measurement = get_token(tokens, 4)?;
    let (measurement, _) = strip_quote_marker(&raw_measurement);
    expect_keyword(tokens, 5, "WHEN")?;

    // Parse condition expression (supports AND/OR/parentheses).
    let (condition, next_pos) = parse_condition(tokens, 6)?;

    // Parse optional DELIVER and COOLDOWN
    let mut deliver = Vec::new();
    let mut cooldown = None;
    let mut i = next_pos;

    while i < tokens.len() {
        let token = tokens[i].to_uppercase();
        match token.as_str() {
            "DELIVER" => {
                i += 1;
                while i < tokens.len() {
                    let t = &tokens[i];
                    if t == "," {
                        i += 1;
                        continue;
                    }
                    let upper = t.to_uppercase();
                    if upper == "COOLDOWN" || upper == ";" {
                        break;
                    }
                    if upper == "LOG" {
                        deliver.push(DeliveryTarget::Log);
                        i += 1;
                        continue;
                    }
                    // Function call: webhook('url')
                    if i + 2 < tokens.len() && tokens[i + 1] == "(" {
                        let func = t.to_lowercase();
                        let arg = strip_quotes(&tokens[i + 2]);
                        // expect closing paren
                        i += 4; // func ( 'arg' )
                        match func.as_str() {
                            "webhook" => {
                                validate_webhook_url(&arg)?;
                                deliver.push(DeliveryTarget::Webhook(arg));
                            }
                            "nats" => deliver.push(DeliveryTarget::Nats(arg)),
                            "mqtt" => deliver.push(DeliveryTarget::Mqtt(arg)),
                            other => {
                                return Err(SignalError::InvalidConfig(format!(
                                    "Unknown delivery: {other}"
                                )));
                            }
                        }
                        continue;
                    }
                    i += 1;
                }
            }
            "COOLDOWN" => {
                // COOLDOWN INTERVAL '<duration>'
                expect_keyword(tokens, i + 1, "INTERVAL")?;
                let dur_str = strip_quotes(&get_token(tokens, i + 2)?);
                cooldown = Some(parse_duration(&dur_str)?);
                i += 3;
            }
            ";" => {
                i += 1;
            }
            _ => {
                i += 1;
            }
        }
    }

    Ok(TriggerStatement::Create(CreateTrigger {
        name,
        measurement,
        condition,
        deliver,
        cooldown,
    }))
}

fn parse_show(tokens: &[String]) -> Result<TriggerStatement> {
    expect_keyword(tokens, 0, "SHOW")?;
    expect_keyword(tokens, 1, "TRIGGERS")?;
    Ok(TriggerStatement::Show)
}

fn parse_drop(tokens: &[String]) -> Result<TriggerStatement> {
    expect_keyword(tokens, 0, "DROP")?;
    expect_keyword(tokens, 1, "TRIGGER")?;
    let raw = get_token(tokens, 2)?;
    let (name, _) = strip_quote_marker(&raw);
    Ok(TriggerStatement::Drop(name))
}

fn parse_alter(tokens: &[String]) -> Result<TriggerStatement> {
    expect_keyword(tokens, 0, "ALTER")?;
    expect_keyword(tokens, 1, "TRIGGER")?;
    let raw = get_token(tokens, 2)?;
    let (name, _) = strip_quote_marker(&raw);
    let action = get_token(tokens, 3)?;

    match action.to_uppercase().as_str() {
        "ENABLE" => Ok(TriggerStatement::Alter(AlterTrigger::Enable(name))),
        "DISABLE" => Ok(TriggerStatement::Alter(AlterTrigger::Disable(name))),
        other => Err(SignalError::InvalidConfig(format!(
            "Expected ENABLE or DISABLE, got: {other}"
        ))),
    }
}

fn expect_keyword(tokens: &[String], idx: usize, expected: &str) -> Result<()> {
    let token = get_token(tokens, idx)?;
    if token.to_uppercase() != expected {
        return Err(SignalError::InvalidConfig(format!(
            "Expected {expected}, got: {token}"
        )));
    }
    Ok(())
}

fn get_token(tokens: &[String], idx: usize) -> Result<String> {
    tokens
        .get(idx)
        .cloned()
        .ok_or_else(|| SignalError::InvalidConfig(format!("Unexpected end of SQL at token {idx}")))
}

fn parse_op(s: &str) -> Result<ThresholdOp> {
    match s {
        ">" => Ok(ThresholdOp::Gt),
        ">=" => Ok(ThresholdOp::Gte),
        "<" => Ok(ThresholdOp::Lt),
        "<=" => Ok(ThresholdOp::Lte),
        "==" | "=" => Ok(ThresholdOp::Eq),
        other => Err(SignalError::InvalidConfig(format!(
            "Unknown operator: {other}"
        ))),
    }
}

fn strip_quotes(s: &str) -> String {
    s.trim_matches('\'').to_string()
}

// ── Condition parser (recursive-descent with precedence) ────────────

/// Parse a condition expression.  OR has the lowest precedence, AND
/// binds tighter, and parentheses override both.
///
/// Returns `(condition, next_token_index)`.
fn parse_condition(tokens: &[String], pos: usize) -> Result<(ParsedCondition, usize)> {
    parse_or_expr(tokens, pos)
}

/// `or_expr  ::= and_expr ( 'OR' and_expr )*`
fn parse_or_expr(tokens: &[String], pos: usize) -> Result<(ParsedCondition, usize)> {
    let (mut left, mut pos) = parse_and_expr(tokens, pos)?;
    while pos < tokens.len() && tokens[pos].eq_ignore_ascii_case("OR") {
        let (right, next) = parse_and_expr(tokens, pos + 1)?;
        left = ParsedCondition::Or(Box::new(left), Box::new(right));
        pos = next;
    }
    Ok((left, pos))
}

/// `and_expr ::= primary ( 'AND' primary )*`
fn parse_and_expr(tokens: &[String], pos: usize) -> Result<(ParsedCondition, usize)> {
    let (mut left, mut pos) = parse_primary_condition(tokens, pos)?;
    while pos < tokens.len() && tokens[pos].eq_ignore_ascii_case("AND") {
        let (right, next) = parse_primary_condition(tokens, pos + 1)?;
        left = ParsedCondition::And(Box::new(left), Box::new(right));
        pos = next;
    }
    Ok((left, pos))
}

/// `primary  ::= '(' or_expr ')' | atom`
///
/// `atom     ::= <field> <op> <value>`
fn parse_primary_condition(tokens: &[String], pos: usize) -> Result<(ParsedCondition, usize)> {
    // Parenthesised sub-expression
    if pos < tokens.len() && tokens[pos] == "(" {
        let (cond, next) = parse_or_expr(tokens, pos + 1)?;
        if next >= tokens.len() || tokens[next] != ")" {
            return Err(SignalError::InvalidConfig("Expected closing ')'".into()));
        }
        return Ok((cond, next + 1));
    }

    // Atomic condition: field op value
    let raw_field = get_token(tokens, pos)?;
    let (field, quoted) = strip_quote_marker(&raw_field);
    if !quoted && !is_valid_identifier(&field) {
        return Err(SignalError::InvalidConfig(format!(
            "Invalid condition field '{field}': must start with a letter or underscore \
             and contain only ASCII alphanumeric characters and underscores"
        )));
    }
    let op_str = get_token(tokens, pos + 1)?;
    let value_str = get_token(tokens, pos + 2)?;

    let op = parse_op(&op_str)?;
    let value: f64 = value_str
        .parse()
        .map_err(|_| SignalError::InvalidConfig(format!("Invalid number: {value_str}")))?;

    let condition = match field.to_lowercase().as_str() {
        "anomaly_score" => ParsedCondition::AnomalyScore { op, value },
        "forecast_deviation" => ParsedCondition::ForecastDeviation { op, value },
        other => ParsedCondition::FieldThreshold {
            field: other.to_string(),
            op,
            value,
        },
    };

    Ok((condition, pos + 3))
}

/// Validate webhook URLs to prevent SSRF attacks.
///
/// Requirements:
/// Must use `https` scheme (or `http` only for `localhost` in dev).
/// Host must not resolve to a private/loopback/link-local IP.
fn validate_webhook_url(raw: &str) -> Result<()> {
    // Very basic URL parsing — extract scheme and host without adding
    // a full `url` crate dependency.
    let (scheme, rest) = raw.split_once("://").ok_or_else(|| {
        SignalError::InvalidConfig("Webhook URL must include a scheme (https://)".into())
    })?;

    let scheme_lower = scheme.to_lowercase();
    if scheme_lower != "https" && scheme_lower != "http" {
        return Err(SignalError::InvalidConfig(
            "Webhook URL must use https (or http for localhost)".into(),
        ));
    }

    // Extract host (before any port or path).
    let authority = rest.split('/').next().unwrap_or(rest);
    let host = authority.split(':').next().unwrap_or(authority);

    if host.is_empty() {
        return Err(SignalError::InvalidConfig(
            "Webhook URL has empty host".into(),
        ));
    }

    // Allow http only for localhost / 127.0.0.1.
    if scheme_lower == "http" && host != "localhost" && host != "127.0.0.1" && host != "::1" {
        return Err(SignalError::InvalidConfig(
            "Webhook URL must use https for non-localhost hosts".into(),
        ));
    }

    // Block private/reserved IP addresses (SSRF mitigation).
    if let Ok(ip) = host.parse::<IpAddr>() {
        if is_private_ip(ip) && host != "127.0.0.1" && host != "::1" {
            return Err(SignalError::InvalidConfig(format!(
                "Webhook URL must not target private IP: {host}"
            )));
        }
    }

    Ok(())
}

/// Returns true if the IP is in a private / reserved range.
fn is_private_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback()          // 127.0.0.0/8
                || v4.is_private()    // 10/8, 172.16/12, 192.168/16
                || v4.is_link_local() // 169.254/16
                || v4.octets()[0] == 0 // 0.0.0.0/8
        }
        IpAddr::V6(v6) => {
            v6.is_loopback()          // ::1
                || v6.is_unspecified() // ::
                // fc00::/7 unique-local
                || (v6.segments()[0] & 0xfe00) == 0xfc00
                // fe80::/10 link-local
                || (v6.segments()[0] & 0xffc0) == 0xfe80
        }
    }
}

fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    if let Some(rest) = s.strip_suffix('s') {
        let secs: u64 = rest
            .parse()
            .map_err(|_| SignalError::InvalidConfig(format!("Invalid duration: {s}")))?;
        return Ok(Duration::from_secs(secs));
    }
    if let Some(rest) = s.strip_suffix('m') {
        let mins: u64 = rest
            .parse()
            .map_err(|_| SignalError::InvalidConfig(format!("Invalid duration: {s}")))?;
        return Ok(Duration::from_secs(mins * 60));
    }
    if let Some(rest) = s.strip_suffix('h') {
        let hrs: u64 = rest
            .parse()
            .map_err(|_| SignalError::InvalidConfig(format!("Invalid duration: {s}")))?;
        return Ok(Duration::from_secs(hrs * 3600));
    }
    Err(SignalError::InvalidConfig(format!(
        "Invalid duration format: {s} (expected e.g. '5m', '30s', '1h')"
    )))
}

// ── Condition conversion ────────────────────────────────────────────

/// Convert a [`ParsedCondition`] to a [`TriggerCondition`].
fn convert_condition(parsed: &ParsedCondition) -> TriggerCondition {
    match parsed {
        ParsedCondition::AnomalyScore { op, value } => TriggerCondition::AnomalyScore {
            threshold: *value,
            op: *op,
            detector_type: None,
        },
        ParsedCondition::ForecastDeviation { op, value } => TriggerCondition::ForecastDeviation {
            tolerance_pct: *value,
            op: *op,
            horizon: None,
        },
        ParsedCondition::FieldThreshold { field, op, value } => TriggerCondition::FieldThreshold {
            field: field.clone(),
            op: *op,
            value: *value,
        },
        ParsedCondition::And(left, right) => TriggerCondition::And {
            left: Box::new(convert_condition(left)),
            right: Box::new(convert_condition(right)),
        },
        ParsedCondition::Or(left, right) => TriggerCondition::Or {
            left: Box::new(convert_condition(left)),
            right: Box::new(convert_condition(right)),
        },
    }
}

// ── Executor ────────────────────────────────────────────────────────

/// Execute a parsed trigger SQL statement against a `TriggerEngine`.
///
/// # Errors
///
/// Returns an error if the statement cannot be executed.
pub fn execute_trigger_sql(engine: &TriggerEngine, stmt: &TriggerStatement) -> Result<SqlResult> {
    match stmt {
        TriggerStatement::Create(create) => {
            let condition = convert_condition(&create.condition);

            let delivery_targets: Vec<String> = create
                .deliver
                .iter()
                .map(|d| match d {
                    DeliveryTarget::Webhook(url) => format!("webhook:{url}"),
                    DeliveryTarget::Nats(subj) => format!("nats:{subj}"),
                    DeliveryTarget::Mqtt(topic) => format!("mqtt:{topic}"),
                    DeliveryTarget::Log => "log".to_string(),
                })
                .collect();

            let mut trigger =
                EventTrigger::new(&create.name, &create.name, &create.measurement, condition)
                    .with_delivery_targets(delivery_targets);

            if let Some(cooldown) = create.cooldown {
                trigger = trigger.with_cooldown(cooldown);
            }

            engine.register(trigger)?;
            debug!(name = %create.name, "Trigger created via SQL");

            Ok(SqlResult::Created(create.name.clone()))
        }

        TriggerStatement::Show => {
            let ids = engine.trigger_ids();
            let mut triggers = Vec::new();
            for id in ids {
                if let Some(t) = engine.get_trigger(&id) {
                    triggers.push(TriggerInfo {
                        id: t.id.clone(),
                        name: t.name.clone(),
                        measurement: t.measurement.clone(),
                        enabled: t.enabled,
                        condition: format!("{:?}", t.condition),
                        delivery: t.delivery_targets.clone(),
                        cooldown: Some(format!("{:?}", t.cooldown)),
                    });
                }
            }
            Ok(SqlResult::Triggers(triggers))
        }

        TriggerStatement::Drop(name) => {
            engine.unregister(name)?;
            debug!(name = %name, "Trigger dropped via SQL");
            Ok(SqlResult::Dropped(name.clone()))
        }

        TriggerStatement::Alter(alter) => match alter {
            AlterTrigger::Enable(name) => {
                engine.set_enabled(name, true)?;
                debug!(name = %name, "Trigger enabled via SQL");
                Ok(SqlResult::Altered(name.clone()))
            }
            AlterTrigger::Disable(name) => {
                engine.set_enabled(name, false)?;
                debug!(name = %name, "Trigger disabled via SQL");
                Ok(SqlResult::Altered(name.clone()))
            }
        },
    }
}

/// Result of a trigger SQL execution.
#[derive(Debug)]
pub enum SqlResult {
    /// Trigger created.
    Created(String),
    /// List of triggers.
    Triggers(Vec<TriggerInfo>),
    /// Trigger dropped.
    Dropped(String),
    /// Trigger altered.
    Altered(String),
}

// ── TriggerCatalog ──────────────────────────────────────────────────

/// Persists trigger definitions to survive restarts.
///
/// Serializes the trigger registry to JSON.
pub struct TriggerCatalog {
    entries: parking_lot::RwLock<HashMap<String, CatalogEntry>>,
}

/// A persistable trigger definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogEntry {
    /// Original SQL statement.
    pub sql: String,
    /// Trigger name.
    pub name: String,
    /// Whether enabled.
    pub enabled: bool,
}

impl TriggerCatalog {
    /// Create a new empty catalog.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    /// Add an entry.
    pub fn insert(&self, name: impl Into<String>, sql: impl Into<String>) {
        let name = name.into();
        self.entries.write().insert(
            name.clone(),
            CatalogEntry {
                sql: sql.into(),
                name,
                enabled: true,
            },
        );
    }

    /// Remove an entry.
    pub fn remove(&self, name: &str) {
        self.entries.write().remove(name);
    }

    /// Update the enabled state of an entry.
    pub fn set_enabled(&self, name: &str, enabled: bool) {
        if let Some(entry) = self.entries.write().get_mut(name) {
            entry.enabled = enabled;
        }
    }

    /// Get all entries.
    #[must_use]
    pub fn entries(&self) -> Vec<CatalogEntry> {
        self.entries.read().values().cloned().collect()
    }

    /// Serialize to JSON.
    pub fn to_json(&self) -> Result<String> {
        let entries = self.entries.read();
        serde_json::to_string_pretty(&*entries)
            .map_err(|e| SignalError::Serialization { source: e })
    }

    /// Deserialize from JSON.
    pub fn from_json(json: &str) -> Result<Self> {
        let entries: HashMap<String, CatalogEntry> =
            serde_json::from_str(json).map_err(|e| SignalError::Serialization { source: e })?;
        Ok(Self {
            entries: parking_lot::RwLock::new(entries),
        })
    }

    /// Number of entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    /// Whether the catalog is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }

    /// Persist the catalog to disk using atomic write (write to
    /// temp + rename) for crash safety.
    pub fn save_to_disk(&self, path: &std::path::Path) -> Result<()> {
        let json = self.to_json()?;
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, json.as_bytes())
            .map_err(|e| SignalError::InvalidConfig(format!("write catalog: {e}")))?;
        std::fs::rename(&tmp, path)
            .map_err(|e| SignalError::InvalidConfig(format!("rename catalog: {e}")))?;
        // fsync the parent directory to ensure the rename is durable.
        if let Some(parent) = path.parent() {
            if let Ok(dir) = std::fs::File::open(parent) {
                let _ = dir.sync_all();
            }
        }
        Ok(())
    }

    /// Load the catalog from a JSON file on disk.
    pub fn load_from_disk(path: &std::path::Path) -> Result<Self> {
        let json = std::fs::read_to_string(path)
            .map_err(|e| SignalError::InvalidConfig(format!("read catalog: {e}")))?;
        Self::from_json(&json)
    }
}

impl Default for TriggerCatalog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Tokenizer tests ─────────

    #[test]
    fn tokenize_basic() {
        let tokens = tokenize("CREATE TRIGGER foo ON cpu WHEN value > 3.0");
        assert_eq!(
            tokens,
            &["CREATE", "TRIGGER", "foo", "ON", "cpu", "WHEN", "value", ">", "3.0"]
        );
    }

    #[test]
    fn tokenize_with_quotes() {
        let tokens = tokenize("DELIVER webhook('https://example.com')");
        assert_eq!(
            tokens,
            &["DELIVER", "webhook", "(", "'https://example.com'", ")"]
        );
    }

    #[test]
    fn tokenize_double_quoted_identifier() {
        let tokens = tokenize(r#"CREATE TRIGGER "my-trigger" ON "cpu.usage""#);
        assert_eq!(
            tokens,
            &["CREATE", "TRIGGER", "\x01my-trigger", "ON", "\x01cpu.usage"]
        );
    }

    #[test]
    fn tokenize_backtick_quoted_identifier() {
        let tokens = tokenize("CREATE TRIGGER `my trigger` ON `cpu usage`");
        assert_eq!(
            tokens,
            &["CREATE", "TRIGGER", "\x01my trigger", "ON", "\x01cpu usage"]
        );
    }

    #[test]
    fn tokenize_escaped_double_quote() {
        // "" inside double-quoted identifier becomes a literal "
        let tokens = tokenize(r#"CREATE TRIGGER "say""hello" ON cpu"#);
        assert_eq!(
            tokens,
            &["CREATE", "TRIGGER", "\x01say\"hello", "ON", "cpu"]
        );
    }

    #[test]
    fn parse_create_with_quoted_names() {
        let stmt = parse_trigger_sql(
            r#"CREATE TRIGGER "high-cpu-alert" ON "system.cpu" WHEN value > 90.0"#,
        )
        .unwrap();
        match stmt {
            TriggerStatement::Create(c) => {
                assert_eq!(c.name, "high-cpu-alert");
                assert_eq!(c.measurement, "system.cpu");
            }
            _ => panic!("Expected Create"),
        }
    }

    #[test]
    fn parse_create_with_backtick_names() {
        let stmt =
            parse_trigger_sql("CREATE TRIGGER `my trigger` ON `my measurement` WHEN value > 1.0")
                .unwrap();
        match stmt {
            TriggerStatement::Create(c) => {
                assert_eq!(c.name, "my trigger");
                assert_eq!(c.measurement, "my measurement");
            }
            _ => panic!("Expected Create"),
        }
    }

    // ── Parser tests ────────────

    #[test]
    fn parse_create_simple() {
        let stmt =
            parse_trigger_sql("CREATE TRIGGER alert_cpu ON cpu_usage WHEN anomaly_score > 3.0")
                .unwrap();

        match stmt {
            TriggerStatement::Create(c) => {
                assert_eq!(c.name, "alert_cpu");
                assert_eq!(c.measurement, "cpu_usage");
                assert_eq!(
                    c.condition,
                    ParsedCondition::AnomalyScore {
                        op: ThresholdOp::Gt,
                        value: 3.0
                    }
                );
                assert!(c.deliver.is_empty());
                assert!(c.cooldown.is_none());
            }
            _ => panic!("Expected Create"),
        }
    }

    #[test]
    fn parse_create_with_deliver_and_cooldown() {
        let stmt = parse_trigger_sql(
            "CREATE TRIGGER forecast_dev ON energy WHEN forecast_deviation > 0.15 DELIVER webhook('https://example.com') COOLDOWN INTERVAL '5m'"
        ).unwrap();

        match stmt {
            TriggerStatement::Create(c) => {
                assert_eq!(c.name, "forecast_dev");
                assert_eq!(c.measurement, "energy");
                assert_eq!(
                    c.condition,
                    ParsedCondition::ForecastDeviation {
                        op: ThresholdOp::Gt,
                        value: 0.15
                    }
                );
                assert_eq!(
                    c.deliver,
                    vec![DeliveryTarget::Webhook("https://example.com".into())]
                );
                assert_eq!(c.cooldown, Some(Duration::from_secs(300)));
            }
            _ => panic!("Expected Create"),
        }
    }

    #[test]
    fn parse_create_field_threshold() {
        let stmt = parse_trigger_sql("CREATE TRIGGER temp_high ON temperature WHEN value >= 100.0")
            .unwrap();

        match stmt {
            TriggerStatement::Create(c) => {
                assert_eq!(
                    c.condition,
                    ParsedCondition::FieldThreshold {
                        field: "value".into(),
                        op: ThresholdOp::Gte,
                        value: 100.0
                    }
                );
            }
            _ => panic!("Expected Create"),
        }
    }

    #[test]
    fn parse_create_multi_deliver() {
        let stmt = parse_trigger_sql(
            "CREATE TRIGGER alert ON cpu WHEN anomaly_score > 2.0 DELIVER webhook('https://a.com'), nats('alerts.cpu'), mqtt('alerts/cpu')"
        ).unwrap();

        match stmt {
            TriggerStatement::Create(c) => {
                assert_eq!(c.deliver.len(), 3);
                assert_eq!(
                    c.deliver[0],
                    DeliveryTarget::Webhook("https://a.com".into())
                );
                assert_eq!(c.deliver[1], DeliveryTarget::Nats("alerts.cpu".into()));
                assert_eq!(c.deliver[2], DeliveryTarget::Mqtt("alerts/cpu".into()));
            }
            _ => panic!("Expected Create"),
        }
    }

    #[test]
    fn parse_show() {
        let stmt = parse_trigger_sql("SHOW TRIGGERS").unwrap();
        assert_eq!(stmt, TriggerStatement::Show);
    }

    #[test]
    fn parse_drop() {
        let stmt = parse_trigger_sql("DROP TRIGGER alert_cpu").unwrap();
        assert_eq!(stmt, TriggerStatement::Drop("alert_cpu".into()));
    }

    #[test]
    fn parse_alter_disable() {
        let stmt = parse_trigger_sql("ALTER TRIGGER alert_cpu DISABLE").unwrap();
        assert_eq!(
            stmt,
            TriggerStatement::Alter(AlterTrigger::Disable("alert_cpu".into()))
        );
    }

    #[test]
    fn parse_alter_enable() {
        let stmt = parse_trigger_sql("ALTER TRIGGER alert_cpu ENABLE").unwrap();
        assert_eq!(
            stmt,
            TriggerStatement::Alter(AlterTrigger::Enable("alert_cpu".into()))
        );
    }

    #[test]
    fn parse_error_empty() {
        assert!(parse_trigger_sql("").is_err());
    }

    #[test]
    fn parse_error_unknown_keyword() {
        assert!(parse_trigger_sql("SELECT * FROM triggers").is_err());
    }

    // ── Duration parser tests ───

    #[test]
    fn parse_duration_seconds() {
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
    }

    #[test]
    fn parse_duration_minutes() {
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
    }

    #[test]
    fn parse_duration_hours() {
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
    }

    #[test]
    fn parse_duration_invalid() {
        assert!(parse_duration("abc").is_err());
    }

    // ── Executor tests ──────────

    #[test]
    fn execute_create_show_drop() {
        let engine = TriggerEngine::new();

        // CREATE
        let stmt = parse_trigger_sql(
            "CREATE TRIGGER alert_cpu ON cpu WHEN anomaly_score > 3.0 COOLDOWN INTERVAL '5m'",
        )
        .unwrap();
        let result = execute_trigger_sql(&engine, &stmt).unwrap();
        assert!(matches!(result, SqlResult::Created(ref name) if name == "alert_cpu"));
        assert_eq!(engine.trigger_count(), 1);

        // SHOW
        let stmt = parse_trigger_sql("SHOW TRIGGERS").unwrap();
        let result = execute_trigger_sql(&engine, &stmt).unwrap();
        match result {
            SqlResult::Triggers(triggers) => {
                assert_eq!(triggers.len(), 1);
                assert_eq!(triggers[0].name, "alert_cpu");
                assert!(triggers[0].enabled);
            }
            _ => panic!("Expected Triggers"),
        }

        // DROP
        let stmt = parse_trigger_sql("DROP TRIGGER alert_cpu").unwrap();
        let result = execute_trigger_sql(&engine, &stmt).unwrap();
        assert!(matches!(result, SqlResult::Dropped(ref name) if name == "alert_cpu"));
        assert_eq!(engine.trigger_count(), 0);
    }

    #[test]
    fn execute_alter_disable_enable() {
        let engine = TriggerEngine::new();

        let stmt = parse_trigger_sql("CREATE TRIGGER t1 ON cpu WHEN value > 90.0").unwrap();
        execute_trigger_sql(&engine, &stmt).unwrap();

        // Verify enabled
        assert!(engine.get_trigger("t1").unwrap().enabled);

        // DISABLE
        let stmt = parse_trigger_sql("ALTER TRIGGER t1 DISABLE").unwrap();
        execute_trigger_sql(&engine, &stmt).unwrap();
        assert!(!engine.get_trigger("t1").unwrap().enabled);

        // ENABLE
        let stmt = parse_trigger_sql("ALTER TRIGGER t1 ENABLE").unwrap();
        execute_trigger_sql(&engine, &stmt).unwrap();
        assert!(engine.get_trigger("t1").unwrap().enabled);
    }

    #[test]
    fn execute_drop_nonexistent() {
        let engine = TriggerEngine::new();
        let stmt = parse_trigger_sql("DROP TRIGGER nonexistent").unwrap();
        // Drop of nonexistent trigger is now an error
        let result = execute_trigger_sql(&engine, &stmt);
        assert!(result.is_err());
    }

    // ── Op wiring tests ──────────

    #[test]
    fn execute_create_anomaly_score_preserves_op() {
        let engine = TriggerEngine::new();
        let stmt = parse_trigger_sql(
            "CREATE TRIGGER a1 ON cpu WHEN anomaly_score < 2.0 COOLDOWN INTERVAL '1m'",
        )
        .unwrap();
        execute_trigger_sql(&engine, &stmt).unwrap();

        let t = engine.get_trigger("a1").unwrap();
        if let TriggerCondition::AnomalyScore { threshold, op, .. } = &t.condition {
            assert_eq!(*threshold, 2.0);
            assert_eq!(*op, ThresholdOp::Lt);
        } else {
            panic!("Expected AnomalyScore condition");
        }
    }

    #[test]
    fn execute_create_forecast_dev_preserves_op() {
        let engine = TriggerEngine::new();
        let stmt = parse_trigger_sql("CREATE TRIGGER f1 ON energy WHEN forecast_deviation >= 0.25")
            .unwrap();
        execute_trigger_sql(&engine, &stmt).unwrap();

        let t = engine.get_trigger("f1").unwrap();
        if let TriggerCondition::ForecastDeviation {
            tolerance_pct, op, ..
        } = &t.condition
        {
            assert_eq!(*tolerance_pct, 0.25);
            assert_eq!(*op, ThresholdOp::Gte);
        } else {
            panic!("Expected ForecastDeviation condition");
        }
    }

    // ── Delivery target wiring tests ─────

    #[test]
    fn execute_create_with_delivery_targets() {
        let engine = TriggerEngine::new();
        let stmt = parse_trigger_sql(
            "CREATE TRIGGER d1 ON cpu WHEN anomaly_score > 3.0 DELIVER webhook('https://hooks.example.com'), log"
        ).unwrap();
        execute_trigger_sql(&engine, &stmt).unwrap();

        let t = engine.get_trigger("d1").unwrap();
        assert_eq!(t.delivery_targets.len(), 2);
        assert_eq!(t.delivery_targets[0], "webhook:https://hooks.example.com");
        assert_eq!(t.delivery_targets[1], "log");
    }

    #[test]
    fn show_triggers_includes_delivery() {
        let engine = TriggerEngine::new();
        let stmt = parse_trigger_sql(
            "CREATE TRIGGER d1 ON cpu WHEN value > 90.0 DELIVER nats('alerts.cpu'), mqtt('alerts/cpu')"
        ).unwrap();
        execute_trigger_sql(&engine, &stmt).unwrap();

        let stmt = parse_trigger_sql("SHOW TRIGGERS").unwrap();
        let result = execute_trigger_sql(&engine, &stmt).unwrap();
        if let SqlResult::Triggers(triggers) = result {
            assert_eq!(triggers.len(), 1);
            assert_eq!(triggers[0].delivery.len(), 2);
            assert_eq!(triggers[0].delivery[0], "nats:alerts.cpu");
            assert_eq!(triggers[0].delivery[1], "mqtt:alerts/cpu");
        } else {
            panic!("Expected Triggers result");
        }
    }

    // ── Error propagation tests ─────

    #[test]
    fn alter_nonexistent_trigger_is_error() {
        let engine = TriggerEngine::new();
        let stmt = parse_trigger_sql("ALTER TRIGGER ghost ENABLE").unwrap();
        assert!(execute_trigger_sql(&engine, &stmt).is_err());
    }

    // ── Catalog tests ───────────

    #[test]
    fn catalog_insert_and_query() {
        let catalog = TriggerCatalog::new();
        catalog.insert("t1", "CREATE TRIGGER t1 ON cpu WHEN value > 90");
        catalog.insert("t2", "CREATE TRIGGER t2 ON mem WHEN value > 80");

        assert_eq!(catalog.len(), 2);
        let entries = catalog.entries();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn catalog_remove() {
        let catalog = TriggerCatalog::new();
        catalog.insert("t1", "CREATE ...");
        catalog.remove("t1");
        assert!(catalog.is_empty());
    }

    #[test]
    fn catalog_json_roundtrip() {
        let catalog = TriggerCatalog::new();
        catalog.insert("t1", "CREATE TRIGGER t1 ON cpu WHEN value > 90");

        let json = catalog.to_json().unwrap();
        let restored = TriggerCatalog::from_json(&json).unwrap();
        assert_eq!(restored.len(), 1);
        let entries = restored.entries();
        assert_eq!(entries[0].name, "t1");
    }

    #[test]
    fn test_negative_number_in_threshold() {
        let stmt = parse_trigger_sql("CREATE TRIGGER neg_test ON temp WHEN value > -5.0").unwrap();

        match stmt {
            TriggerStatement::Create(c) => {
                assert_eq!(c.name, "neg_test");
                assert_eq!(c.measurement, "temp");
                assert_eq!(
                    c.condition,
                    ParsedCondition::FieldThreshold {
                        field: "value".into(),
                        op: ThresholdOp::Gt,
                        value: -5.0,
                    }
                );
            }
            _ => panic!("Expected Create"),
        }
    }

    #[test]
    fn test_eq_operator() {
        let stmt = parse_trigger_sql("CREATE TRIGGER eq_test ON cpu WHEN value == 90.0").unwrap();

        match stmt {
            TriggerStatement::Create(c) => {
                assert_eq!(
                    c.condition,
                    ParsedCondition::FieldThreshold {
                        field: "value".into(),
                        op: ThresholdOp::Eq,
                        value: 90.0,
                    }
                );
            }
            _ => panic!("Expected Create"),
        }
    }

    #[test]
    fn test_single_eq_operator() {
        let stmt = parse_trigger_sql("CREATE TRIGGER eq2 ON cpu WHEN value = 90.0").unwrap();

        match stmt {
            TriggerStatement::Create(c) => {
                assert_eq!(
                    c.condition,
                    ParsedCondition::FieldThreshold {
                        field: "value".into(),
                        op: ThresholdOp::Eq,
                        value: 90.0,
                    }
                );
            }
            _ => panic!("Expected Create"),
        }
    }

    // ── Identifier validation tests ─────

    #[test]
    fn test_valid_identifiers_accepted() {
        assert!(is_valid_identifier("cpu_usage"));
        assert!(is_valid_identifier("_private"));
        assert!(is_valid_identifier("A1"));
        assert!(is_valid_identifier("abc"));
        assert!(is_valid_identifier("_"));
        assert!(is_valid_identifier("x"));
    }

    #[test]
    fn test_invalid_identifiers_rejected_by_helper() {
        assert!(!is_valid_identifier("foo;bar"));
        assert!(!is_valid_identifier("table--name"));
        assert!(!is_valid_identifier("123abc"));
        assert!(!is_valid_identifier(""));
        assert!(!is_valid_identifier("has space"));
        assert!(!is_valid_identifier("no.dots"));
    }

    #[test]
    fn test_invalid_trigger_name_rejected() {
        // `@` stays as part of the token (not a tokenizer separator)
        let result = parse_trigger_sql("CREATE TRIGGER foo@bar ON cpu WHEN value > 1.0");
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("Invalid trigger name"), "got: {err_msg}");
    }

    #[test]
    fn test_invalid_trigger_name_starts_with_digit() {
        let result = parse_trigger_sql("CREATE TRIGGER 123abc ON cpu WHEN value > 1.0");
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("Invalid trigger name"), "got: {err_msg}");
    }

    #[test]
    fn test_invalid_condition_field_rejected() {
        let result = parse_trigger_sql("CREATE TRIGGER ok_name ON cpu WHEN bad@field > 1.0");
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("Invalid condition field"),
            "got: {err_msg}"
        );
    }

    #[test]
    fn test_valid_trigger_name_accepted() {
        let stmt = parse_trigger_sql("CREATE TRIGGER cpu_usage ON cpu WHEN value > 1.0").unwrap();
        match stmt {
            TriggerStatement::Create(c) => assert_eq!(c.name, "cpu_usage"),
            _ => panic!("Expected Create"),
        }

        let stmt = parse_trigger_sql("CREATE TRIGGER _private ON cpu WHEN value > 1.0").unwrap();
        match stmt {
            TriggerStatement::Create(c) => assert_eq!(c.name, "_private"),
            _ => panic!("Expected Create"),
        }

        let stmt = parse_trigger_sql("CREATE TRIGGER A1 ON cpu WHEN value > 1.0").unwrap();
        match stmt {
            TriggerStatement::Create(c) => assert_eq!(c.name, "A1"),
            _ => panic!("Expected Create"),
        }
    }

    // ── Compound condition tests ─────

    #[test]
    fn test_parse_and_condition() {
        let _stmt = parse_trigger_sql(
            "CREATE TRIGGER t WHEN value > 100 AND rate < 5 ON cpu_usage DELIVER log",
        );
        // Oops — ON must come before WHEN.  Use correct order.
        let stmt = parse_trigger_sql(
            "CREATE TRIGGER t ON cpu_usage WHEN value > 100 AND rate < 5 DELIVER log",
        )
        .unwrap();

        match stmt {
            TriggerStatement::Create(c) => {
                assert_eq!(
                    c.condition,
                    ParsedCondition::And(
                        Box::new(ParsedCondition::FieldThreshold {
                            field: "value".into(),
                            op: ThresholdOp::Gt,
                            value: 100.0,
                        }),
                        Box::new(ParsedCondition::FieldThreshold {
                            field: "rate".into(),
                            op: ThresholdOp::Lt,
                            value: 5.0,
                        }),
                    )
                );
                assert_eq!(c.deliver, vec![DeliveryTarget::Log]);
            }
            _ => panic!("Expected Create"),
        }
    }

    #[test]
    fn test_parse_or_condition() {
        let stmt = parse_trigger_sql(
            "CREATE TRIGGER t ON cpu_usage WHEN value > 100 OR anomaly_score > 3.0",
        )
        .unwrap();

        match stmt {
            TriggerStatement::Create(c) => {
                assert_eq!(
                    c.condition,
                    ParsedCondition::Or(
                        Box::new(ParsedCondition::FieldThreshold {
                            field: "value".into(),
                            op: ThresholdOp::Gt,
                            value: 100.0,
                        }),
                        Box::new(ParsedCondition::AnomalyScore {
                            op: ThresholdOp::Gt,
                            value: 3.0,
                        }),
                    )
                );
            }
            _ => panic!("Expected Create"),
        }
    }

    #[test]
    fn test_parse_compound_with_parens() {
        let stmt = parse_trigger_sql(
            "CREATE TRIGGER t ON cpu WHEN (value > 100 AND rate < 5) OR anomaly_score > 3.0",
        )
        .unwrap();

        match stmt {
            TriggerStatement::Create(c) => {
                let expected = ParsedCondition::Or(
                    Box::new(ParsedCondition::And(
                        Box::new(ParsedCondition::FieldThreshold {
                            field: "value".into(),
                            op: ThresholdOp::Gt,
                            value: 100.0,
                        }),
                        Box::new(ParsedCondition::FieldThreshold {
                            field: "rate".into(),
                            op: ThresholdOp::Lt,
                            value: 5.0,
                        }),
                    )),
                    Box::new(ParsedCondition::AnomalyScore {
                        op: ThresholdOp::Gt,
                        value: 3.0,
                    }),
                );
                assert_eq!(c.condition, expected);
            }
            _ => panic!("Expected Create"),
        }
    }

    #[test]
    fn test_and_has_higher_precedence_than_or() {
        // a > 1 OR b > 2 AND c > 3  ⟹  a > 1 OR (b > 2 AND c > 3)
        let stmt =
            parse_trigger_sql("CREATE TRIGGER t ON m WHEN a > 1 OR b > 2 AND c > 3").unwrap();

        match stmt {
            TriggerStatement::Create(c) => {
                let expected = ParsedCondition::Or(
                    Box::new(ParsedCondition::FieldThreshold {
                        field: "a".into(),
                        op: ThresholdOp::Gt,
                        value: 1.0,
                    }),
                    Box::new(ParsedCondition::And(
                        Box::new(ParsedCondition::FieldThreshold {
                            field: "b".into(),
                            op: ThresholdOp::Gt,
                            value: 2.0,
                        }),
                        Box::new(ParsedCondition::FieldThreshold {
                            field: "c".into(),
                            op: ThresholdOp::Gt,
                            value: 3.0,
                        }),
                    )),
                );
                assert_eq!(c.condition, expected);
            }
            _ => panic!("Expected Create"),
        }
    }

    #[test]
    fn test_compound_with_cooldown() {
        let stmt = parse_trigger_sql(
            "CREATE TRIGGER t ON cpu WHEN value > 90 AND rate < 10 DELIVER log COOLDOWN INTERVAL '5m'"
        ).unwrap();

        match stmt {
            TriggerStatement::Create(c) => {
                assert!(matches!(c.condition, ParsedCondition::And(..)));
                assert_eq!(c.deliver, vec![DeliveryTarget::Log]);
                assert_eq!(c.cooldown, Some(Duration::from_secs(300)));
            }
            _ => panic!("Expected Create"),
        }
    }

    #[test]
    fn test_execute_compound_condition() {
        let engine = TriggerEngine::new();
        let stmt =
            parse_trigger_sql("CREATE TRIGGER t ON cpu WHEN value > 90.0 AND anomaly_score > 2.0")
                .unwrap();
        let result = execute_trigger_sql(&engine, &stmt).unwrap();
        assert!(matches!(result, SqlResult::Created(ref n) if n == "t"));

        let t = engine.get_trigger("t").unwrap();
        assert!(matches!(t.condition, TriggerCondition::And { .. }));
    }
}
