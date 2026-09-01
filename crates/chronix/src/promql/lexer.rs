//! `PromQL` lexer — tokenizes a `PromQL` expression string.

use std::fmt;

/// Token types produced by the lexer.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    /// Numeric literal.
    Number(f64),
    /// String literal.
    String(String),
    /// Identifier (metric name, label name, function name, etc.).
    Ident(String),

    /// `(`.
    LeftParen,
    /// `)`.
    RightParen,
    /// `{`.
    LeftBrace,
    /// `}`.
    RightBrace,
    /// `[`.
    LeftBracket,
    /// `]`.
    RightBracket,
    /// `,`.
    Comma,
    /// `:`.
    Colon,

    /// `+`.
    Plus,
    /// `-`.
    Minus,
    /// `*`.
    Star,
    /// `/`.
    Slash,
    /// `%`.
    Percent,
    /// `^`.
    Caret,

    /// `==`.
    Eql,
    /// `!=`.
    Neq,
    /// `<`.
    Lss,
    /// `>`.
    Gtr,
    /// `<=`.
    Lte,
    /// `>=`.
    Gte,

    /// `=~` (regex match).
    EqlRegex,
    /// `!~` (negative regex match).
    NeqRegex,
    /// `=` (assignment / label matcher).
    Assign,

    /// `by` keyword.
    By,
    /// `without` keyword.
    Without,
    /// `on` keyword.
    On,
    /// `ignoring` keyword.
    Ignoring,
    /// `group_left` keyword.
    GroupLeft,
    /// `group_right` keyword.
    GroupRight,
    /// `offset` keyword.
    Offset,
    /// `bool` keyword.
    Bool,
    /// `and` keyword.
    And,
    /// `or` keyword.
    Or,
    /// `unless` keyword.
    Unless,

    /// End of input.
    Eof,
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Token::Number(n) => write!(f, "{n}"),
            Token::String(s) => write!(f, "\"{s}\""),
            Token::Ident(s) => write!(f, "{s}"),
            Token::LeftParen => write!(f, "("),
            Token::RightParen => write!(f, ")"),
            Token::LeftBrace => write!(f, "{{"),
            Token::RightBrace => write!(f, "}}"),
            Token::LeftBracket => write!(f, "["),
            Token::RightBracket => write!(f, "]"),
            Token::Comma => write!(f, ","),
            Token::Colon => write!(f, ":"),
            Token::Plus => write!(f, "+"),
            Token::Minus => write!(f, "-"),
            Token::Star => write!(f, "*"),
            Token::Slash => write!(f, "/"),
            Token::Percent => write!(f, "%"),
            Token::Caret => write!(f, "^"),
            Token::Eql => write!(f, "=="),
            Token::Neq => write!(f, "!="),
            Token::Lss => write!(f, "<"),
            Token::Gtr => write!(f, ">"),
            Token::Lte => write!(f, "<="),
            Token::Gte => write!(f, ">="),
            Token::EqlRegex => write!(f, "=~"),
            Token::NeqRegex => write!(f, "!~"),
            Token::Assign => write!(f, "="),
            Token::By => write!(f, "by"),
            Token::Without => write!(f, "without"),
            Token::On => write!(f, "on"),
            Token::Ignoring => write!(f, "ignoring"),
            Token::GroupLeft => write!(f, "group_left"),
            Token::GroupRight => write!(f, "group_right"),
            Token::Offset => write!(f, "offset"),
            Token::Bool => write!(f, "bool"),
            Token::And => write!(f, "and"),
            Token::Or => write!(f, "or"),
            Token::Unless => write!(f, "unless"),
            Token::Eof => write!(f, "EOF"),
        }
    }
}

/// Lexer error.
#[derive(Debug, Clone, PartialEq)]
pub struct LexError {
    /// Error description.
    pub msg: String,
    /// Byte position where the error occurred.
    pub pos: usize,
}

impl fmt::Display for LexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "lex error at position {}: {}", self.pos, self.msg)
    }
}

impl std::error::Error for LexError {}

/// Tokenize a `PromQL` input string.
pub fn lex(input: &str) -> Result<Vec<Token>, LexError> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        let c = chars[i];

        // Skip whitespace
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }

        // Skip comments
        if c == '#' {
            while i < len && chars[i] != '\n' {
                i += 1;
            }
            continue;
        }

        match c {
            '(' => {
                tokens.push(Token::LeftParen);
                i += 1;
            }
            ')' => {
                tokens.push(Token::RightParen);
                i += 1;
            }
            '{' => {
                tokens.push(Token::LeftBrace);
                i += 1;
            }
            '}' => {
                tokens.push(Token::RightBrace);
                i += 1;
            }
            '[' => {
                tokens.push(Token::LeftBracket);
                i += 1;
            }
            ']' => {
                tokens.push(Token::RightBracket);
                i += 1;
            }
            ',' => {
                tokens.push(Token::Comma);
                i += 1;
            }
            ':' => {
                tokens.push(Token::Colon);
                i += 1;
            }
            '+' => {
                tokens.push(Token::Plus);
                i += 1;
            }
            '-' => {
                tokens.push(Token::Minus);
                i += 1;
            }
            '*' => {
                tokens.push(Token::Star);
                i += 1;
            }
            '/' => {
                tokens.push(Token::Slash);
                i += 1;
            }
            '%' => {
                tokens.push(Token::Percent);
                i += 1;
            }
            '^' => {
                tokens.push(Token::Caret);
                i += 1;
            }
            '=' => {
                if i + 1 < len {
                    match chars[i + 1] {
                        '=' => {
                            tokens.push(Token::Eql);
                            i += 2;
                        }
                        '~' => {
                            tokens.push(Token::EqlRegex);
                            i += 2;
                        }
                        _ => {
                            tokens.push(Token::Assign);
                            i += 1;
                        }
                    }
                } else {
                    tokens.push(Token::Assign);
                    i += 1;
                }
            }
            '!' => {
                if i + 1 < len {
                    match chars[i + 1] {
                        '=' => {
                            tokens.push(Token::Neq);
                            i += 2;
                        }
                        '~' => {
                            tokens.push(Token::NeqRegex);
                            i += 2;
                        }
                        _ => {
                            return Err(LexError {
                                msg: format!("unexpected character after '!': '{}'", chars[i + 1]),
                                pos: i,
                            });
                        }
                    }
                } else {
                    return Err(LexError {
                        msg: "unexpected end of input after '!'".to_string(),
                        pos: i,
                    });
                }
            }
            '<' => {
                if i + 1 < len && chars[i + 1] == '=' {
                    tokens.push(Token::Lte);
                    i += 2;
                } else {
                    tokens.push(Token::Lss);
                    i += 1;
                }
            }
            '>' => {
                if i + 1 < len && chars[i + 1] == '=' {
                    tokens.push(Token::Gte);
                    i += 2;
                } else {
                    tokens.push(Token::Gtr);
                    i += 1;
                }
            }
            '"' | '\'' | '`' => {
                let quote = c;
                i += 1;
                let mut s = String::new();
                while i < len && chars[i] != quote {
                    // Backtick strings are raw — no escape processing.
                    if quote != '`' && chars[i] == '\\' && i + 1 < len {
                        i += 1;
                        match chars[i] {
                            'n' => s.push('\n'),
                            't' => s.push('\t'),
                            'r' => s.push('\r'),
                            'a' => s.push('\x07'), // bell
                            'b' => s.push('\x08'), // backspace
                            'f' => s.push('\x0C'), // form feed
                            'v' => s.push('\x0B'), // vertical tab
                            '\\' => s.push('\\'),
                            q if q == quote => s.push(q),
                            other => {
                                s.push('\\');
                                s.push(other);
                            }
                        }
                    } else {
                        s.push(chars[i]);
                    }
                    i += 1;
                }
                if i >= len {
                    return Err(LexError {
                        msg: format!("unterminated string literal (started with {quote})"),
                        pos: i,
                    });
                }
                i += 1; // closing quote
                tokens.push(Token::String(s));
            }
            _ if c.is_ascii_digit()
                || (c == '.' && i + 1 < len && chars[i + 1].is_ascii_digit()) =>
            {
                let start = i;
                // Integer part
                while i < len && chars[i].is_ascii_digit() {
                    i += 1;
                }
                // Decimal part
                if i < len && chars[i] == '.' {
                    i += 1;
                    while i < len && chars[i].is_ascii_digit() {
                        i += 1;
                    }
                }
                // Exponent
                if i < len && (chars[i] == 'e' || chars[i] == 'E') {
                    i += 1;
                    if i < len && (chars[i] == '+' || chars[i] == '-') {
                        i += 1;
                    }
                    while i < len && chars[i].is_ascii_digit() {
                        i += 1;
                    }
                }

                // Check if this is actually a duration (digits followed by duration suffix)
                // Only if we haven't seen a dot or exponent
                let num_str: String = chars[start..i].iter().collect();

                // Check if followed by a duration suffix (and the number is an integer)
                // Duration suffixes are handled contextually by the parser,
                // so we just emit the number regardless.
                let val: f64 = num_str.parse().map_err(|_| LexError {
                    msg: format!("invalid number: {num_str}"),
                    pos: start,
                })?;
                tokens.push(Token::Number(val));

                // If a duration suffix follows an integer, skip it — the
                // parser handles duration parsing from separate tokens.
                if i < len
                    && is_duration_suffix(chars[i])
                    && !num_str.contains('.')
                    && !num_str.contains('e')
                    && !num_str.contains('E')
                {
                    // Duration suffix will be consumed as an ident token
                    // on the next iteration.
                }
            }
            _ if is_ident_start(c) => {
                let start = i;
                while i < len && is_ident_char_with_colon(chars[i]) {
                    // Stop at ':' if followed by a digit (subquery separator like `:1m`)
                    // or if followed by `]` or end-of-input.
                    if chars[i] == ':' {
                        let next = if i + 1 < len {
                            Some(chars[i + 1])
                        } else {
                            None
                        };
                        match next {
                            Some(nc) if nc.is_ascii_digit() || nc == ']' => break,
                            None => break,
                            _ => {}
                        }
                    }
                    i += 1;
                }
                let word: String = chars[start..i].iter().collect();
                let tok = match word.to_lowercase().as_str() {
                    "by" => Token::By,
                    "without" => Token::Without,
                    "on" => Token::On,
                    "ignoring" => Token::Ignoring,
                    "group_left" => Token::GroupLeft,
                    "group_right" => Token::GroupRight,
                    "offset" => Token::Offset,
                    "bool" => Token::Bool,
                    "and" => Token::And,
                    "or" => Token::Or,
                    "unless" => Token::Unless,
                    "inf" | "infinity" => Token::Number(f64::INFINITY),
                    "nan" => Token::Number(f64::NAN),
                    _ => Token::Ident(word),
                };
                tokens.push(tok);
            }
            _ => {
                return Err(LexError {
                    msg: format!("unexpected character: '{c}'"),
                    pos: i,
                });
            }
        }
    }

    tokens.push(Token::Eof);
    Ok(tokens)
}

fn is_ident_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_'
}

fn is_ident_char_with_colon(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == ':'
}

fn is_duration_suffix(c: char) -> bool {
    matches!(c, 's' | 'm' | 'h' | 'd' | 'w' | 'y')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lex_simple_metric() {
        let tokens = lex("http_requests_total").unwrap();
        assert_eq!(
            tokens,
            vec![Token::Ident("http_requests_total".into()), Token::Eof]
        );
    }

    #[test]
    fn lex_selector_with_labels() {
        let tokens = lex(r#"cpu_usage{host="srv1", cpu!="idle"}"#).unwrap();
        assert_eq!(tokens[0], Token::Ident("cpu_usage".into()));
        assert_eq!(tokens[1], Token::LeftBrace);
        assert_eq!(tokens[2], Token::Ident("host".into()));
        assert_eq!(tokens[3], Token::Assign);
        assert_eq!(tokens[4], Token::String("srv1".into()));
        assert_eq!(tokens[5], Token::Comma);
        assert_eq!(tokens[6], Token::Ident("cpu".into()));
        assert_eq!(tokens[7], Token::Neq);
        assert_eq!(tokens[8], Token::String("idle".into()));
        assert_eq!(tokens[9], Token::RightBrace);
        assert_eq!(tokens[10], Token::Eof);
    }

    #[test]
    fn lex_number() {
        let tokens = lex("42.5e3").unwrap();
        assert_eq!(tokens, vec![Token::Number(42500.0), Token::Eof]);
    }

    #[test]
    fn lex_operators() {
        let tokens = lex("a + b * c == d != e").unwrap();
        assert_eq!(tokens[1], Token::Plus);
        assert_eq!(tokens[3], Token::Star);
        assert_eq!(tokens[5], Token::Eql);
        assert_eq!(tokens[7], Token::Neq);
    }

    #[test]
    fn lex_regex_matchers() {
        let tokens = lex(r#"{method=~"GET|POST", code!~"5.."}"#).unwrap();
        // 0:{  1:method  2:=~  3:"GET|POST"  4:,  5:code  6:!~  7:"5.."  8:}
        assert_eq!(tokens[2], Token::EqlRegex);
        assert_eq!(tokens[6], Token::NeqRegex);
    }

    #[test]
    fn lex_keywords() {
        let tokens = lex("sum by (host) (rate(m[5m]))").unwrap();
        assert_eq!(tokens[0], Token::Ident("sum".into()));
        assert_eq!(tokens[1], Token::By);
        // "5m" inside brackets: 5 is a number, m is an ident
        let five_idx = tokens
            .iter()
            .position(|t| *t == Token::Number(5.0))
            .unwrap();
        assert!(five_idx > 0);
    }

    #[test]
    fn lex_comparison_operators() {
        let tokens = lex("a < b <= c > d >= e").unwrap();
        assert_eq!(tokens[1], Token::Lss);
        assert_eq!(tokens[3], Token::Lte);
        assert_eq!(tokens[5], Token::Gtr);
        assert_eq!(tokens[7], Token::Gte);
    }

    #[test]
    fn lex_unterminated_string() {
        let err = lex(r#""hello"#).unwrap_err();
        assert!(err.msg.contains("unterminated"));
    }

    #[test]
    fn lex_escape_sequences() {
        // Standard escapes
        let tokens = lex(r#""\n\t\r\\\a\b\f\v""#).unwrap();
        match &tokens[0] {
            Token::String(s) => {
                assert_eq!(s, "\n\t\r\\\x07\x08\x0C\x0B");
            }
            other => panic!("expected String token, got {other:?}"),
        }
    }

    #[test]
    fn lex_backtick_raw_string() {
        // Backtick strings should NOT process escape sequences
        let tokens = lex(r#"`hello\nworld`"#).unwrap();
        match &tokens[0] {
            Token::String(s) => {
                assert_eq!(s, r"hello\nworld");
            }
            other => panic!("expected String token, got {other:?}"),
        }
    }
}
