//! What a webhook URL is allowed to reach.
//!
//! A trigger's `DELIVER webhook('…')` target is attacker-influenced whenever
//! trigger creation is exposed to a tenant, and the process making the request
//! sits inside the deployment's network. That is textbook SSRF, and the
//! validator that stood against it had four holes, each of which is a test
//! here:
//!
//! 1. **A bracketed IPv6 literal skipped the check entirely.** The host was
//!    taken as everything before the first `:`, so `https://[::1]/x` yielded
//!    the host `[`, which does not parse as an IP, so the private-address
//!    check never ran.
//! 2. **Userinfo was read as the host.** `https://evil.com@169.254.169.254/`
//!    yielded `evil.com@169.254.169.254`, which does not parse as an IP
//!    either — while the request would go to the metadata service.
//! 3. **Loopback was explicitly allowed.** `http://127.0.0.1:2375/` — the
//!    Docker socket — was a *permitted* target, written in as a dev
//!    convenience.
//! 4. **A decimal or hexadecimal IP is not spelled like an IP.**
//!    `http://2130706433/` is 127.0.0.1 to every resolver and was not
//!    recognised as one here.
//!
//! Name resolution is deliberately *not* part of this layer: `CREATE TRIGGER`
//! must not do network I/O, and a check done at parse time is stale by the
//! time the request is made. Resolution and address pinning happen where the
//! connection is made.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap

use chronix_streaming::signal::sql::parse_trigger_sql;

/// Does `CREATE TRIGGER … DELIVER webhook('<url>')` parse?
fn accepts(url: &str) -> bool {
    parse_trigger_sql(&format!(
        "CREATE TRIGGER t ON cpu WHEN value > 1.0 DELIVER webhook('{url}')"
    ))
    .is_ok()
}

#[test]
fn a_bracketed_ipv6_loopback_is_refused() {
    for url in [
        "https://[::1]/hook",
        "https://[::1]:8443/hook",
        "https://[fd00::1]/hook",
        "https://[fe80::1]/hook",
        "https://[::ffff:127.0.0.1]/hook",
    ] {
        assert!(!accepts(url), "{url} must be refused");
    }
}

#[test]
fn userinfo_must_not_be_mistaken_for_the_host() {
    for url in [
        "https://evil.com@169.254.169.254/latest/meta-data/",
        "https://user:pass@127.0.0.1/hook",
        "https://evil.com@[::1]/hook",
    ] {
        assert!(!accepts(url), "{url} must be refused");
    }
}

#[test]
fn loopback_is_not_a_permitted_target() {
    for url in [
        "http://127.0.0.1:2375/containers/json",
        "http://localhost:8500/v1/kv/",
        "https://127.0.0.1/hook",
        "http://127.1/hook",
        "http://0.0.0.0/hook",
    ] {
        assert!(!accepts(url), "{url} must be refused");
    }
}

#[test]
fn an_ip_written_as_a_number_is_still_an_ip() {
    // https, so these are refused for being loopback rather than for being
    // plaintext — otherwise the test would pass without the check existing.
    for url in [
        "https://2130706433/hook",   // 127.0.0.1, decimal
        "https://0x7f000001/hook",   // 127.0.0.1, hexadecimal
        "https://017700000001/hook", // 127.0.0.1, octal
        "https://192.168.1.1/hook",
        "https://0xa9fea9fe/hook", // 169.254.169.254
    ] {
        assert!(!accepts(url), "{url} must be refused");
    }
}

#[test]
fn the_cloud_metadata_services_are_refused() {
    for url in [
        "https://169.254.169.254/latest/meta-data/",
        "https://[fd00:ec2::254]/latest/meta-data/",
        "https://100.100.100.200/latest/meta-data/", // Alibaba
    ] {
        assert!(!accepts(url), "{url} must be refused");
    }
}

/// The point of the control is that ordinary webhooks still work.
#[test]
fn an_ordinary_https_endpoint_is_accepted() {
    for url in [
        "https://hooks.example.com/services/T000/B000/XXXX",
        "https://example.com:8443/alerts",
        "https://93.184.216.34/hook",
    ] {
        assert!(accepts(url), "{url} must be accepted");
    }
}

/// Plain HTTP leaks the signing payload; it is refused regardless of host.
#[test]
fn plain_http_is_refused() {
    assert!(!accepts("http://hooks.example.com/hook"));
    assert!(!accepts("ftp://example.com/hook"));
    assert!(!accepts("file:///etc/passwd"));
    assert!(!accepts("example.com/hook"));
}

// ── The DELIVER clause has one spelling ─────────────────────────────────
//
// The token loop ended in a bare `i += 1`, so anything it did not recognise
// was skipped in silence. Three consequences, all of them worse now that the
// clause routes:
//
// - `DELIVER slack` — a channel that does not exist — parsed to *no* targets,
//   and no targets means "every channel", so asking for one place delivered
//   everywhere.
// - `DELIVER lag` (a typo for `log`) did the same.
// - `DELIVER TO log` worked by accident, and this tree's own example used
//   that spelling while the documented grammar is `DELIVER log`.

/// A channel that does not exist must be refused, not skipped.
#[test]
fn an_unknown_delivery_target_is_refused() {
    for target in ["slack", "lag", "email", "TO log"] {
        let sql = format!("CREATE TRIGGER t ON cpu WHEN value > 1.0 DELIVER {target}");
        let Err(err) = parse_trigger_sql(&sql) else {
            panic!("`DELIVER {target}` must be refused");
        };
        let msg = err.to_string();
        assert!(
            msg.contains("log") && msg.contains("webhook"),
            "the error must list the channels that do exist, got: {msg}"
        );
    }
}

/// The one spelling still works, in both orders and with a trailing clause.
#[test]
fn the_documented_deliver_spellings_parse() {
    for sql in [
        "CREATE TRIGGER t ON cpu WHEN value > 1.0 DELIVER log",
        "CREATE TRIGGER t ON cpu WHEN value > 1.0 DELIVER log COOLDOWN INTERVAL '5m'",
        "CREATE TRIGGER t ON cpu WHEN value > 1.0 DELIVER webhook('https://a.example.com/h'), log",
        "CREATE TRIGGER t ON cpu WHEN value > 1.0",
    ] {
        assert!(parse_trigger_sql(sql).is_ok(), "{sql} must parse");
    }
}

/// A `DELIVER` with nothing after it is a mistake, not "everywhere".
#[test]
fn an_empty_deliver_clause_is_refused() {
    assert!(
        parse_trigger_sql("CREATE TRIGGER t ON cpu WHEN value > 1.0 DELIVER").is_err(),
        "an empty DELIVER clause must be refused"
    );
    assert!(
        parse_trigger_sql(
            "CREATE TRIGGER t ON cpu WHEN value > 1.0 DELIVER COOLDOWN INTERVAL '5m'"
        )
        .is_err(),
        "a DELIVER clause with no channel must be refused"
    );
}
