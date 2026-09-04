//! What a webhook is allowed to reach.
//!
//! A trigger's delivery target is attacker-influenced wherever trigger
//! creation is exposed to a tenant, and the process making the request sits
//! *inside* the deployment's network — with a cloud metadata service on a
//! link-local address, an unauthenticated Docker socket on loopback, and a
//! service mesh on RFC1918. That is server-side request forgery, and the URL
//! check is the control against it.
//!
//! # Two callers, one address rule
//!
//! [`validate_webhook_url`] guards the **untrusted** surface — the trigger
//! DSL, where the URL may come from a tenant. It is strict: `https` only, no
//! userinfo, no internal host name, no non-routable literal.
//!
//! [`resolve_allowed_addrs`] guards the **connection**, and is what the
//! delivery channel calls. It enforces the address rule — the part that is
//! actually about SSRF — and takes an explicit `allow_private_targets` flag
//! for the operator deliberately pointing a webhook at their own network. The
//! flag is named after what it grants, defaults to off, and the DSL never sets
//! it.
//!
//! # Parsing, and what this layer does not do
//!
//! URLs are parsed with the same WHATWG parser the HTTP client uses, so the
//! host the check sees is the host the request reaches — bracketed IPv6,
//! userinfo, and the decimal, octal and hexadecimal spellings of an IPv4
//! address all resolve the way the client will resolve them.
//!
//! Names are **not** resolved here. `CREATE TRIGGER` must not do network I/O,
//! and a resolution done at parse time is stale by the time the request is
//! made anyway. Resolution belongs where the connection is: the delivery
//! channel pins the client to the addresses it validated, so the name cannot
//! be re-pointed underneath it.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs};

use url::{Host, Url};

use crate::signal::error::{Result, SignalError};

/// Host suffixes that name something inside the deployment by convention.
///
/// Refused by name rather than by address, because the address is what an
/// attacker controls: these resolve to a loopback or a metadata service on
/// every platform that defines them, and a deployment that genuinely wants to
/// reach one can use the embedded delivery API.
const INTERNAL_SUFFIXES: [&str; 5] = [".localhost", ".local", ".internal", ".home.arpa", ".onion"];

/// Validate a webhook URL without touching the network.
///
/// # Errors
///
/// Returns `InvalidConfig` if the URL is unparseable, is not `https`, carries
/// userinfo, has no host, names an internal host by convention, or is an IP
/// literal in a range that is not routable on the public internet.
pub fn validate_webhook_url(raw: &str) -> Result<()> {
    let url = Url::parse(raw).map_err(|e| {
        SignalError::InvalidConfig(format!("webhook URL {raw:?} is not a URL: {e}"))
    })?;

    if url.scheme() != "https" {
        return Err(SignalError::InvalidConfig(format!(
            "webhook URL must use https, got {:?}. A webhook payload carries the \
             alert's contents and its HMAC signature, so plaintext is refused \
             rather than warned about; construct a `WebhookChannel` directly if \
             a deployment genuinely needs one.",
            url.scheme()
        )));
    }

    // Userinfo has no use in a webhook — credentials belong in a header — and
    // it is the oldest way there is to make a URL look like it points
    // somewhere it does not.
    if !url.username().is_empty() || url.password().is_some() {
        return Err(SignalError::InvalidConfig(
            "webhook URL must not carry userinfo: put credentials in a header".into(),
        ));
    }

    let Some(host) = url.host() else {
        return Err(SignalError::InvalidConfig("webhook URL has no host".into()));
    };

    match host {
        // An IP literal is judged here and now: there is nothing to resolve,
        // and the WHATWG parser has already normalised the decimal, octal and
        // hexadecimal spellings of an IPv4 address into one.
        Host::Ipv4(v4) => {
            if let Some(reason) = forbidden_v4(v4) {
                return Err(SignalError::InvalidConfig(format!(
                    "webhook URL must not target {v4}, which is {reason}"
                )));
            }
        }
        Host::Ipv6(v6) => {
            if let Some(reason) = forbidden_reason(IpAddr::V6(v6)) {
                return Err(SignalError::InvalidConfig(format!(
                    "webhook URL must not target [{v6}], which is {reason}"
                )));
            }
        }
        Host::Domain(name) => {
            let lower = name.to_ascii_lowercase();
            if lower == "localhost" || INTERNAL_SUFFIXES.iter().any(|s| lower.ends_with(s)) {
                return Err(SignalError::InvalidConfig(format!(
                    "webhook URL must not target the internal host {name:?}"
                )));
            }
            // The address this name resolves to is checked where the
            // connection is made, not here.
        }
    }

    Ok(())
}

/// Resolve `raw`'s host and return the addresses the client may connect to.
///
/// Every resolved address is checked; one bad address fails the whole target,
/// because which of them a client picks is not a choice the caller controls.
///
/// `allow_private_targets` waives the address rule for an embedded operator
/// pointing a webhook at their own network. It does not waive the userinfo or
/// scheme checks, and the trigger DSL never sets it.
///
/// # Errors
///
/// Returns `InvalidConfig` if the URL is unparseable, is not http or https,
/// carries userinfo, has no host, cannot be resolved, or — unless
/// `allow_private_targets` — resolves to an address that is not routable on
/// the public internet.
pub fn resolve_allowed_addrs(raw: &str, allow_private_targets: bool) -> Result<Vec<SocketAddr>> {
    let url = Url::parse(raw).map_err(|e| {
        SignalError::InvalidConfig(format!("webhook URL {raw:?} is not a URL: {e}"))
    })?;

    if url.scheme() != "https" && url.scheme() != "http" {
        return Err(SignalError::InvalidConfig(format!(
            "webhook URL must be http or https, got {:?}",
            url.scheme()
        )));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(SignalError::InvalidConfig(
            "webhook URL must not carry userinfo: put credentials in a header".into(),
        ));
    }

    let host = url
        .host_str()
        .ok_or_else(|| SignalError::InvalidConfig("webhook URL has no host".into()))?;
    let port = url.port_or_known_default().unwrap_or(443);

    let addrs: Vec<SocketAddr> = (host, port)
        .to_socket_addrs()
        .map_err(|e| {
            SignalError::InvalidConfig(format!("webhook host {host:?} does not resolve: {e}"))
        })?
        .collect();

    if addrs.is_empty() {
        return Err(SignalError::InvalidConfig(format!(
            "webhook host {host:?} resolved to no addresses"
        )));
    }

    if !allow_private_targets {
        for addr in &addrs {
            if let Some(reason) = forbidden_reason(addr.ip()) {
                return Err(SignalError::InvalidConfig(format!(
                    "webhook host {host:?} resolves to {}, which is {reason}. Set \
                     `WebhookConfig::allow_private_targets` to reach it deliberately.",
                    addr.ip()
                )));
            }
        }
    }

    Ok(addrs)
}

/// Why `ip` may not be a webhook target, or `None` if it may.
///
/// The rule is an allowlist in disguise: everything that is not globally
/// routable unicast is refused. Enumerating the dangerous ranges instead would
/// mean the check is only as good as the list, and the list is what keeps
/// being short by one — Alibaba's metadata service lives at `100.100.100.200`,
/// inside carrier-grade NAT space that nobody thinks of as private.
#[must_use]
pub fn forbidden_reason(ip: IpAddr) -> Option<&'static str> {
    match ip {
        IpAddr::V4(v4) => forbidden_v4(v4),
        IpAddr::V6(v6) => {
            // An IPv4 address wearing an IPv6 hat still reaches the IPv4
            // host. `::ffff:127.0.0.1` and `::127.0.0.1` are the two spellings,
            // and 6to4 (`2002::/16`) embeds one in its next 32 bits.
            if let Some(v4) = v6.to_ipv4_mapped() {
                return forbidden_v4(v4);
            }
            if let Some(v4) = v6.to_ipv4() {
                return forbidden_v4(v4);
            }
            let seg = v6.segments();
            if seg[0] == 0x2002 {
                let embedded = Ipv4Addr::new(
                    seg[1].to_be_bytes()[0],
                    seg[1].to_be_bytes()[1],
                    seg[2].to_be_bytes()[0],
                    seg[2].to_be_bytes()[1],
                );
                if let Some(reason) = forbidden_v4(embedded) {
                    return Some(reason);
                }
            }
            forbidden_v6(v6)
        }
    }
}

fn forbidden_v4(v4: Ipv4Addr) -> Option<&'static str> {
    let [a, b, ..] = v4.octets();
    if v4.is_loopback() {
        return Some("a loopback address");
    }
    if v4.is_unspecified() || a == 0 {
        return Some("in the unspecified range 0.0.0.0/8");
    }
    if v4.is_private() {
        return Some("a private address");
    }
    if v4.is_link_local() {
        // 169.254.169.254 is the AWS/GCP/Azure metadata service.
        return Some("a link-local address");
    }
    if a == 100 && (64..128).contains(&b) {
        // Alibaba Cloud's metadata service is 100.100.100.200.
        return Some("in carrier-grade NAT space 100.64.0.0/10");
    }
    if a == 198 && (b == 18 || b == 19) {
        return Some("in the benchmarking range 198.18.0.0/15");
    }
    if v4.is_multicast() {
        return Some("a multicast address");
    }
    if v4.is_broadcast() {
        return Some("the broadcast address");
    }
    if a >= 240 {
        return Some("in the reserved range 240.0.0.0/4");
    }
    None
}

fn forbidden_v6(v6: Ipv6Addr) -> Option<&'static str> {
    if v6.is_loopback() {
        return Some("a loopback address");
    }
    if v6.is_unspecified() {
        return Some("the unspecified address");
    }
    let seg = v6.segments();
    if (seg[0] & 0xfe00) == 0xfc00 {
        return Some("a unique-local address (fc00::/7)");
    }
    if (seg[0] & 0xffc0) == 0xfe80 {
        return Some("a link-local address (fe80::/10)");
    }
    if v6.is_multicast() {
        return Some("a multicast address");
    }
    if seg[0] == 0x0064 && seg[1] == 0xff9b {
        return Some("in NAT64 space (64:ff9b::/96)");
    }
    if seg[0] == 0x0100 && seg[1] == 0 {
        return Some("in the discard range 100::/64");
    }
    None
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    /// The DSL check and the connection check disagree about *scheme* on
    /// purpose, and must not disagree about anything else.
    #[test]
    fn the_connection_check_still_refuses_userinfo_and_odd_schemes() {
        assert!(resolve_allowed_addrs("ftp://example.com/hook", true).is_err());
        assert!(resolve_allowed_addrs("https://u:p@example.com/hook", true).is_err());
    }

    /// The opt-out is the only way to a private address, and it is opt-in.
    #[test]
    fn a_private_target_needs_the_flag() {
        let err = resolve_allowed_addrs("http://127.0.0.1:1/hook", false)
            .expect_err("loopback must be refused by default");
        assert!(
            err.to_string().contains("allow_private_targets"),
            "the error must name the way out, got: {err}"
        );
        assert!(resolve_allowed_addrs("http://127.0.0.1:1/hook", true).is_ok());
    }

    #[test]
    fn the_ranges_that_reach_inside_a_deployment_are_forbidden() {
        for s in [
            "127.0.0.1",
            "127.1.2.3",
            "0.0.0.0",
            "10.1.2.3",
            "172.16.0.1",
            "192.168.1.1",
            "169.254.169.254",
            "100.100.100.200",
            "198.18.0.1",
            "224.0.0.1",
            "255.255.255.255",
            "240.0.0.1",
            "::1",
            "::",
            "fd00::1",
            "fd00:ec2::254",
            "fe80::1",
            "ff02::1",
            "::ffff:127.0.0.1",
            "64:ff9b::7f00:1",
        ] {
            assert!(
                forbidden_reason(ip(s)).is_some(),
                "{s} must be forbidden as a webhook target"
            );
        }
    }

    #[test]
    fn ordinary_public_addresses_are_allowed() {
        for s in ["93.184.216.34", "1.1.1.1", "2606:4700:4700::1111"] {
            assert_eq!(
                forbidden_reason(ip(s)),
                None,
                "{s} must be allowed as a webhook target"
            );
        }
    }

    /// 6to4 carries an IPv4 address in its next 32 bits, so the v4 rules have
    /// to be applied to it.
    #[test]
    fn a_6to4_address_is_judged_by_the_address_it_embeds() {
        // 2002:7f00:0001:: encodes 127.0.0.1
        assert!(forbidden_reason(ip("2002:7f00:1::")).is_some());
        // 2002:5db8:d822:: encodes 93.184.216.34
        assert_eq!(forbidden_reason(ip("2002:5db8:d822::")), None);
    }
}
