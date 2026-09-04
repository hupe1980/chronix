#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! A library must not need a process-global crypto provider, and must not
//! install one.
//!
//! `reqwest` is built with `rustls-no-provider`, so it resolves the
//! *process-level* provider when a client is constructed and panics when none
//! is installed. Reaching for `CryptoProvider::install_default` from a library
//! makes that panic go away and introduces a worse problem: the first caller
//! wins, so a crate that installs one on its first webhook makes a later
//! `install_default` in the embedding application fail silently, and the
//! application gets a provider it did not choose.
//!
//! This runs in its own test binary precisely so that nothing else has
//! installed a provider first — which is the only way to assert "works from a
//! clean process".

use std::time::Duration;

use chronix_streaming::signal::{WebhookChannel, WebhookConfig};

#[test]
fn a_client_builds_with_no_provider_installed() {
    assert!(
        rustls::crypto::CryptoProvider::get_default().is_none(),
        "another test installed a provider — this one must run in a clean process"
    );

    // Constructing the channel builds a `reqwest::Client`, which is where the
    // panic would happen.
    // Loopback is a forbidden webhook target by default; this test is about
    // the TLS provider, not about the address rule, so it says so.
    let config = WebhookConfig::new("https://127.0.0.1:1/hook", "secret")
        .with_timeout(Duration::from_millis(200))
        .allow_private_targets(true);
    let channel =
        WebhookChannel::new(config).expect("a client must build without a global provider");
    let _ = &channel;

    assert!(
        rustls::crypto::CryptoProvider::get_default().is_none(),
        "the library installed a process-wide crypto provider"
    );
}
