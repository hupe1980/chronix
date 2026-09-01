//! TLS configuration for HTTP and gRPC endpoints.
//!
//! ## Certificate Rotation
//!
//! TLS certificates loaded by this module are **not** refreshed automatically
//! at the point of load. However, the HTTP endpoint supports **hot-reload**
//! via the companion [`tls_watcher`](crate::tls_watcher) module, which polls
//! cert/key file modification times and atomically swaps the live
//! `rustls::ServerConfig` when changes are detected. Enable it by setting
//! `--tls-reload-interval` (or `CHRONIXD_TLS_RELOAD_INTERVAL`) to a
//! non-zero number of seconds.
//!
//! The gRPC and Flight SQL (tonic) endpoints use a custom
//! [`ReloadableCertResolver`] that implements `rustls::server::ResolvesServerCert`.
//! When the TLS watcher detects file changes it atomically swaps the
//! `CertifiedKey` inside the resolver, so new gRPC/Flight connections
//! immediately use the rotated certificate — no restart required.
//!
//! ## TLS reload is disabled by default
//!
//! The `reload_interval_secs` field in [`TlsConfig`]
//! defaults to **0**, meaning hot-reload is **silently disabled** unless
//! explicitly configured. Operators must set `--tls-reload-interval` to a
//! non-zero value (e.g. 300 for 5-minute polling) to enable automatic
//! certificate rotation. Without this, certificate renewal (e.g. via
//! cert-manager) requires a full process restart.

use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use parking_lot::RwLock;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::ResolvesServerCert;
use rustls::sign::CertifiedKey;
use tonic::transport::{Certificate, ClientTlsConfig, Identity};
use tracing::warn;

use crate::config::{ClusterTlsConfig, TlsConfig};

/// Install rustls's process-level crypto provider, once.
///
/// `rustls::ServerConfig::builder()` resolves the process-level
/// [`CryptoProvider`](rustls::crypto::CryptoProvider). When exactly one
/// provider is compiled in it picks that one; when **two** are and none has
/// been installed, it cannot choose and panics — taking the server down at the
/// first TLS setup rather than returning an error.
///
/// That is not hypothetical: `axum-server`'s `tls-rustls` feature enables
/// `rustls/aws-lc-rs` while this workspace asks for `ring`, so every TLS
/// deployment of `chronixd` aborted with "Could not automatically determine
/// the process-level CryptoProvider". The features are now unambiguous
///, and this call is the belt to that pair of braces: a future
/// dependency that re-enables a second provider cannot silently bring the
/// panic back. Installing is idempotent and losing the race is not an error.
///
/// Called at process start rather than only from the TLS path: `reqwest` is
/// built with `rustls-no-provider` and resolves the process-level provider
/// when a `Client` is constructed, so an outbound HTTPS request panics
/// without one even on a server that serves plaintext.
pub fn ensure_crypto_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

/// Load TLS configuration from PEM files and return an `Arc<ServerConfig>`.
///
/// When `config.client_ca` is set, the server will require mutual TLS:
/// connecting clients must present a certificate signed by the specified CA.
///
/// # Errors
///
/// Returns an error if the cert or key files cannot be read or parsed.
pub fn load_rustls_config(config: &TlsConfig) -> Result<Arc<rustls::ServerConfig>, TlsError> {
    ensure_crypto_provider();
    let certs = load_certs(&config.cert)?;
    let key = load_private_key(&config.key)?;

    let tls_config = if let Some(ref ca_path) = config.client_ca {
        // Mutual TLS: verify client certificates against the provided CA.
        let ca_certs = load_certs(ca_path)?;
        let mut root_store = rustls::RootCertStore::empty();
        for cert in ca_certs {
            root_store
                .add(cert)
                .map_err(|e| TlsError::Config(format!("invalid CA cert: {e}")))?;
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
            .build()
            .map_err(|e| TlsError::Config(format!("client verifier: {e}")))?;
        rustls::ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)
            .map_err(|e| TlsError::Config(e.to_string()))?
    } else {
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| TlsError::Config(e.to_string()))?
    };

    // NOTE: rustls 0.23+ defaults to TLS 1.2+ (TLS 1.0/1.1 are not
    // supported). No additional protocol_versions configuration is
    // required.

    Ok(Arc::new(tls_config))
}

/// Load a `tonic` TLS identity from PEM files for the gRPC server.
///
/// When `config.client_ca` is set, enables mutual TLS on the gRPC endpoint.
///
/// # Errors
///
/// Returns an error if the cert or key files cannot be read.
pub fn load_tonic_tls_config(
    config: &TlsConfig,
) -> Result<tonic::transport::ServerTlsConfig, TlsError> {
    let cert_pem = std::fs::read(&config.cert)
        .map_err(|e| TlsError::Io(config.cert.display().to_string(), e))?;
    let key_pem = std::fs::read(&config.key)
        .map_err(|e| TlsError::Io(config.key.display().to_string(), e))?;

    let identity = tonic::transport::Identity::from_pem(cert_pem, key_pem);

    let mut tls = tonic::transport::ServerTlsConfig::new().identity(identity);

    if let Some(ref ca_path) = config.client_ca {
        let ca_pem =
            std::fs::read(ca_path).map_err(|e| TlsError::Io(ca_path.display().to_string(), e))?;
        tls = tls.client_ca_root(Certificate::from_pem(ca_pem));
    }

    Ok(tls)
}

/// Load PEM certificates from a file.
fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let file =
        std::fs::File::open(path).map_err(|e| TlsError::Io(path.display().to_string(), e))?;
    let mut reader = BufReader::new(file);

    let mut certs: Vec<CertificateDer<'static>> = Vec::new();
    for (i, result) in CertificateDer::pem_reader_iter(&mut reader).enumerate() {
        match result {
            Ok(cert) => certs.push(cert),
            Err(e) => {
                warn!(
                    path = %path.display(),
                    index = i,
                    error = %e,
                    "skipping malformed certificate in PEM file"
                );
            }
        }
    }

    if certs.is_empty() {
        return Err(TlsError::Config(format!(
            "no certificates found in {}",
            path.display()
        )));
    }

    Ok(certs)
}

/// Load a PEM private key from a file.
fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsError> {
    let file =
        std::fs::File::open(path).map_err(|e| TlsError::Io(path.display().to_string(), e))?;
    let mut reader = BufReader::new(file);

    // Accepts PKCS#8, PKCS#1 (RSA) and SEC1 (EC) alike.
    let key = PrivateKeyDer::from_pem_reader(&mut reader).map_err(|e| {
        TlsError::Config(format!(
            "failed to read private key from {}: {e}",
            path.display()
        ))
    })?;

    Ok(key)
}

/// Load cluster mTLS client configuration from PEM files.
///
/// Returns a `tonic::transport::ClientTlsConfig` configured with:
/// The CA certificate for verifying peer server certs
/// The client identity (cert + key) for mutual authentication
///
/// # Errors
///
/// Returns an error if any PEM files cannot be read.
pub fn load_cluster_client_tls(config: &ClusterTlsConfig) -> Result<ClientTlsConfig, TlsError> {
    let ca_pem = std::fs::read(&config.ca_cert)
        .map_err(|e| TlsError::Io(config.ca_cert.display().to_string(), e))?;
    let cert_pem = std::fs::read(&config.cert)
        .map_err(|e| TlsError::Io(config.cert.display().to_string(), e))?;
    let key_pem = std::fs::read(&config.key)
        .map_err(|e| TlsError::Io(config.key.display().to_string(), e))?;

    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(ca_pem))
        .identity(Identity::from_pem(cert_pem, key_pem));

    Ok(tls)
}

/// Load cluster mTLS server configuration from PEM files.
///
/// Returns a `tonic::transport::ServerTlsConfig` configured with:
/// The server identity (cert + key)
/// The CA certificate for verifying client certs (mutual TLS)
///
/// # Errors
///
/// Returns an error if any PEM files cannot be read.
pub fn load_cluster_server_tls(
    config: &ClusterTlsConfig,
) -> Result<tonic::transport::ServerTlsConfig, TlsError> {
    let ca_pem = std::fs::read(&config.ca_cert)
        .map_err(|e| TlsError::Io(config.ca_cert.display().to_string(), e))?;
    let cert_pem = std::fs::read(&config.cert)
        .map_err(|e| TlsError::Io(config.cert.display().to_string(), e))?;
    let key_pem = std::fs::read(&config.key)
        .map_err(|e| TlsError::Io(config.key.display().to_string(), e))?;

    let tls = tonic::transport::ServerTlsConfig::new()
        .identity(Identity::from_pem(cert_pem, key_pem))
        .client_ca_root(Certificate::from_pem(ca_pem));

    Ok(tls)
}

// ── Reloadable gRPC/Flight TLS ────────────────────────────────

/// A certificate resolver that can be atomically updated at runtime,
/// enabling TLS hot-reload for tonic-based servers (gRPC, Flight SQL).
#[derive(Debug)]
pub struct ReloadableCertResolver {
    inner: RwLock<Option<Arc<CertifiedKey>>>,
}

impl ReloadableCertResolver {
    /// Create a new resolver pre-loaded with the given certified key.
    pub fn new(key: Arc<CertifiedKey>) -> Arc<Self> {
        Arc::new(Self {
            inner: RwLock::new(Some(key)),
        })
    }

    /// Atomically replace the certificate used for new connections.
    pub fn update(&self, key: Arc<CertifiedKey>) {
        *self.inner.write() = Some(key);
    }
}

impl ResolvesServerCert for ReloadableCertResolver {
    fn resolve(&self, _client_hello: rustls::server::ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        self.inner.read().clone()
    }
}

/// Build a [`CertifiedKey`] from PEM cert and key files.
pub fn load_certified_key(config: &TlsConfig) -> Result<Arc<CertifiedKey>, TlsError> {
    let certs = load_certs(&config.cert)?;
    let key = load_private_key(&config.key)?;
    let signing_key = rustls::crypto::aws_lc_rs::sign::any_supported_type(&key)
        .map_err(|e| TlsError::Config(format!("unsupported private key type: {e}")))?;
    Ok(Arc::new(CertifiedKey::new(certs, signing_key)))
}

/// Build a reloadable TLS acceptor for gRPC / Flight SQL.
///
/// Returns the `TlsAcceptor` and the shared `ReloadableCertResolver`.
/// The resolver can be passed to [`crate::tls_watcher::spawn_tls_watcher`]
/// so certificate rotations automatically apply to gRPC connections too.
pub fn build_reloadable_grpc_tls(
    config: &TlsConfig,
) -> Result<(tokio_rustls::TlsAcceptor, Arc<ReloadableCertResolver>), TlsError> {
    ensure_crypto_provider();
    let certified_key = load_certified_key(config)?;
    let resolver = ReloadableCertResolver::new(certified_key);

    let server_config = if let Some(ref ca_path) = config.client_ca {
        let ca_certs = load_certs(ca_path)?;
        let mut root_store = rustls::RootCertStore::empty();
        for cert in ca_certs {
            root_store
                .add(cert)
                .map_err(|e| TlsError::Config(format!("invalid CA cert: {e}")))?;
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
            .build()
            .map_err(|e| TlsError::Config(format!("client verifier: {e}")))?;
        rustls::ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_cert_resolver(resolver.clone())
    } else {
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_cert_resolver(resolver.clone())
    };

    // gRPC requires HTTP/2 ALPN negotiation.
    let mut cfg = server_config;
    cfg.alpn_protocols = vec![b"h2".to_vec()];

    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(cfg));
    Ok((acceptor, resolver))
}

/// A TLS-wrapped TCP stream that implements tonic's `Connected` trait.
///
/// Used to feed manually-accepted TLS connections into tonic's
/// `serve_with_incoming_shutdown`, enabling hot-reloadable gRPC/Flight TLS.
pub struct GrpcTlsStream {
    inner: tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
    remote_addr: std::net::SocketAddr,
}

impl GrpcTlsStream {
    /// Wrap an accepted TLS stream together with the peer address.
    pub fn new(
        inner: tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
        remote_addr: std::net::SocketAddr,
    ) -> Self {
        Self { inner, remote_addr }
    }
}

/// Connection metadata for TLS-wrapped gRPC connections.
#[derive(Clone, Debug)]
pub struct GrpcTlsConnectInfo {
    /// Address of the remote peer.
    pub remote_addr: std::net::SocketAddr,
}

impl tonic::transport::server::Connected for GrpcTlsStream {
    type ConnectInfo = GrpcTlsConnectInfo;

    fn connect_info(&self) -> GrpcTlsConnectInfo {
        GrpcTlsConnectInfo {
            remote_addr: self.remote_addr,
        }
    }
}

impl tokio::io::AsyncRead for GrpcTlsStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for GrpcTlsStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// TLS configuration errors.
#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    /// I/O error reading TLS files.
    #[error("failed to read {0}: {1}")]
    Io(String, #[source] std::io::Error),
    /// TLS configuration error.
    #[error("TLS configuration error: {0}")]
    Config(String),
}

#[cfg(test)]
mod pem_tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    //! Coverage for the PEM loaders.
    //!
    //! These had no tests at all, which mattered when the parsing moved off
    //! `rustls-pemfile` (unmaintained; its functionality now lives in
    //! `rustls-pki-types`, RUSTSEC-2025-0134). A certificate loader that
    //! silently returns the wrong thing does not fail loudly — it fails as a
    //! handshake rejection at the far end of a deployment.
    //!
    //! The keys below are throwaway fixtures generated for this test file and
    //! are not used to secure anything.

    use super::*;
    use std::io::Write;

    const TEST_CERT: &str = "\
-----BEGIN CERTIFICATE-----
MIIDETCCAfmgAwIBAgIUQa9c/JA5YqKNuXyFsk+cs3uPOQEwDQYJKoZIhvcNAQEL
BQAwFzEVMBMGA1UEAwwMY2hyb25peC10ZXN0MCAXDTI2MDgzMTA5Mzc0OFoYDzIx
MjYwODA3MDkzNzQ4WjAXMRUwEwYDVQQDDAxjaHJvbml4LXRlc3QwggEiMA0GCSqG
SIb3DQEBAQUAA4IBDwAwggEKAoIBAQCi/waq/QbOPWZ+JJrM6ZFGRUtZLghDaATb
kYpiNFDXAliz1AKxIo3gV4kTQkuzNZHNSKr45NYReqenQSbVteROvymyZa/CVHeF
UkELLd6Kiw2vEtdvZxwS4xjiQSlYCjo9deObIh5IRIGvb09Dn53POAHUv8YTVm0y
sU/JDaFk38jQuzRoQ5IESC6IU5H5Qek3jygtn4ii126kAL5yrQ7GUBOsee3ENz7N
tXsqCFNXAzmCGM3qQ48KdAReTyQ6TmvXjo1NX78KmDzqZh+l8XJeMz2eYQd61Cvt
3lxIEKPf9t6gXPzkjacbWNjcGADX9mPemwVAetdfWw+U7P9f49hFAgMBAAGjUzBR
MB0GA1UdDgQWBBSUApIX4/RiQWLgN6FrjYNmZLvJ0TAfBgNVHSMEGDAWgBSUApIX
4/RiQWLgN6FrjYNmZLvJ0TAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUA
A4IBAQAaUnv3J4gWMLkurwCq1Ibc5teTdCQDh9u8q22Ry0pzsBak/pRAx58iWPA7
WvljVoMf8BnNLvWY2gVF3GLBArvWJ/S8fNM3yoNK+7hh7MBbrXkxHaEQe0zcsWFi
Ra2RevxDYxNLZyaptRnvDmzi9sRGhKx3oV51tiVWUkBsMAbUD9rL+duHP44TwLgD
8EKUl89Xblnh5ODH8+EVkpTZE0iCWJCozq0e8WM3woJbKKpko/lk77FcX6tgSQuz
CzNzmGQC+E1kYIVznz1Y+W4nIwDfjHH0jVFHRAT7gZRlrjJwIcXfUEekQY6VZNC4
KoWChVh0q1upw9BXSWpEUg0GjjVU
-----END CERTIFICATE-----";

    /// PKCS#8 — what `openssl req -newkey` emits by default.
    const TEST_KEY_PKCS8: &str = "\
-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCi/waq/QbOPWZ+
JJrM6ZFGRUtZLghDaATbkYpiNFDXAliz1AKxIo3gV4kTQkuzNZHNSKr45NYReqen
QSbVteROvymyZa/CVHeFUkELLd6Kiw2vEtdvZxwS4xjiQSlYCjo9deObIh5IRIGv
b09Dn53POAHUv8YTVm0ysU/JDaFk38jQuzRoQ5IESC6IU5H5Qek3jygtn4ii126k
AL5yrQ7GUBOsee3ENz7NtXsqCFNXAzmCGM3qQ48KdAReTyQ6TmvXjo1NX78KmDzq
Zh+l8XJeMz2eYQd61Cvt3lxIEKPf9t6gXPzkjacbWNjcGADX9mPemwVAetdfWw+U
7P9f49hFAgMBAAECggEAMDCWGBeW24Lrun+4BL1JZi02ibdCWit2xDPTZhVxkR/w
ebpE0XoV2C4JKNiA6Qr9gGPrqIp/f8tzpc9tW+HbDi0Wdtf5jsrKS3B2Kof1M6DI
+unnJ9ikRDFAOiRpxM3BSkqAcG015sfaT7PpC2U1kv5MDEpmlXEH9+TUYezVuqy2
Hg29Y2tfwCkZdqQMq7Z2h7G3ML5pJ5VFtMkVxQnUUlCcKT++UFKeZ3Dzkj9e8Thj
dIDOjDmx82OdbBpL5+wjmhLuDAPvU7Vjpd6nePskAzLqeNhfE3+fqr3hSEJkv9wH
q65X1Z2ZTXHIrtdGuuQE3ap/vysTsa7qT+Q/Iu2EQQKBgQDU89xIXM+mTAvopyqw
XQVls41zZg7M4OEgzZ4LnfzzSRfg68PaGlExFcfm0AZ/oUM9Pft5DbS8S/zduZ02
5UcLymPxZtn1S+raaoZDl2ZRg4aaxyqwUYL7X6E0+kK1LFV1geXVJS+sUSdLvpWg
YyTKA6frj4GVSET4Wizz8O1hfwKBgQDD8fWaAgkfyC9ALN9FmoD56+eCAxra9wsO
f055/X4YKGJ/W+3/KE0FITYncMyKIvHQIB77ev7jD1s8a7pG7z3DU0NyRP1/3ebJ
gwPU+tliaKP/6SSwouMCwdoPwgX95HPccGhPpiOmFfAmKEp+qEHa8V2tiltO0RRz
1iUaxUGgOwKBgE5vn+R7YvSCsCQ6ZmvdZ16FWwV1QuBNuD5H3f2zbHcDpirvTA0q
gltNBXtLhgk+kbCeAuEcnkR4zKOyeWi93IRIQLWqx38lPlTCxb9hpYtCobKix2N4
MoF6QLttrmJi+Ps2JDx03PFgVtP8V8pimitaW2BVVEpy+wxq0oHrbNPrAoGASG3K
7DeuaboUhTNRAKiA3mDt/WfqaGADDUPPnVYvYvyilBNGIRgjiC7jlqaiZLlQCy+k
ZC7twr6taeMkQw4yFV5UwwtvbPF4Wqp8IyDqc+7cGD902XoMUbuYQFTm5BerICPI
2xA9gyn+0Av6pWDKnwSzLO/Empi8Z6kTpBagEHsCgYAA1if0XB9yzqepdb/EWHRs
uMpOKw/Vf1dAyFpzjyZoTfXHxqjcGlT903/5y30LfNzY0HOl/B6C74z/Qv5K86Md
4VY+dHUPon1iuosHde35eSuPJroTX7qNkGAeZUr3xNmt6e6eGoElvgdnXoS2dSuB
6DOxB5VC1kafu7/NeEAoUQ==
-----END PRIVATE KEY-----";

    /// SEC1 — what `openssl ecparam -genkey` emits. A different PEM label, and
    /// the case a loader that only understands PKCS#8 gets wrong.
    const TEST_KEY_SEC1: &str = "\
-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIG9x5sr5ZPaxlGvpvMJr0q1YuZCcNih+QGL6/MF6uiq4oAoGCCqGSM49
AwEHoUQDQgAE6eu/vqrZmNSiCj3c0ZpyxyFt/kpgHNPuWNhSUNItF1kfqKxhdShm
vczshR4c1VdCn2XFZ2br9PDl/yT/CM3ZwQ==
-----END EC PRIVATE KEY-----";

    fn write_temp(contents: &str, suffix: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::Builder::new().suffix(suffix).tempfile().unwrap();
        f.write_all(contents.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    #[test]
    fn loads_a_certificate_chain() {
        let f = write_temp(TEST_CERT, ".pem");
        let certs = load_certs(f.path()).expect("a valid PEM certificate must load");
        assert_eq!(certs.len(), 1);
        assert!(!certs[0].is_empty(), "the DER body must not be empty");
    }

    #[test]
    fn loads_every_certificate_in_a_chain() {
        let chain = format!("{TEST_CERT}\n{TEST_CERT}");
        let f = write_temp(&chain, ".pem");
        let certs = load_certs(f.path()).unwrap();
        assert_eq!(certs.len(), 2, "a chain must not be truncated to its leaf");
    }

    #[test]
    fn loads_a_pkcs8_private_key() {
        let f = write_temp(TEST_KEY_PKCS8, ".pem");
        let key = load_private_key(f.path()).expect("a PKCS#8 key must load");
        assert!(matches!(key, PrivateKeyDer::Pkcs8(_)), "got {key:?}");
    }

    #[test]
    fn loads_a_sec1_private_key() {
        let f = write_temp(TEST_KEY_SEC1, ".pem");
        let key = load_private_key(f.path()).expect("a SEC1 EC key must load");
        assert!(matches!(key, PrivateKeyDer::Sec1(_)), "got {key:?}");
    }

    /// Building a `rustls::ServerConfig` must not panic.
    ///
    /// Every one of these loaders ends in `rustls::ServerConfig::builder()`,
    /// which resolves the process-level `CryptoProvider`. When more than one
    /// provider is compiled in and none has been installed, rustls cannot pick
    /// and **panics** — so the whole TLS surface is decided by the crate
    /// features of a transitive dependency, and nothing in this file reached
    /// far enough to notice. The PEM tests stop one call short of it.
    #[test]
    fn a_server_config_can_actually_be_built() {
        let cert = write_temp(TEST_CERT, ".pem");
        let key = write_temp(TEST_KEY_PKCS8, ".pem");
        let config = TlsConfig {
            cert: cert.path().to_path_buf(),
            key: key.path().to_path_buf(),
            client_ca: None,
            reload_interval_secs: 0,
        };
        load_rustls_config(&config).expect("a server config must build");
    }

    /// The same for the gRPC / Flight acceptor, which reaches
    /// `ServerConfig::builder()` by a different route.
    #[test]
    fn a_grpc_acceptor_can_actually_be_built() {
        let cert = write_temp(TEST_CERT, ".pem");
        let key = write_temp(TEST_KEY_PKCS8, ".pem");
        let config = TlsConfig {
            cert: cert.path().to_path_buf(),
            key: key.path().to_path_buf(),
            client_ca: None,
            reload_interval_secs: 0,
        };
        build_reloadable_grpc_tls(&config).expect("a gRPC TLS acceptor must build");
    }

    #[test]
    fn a_file_with_no_certificate_is_an_error_not_an_empty_chain() {
        let f = write_temp("not a pem file at all\n", ".pem");
        let err = load_certs(f.path()).expect_err("garbage must not load as an empty chain");
        assert!(
            err.to_string().contains("no certificates found"),
            "got: {err}"
        );
    }

    #[test]
    fn a_file_with_no_key_is_an_error() {
        let f = write_temp(TEST_CERT, ".pem");
        assert!(
            load_private_key(f.path()).is_err(),
            "a certificate is not a private key"
        );
    }

    #[test]
    fn a_missing_file_is_reported_with_its_path() {
        let err = load_certs(std::path::Path::new("/nonexistent/chronix-test.pem"))
            .expect_err("a missing file must be an error");
        assert!(err.to_string().contains("chronix-test.pem"), "got: {err}");
    }
}
