//! mTLS client certificate authentication.
//!
//! Extracts client identity from X.509 certificate Common Name (CN)
//! or Subject Alternative Name (SAN) fields.
//!
//! # Certificate Revocation
//!
//! CRL/OCSP checking is not implemented in Chronix — revocation is an
//! infrastructure concern.  Use short-lived certificates (≤ 24 h TTL),
//! a service-mesh sidecar with CRL enforcement, or a reverse proxy
//! with `ssl_crl`.  See `docs/security.md` for recommended approaches.

use crate::auth::error::AuthError;

/// mTLS certificate validation configuration.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct MtlsConfig {
    /// Whether mTLS is enabled.
    pub enabled: bool,
    /// Extract identity from certificate CN (Common Name).
    pub use_cn: bool,
    /// Extract identity from SAN (Subject Alternative Name) DNS entries.
    pub use_san_dns: bool,
    /// Extract identity from SAN email entries.
    pub use_san_email: bool,
    /// Optional allowlist of acceptable CN values.
    ///
    /// When set, only certificates whose CN appears in this list are
    /// accepted.  When `None`, any syntactically valid CN is accepted.
    #[serde(default)]
    pub allowed_cns: Option<Vec<String>>,
    /// Maximum acceptable length for any identity string (CN / SAN).
    ///
    /// Defaults to 256 when absent (via [`MtlsConfig::max_identity_len`]).
    #[serde(default)]
    pub max_identity_len: Option<usize>,
}

/// Client identity extracted and verified from a TLS certificate.
///
/// Fields are private — instances can only be created through
/// [`MtlsValidator`] methods, which require TLS-layer inputs. This
/// prevents callers from fabricating identities via HTTP headers.
#[derive(Debug, Clone)]
pub struct CertificateIdentity {
    principal: String,
    source: CertIdentitySource,
}

impl CertificateIdentity {
    /// The verified principal identifier (CN, SAN DNS, or SAN email).
    #[must_use]
    pub fn principal(&self) -> &str {
        &self.principal
    }

    /// How the identity was extracted from the certificate.
    #[must_use]
    pub fn source(&self) -> &CertIdentitySource {
        &self.source
    }
}

/// How the certificate identity was determined.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CertIdentitySource {
    /// Extracted from Common Name (CN).
    CommonName,
    /// Extracted from SAN DNS entry.
    SanDns,
    /// Extracted from SAN email entry.
    SanEmail,
}

/// mTLS validator that extracts identity from client certificates.
#[derive(Debug, Clone)]
pub struct MtlsValidator {
    config: MtlsConfig,
}

impl MtlsValidator {
    /// Create a new mTLS validator.
    #[must_use]
    pub fn new(config: MtlsConfig) -> Self {
        Self { config }
    }

    /// Default maximum identity string length.
    /// RFC 1035: DNS labels ≤63, FQDNs ≤253. Using 253 as a
    /// realistic default for SAN DNS names / CN values.
    const DEFAULT_MAX_LEN: usize = 253;

    /// Effective maximum identity length from config.
    fn max_len(&self) -> usize {
        self.config
            .max_identity_len
            .unwrap_or(Self::DEFAULT_MAX_LEN)
    }

    /// Validate an identity string for sanity.
    ///
    /// Rejects empty, too-long, and strings containing control characters
    /// or path separators (which would be unusual in a valid CN/SAN and
    /// could indicate injection attempts).
    fn validate_identity(&self, value: &str, field: &str) -> Result<(), AuthError> {
        if value.is_empty() {
            return Err(AuthError::InvalidCertificate(format!("empty {field}")));
        }
        if value.len() > self.max_len() {
            return Err(AuthError::InvalidCertificate(format!(
                "{field} exceeds maximum length ({})",
                self.max_len()
            )));
        }
        if value.chars().any(char::is_control) {
            return Err(AuthError::InvalidCertificate(format!(
                "{field} contains control characters"
            )));
        }
        // Reject path separators — a valid CN/SAN should not contain these.
        if value.contains("..") || value.contains('/') || value.contains('\\') {
            return Err(AuthError::InvalidCertificate(format!(
                "{field} contains path separators"
            )));
        }
        Ok(())
    }

    /// Extract identity from a certificate CN.
    ///
    /// # Security contract
    ///
    /// The `cn` parameter **MUST** come from the TLS layer (e.g., parsed
    /// from the peer certificate by the TLS terminator).  Passing
    /// user-controlled strings directly (e.g., from an HTTP header)
    /// bypasses all certificate verification and is a critical security
    /// violation.
    ///
    /// When [`MtlsConfig::allowed_cns`] is set, only CN values present
    /// in the allowlist are accepted.
    pub fn extract_identity_from_cn(&self, cn: &str) -> Result<CertificateIdentity, AuthError> {
        if !self.config.enabled {
            return Err(AuthError::Config("mTLS not enabled".into()));
        }

        if !self.config.use_cn {
            return Err(AuthError::Config(
                "CN-based identity extraction not enabled".into(),
            ));
        }

        self.validate_identity(cn, "CN")?;

        // Enforce allowlist when configured.
        if let Some(ref allowed) = self.config.allowed_cns {
            if !allowed.iter().any(|a| a == cn) {
                return Err(AuthError::InvalidCertificate(format!(
                    "CN \"{cn}\" is not in the allowed list"
                )));
            }
        }

        Ok(CertificateIdentity {
            principal: cn.to_string(),
            source: CertIdentitySource::CommonName,
        })
    }

    /// Extract identity from a SAN DNS name.
    ///
    /// # Security contract
    ///
    /// The `dns_name` parameter **MUST** come from the TLS layer.
    pub fn extract_identity_from_san_dns(
        &self,
        dns_name: &str,
    ) -> Result<CertificateIdentity, AuthError> {
        if !self.config.enabled {
            return Err(AuthError::Config("mTLS not enabled".into()));
        }

        if !self.config.use_san_dns {
            return Err(AuthError::Config(
                "SAN DNS identity extraction not enabled".into(),
            ));
        }

        self.validate_identity(dns_name, "SAN DNS")?;

        Ok(CertificateIdentity {
            principal: dns_name.to_string(),
            source: CertIdentitySource::SanDns,
        })
    }

    /// Extract identity from a SAN email.
    ///
    /// # Security contract
    ///
    /// The `email` parameter **MUST** come from the TLS layer.
    pub fn extract_identity_from_san_email(
        &self,
        email: &str,
    ) -> Result<CertificateIdentity, AuthError> {
        if !self.config.enabled {
            return Err(AuthError::Config("mTLS not enabled".into()));
        }

        if !self.config.use_san_email {
            return Err(AuthError::Config(
                "SAN email identity extraction not enabled".into(),
            ));
        }

        self.validate_identity(email, "SAN email")?;

        Ok(CertificateIdentity {
            principal: email.to_string(),
            source: CertIdentitySource::SanEmail,
        })
    }

    /// Check if mTLS is enabled.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_cn_identity() {
        let validator = MtlsValidator::new(MtlsConfig {
            enabled: true,
            use_cn: true,
            use_san_dns: false,
            use_san_email: false,
            ..Default::default()
        });

        let identity = validator
            .extract_identity_from_cn("client.example.com")
            .unwrap();
        assert_eq!(identity.principal(), "client.example.com");
        assert_eq!(*identity.source(), CertIdentitySource::CommonName);
    }

    #[test]
    fn extract_san_dns_identity() {
        let validator = MtlsValidator::new(MtlsConfig {
            enabled: true,
            use_cn: false,
            use_san_dns: true,
            use_san_email: false,
            ..Default::default()
        });

        let identity = validator
            .extract_identity_from_san_dns("service.internal")
            .unwrap();
        assert_eq!(identity.principal(), "service.internal");
        assert_eq!(*identity.source(), CertIdentitySource::SanDns);
    }

    #[test]
    fn extract_san_email_identity() {
        let validator = MtlsValidator::new(MtlsConfig {
            enabled: true,
            use_cn: false,
            use_san_dns: false,
            use_san_email: true,
            ..Default::default()
        });

        let identity = validator
            .extract_identity_from_san_email("admin@chronix.io")
            .unwrap();
        assert_eq!(identity.principal(), "admin@chronix.io");
        assert_eq!(*identity.source(), CertIdentitySource::SanEmail);
    }

    #[test]
    fn disabled_mtls_rejects() {
        let validator = MtlsValidator::new(MtlsConfig {
            enabled: false,
            ..Default::default()
        });

        assert!(validator.extract_identity_from_cn("test").is_err());
    }

    #[test]
    fn empty_cn_rejected() {
        let validator = MtlsValidator::new(MtlsConfig {
            enabled: true,
            use_cn: true,
            ..Default::default()
        });

        assert!(validator.extract_identity_from_cn("").is_err());
    }

    #[test]
    fn cn_allowlist_accepts_listed() {
        let validator = MtlsValidator::new(MtlsConfig {
            enabled: true,
            use_cn: true,
            allowed_cns: Some(vec!["service-a".into(), "service-b".into()]),
            ..Default::default()
        });

        assert!(validator.extract_identity_from_cn("service-a").is_ok());
        assert!(validator.extract_identity_from_cn("service-b").is_ok());
    }

    #[test]
    fn cn_allowlist_rejects_unlisted() {
        let validator = MtlsValidator::new(MtlsConfig {
            enabled: true,
            use_cn: true,
            allowed_cns: Some(vec!["service-a".into()]),
            ..Default::default()
        });

        let err = validator
            .extract_identity_from_cn("evil-service")
            .unwrap_err();
        assert!(err.to_string().contains("not in the allowed list"));
    }

    #[test]
    fn cn_no_allowlist_accepts_any_valid() {
        let validator = MtlsValidator::new(MtlsConfig {
            enabled: true,
            use_cn: true,
            allowed_cns: None,
            ..Default::default()
        });

        assert!(validator.extract_identity_from_cn("anything.valid").is_ok());
    }

    #[test]
    fn rejects_path_separators() {
        let validator = MtlsValidator::new(MtlsConfig {
            enabled: true,
            use_cn: true,
            ..Default::default()
        });

        // Path traversal attempts
        assert!(validator.extract_identity_from_cn("../etc/passwd").is_err());
        assert!(validator.extract_identity_from_cn("foo/bar").is_err());
        assert!(validator.extract_identity_from_cn("foo\\bar").is_err());
        assert!(validator.extract_identity_from_cn("a..b").is_err());
    }

    #[test]
    fn rejects_control_characters() {
        let validator = MtlsValidator::new(MtlsConfig {
            enabled: true,
            use_cn: true,
            ..Default::default()
        });

        assert!(validator.extract_identity_from_cn("test\0null").is_err());
        assert!(validator.extract_identity_from_cn("test\nnewline").is_err());
    }

    #[test]
    fn rejects_overly_long_cn() {
        let validator = MtlsValidator::new(MtlsConfig {
            enabled: true,
            use_cn: true,
            max_identity_len: Some(20),
            ..Default::default()
        });

        let long_cn = "a".repeat(21);
        let err = validator.extract_identity_from_cn(&long_cn).unwrap_err();
        assert!(err.to_string().contains("exceeds maximum length"));

        // Exactly at limit should pass
        let ok_cn = "a".repeat(20);
        assert!(validator.extract_identity_from_cn(&ok_cn).is_ok());
    }

    #[test]
    fn san_dns_validated() {
        let validator = MtlsValidator::new(MtlsConfig {
            enabled: true,
            use_san_dns: true,
            ..Default::default()
        });

        assert!(validator.extract_identity_from_san_dns("").is_err());
        assert!(validator.extract_identity_from_san_dns("../evil").is_err());
        assert!(validator.extract_identity_from_san_dns("good.host").is_ok());
    }

    #[test]
    fn san_email_validated() {
        let validator = MtlsValidator::new(MtlsConfig {
            enabled: true,
            use_san_email: true,
            ..Default::default()
        });

        assert!(validator.extract_identity_from_san_email("").is_err());
        assert!(validator
            .extract_identity_from_san_email("user\0@evil.com")
            .is_err());
        assert!(validator
            .extract_identity_from_san_email("admin@chronix.io")
            .is_ok());
    }
}
