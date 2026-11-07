//! Validated PostgreSQL listener configuration.
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, io, net::SocketAddr};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TlsMode {
    Disabled,
    Optional,
    Required,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> PostgresqlConfig {
        serde_json::from_str(r#"{"listen":"127.0.0.1:5432","tls":"disabled"}"#).unwrap()
    }

    #[test]
    fn serde_defaults_and_unknown_fields() {
        let config = config();
        assert_eq!(config.databases, ["postgres"]);
        assert_eq!(config.connections, 32);
        assert!(!config.loopback_trust);
        assert!(config.certificates.is_empty());
        assert!(config.certificate.is_none());
        assert!(config.private_key.is_none());
        assert!(config.client_ca.is_none());
        assert!(config.build().is_ok());
        assert!(
            serde_json::from_str::<PostgresqlConfig>(
                r#"{"listen":"127.0.0.1:5432","tls":"disabled","typo":true}"#
            )
            .is_err()
        );
    }

    #[test]
    fn validation_rejects_limits_names_and_tls_combinations_before_io() {
        for connections in [0, 129] {
            let mut config = config();
            config.connections = connections;
            assert!(config.build().is_err());
        }
        for databases in [
            vec![],
            vec!["postgres".into(); 17],
            vec!["".into()],
            vec!["a\0b".into()],
            vec!["a".repeat(64)],
        ] {
            let mut config = config();
            config.databases = databases;
            assert!(config.build().is_err());
        }
        for tls in [TlsMode::Optional, TlsMode::Required] {
            let mut config = config();
            config.tls = tls;
            assert!(
                config
                    .build()
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("requires certificate")
            );
        }
        for field in ["certificate", "private_key", "client_ca"] {
            let mut value = serde_json::to_value(config()).unwrap();
            value[field] = "unused.pem".into();
            assert!(
                serde_json::from_value::<PostgresqlConfig>(value)
                    .unwrap()
                    .build()
                    .is_err()
            );
        }
        for (fingerprint, role) in [
            ("A".repeat(64), "owner".into()),
            ("a".repeat(63), "owner".into()),
            ("a".repeat(64), "".into()),
            ("a".repeat(64), "a\0b".into()),
            ("a".repeat(64), "a".repeat(64)),
        ] {
            let mut config = config();
            config.tls = TlsMode::Required;
            config.certificate = Some("unused.pem".into());
            config.private_key = Some("unused.key".into());
            config.client_ca = Some("unused-ca.pem".into());
            config.certificates.insert(fingerprint, role);
            let error = config.build().err().unwrap();
            assert!(
                error.to_string().contains("fingerprints")
                    || error.to_string().contains("role names")
            );
        }
        let mut config = config();
        config.certificates.insert("a".repeat(64), "owner".into());
        assert!(
            config
                .build()
                .err()
                .unwrap()
                .to_string()
                .contains("require TLS and client_ca")
        );
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresqlConfig {
    pub listen: SocketAddr,
    pub tls: TlsMode,
    #[serde(default)]
    pub certificate: Option<String>,
    #[serde(default)]
    pub private_key: Option<String>,
    #[serde(default)]
    pub client_ca: Option<String>,
    /// SHA-256 client certificate fingerprints mapped to PostgreSQL role names.
    #[serde(default)]
    pub certificates: BTreeMap<String, String>,
    #[serde(default = "default_databases")]
    pub databases: Vec<String>,
    #[serde(default)]
    pub loopback_trust: bool,
    #[serde(default = "default_connections")]
    pub connections: usize,
}

fn default_databases() -> Vec<String> {
    vec!["postgres".into()]
}

fn default_connections() -> usize {
    32
}

fn valid_name(name: &str) -> bool {
    (1..=63).contains(&name.len()) && !name.contains('\0')
}

impl PostgresqlConfig {
    pub fn build(&self) -> io::Result<crate::auth::listener::PostgresListener> {
        match self.tls {
            TlsMode::Disabled => {
                if self.certificate.is_some()
                    || self.private_key.is_some()
                    || self.client_ca.is_some()
                {
                    return Err(io::Error::other(
                        "disabled PostgreSQL TLS cannot configure certificate, private_key, or client_ca",
                    ));
                }
            }
            TlsMode::Optional | TlsMode::Required => {
                if self.certificate.is_none() || self.private_key.is_none() {
                    return Err(io::Error::other(
                        "PostgreSQL TLS requires certificate and private_key",
                    ));
                }
            }
        }
        if !self.certificates.is_empty()
            && (self.tls == TlsMode::Disabled || self.client_ca.is_none())
        {
            return Err(io::Error::other(
                "PostgreSQL certificate mappings require TLS and client_ca",
            ));
        }
        for (fingerprint, role) in &self.certificates {
            if fingerprint.len() != 64
                || !fingerprint
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            {
                return Err(io::Error::other(
                    "PostgreSQL certificate fingerprints must be 64 lowercase hexadecimal characters",
                ));
            }
            if !valid_name(role) {
                return Err(io::Error::other(
                    "PostgreSQL mapped role names must be 1..=63 bytes without NUL",
                ));
            }
        }
        if !(1..=16).contains(&self.databases.len()) {
            return Err(io::Error::other(
                "PostgreSQL databases must contain 1..=16 names",
            ));
        }
        if self.databases.iter().any(|name| !valid_name(name)) {
            return Err(io::Error::other(
                "PostgreSQL database names must be 1..=63 bytes without NUL",
            ));
        }
        if !(1..=128).contains(&self.connections) {
            return Err(io::Error::other("PostgreSQL connections must be 1..=128"));
        }
        let tls_config = match self.tls {
            TlsMode::Disabled => None,
            TlsMode::Optional | TlsMode::Required => Some(crate::tls::server_optional_client(
                crate::tls::certs(self.certificate.as_ref().expect("validated certificate"))?,
                crate::tls::key(self.private_key.as_ref().expect("validated private key"))?,
                self.client_ca.as_ref().map(crate::tls::certs).transpose()?,
            )?),
        };
        Ok(crate::auth::listener::PostgresListener {
            tls: self.tls,
            tls_config,
            certificates: self.certificates.clone(),
            databases: self.databases.clone(),
            loopback_trust: self.loopback_trust,
            connections: self.connections,
        })
    }
}
