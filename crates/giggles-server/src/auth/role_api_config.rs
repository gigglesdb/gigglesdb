//! Validated role-authenticated HTTP and gRPC listener configuration.
use super::config::{
    TlsMode, default_connections, default_databases, valid_fingerprint, valid_name,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, io, net::SocketAddr, sync::Arc};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoleApiConfig {
    #[serde(default)]
    pub http: Option<SocketAddr>,
    #[serde(default)]
    pub grpc: Option<SocketAddr>,
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
    #[serde(default)]
    pub idle_timeout_seconds: Option<u32>,
    #[serde(default)]
    pub max_age_seconds: Option<u32>,
}

#[derive(Clone, Debug)]
pub struct RoleApiListener {
    pub http: Option<SocketAddr>,
    pub grpc: Option<SocketAddr>,
    pub tls: TlsMode,
    pub tls_config: Option<Arc<rustls::ServerConfig>>,
    pub certificates: BTreeMap<String, String>,
    pub databases: Vec<String>,
    pub loopback_trust: bool,
    pub connections: usize,
    pub idle_timeout_seconds: u32,
    pub max_age_seconds: u32,
}

impl RoleApiConfig {
    fn validate(&self) -> io::Result<(u32, u32)> {
        if self.http.is_none() && self.grpc.is_none() {
            return Err(io::Error::other(
                "role_api requires at least one of http or grpc",
            ));
        }
        if self.http.is_some() && self.http == self.grpc {
            return Err(io::Error::other(
                "role_api http and grpc addresses must differ",
            ));
        }
        match self.tls {
            TlsMode::Disabled => {
                if self.certificate.is_some()
                    || self.private_key.is_some()
                    || self.client_ca.is_some()
                {
                    return Err(io::Error::other(
                        "role_api disabled TLS cannot configure certificate, private_key, or client_ca",
                    ));
                }
            }
            TlsMode::Optional => return Err(io::Error::other("role_api TLS cannot be optional")),
            TlsMode::Required => {
                if self.certificate.is_none() || self.private_key.is_none() {
                    return Err(io::Error::other(
                        "role_api TLS requires certificate and private_key",
                    ));
                }
            }
        }
        if !self.certificates.is_empty()
            && (self.tls == TlsMode::Disabled || self.client_ca.is_none())
        {
            return Err(io::Error::other(
                "role_api certificate mappings require TLS and client_ca",
            ));
        }
        for (fingerprint, role) in &self.certificates {
            if !valid_fingerprint(fingerprint) {
                return Err(io::Error::other(
                    "role_api certificate fingerprints must be 64 lowercase hexadecimal characters",
                ));
            }
            if !valid_name(role) {
                return Err(io::Error::other(
                    "role_api mapped role names must be 1..=63 bytes without NUL",
                ));
            }
        }
        if !(1..=16).contains(&self.databases.len()) {
            return Err(io::Error::other(
                "role_api databases must contain 1..=16 names",
            ));
        }
        if self.databases.iter().any(|name| !valid_name(name)) {
            return Err(io::Error::other(
                "role_api database names must be 1..=63 bytes without NUL",
            ));
        }
        if !(1..=128).contains(&self.connections) {
            return Err(io::Error::other("role_api connections must be 1..=128"));
        }
        let idle = self.idle_timeout_seconds.unwrap_or(60);
        let max_age = self
            .max_age_seconds
            .unwrap_or(if self.tls == TlsMode::Required {
                3600
            } else {
                900
            });
        if !(10..=3600).contains(&idle) {
            return Err(io::Error::other(
                "role_api idle_timeout_seconds must be 10..=3600",
            ));
        }
        if !(idle..=86400).contains(&max_age) {
            return Err(io::Error::other(
                "role_api max_age_seconds must be idle_timeout_seconds..=86400",
            ));
        }
        Ok((idle, max_age))
    }

    pub fn build(&self) -> io::Result<RoleApiListener> {
        let (idle_timeout_seconds, max_age_seconds) = self.validate()?;
        let tls_config = match self.tls {
            TlsMode::Disabled => None,
            TlsMode::Optional => unreachable!("validated TLS mode"),
            TlsMode::Required => Some(
                (|| -> io::Result<_> {
                    crate::tls::server_optional_client(
                        crate::tls::certs(
                            self.certificate.as_ref().expect("validated certificate"),
                        )?,
                        crate::tls::key(self.private_key.as_ref().expect("validated private key"))?,
                        self.client_ca.as_ref().map(crate::tls::certs).transpose()?,
                    )
                })()
                .map_err(|error| io::Error::other(format!("role_api TLS: {error}")))?,
            ),
        };
        let tls_config = tls_config.map(|mut config| {
            Arc::make_mut(&mut config).alpn_protocols = if self.http.is_some() {
                vec![b"h2".to_vec(), b"http/1.1".to_vec()]
            } else {
                vec![b"h2".to_vec()]
            };
            config
        });
        Ok(RoleApiListener {
            http: self.http,
            grpc: self.grpc,
            idle_timeout_seconds,
            max_age_seconds,
            tls: self.tls,
            tls_config,
            certificates: self.certificates.clone(),
            databases: self.databases.clone(),
            loopback_trust: self.loopback_trust,
            connections: self.connections,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn config() -> RoleApiConfig {
        serde_json::from_value(json!({"http":"127.0.0.1:8080", "tls":"disabled"})).unwrap()
    }

    #[test]
    fn defaults_and_unknown_fields() {
        let c = config();
        assert_eq!(c.databases, ["postgres"]);
        assert_eq!(c.connections, 32);
        assert!(!c.loopback_trust);
        assert!(c.grpc.is_none() && c.certificates.is_empty());
        assert!(c.certificate.is_none() && c.private_key.is_none() && c.client_ca.is_none());
        assert!(c.idle_timeout_seconds.is_none() && c.max_age_seconds.is_none());
        let listener = c.build().unwrap();
        assert_eq!(
            (listener.idle_timeout_seconds, listener.max_age_seconds),
            (60, 900)
        );
        assert!(listener.tls_config.is_none());
        assert!(
            serde_json::from_value::<RoleApiConfig>(
                json!({"http":"127.0.0.1:8080", "tls":"disabled", "typo":true})
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_invalid_combinations_before_io() {
        let cases = [
            (json!({"http":null}), "requires at least one"),
            (json!({"grpc":"127.0.0.1:8080"}), "addresses must differ"),
            (json!({"tls":"optional"}), "cannot be optional"),
            (json!({"certificate":"unused"}), "disabled TLS"),
            (json!({"private_key":"unused"}), "disabled TLS"),
            (json!({"client_ca":"unused"}), "disabled TLS"),
            (json!({"tls":"required"}), "requires certificate"),
            (
                json!({"tls":"required", "certificate":"unused"}),
                "requires certificate",
            ),
            (
                json!({"tls":"required", "private_key":"unused"}),
                "requires certificate",
            ),
            (
                json!({"certificates":{("a".repeat(64)):"owner"}}),
                "require TLS and client_ca",
            ),
            (
                json!({"tls":"required", "certificate":"unused", "private_key":"unused", "certificates":{("a".repeat(64)):"owner"}}),
                "require TLS and client_ca",
            ),
            (json!({"databases":[]}), "1..=16 names"),
            (json!({"databases":vec!["db";17]}), "1..=16 names"),
            (json!({"databases":[""]}), "database names"),
            (json!({"databases":["a\0b"]}), "database names"),
            (json!({"databases":["a".repeat(64)]}), "database names"),
            (json!({"connections":0}), "connections"),
            (json!({"connections":129}), "connections"),
            (
                json!({"idle_timeout_seconds":9}),
                "idle_timeout_seconds must",
            ),
            (
                json!({"idle_timeout_seconds":3601}),
                "idle_timeout_seconds must",
            ),
            (json!({"max_age_seconds":59}), "max_age_seconds must"),
            (json!({"max_age_seconds":86401}), "max_age_seconds must"),
        ];
        for (patch, expected) in cases {
            let mut value = serde_json::to_value(config()).unwrap();
            value
                .as_object_mut()
                .unwrap()
                .extend(patch.as_object().unwrap().clone());
            let error = serde_json::from_value::<RoleApiConfig>(value)
                .unwrap()
                .build()
                .unwrap_err()
                .to_string();
            assert!(
                error.starts_with("role_api ") && error.contains(expected),
                "{error}: {expected}"
            );
        }
        for (fingerprint, role, expected) in [
            ("A".repeat(64), "owner".into(), "fingerprints"),
            ("a".repeat(63), "owner".into(), "fingerprints"),
            ("a".repeat(65), "owner".into(), "fingerprints"),
            ("g".repeat(64), "owner".into(), "fingerprints"),
            ("a".repeat(64), "".into(), "role names"),
            ("a".repeat(64), "a\0b".into(), "role names"),
            ("a".repeat(64), "a".repeat(64), "role names"),
        ] {
            let mut c = config();
            c.tls = TlsMode::Required;
            c.certificate = Some("unused".into());
            c.private_key = Some("unused".into());
            c.client_ca = Some("unused".into());
            c.certificates.insert(fingerprint, role);
            assert!(c.build().unwrap_err().to_string().contains(expected));
        }
    }

    #[test]
    fn lifetime_boundaries_and_required_defaults() {
        let mut c = config();
        c.tls = TlsMode::Required;
        c.certificate = Some("unused".into());
        c.private_key = Some("unused".into());
        assert_eq!(c.validate().unwrap(), (60, 3600));
        for tls in [TlsMode::Disabled, TlsMode::Required] {
            c.tls = tls;
            c.certificate = (tls == TlsMode::Required).then(|| "unused".into());
            c.private_key = c.certificate.clone();
            for (idle, age) in [(10, 10), (3600, 3600), (3600, 86400)] {
                c.idle_timeout_seconds = Some(idle);
                c.max_age_seconds = Some(age);
                assert_eq!(c.validate().unwrap(), (idle, age));
            }
        }
    }

    #[test]
    fn required_tls_build_and_alpn() {
        let dir = std::env::temp_dir().join(format!("role-api-config-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let pem = dir.join("server.pem");
        let key = dir.join("server.key");
        std::fs::write(&pem, cert.cert.pem()).unwrap();
        std::fs::write(&key, cert.key_pair.serialize_pem()).unwrap();
        let mut c = config();
        c.tls = TlsMode::Required;
        c.certificate = Some(pem.to_str().unwrap().into());
        c.private_key = Some(key.to_str().unwrap().into());
        let listener = c.build().unwrap();
        assert_eq!(
            (listener.idle_timeout_seconds, listener.max_age_seconds),
            (60, 3600)
        );
        assert_eq!(
            listener.tls_config.unwrap().alpn_protocols,
            [b"h2".to_vec(), b"http/1.1".to_vec()]
        );
        c.grpc = Some("127.0.0.1:8081".parse().unwrap());
        assert_eq!(
            c.build().unwrap().tls_config.unwrap().alpn_protocols.len(),
            2
        );
        c.http = None;
        assert_eq!(
            c.build().unwrap().tls_config.unwrap().alpn_protocols,
            [b"h2".to_vec()]
        );
        c.client_ca = c.certificate.clone();
        c.certificates.insert("a".repeat(64), "owner".into());
        assert!(c.build().is_ok());
        std::fs::remove_dir_all(dir).unwrap();
    }
}
