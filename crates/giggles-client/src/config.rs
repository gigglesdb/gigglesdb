use crate::ClientError;
use rustls::pki_types::ServerName;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, net::SocketAddr, path::PathBuf, time::Duration};

/// Independent transport budgets. All waits also consume the operation budget.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub connect: Duration,
    pub request: Duration,
    pub operation: Duration,
    pub backoff: Duration,
    pub max_attempts: usize,
    pub max_in_flight: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            connect: Duration::from_secs(1),
            request: Duration::from_secs(5),
            operation: Duration::from_secs(10),
            backoff: Duration::from_millis(25),
            max_attempts: 3,
            max_in_flight: 8,
        }
    }
}
/// Only these preconfigured endpoints may be contacted. Debug omits credentials.
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub endpoints: Vec<SocketAddr>,
    pub ca: PathBuf,
    pub certificate: PathBuf,
    pub key: PathBuf,
    pub server_name: String,
    #[serde(default)]
    pub limits: Limits,
}
impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}
impl Config {
    pub fn validate(&self) -> Result<(), ClientError> {
        let l = &self.limits;
        if self.endpoints.is_empty()
            || self.endpoints.len() > 3
            || self
                .endpoints
                .iter()
                .any(|a| a.port() == 0 || a.ip().is_unspecified() || a.ip().is_multicast())
            || self.endpoints.iter().collect::<BTreeSet<_>>().len() != self.endpoints.len()
            || self.server_name.len() > 253
            || ServerName::try_from(self.server_name.clone()).is_err()
            || [l.connect, l.request, l.operation]
                .iter()
                .any(|d| d.is_zero() || *d > Duration::from_secs(300))
            || l.backoff > Duration::from_secs(5)
            || !(1..=16).contains(&l.max_attempts)
            || !(1..=32).contains(&l.max_in_flight)
        {
            return Err(ClientError::InvalidConfig);
        }
        Ok(())
    }
}
