use super::error::ApiError;
use crate::{Role, Service};
use std::sync::Arc;

/// Authority captured after TLS verification, immutable for that connection.
/// Fields and construction are deliberately unavailable to external callers.
#[derive(Clone, PartialEq, Eq)]
pub struct ConnectionIdentity {
    pub(crate) fingerprint: String,
    pub(crate) role: Role,
    pub(crate) epoch: u64,
}

impl ConnectionIdentity {
    pub(crate) fn verified(service: &Arc<Service>, fingerprint: &str) -> Result<Self, ApiError> {
        let session = service
            .session(fingerprint)
            .map_err(|_| ApiError::Unauthenticated)?;
        Ok(Self {
            fingerprint: fingerprint.into(),
            role: session.role.clone(),
            epoch: session.epoch,
        })
    }

    pub(crate) fn check(&self, service: &Arc<Service>) -> Result<(), ApiError> {
        match service.session(&self.fingerprint) {
            Ok(session) if session.role == self.role && session.epoch == self.epoch => Ok(()),
            _ => Err(ApiError::Unauthenticated),
        }
    }
}

impl std::fmt::Debug for ConnectionIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ConnectionIdentity([verified, redacted])")
    }
}
