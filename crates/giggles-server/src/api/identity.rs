use super::error::ApiError;
use crate::{Role, Service};
use std::sync::Arc;

/// Authority captured after TLS verification, immutable for that connection.
/// Fields and construction are deliberately unavailable to external callers.
#[derive(Clone)]
pub struct ConnectionIdentity {
    pub(crate) fingerprint: String,
    pub(crate) role: Role,
    pub(crate) epoch: u64,
    role_fence: Option<(u32, u64)>,
}

// Handle ownership follows the certificate binding. Role lifetime is checked
// separately so password-only generation aliases also survive TLS reconnection.
impl PartialEq for ConnectionIdentity {
    fn eq(&self, other: &Self) -> bool {
        self.fingerprint == other.fingerprint && self.role == other.role && self.epoch == other.epoch
    }
}
impl Eq for ConnectionIdentity {}

impl ConnectionIdentity {
    pub(crate) fn verified(service: &Arc<Service>, fingerprint: &str) -> Result<Self, ApiError> {
        let session = service
            .session(fingerprint)
            .map_err(|_| ApiError::Unauthenticated)?;
        Ok(Self {
            fingerprint: fingerprint.into(),
            role: session.role.clone(),
            epoch: session.epoch,
            role_fence: session.role_fence,
        })
    }

    pub(crate) fn check(&self, service: &Arc<Service>) -> Result<(), ApiError> {
        match service.session(&self.fingerprint) {
            Ok(mut session) if session.role == self.role && session.epoch == self.epoch => {
                // Revalidate the role lifetime captured at TLS verification, including
                // the same password-only generation aliases as retained sessions.
                session.role_fence = self.role_fence;
                session.check_binding().map_err(|_| ApiError::Unauthenticated)
            }
            _ => Err(ApiError::Unauthenticated),
        }
    }
}

impl std::fmt::Debug for ConnectionIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ConnectionIdentity([verified, redacted])")
    }
}
