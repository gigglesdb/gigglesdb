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
    pub(crate) connection: Option<super::net::RoleConnection>,
    database: Option<String>,
    role_listener: Option<Arc<crate::auth::RoleApiListener>>,
    role_fence: Option<(u32, u64)>,
    // Captured by the agent runtime from the agent map only (GD-0061 R3-208).
    pub(crate) agent: bool,
}

// Handle ownership follows the certificate binding. Role lifetime is checked
// separately so password-only generation aliases also survive TLS reconnection.
impl PartialEq for ConnectionIdentity {
    fn eq(&self, other: &Self) -> bool {
        self.fingerprint == other.fingerprint
            && self.role == other.role
            && self.epoch == other.epoch
            && self.agent == other.agent
    }
}
impl Eq for ConnectionIdentity {}

impl ConnectionIdentity {
    pub(crate) fn role_connection(connection: super::net::RoleConnection) -> Self {
        Self {
            fingerprint: String::new(),
            role: Role::Database {
                name: String::new(),
            },
            epoch: 0,
            role_fence: None,
            agent: false,
            connection: Some(connection),
            database: None,
            role_listener: None,
        }
    }

    pub(crate) fn role_signed_in(
        session: &crate::Session,
        listener: Arc<crate::auth::RoleApiListener>,
    ) -> Self {
        Self {
            fingerprint: session.fingerprint.clone(),
            role: session.role.clone(),
            epoch: session.epoch,
            role_fence: session.role_fence,
            agent: false,
            connection: None,
            database: session.database.clone(),
            role_listener: Some(listener),
        }
    }

    pub(crate) fn verified(service: &Arc<Service>, fingerprint: &str) -> Result<Self, ApiError> {
        let session = service
            .session(fingerprint)
            .map_err(|_| ApiError::Unauthenticated)?;
        Ok(Self {
            fingerprint: fingerprint.into(),
            role: session.role.clone(),
            epoch: session.epoch,
            role_fence: session.role_fence,
            agent: false,
            connection: None,
            database: None,
            role_listener: None,
        })
    }

    /// An agent-runtime identity: the certificate must be in the agent map, and
    /// only there. The ordinary map is never consulted.
    pub(crate) fn verified_agent(
        service: &Arc<Service>,
        fingerprint: &str,
    ) -> Result<Self, ApiError> {
        let (executor, epoch) = service
            .agent_entry(fingerprint)
            .ok_or(ApiError::Unauthenticated)?;
        let identity = Self {
            fingerprint: fingerprint.into(),
            role: Role::Database { name: executor },
            epoch,
            role_fence: None,
            agent: true,
            connection: None,
            database: None,
            role_listener: None,
        };
        identity.check(service)?;
        Ok(identity)
    }

    pub(crate) fn check(&self, service: &Arc<Service>) -> Result<(), ApiError> {
        // Bare role connections authorize only open and token lookup. Registry
        // lookup and response leases must check the stored role_signed_in owner.
        if self.connection.is_some() {
            return Ok(());
        }
        if let Some(database) = &self.database {
            if !self
                .role_listener
                .as_ref()
                .is_some_and(|listener| listener.databases.contains(database))
            {
                return Err(ApiError::Unauthenticated);
            }
            let Role::Database { name } = &self.role else {
                return Err(ApiError::Unauthenticated);
            };
            let mut session = service
                .role_session(name)
                .map_err(|_| ApiError::Unauthenticated)?;
            session.role_fence = self.role_fence;
            return session
                .check_binding()
                .map_err(|_| ApiError::Unauthenticated);
        }
        if self.agent {
            // The agent entry keeps its epoch only while its executor and tasks
            // are unchanged; the executor must still be a login task-only role.
            let Role::Database { name } = &self.role else {
                return Err(ApiError::Unauthenticated);
            };
            return match service.agent_entry(&self.fingerprint) {
                Some((executor, epoch))
                    if epoch == self.epoch
                        && &executor == name
                        && service.agent_executor_valid(name) =>
                {
                    Ok(())
                }
                _ => Err(ApiError::Unauthenticated),
            };
        }
        match service.session(&self.fingerprint) {
            Ok(mut session) if session.role == self.role && session.epoch == self.epoch => {
                // Revalidate the role lifetime captured at TLS verification, including
                // the same password-only generation aliases as retained sessions.
                session.role_fence = self.role_fence;
                session
                    .check_binding()
                    .map_err(|_| ApiError::Unauthenticated)
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
