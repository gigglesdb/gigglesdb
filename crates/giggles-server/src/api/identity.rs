use super::error::ApiError;
use crate::{Role, Service};
use std::sync::Arc;

/// Authority captured from a verified certificate or a per-request credential.
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
    // Captured by the agent runtime from its certificate map or SQL credentials.
    pub(crate) agent: bool,
    credential: Option<giggles_core::task::definition::TaskObjectId>,
    // Never included in Debug or identity equality. Retained only on a request.
    pub(crate) credential_secret: Option<String>,
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
            credential: None,
            credential_secret: None,
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
            credential: None,
            credential_secret: None,
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
            credential: None,
            credential_secret: None,
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
            credential: None,
            credential_secret: None,
            connection: None,
            database: None,
            role_listener: None,
        };
        identity.check(service)?;
        Ok(identity)
    }

    pub(crate) fn verified_credential(
        service: &Arc<Service>,
        secret: &str,
    ) -> Result<Self, ApiError> {
        let now = crate::agent::wall_now(&service.clock).map_err(|_| ApiError::Unauthenticated)?;
        let (fingerprint, executor, epoch, task) = service
            .credential_identity(secret, now)
            .ok_or(ApiError::Unauthenticated)?;
        let role = service
            .admin
            .roles()
            .get(&executor)
            .ok()
            .flatten()
            .filter(crate::agent::bindings::executor_role)
            .ok_or(ApiError::Unauthenticated)?;
        let identity = Self {
            fingerprint,
            role: Role::Database { name: executor },
            epoch,
            connection: None,
            database: None,
            role_listener: None,
            role_fence: Some((role.oid, role.generation)),
            agent: true,
            credential: Some(task),
            credential_secret: Some(secret.to_owned()),
        };
        identity.check(service)?;
        Ok(identity)
    }

    pub(crate) fn forget_secret(&mut self) {
        self.credential_secret = None;
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
            let Role::Database { .. } = &self.role else {
                return Err(ApiError::Unauthenticated);
            };
            return service
                .check_native_identity(&self.role, self.epoch, self.role_fence, &self.fingerprint, false)
                .map_err(|_| ApiError::Unauthenticated);
        }
        if let Some(task) = self.credential {
            let Role::Database { name } = &self.role else {
                return Err(ApiError::Unauthenticated);
            };
            let now =
                crate::agent::wall_now(&service.clock).map_err(|_| ApiError::Unauthenticated)?;
            let valid_role = service
                .admin
                .roles()
                .get(name)
                .ok()
                .flatten()
                .is_some_and(|role| {
                    crate::agent::bindings::executor_role(&role)
                        && self.role_fence == Some((role.oid, role.generation))
                });
            return if valid_role && service.admin.tasks().credential_live(task, self.epoch, now) {
                Ok(())
            } else {
                Err(ApiError::Unauthenticated)
            };
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
        service
            .check_native_identity(&self.role, self.epoch, self.role_fence, &self.fingerprint, true)
            .map_err(|_| ApiError::Unauthenticated)
    }
}

impl std::fmt::Debug for ConnectionIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ConnectionIdentity([verified, redacted])")
    }
}
