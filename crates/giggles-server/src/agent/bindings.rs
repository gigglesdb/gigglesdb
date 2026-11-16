//! Agent binding map (GD-0061 R3-204, design-2 section 2).
//!
//! An agent binding maps a client-certificate fingerprint to a task-only executor
//! role and the tasks it may open. It is a separate map from the ordinary
//! fingerprint bindings, and a fingerprint may appear in only one of the two, so
//! one certificate never carries two authorities.

use crate::service::Role;
use giggles_core::{task::definition::TaskName, Admin, CoreError, Error};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Most agent bindings one map may hold.
pub const MAX_AGENT_BINDINGS: usize = 1024;
/// Most tasks one agent binding may list.
pub const MAX_BINDING_TASKS: usize = 64;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentBinding {
    pub fingerprint: String,
    pub executor: String,
    pub tasks: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AgentEntry {
    pub executor: String,
    pub tasks: BTreeSet<TaskName>,
}

impl AgentEntry {
    fn binding(&self, fingerprint: &str) -> AgentBinding {
        AgentBinding {
            fingerprint: fingerprint.to_owned(),
            executor: self.executor.clone(),
            tasks: self.tasks.iter().map(|t| t.as_str().to_owned()).collect(),
        }
    }
}

/// The live agent map. Epochs come from the same counter as the ordinary map's,
/// so a cancellation key (fingerprint, epoch) never matches across the two maps.
#[derive(Default)]
pub(crate) struct AgentCredentials {
    pub next: u64,
    pub entries: BTreeMap<String, (AgentEntry, u64)>,
}

impl AgentCredentials {
    /// The current entries as bindings, for a reload of the ordinary map only.
    pub(crate) fn bindings(&self) -> Vec<AgentBinding> {
        self.entries
            .iter()
            .map(|(fp, (entry, _))| entry.binding(fp))
            .collect()
    }
}

pub(crate) fn valid_fingerprint(fp: &str) -> bool {
    fp.len() == 64
        && fp
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Validates a whole agent map against the ordinary map it will sit beside.
/// Malformed input is `Limit`; a semantic refusal is `Policy(Authentication)`.
/// Tasks need not exist yet: they are checked at connect.
pub(crate) fn validate(
    admin: &Admin,
    ordinary: &BTreeMap<String, Role>,
    agent: Vec<AgentBinding>,
) -> Result<BTreeMap<String, AgentEntry>, CoreError> {
    if agent.len() > MAX_AGENT_BINDINGS {
        return Err(CoreError::Limit);
    }
    let mut map = BTreeMap::new();
    for b in agent {
        if !valid_fingerprint(&b.fingerprint) {
            return Err(CoreError::Limit);
        }
        if ordinary.contains_key(&b.fingerprint) || map.contains_key(&b.fingerprint) {
            return Err(CoreError::Policy(Error::Authentication));
        }
        if b.tasks.is_empty() || b.tasks.len() > MAX_BINDING_TASKS {
            return Err(CoreError::Limit);
        }
        let mut tasks = BTreeSet::new();
        for name in &b.tasks {
            let name = TaskName::new(name).map_err(|_| CoreError::Limit)?;
            if !tasks.insert(name) {
                return Err(CoreError::Policy(Error::Authentication));
            }
        }
        admin
            .roles()
            .get(&b.executor)
            .map_err(|_| CoreError::Policy(Error::Authentication))?
            .filter(executor_role)
            .ok_or(CoreError::Policy(Error::Authentication))?;
        map.insert(
            b.fingerprint,
            AgentEntry {
                executor: b.executor,
                tasks,
            },
        );
    }
    Ok(map)
}

/// D6: an executor logs in, is task-only and is never a superuser.
pub(crate) fn executor_role(role: &giggles_core::roles::CatalogRole) -> bool {
    role.login && role.task_only && !role.superuser
}
