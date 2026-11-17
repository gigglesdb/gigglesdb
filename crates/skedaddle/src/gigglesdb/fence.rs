//! Journalled GigglesDB source fence. The caller must durably retain every
//! intent (especially Save) before acknowledging it. Resume that saved Fence,
//! never construct a fresh fence after a crash. Use a dedicated administrator
//! outside `applications`, and serialize catalog changes throughout cutover.
//! No WAL/final-marker claim is made here; capture support is still required.
pub use crate::pg::fence::{
    Catalog, Connection, FenceError, Grant, Grantee, Privilege, Residual, ResidualKind,
    ResidualPrincipal, Role, Rows, SavedState, Table,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, time::Duration};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FenceConfig {
    pub applications: BTreeSet<String>,
    pub tables: Vec<Table>,
    pub deadline_ms: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Step {
    PreFence,
    Save,
    Revoke,
    NoLogin,
    Prove,
    Complete,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fence {
    pub migration: String,
    pub config: FenceConfig,
    pub next: Step,
    pub saved: Option<SavedState>,
    pub residuals: Vec<Residual>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Intent {
    Step(Box<Fence>),
    Save(Box<Fence>),
    Unfence(Box<Fence>),
}

impl Fence {
    pub fn new(config: FenceConfig) -> Result<Self, FenceError> {
        if config.applications.is_empty()
            || config.tables.is_empty()
            || config.deadline_ms == 0
            || config
                .applications
                .iter()
                .any(|s| s.is_empty() || s.contains('\0'))
            || config.tables.iter().any(|t| {
                t.schema.is_empty()
                    || t.name.is_empty()
                    || t.schema.contains('\0')
                    || t.name.contains('\0')
            })
            || config
                .tables
                .iter()
                .map(table_sql)
                .collect::<BTreeSet<_>>()
                .len()
                != config.tables.len()
        {
            return Err(FenceError::Invalid(
                "distinct tables, roles and positive deadline required".into(),
            ));
        }
        Ok(Self {
            config,
            migration: String::new(),
            next: Step::PreFence,
            saved: None,
            residuals: vec![],
        })
    }
    /// Bind before journalling any fence step; retries retain their namespace.
    pub fn bind_migration(&mut self, migration: &str) -> Result<(), FenceError> {
        if migration.is_empty() || (!self.migration.is_empty() && self.migration != migration) {
            return Err(invalid("missing or conflicting migration"));
        }
        self.migration = migration.into();
        Ok(())
    }
    fn query(&self, c: &mut impl Connection, sql: &str) -> Result<Rows, FenceError> {
        c.query(sql, Duration::from_millis(self.config.deadline_ms))
            .map_err(FenceError::Database)
    }
    fn capability(&self, c: &mut impl Connection) -> Result<(), FenceError> {
        require_capability(&self.query(c, "SHOW giggles_capabilities")?)
    }
    /// Database mutations are intent-first and replayable. Pre-fence hooks must
    /// tolerate retry until their completion checkpoint is durable. Steps execute
    /// separate statements; replay handles a crash within a step.
    pub fn execute_next(
        &mut self,
        c: &mut impl Connection,
        journal: &mut impl FnMut(&Intent) -> Result<(), String>,
        pre_fence: &mut impl FnMut() -> Result<(), String>,
    ) -> Result<(), FenceError> {
        if self.next == Step::Complete {
            return Ok(());
        }
        if self.migration.is_empty() {
            return Err(invalid("migration must be bound before fencing"));
        }
        if self.next == Step::PreFence {
            pre_fence().map_err(FenceError::Hook)?;
            let mut completed = self.clone();
            completed.next = Step::Save;
            journal(&Intent::Step(Box::new(completed.clone()))).map_err(FenceError::Journal)?;
            *self = completed;
            return Ok(());
        }
        self.capability(c)?;
        if self.next != Step::Save && self.saved.is_none() {
            return Err(FenceError::Invalid(
                "resume requires saved original state".into(),
            ));
        }
        journal(&Intent::Step(Box::new(self.clone()))).map_err(FenceError::Journal)?;
        self.next = match self.next {
            Step::Save => {
                if self.saved.is_none() {
                    let catalog = self.read_catalog(c)?;
                    self.residuals = classify_residuals(&catalog, &self.config.applications)
                        .into_iter()
                        .filter(|r| {
                            matches!(
                                r.principal.kind,
                                ResidualKind::TableOwner | ResidualKind::Superuser
                            )
                        })
                        .collect();
                    if !self.residuals.is_empty() {
                        return Err(FenceError::ResidualBlocked(self.residuals.clone()));
                    }
                    let saved = SavedState {
                        catalog,
                        authority: None,
                    };
                    restoration_sql(&saved, &self.config.applications)?;
                    self.saved = Some(saved);
                }
                journal(&Intent::Save(Box::new(self.clone()))).map_err(FenceError::Journal)?;
                Step::Revoke
            }
            Step::Revoke => {
                for role in &self.config.applications {
                    for table in &self.config.tables {
                        self.query(
                            c,
                            &format!(
                                "REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON TABLE {} FROM {}",
                                table_sql(table),
                                ident(role)
                            ),
                        )?;
                    }
                }
                Step::NoLogin
            }
            Step::NoLogin => {
                for role in &self.config.applications {
                    self.query(c, &format!("ALTER ROLE {} NOLOGIN", ident(role)))?;
                }
                Step::Prove
            }
            Step::Prove => {
                self.prove(c)?;
                Step::Complete
            }
            Step::PreFence | Step::Complete => unreachable!(),
        };
        Ok(())
    }
    /// Fresh proof: generation invalidation terminates pre-fence sessions. Owners
    /// and superusers remain blocking even when NOLOGIN; they cannot be waived.
    pub fn prove(&mut self, c: &mut impl Connection) -> Result<(), FenceError> {
        self.capability(c)?;
        let catalog = self.read_catalog(c)?;
        self.residuals = classify_residuals(&catalog, &self.config.applications);
        if !self.residuals.is_empty() {
            return Err(FenceError::ResidualBlocked(self.residuals.clone()));
        }
        if catalog.roles.iter().any(|r| r.login) {
            return Err(FenceError::Invalid(
                "application LOGIN remains enabled".into(),
            ));
        }
        // Check effective access independently of the explicit ACL snapshot.
        for role in &self.config.applications {
            for table in &self.config.tables {
                for privilege in ["INSERT", "UPDATE", "DELETE", "TRUNCATE"] {
                    let rows = self.query(
                        c,
                        &format!(
                            "SELECT has_table_privilege({}, {}, {})",
                            literal(role),
                            literal(&table_sql(table)),
                            literal(privilege)
                        ),
                    )?;
                    if boolean(cell(
                        rows.first()
                            .ok_or_else(|| invalid("missing privilege result"))?,
                        0,
                    )?)? {
                        return Err(invalid("effective write privilege remains"));
                    }
                }
            }
        }
        Ok(())
    }
    /// Caller must exclude activation before restoring. Original snapshot stays
    /// available for repeated restoration after a crash.
    pub fn unfence(
        &self,
        c: &mut impl Connection,
        journal: &mut impl FnMut(&Intent) -> Result<(), String>,
    ) -> Result<(), FenceError> {
        if self.migration.is_empty() {
            return Err(invalid("migration must be bound before unfencing"));
        }
        let Some(saved) = &self.saved else {
            return Ok(());
        };
        journal(&Intent::Unfence(Box::new(self.clone()))).map_err(FenceError::Journal)?;
        for sql in restoration_sql(saved, &self.config.applications)? {
            self.query(c, &sql)?;
        }
        if self.read_catalog(c)? != saved.catalog {
            return Err(invalid("restoration differs from saved catalog"));
        }
        Ok(())
    }
    fn read_catalog(&self, c: &mut impl Connection) -> Result<Catalog, FenceError> {
        let mut catalog = Catalog::default();
        for name in &self.config.applications {
            let rows = self.query(
                c,
                &format!(
                    "SELECT rolname, rolcanlogin, rolsuper FROM pg_roles WHERE rolname={}",
                    literal(name)
                ),
            )?;
            if rows.len() != 1 {
                return Err(invalid("application role missing"));
            }
            catalog.roles.push(Role {
                name: cell(&rows[0], 0)?.into(),
                login: boolean(cell(&rows[0], 1)?)?,
                superuser: boolean(cell(&rows[0], 2)?)?,
                bypass_rls: false,
            });
        }
        for selected in &self.config.tables {
            let filter = format!(
                "n.nspname={} AND c.relname={}",
                literal(&selected.schema),
                literal(&selected.name)
            );
            let rows = self.query(c, &format!("SELECT pg_get_userbyid(c.relowner) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE {filter}"))?;
            if rows.len() != 1 {
                return Err(invalid("selected table missing"));
            }
            let table = Table {
                owner: cell(&rows[0], 0)?.into(),
                ..selected.clone()
            };
            let rows = self.query(c, &format!("SELECT pg_get_userbyid(x.grantee), pg_get_userbyid(x.grantor), x.privilege_type, x.is_grantable FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace CROSS JOIN LATERAL aclexplode(c.relacl) x WHERE {filter}"))?;
            for row in rows {
                let grantee = cell(&row, 0)?;
                if !self.config.applications.contains(grantee) {
                    continue;
                }
                let privilege = match cell(&row, 2)? {
                    "INSERT" => Privilege::Insert,
                    "UPDATE" => Privilege::Update,
                    "DELETE" => Privilege::Delete,
                    "TRUNCATE" => Privilege::Truncate,
                    _ => continue,
                };
                catalog.grants.push(Grant {
                    table: table.clone(),
                    column: None,
                    grantee: Grantee::Role(grantee.into()),
                    grantor: cell(&row, 1)?.into(),
                    privilege,
                    grant_option: boolean(cell(&row, 3)?)?,
                });
            }
            catalog.tables.push(table);
        }
        catalog.tables.sort();
        catalog.grants.sort();
        Ok(catalog)
    }
}
/// Only configured application principals are fenced; administrative owners
/// outside that set remain an explicitly documented operational residual risk.
pub fn classify_residuals(catalog: &Catalog, applications: &BTreeSet<String>) -> Vec<Residual> {
    crate::pg::fence::classify_residuals(catalog, applications, &BTreeSet::new())
        .into_iter()
        .filter(|r| applications.contains(&r.principal.role))
        .collect()
}
pub fn effective_writers(catalog: &Catalog) -> BTreeSet<String> {
    crate::pg::fence::effective_writers(catalog)
}
/// Restore direct grants as standalone administrative statements. GigglesDB
/// records the table owner as grantor and supports no grant options. Reject
/// other snapshots instead of silently changing their authority.
pub fn restoration_sql(
    saved: &SavedState,
    applications: &BTreeSet<String>,
) -> Result<Vec<String>, FenceError> {
    let mut result = vec![];
    for g in &saved.catalog.grants {
        let Grantee::Role(role) = &g.grantee else {
            return Err(invalid("PUBLIC grants unsupported"));
        };
        if !applications.contains(role)
            || g.column.is_some()
            || g.grant_option
            || g.grantor != g.table.owner
        {
            return Err(invalid("unsupported saved grant"));
        }
        result.push(format!(
            "GRANT {} ON TABLE {} TO {}",
            privilege_sql(g.privilege),
            table_sql(&g.table),
            ident(role)
        ));
    }
    for r in &saved.catalog.roles {
        if applications.contains(&r.name) {
            result.push(format!(
                "ALTER ROLE {} {}",
                ident(&r.name),
                if r.login { "LOGIN" } else { "NOLOGIN" }
            ));
        }
    }
    Ok(result)
}
pub fn require_capability(rows: &Rows) -> Result<(), FenceError> {
    if rows
        .iter()
        .flatten()
        .any(|v| v.split(',').any(|s| s.trim() == "role_session_termination"))
    {
        Ok(())
    } else {
        Err(FenceError::InstallRequired(
            "role_session_termination capability required",
        ))
    }
}
fn privilege_sql(p: Privilege) -> &'static str {
    match p {
        Privilege::Insert => "INSERT",
        Privilege::Update => "UPDATE",
        Privilege::Delete => "DELETE",
        Privilege::Truncate => "TRUNCATE",
    }
}
fn ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}
fn literal(s: &str) -> String {
    format!("E'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
}
fn table_sql(t: &Table) -> String {
    format!("{}.{}", ident(&t.schema), ident(&t.name))
}
fn invalid(s: &str) -> FenceError {
    FenceError::Invalid(s.into())
}
fn cell(row: &[String], i: usize) -> Result<&str, FenceError> {
    row.get(i)
        .map(String::as_str)
        .ok_or_else(|| invalid("short catalog row"))
}
fn boolean(s: &str) -> Result<bool, FenceError> {
    match s {
        "t" | "true" => Ok(true),
        "f" | "false" => Ok(false),
        _ => Err(invalid("invalid boolean")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct NoDatabase;
    impl Connection for NoDatabase {
        fn query(&mut self, _: &str, _: Duration) -> Result<Rows, String> {
            panic!("guard/hook must run before database access")
        }
    }
    fn fresh() -> Fence {
        Fence::new(FenceConfig {
            applications: BTreeSet::from(["app".into()]),
            tables: vec![Table {
                schema: "public".into(),
                name: "t".into(),
                owner: "admin".into(),
            }],
            deadline_ms: 1000,
        })
        .unwrap()
    }
    #[test]
    fn migration_binding_and_unbound_refusal() {
        let mut f = fresh();
        for step in [
            Step::PreFence,
            Step::Save,
            Step::Revoke,
            Step::NoLogin,
            Step::Prove,
        ] {
            f.next = step;
            assert!(matches!(
                f.execute_next(
                    &mut NoDatabase,
                    &mut |_| panic!("unbound journal"),
                    &mut || panic!("unbound hook")
                ),
                Err(FenceError::Invalid(_))
            ));
        }
        assert!(
            f.unfence(&mut NoDatabase, &mut |_| panic!("unbound restore"))
                .is_err()
        );
        assert!(f.bind_migration("").is_err());
        f.bind_migration("m").unwrap();
        f.bind_migration("m").unwrap();
        assert!(f.bind_migration("other").is_err());
        assert_eq!(f.migration, "m");
    }
    #[test]
    fn hook_completion_is_durable_and_errors_leave_prefence() {
        let mut f = fresh();
        f.bind_migration("m").unwrap();
        let before = f.clone();
        assert!(matches!(
            f.execute_next(
                &mut NoDatabase,
                &mut |_| panic!("failed hook journalled"),
                &mut || Err("hook failed".into())
            ),
            Err(FenceError::Hook(_))
        ));
        assert_eq!(f, before);
        assert!(matches!(
            f.execute_next(&mut NoDatabase, &mut |_| Err("disk".into()), &mut || Ok(())),
            Err(FenceError::Journal(_))
        ));
        assert_eq!(f, before);
        let mut record = None;
        let mut calls = 0;
        f.execute_next(
            &mut NoDatabase,
            &mut |i| {
                record = Some(serde_json::to_string(i).unwrap());
                Ok(())
            },
            &mut || {
                calls += 1;
                Ok(())
            },
        )
        .unwrap();
        let Intent::Step(recovered) = serde_json::from_str::<Intent>(&record.unwrap()).unwrap()
        else {
            panic!("not a step")
        };
        assert_eq!(*recovered, f);
        assert_eq!(recovered.next, Step::Save);
        assert_eq!(calls, 1);
    }
    #[test]
    fn quoting() {
        assert_eq!(ident("a\"b"), "\"a\"\"b\"");
        assert_eq!(literal("a'\\b"), "E'a''\\\\b'");
    }
    #[test]
    fn qualified_table_sql() {
        let table = Table {
            schema: "odd\"schema".into(),
            name: "a.b".into(),
            owner: "ignored".into(),
        };
        assert_eq!(table_sql(&table), "\"odd\"\"schema\".\"a.b\"");
    }
    #[test]
    fn missing_capability_refuses() {
        assert!(require_capability(&vec![vec!["role_session_termination_fake".into()]]).is_err());
        assert!(require_capability(&vec![vec!["a,role_session_termination,b".into()]]).is_ok());
    }
    #[test]
    fn owner_superuser_are_blocking() {
        let catalog = Catalog {
            roles: vec![Role {
                name: "app".into(),
                login: false,
                superuser: true,
                bypass_rls: false,
            }],
            tables: vec![Table {
                schema: "public".into(),
                name: "t".into(),
                owner: "app".into(),
            }],
            ..Catalog::default()
        };
        let residuals = classify_residuals(&catalog, &BTreeSet::from(["app".into()]));
        assert!(residuals.iter().all(|r| !r.acknowledged));
        assert!(
            residuals
                .iter()
                .any(|r| r.principal.kind == ResidualKind::TableOwner)
        );
        assert!(
            residuals
                .iter()
                .any(|r| r.principal.kind == ResidualKind::Superuser)
        );
    }
}
