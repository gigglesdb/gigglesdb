//! Explicit, resumable installation cleanup with catalog evidence on both sides.
use crate::{cutover, pg::fence::Connection};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, time::Duration};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Side {
    Source,
    Destination,
}
// Dependency order: slots before publications, event triggers before schema.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ObjectKind {
    Slot,
    EventTrigger,
    Publication,
    ReplicationOrigin,
    Schema,
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Object {
    pub side: Side,
    pub kind: ObjectKind,
    pub name: String,
}
pub type Inventory = BTreeSet<Object>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FinalizeStep {
    Before,
    StopCapture,
    DropObjects,
    After,
    Complete,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Finalize {
    pub key: cutover::Key,
    pub drop_objects: bool,
    pub installed: Inventory,
    pub next: FinalizeStep,
    pub before: Option<Inventory>,
    pub after: Option<Inventory>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum FinalizeRecord {
    Intent(Finalize),
    Drop(Object),
    Checkpoint(Finalize),
}
pub type Journal<'a> = dyn FnMut(&FinalizeRecord) -> Result<(), String> + 'a;

impl Finalize {
    pub fn new(
        key: cutover::Key,
        drop_objects: bool,
        installed: Inventory,
    ) -> cutover::Result<Self> {
        cutover::State::new(key.clone())?;
        for object in &installed {
            if object.name.is_empty()
                || object.name.len() > 63
                || object.name.contains('\0')
                || (object.kind == ObjectKind::Schema && object.name != "_skedaddle")
            {
                return Err(cutover::Error::Invalid("invalid installed object manifest"));
            }
        }
        Ok(Self {
            key,
            drop_objects,
            installed,
            next: FinalizeStep::Before,
            before: None,
            after: None,
        })
    }

    /// `stop_capture` must durably stop AND join ALL forward and reverse capture
    /// workers on BOTH endpoints, idempotently under key, before returning.
    /// It is called again on drop retries: serialized progress is not runtime proof.
    /// Authority must exclude concurrent capture/trigger installers until return.
    /// Journal/audit remain outside this manifest.
    pub fn execute_next<A: cutover::Authority, S: Connection, D: Connection>(
        &mut self,
        authority: &mut A,
        source: &mut S,
        destination: &mut D,
        stop_capture: &mut impl FnMut(&cutover::Key) -> cutover::Result<()>,
        journal: &mut Journal<'_>,
    ) -> cutover::Result<()> {
        let key = self.key.clone();
        authority.run(&key, || {
            if self.next == FinalizeStep::Complete {
                return Ok(());
            }
            journal(&FinalizeRecord::Intent(self.clone())).map_err(cutover::Error::Journal)?;
            let mut updated = self.clone();
            updated.next = match self.next {
                FinalizeStep::Before => {
                    updated.before = Some(inventory(&self.installed, source, destination)?);
                    FinalizeStep::StopCapture
                }
                FinalizeStep::StopCapture => {
                    stop_capture(&key)?;
                    FinalizeStep::DropObjects
                }
                FinalizeStep::DropObjects => {
                    if self.drop_objects {
                        // Re-establish runtime proof even after a restored checkpoint.
                        stop_capture(&key)?;
                        // Drop every endpoint's trigger before any global DDL.
                        let mut drops: Vec<_> = self
                            .planned_drops(&inventory(&self.installed, source, destination)?)
                            .into_iter()
                            .collect();
                        drops.sort_by_key(|object| (object.kind, object.side));
                        for object in drops {
                            if matches!(object.kind, ObjectKind::Publication | ObjectKind::Schema) {
                                let slots = owned_slots(&self.installed);
                                verify_global_drop(source, &slots)?;
                                verify_global_drop(destination, &slots)?;
                            }
                            journal(&FinalizeRecord::Drop(object.clone()))
                                .map_err(cutover::Error::Journal)?;
                            match object.side {
                                Side::Source => drop_object(source, &object)?,
                                Side::Destination => drop_object(destination, &object)?,
                            };
                        }
                    }
                    FinalizeStep::After
                }
                FinalizeStep::After => {
                    updated.after = Some(inventory(&self.installed, source, destination)?);
                    // Retain evidence even when a concurrent leftover is detected.
                    FinalizeStep::Complete
                }
                FinalizeStep::Complete => unreachable!(),
            };
            journal(&FinalizeRecord::Checkpoint(updated.clone()))
                .map_err(cutover::Error::Journal)?;
            *self = updated;
            Ok(())
        })
    }

    /// Remaining installed objects; empty after successful explicit removal.
    /// A retained schema also represents all of its members (DROP CASCADE).
    pub fn leftovers(&self) -> Option<&Inventory> {
        self.after.as_ref()
    }

    /// Pure removal plan, restricted to the explicit installation manifest.
    pub fn planned_drops(&self, observed: &Inventory) -> Inventory {
        if self.drop_objects {
            self.installed.intersection(observed).cloned().collect()
        } else {
            Inventory::new()
        }
    }
}

fn literal(s: &str) -> String {
    format!("E'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
}
fn ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}
fn query(c: &mut impl Connection, sql: &str) -> cutover::Result<Vec<Vec<String>>> {
    c.query(sql, Duration::from_secs(30))
        .map_err(cutover::Error::Component)
}
fn inventory(
    installed: &Inventory,
    source: &mut impl Connection,
    destination: &mut impl Connection,
) -> cutover::Result<Inventory> {
    let mut found = Inventory::new();
    for object in installed {
        let (catalog, column, scope) = match object.kind {
            ObjectKind::Slot => (
                "pg_replication_slots",
                "slot_name",
                " AND database=current_database()",
            ),
            ObjectKind::EventTrigger => ("pg_event_trigger", "evtname", ""),
            ObjectKind::Publication => ("pg_publication", "pubname", ""),
            ObjectKind::ReplicationOrigin => ("pg_replication_origin", "roname", ""),
            ObjectKind::Schema => ("pg_namespace", "nspname", ""),
        };
        let sql = format!(
            "SELECT {column}::text FROM pg_catalog.{catalog} WHERE {column}={}{}",
            literal(&object.name),
            scope
        );
        let rows = match object.side {
            Side::Source => query(source, &sql)?,
            Side::Destination => query(destination, &sql)?,
        };
        if !rows.is_empty() {
            if rows.len() != 1 || rows[0] != [object.name.clone()] {
                return Err(cutover::Error::Invalid("unexpected catalog inventory"));
            }
            found.insert(object.clone());
        }
    }
    Ok(found)
}
fn drop_sql(object: &Object) -> String {
    let name = ident(&object.name);
    match object.kind {
        ObjectKind::Slot => format!(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name={} AND database=current_database()",
            literal(&object.name)
        ),
        ObjectKind::ReplicationOrigin => {
            format!(
                "SELECT pg_replication_origin_drop(roname) FROM pg_replication_origin WHERE roname={}",
                literal(&object.name)
            )
        }
        ObjectKind::EventTrigger => format!("DROP EVENT TRIGGER IF EXISTS {name}"),
        ObjectKind::Publication => format!("DROP PUBLICATION IF EXISTS {name}"),
        ObjectKind::Schema => format!("DROP SCHEMA IF EXISTS {name} CASCADE"),
    }
}

// Check only this installation's logical capture slots (the manifest's slot
// names, from either side) in each database: an unrelated application's active
// slot must not block finalize. The stop/join callback covers buffered workers.
const ACTIVE_CAPTURE_SQL: &str = "SELECT slot_name::text FROM pg_catalog.pg_replication_slots WHERE database=current_database() AND slot_type='logical' AND active";

/// Names of every slot in the installation manifest, regardless of side.
fn owned_slots(installed: &Inventory) -> BTreeSet<&str> {
    installed
        .iter()
        .filter(|object| object.kind == ObjectKind::Slot)
        .map(|object| object.name.as_str())
        .collect()
}

/// Active-capture probe restricted to `slots`; `None` when nothing is owned.
/// `Connection` has no bind parameters, so names are escaped literals.
fn active_capture_sql(slots: &BTreeSet<&str>) -> Option<String> {
    if slots.is_empty() {
        return None;
    }
    let names: Vec<_> = slots.iter().map(|name| literal(name)).collect();
    Some(format!(
        "{ACTIVE_CAPTURE_SQL} AND slot_name IN ({})",
        names.join(",")
    ))
}
// Disabled or altered owned triggers are also unsafe; do not reuse the enabled
// installation predicate, which deliberately recognizes only a valid producer.
const OWNED_TRIGGER_SQL: &str = "SELECT e.evtname::text FROM pg_catalog.pg_event_trigger e JOIN pg_catalog.pg_proc p ON p.oid=e.evtfoid JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace WHERE starts_with(e.evtname,'skedaddle_ddl_') OR (n.nspname='_skedaddle' AND starts_with(p.proname,'skedaddle_ddl_'))";

fn verify_global_drop(
    connection: &mut impl Connection,
    slots: &BTreeSet<&str>,
) -> cutover::Result<()> {
    let active = match active_capture_sql(slots) {
        Some(sql) => query(connection, &sql)?,
        None => Vec::new(),
    };
    if !active.is_empty() {
        return Err(cutover::Error::Invalid(
            "finalize requires all capture stopped",
        ));
    }
    if !query(connection, OWNED_TRIGGER_SQL)?.is_empty() {
        return Err(cutover::Error::Invalid(
            "finalize requires owned event trigger absent",
        ));
    }
    Ok(())
}

fn drop_object(connection: &mut impl Connection, object: &Object) -> cutover::Result<()> {
    query(connection, "BEGIN")?;
    let result = (|| {
        let origin = crate::pg::ddl::administrative_origin(connection, Duration::from_secs(30))
            .map_err(|e| cutover::Error::Component(e.to_string()))?;
        query(
            connection,
            &format!("{origin}{}; COMMIT;", drop_sql(object)),
        )?;
        Ok(())
    })();
    if result.is_err() {
        let _ = query(connection, "ROLLBACK");
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct Recording {
        active: bool,
        trigger: bool,
        installed: bool,
        sql: Vec<String>,
    }
    impl Connection for Recording {
        fn query(&mut self, sql: &str, _: Duration) -> Result<Vec<Vec<String>>, String> {
            self.sql.push(sql.into());
            if (sql.starts_with(ACTIVE_CAPTURE_SQL) && self.active)
                || (sql == OWNED_TRIGGER_SQL && self.trigger)
            {
                Ok(vec![vec!["still_present".into()]])
            } else if sql == crate::pg::ddl::ADMIN_INSTALLATION_SQL && self.installed {
                Ok(vec![vec!["d9".into()]])
            } else if sql.starts_with("SELECT evtname::text") && self.trigger {
                Ok(vec![vec!["skedaddle_ddl_d9".into()]])
            } else if sql.contains("DROP EVENT TRIGGER") {
                self.trigger = false;
                Ok(vec![])
            } else if sql.starts_with("SELECT pubname::text") {
                Ok(vec![vec!["publication".into()]])
            } else if sql.starts_with("SELECT nspname::text") {
                Ok(vec![vec!["_skedaddle".into()]])
            } else {
                Ok(vec![])
            }
        }
    }
    struct Guard;
    impl cutover::Authority for Guard {
        fn run<T>(
            &mut self,
            _: &cutover::Key,
            run: impl FnOnce() -> cutover::Result<T>,
        ) -> cutover::Result<T> {
            run()
        }
    }
    fn finalizer(kind: ObjectKind) -> Finalize {
        let mut state = Finalize::new(
            cutover::Key {
                migration: "m".into(),
                cutover: "c".into(),
                epoch: 1,
                coordinator: "owner".into(),
            },
            true,
            BTreeSet::from([
                Object {
                    side: Side::Source,
                    kind,
                    name: if kind == ObjectKind::Schema {
                        "_skedaddle"
                    } else {
                        "publication"
                    }
                    .into(),
                },
                Object {
                    side: Side::Source,
                    kind: ObjectKind::Slot,
                    name: "skedaddle_m".into(),
                },
            ]),
        )
        .unwrap();
        state.next = FinalizeStep::DropObjects;
        state
    }
    #[test]
    fn active_capture_probe_is_restricted_to_manifest_slots() {
        let installed = BTreeSet::from([
            Object {
                side: Side::Source,
                kind: ObjectKind::Slot,
                name: "skedaddle_m".into(),
            },
            Object {
                side: Side::Destination,
                kind: ObjectKind::Slot,
                name: "skedaddle_m_rev".into(),
            },
            Object {
                side: Side::Source,
                kind: ObjectKind::Publication,
                name: "skedaddle_pub".into(),
            },
        ]);
        let slots = owned_slots(&installed);
        assert_eq!(slots, BTreeSet::from(["skedaddle_m", "skedaddle_m_rev"]));
        assert_eq!(
            active_capture_sql(&slots).unwrap(),
            format!("{ACTIVE_CAPTURE_SQL} AND slot_name IN (E'skedaddle_m',E'skedaddle_m_rev')")
        );
        // Names are escaped, never spliced raw.
        assert_eq!(
            active_capture_sql(&BTreeSet::from(["a'b\\c"])).unwrap(),
            format!("{ACTIVE_CAPTURE_SQL} AND slot_name IN (E'a''b\\\\c')")
        );
        assert_eq!(active_capture_sql(&BTreeSet::new()), None);
    }

    #[test]
    fn unrelated_active_slot_does_not_block_global_drop() {
        // A connection reporting an active slot for any unrestricted probe.
        struct Foreign;
        impl Connection for Foreign {
            fn query(&mut self, sql: &str, _: Duration) -> Result<Vec<Vec<String>>, String> {
                if sql == ACTIVE_CAPTURE_SQL {
                    Ok(vec![vec!["app_slot".into()]])
                } else {
                    Ok(vec![])
                }
            }
        }
        verify_global_drop(&mut Foreign, &BTreeSet::from(["skedaddle_m"])).unwrap();
        verify_global_drop(&mut Foreign, &BTreeSet::new()).unwrap();
    }

    #[test]
    fn global_drops_refuse_active_capture_or_owned_trigger_on_either_endpoint() {
        for kind in [ObjectKind::Publication, ObjectKind::Schema] {
            for side in [Side::Source, Side::Destination] {
                for active in [false, true] {
                    let mut source = Recording::default();
                    let mut destination = Recording::default();
                    let blocked = if side == Side::Source {
                        &mut source
                    } else {
                        &mut destination
                    };
                    blocked.active = active;
                    blocked.trigger = !active;
                    let mut state = finalizer(kind);
                    let mut stopped = 0;
                    let error = state
                        .execute_next(
                            &mut Guard,
                            &mut source,
                            &mut destination,
                            &mut |_| {
                                stopped += 1;
                                Ok(())
                            },
                            &mut |_| Ok(()),
                        )
                        .unwrap_err();
                    assert!(matches!(error, cutover::Error::Invalid(_)));
                    assert_eq!(stopped, 1);
                    assert_eq!(state.next, FinalizeStep::DropObjects);
                    assert!(
                        !source
                            .sql
                            .iter()
                            .chain(&destination.sql)
                            .any(|sql| sql.contains("DROP "))
                    );
                }
            }
        }
    }
    #[test]
    fn resumed_drop_requires_fresh_runtime_stop_and_join() {
        let mut state = finalizer(ObjectKind::Publication);
        let mut source = Recording::default();
        let mut destination = Recording::default();
        let result = state.execute_next(
            &mut Guard,
            &mut source,
            &mut destination,
            &mut |_| Err(cutover::Error::Invalid("capture not joined")),
            &mut |_| Ok(()),
        );
        assert!(matches!(
            result,
            Err(cutover::Error::Invalid("capture not joined"))
        ));
        assert!(source.sql.is_empty() && destination.sql.is_empty());
    }
    #[test]
    fn drop_transaction_looks_up_origin_before_drop() {
        for installed in [false, true] {
            let mut connection = Recording {
                installed,
                ..Default::default()
            };
            let object = Object {
                side: Side::Source,
                kind: ObjectKind::EventTrigger,
                name: "skedaddle_ddl_d9".into(),
            };
            drop_object(&mut connection, &object).unwrap();
            assert_eq!(connection.sql[0], "BEGIN");
            assert_eq!(connection.sql[1], crate::pg::ddl::ADMIN_INSTALLATION_SQL);
            assert_eq!(
                connection.sql[2].contains("SET LOCAL skedaddle.admin_origin = 'd9';"),
                installed
            );
            assert!(connection.sql[2].contains("DROP EVENT TRIGGER"));
            assert!(connection.sql[2].ends_with("COMMIT;"));
        }
    }
}

#[cfg(test)]
mod ordering_tests {
    use super::*;
    #[test]
    fn drop_order_removes_both_endpoints_triggers_before_global_ddl() {
        // The safety callback rejects a global drop until every event trigger
        // has been removed, including the later-sorting destination side.
        let mut installed = Inventory::new();
        for side in [Side::Source, Side::Destination] {
            for (kind, name) in [
                (ObjectKind::EventTrigger, "skedaddle_ddl_d9"),
                (ObjectKind::Publication, "publication"),
            ] {
                installed.insert(Object {
                    side,
                    kind,
                    name: name.into(),
                });
            }
        }
        struct Catalog {
            trigger: bool,
        }
        impl Connection for Catalog {
            fn query(&mut self, sql: &str, _: Duration) -> Result<Vec<Vec<String>>, String> {
                if sql.contains("DROP EVENT TRIGGER") {
                    self.trigger = false;
                }
                Ok(if sql.starts_with("SELECT evtname::text") && self.trigger {
                    vec![vec!["skedaddle_ddl_d9".into()]]
                } else if sql.starts_with("SELECT pubname::text") {
                    vec![vec!["publication".into()]]
                } else if sql == OWNED_TRIGGER_SQL && self.trigger {
                    vec![vec!["skedaddle_ddl_d9".into()]]
                } else {
                    vec![]
                })
            }
        }
        struct Guard;
        impl cutover::Authority for Guard {
            fn run<T>(
                &mut self,
                _: &cutover::Key,
                run: impl FnOnce() -> cutover::Result<T>,
            ) -> cutover::Result<T> {
                run()
            }
        }
        let key = cutover::Key {
            migration: "m".into(),
            cutover: "c".into(),
            epoch: 1,
            coordinator: "owner".into(),
        };
        let mut state = Finalize::new(key, true, installed).unwrap();
        state.next = FinalizeStep::DropObjects;
        let mut source = Catalog { trigger: true };
        let mut destination = Catalog { trigger: true };
        let mut dropped = vec![];
        state
            .execute_next(
                &mut Guard,
                &mut source,
                &mut destination,
                &mut |_| Ok(()),
                &mut |record| {
                    if let FinalizeRecord::Drop(object) = record {
                        dropped.push((object.kind, object.side));
                    }
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(
            dropped,
            [
                (ObjectKind::EventTrigger, Side::Source),
                (ObjectKind::EventTrigger, Side::Destination),
                (ObjectKind::Publication, Side::Source),
                (ObjectKind::Publication, Side::Destination)
            ]
        );
    }
}
