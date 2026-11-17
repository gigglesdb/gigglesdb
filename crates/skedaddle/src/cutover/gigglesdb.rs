//! GigglesDB source fencing only. Final markers await GD-0077 capture.
use super::*;
use crate::gigglesdb::fence;

pub struct Source<C, H> {
    pub connection: C,
    pub state: fence::Fence,
    pub drained: DrainCounts,
    pub pre_fence: H,
}
fn component(error: impl fmt::Display) -> Error {
    Error::Component(error.to_string())
}
impl<C, H> Source<C, H> {
    /// Restore native snapshots and drain observations in journal order.
    pub fn replay(&mut self, record: &Record) {
        match record {
            Record::GigglesdbSource(
                fence::Intent::Step(state)
                | fence::Intent::Save(state)
                | fence::Intent::Unfence(state),
            ) => self.state = *state.clone(),
            Record::Drain(counts) => self.drained = counts.clone(),
            _ => {}
        }
    }
}
impl<C: fence::Connection, H: FnMut() -> std::result::Result<(), String>> SourceFence
    for Source<C, H>
{
    fn fence(&mut self, key: &Key, journal: &mut Journal<'_>) -> Result<DrainCounts> {
        self.state
            .bind_migration(&key.migration)
            .map_err(component)?;
        while self.state.next != fence::Step::Complete {
            self.state
                .execute_next(
                    &mut self.connection,
                    &mut |i| journal(&Record::GigglesdbSource(i.clone())),
                    &mut self.pre_fence,
                )
                .map_err(component)?;
            journal(&Record::GigglesdbSource(fence::Intent::Step(Box::new(
                self.state.clone(),
            ))))
            .map_err(Error::Journal)?;
        }
        // N7 requires role_session_termination: generation invalidation proves
        // the drain without observing a session count or inventing one.
        self.drained.remaining_sessions = 0;
        self.drained.prepared_transactions = 0;
        journal(&Record::Drain(self.drained.clone())).map_err(Error::Journal)?;
        Ok(self.drained.clone())
    }
    fn final_marker(&mut self, _key: &Key) -> Result<Marker> {
        Err(Error::Invalid(
            if self.state.next == fence::Step::Complete {
                "GigglesDB final marker needs GD-0077 capture"
            } else {
                "source not fenced"
            },
        ))
    }
    fn unfence(&mut self, key: &Key, journal: &mut Journal<'_>) -> Result<()> {
        self.state
            .bind_migration(&key.migration)
            .map_err(component)?;
        self.state
            .unfence(&mut self.connection, &mut |i| {
                journal(&Record::GigglesdbSource(i.clone()))
            })
            .map_err(component)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{cell::Cell, collections::BTreeSet, rc::Rc, time::Duration};

    // Scripted transport for adapter ordering only; real database behavior is
    // qualified separately by tests/gigglesdb_fence.rs.
    #[derive(Clone)]
    struct Script {
        login: bool,
        grant: bool,
        mutations: Vec<String>,
        observed: Rc<Cell<(bool, bool, usize)>>,
    }
    impl Default for Script {
        fn default() -> Self {
            Self {
                login: true,
                grant: true,
                mutations: vec![],
                observed: Rc::new(Cell::new((true, true, 0))),
            }
        }
    }
    impl fence::Connection for Script {
        fn query(&mut self, sql: &str, _: Duration) -> std::result::Result<fence::Rows, String> {
            let rows: Vec<Vec<&str>> = if sql == "SHOW giggles_capabilities" {
                vec![vec!["role_session_termination"]]
            } else if sql.starts_with("SELECT rolname") {
                vec![vec!["app", if self.login { "t" } else { "f" }, "f"]]
            } else if sql.starts_with("SELECT pg_get_userbyid(c.relowner)") {
                vec![vec!["admin"]]
            } else if sql.contains("aclexplode") {
                if self.grant {
                    vec![vec!["app", "admin", "INSERT", "f"]]
                } else {
                    vec![]
                }
            } else if sql.starts_with("SELECT has_table_privilege") {
                vec![vec![if self.grant { "t" } else { "f" }]]
            } else {
                self.mutations.push(sql.into());
                if sql.starts_with("REVOKE ") {
                    self.grant = false;
                } else if sql.starts_with("GRANT ") {
                    self.grant = true;
                } else if sql == "ALTER ROLE \"app\" NOLOGIN" {
                    self.login = false;
                } else if sql == "ALTER ROLE \"app\" LOGIN" {
                    self.login = true;
                } else {
                    panic!("unexpected SQL: {sql}");
                }
                self.observed
                    .set((self.login, self.grant, self.mutations.len()));
                vec![]
            };
            Ok(rows
                .into_iter()
                .map(|r| r.into_iter().map(str::to_owned).collect())
                .collect())
        }
    }
    fn key() -> Key {
        Key {
            migration: "m".into(),
            cutover: "c".into(),
            epoch: 1,
            coordinator: "owner".into(),
        }
    }
    fn source(
        calls: Rc<Cell<usize>>,
    ) -> Source<Script, impl FnMut() -> std::result::Result<(), String>> {
        Source {
            connection: Script::default(),
            state: fence::Fence::new(fence::FenceConfig {
                applications: BTreeSet::from(["app".into()]),
                tables: vec![fence::Table {
                    schema: "public".into(),
                    name: "t".into(),
                    owner: "admin".into(),
                }],
                deadline_ms: 1000,
            })
            .unwrap(),
            drained: DrainCounts::default(),
            pre_fence: move || {
                calls.set(calls.get() + 1);
                Ok(())
            },
        }
    }
    #[test]
    fn intents_restart_and_restore() {
        let calls = Rc::new(Cell::new(0));
        let mut s = source(calls.clone());
        let mut records = vec![];
        let observed = s.connection.observed.clone();
        let mut snapshots = vec![];
        let counts = SourceFence::fence(&mut s, &key(), &mut |r| {
            records.push(r.clone());
            snapshots.push(observed.get());
            Ok(())
        })
        .unwrap();
        assert_eq!(calls.get(), 1);
        assert_eq!(counts, DrainCounts::default());
        let steps: Vec<_> = records
            .iter()
            .filter_map(|r| match r {
                Record::GigglesdbSource(fence::Intent::Step(f)) => Some(f.next),
                _ => None,
            })
            .collect();
        use fence::Step::*;
        assert_eq!(
            steps,
            [
                Save, Save, Save, Revoke, Revoke, NoLogin, NoLogin, Prove, Prove, Complete
            ]
        );
        assert!(matches!(
            records[3],
            Record::GigglesdbSource(fence::Intent::Save(_))
        ));
        assert!(matches!(records.last(), Some(Record::Drain(_))));
        for (record, (login, grant, mutations)) in records.iter().zip(snapshots) {
            if !matches!(record, Record::GigglesdbSource(_)) {
                continue;
            }
            let mut restarted = source(calls.clone());
            restarted.replay(record);
            // A real server may already have executed an intent when the
            // client crashes. Replaying it must retain the original snapshot.
            restarted.connection.login = login;
            restarted.connection.grant = grant;
            restarted.connection.mutations = s.connection.mutations[..mutations].to_vec();
            let at = restarted.state.next;
            SourceFence::fence(&mut restarted, &key(), &mut |_| Ok(())).unwrap();
            assert_eq!(calls.get(), 1, "hook repeated from {at:?}");
            let expected_new = match at {
                Save | Revoke => 2,
                NoLogin => 1,
                Prove | Complete => 0,
                PreFence => unreachable!(),
            };
            assert_eq!(
                restarted.connection.mutations.len(),
                mutations + expected_new
            );
            SourceFence::unfence(&mut restarted, &key(), &mut |_| Ok(())).unwrap();
            assert!(restarted.connection.login && restarted.connection.grant);
        }
        let mut restore = vec![];
        SourceFence::unfence(&mut s, &key(), &mut |r| {
            restore.push(r.clone());
            Ok(())
        })
        .unwrap();
        assert!(matches!(
            restore[0],
            Record::GigglesdbSource(fence::Intent::Unfence(_))
        ));
        SourceFence::unfence(&mut s, &key(), &mut |_| Ok(())).unwrap();
        assert!(s.connection.login && s.connection.grant);
    }
    #[test]
    fn marker_refuses_and_binding_conflicts() {
        let mut s = source(Rc::new(Cell::new(0)));
        assert_eq!(
            SourceFence::final_marker(&mut s, &key()),
            Err(Error::Invalid("source not fenced"))
        );
        SourceFence::fence(&mut s, &key(), &mut |_| Ok(())).unwrap();
        assert_eq!(
            SourceFence::final_marker(&mut s, &key()),
            Err(Error::Invalid(
                "GigglesDB final marker needs GD-0077 capture"
            ))
        );
        let mut other = key();
        other.migration = "other".into();
        assert!(
            SourceFence::fence(&mut s, &other, &mut |_| panic!("conflict journalled")).is_err()
        );
        assert!(
            SourceFence::unfence(&mut s, &other, &mut |_| panic!("conflict journalled")).is_err()
        );
    }
    #[test]
    fn records_roundtrip_and_legacy_json() {
        let mut s = source(Rc::new(Cell::new(0)));
        SourceFence::fence(&mut s, &key(), &mut |r| {
            let json = serde_json::to_string(r).unwrap();
            let decoded: Record = serde_json::from_str(&json).unwrap();
            assert_eq!(
                serde_json::to_value(&decoded).unwrap(),
                serde_json::to_value(r).unwrap()
            );
            Ok(())
        })
        .unwrap();
        let old = r#"{"Drain":{"terminated":3,"termination_outcome_unknown":false,"remaining_sessions":0,"prepared_transactions":0}}"#;
        let record: Record = serde_json::from_str(old).unwrap();
        s.replay(&record);
        assert_eq!(s.drained.terminated, 3);
        assert!(matches!(
            serde_json::from_str::<Record>(old).unwrap(),
            Record::Drain(_)
        ));
    }
    #[test]
    fn journal_failure_precedes_mutations_and_partial_unfence() {
        let mut s = source(Rc::new(Cell::new(0)));
        let mut records = vec![];
        let result = SourceFence::fence(&mut s, &key(), &mut |r| {
            records.push(r.clone());
            if matches!(r, Record::GigglesdbSource(fence::Intent::Step(f)) if f.next == fence::Step::NoLogin)
            {
                Err("crash".into())
            } else {
                Ok(())
            }
        });
        assert!(result.is_err());
        assert!(!s.connection.grant && s.connection.login);
        assert_eq!(s.connection.mutations.len(), 1);
        s.replay(records.last().unwrap());
        SourceFence::unfence(&mut s, &key(), &mut |_| Ok(())).unwrap();
        assert!(s.connection.grant && s.connection.login);
    }
}
