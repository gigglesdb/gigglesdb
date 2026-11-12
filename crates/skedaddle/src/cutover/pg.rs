//! Thin synchronous adapters for P10's existing PostgreSQL modules.
//!
//! Use dedicated administrative transports implementing `fence::Connection`.
//! The external `Authority` guard must cover these transports as well as the
//! journal. Restore native snapshots and Drain records before resuming. All
//! administrative changes must be excluded while the guard/fences are held.

use super::*;
use crate::pg::{activate, fence};
use std::{collections::BTreeSet, time::Duration};

fn component(error: impl fmt::Display) -> Error {
    Error::Component(error.to_string())
}
fn activation_error(error: activate::Error) -> Error {
    match error {
        activate::Error::FenceBreached(reason) => Error::FenceBreached(reason),
        other => component(other),
    }
}
fn literal(s: &str) -> String {
    format!("E'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
}
const FINAL_MARKER_METADATA_SQL: &str =
    "SELECT to_regclass('_skedaddle.cutover_marker') IS NOT NULL";

fn final_marker_sql(key: &Key, marker: &Marker, ms: u64) -> String {
    format!(
        "BEGIN; SET LOCAL statement_timeout='{}ms'; INSERT INTO _skedaddle.cutover_marker VALUES({},{},{},pg_logical_emit_message(true,{},{})); COMMIT;",
        ms,
        literal(&key.migration),
        literal(&key.cutover),
        marker.epoch,
        literal(crate::pg::chunk::MESSAGE_PREFIX),
        literal(&marker.payload())
    )
}
fn cell(rows: &fence::Rows, column: usize) -> Result<&str> {
    rows.first()
        .and_then(|r| r.get(column))
        .map(String::as_str)
        .ok_or(Error::Invalid("missing PostgreSQL receipt field"))
}
fn timeout(ms: u64) -> Duration {
    Duration::from_millis(ms)
}
fn query(c: &mut impl fence::Connection, sql: &str, ms: u64) -> Result<fence::Rows> {
    c.query(sql, timeout(ms)).map_err(component)
}
fn timestamp(rows: &fence::Rows) -> Result<u64> {
    cell(rows, 0)?.parse().map_err(component)
}

pub struct Source<C, H> {
    pub connection: C,
    pub state: fence::Fence,
    /// Restore the latest Drain record alongside the native fence snapshot.
    pub drained: DrainCounts,
    pub pre_fence: H,
}

struct Observed<'a, C> {
    connection: &'a mut C,
    counts: &'a mut DrainCounts,
    journal: &'a mut Journal<'a>,
}
// Counting is observational; actual fencing/drain decisions stay in pg/fence.
impl<C: fence::Connection> fence::Connection for Observed<'_, C> {
    fn query(&mut self, sql: &str, timeout: Duration) -> std::result::Result<fence::Rows, String> {
        let terminating = sql.starts_with("SELECT pid::text, pg_terminate_backend");
        let already_uncertain = self.counts.termination_outcome_unknown;
        if terminating {
            self.counts.termination_outcome_unknown = true;
            (self.journal)(&Record::Drain(self.counts.clone()))?;
        }
        let rows = self.connection.query(sql, timeout)?;
        if terminating {
            self.counts.terminated += rows
                .iter()
                .filter(|r| r.get(1).is_some_and(|s| s == "t" || s == "true"))
                .count() as u64;
            self.counts.termination_outcome_unknown = already_uncertain;
            (self.journal)(&Record::Drain(self.counts.clone()))?;
        }
        Ok(rows)
    }
}
impl<C: fence::Connection, H: FnMut() -> std::result::Result<(), String>> SourceFence
    for Source<C, H>
{
    fn fence(&mut self, key: &Key, journal: &mut Journal<'_>) -> Result<DrainCounts> {
        self.state
            .bind_migration(&key.migration)
            .map_err(component)?;
        // A single forwarding callback avoids sharing mutable journal borrows.
        let sink = std::cell::RefCell::new(journal);
        while self.state.next != fence::Step::Complete {
            let mut counts_journal = |r: &Record| (*sink.borrow_mut())(r);
            let mut observed = Observed {
                connection: &mut self.connection,
                counts: &mut self.drained,
                journal: &mut counts_journal,
            };
            self.state
                .execute_next(
                    &mut observed,
                    &mut |i| (*sink.borrow_mut())(&Record::Source(i.clone())),
                    &mut self.pre_fence,
                )
                .map_err(component)?;
            // Native callback is intent-first; this also retains successful
            // drain/residual observations rather than re-sampling them on restart.
            (*sink.borrow_mut())(&Record::Source(fence::Intent::Step(Box::new(
                self.state.clone(),
            ))))
            .map_err(Error::Journal)?;
        }
        self.drained.remaining_sessions = 0;
        self.drained.prepared_transactions = 0;
        (*sink.borrow_mut())(&Record::Drain(self.drained.clone())).map_err(Error::Journal)?;
        Ok(self.drained.clone())
    }

    fn final_marker(&mut self, key: &Key) -> Result<Marker> {
        if self.state.next != fence::Step::Complete {
            return Err(Error::Invalid("source not fenced"));
        }
        let ms = self.state.config.deadline_ms;
        let metadata = query(&mut self.connection, FINAL_MARKER_METADATA_SQL, ms)?;
        if !matches!(cell(&metadata, 0)?, "t" | "true") {
            return Err(Error::Invalid(
                "missing _skedaddle.cutover_marker: run DDL capture trigger install before capture",
            ));
        }
        let read = format!(
            "SELECT cutover,epoch::text,lsn::text FROM _skedaddle.cutover_marker WHERE migration={}",
            literal(&key.migration)
        );
        let mut rows = query(&mut self.connection, &read, ms)?;
        if rows.is_empty() {
            let marker = Marker {
                cutover: key.cutover.clone(),
                epoch: self.state.config.epoch,
                lsn: 0,
            };
            // Message and receipt commit atomically. On a lost reply, retry the
            // read; never emit a replacement marker or guess F from WAL progress.
            query(
                &mut self.connection,
                &final_marker_sql(key, &marker, ms),
                ms,
            )?;
            rows = query(&mut self.connection, &read, ms)?;
        }
        if cell(&rows, 0)? != key.cutover {
            return Err(Error::Invalid("different cutover owns marker"));
        }
        Ok(Marker {
            cutover: cell(&rows, 0)?.into(),
            epoch: cell(&rows, 1)?.parse().map_err(component)?,
            lsn: fence::parse_lsn(cell(&rows, 2)?).map_err(component)?,
        })
    }

    fn unfence(&mut self, key: &Key, journal: &mut Journal<'_>) -> Result<()> {
        self.state
            .bind_migration(&key.migration)
            .map_err(component)?;
        self.state
            .unfence(&mut self.connection, &mut |i| {
                journal(&Record::Source(i.clone()))
            })
            .map_err(component)
    }
}

pub struct Target<C> {
    pub connection: C,
    pub state: activate::DestinationFence,
    pub acknowledged_triggers: BTreeSet<String>,
}
impl<C: fence::Connection> Target<C> {
    /// Explicit connector setup, before cutover. This DDL is never hidden in
    /// the machine's read-only activation-status probe.
    pub fn initialize(&mut self) -> Result<()> {
        let ms = self.state.config.timeout_ms;
        query(&mut self.connection, "BEGIN", ms)?;
        let result = (|| {
            let origin = crate::pg::ddl::administrative_origin(&mut self.connection, timeout(ms))
                .map_err(component)?;
            query(
                &mut self.connection,
                &format!(
                    "{origin}CREATE SCHEMA IF NOT EXISTS _skedaddle; CREATE TABLE IF NOT EXISTS _skedaddle.cutover_activation(migration text PRIMARY KEY,cutover text NOT NULL,epoch bigint NOT NULL,activated_ms bigint NOT NULL); COMMIT;"
                ),
                ms,
            )?;
            Ok(())
        })();
        if result.is_err() {
            let _ = query(&mut self.connection, "ROLLBACK", ms);
        }
        result
    }
    fn native_next(&mut self, journal: &mut Journal<'_>) -> Result<()> {
        self.state
            .execute_next(&mut self.connection, &mut |i| {
                journal(&Record::Destination(i.clone()))
            })
            .map_err(component)?;
        journal(&Record::Destination(activate::Intent {
            state: self.state.clone(),
        }))
        .map_err(Error::Journal)
    }
}

/// Injects the durable activation receipt INTO the native restoration
/// transaction, so application access and abort eligibility cannot disagree.
struct ActivationConnection<'a, C> {
    connection: &'a mut C,
    key: &'a Key,
}
impl<C: fence::Connection> fence::Connection for ActivationConnection<'_, C> {
    fn query(&mut self, sql: &str, timeout: Duration) -> std::result::Result<fence::Rows, String> {
        if sql == crate::pg::ddl::ADMIN_INSTALLATION_SQL {
            return self.connection.query(sql, timeout);
        }
        let body = sql
            .strip_suffix("COMMIT;")
            .ok_or("activation must be a transaction")?;
        let guarded = format!(
            "{body} INSERT INTO _skedaddle.cutover_activation VALUES({},{},{},floor(extract(epoch FROM clock_timestamp())*1000)::bigint); COMMIT;",
            literal(&self.key.migration),
            literal(&self.key.cutover),
            self.key.epoch
        );
        self.connection.query(&guarded, timeout)
    }
}
impl<C: fence::Connection> Destination for Target<C> {
    fn activated(&mut self, key: &Key) -> Result<Option<u64>> {
        let rows = query(
            &mut self.connection,
            &format!(
                "SELECT activated_ms::text,cutover FROM _skedaddle.cutover_activation WHERE migration={}",
                literal(&key.migration)
            ),
            self.state.config.timeout_ms,
        )?;
        if rows.is_empty() {
            return Ok(None);
        }
        if cell(&rows, 1)? != key.cutover {
            return Err(Error::UseRollback);
        }
        Ok(Some(timestamp(&rows)?))
    }
    fn prove_fenced(&mut self, _key: &Key, journal: &mut Journal<'_>) -> Result<()> {
        // CheckActivation is still fenced; Restore is ambiguous and must first
        // be resolved using the activation receipt by the caller.
        if self.state.next == activate::Step::Restore {
            self.state.next = activate::Step::CheckActivation;
        }
        self.state
            .observe(&mut self.connection, &mut |i| {
                journal(&Record::Destination(i.clone()))
            })
            .map_err(activation_error)
    }
    fn readiness(&mut self, key: &Key, journal: &mut Journal<'_>) -> Result<()> {
        self.prove_fenced(key, journal)?;
        let blockers = activate::inspect_validity(
            &mut self.connection,
            &self.state.config.tables,
            &self.acknowledged_triggers,
            timeout(self.state.config.timeout_ms),
        )
        .map_err(component)?;
        if blockers.iter().any(activate::Blocker::blocks) {
            return Err(Error::Component(format!(
                "destination blocked: {blockers:?}"
            )));
        }
        Ok(())
    }
    fn activate(
        &mut self,
        key: &Key,
        marker: &Marker,
        journal: &mut Journal<'_>,
        authorize: &mut dyn FnMut() -> Result<()>,
    ) -> Result<u64> {
        let receipt = self.activated(key)?;
        if receipt.is_some() {
            // Restoring grants again would resurrect privileges that an operator
            // may have intentionally changed after activation.
            if !matches!(
                self.state.next,
                activate::Step::PostActivate | activate::Step::Complete
            ) {
                self.state.next = activate::Step::VerifyActivation;
            }
        } else {
            self.state
                .begin_activation(
                    marker.payload(),
                    self.acknowledged_triggers.clone(),
                    &mut |i| journal(&Record::Destination(i.clone())),
                )
                .map_err(component)?;
        }
        while !matches!(
            self.state.next,
            activate::Step::PostActivate | activate::Step::Complete
        ) {
            if self.state.next == activate::Step::Restore {
                let proof = self
                    .state
                    .reverse_proof()
                    .cloned()
                    .ok_or(Error::ReverseProof(
                        crate::coordinator::reverse::ProofError::Missing,
                    ))?;
                // Resume at Restore must re-observe the physical fence too.
                let mut observed = self.state.clone();
                observed.next = activate::Step::CheckActivation;
                observed
                    .observe(&mut self.connection, &mut |i| {
                        journal(&Record::Destination(i.clone()))
                    })
                    .map_err(activation_error)?;
                observed.next = activate::Step::Restore;
                self.state = observed;
                let (identity, barrier) = crate::coordinator::reverse::destination_barrier(
                    &mut self.connection,
                    timeout(self.state.config.timeout_ms),
                )
                .map_err(component)?;
                proof
                    .check(key, &self.state, &identity, barrier)
                    .map_err(Error::ReverseProof)?;
                authorize()?;
                // Native intent must be saved before the atomic receipt/grants.
                let mut connection = ActivationConnection {
                    connection: &mut self.connection,
                    key,
                };
                self.state
                    .execute_next(&mut connection, &mut |i| {
                        journal(&Record::Destination(i.clone()))
                    })
                    .map_err(activation_error)?;
                journal(&Record::Destination(activate::Intent {
                    state: self.state.clone(),
                }))
                .map_err(Error::Journal)?;
            } else {
                self.native_next(journal)?;
            }
        }
        self.activated(key)?
            .ok_or(Error::Invalid("activation receipt missing"))
    }
    fn post_activate(&mut self, _key: &Key, journal: &mut Journal<'_>) -> Result<()> {
        if !matches!(
            self.state.next,
            activate::Step::PostActivate | activate::Step::Complete
        ) {
            return Err(Error::Invalid("destination not activated"));
        }
        if self.state.next == activate::Step::PostActivate {
            self.native_next(journal)?;
        }
        Ok(())
    }
}

pub struct SequenceSync<S, D> {
    pub source: S,
    pub destination: D,
    pub scope: activate::SequenceScope,
    pub timeout_ms: u64,
}
impl<S: fence::Connection, D: fence::Connection> Sequences for SequenceSync<S, D> {
    fn sync(&mut self, marker: &Marker) -> Result<()> {
        activate::sync_sequences(
            &mut self.source,
            &mut self.destination,
            &self.scope,
            &marker.payload(),
            timeout(self.timeout_ms),
        )
        .map_err(component)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn final_marker_sql_uses_exact_capture_prefix() {
        let key = Key {
            migration: "migration'\\".into(),
            cutover: "cutover".into(),
            epoch: 2,
            coordinator: "coordinator".into(),
        };
        let marker = Marker {
            cutover: key.cutover.clone(),
            epoch: 1,
            lsn: 0,
        };
        let sql = final_marker_sql(&key, &marker, 10_000);
        assert!(!sql.contains("CREATE"));
        assert!(!FINAL_MARKER_METADATA_SQL.contains("CREATE"));
        assert!(FINAL_MARKER_METADATA_SQL.contains("to_regclass('_skedaddle.cutover_marker')"));
        let arguments = sql.split_once("pg_logical_emit_message(true,").unwrap().1;
        let (prefix, _) = arguments.split_once(',').unwrap();
        assert_eq!(prefix, literal(crate::pg::chunk::MESSAGE_PREFIX));
        assert_eq!(sql.matches("pg_logical_emit_message(").count(), 1);
        assert!(sql.starts_with("BEGIN; SET LOCAL statement_timeout='10000ms';"));
        assert!(sql.ends_with(")); COMMIT;"));
        assert!(sql.contains(&literal(&key.migration)));
        assert!(sql.contains(&format!(",{},pg_logical_emit_message", marker.epoch)));
        assert!(sql.contains(&literal(&marker.payload())));
    }
}

#[cfg(test)]
mod administrative_tests {
    use super::*;

    #[derive(Default)]
    struct Recording {
        rows: fence::Rows,
        sql: Vec<String>,
    }
    impl fence::Connection for Recording {
        fn query(&mut self, sql: &str, _: Duration) -> std::result::Result<fence::Rows, String> {
            self.sql.push(sql.into());
            Ok(if sql == crate::pg::ddl::ADMIN_INSTALLATION_SQL {
                self.rows.clone()
            } else {
                vec![]
            })
        }
    }
    fn target(rows: fence::Rows) -> Target<Recording> {
        Target {
            connection: Recording {
                rows,
                ..Default::default()
            },
            state: activate::DestinationFence::new(activate::Config {
                epoch: 1,
                applications: BTreeSet::from(["app".into()]),
                tables: vec![fence::Table {
                    schema: "public".into(),
                    name: "items".into(),
                    owner: "owner".into(),
                }],
                timeout_ms: 1000,
                post_activate: None,
            })
            .unwrap(),
            acknowledged_triggers: BTreeSet::new(),
        }
    }
    #[test]
    fn initialization_tags_ddl_inside_transaction_and_rolls_back_ambiguity() {
        for installed in [false, true] {
            let mut target = target(if installed {
                vec![vec!["d9".into()]]
            } else {
                vec![]
            });
            target.initialize().unwrap();
            let sql = &target.connection.sql;
            assert_eq!(sql[0], "BEGIN");
            assert_eq!(sql[1], crate::pg::ddl::ADMIN_INSTALLATION_SQL);
            assert_eq!(
                sql[2].contains("SET LOCAL skedaddle.admin_origin = 'd9';"),
                installed
            );
            assert!(sql[2].ends_with("COMMIT;"));
            assert!(sql[2].contains("CREATE SCHEMA"));
            assert!(sql[2].contains("CREATE TABLE"));
        }
        let mut target = target(vec![vec!["one".into()], vec!["two".into()]]);
        assert!(target.initialize().is_err());
        assert_eq!(target.connection.sql.last().unwrap(), "ROLLBACK");
        assert!(
            !target
                .connection
                .sql
                .iter()
                .any(|sql| sql.contains("CREATE"))
        );
    }
    #[test]
    fn activation_receipt_wrapper_forwards_origin_lookup_and_keeps_atomic_receipt() {
        let key = Key {
            migration: "m".into(),
            cutover: "c".into(),
            epoch: 1,
            coordinator: "owner".into(),
        };
        let mut recording = Recording {
            rows: vec![vec!["d9".into()]],
            ..Default::default()
        };
        let mut connection = ActivationConnection {
            connection: &mut recording,
            key: &key,
        };
        let origin =
            crate::pg::ddl::administrative_origin(&mut connection, Duration::from_secs(1)).unwrap();
        fence::Connection::query(
            &mut connection,
            &format!("BEGIN; {origin}GRANT SELECT ON public.items TO app; COMMIT;"),
            Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(recording.sql.len(), 2);
        assert!(!recording.sql[0].contains("INSERT"));
        assert!(recording.sql[1].contains("admin_origin = 'd9'"));
        assert!(recording.sql[1].contains("INSERT INTO _skedaddle.cutover_activation"));
        assert!(recording.sql[1].ends_with("COMMIT;"));
    }
}
