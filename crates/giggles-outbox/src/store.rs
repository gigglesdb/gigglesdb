use crate::{
    state::{Action, State, identifier},
    *,
};
use giggles_core::{
    Admin, CoreError, SqlSession,
    relational::{DataType, SqlError},
};
use serde::Serialize;
use std::path::Path;

const TABLE: &str = "outbox_internal_state";
const CREATE: &str =
    "CREATE TABLE outbox_internal_state (id BIGINT PRIMARY KEY, body BYTEA NOT NULL)";
fn core(e: CoreError) -> Error {
    match e {
        CoreError::Sql(SqlError::Conflict | SqlError::Version | SqlError::Duplicate) => {
            Error::Conflict
        }
        CoreError::Limit | CoreError::Sql(SqlError::Limit) => Error::Limit,
        _ => Error::Core,
    }
}
fn internal(table: &str) -> bool {
    table.to_ascii_lowercase().starts_with("outbox_internal_")
}

/// A private trusted local Admin handle. Clones share the core, not a wrapper lock.
#[derive(Clone)]
pub struct Outbox {
    admin: Admin,
}

/// An error during publication can leave a committed decision with no reply.
/// Reopen and inspect both application identity and effect status. Never replay
/// arbitrary application SQL based on an effect ID alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum CommitOutcome {
    Committed,
    Unknown,
}

/// Owns a core explicit transaction. Drop and rollback discard all staged work.
pub struct OutboxTransaction {
    session: SqlSession,
    state: State,
    poisoned: bool,
    statements: usize,
    bytes: usize,
}
impl Outbox {
    pub fn create(path: impl AsRef<Path>, limits: Limits) -> Result<Self> {
        let state = State::new(limits)?;
        let admin = Admin::create(path, giggles_core::Limits::default()).map_err(core)?;
        let mut s = admin.sql_session();
        s.execute("BEGIN", &[], None).map_err(core)?;
        s.execute(CREATE, &[], None).map_err(core)?;
        s.execute(
            "INSERT INTO outbox_internal_state VALUES ($1,$2)",
            &[Value::Int(1), Value::Bytes(state.encode()?)],
            None,
        )
        .map_err(core)?;
        s.execute("COMMIT", &[], None).map_err(core)?;
        let out = Self { admin };
        out.validate_schema()?;
        Ok(out)
    }
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let out = Self {
            admin: Admin::open(path, giggles_core::Limits::default()).map_err(core)?,
        };
        out.validate_schema()?;
        let mut session = out.admin.sql_session();
        load(&mut session)?;
        Ok(out)
    }
    fn validate_schema(&self) -> Result<()> {
        let (_, tables) = self.admin.catalog().map_err(core)?;
        let ours: Vec<_> = tables.iter().filter(|t| internal(&t.name)).collect();
        if ours.len() != 1 {
            return Err(Error::Corrupt);
        }
        let t = ours[0];
        if t.name != TABLE
            || t.primary_key != [0]
            || t.columns.len() != 2
            || t.columns[0].name != "id"
            || t.columns[0].data_type != DataType::BigInt
            || !t.columns[0].not_null
            || t.columns[1].name != "body"
            || t.columns[1].data_type != DataType::Bytes
            || !t.columns[1].not_null
        {
            return Err(Error::Corrupt);
        }
        Ok(())
    }
    pub fn begin(&self) -> Result<OutboxTransaction> {
        self.validate_schema()?;
        let mut session = self.admin.sql_session();
        session.execute("BEGIN", &[], None).map_err(core)?;
        let state = load(&mut session)?;
        Ok(OutboxTransaction {
            session,
            state,
            poisoned: false,
            statements: 0,
            bytes: 0,
        })
    }
    /// Payload-free permanent enqueue/delivery status.
    pub fn status(&self, stream: &str, id: &str) -> Result<Option<EffectStatus>> {
        identifier(stream)?;
        identifier(id)?;
        let tx = self.begin()?;
        Ok(tx
            .state
            .index(stream, id)
            .ok()
            .map(|n| tx.state.effects[n].status()))
    }
    pub fn reconcile(&self) -> Result<Reconciliation> {
        Ok(self.begin()?.state.reconcile())
    }
    /// Read application rows through core's bounded single-table scan API.
    pub fn scan_application(&self, scan: Scan) -> Result<SqlResult> {
        if internal(&scan.table) {
            return Err(Error::Forbidden);
        }
        self.admin.sql_session().scan(scan).map_err(core)
    }
    pub fn claim(
        &self,
        stream: &str,
        owner: &str,
        now: TrustedTime,
        ttl: u64,
        limit: usize,
    ) -> Result<ClaimOutcome<Vec<Delivery>>> {
        let mut tx = self.begin()?;
        let value = tx.state.claim(stream, owner, now, ttl, limit)?;
        Ok(match tx.commit()? {
            CommitOutcome::Committed => ClaimOutcome::Committed(value),
            CommitOutcome::Unknown => ClaimOutcome::Unknown,
        })
    }
    fn resolve(
        &self,
        token: &ClaimToken,
        now: TrustedTime,
        action: Action,
    ) -> Result<CommitOutcome> {
        let mut tx = self.begin()?;
        tx.state.resolve(token, now, action)?;
        tx.commit()
    }
    pub fn renew(&self, token: &ClaimToken, now: TrustedTime, ttl: u64) -> Result<CommitOutcome> {
        self.resolve(token, now, Action::Renew(ttl))
    }
    pub fn release(&self, token: &ClaimToken, now: TrustedTime) -> Result<CommitOutcome> {
        self.resolve(token, now, Action::Release)
    }
    pub fn ack(&self, token: &ClaimToken, now: TrustedTime) -> Result<CommitOutcome> {
        self.resolve(token, now, Action::Ack)
    }
}
/// Unknown claims disclose no payload or token. Reconcile before retrying.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum ClaimOutcome<T> {
    Committed(T),
    Unknown,
}

fn load(s: &mut SqlSession) -> Result<State> {
    // LIMIT 2 detects non-singleton state, rather than ignoring extra rows.
    let r = s
        .execute(
            "SELECT id,body FROM outbox_internal_state LIMIT 2",
            &[],
            None,
        )
        .map_err(core)?;
    if r.rows.len() != 1 {
        return Err(Error::Corrupt);
    }
    match r.rows[0].values.as_slice() {
        [Value::Int(1), Value::Bytes(b)] => State::decode(b),
        _ => Err(Error::Corrupt),
    }
}
impl OutboxTransaction {
    fn guard<T>(&mut self, result: Result<T>) -> Result<T> {
        if result.is_err() {
            self.poisoned = true;
            self.session.abort();
        }
        result
    }
    /// Only parsed application mutations/DDL are allowed. Controls and SELECT
    /// (including joins) are refused. Errors poison the complete transaction.
    pub fn execute(
        &mut self,
        sql: &str,
        params: &[Value],
        expected_version: Option<u64>,
    ) -> Result<SqlResult> {
        if self.poisoned {
            return Err(Error::Aborted);
        }
        let result = (|| {
            let param_bytes = params.iter().try_fold(0usize, |n, v| {
                n.checked_add(match v {
                    Value::Text(s) => s.len(),
                    Value::Bytes(b) => b.len(),
                    Value::Numeric(n) => n.to_string().len(),
                    Value::Date(_) => 4,
                    Value::Time(_) => 8,
                    Value::TimeTz(_) => 12,
                    Value::Timestamp(_) => 8,
                    Value::TimestampTz(_) => 8,
                    Value::Interval(_) => 16,
                    Value::Float4(_) => 4,
                    Value::Float8(_) => 8,
                    Value::Null | Value::Bool(_) | Value::Int(_) => 9,
                })
                .ok_or(Error::Limit)
            })?;
            let bytes = self
                .bytes
                .checked_add(sql.len())
                .and_then(|n| n.checked_add(param_bytes))
                .ok_or(Error::Limit)?;
            if bytes > self.state.limits.sql_bytes
                || self.statements >= self.state.limits.statements
            {
                return Err(Error::Limit);
            }
            let p = self.session.prepare(sql).map_err(core)?;
            if !matches!(p.command(), "CREATE TABLE" | "INSERT" | "UPDATE" | "DELETE")
                || p.table().is_none_or(internal)
            {
                return Err(Error::Forbidden);
            }
            self.bytes = bytes;
            self.statements += 1;
            self.session
                .execute_prepared(&p, params, expected_version)
                .map_err(core)
        })();
        self.guard(result)
    }
    pub fn enqueue(&mut self, intent: EnqueueIntent<'_>) -> Result<EnqueueReceipt> {
        self.enqueue_batch(&[intent]).map(|mut r| r.remove(0))
    }
    pub fn enqueue_batch(&mut self, intents: &[EnqueueIntent<'_>]) -> Result<Vec<EnqueueReceipt>> {
        if self.poisoned {
            return Err(Error::Aborted);
        }
        let result = self.state.enqueue(intents);
        self.guard(result)
    }
    pub fn rollback(mut self) {
        self.session.rollback();
    }
    pub fn commit(mut self) -> Result<CommitOutcome> {
        if self.poisoned {
            return Err(Error::Aborted);
        }
        let bytes = self.state.encode()?;
        self.session
            .execute(
                "UPDATE outbox_internal_state SET body=$1 WHERE id=1",
                &[Value::Bytes(bytes)],
                None,
            )
            .map_err(core)?;
        match self.session.execute("COMMIT", &[], None) {
            Ok(_) => Ok(CommitOutcome::Committed),
            Err(CoreError::Storage(_) | CoreError::Fenced) => Ok(CommitOutcome::Unknown),
            Err(e) => Err(core(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fresh(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "outbox-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }
    fn appscan() -> Scan {
        Scan {
            table: "orders".into(),
            projection: vec![],
            prefix: vec![],
            lower: None,
            upper: None,
            reverse: false,
            limit: 10,
        }
    }
    #[test]
    fn crash_child() {
        let Ok(path) = std::env::var("OUTBOX_CRASH_FIXTURE") else {
            return;
        };
        let point = match std::env::var("OUTBOX_CRASH_POINT").unwrap().as_str() {
            "before" => giggles_core::CrashPoint::BeforeCommit,
            "after" => giggles_core::CrashPoint::AfterCommit,
            _ => panic!("invalid crash point"),
        };
        let o = Outbox::open(path).unwrap();
        let mut tx = o.begin().unwrap();
        tx.execute("INSERT INTO orders VALUES (1,'accepted')", &[], None)
            .unwrap();
        tx.enqueue(EnqueueIntent {
            stream: "tenant",
            effect_id: "order1",
            destination_kind: "local",
            payload: b"accepted",
        })
        .unwrap();
        o.admin.set_crash_point(point).unwrap();
        let _ = tx.commit();
        panic!("crash seam did not terminate");
    }
    #[test]
    fn subprocess_termination_before_after_actual_commit() {
        for (point, present) in [("before", false), ("after", true)] {
            let p = fresh(point);
            let o = Outbox::create(&p, Limits::default()).unwrap();
            let mut t = o.begin().unwrap();
            t.execute(
                "CREATE TABLE orders (id BIGINT PRIMARY KEY, status TEXT NOT NULL)",
                &[],
                None,
            )
            .unwrap();
            t.commit().unwrap();
            drop(o);
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "store::tests::crash_child", "--nocapture"])
                .env("OUTBOX_CRASH_FIXTURE", &p)
                .env("OUTBOX_CRASH_POINT", point)
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(86),
                "point={point} stdout={} stderr={}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            let o = Outbox::open(&p).unwrap();
            assert_eq!(
                !o.scan_application(appscan()).unwrap().rows.is_empty(),
                present
            );
            assert_eq!(o.status("tenant", "order1").unwrap().is_some(), present);
        }
    }
    #[test]
    fn ambiguous_commit_status_no_automatic_replay() {
        for (point, present) in [
            (giggles_core::CrashPoint::ErrorBeforeCommit, false),
            (giggles_core::CrashPoint::ErrorAfterDecisionCommit, true),
        ] {
            let p = fresh("unknown");
            let o = Outbox::create(&p, Limits::default()).unwrap();
            let mut t = o.begin().unwrap();
            t.execute(
                "CREATE TABLE orders (id BIGINT PRIMARY KEY, status TEXT NOT NULL)",
                &[],
                None,
            )
            .unwrap();
            t.commit().unwrap();
            let mut t = o.begin().unwrap();
            t.execute("INSERT INTO orders VALUES (1,'accepted')", &[], None)
                .unwrap();
            t.enqueue(EnqueueIntent {
                stream: "tenant",
                effect_id: "order1",
                destination_kind: "local",
                payload: b"accepted",
            })
            .unwrap();
            o.admin.set_crash_point(point).unwrap();
            assert_eq!(t.commit(), Ok(CommitOutcome::Unknown));
            drop(o);
            let o = Outbox::open(p).unwrap();
            assert_eq!(
                o.scan_application(appscan()).unwrap().rows.len(),
                usize::from(present)
            );
            assert_eq!(o.status("tenant", "order1").unwrap().is_some(), present);
            // Repeated status reads have no application side effects.
            for _ in 0..3 {
                let _ = o.status("tenant", "order1").unwrap();
            }
            assert_eq!(
                o.scan_application(appscan()).unwrap().rows.len(),
                usize::from(present)
            );
        }
    }
    #[test]
    fn malformed_schema_refused() {
        let p = std::env::temp_dir().join(format!(
            "outbox-badschema-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let admin = Admin::create(&p, giggles_core::Limits::default()).unwrap();
        admin
            .sql_session()
            .execute(
                "CREATE TABLE outbox_internal_state (id BIGINT PRIMARY KEY, body TEXT NOT NULL)",
                &[],
                None,
            )
            .unwrap();
        assert_eq!(Outbox { admin }.validate_schema(), Err(Error::Corrupt));
    }

    #[test]
    fn lifecycle_unknown_results_reconcile_each_persisted_boundary() {
        for action in ["claim", "renew", "release", "ack"] {
            for (point, persisted) in [
                (giggles_core::CrashPoint::ErrorBeforeCommit, false),
                (giggles_core::CrashPoint::ErrorAfterDecisionCommit, true),
            ] {
                let path = fresh(action);
                let o = Outbox::create(&path, Limits::default()).unwrap();
                let mut tx = o.begin().unwrap();
                tx.enqueue(EnqueueIntent {
                    stream: "s",
                    effect_id: "e",
                    destination_kind: "local",
                    payload: b"body",
                })
                .unwrap();
                assert_eq!(tx.commit(), Ok(CommitOutcome::Committed));
                let now = TrustedTime {
                    lower: 10,
                    upper: 10,
                };
                let token = if action != "claim" {
                    let ClaimOutcome::Committed(delivery) =
                        o.claim("s", "worker", now, 10, 1).unwrap()
                    else {
                        panic!("setup")
                    };
                    Some(delivery[0].token.clone())
                } else {
                    None
                };
                o.admin.set_crash_point(point).unwrap();
                let later = TrustedTime {
                    lower: 11,
                    upper: 11,
                };
                match action {
                    "claim" => assert_eq!(
                        o.claim("s", "worker", now, 10, 1),
                        Ok(ClaimOutcome::Unknown)
                    ),
                    "renew" => assert_eq!(
                        o.renew(token.as_ref().unwrap(), later, 20),
                        Ok(CommitOutcome::Unknown)
                    ),
                    "release" => assert_eq!(
                        o.release(token.as_ref().unwrap(), later),
                        Ok(CommitOutcome::Unknown)
                    ),
                    "ack" => assert_eq!(
                        o.ack(token.as_ref().unwrap(), later),
                        Ok(CommitOutcome::Unknown)
                    ),
                    _ => unreachable!(),
                }
                drop(o);
                let o = Outbox::open(&path).unwrap();
                let expected = match (action, persisted) {
                    ("claim", false) | ("release", true) => Phase::Ready,
                    ("ack", true) => Phase::Acked,
                    ("renew", true) => Phase::Claimed {
                        owner: "worker".into(),
                        deadline: 31,
                    },
                    _ => Phase::Claimed {
                        owner: "worker".into(),
                        deadline: 20,
                    },
                };
                for _ in 0..3 {
                    let state = o.status("s", "e").unwrap().unwrap();
                    assert_eq!(state.phase, expected);
                    assert_eq!(state.generation, u64::from(action != "claim" || persisted));
                    assert_eq!(state.attempts, state.generation);
                }
                assert_eq!(o.reconcile().unwrap().retained_ids, 1);
            }
        }
    }

    #[test]
    fn persisted_time_watermark_rejects_regression_after_reopen() {
        let path = fresh("time-reopen");
        let o = Outbox::create(&path, Limits::default()).unwrap();
        assert_eq!(
            o.claim(
                "s",
                "worker",
                TrustedTime {
                    lower: 100,
                    upper: 100
                },
                10,
                1
            ),
            Ok(ClaimOutcome::Committed(vec![]))
        );
        drop(o);
        let o = Outbox::open(&path).unwrap();
        assert_eq!(
            o.claim(
                "s",
                "worker",
                TrustedTime {
                    lower: 99,
                    upper: 99
                },
                10,
                1
            ),
            Err(Error::Time)
        );
        assert_eq!(
            o.claim(
                "s",
                "worker",
                TrustedTime {
                    lower: 100,
                    upper: 100
                },
                10,
                1
            ),
            Ok(ClaimOutcome::Committed(vec![]))
        );
    }
}
