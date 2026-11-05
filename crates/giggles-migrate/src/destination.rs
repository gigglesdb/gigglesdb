use crate::*;
use giggles_core::{Admin, Limits, SqlSession, relational::Value as V};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{collections::BTreeMap, path::Path};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Control {
    manifest: Manifest,
    checkpoints: BTreeMap<String, String>,
    state_digest: String,
    slots: usize,
    verified: Option<String>,
}
/// Status describes only an explicitly declared normalized offline cut.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Status {
    pub phase: String,
    pub chunks: usize,
    pub events: usize,
    pub declared_cut_complete: bool,
    pub source_completeness_proven: bool,
    pub horizon_micros: i64,
    pub writer_authority: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionDigest {
    pub partition: Key,
    pub count: usize,
    pub first: Key,
    pub last: Key,
    pub sha256: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Difference {
    pub index: usize,
    pub expected: Option<MaterializedRow>,
    pub actual: Option<MaterializedRow>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Comparison {
    pub equal: bool,
    pub horizon_micros: i64,
    pub expected: Vec<PartitionDigest>,
    pub actual: Vec<PartitionDigest>,
    pub differences: Vec<Difference>,
    pub differences_truncated: bool,
}
fn sql(s: &mut SqlSession, q: &str, p: &[V]) -> Result<giggles_core::relational::SqlResult> {
    Ok(s.execute(q, p, None)?)
}
fn payload<T: Serialize>(v: &T) -> Result<V> {
    let bytes = serde_json::to_vec(v)?;
    require(bytes.len() <= 48 * 1024, "SQL payload bound")?;
    Ok(V::Bytes(bytes))
}
fn decode<T: DeserializeOwned>(v: &V) -> Result<T> {
    match v {
        V::Bytes(b) => Ok(serde_json::from_slice(b)?),
        _ => Err(Error("invalid SQL payload type".into())),
    }
}
fn read_one<T: DeserializeOwned>(s: &mut SqlSession, table: &str) -> Result<T> {
    let r = sql(s, &format!("SELECT payload FROM {table} WHERE id=0"), &[])?;
    require(r.rows.len() == 1, "missing durable rehearsal metadata")?;
    decode(&r.rows[0].values[0])
}
fn write_one<T: Serialize>(s: &mut SqlSession, table: &str, v: &T) -> Result<()> {
    let r = sql(
        s,
        &format!("UPDATE {table} SET payload=$1 WHERE id=0"),
        &[payload(v)?],
    )?;
    require(r.affected == 1, "metadata update missing")
}
fn load(s: &mut SqlSession, m: &Manifest) -> Result<(State, Control)> {
    let state: State = read_one(s, "migration_stage")?;
    let control: Control = read_one(s, "migration_control")?;
    require(control.manifest == *m, "durable manifest mismatch")?;
    require(
        digest(&state)? == control.state_digest,
        "staging/checkpoint digest mismatch",
    )?;
    require(control.slots <= m.bounds.max_rows, "invalid slot count")?;
    let mut checked = State::default();
    checked.apply(m, &state.events.values().cloned().collect::<Vec<_>>())?;
    Ok((state, control))
}
/// SQL owner. All logical mutations and checkpoints commit in one core transaction.
pub struct Importer {
    admin: Admin,
    manifest: Manifest,
}
impl Importer {
    pub fn create(path: impl AsRef<Path>, manifest: Manifest) -> Result<Self> {
        manifest.validate()?;
        let admin = Admin::create(path, Limits::default())?;
        let mut s = admin.sql_session();
        sql(&mut s, "BEGIN", &[])?;
        let result: Result<_> = (|| {
            sql(
                &mut s,
                "CREATE TABLE migration_stage (id BIGINT PRIMARY KEY, payload BYTEA NOT NULL)",
                &[],
            )?;
            sql(
                &mut s,
                "CREATE TABLE migration_control (id BIGINT PRIMARY KEY, payload BYTEA NOT NULL)",
                &[],
            )?;
            sql(
                &mut s,
                "CREATE TABLE migration_rows (id BIGINT PRIMARY KEY, active BOOLEAN NOT NULL, payload BYTEA NOT NULL)",
                &[],
            )?;
            let state = State::default();
            let c = Control {
                manifest: manifest.clone(),
                checkpoints: BTreeMap::new(),
                state_digest: digest(&state)?,
                slots: 0,
                verified: None,
            };
            sql(
                &mut s,
                "INSERT INTO migration_stage VALUES(0,$1)",
                &[payload(&state)?],
            )?;
            sql(
                &mut s,
                "INSERT INTO migration_control VALUES(0,$1)",
                &[payload(&c)?],
            )?;
            sql(&mut s, "COMMIT", &[])?;
            Ok(())
        })();
        if result.is_err() {
            s.rollback();
        }
        result?;
        Ok(Self { admin, manifest })
    }
    pub fn open(path: impl AsRef<Path>, manifest: Manifest) -> Result<Self> {
        manifest.validate()?;
        let admin = Admin::open(path, Limits::default())?;
        let mut s = admin.sql_session();
        sql(&mut s, "BEGIN", &[])?;
        let result = load(&mut s, &manifest);
        s.rollback();
        result?;
        Ok(Self { admin, manifest })
    }
    /// Exposes the actual destination SQL session for independent inspection.
    /// This trusted offline tool does not restrict the database owner's authority.
    pub fn sql_session(&self) -> SqlSession {
        self.admin.sql_session()
    }
    pub fn set_crash_point(&self, p: giggles_core::CrashPoint) -> Result<()> {
        Ok(self.admin.set_crash_point(p)?)
    }
    pub fn import(&mut self, chunk: &Chunk) -> Result<bool> {
        self.import_with_stage_hook(chunk, || Ok(()))
    }
    /// Hook after staged SQL effects, before checkpoint write, for fault injection.
    pub fn import_with_stage_hook(
        &mut self,
        chunk: &Chunk,
        hook: impl FnOnce() -> Result<()>,
    ) -> Result<bool> {
        require(!chunk.id.is_empty(), "empty chunk identity")?;
        require(
            chunk.events.len() <= self.manifest.bounds.max_events
                && serde_json::to_vec(chunk)?.len() <= self.manifest.bounds.max_chunk_bytes,
            "chunk bound",
        )?;
        let hash = chunk_digest(chunk)?;
        let mut s = self.admin.sql_session();
        sql(&mut s, "BEGIN", &[])?;
        let result: Result<_> = (|| {
            let (mut state, mut c) = load(&mut s, &self.manifest)?;
            if let Some(old) = c.checkpoints.get(&chunk.id) {
                require(*old == hash, "permanent chunk ID mismatch")?;
                s.rollback();
                return Ok(false);
            }
            state.apply(&self.manifest, &chunk.events)?;
            let rows = state.rows(&self.manifest, self.manifest.horizon_micros)?;
            write_one(&mut s, "migration_stage", &state)?;
            for (i, row) in rows.iter().enumerate() {
                if i < c.slots {
                    sql(
                        &mut s,
                        "UPDATE migration_rows SET active=true,payload=$1 WHERE id=$2",
                        &[payload(row)?, V::Int(i as i64)],
                    )?;
                } else {
                    sql(
                        &mut s,
                        "INSERT INTO migration_rows VALUES($1,true,$2)",
                        &[V::Int(i as i64), payload(row)?],
                    )?;
                }
            }
            for i in rows.len()..c.slots {
                sql(
                    &mut s,
                    "UPDATE migration_rows SET active=false WHERE id=$1",
                    &[V::Int(i as i64)],
                )?;
            }
            hook()?;
            c.slots = c.slots.max(rows.len());
            c.state_digest = digest(&state)?;
            c.checkpoints.insert(chunk.id.clone(), hash);
            c.verified = None;
            write_one(&mut s, "migration_control", &c)?;
            sql(&mut s, "COMMIT", &[])?;
            Ok(true)
        })();
        if result.is_err() {
            s.rollback();
        }
        result
    }
    pub fn staged_state(&self) -> Result<State> {
        let mut s = self.admin.sql_session();
        sql(&mut s, "BEGIN", &[])?;
        let r = load(&mut s, &self.manifest).map(|x| x.0);
        s.rollback();
        r
    }
    pub fn read_destination(&self) -> Result<Vec<MaterializedRow>> {
        read_destination(&mut self.admin.sql_session(), &self.manifest)
    }
    /// Expected state must be independently built from the normalized input cut.
    pub fn verify(&mut self, expected: &State, max_differences: usize) -> Result<Comparison> {
        let mut s = self.admin.sql_session();
        sql(&mut s, "BEGIN", &[])?;
        let result: Result<_> = (|| {
            let (state, mut c) = load(&mut s, &self.manifest)?;
            require(
                state == *expected,
                "expected provenance/cut differs from imported state",
            )?;
            let actual = read_destination(&mut s, &self.manifest)?;
            let rows = expected.rows(&self.manifest, self.manifest.horizon_micros)?;
            let report = compare_rows(&self.manifest, &rows, &actual, max_differences)?;
            c.verified = if report.equal {
                Some(digest(&rows)?)
            } else {
                None
            };
            write_one(&mut s, "migration_control", &c)?;
            sql(&mut s, "COMMIT", &[])?;
            Ok(report)
        })();
        if result.is_err() {
            s.rollback();
        }
        result
    }
    pub fn status(&self) -> Result<Status> {
        let mut s = self.admin.sql_session();
        sql(&mut s, "BEGIN", &[])?;
        let result: Result<_> = (|| {
            let (state, c) = load(&mut s, &self.manifest)?;
            let complete = state.declared_cut_complete(&self.manifest);
            let actual = read_destination(&mut s, &self.manifest)?;
            let expected = state.rows(&self.manifest, self.manifest.horizon_micros)?;
            let verified = c.verified.as_ref() == Some(&digest(&expected)?) && actual == expected;
            let phase = if verified && complete {
                "canary-ready"
            } else if verified {
                "reconciled"
            } else if complete {
                "capture-complete"
            } else {
                "source-only-baseline"
            };
            Ok(Status {
                phase: phase.into(),
                chunks: c.checkpoints.len(),
                events: state.events.len(),
                declared_cut_complete: complete,
                source_completeness_proven: false,
                horizon_micros: self.manifest.horizon_micros,
                writer_authority: "external; not inspected or transferred".into(),
            })
        })();
        s.rollback();
        result
    }
}
/// Reads actual SQL rows, never staging. Also checks physical sequence/order.
pub fn read_destination(s: &mut SqlSession, m: &Manifest) -> Result<Vec<MaterializedRow>> {
    let r = sql(
        s,
        "SELECT id,active,payload FROM migration_rows ORDER BY id LIMIT 257",
        &[],
    )?;
    require(r.rows.len() <= m.bounds.max_rows, "destination slot bound")?;
    let mut out: Vec<MaterializedRow> = vec![];
    let mut inactive = false;
    for (i, row) in r.rows.iter().enumerate() {
        require(
            row.values[0] == V::Int(i as i64),
            "destination sequence gap/order",
        )?;
        match &row.values[1] {
            V::Bool(false) => {
                inactive = true;
                continue;
            }
            V::Bool(true) => require(!inactive, "active row after inactive slot")?,
            _ => return Err(Error("destination active type".into())),
        }
        let row: MaterializedRow = decode(&row.values[2])?;
        m.validate_key(&row.partition, true)?;
        m.validate_key(&row.key, false)?;
        require(
            !row.cells.is_empty()
                && row.cells.iter().all(|(name, c)| {
                    m.columns.get(name).is_some_and(|t| t.accepts(&c.value))
                        && c.expires_at.is_none_or(|t| t > m.horizon_micros)
                }),
            "destination invalid/expired cells",
        )?;
        if let Some(last) = out.last() {
            require(
                row_order(m, last, &row) == std::cmp::Ordering::Less,
                "destination wrong key order/duplicate",
            )?;
        }
        out.push(row);
    }
    Ok(out)
}
fn partition_digests(m: &Manifest, rows: &[MaterializedRow]) -> Result<Vec<PartitionDigest>> {
    for pair in rows.windows(2) {
        require(
            row_order(m, &pair[0], &pair[1]) == std::cmp::Ordering::Less,
            "comparison row order",
        )?;
    }
    let mut groups: BTreeMap<Key, Vec<&MaterializedRow>> = BTreeMap::new();
    for r in rows {
        groups.entry(r.partition.clone()).or_default().push(r);
    }
    groups
        .into_iter()
        .map(|(partition, rows)| {
            Ok(PartitionDigest {
                partition,
                count: rows.len(),
                first: rows[0].key.clone(),
                last: rows[rows.len() - 1].key.clone(),
                sha256: digest(&("giggles-migrate-partition-v1", m.horizon_micros, &rows))?,
            })
        })
        .collect()
}
pub fn compare_rows(
    m: &Manifest,
    expected: &[MaterializedRow],
    actual: &[MaterializedRow],
    max_differences: usize,
) -> Result<Comparison> {
    require(max_differences > 0 && max_differences <= 256, "diff bound")?;
    let mut differences = vec![];
    let mut total = 0;
    for i in 0..expected.len().max(actual.len()) {
        if expected.get(i) != actual.get(i) {
            total += 1;
            if differences.len() < max_differences {
                differences.push(Difference {
                    index: i,
                    expected: expected.get(i).cloned(),
                    actual: actual.get(i).cloned(),
                });
            }
        }
    }
    Ok(Comparison {
        equal: total == 0,
        horizon_micros: m.horizon_micros,
        expected: partition_digests(m, expected)?,
        actual: partition_digests(m, actual)?,
        differences,
        differences_truncated: total > max_differences,
    })
}
/// Exact comparison of one bounded partition slice in declared clustering order.
/// The caller supplies independently read actual destination rows.
pub fn compare_range(
    m: &Manifest,
    partition: &Key,
    lower: Option<Bound>,
    upper: Option<Bound>,
    expected: &[MaterializedRow],
    actual: &[MaterializedRow],
    max_differences: usize,
) -> Result<Comparison> {
    m.validate_key(partition, true)?;
    for b in [&lower, &upper].into_iter().flatten() {
        m.validate_key(&b.key, false)?;
    }
    if let (Some(l), Some(u)) = (&lower, &upper) {
        require(
            m.compare_clustering(&l.key, &u.key) != std::cmp::Ordering::Greater,
            "reversed comparison range",
        )?;
    }
    let scope = Scope::Range { lower, upper };
    let slice = |rows: &[MaterializedRow]| {
        rows.iter()
            .filter(|r| r.partition == *partition && covers(m, &scope, &r.key, ""))
            .cloned()
            .collect::<Vec<_>>()
    };
    compare_rows(m, &slice(expected), &slice(actual), max_differences)
}
