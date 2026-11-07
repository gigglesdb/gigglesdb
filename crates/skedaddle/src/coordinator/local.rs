//! Rebuildable giggles-core projection. No core files or APIs are modified.

use std::path::{Path, PathBuf};

use giggles_core::{
    Admin, Limits, SqlSession,
    relational::{MAX_ROWS, Scan, SqlRow, Value as V},
};

use super::{Error, Materialized, Result, Transition, journal::Entry};

const SCHEMA: &[&str] = &[
    "CREATE TABLE definition (version BIGINT PRIMARY KEY, digest TEXT NOT NULL, json TEXT NOT NULL)",
    "CREATE TABLE lifecycle (id BIGINT PRIMARY KEY, state TEXT NOT NULL, since_seq BIGINT NOT NULL, authority TEXT NOT NULL, blockers_json TEXT NOT NULL)",
    "CREATE TABLE journal_applied (id BIGINT PRIMARY KEY, seq BIGINT NOT NULL, digest TEXT NOT NULL)",
    "CREATE TABLE progress (stream TEXT PRIMARY KEY, marker TEXT NOT NULL, position TEXT NOT NULL)",
    "CREATE TABLE chunks (id TEXT PRIMARY KEY, table_name TEXT NOT NULL, range_lo TEXT NOT NULL, range_hi TEXT NOT NULL, state TEXT NOT NULL, low_wm TEXT, high_wm TEXT)",
    "CREATE TABLE audit (seq BIGINT PRIMARY KEY, kind TEXT NOT NULL, json TEXT NOT NULL)",
    "CREATE TABLE fence_state (id TEXT PRIMARY KEY, db TEXT NOT NULL, role TEXT NOT NULL, saved_privileges_json TEXT NOT NULL)",
];
const TABLES: &[(&str, &[(&str, &str)])] = &[
    (
        "definition",
        &[("version", "bigint"), ("digest", "text"), ("json", "text")],
    ),
    (
        "lifecycle",
        &[
            ("id", "bigint"),
            ("state", "text"),
            ("since_seq", "bigint"),
            ("authority", "text"),
            ("blockers_json", "text"),
        ],
    ),
    (
        "journal_applied",
        &[("id", "bigint"), ("seq", "bigint"), ("digest", "text")],
    ),
    (
        "progress",
        &[("stream", "text"), ("marker", "text"), ("position", "text")],
    ),
    (
        "chunks",
        &[
            ("id", "text"),
            ("table_name", "text"),
            ("range_lo", "text"),
            ("range_hi", "text"),
            ("state", "text"),
            ("low_wm", "text"),
            ("high_wm", "text"),
        ],
    ),
    (
        "audit",
        &[("seq", "bigint"), ("kind", "text"), ("json", "text")],
    ),
    (
        "fence_state",
        &[
            ("id", "text"),
            ("db", "text"),
            ("role", "text"),
            ("saved_privileges_json", "text"),
        ],
    ),
];

fn text(v: impl Into<String>) -> V {
    V::Text(v.into())
}
fn num(n: u64) -> Result<V> {
    Ok(V::Int(n.try_into().map_err(|_| {
        Error::Rejected("local BIGINT exhausted".into())
    })?))
}
fn json(value: &impl serde::Serialize) -> Result<V> {
    Ok(text(serde_json::to_string(value)?))
}
fn label(value: &impl serde::Serialize) -> Result<V> {
    match serde_json::to_value(value)? {
        serde_json::Value::String(value) => Ok(text(value)),
        _ => Err(Error::Rejected("local enum label must be a string".into())),
    }
}
fn optional(s: &Option<String>) -> V {
    s.as_ref().map_or(V::Null, |s| text(s.clone()))
}
fn execute(s: &mut SqlSession, sql: &str, params: &[V]) -> Result<()> {
    s.execute(sql, params, None)?;
    Ok(())
}

pub struct Local {
    admin: Admin,
    state: Materialized,
    path: PathBuf,
}

impl Local {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        super::scratch::check(&path)?;
        let admin = Admin::create(&path, Limits::default())?;
        let mut s = admin.sql_session();
        execute(&mut s, "BEGIN", &[])?;
        for ddl in SCHEMA {
            execute(&mut s, ddl, &[])?;
        }
        let state = Materialized::default();
        execute(
            &mut s,
            "INSERT INTO lifecycle VALUES (0,$1,0,$2,$3)",
            &[
                label(&state.lifecycle.state)?,
                label(&state.lifecycle.authority)?,
                json(&Vec::<String>::new())?,
            ],
        )?;
        execute(&mut s, "INSERT INTO journal_applied VALUES (0,0,'')", &[])?;
        execute(&mut s, "COMMIT", &[])?;
        let local = Self { admin, state, path };
        local.validate_schema()?;
        Ok(local)
    }

    /// Opening requires a fully validated journal; local bytes never establish authority.
    pub fn open(path: impl AsRef<Path>, journal: &Materialized) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        super::scratch::check(&path)?;
        let admin = Admin::open(&path, Limits::default())?;
        let mut local = Self {
            admin,
            state: Materialized::default(),
            path,
        };
        local.validate_schema()?;
        let rows = local.read_collection("journal_applied", 128)?;
        let [row] = rows.as_slice() else {
            return Err(Error::RecoveryRequired(
                "journal_applied must be a singleton".into(),
            ));
        };
        let [V::Int(0), V::Int(seq), V::Text(digest)] = row.values.as_slice() else {
            return Err(Error::RecoveryRequired("invalid local sequence row".into()));
        };
        if *seq < 0 || *seq as u64 > journal.applied_seq {
            return Err(Error::RecoveryRequired(
                "local store is ahead of the journal".into(),
            ));
        }
        for entry in journal.audit.values().take(*seq as usize) {
            local.state.apply(entry)?;
        }
        if &local.state.digest != digest {
            return Err(Error::RecoveryRequired(
                "local digest differs from journal".into(),
            ));
        }
        // Reconstruct an independent expected projection and compare every durable row.
        local.validate_rows(true)?;
        Ok(local)
    }

    pub fn state(&self) -> &Materialized {
        &self.state
    }

    pub fn validate_schema(&self) -> Result<()> {
        use giggles_core::relational::DataType;
        let tables = self.admin.catalog()?.1;
        if tables.len() != TABLES.len() {
            return Err(Error::RecoveryRequired(
                "local schema table count differs".into(),
            ));
        }
        for (name, columns) in TABLES {
            let table = tables
                .iter()
                .find(|t| t.name == *name)
                .ok_or_else(|| Error::RecoveryRequired(format!("missing local table {name}")))?;
            if table.primary_key != [0]
                || table.columns.len() != columns.len()
                || table
                    .columns
                    .iter()
                    .zip(*columns)
                    .enumerate()
                    .any(|(i, (c, (name, ty)))| {
                        let ty = if *ty == "bigint" {
                            DataType::BigInt
                        } else {
                            DataType::Text
                        };
                        c.name != *name
                            || c.data_type != ty
                            || c.not_null != !(table.name == "chunks" && i >= 5)
                    })
            {
                return Err(Error::RecoveryRequired(format!(
                    "local schema differs for {name}"
                )));
            }
        }
        Ok(())
    }

    pub fn apply(&mut self, entry: &Entry) -> Result<()> {
        self.apply_checked(entry, true)
    }

    /// Reject unmaterializable input before the journal commit point. This stages
    /// the same SQL transaction and rolls it back; nothing durable is published.
    pub fn validate_transition(&mut self, entry: &Entry) -> Result<()> {
        self.apply_checked(entry, false)
    }

    fn apply_checked(&mut self, entry: &Entry, commit: bool) -> Result<()> {
        if entry.seq <= self.state.applied_seq {
            return self.state.apply(entry);
        }
        let mut next = self.state.clone();
        next.apply(entry)?;
        let mut s = self.admin.sql_session();
        execute(&mut s, "BEGIN", &[])?;
        let result = (|| {
            match &entry.transition {
                Transition::Define { definition } => execute(
                    &mut s,
                    "INSERT INTO definition VALUES ($1,$2,$3)",
                    &[
                        num(u64::from(definition.definition_version))?,
                        text(definition.canonical_digest()?),
                        json(definition)?,
                    ],
                )?,
                Transition::Progress {
                    stream,
                    marker,
                    position,
                } => upsert(
                    &mut s,
                    "progress",
                    "stream",
                    text(stream),
                    &[text(marker), text(position)],
                    &["marker", "position"],
                )?,
                Transition::Chunk { chunk } => {
                    let id = serde_json::to_string(&(&chunk.table, &chunk.range_lo))?;
                    upsert(
                        &mut s,
                        "chunks",
                        "id",
                        text(id),
                        &[
                            text(&chunk.table),
                            text(&chunk.range_lo),
                            text(&chunk.range_hi),
                            text(&chunk.state),
                            optional(&chunk.low_wm),
                            optional(&chunk.high_wm),
                        ],
                        &[
                            "table_name",
                            "range_lo",
                            "range_hi",
                            "state",
                            "low_wm",
                            "high_wm",
                        ],
                    )?;
                }
                Transition::FenceState {
                    db,
                    role,
                    saved_privileges_json,
                } => upsert(
                    &mut s,
                    "fence_state",
                    "id",
                    json(&(db, role))?,
                    &[text(db), text(role), text(saved_privileges_json)],
                    &["db", "role", "saved_privileges_json"],
                )?,
                _ => {}
            }
            let lc = &next.lifecycle;
            execute(
                &mut s,
                "UPDATE lifecycle SET state=$1,since_seq=$2,authority=$3,blockers_json=$4 WHERE id=0",
                &[
                    label(&lc.state)?,
                    num(lc.since_seq)?,
                    label(&lc.authority)?,
                    json(&lc.blockers)?,
                ],
            )?;
            // The authoritative entry stays in object storage. Local audit retains
            // the complete transition, avoiding duplicate timestamps and chain hashes.
            let kind = kind(&entry.transition);
            execute(
                &mut s,
                "INSERT INTO audit VALUES ($1,$2,$3)",
                &[num(entry.seq)?, text(kind), json(&entry.transition)?],
            )?;
            execute(
                &mut s,
                "UPDATE journal_applied SET seq=$1,digest=$2 WHERE id=0",
                &[num(entry.seq)?, text(&entry.digest)],
            )?;
            if commit {
                execute(&mut s, "COMMIT", &[])
            } else {
                s.rollback();
                Ok(())
            }
        })();
        if result.is_err() {
            s.rollback();
        }
        result?;
        if commit {
            self.state = next;
        }
        Ok(())
    }

    pub fn replay(&mut self, journal: &Materialized) -> Result<()> {
        if self.state.applied_seq > journal.applied_seq {
            return Err(Error::RecoveryRequired(
                "local store ahead of journal".into(),
            ));
        }
        let prior = self.state.applied_seq;
        if prior > 0
            && !journal
                .audit
                .get(&prior)
                .is_some_and(|entry| entry.digest == self.state.digest)
        {
            return Err(Error::RecoveryRequired(
                "local replay anchor differs from verified journal".into(),
            ));
        }
        for (_, entry) in journal
            .audit
            .range((std::ops::Bound::Excluded(prior), std::ops::Bound::Unbounded))
        {
            self.apply(entry)?;
        }
        Ok(())
    }

    pub fn rebuild(path: impl AsRef<Path>, journal: &Materialized) -> Result<Self> {
        let mut local = Self::create(path)?;
        local.replay(journal)?;
        Ok(local)
    }

    /// All collections use primary-key keyset paging in one read transaction.
    /// Each page is proved against a one-row reverse scan of the same interval:
    /// a short page whose last key isn't the interval maximum is truncation.
    pub fn read_collection(&self, table: &str, page_size: usize) -> Result<Vec<SqlRow>> {
        if page_size == 0 || page_size > MAX_ROWS {
            return Err(Error::TruncatedRead("page bound must be 1..=1024".into()));
        }
        if !TABLES.iter().any(|(name, _)| *name == table) {
            return Err(Error::Rejected("unknown projection table".into()));
        }
        let mut s = self.admin.sql_session();
        execute(&mut s, "BEGIN", &[])?;
        let mut lower = None;
        let mut rows = vec![];
        loop {
            let scan = |limit, reverse| Scan {
                table: table.into(),
                projection: vec![],
                prefix: vec![],
                lower: lower.clone(),
                upper: None,
                reverse,
                limit,
            };
            let page = s.scan(scan(page_size, false)).map_err(read_error)?.rows;
            let last = page.last().map(|r| r.values[0].clone());
            let maximum = s
                .scan(scan(1, true))
                .map_err(read_error)?
                .rows
                .first()
                .map(|r| r.values[0].clone());
            if page.len() < page_size && last != maximum {
                return Err(Error::TruncatedRead(format!(
                    "{table} page ended before its maximum"
                )));
            }
            if page.is_empty() {
                break;
            }
            if last == lower.as_ref().map(|(v, _)| v.clone()) {
                return Err(Error::TruncatedRead("page cursor did not advance".into()));
            }
            lower = last.clone().map(|v| (v, false));
            rows.extend(page);
            if last == maximum {
                break;
            }
        }
        s.rollback();
        Ok(rows)
    }

    fn validate_rows(&self, validate_audit: bool) -> Result<()> {
        if validate_audit {
            let audit = self.read_collection("audit", 128)?;
            if audit.len() as u64 != self.state.applied_seq {
                return Err(Error::RecoveryRequired(
                    "local audit has a gap or extra row".into(),
                ));
            }
            for (row, entry) in audit.iter().zip(self.state.audit.values()) {
                if row.values
                    != [
                        num(entry.seq)?,
                        text(kind(&entry.transition)),
                        json(&entry.transition)?,
                    ]
                {
                    return Err(Error::RecoveryRequired(
                        "local audit differs from journal".into(),
                    ));
                }
            }
        }
        let lc = &self.state.lifecycle;
        let lifecycle = self.read_collection("lifecycle", 128)?;
        if lifecycle.len() != 1
            || lifecycle[0].values
                != [
                    V::Int(0),
                    label(&lc.state)?,
                    num(lc.since_seq)?,
                    label(&lc.authority)?,
                    json(&lc.blockers)?,
                ]
        {
            return Err(Error::RecoveryRequired(
                "local lifecycle differs from journal".into(),
            ));
        }
        let definitions = self.read_collection("definition", 128)?;
        let expected = match &self.state.definition {
            Some(d) => vec![vec![
                num(u64::from(d.definition_version))?,
                text(d.canonical_digest()?),
                json(d)?,
            ]],
            None => vec![],
        };
        if definitions
            .iter()
            .map(|r| r.values.clone())
            .collect::<Vec<_>>()
            != expected
        {
            return Err(Error::RecoveryRequired(
                "local definition differs from journal".into(),
            ));
        }
        let mut expected = std::collections::BTreeMap::new();
        expected.insert(
            "progress",
            self.state
                .progress
                .iter()
                .map(|(stream, (marker, position))| {
                    vec![text(stream), text(marker), text(position)]
                })
                .collect::<Vec<_>>(),
        );
        expected.insert(
            "chunks",
            self.state
                .chunks
                .iter()
                .map(|(id, c)| {
                    vec![
                        text(id),
                        text(&c.table),
                        text(&c.range_lo),
                        text(&c.range_hi),
                        text(&c.state),
                        optional(&c.low_wm),
                        optional(&c.high_wm),
                    ]
                })
                .collect(),
        );
        expected.insert(
            "fence_state",
            self.state
                .fence_state
                .iter()
                .map(|(id, (db, role, p))| vec![text(id), text(db), text(role), text(p)])
                .collect(),
        );
        for (name, expected) in expected {
            if self
                .read_collection(name, 128)?
                .into_iter()
                .map(|r| r.values)
                .collect::<Vec<_>>()
                != expected
            {
                return Err(Error::RecoveryRequired(format!(
                    "local {name} differs from journal"
                )));
            }
        }
        Ok(())
    }
}

fn kind(t: &Transition) -> &'static str {
    match t {
        Transition::Define { .. } => "define",
        Transition::AcquireOwnership { .. } => "ownership",
        Transition::Lifecycle { .. } => "lifecycle",
        Transition::Progress { .. } => "progress",
        Transition::Chunk { .. } => "chunk",
        Transition::FenceState { .. } => "fence-state",
        Transition::Intent { .. } => "intent",
        Transition::ResumeEffect { .. } => "resume-effect",
        Transition::EffectCompleted { .. } => "effect-completed",
    }
}

fn read_error(e: giggles_core::CoreError) -> Error {
    match e {
        giggles_core::CoreError::Limit
        | giggles_core::CoreError::Sql(giggles_core::relational::SqlError::Limit) => {
            Error::TruncatedRead("core refused the bounded result".into())
        }
        other => Error::Core(other),
    }
}

fn upsert(
    s: &mut SqlSession,
    table: &str,
    key: &str,
    value: V,
    fields: &[V],
    columns: &[&str],
) -> Result<()> {
    let found = s.execute(
        &format!("SELECT {key} FROM {table} WHERE {key}=$1"),
        std::slice::from_ref(&value),
        None,
    )?;
    let mut params = vec![value];
    params.extend_from_slice(fields);
    if found.rows.is_empty() {
        let placeholders = (1..=params.len())
            .map(|n| format!("${n}"))
            .collect::<Vec<_>>()
            .join(",");
        execute(
            s,
            &format!("INSERT INTO {table} VALUES ({placeholders})"),
            &params,
        )
    } else {
        let sets = columns
            .iter()
            .enumerate()
            .map(|(i, c)| format!("{c}=${}", i + 2))
            .collect::<Vec<_>>()
            .join(",");
        execute(
            s,
            &format!("UPDATE {table} SET {sets} WHERE {key}=$1"),
            &params,
        )
    }
}

impl Local {
    /// Publish a full snapshot through core's public export API, cleaning its
    /// archive and staging files before publishing the checkpoint.
    pub async fn checkpoint(&self, journal: &super::journal::Journal) -> Result<()> {
        use base64::Engine;
        let archive = super::scratch::with_archive(&self.path, |export| {
            self.admin.export(export)?;
            Ok(std::fs::read(export)?)
        })?;
        journal
            .checkpoint_archive(
                &self.state,
                base64::engine::general_purpose::STANDARD.encode(archive),
            )
            .await
    }

    /// Rebuild on a new host with only object-store access. A full checkpoint
    /// restores through core's public API, then applies only journal tail rows.
    pub async fn recover(
        path: impl AsRef<Path>,
        journal: &super::journal::Journal,
    ) -> Result<Self> {
        use base64::Engine;
        use std::io::Write;
        let path = path.as_ref().to_path_buf();
        let recovery = journal.recover(true).await?;
        super::scratch::check(&path)?;
        if let Some(cp) = &recovery.checkpoint
            && let Some(encoded) = &cp.local_archive
        {
            let archive = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map_err(|e| {
                    Error::RecoveryRequired(format!("invalid local checkpoint encoding: {e}"))
                })?;
            let admin = super::scratch::with_archive(&path, |export| {
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(export)?;
                file.write_all(&archive)?;
                file.sync_all()?;
                drop(file);
                Admin::restore(export, &path, Limits::default()).map_err(|e| {
                    Error::RecoveryRequired(format!(
                        "local checkpoint archive rejected by core: {e}"
                    ))
                })
            })?;
            let mut local = Self {
                admin,
                state: cp.state.clone(),
                path,
            };
            local.validate_schema()?;
            // The checkpoint digest covers the complete exported state;
            // do not re-scan its historical audit rows during bounded recovery.
            local.validate_rows(false)?;
            let rows = local.read_collection("journal_applied", 128)?;
            if rows.len() != 1
                || rows[0].values != [V::Int(0), num(cp.seq)?, text(&cp.journal_digest)]
            {
                return Err(Error::RecoveryRequired(
                    "local archive anchor differs from checkpoint".into(),
                ));
            }
            for entry in &recovery.tail {
                local.apply(entry)?;
            }
            return Ok(local);
        }
        Self::rebuild(path, &recovery.state)
    }
}
