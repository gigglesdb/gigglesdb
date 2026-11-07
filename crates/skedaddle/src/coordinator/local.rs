//! Rebuildable giggles-core projection. No core files or APIs are modified.

use std::path::{Path, PathBuf};

use giggles_core::{
    Admin, Limits, SqlSession,
    relational::{MAX_ROWS, Scan, SqlRow, Value as V},
};

use super::{Error, Materialized, Result, journal::Entry};

const SCHEMA: &[&str] = &[
    "CREATE TABLE definition (version BIGINT PRIMARY KEY, digest TEXT NOT NULL, json TEXT NOT NULL)",
    "CREATE TABLE lifecycle (id BIGINT PRIMARY KEY, state TEXT NOT NULL, since_seq BIGINT NOT NULL, authority TEXT NOT NULL, blockers_json TEXT NOT NULL)",
    "CREATE TABLE journal_applied (id BIGINT PRIMARY KEY, seq BIGINT NOT NULL, digest TEXT NOT NULL)",
    "CREATE TABLE progress (stream TEXT PRIMARY KEY, marker TEXT NOT NULL, position TEXT NOT NULL)",
    "CREATE TABLE chunks (id TEXT PRIMARY KEY, table_name TEXT NOT NULL, range_lo TEXT NOT NULL, range_hi TEXT NOT NULL, state TEXT NOT NULL, low_wm TEXT, high_wm TEXT)",
    "CREATE TABLE verifier_ranges (id TEXT PRIMARY KEY, json TEXT NOT NULL)",
    "CREATE TABLE verifier_exceptions (id TEXT PRIMARY KEY, json TEXT NOT NULL)",
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
    ("verifier_ranges", &[("id", "text"), ("json", "text")]),
    ("verifier_exceptions", &[("id", "text"), ("json", "text")]),
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
        if *seq as u64 != journal.applied_seq || digest != &journal.digest {
            return Err(Error::RecoveryRequired(
                "local anchor differs; recover to a new path from the paged journal".into(),
            ));
        }
        local.state = journal.clone();
        // Reconstruct an independent expected projection and compare every durable row.
        local.validate_rows(true)?;
        Ok(local)
    }

    /// Reopen a lagging cache by verifying its exact prefix through bounded
    /// journal pages, then replay the remaining tail. No history clone is built.
    pub async fn open_journal(
        path: impl AsRef<Path>,
        journal: &super::journal::Journal,
    ) -> Result<Self> {
        let verified = journal.load(true).await?;
        let path = path.as_ref().to_path_buf();
        super::scratch::check(&path)?;
        let admin = Admin::open(&path, Limits::default())?;
        let probe = Self {
            admin,
            state: Materialized::default(),
            path: path.clone(),
        };
        probe.validate_schema()?;
        let anchor = probe.read_collection("journal_applied", 1)?;
        let seq = match anchor.as_slice() {
            [SqlRow { values, .. }] => match values.as_slice() {
                [V::Int(0), V::Int(seq), V::Text(_)] if *seq >= 0 => *seq as u64,
                _ => return Err(Error::RecoveryRequired("invalid local anchor".into())),
            },
            _ => return Err(Error::RecoveryRequired("invalid local anchor count".into())),
        };
        drop(probe);
        if seq > verified.applied_seq {
            return Err(Error::RecoveryRequired(
                "local store is ahead of journal".into(),
            ));
        }
        if seq == verified.applied_seq {
            return Self::open(path, &verified);
        }
        let mut prefix = Materialized::default();
        while prefix.applied_seq < seq {
            let page = journal
                .history_page_through(prefix.applied_seq, 128, seq)
                .await?;
            for entry in page {
                journal.apply_entry(&mut prefix, &entry).await?;
            }
        }
        let mut local = Self::open(path, &prefix)?;
        local.sync_verified(&verified, journal).await?;
        Ok(local)
    }

    /// Conservative encoded envelope estimate, including 128 KiB catalog reserve.
    pub fn estimate(state: &Materialized) -> Result<usize> {
        super::budget::state(state)?;
        Ok(128 * 1024
            + projection(state)?
                .values()
                .flatten()
                .map(|r| row_bytes(r))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .sum::<usize>())
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
        let next = self.state.projected(entry)?;
        self.replace_state(next, commit)
    }

    fn replace_state(&mut self, next: Materialized, commit: bool) -> Result<()> {
        super::budget::state(&next)?;
        let before = projection(&self.state)?;
        let after = projection(&next)?;
        let keyed = |tables: &std::collections::BTreeMap<&'static str, Vec<Vec<V>>>| {
            tables
                .iter()
                .map(|(name, rows)| {
                    (
                        *name,
                        rows.iter()
                            .map(|row| {
                                (
                                    giggles_core::relational::encode_key(&row[..1]).expect("key"),
                                    row.clone(),
                                )
                            })
                            .collect::<std::collections::BTreeMap<_, _>>(),
                    )
                })
                .collect::<std::collections::BTreeMap<_, _>>()
        };
        let before_keys = keyed(&before);
        let after_keys = keyed(&after);
        let mut s = self.admin.sql_session();
        execute(&mut s, "BEGIN", &[])?;
        let result = (|| {
            // Delete changed rows before insertions so replacement never needs
            // space for two copies of a definition or privilege snapshot.
            for (table, rows) in &before {
                for row in rows {
                    if after_keys[table]
                        .get(&giggles_core::relational::encode_key(&row[..1]).expect("key"))
                        != Some(row)
                    {
                        let key = TABLES
                            .iter()
                            .find(|(name, _)| name == table)
                            .expect("table")
                            .1[0]
                            .0;
                        execute(
                            &mut s,
                            &format!("DELETE FROM {table} WHERE {key}=$1"),
                            &row[..1],
                        )?;
                    }
                }
            }
            for (table, rows) in &after {
                for row in rows {
                    if before_keys[table]
                        .get(&giggles_core::relational::encode_key(&row[..1]).expect("key"))
                        != Some(row)
                    {
                        let params = (1..=row.len())
                            .map(|n| format!("${n}"))
                            .collect::<Vec<_>>()
                            .join(",");
                        execute(
                            &mut s,
                            &format!("INSERT INTO {table} VALUES ({params})"),
                            row,
                        )?;
                    }
                }
            }
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

    /// Compatibility entry point for an already verified current snapshot.
    /// Missing intermediate anchors must be obtained through replay_journal.
    pub fn replay(&mut self, journal: &Materialized) -> Result<()> {
        if self.state.applied_seq == journal.applied_seq {
            return if self.state == *journal {
                Ok(())
            } else {
                Err(Error::RecoveryRequired(
                    "local replay anchor differs from verified journal".into(),
                ))
            };
        }
        Err(Error::RecoveryRequired(
            "replay requires paged journal entries".into(),
        ))
    }

    /// Synchronize a snapshot whose authority and chain were verified by Journal.
    pub(crate) async fn sync_verified(
        &mut self,
        state: &Materialized,
        journal: &super::journal::Journal,
    ) -> Result<()> {
        if self.state.applied_seq > state.applied_seq {
            return Err(Error::RecoveryRequired(
                "local store is ahead of journal".into(),
            ));
        }
        if self.state.applied_seq == state.applied_seq {
            return self.replay(state);
        }
        if self.state.applied_seq > 0
            && journal.entry(self.state.applied_seq).await?.digest != self.state.digest
        {
            return Err(Error::RecoveryRequired(
                "local replay anchor differs from journal".into(),
            ));
        }
        self.replace_state(state.clone(), true)
    }

    pub async fn replay_journal(&mut self, journal: &super::journal::Journal) -> Result<()> {
        let verified = journal.load(true).await?;
        if self.state.applied_seq > verified.applied_seq {
            return Err(Error::RecoveryRequired(
                "local store is ahead of journal".into(),
            ));
        }
        if self.state.applied_seq > 0 {
            let anchor = journal.entry(self.state.applied_seq).await?;
            if anchor.digest != self.state.digest {
                return Err(Error::RecoveryRequired(
                    "local replay anchor differs from journal".into(),
                ));
            }
        }
        while self.state.applied_seq < verified.applied_seq {
            let page = journal
                .history_page_through(self.state.applied_seq, 128, verified.applied_seq)
                .await?;
            for entry in page {
                let mut next = self.state.clone();
                journal.apply_entry(&mut next, &entry).await?;
                self.replace_state(next, true)?;
            }
        }
        if self.state != verified {
            return Err(Error::RecoveryRequired(
                "replayed state differs from verified journal".into(),
            ));
        }
        Ok(())
    }

    pub fn rebuild(path: impl AsRef<Path>, journal: &Materialized) -> Result<Self> {
        let mut local = Self::create(path)?;
        local.replace_state(journal.clone(), true)?;
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

    fn validate_rows(&self, _validate_audit: bool) -> Result<()> {
        for (name, expected) in projection(&self.state)? {
            let largest = expected
                .iter()
                .map(|row| row_bytes(row))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .max()
                .unwrap_or(1);
            let page_size = (512 * 1024 / largest).clamp(1, 128);
            let actual = self
                .read_collection(name, page_size)?
                .into_iter()
                .map(|r| r.values)
                .collect::<Vec<_>>();
            if actual != expected {
                return Err(Error::RecoveryRequired(format!(
                    "local {name} differs from journal"
                )));
            }
        }
        Ok(())
    }
}

/// D2's exact fixed-int row-map accounting, plus a conservative 128 KiB
/// reserve for catalog/envelope metadata (measured nine-table catalog < 4 KiB).
fn row_bytes(values: &[V]) -> Result<usize> {
    Ok(24
        + giggles_core::relational::encode_key(&values[..1])
            .map_err(giggles_core::CoreError::Sql)?
            .len()
        + values
            .iter()
            .map(|v| match v {
                V::Text(s) => 12 + s.len(),
                V::Bytes(b) => 12 + b.len(),
                V::Int(_) => 12,
                V::Bool(_) => 5,
                V::Null => 4,
            })
            .sum::<usize>())
}

fn fragments(value: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut rest = value;
    while !rest.is_empty() {
        let mut end = rest.len().min(24 * 1024);
        while !rest.is_char_boundary(end) {
            end -= 1;
        }
        out.push(&rest[..end]);
        rest = &rest[end..];
    }
    if out.is_empty() {
        out.push("");
    }
    out
}

pub(crate) fn projection(
    state: &Materialized,
) -> Result<std::collections::BTreeMap<&'static str, Vec<Vec<V>>>> {
    use super::budget::{self, Budget};
    let mut rows = TABLES
        .iter()
        .map(|(name, _)| (*name, Vec::new()))
        .collect::<std::collections::BTreeMap<_, _>>();
    let lc = &state.lifecycle;
    rows.get_mut("lifecycle").unwrap().push(vec![
        V::Int(0),
        label(&lc.state)?,
        num(lc.since_seq)?,
        label(&lc.authority)?,
        json(&lc.blockers)?,
    ]);
    rows.get_mut("journal_applied").unwrap().push(vec![
        V::Int(0),
        num(state.applied_seq)?,
        text(&state.digest),
    ]);
    if let Some(d) = &state.definition {
        let encoded = serde_json::to_string(d)?;
        let digest = d.canonical_digest()?;
        for (part, fragment) in fragments(&encoded).into_iter().enumerate() {
            rows.get_mut("definition").unwrap().push(vec![
                num(part as u64)?,
                text(&digest),
                text(fragment),
            ]);
        }
    }
    for (id, (marker, position)) in &state.progress {
        rows.get_mut("progress")
            .unwrap()
            .push(vec![text(id), text(marker), text(position)]);
    }
    for (id, c) in &state.chunks {
        rows.get_mut("chunks").unwrap().push(vec![
            text(id),
            text(&c.table),
            text(&c.range_lo),
            text(&c.range_hi),
            text(&c.state),
            optional(&c.low_wm),
            optional(&c.high_wm),
        ]);
    }
    for (id, value) in &state.verifier_ranges {
        rows.get_mut("verifier_ranges")
            .unwrap()
            .push(vec![text(id), json(value)?]);
    }
    for (id, value) in &state.verifier_exceptions {
        rows.get_mut("verifier_exceptions")
            .unwrap()
            .push(vec![text(id), json(value)?]);
    }
    for (id, (db, role, saved)) in &state.fence_state {
        for (part, fragment) in fragments(saved).into_iter().enumerate() {
            rows.get_mut("fence_state").unwrap().push(vec![
                json(&(id, part))?,
                text(db),
                text(role),
                text(fragment),
            ]);
        }
    }
    for (seq, entry) in &state.audit {
        let active = state.intent_roots.contains_key(seq)
            || state.intent_roots.values().any(|root| root == seq);
        rows.get_mut("audit").unwrap().push(vec![
            num(*seq)?,
            text(if active {
                "active-intent"
            } else {
                "replay-anchor"
            }),
            if active {
                json(entry)?
            } else {
                json(&(entry.epoch, &entry.coordinator_instance, &entry.digest))?
            },
        ]);
    }
    let mut total = 128 * 1024;
    for (table, entries) in &mut rows {
        let mut bytes = 0;
        for row in entries.iter() {
            let charged = row
                .iter()
                .map(|v| match v {
                    V::Text(s) => s.len(),
                    V::Bytes(b) => b.len(),
                    _ => 9,
                })
                .sum();
            budget::check(Budget::Row, charged, budget::ROW_BYTES)?;
            bytes += row_bytes(row)?;
        }
        // Core orders by canonical primary key, which is also the paging cursor.
        entries.sort_by_cached_key(|row| {
            giggles_core::relational::encode_key(&row[..1]).expect("valid primary key")
        });
        match *table {
            "chunks" => budget::check(Budget::Chunks, bytes, budget::CHUNK_BYTES)?,
            "verifier_ranges" => {
                budget::check(Budget::VerifierRanges, bytes, budget::VERIFIER_BYTES)?
            }
            "verifier_exceptions" => {
                budget::check(Budget::VerifierExceptions, bytes, budget::EXCEPTION_BYTES)?
            }
            "progress" => budget::check(Budget::Progress, bytes, budget::PROGRESS_BYTES)?,
            _ => {}
        }
        total += bytes;
    }
    budget::check(Budget::Envelope, total, budget::ENVELOPE_BYTES)?;
    Ok(rows)
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
            if local.state != recovery.state {
                local.replace_state(recovery.state, true)?;
            }
            return Ok(local);
        }
        Self::rebuild(path, &recovery.state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn projection_sql_error_rolls_back_deletions_and_anchor() {
        let root = PathBuf::from(std::env::var_os("TEMP").expect("set task TEMP on J:"))
            .join(format!("local-atomic-{}", uuid::Uuid::new_v4()));
        assert!(root.starts_with("J:/gigglesdb-work"));
        std::fs::create_dir_all(&root).unwrap();
        let mut local = Local::create(root.join("db")).unwrap();
        let owner = Entry::new(
            local.state(),
            super::super::Transition::AcquireOwnership { epoch: 1 },
            "writer",
        )
        .unwrap();
        local.apply(&owner).unwrap();
        // Inject a real SQL primary-key conflict. The projection transaction
        // first deletes its old anchor/audit rows, then this insertion fails.
        local
            .admin
            .sql_session()
            .execute(
                "INSERT INTO audit VALUES (2,'fixture','conflicting row')",
                &[],
                None,
            )
            .unwrap();
        let state = local.state().clone();
        let audit = local.read_collection("audit", 2).unwrap();
        let anchor = local.read_collection("journal_applied", 1).unwrap();
        let entry = Entry::new(
            &state,
            super::super::Transition::Progress {
                stream: "s".into(),
                marker: "m".into(),
                position: "p".into(),
            },
            "writer",
        )
        .unwrap();
        assert!(matches!(local.apply(&entry), Err(Error::Core(_))));
        assert_eq!(local.state(), &state);
        assert_eq!(local.read_collection("audit", 2).unwrap(), audit);
        assert_eq!(local.read_collection("journal_applied", 1).unwrap(), anchor);
        assert!(local.read_collection("progress", 1).unwrap().is_empty());
    }
}
