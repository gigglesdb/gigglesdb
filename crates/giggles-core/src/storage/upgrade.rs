//! Explicit offline conversion; all payloads use existing v2 codecs.
use super::*;
use std::path::PathBuf;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UpgradePoint {
    None,
    ExportWritten,
    ExportVerified,
    V2Built,
    Verified,
    FirstRename,
    SecondRename,
}

#[derive(Debug)]
pub struct UpgradeReport {
    pub export: PathBuf,
    pub legacy: PathBuf,
}

fn sibling(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(suffix);
    path.with_file_name(name)
}

pub(super) fn check_startup(path: &Path) -> Result<(), CoreError> {
    // Never guess which copy an interrupted operator intended to publish. A tmp
    // sibling always blocks opening; a missing path with a retained v1 sibling
    // also blocks. Explicit recovery resolves these states before reopening.
    let tmp = sibling(path, ".v2-upgrade-tmp");
    let retained = if !path.exists() {
        let prefix = format!(
            "{}.v1-pre-upgrade-",
            path.file_name().unwrap_or_default().to_string_lossy()
        );
        match fs::read_dir(
            path.parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new(".")),
        ) {
            Ok(entries) => entries
                .flatten()
                .any(|e| e.file_name().to_string_lossy().starts_with(&prefix)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
            Err(e) => return Err(io(e)),
        }
    } else {
        false
    };
    if tmp.exists() || retained {
        return Err(CoreError::Storage(format!(
            "upgrade in progress at {}; run giggles upgrade --recover {}",
            path.display(),
            path.display()
        )));
    }
    Ok(())
}

impl Admin {
    /// Resolve an interrupted offline upgrade without discarding its v1 or export backup.
    /// Each mutation leaves a state that this same operation can resolve again.
    pub fn recover_upgrade(path: impl AsRef<Path>) -> Result<&'static str, CoreError> {
        use engine::meta::{DirClass, classify_dir};
        let path = path.as_ref();
        let tmp = sibling(path, ".v2-upgrade-tmp");
        if !path.exists() {
            let prefix = sibling(path, ".v1-pre-upgrade-");
            let prefix = prefix.file_name().unwrap().to_string_lossy();
            let parent = path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            let mut retained = Vec::new();
            match fs::read_dir(parent) {
                Ok(entries) => {
                    for entry in entries {
                        let entry = entry.map_err(io)?;
                        if entry
                            .file_name()
                            .to_string_lossy()
                            .starts_with(prefix.as_ref())
                        {
                            retained.push(entry.path());
                        }
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(io(e)),
            }
            if retained.len() > 1 {
                return Err(io(
                    "upgrade recovery refuses multiple v1-pre-upgrade siblings",
                ));
            }
            if let Some(legacy) = retained.first() {
                if !tmp.is_dir()
                    || classify_dir(legacy).map_err(v2::format_error)? != DirClass::Legacy
                {
                    return Err(io(
                        "upgrade recovery requires a legacy retained copy and tmp directory",
                    ));
                }
                // Roll forward would require re-verification. Q6/Q5 favours restoring
                // the known v1 copy; rename first so interrupted cleanup can retry.
                fs::rename(legacy, path).map_err(io)?;
                qualification_crash("RecoveryRename");
            } else if tmp.exists() {
                return Err(io("upgrade recovery cannot find the original v1 database"));
            } else {
                return Ok("no upgrade in progress");
            }
        }
        if !tmp.exists() {
            return Ok("no upgrade in progress");
        }
        let outcome = match classify_dir(path).map_err(v2::format_error)? {
            DirClass::Legacy => "rolled back",
            DirClass::V2 => "completed",
            DirClass::Empty => return Err(io("upgrade recovery refuses an empty active database")),
        };
        fs::remove_dir_all(&tmp).map_err(io)?;
        qualification_crash("RecoveryDelete");
        Ok(outcome)
    }

    pub fn upgrade_to_v2(
        path: impl AsRef<Path>,
        limits: Limits,
    ) -> Result<UpgradeReport, CoreError> {
        Self::upgrade_to_v2_with_fault(path, limits, UpgradePoint::None)
    }

    pub fn upgrade_to_v2_with_fault(
        path: impl AsRef<Path>,
        limits: Limits,
        point: UpgradePoint,
    ) -> Result<UpgradeReport, CoreError> {
        Self::upgrade_to_v2_with_hook(path, limits, |at| {
            if at == point {
                Err(io(format!("injected {at:?}")))
            } else {
                Ok(())
            }
        })
    }

    /// Trusted boundary hook, also usable by abrupt-crash qualification children.
    pub fn upgrade_to_v2_with_hook(
        path: impl AsRef<Path>,
        limits: Limits,
        mut hook: impl FnMut(UpgradePoint) -> Result<(), CoreError>,
    ) -> Result<UpgradeReport, CoreError> {
        #[cfg(not(feature = "engine-rocksdb"))]
        {
            let _ = (path, limits, &mut hook);
            Err(CoreError::EngineUnsupported {
                engine: "v2",
                feature: ROCKSDB_NOT_COMPILED,
            })
        }
        #[cfg(feature = "engine-rocksdb")]
        {
            run(path.as_ref(), limits, &mut hook)
        }
    }
}

// Explicit opt-in qualification seam. Termination uses the existing
// no-destructor crash primitive; ordinary builds never read the environment.
#[cfg(feature = "upgrade-fault-injection")]
fn qualification_crash(point: &str) {
    if std::env::var("GD_UPGRADE_CRASH").as_deref() == Ok(point) {
        crash_now();
    }
}

#[cfg(not(feature = "upgrade-fault-injection"))]
fn qualification_crash(_point: &str) {}

#[cfg(feature = "engine-rocksdb")]
fn refuse(s: &State) -> Result<(), CoreError> {
    let unsupported = [
        ("tasks", s.view.tasks.len()),
        (
            "receipts",
            s.view
                .operations
                .values()
                .filter(|v| v.decision.is_ok())
                .count(),
        ),
        ("approvals", s.view.approvals.len()),
        ("executor ledger", s.accounting.len()),
        ("kernel records", s.view.rows.len()),
        (
            "once/outcome ledger",
            usize::from(s.native_outcomes.is_some()),
        ),
        ("application ledger", usize::from(s.applications.is_some())),
        ("protected operations", s.view.operations.len()),
        ("native schema counter", usize::from(s.view.schema != 0)),
        ("authority clock", usize::from(s.last_time != 0)),
        ("approval counter", usize::from(s.next_approval != 0)),
    ];
    let found = unsupported
        .into_iter()
        .filter(|(_, n)| *n != 0)
        .map(|(kind, count)| format!("{kind}: {count}"))
        .collect::<Vec<_>>();
    if !found.is_empty() {
        return Err(io(format!("upgrade cannot represent {}", found.join(", "))));
    }
    Ok(())
}

#[cfg(feature = "engine-rocksdb")]
fn run(
    path: &Path,
    limits: Limits,
    hook: &mut impl FnMut(UpgradePoint) -> Result<(), CoreError>,
) -> Result<UpgradeReport, CoreError> {
    use std::time::{SystemTime, UNIX_EPOCH};
    check_startup(path)?;
    if engine::meta::classify_dir(path).map_err(v2::format_error)? != engine::meta::DirClass::Legacy
    {
        return Err(io("upgrade requires a legacy v1 database"));
    }
    // redb's exclusive file lock is held through verification. Windows requires
    // closing it before rename; the tmp sibling gates all startups during switch.
    let legacy = Admin::open_legacy_unaccepted(path, limits)?;
    let state = legacy.inner.read()?;
    refuse(&state)?;
    let roles = legacy.roles().upgrade_records()?;
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(io)?
        .as_nanos();
    let report = UpgradeReport {
        export: sibling(path, &format!(".pre-v2-{stamp}.gdexp")),
        legacy: sibling(path, &format!(".v1-pre-upgrade-{stamp}")),
    };
    let tmp = sibling(path, ".v2-upgrade-tmp");
    fs::create_dir(&tmp).map_err(io)?;
    let (mut first, mut second) = (false, false);
    let result = (|| {
        legacy.export(&report.export)?;
        hook(UpgradePoint::ExportWritten)?;
        Admin::verify_export(&report.export)?;
        hook(UpgradePoint::ExportVerified)?;
        build(&tmp, &state, &roles)?;
        hook(UpgradePoint::V2Built)?;
        verify(&tmp, &state, &roles, limits)?;
        hook(UpgradePoint::Verified)?;
        drop(legacy);
        fs::rename(path, &report.legacy).map_err(io)?;
        first = true;
        hook(UpgradePoint::FirstRename)?;
        fs::rename(&tmp, path).map_err(io)?;
        second = true;
        hook(UpgradePoint::SecondRename)
    })();
    if let Err(error) = result {
        // Rollback errors retain all evidence and keep startup fail-closed.
        if second {
            fs::rename(path, &tmp).map_err(io)?;
        }
        if first {
            fs::rename(&report.legacy, path).map_err(io)?;
        }
        fs::remove_dir_all(&tmp).map_err(io)?;
        return Err(error);
    }
    Ok(report)
}

#[cfg(feature = "engine-rocksdb")]
fn legacy_rows(
    s: &State,
    table: &relational::TableDef,
) -> Result<BTreeMap<Vec<u8>, relational::SqlRow>, CoreError> {
    use relational::scan::{Direction, KeyRange, LegacyRows, RowSource};
    let range = KeyRange {
        lo: std::ops::Bound::Unbounded,
        hi: std::ops::Bound::Unbounded,
    };
    LegacyRows(&s.sql)
        .scan(table, &range, Direction::Forward)
        .map_err(CoreError::Sql)?
        .map(|row| {
            let row = row.map_err(CoreError::Sql)?.row.into_owned();
            let pk = table
                .primary_key
                .iter()
                .map(|i| row.values[*i].clone())
                .collect::<Vec<_>>();
            Ok((engine::codec::pk_bytes(&pk)?, row))
        })
        .collect()
}

#[cfg(feature = "engine-rocksdb")]
fn parts(s: &State) -> engine::catalog::CatalogParts {
    let (tables, ext, _, _) = s.sql.catalog_parts();
    let heap = tables
        .values()
        .filter(|t| t.hidden_rowid().is_some())
        .map(|t| t.id)
        .collect();
    engine::catalog::CatalogParts {
        tables,
        ext,
        heap,
        indexes: s.sql.secondary_descriptors_engine().to_vec(),
        sql_views: BTreeMap::new(), comments: BTreeMap::new(),
    }
}

#[cfg(feature = "engine-rocksdb")]
fn build(path: &Path, s: &State, roles: &BTreeMap<Vec<u8>, Vec<u8>>) -> Result<(), CoreError> {
    use engine::{
        codec,
        hlc::Ts,
        kv::{Durability, Keyspace, Kv, WriteBatch},
        mvcc,
    };
    let kv = Arc::new(Kv::new(Arc::new(engine::kv_rocksdb::RocksKv::open(
        path,
        64 * 1024 * 1024,
    )?)));
    let mut batch = WriteBatch::default();
    let mut bits = feature_envelope::feature_bits_present(s)?;
    let put = |batch: &mut WriteBatch, ks, key: Vec<u8>, value: Vec<u8>| {
        batch.put(
            ks,
            codec::user_key_seek_at(&key, Ts(1)),
            mvcc::encode_live(&value),
        );
    };
    for (key, value) in engine::catalog::parts_records(&parts(s))?
        .into_iter()
        .chain(roles.clone())
    {
        bits |=
            engine::content_check::required_bits_for_catalog_key(&key).map_err(v2::format_error)?;
        bits |= engine::content_check::required_bits_for_catalog_value(&key, &value)
            .map_err(v2::format_error)?;
        put(&mut batch, Keyspace::Catalog, key, value);
    }
    for table in s.sql.tables.values() {
        let rows = legacy_rows(s, table)?;
        for (pk, row) in &rows {
            let key = [table.id.to_be_bytes().as_slice(), pk].concat();
            let value = codec::encode_row(&key, row.version, &row.values)?;
            put(&mut batch, Keyspace::Rows, key, value);
        }
        for (key, value) in
            engine::indexes::entries(table, &rows, s.sql.secondary_descriptors_engine())?
        {
            put(&mut batch, Keyspace::Index, key, value);
        }
        kv.write(std::mem::take(&mut batch), Durability::Sync)?;
        qualification_crash("BuildBatch");
    }
    for (&id, counter) in &s.seq {
        let def = &s.sql.catalog_ext().sequences[&id];
        batch.put(
            Keyspace::Seq,
            engine::catalog_codec::counter_key(id, def.incarnation),
            engine::catalog_codec::encode_counter(counter, def)?,
        );
    }
    for (key, value) in [
        (b"family".as_slice(), 2u32.to_be_bytes().to_vec()),
        (
            b"subversion",
            engine::meta::INITIAL_SUBVERSION.to_be_bytes().to_vec(),
        ),
        (b"domain", s.view.domain.to_be_bytes().to_vec()),
        (b"engine", b"rocksdb".to_vec()),
        (b"last_commit_ts", 1u64.to_be_bytes().to_vec()),
    ] {
        batch.put(Keyspace::Meta, key.to_vec(), value);
    }
    if s.origin.is_some() {
        // Reuse the kernel's existing namespace-0 metadata codec. No SQL or
        // otherwise refused native records belong in this image.
        let mut kernel = empty(s.view.domain);
        kernel.origin = s.origin;
        let metadata = records(&kernel)?.remove(&vec![0]).expect("metadata record");
        put(&mut batch, Keyspace::Kernel, vec![0], metadata);
    }
    engine::meta::put_features(&mut batch, bits).map_err(v2::format_error)?;
    kv.write(batch, Durability::Sync)?;
    let allocator = engine::seq::SequenceAllocator::new(kv.clone(), 0);
    let (_, _, next_id, next_version) = s.sql.catalog_parts();
    allocator.ensure_at_least(0, 0, next_id)?;
    allocator.ensure_at_least(3, 0, next_version)?;
    // Legacy heaps allocate row IDs from the global version counter, including
    // deleted rows. Live maxima alone would allow reuse after an upgrade.
    for table in s.sql.tables.values().filter(|t| t.hidden_rowid().is_some()) {
        allocator.ensure_at_least(1, table.id, next_version)?;
    }
    engine::meta::open_checks(kv.as_ref()).map_err(v2::format_error)?;
    let mut marker = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path.join("GIGGLES_FORMAT.tmp"))
        .map_err(io)?;
    marker.write_all(b"giggles-v2\n").map_err(io)?;
    marker.sync_all().map_err(io)?;
    drop(marker);
    qualification_crash("BeforeMarkerRename");
    fs::rename(path.join("GIGGLES_FORMAT.tmp"), path.join("GIGGLES_FORMAT")).map_err(io)?;
    Ok(())
}

#[cfg(feature = "engine-rocksdb")]
fn verify(
    path: &Path,
    s: &State,
    roles: &BTreeMap<Vec<u8>, Vec<u8>>,
    limits: Limits,
) -> Result<(), CoreError> {
    use engine::{
        kv::{Direction, Keyspace},
        txn::Isolation,
    };
    use std::ops::Bound::Unbounded;
    let admin = Admin::open(path, limits)?;
    let Backend::V2(db) = &admin.inner.backend else {
        return Err(io("upgrade verification engine"));
    };
    let mut expected = relational::Relational::catalog_image(
        s.sql.tables.clone(),
        s.sql.catalog_ext().clone(),
        u64::MAX,
        u64::MAX,
    )
    .map_err(CoreError::Sql)?;
    expected
        .catalog_indexes(s.sql.secondary_descriptors_engine())
        .map_err(CoreError::Sql)?;
    expected.epoch = 1;
    let metadata = db.metadata_state(None)?;
    if db.meta.domain != s.view.domain.to_be_bytes()
        || metadata.origin != s.origin
        || metadata.sql != expected
        || &admin.roles().upgrade_records()? != roles
    {
        return Err(io(
            "upgrade verification catalog/domain/origin/roles mismatch",
        ));
    }
    for (&id, counter) in &s.seq {
        let def = &s.sql.catalog_ext().sequences[&id];
        if db.kv.get(
            Keyspace::Seq,
            &engine::catalog_codec::counter_key(id, def.incarnation),
        )? != Some(engine::catalog_codec::encode_counter(counter, def)?)
        {
            return Err(io("upgrade verification sequence mismatch"));
        }
    }
    let (_, _, next_id, next_version) = s.sql.catalog_parts();
    for (kind, id, floor) in [(0, 0, next_id), (3, 0, next_version)].into_iter().chain(
        s.sql
            .tables
            .values()
            .filter(|t| t.hidden_rowid().is_some())
            .map(|t| (1, t.id, next_version)),
    ) {
        let key = [vec![kind], id.to_be_bytes().to_vec()].concat();
        let saved = db
            .kv
            .get(Keyspace::Seq, &key)?
            .unwrap_or_else(|| 1u64.to_be_bytes().to_vec());
        if saved != floor.to_be_bytes() {
            return Err(io("upgrade verification allocator mismatch"));
        }
    }
    let mut txn = db.begin(Isolation::ReadCommitted);
    for table in s.sql.tables.values() {
        let entry = db
            .catalog()
            .resolve(
                &mut txn,
                &s.sql.table_key(table),
                engine::locks::LockMode::AccessShare,
                None,
                &|| false,
            )
            .map_err(|e| match e {
                engine::catalog::CatalogError::Core(e) => e,
                engine::catalog::CatalogError::Txn(e) => e.into(),
            })?;
        let expected = legacy_rows(s, table)?;
        let scanned = db
            .rows
            .scan(
                &mut txn,
                &entry,
                Unbounded,
                Unbounded,
                Direction::Forward,
                None,
            )?
            .map(|pair| {
                pair.map(|(pk, row)| {
                    (
                        pk,
                        relational::SqlRow {
                            values: row.values,
                            version: row.version,
                        },
                    )
                })
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        if digest(&expected)? != digest(&scanned)? {
            return Err(io("upgrade verification row digest mismatch"));
        }
        for index in s
            .sql
            .secondary_descriptors_engine()
            .iter()
            .filter(|d| d.table == table.id)
        {
            let rows = engine::indexes::scan(&mut txn, &entry, index, Unbounded, Unbounded)?;
            let count = rows.len();
            let rows = rows
                .into_iter()
                .map(|(pk, row)| {
                    (
                        pk,
                        relational::SqlRow {
                            values: row.values,
                            version: row.version,
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>();
            if count != expected.len() || digest(&rows)? != digest(&scanned)? {
                return Err(io("upgrade verification index scan mismatch"));
            }
        }
    }
    Ok(())
}

#[cfg(feature = "engine-rocksdb")]
fn digest(rows: &BTreeMap<Vec<u8>, relational::SqlRow>) -> Result<[u8; 32], CoreError> {
    Ok(Sha256::digest(encode(rows)?).into())
}
