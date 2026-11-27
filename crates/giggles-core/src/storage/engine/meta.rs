//! Fail-closed v2 directory and unversioned metadata gate.
use super::kv::{
    COMMIT_GROUP_KEY, Direction, Durability, Keyspace, KvBackend, KvError, KvRead, WriteBatch,
};
use std::{
    fs,
    io::{Read, Write},
    ops::Bound,
    path::Path,
    sync::Arc,
};

// Reader capability is separate from the version written by receipt-free stores.
pub(crate) const INITIAL_SUBVERSION: u32 = 1;
pub(crate) const CURRENT_SUBVERSION: u32 = 2;
pub(crate) const NATIVE_SUBVERSION: u32 = 2;
pub(crate) const NATIVE_OUTCOMES: u64 = 1 << 1;
pub(crate) const SECONDARY_INDEXES: u64 = 1 << 2;
pub(crate) const TEXT_BACKED_COLUMN_TYPES: u64 = 1 << 10;
pub(crate) const SCHEMAS_AND_USER_TYPES: u64 = 1 << 12;
pub(crate) const UNIQUE_CONSTRAINT_IDENTITY: u64 = 1 << 13;
pub(crate) const SQL_VIEWS: u64 = 1 << 14;
pub(crate) const RELATION_COMMENTS: u64 = 1 << 15;
pub(crate) const REPLICATED: u64 = 1 << 16;
pub(crate) const KEY_DEPENDENCY_IDENTITY: u64 = 1 << 17;
/// Range-local Meta keys (`range/<id BE8>/<sub>`); requires REPLICATED.
pub(crate) const RANGES: u64 = 1 << 18;
/// The native once ledger holds a GDOU version 2 (u32 count) receipt.
pub(crate) const WIDE_ONCE_RECEIPTS: u64 = 1 << 19;
/// Task definitions (catalog kind 19) and the task-only role attribute (GD-0061).
pub(crate) const TASK_CATALOG: u64 = 1 << 20;
/// Kernel namespaces 0x20..=0x2B hold protected task records (GD-0061 R3-600).
pub(crate) const PROTECTED_RECORDS: u64 = 1 << 21;
/// A persisted embedded write-fence record is present.
pub(crate) const WRITE_FENCE: u64 = 1 << 22;
/// Feature bits whose databases export and restore refuse (GD-0061 D8, R3-111).
pub(crate) const EXPORT_REFUSED: u64 = TASK_CATALOG | PROTECTED_RECORDS;
/// V2 bits differ from legacy format 53: bit 22 belongs to WRITE_FENCE.
pub(crate) const USER_COLLATIONS: u64 = 1 << 23;
pub(crate) const COLUMN_COLLATIONS: u64 = 1 << 24;
pub(crate) const BUILTIN_EXTENSIONS: u64 = 1 << 25;
pub(crate) const ROLE_MEMBERSHIP: u64 = 1 << 27;
pub(crate) const WIDE_ROWS: u64 = 1 << 28;
pub(crate) const ENUM_ARRAY_COLUMNS: u64 = 1 << 29;
pub(crate) const ENUM_LABEL_IDENTITIES: u64 = 1 << 30;
pub(crate) const UUID_ARRAY_COLUMNS: u64 = 1 << 32;
/// A committed catalog crossed a count accepted by older readers.
pub(crate) const WIDE_CATALOG: u64 = 1 << 31;
pub(crate) const PARTIAL_INDEXES: u64 = 1 << 33;
pub(crate) const TEMPORAL_INDEXES: u64 = 1 << 34;
pub(crate) const GENERATED_COLUMNS: u64 = 1 << 36;
pub(crate) const TEXT_SEARCH_TYPES: u64 = 1 << 37;
pub(crate) const GIN_INDEXES: u64 = 1 << 38;
pub(crate) const USER_ROUTINES: u64 = 1 << 39;
pub(crate) const STORAGE_METADATA: u64 = 1 << 40;
pub(crate) const KEY_TYPES: u64 = 1 << 41;
pub(crate) const FEATURE_TABLE: [(u64, &str); 40] = [
    (1, "typed_values"),
    (2, "native_outcomes"),
    (4, "secondary_indexes"),
    (8, "applications"),
    (16, "temporal"),
    (32, "heap"),
    (64, "catalog_ext"),
    (128, "extended_types"),
    (256, "sequence_counters"),
    (512, "role_bootstrap"),
    (TEXT_BACKED_COLUMN_TYPES, "text_backed_column_types"),
    (2048, "role_catalog"),
    (SCHEMAS_AND_USER_TYPES, "schemas_and_user_types"),
    (UNIQUE_CONSTRAINT_IDENTITY, "unique_constraint_identity"),
    (SQL_VIEWS, "sql_views"),
    (RELATION_COMMENTS, "relation_comments"),
    (REPLICATED, "replicated"),
    (KEY_DEPENDENCY_IDENTITY, "key_dependency_identity"),
    (RANGES, "ranges"),
    (WIDE_ONCE_RECEIPTS, "wide_once_receipts"),
    (TASK_CATALOG, "task_catalog"),
    (PROTECTED_RECORDS, "protected_records"),
    (WRITE_FENCE, "write_fence"),
    (USER_COLLATIONS, "user_collations"),
    (COLUMN_COLLATIONS, "column_collations"),
    (BUILTIN_EXTENSIONS, "builtin_extensions"),
    (ROLE_MEMBERSHIP, "role_membership"),
    (WIDE_ROWS, "wide_rows"),
    (ENUM_ARRAY_COLUMNS, "enum_array_columns"),
    (ENUM_LABEL_IDENTITIES, "enum_label_identities"),
    (UUID_ARRAY_COLUMNS, "uuid_array_columns"),
    (WIDE_CATALOG, "wide_catalog"),
    (PARTIAL_INDEXES, "partial_indexes"),
    (TEMPORAL_INDEXES, "temporal_indexes"),
    (GENERATED_COLUMNS, "generated_columns"),
    (TEXT_SEARCH_TYPES, "text_search_types"),
    (GIN_INDEXES, "gin_indexes"),
    (USER_ROUTINES, "user_routines"),
    (STORAGE_METADATA, "storage_metadata"),
    (KEY_TYPES, "key_types"),
];
#[path = "meta_range.rs"]
mod range_local;
#[cfg_attr(not(feature = "engine-rocksdb"), allow(unused_imports))] // used by storage::range
pub(crate) use range_local::{
    RangeLocalKey, check_range_value, parse_range_local_key, range_local_key, range_local_prefix,
};
pub(in crate::storage) const MARKER: &str = "GIGGLES_FORMAT";
pub(in crate::storage) const TMP: &str = "GIGGLES_FORMAT.tmp";
const V2: &[u8] = b"giggles-v2\n";
#[cfg(feature = "engine-rocksdb")]
const V2_COMMIT_GROUPS: &[u8] = b"giggles-v2\ncommit-groups\n";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum StoreMarker {
    V2,
    #[cfg(feature = "engine-rocksdb")]
    CommitGroups,
}

pub(super) fn read_store_marker(dir: &Path) -> Result<Option<StoreMarker>, FormatError> {
    let file = match fs::File::open(dir.join(MARKER)) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(FormatError::io(error)),
    };
    if file.metadata().map_err(FormatError::io)?.len() > 64 {
        return Err(corrupt("oversized marker"));
    }
    let mut bytes = Vec::new();
    file.take(65).read_to_end(&mut bytes).map_err(FormatError::io)?;
    match bytes.as_slice() {
        V2 => return Ok(Some(StoreMarker::V2)),
        #[cfg(feature = "engine-rocksdb")]
        V2_COMMIT_GROUPS => return Ok(Some(StoreMarker::CommitGroups)),
        _ => {}
    }
    if let Some(n) = bytes
        .strip_prefix(b"giggles-v")
        .and_then(|s| s.strip_suffix(b"\n"))
        && n.first().is_some_and(|b| matches!(*b, b'1'..=b'9'))
        && n.iter().all(u8::is_ascii_digit)
        && n != b"2"
    {
        return Err(FormatError::Unsupported("marker version".into()));
    }
    Err(corrupt("invalid marker"))
}

#[cfg(feature = "engine-rocksdb")]
pub(super) fn write_commit_groups_marker(dir: &Path) -> Result<(), FormatError> {
    // Replace the marker atomically. Re-publish even on a retry, so success
    // includes the directory sync if a previous attempt failed there.
    let temp = dir.join(TMP);
    let removed = fs::symlink_metadata(&temp).and_then(|metadata| {
        let directory = metadata.is_dir();
        #[cfg(windows)]
        let directory = {
            use std::os::windows::fs::FileTypeExt;

            directory || metadata.file_type().is_symlink_dir()
        };
        if directory {
            fs::remove_dir(&temp)
        } else {
            fs::remove_file(&temp)
        }
    });
    match removed {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(FormatError::io(error)),
    }
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        unsafe extern "C" {
            fn giggles_marker_no_follow_flag() -> std::ffi::c_int;
        }
        // SAFETY: the shim returns the platform's O_NOFOLLOW constant.
        options.custom_flags(unsafe { giggles_marker_no_follow_flag() });
    }
    let mut file = options.open(&temp).map_err(FormatError::io)?;
    file.write_all(V2_COMMIT_GROUPS).map_err(FormatError::io)?;
    file.sync_all().map_err(FormatError::io)?;
    drop(file);
    fs::rename(&temp, dir.join(MARKER)).map_err(FormatError::io)?;
    sync_marker_directory(dir).map_err(FormatError::io)
}

#[cfg(all(feature = "engine-rocksdb", unix))]
fn sync_marker_directory(dir: &Path) -> std::io::Result<()> {
    fs::File::open(dir)?.sync_all()
}

#[cfg(all(feature = "engine-rocksdb", windows))]
fn sync_marker_directory(dir: &Path) -> std::io::Result<()> {
    use std::os::windows::fs::OpenOptionsExt;

    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    fs::OpenOptions::new()
        .write(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(dir)?
        .sync_all()
}

#[cfg(all(feature = "engine-rocksdb", not(any(unix, windows))))]
fn sync_marker_directory(_dir: &Path) -> std::io::Result<()> {
    Err(std::io::ErrorKind::Unsupported.into())
}
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum DirClass {
    Empty,
    Legacy,
    V2,
}
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum FormatError {
    Corrupt(String),
    Unsupported(String),
    Incomplete(String),
    WrappedIo { message: String, source: super::kv::KvError },
}
impl std::fmt::Display for FormatError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Corrupt(s) | Self::WrappedIo { message: s, .. } => write!(f, "corrupt format: {s}"),
            Self::Unsupported(s) => write!(f, "unsupported format: {s}"),
            Self::Incomplete(s) => write!(f, "incomplete format: {s}"),
        }
    }
}
impl std::error::Error for FormatError {}
fn corrupt(error: impl std::fmt::Display) -> FormatError {
    FormatError::Corrupt(error.to_string())
}
impl FormatError {
    pub(super) fn kv(error: super::kv::KvError) -> Self {
        let message = error.to_string();
        Self::wrap_io(message, error)
    }

    pub(super) fn txn(error: super::txn::TxnError) -> Self {
        let message = format!("{error:?}");
        match error {
            super::txn::TxnError::Kv(error) => Self::wrap_io(message, error),
            _ => Self::Corrupt(message),
        }
    }

    fn wrap_io(message: String, source: super::kv::KvError) -> Self {
        match source {
            super::kv::KvError::Io(_) | super::kv::KvError::RocksIo { .. } => {
                Self::WrappedIo { message, source }
            }
            _ => Self::Corrupt(message),
        }
    }

    fn io(error: std::io::Error) -> Self {
        let message = error.to_string();
        Self::WrappedIo { source: super::kv::KvError::Io(message.clone()), message }
    }
}
pub(crate) fn rocks_file(name: &str) -> bool {
    matches!(name, "CURRENT" | "LOCK" | "IDENTITY" | "LOG")
        || name.starts_with("MANIFEST-")
        || name.starts_with("OPTIONS-")
        || name.starts_with("LOG.old.")
        || name
            .strip_suffix(".sst")
            .or_else(|| name.strip_suffix(".log"))
            .or_else(|| name.strip_suffix(".dbtmp"))
            .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
}
pub(crate) fn classify_dir(dir: &Path) -> Result<DirClass, FormatError> {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(DirClass::Empty),
        Err(e) => return Err(FormatError::io(e)),
    };
    let (mut marker, mut legacy, mut current, mut rocks, mut unknown) =
        (false, false, false, false, false);
    for entry in entries {
        let entry = entry.map_err(FormatError::io)?;
        if !entry.file_type().map_err(FormatError::io)?.is_file() {
            unknown = true;
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            unknown = true;
            continue;
        };
        match name {
            MARKER => marker = true,
            "data.redb" => legacy = true,
            TMP => {}
            "giggles.lock" => {}
            n if rocks_file(n) => {
                rocks = true;
                current |= n == "CURRENT";
            }
            _ => unknown = true,
        }
    }
    if marker && legacy {
        return Err(corrupt("marker and data.redb coexist"));
    }
    if marker {
        if read_store_marker(dir)?.is_none() {
            return Err(corrupt("format marker disappeared"));
        }
        return if current {
            Ok(DirClass::V2)
        } else {
            Err(corrupt("marker without CURRENT"))
        };
    }
    if rocks {
        return Err(FormatError::Incomplete(
            "RocksDB files without marker".into(),
        ));
    }
    // A legacy directory may hold unrelated files (backups, copies); only a
    // directory with neither data.redb nor v2 files must be free of them.
    if legacy {
        Ok(DirClass::Legacy)
    } else if unknown {
        Err(corrupt("unrecognized directory contents"))
    } else {
        Ok(DirClass::Empty)
    }
}
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct MetaInfo {
    pub(crate) stale_counters: Vec<Vec<u8>>,
    pub(crate) family: u32,
    pub(crate) subversion: u32,
    pub(crate) features: u64,
    pub(crate) domain: [u8; 16],
    pub(crate) engine: String,
    pub(crate) last_commit_ts: Option<u64>,
    pub(crate) gc_watermark: Option<u64>,
    pub(crate) write_fence: Option<super::write_fence::WriteFenceRecord>,
}
fn fixed<const N: usize>(kv: &dyn KvRead, key: &[u8]) -> Result<[u8; N], FormatError> {
    kv.get(Keyspace::Meta, key)
        .map_err(FormatError::kv)?
        .ok_or_else(|| corrupt(format!("missing {}", String::from_utf8_lossy(key))))?
        .try_into()
        .map_err(|_| corrupt("wrong metadata length"))
}
fn optional(kv: &dyn KvRead, key: &[u8]) -> Result<Option<u64>, FormatError> {
    kv.get(Keyspace::Meta, key)
        .map_err(FormatError::kv)?
        .map(|v| {
            v.try_into()
                .map(u64::from_be_bytes)
                .map_err(|_| corrupt("wrong timestamp length"))
        })
        .transpose()
}
/// Live role-bootstrap (kind-7) catalog entries in the Catalog column family.
pub(crate) fn live_role_bootstrap_entries(kv: &dyn KvRead) -> Result<usize, FormatError> {
    // Descending timestamps make the first physical version the latest. Only
    // kind-7 keys are relevant, but malformed kind-7 envelopes fail closed.
    let mut previous = None;
    let mut live = 0usize;
    for item in kv
        .range(
            Keyspace::Catalog,
            Bound::Included(&[7]),
            Bound::Excluded(&[8]),
            Direction::Forward,
        )
        .map_err(FormatError::kv)?
    {
        let (key, value) = item.map_err(FormatError::kv)?;
        if key.len() < 9 {
            return Err(corrupt("short kind-7 versioned key"));
        }
        let user = &key[..key.len() - 8];
        if previous.as_deref() == Some(user) {
            continue;
        }
        previous = Some(user.to_vec());
        if super::mvcc::decode(&value).map_err(FormatError::kv)?.is_some() {
            live += 1;
        }
    }
    Ok(live)
}
pub(crate) fn validate_meta(kv: &dyn KvRead) -> Result<MetaInfo, FormatError> {
    // Features gate the key allowlist; a missing or malformed value gates
    // nothing and fails below exactly as before.
    let gate = kv
        .get(Keyspace::Meta, b"features")
        .map_err(FormatError::kv)?
        .and_then(|v| <[u8; 8]>::try_from(v.as_slice()).ok())
        .map_or(0, u64::from_be_bytes);
    if gate & RANGES != 0 {
        check_features(gate)?;
    }
    for item in kv
        .range(
            Keyspace::Meta,
            Bound::Unbounded,
            Bound::Unbounded,
            Direction::Forward,
        )
        .map_err(FormatError::kv)?
    {
        let (key, value) = item.map_err(FormatError::kv)?;
        if gate & RANGES != 0
            && let Some((id, sub)) = parse_range_local_key(&key)
        {
            range_local::check_range_value(id, &sub, &value, gate)?;
            continue;
        }
        if gate & REPLICATED == 0 && key == COMMIT_GROUP_KEY {
            if value.len() != size_of::<u64>() {
                return Err(corrupt("invalid commit_group"));
            }
            continue;
        }
        if !matches!(
            key.as_slice(),
            b"family"
                | b"subversion"
                | b"features"
                | b"domain"
                | b"engine"
                | b"last_commit_ts"
                | b"gc_watermark"
                | b"raft_applied"
                | b"write_fence"
        ) {
            return Err(FormatError::Unsupported("unknown metadata key".into()));
        }
    }
    let family = u32::from_be_bytes(fixed(kv, b"family")?);
    if family != 2 {
        return Err(FormatError::Unsupported("family".into()));
    }
    let subversion = u32::from_be_bytes(fixed(kv, b"subversion")?);
    check_subversion(subversion, CURRENT_SUBVERSION)?;
    let features = u64::from_be_bytes(fixed(kv, b"features")?);
    check_features(features)?;
    if let Some(applied) = kv.get(Keyspace::Meta, b"raft_applied").map_err(FormatError::kv)? {
        if applied.len() != 16 {
            return Err(corrupt("wrong raft_applied length"));
        }
        if features & REPLICATED == 0 {
            return Err(FormatError::Unsupported(
                "raft_applied without replicated feature".into(),
            ));
        }
    }
    let fence_bytes = kv.get(Keyspace::Meta, b"write_fence").map_err(FormatError::kv)?;
    if (features & WRITE_FENCE != 0) != fence_bytes.is_some() {
        return Err(corrupt("write_fence feature/key mismatch"));
    }
    let write_fence = fence_bytes
        .as_deref()
        .map(super::write_fence::WriteFenceRecord::decode)
        .transpose()?;
    let domain = fixed(kv, b"domain")?;
    if kv
        .get(Keyspace::Meta, b"engine")
        .map_err(FormatError::kv)?
        .as_deref()
        != Some(b"rocksdb")
    {
        return Err(corrupt("missing or invalid engine"));
    }
    let live = live_role_bootstrap_entries(kv)?;
    // In range mode (R5) the node's features are the union of its ranges' bits and the role
    // catalog lives in the system range only; the range engine checks it per range at open.
    if features & RANGES == 0
        && ((features & 512 != 0 && live != 1) || (features & 512 == 0 && live != 0))
    {
        return Err(corrupt("role bootstrap feature/catalog mismatch"));
    }
    Ok(MetaInfo {
        stale_counters: Vec::new(),
        write_fence,
        family,
        subversion,
        features,
        domain,
        engine: "rocksdb".into(),
        last_commit_ts: optional(kv, b"last_commit_ts")?,
        gc_watermark: optional(kv, b"gc_watermark")?,
    })
}
pub(crate) fn check_subversion(version: u32, supported: u32) -> Result<(), FormatError> {
    if version == 0 {
        return Err(corrupt("zero subversion"));
    }
    if version > supported {
        return Err(FormatError::Unsupported("subversion".into()));
    }
    Ok(())
}
pub(crate) fn check_features(bits: u64) -> Result<(), FormatError> {
    if bits & 8 != 0 {
        return Err(FormatError::Unsupported("retired applications feature (GD-0052 application ledger); open with a GigglesDB build at or before commit 429cbd2c".into()));
    }
    check_features_allowed(
        bits,
        0x3FFF
            | SQL_VIEWS
            | USER_ROUTINES
            | RELATION_COMMENTS
            | REPLICATED
            | KEY_DEPENDENCY_IDENTITY
            | RANGES
            | WIDE_ONCE_RECEIPTS
            | TASK_CATALOG
            | PROTECTED_RECORDS
            | WRITE_FENCE
            | USER_COLLATIONS
            | COLUMN_COLLATIONS | BUILTIN_EXTENSIONS | ROLE_MEMBERSHIP | WIDE_ROWS | UUID_ARRAY_COLUMNS
            | ENUM_ARRAY_COLUMNS | ENUM_LABEL_IDENTITIES | WIDE_CATALOG | PARTIAL_INDEXES | TEMPORAL_INDEXES | GENERATED_COLUMNS | TEXT_SEARCH_TYPES | GIN_INDEXES | STORAGE_METADATA | KEY_TYPES,
    )
}
pub(in crate::storage) fn check_features_allowed(bits: u64, allowed: u64) -> Result<(), FormatError> {
    if bits & !allowed != 0 {
        return Err(FormatError::Unsupported("feature bits".into()));
    }
    if bits & KEY_DEPENDENCY_IDENTITY != 0 && bits & 64 == 0 {
        return Err(corrupt("key dependency identity requires catalog_ext"));
    }
    if bits & PARTIAL_INDEXES != 0 && bits & SECONDARY_INDEXES == 0 {
        return Err(corrupt("partial indexes require secondary indexes"));
    }
    let enum_array_dependencies = 64 | TEXT_BACKED_COLUMN_TYPES | SCHEMAS_AND_USER_TYPES;
    if bits & ENUM_ARRAY_COLUMNS != 0 && bits & enum_array_dependencies != enum_array_dependencies {
        return Err(corrupt("enum array columns require catalog_ext, text-backed types and user types"));
    }
    let enum_label_dependencies = 64 | SCHEMAS_AND_USER_TYPES;
    if bits & ENUM_LABEL_IDENTITIES != 0 && bits & enum_label_dependencies != enum_label_dependencies {
        return Err(corrupt("enum label identities require catalog_ext and user types"));
    }
    if bits & RANGES != 0 && bits & REPLICATED == 0 {
        return Err(corrupt("ranges requires replicated"));
    }
    if bits & (TASK_CATALOG | ROLE_MEMBERSHIP) != 0 && bits & 2048 == 0 {
        return Err(corrupt("task catalog requires role_catalog"));
    }
    if bits & TEMPORAL_INDEXES != 0 && bits & SECONDARY_INDEXES == 0 {
        return Err(corrupt("temporal indexes require secondary_indexes"));
    }
    if bits & UNIQUE_CONSTRAINT_IDENTITY != 0 && bits & 68 != 68 {
        return Err(corrupt("unique constraint identity requires catalog_ext and secondary_indexes"));
    }
    if bits & 256 != 0 && bits & 64 == 0 {
        return Err(corrupt("sequence counters require catalog_ext"));
    }
    if bits & WIDE_ONCE_RECEIPTS != 0 && bits & NATIVE_OUTCOMES == 0 {
        return Err(corrupt("wide once receipts require native_outcomes"));
    }
    Ok(())
}
pub(crate) fn open_checks(kv: &dyn KvRead) -> Result<MetaInfo, FormatError> {
    let mut info = validate_meta(kv)?;
    super::native_content::check(kv, info.features, info.subversion)?;
    info.stale_counters = super::content_check::check(kv, info.features)?;
    Ok(info)
}
pub(crate) fn put_features(batch: &mut WriteBatch, bits: u64) -> Result<(), FormatError> {
    check_features(bits)?;
    batch.put(
        Keyspace::Meta,
        b"features".to_vec(),
        bits.to_be_bytes().to_vec(),
    );
    Ok(())
}
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    Open,
    MetaBatch,
    TmpWrite,
    Rename,
}
pub(crate) fn create(
    dir: &Path,
    domain: [u8; 16],
    open: impl FnOnce(&Path) -> Result<Arc<dyn KvBackend>, KvError>,
) -> Result<Arc<dyn KvBackend>, FormatError> {
    create_with_features(dir, domain, 0, open)
}
pub(crate) fn create_with_features(
    dir: &Path,
    domain: [u8; 16],
    extra_features: u64,
    open: impl FnOnce(&Path) -> Result<Arc<dyn KvBackend>, KvError>,
) -> Result<Arc<dyn KvBackend>, FormatError> {
    #[cfg(test)]
    {
        create_inner(dir, domain, extra_features, open, |_| Ok(()))
    }
    #[cfg(not(test))]
    {
        create_inner(dir, domain, extra_features, open)
    }
}
pub(crate) fn write_initial_meta(
    kv: &dyn KvBackend,
    domain: [u8; 16],
    extra_features: u64,
) -> Result<(), FormatError> {
    let mut batch = WriteBatch::default();
    for (key, value) in [
        (b"family".as_slice(), 2u32.to_be_bytes().to_vec()),
        (b"subversion", INITIAL_SUBVERSION.to_be_bytes().to_vec()),
        (b"domain", domain.to_vec()),
        (b"engine", b"rocksdb".to_vec()),
    ] {
        batch.put(Keyspace::Meta, key.to_vec(), value);
    }
    put_features(&mut batch, extra_features)?;
    kv.write(batch, Durability::Sync).map_err(FormatError::kv)
}
fn create_inner(
    dir: &Path,
    domain: [u8; 16],
    extra_features: u64,
    open: impl FnOnce(&Path) -> Result<Arc<dyn KvBackend>, KvError>,
    #[cfg(test)] hook: impl Fn(Stage) -> Result<(), FormatError>,
) -> Result<Arc<dyn KvBackend>, FormatError> {
    if classify_dir(dir)? != DirClass::Empty {
        return Err(corrupt("creation requires an empty directory"));
    }
    fs::create_dir_all(dir).map_err(corrupt)?;
    let kv = open(dir).map_err(FormatError::kv)?;
    #[cfg(test)]
    hook(Stage::Open)?;
    write_initial_meta(kv.as_ref(), domain, extra_features)?;
    #[cfg(test)]
    hook(Stage::MetaBatch)?;
    let mut file = fs::File::create(dir.join(TMP)).map_err(corrupt)?;
    file.write_all(V2).map_err(corrupt)?;
    file.sync_all().map_err(corrupt)?;
    drop(file);
    #[cfg(test)]
    hook(Stage::TmpWrite)?;
    fs::rename(dir.join(TMP), dir.join(MARKER)).map_err(corrupt)?;
    #[cfg(unix)]
    fs::File::open(dir)
        .and_then(|f| f.sync_all())
        .map_err(corrupt)?;
    #[cfg(test)]
    hook(Stage::Rename)?;
    Ok(kv)
}
#[cfg(test)]
#[path = "meta_tests.rs"]
mod meta_tests;

/// Write and sync the v2 marker for an already-created checkpoint.
pub(crate) fn write_marker(dir: &Path) -> Result<(), FormatError> {
    let mut file = fs::File::create(dir.join(MARKER)).map_err(corrupt)?;
    file.write_all(V2).map_err(corrupt)?;
    file.sync_all().map_err(corrupt)
}
