//! Fail-closed v2 directory and unversioned metadata gate.
use super::kv::{Direction, Durability, Keyspace, KvBackend, KvError, KvRead, WriteBatch};
use std::{
    fs,
    io::{Read, Write},
    ops::Bound,
    path::Path,
    sync::Arc,
};

pub(crate) const CURRENT_SUBVERSION: u32 = 1;
pub(crate) const SECONDARY_INDEXES: u64 = 1 << 2;
pub(crate) const TEXT_BACKED_COLUMN_TYPES: u64 = 1 << 10;
pub(crate) const SCHEMAS_AND_USER_TYPES: u64 = 1 << 12;
pub(crate) const UNIQUE_CONSTRAINT_IDENTITY: u64 = 1 << 13;
pub(crate) const RELATION_COMMENTS: u64 = 1 << 15;
pub(crate) const FEATURE_TABLE: [(u64, &str); 15] = [
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
    (RELATION_COMMENTS, "relation_comments"),
];
const MARKER: &str = "GIGGLES_FORMAT";
const TMP: &str = "GIGGLES_FORMAT.tmp";
const V2: &[u8] = b"giggles-v2\n";
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
}
impl std::fmt::Display for FormatError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Corrupt(s) => write!(f, "corrupt format: {s}"),
            Self::Unsupported(s) => write!(f, "unsupported format: {s}"),
            Self::Incomplete(s) => write!(f, "incomplete format: {s}"),
        }
    }
}
impl std::error::Error for FormatError {}
fn corrupt(error: impl std::fmt::Display) -> FormatError {
    FormatError::Corrupt(error.to_string())
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
        Err(e) => return Err(corrupt(e)),
    };
    let (mut marker, mut legacy, mut current, mut rocks, mut unknown) =
        (false, false, false, false, false);
    for entry in entries {
        let entry = entry.map_err(corrupt)?;
        if !entry.file_type().map_err(corrupt)?.is_file() {
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
        let file = fs::File::open(dir.join(MARKER)).map_err(corrupt)?;
        if file.metadata().map_err(corrupt)?.len() > 64 {
            return Err(corrupt("oversized marker"));
        }
        let mut bytes = Vec::new();
        file.take(64).read_to_end(&mut bytes).map_err(corrupt)?;
        if bytes != V2 {
            if let Some(n) = bytes
                .strip_prefix(b"giggles-v")
                .and_then(|s| s.strip_suffix(b"\n"))
                && n.first().is_some_and(|b| matches!(*b, b'1'..=b'9'))
                && n.iter().all(u8::is_ascii_digit)
                && n != b"2"
            {
                return Err(FormatError::Unsupported("marker version".into()));
            }
            return Err(corrupt("invalid marker"));
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
}
fn fixed<const N: usize>(kv: &dyn KvRead, key: &[u8]) -> Result<[u8; N], FormatError> {
    kv.get(Keyspace::Meta, key)
        .map_err(corrupt)?
        .ok_or_else(|| corrupt(format!("missing {}", String::from_utf8_lossy(key))))?
        .try_into()
        .map_err(|_| corrupt("wrong metadata length"))
}
fn optional(kv: &dyn KvRead, key: &[u8]) -> Result<Option<u64>, FormatError> {
    kv.get(Keyspace::Meta, key)
        .map_err(corrupt)?
        .map(|v| {
            v.try_into()
                .map(u64::from_be_bytes)
                .map_err(|_| corrupt("wrong timestamp length"))
        })
        .transpose()
}
pub(crate) fn validate_meta(kv: &dyn KvRead) -> Result<MetaInfo, FormatError> {
    for item in kv
        .range(
            Keyspace::Meta,
            Bound::Unbounded,
            Bound::Unbounded,
            Direction::Forward,
        )
        .map_err(corrupt)?
    {
        let (key, _) = item.map_err(corrupt)?;
        if !matches!(
            key.as_slice(),
            b"family"
                | b"subversion"
                | b"features"
                | b"domain"
                | b"engine"
                | b"last_commit_ts"
                | b"gc_watermark"
        ) {
            return Err(FormatError::Unsupported("unknown metadata key".into()));
        }
    }
    let family = u32::from_be_bytes(fixed(kv, b"family")?);
    if family != 2 {
        return Err(FormatError::Unsupported("family".into()));
    }
    let subversion = u32::from_be_bytes(fixed(kv, b"subversion")?);
    if subversion == 0 {
        return Err(corrupt("zero subversion"));
    }
    if subversion > CURRENT_SUBVERSION {
        return Err(FormatError::Unsupported("subversion".into()));
    }
    let features = u64::from_be_bytes(fixed(kv, b"features")?);
    check_features(features)?;
    let domain = fixed(kv, b"domain")?;
    if kv
        .get(Keyspace::Meta, b"engine")
        .map_err(corrupt)?
        .as_deref()
        != Some(b"rocksdb")
    {
        return Err(corrupt("missing or invalid engine"));
    }
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
        .map_err(corrupt)?
    {
        let (key, value) = item.map_err(corrupt)?;
        if key.len() < 9 {
            return Err(corrupt("short kind-7 versioned key"));
        }
        let user = &key[..key.len() - 8];
        if previous.as_deref() == Some(user) {
            continue;
        }
        previous = Some(user.to_vec());
        if super::mvcc::decode(&value).map_err(corrupt)?.is_some() {
            live += 1;
        }
    }
    if (features & 512 != 0 && live != 1) || (features & 512 == 0 && live != 0) {
        return Err(corrupt("role bootstrap feature/catalog mismatch"));
    }
    Ok(MetaInfo {
        stale_counters: Vec::new(),
        family,
        subversion,
        features,
        domain,
        engine: "rocksdb".into(),
        last_commit_ts: optional(kv, b"last_commit_ts")?,
        gc_watermark: optional(kv, b"gc_watermark")?,
    })
}
pub(crate) fn check_features(bits: u64) -> Result<(), FormatError> {
    check_features_allowed(bits, 0x3FFF | RELATION_COMMENTS)
}
fn check_features_allowed(bits: u64, allowed: u64) -> Result<(), FormatError> {
    if bits & !allowed != 0 {
        return Err(FormatError::Unsupported("feature bits".into()));
    }
    if bits & UNIQUE_CONSTRAINT_IDENTITY != 0 && bits & 68 != 68 {
        return Err(corrupt("unique constraint identity requires catalog_ext and secondary_indexes"));
    }
    if bits & 256 != 0 && bits & 64 == 0 {
        return Err(corrupt("sequence counters require catalog_ext"));
    }
    Ok(())
}
pub(crate) fn open_checks(kv: &dyn KvRead) -> Result<MetaInfo, FormatError> {
    let mut info = validate_meta(kv)?;
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
    #[cfg(test)]
    {
        create_inner(dir, domain, open, |_| Ok(()))
    }
    #[cfg(not(test))]
    {
        create_inner(dir, domain, open)
    }
}
fn create_inner(
    dir: &Path,
    domain: [u8; 16],
    open: impl FnOnce(&Path) -> Result<Arc<dyn KvBackend>, KvError>,
    #[cfg(test)] hook: impl Fn(Stage) -> Result<(), FormatError>,
) -> Result<Arc<dyn KvBackend>, FormatError> {
    if classify_dir(dir)? != DirClass::Empty {
        return Err(corrupt("creation requires an empty directory"));
    }
    fs::create_dir_all(dir).map_err(corrupt)?;
    let kv = open(dir).map_err(corrupt)?;
    #[cfg(test)]
    hook(Stage::Open)?;
    let mut batch = WriteBatch::default();
    for (key, value) in [
        (b"family".as_slice(), 2u32.to_be_bytes().to_vec()),
        (b"subversion", CURRENT_SUBVERSION.to_be_bytes().to_vec()),
        (b"domain", domain.to_vec()),
        (b"engine", b"rocksdb".to_vec()),
    ] {
        batch.put(Keyspace::Meta, key.to_vec(), value);
    }
    put_features(&mut batch, 0)?;
    kv.write(batch, Durability::Sync).map_err(corrupt)?;
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
