//! Bounded deterministic local release bundles. Hashes provide integrity, not authenticity.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub const MAX_FILE: u64 = 64 * 1024 * 1024;
pub const MAX_ARCHIVE: u64 = 160 * 1024 * 1024;
pub const MAX_REQUEST: u64 = 1024 * 1024;
pub const MAX_MANIFEST: usize = 4 * 1024 * 1024;
fn ensure(ok: bool, message: &str) -> Result<()> {
    if ok { Ok(()) } else { Err(message.into()) }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FileRecord {
    pub path: String,
    pub size: u64,
    pub sha256: String,
    pub role: Role,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Role {
    Binary,
    Readme,
    License,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SourceRecord {
    pub path: String,
    pub size: u64,
    pub sha256: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Dependency {
    pub name: String,
    pub version: String,
    pub source: Option<String>,
    pub checksum: Option<String>,
    pub license: Option<String>,
    pub manifest_path: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReleaseManifest {
    pub format: String,
    pub version: u32,
    pub release: String,
    pub target: String,
    pub profile: String,
    pub rustc: String,
    pub cargo: String,
    pub source_revision: String,
    pub dirty: bool,
    pub sources: Vec<SourceRecord>,
    pub dependencies: Vec<Dependency>,
    pub files: Vec<FileRecord>,
    pub readiness_issues: Vec<String>,
    pub build: Option<BuildReceipt>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputFile {
    pub source: PathBuf,
    pub path: String,
    pub role: Role,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateRequest {
    pub workspace: PathBuf,
    pub manifest: PathBuf,
    pub release: String,
    pub target: String,
    pub profile: String,
    pub source_revision: String,
    pub dirty: bool,
    pub files: Vec<InputFile>,
    pub build_receipt: Option<PathBuf>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Archive {
    pub manifest: ReleaseManifest,
    #[serde(deserialize_with = "unique_files")]
    pub files: BTreeMap<String, String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildRequest {
    pub request: CreateRequest,
    pub package: String,
    pub binary: String,
    pub target_dir: PathBuf,
    pub timeout_seconds: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BuildReceipt {
    pub command: Vec<String>,
    pub target: String,
    pub profile: String,
    pub source_revision: String,
    pub dirty: bool,
    pub rustc: String,
    pub cargo: String,
    pub success: bool,
    pub exit_code: Option<i32>,
    pub output: String,
    pub sources_before: Vec<SourceRecord>,
    pub sources_after: Vec<SourceRecord>,
    pub artifact: Option<FileRecord>,
}
fn unique_files<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<BTreeMap<String, String>, D::Error> {
    struct Visitor;
    impl<'de> serde::de::Visitor<'de> for Visitor {
        type Value = BTreeMap<String, String>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("unique bounded file map")
        }
        fn visit_map<A: serde::de::MapAccess<'de>>(
            self,
            mut map: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            let mut out = BTreeMap::new();
            while let Some((key, value)) = map.next_entry::<String, String>()? {
                if out.len() >= 128 || out.insert(key, value).is_some() {
                    return Err(serde::de::Error::custom("duplicate or excessive files"));
                }
            }
            Ok(out)
        }
    }
    d.deserialize_map(Visitor)
}
pub fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn text(s: &str) -> Result<()> {
    ensure(
        !s.is_empty() && s.len() <= 8192 && !s.contains('\0'),
        "invalid identity",
    )
}
fn digest(s: &str) -> Result<()> {
    ensure(
        s.len() == 64
            && s.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "invalid SHA256",
    )
}
/// Validates a portable relative artifact/source path.
/// ```
/// assert!(giggles_release::portable_path("bin/giggles.exe").is_ok());
/// assert!(giggles_release::portable_path("../secret").is_err());
/// assert!(giggles_release::portable_path("NUL.txt").is_err());
/// ```
pub fn portable_path(s: &str) -> Result<()> {
    ensure(
        !s.is_empty() && s.len() <= 240 && s.is_ascii(),
        "path bound",
    )?;
    for part in s.split('/') {
        ensure(
            !part.is_empty()
                && part != "."
                && part != ".."
                && !part.ends_with([' ', '.'])
                && !part.bytes().any(|b| b < 32 || b"\\:*?\"<>|".contains(&b)),
            "unsafe path",
        )?;
        let stem = part.split('.').next().unwrap_or("").to_ascii_uppercase();
        ensure(
            !["CON", "PRN", "AUX", "NUL", "CLOCK$", "CONIN$", "CONOUT$"].contains(&stem.as_str())
                && !(stem.len() == 4
                    && (stem.starts_with("COM") || stem.starts_with("LPT"))
                    && stem.as_bytes()[3].is_ascii_digit()),
            "reserved path",
        )?;
    }
    Ok(())
}
fn artifact_role(path: &str, role: &Role) -> Result<()> {
    let name = path.split('/').next_back().ok_or("artifact basename")?;
    match role {
        Role::Binary => ensure(
            path.starts_with("bin/") && !path[4..].contains('/'),
            "binary allowlist",
        ),
        Role::Readme => ensure(name == "README.md", "README allowlist"),
        Role::License => ensure(
            name == "LICENSE" || name.starts_with("LICENSE."),
            "license allowlist",
        ),
    }
}
fn no_links(path: &Path) -> Result<()> {
    for ancestor in path.ancestors() {
        if ancestor.as_os_str().is_empty() {
            continue;
        }
        let m = fs::symlink_metadata(ancestor)?;
        ensure(!m.file_type().is_symlink(), "symlink prohibited")?;
        #[cfg(windows)]
        {
            use std::os::windows::fs::MetadataExt;
            ensure(m.file_attributes() & 0x400 == 0, "reparse point prohibited")?;
        }
    }
    Ok(())
}
fn regular(path: &Path) -> Result<fs::Metadata> {
    no_links(path)?;
    let m = fs::symlink_metadata(path)?;
    ensure(m.is_file(), "regular file required")?;
    Ok(m)
}
pub fn read_bounded(path: &Path, max: u64) -> Result<Vec<u8>> {
    let before = regular(path)?;
    ensure(before.len() <= max, "file too large")?;
    let mut bytes = Vec::new();
    File::open(path)?.take(max + 1).read_to_end(&mut bytes)?;
    let after = regular(path)?;
    ensure(
        bytes.len() as u64 <= max
            && before.len() == after.len()
            && before.modified()? == after.modified()?
            && bytes.len() as u64 == after.len(),
        "file changed during read",
    )?;
    Ok(bytes)
}
fn stable_read(path: &Path, max: u64) -> Result<Vec<u8>> {
    let a = read_bounded(path, max)?;
    ensure(a == read_bounded(path, max)?, "source changed")?;
    Ok(a)
}
impl ReleaseManifest {
    pub fn validate(&self) -> Result<()> {
        ensure(
            serde_json::to_vec(self)?.len() <= MAX_MANIFEST,
            "manifest byte bound",
        )?;
        ensure(
            self.version == 1 && self.format == "giggles-release-json-v1",
            "unsupported format/version",
        )?;
        for s in [
            &self.release,
            &self.target,
            &self.profile,
            &self.rustc,
            &self.cargo,
            &self.source_revision,
        ] {
            text(s)?;
        }
        ensure(
            matches!(self.profile.as_str(), "dev" | "release"),
            "unsupported profile",
        )?;
        ensure(
            !self.files.is_empty()
                && self.files.len() <= 128
                && !self.sources.is_empty()
                && self.sources.len() <= 8192
                && self.dependencies.len() <= 2048
                && self.readiness_issues.len() <= 4096,
            "manifest bounds",
        )?;
        let mut seen = BTreeSet::new();
        let mut total = 0u64;
        for f in &self.files {
            portable_path(&f.path)?;
            artifact_role(&f.path, &f.role)?;
            digest(&f.sha256)?;
            ensure(
                seen.insert(f.path.to_ascii_lowercase()),
                "duplicate artifact",
            )?;
            ensure(f.size <= MAX_FILE, "artifact too large")?;
            total = total.checked_add(f.size).ok_or("size overflow")?;
        }
        ensure(total <= MAX_FILE, "total artifacts too large")?;
        for path in &seen {
            let mut parent = path.as_str();
            while let Some((p, _)) = parent.rsplit_once('/') {
                ensure(!seen.contains(p), "file/directory artifact collision")?;
                parent = p;
            }
        }
        ensure(
            self.files.iter().any(|f| f.role == Role::Binary),
            "missing executable identity",
        )?;
        let mut seen = BTreeSet::new();
        for s in &self.sources {
            portable_path(&s.path)?;
            digest(&s.sha256)?;
            ensure(
                s.size <= MAX_FILE && seen.insert(s.path.to_ascii_lowercase()),
                "source bounds/duplicate",
            )?;
        }
        let mut seen = BTreeSet::new();
        for d in &self.dependencies {
            text(&d.name)?;
            text(&d.version)?;
            if d.source.is_none() {
                let path = d
                    .manifest_path
                    .as_deref()
                    .ok_or("missing local dependency manifest")?;
                portable_path(path)?;
                ensure(
                    self.sources.iter().any(|s| s.path == path),
                    "omitted local dependency source",
                )?;
            } else {
                ensure(
                    d.manifest_path.is_none(),
                    "external dependency with local manifest",
                )?;
            }
            ensure(
                seen.insert((&d.name, &d.version, &d.source)),
                "duplicate dependency",
            )?;
            if let Some(c) = &d.checksum {
                digest(c)?;
            }
            for s in [&d.source, &d.license].into_iter().flatten() {
                text(s)?;
            }
        }
        if let Some(b) = &self.build {
            ensure(
                b.success
                    && b.target == self.target
                    && b.profile == self.profile
                    && b.source_revision == self.source_revision
                    && b.dirty == self.dirty
                    && b.exit_code == Some(0)
                    && b.sources_before == self.sources
                    && b.sources_after == self.sources
                    && b.rustc == self.rustc
                    && b.cargo == self.cargo,
                "build/source mismatch",
            )?;
            ensure(
                b.command.len() <= 64 && b.output.len() as u64 <= MAX_REQUEST,
                "receipt bounds",
            )?;
            for arg in &b.command {
                text(arg)?;
            }
            let artifact = b.artifact.as_ref().ok_or("missing built artifact")?;
            ensure(
                self.files.iter().any(|f| {
                    f.role == Role::Binary && f.size == artifact.size && f.sha256 == artifact.sha256
                }),
                "built executable mismatch",
            )?;
        }
        for s in &self.readiness_issues {
            text(s)?;
        }
        Ok(())
    }
}
static NEXT: AtomicU64 = AtomicU64::new(0);
pub fn unique_dir(parent: &Path) -> Result<PathBuf> {
    fs::create_dir_all(parent)?;
    let p = parent.join(format!(
        "staging-{}-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&p)?;
    Ok(p)
}
fn write_new(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut f = OpenOptions::new().write(true).create_new(true).open(path)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    Ok(())
}
fn rooted(workspace: &Path, manifest: &Path) -> Result<(PathBuf, PathBuf)> {
    no_links(workspace)?;
    let w = workspace.canonicalize()?;
    let m = if manifest.is_absolute() {
        manifest.to_path_buf()
    } else {
        w.join(manifest)
    };
    regular(&m)?;
    let m = m.canonicalize()?;
    ensure(
        m.starts_with(&w) && m.file_name().is_some_and(|n| n == "Cargo.toml"),
        "manifest outside workspace",
    )?;
    Ok((w, m))
}
fn captured(
    mut command: Command,
    cap: u64,
    timeout: Duration,
) -> Result<(std::process::ExitStatus, Vec<u8>, Vec<u8>)> {
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let stdout = child.stdout.take().ok_or("stdout")?;
    let stderr = child.stderr.take().ok_or("stderr")?;
    let (tx, rx) = std::sync::mpsc::channel();
    let spawn_reader = |reader: Box<dyn Read + Send>,
                        tx: std::sync::mpsc::Sender<(bool, std::io::Result<Vec<u8>>)>,
                        is_stdout| {
        std::thread::spawn(move || {
            let mut bytes = Vec::new();
            let r = reader.take(cap + 1).read_to_end(&mut bytes).map(|_| bytes);
            let _ = tx.send((is_stdout, r));
        });
    };
    spawn_reader(Box::new(stdout), tx.clone(), true);
    spawn_reader(Box::new(stderr), tx, false);
    let start = Instant::now();
    let mut out = None;
    let mut err = None;
    loop {
        while let Ok((is_out, result)) = rx.try_recv() {
            let bytes = result?;
            if bytes.len() as u64 > cap {
                let _ = child.kill();
                let _ = child.wait();
                return Err("subprocess output bound".into());
            }
            if is_out {
                out = Some(bytes);
            } else {
                err = Some(bytes);
            }
        }
        if let Some(status) = child.try_wait()?
            && out.is_some()
            && err.is_some()
        {
            return Ok((
                status,
                out.take().ok_or("stdout missing")?,
                err.take().ok_or("stderr missing")?,
            ));
        }
        if start.elapsed() > timeout {
            let _ = child.kill();
            let _ = child.wait();
            return Err("subprocess deadline".into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}
fn version(program: &str) -> Result<String> {
    let mut c = Command::new(program);
    c.arg("--version");
    let (status, stdout, _) = captured(c, 8192, Duration::from_secs(10))?;
    ensure(status.success(), "toolchain version failed")?;
    Ok(String::from_utf8(stdout)?.trim().to_owned())
}
// Tokenize instead of rewriting source text: literal whitespace, raw strings,
// comments and escaped characters must retain Rust's meaning.
fn included_paths(
    tokens: proc_macro2::TokenStream,
    out: &mut Vec<String>,
    attribute: bool,
) -> Result<()> {
    use proc_macro2::{Delimiter, TokenTree};
    let tokens: Vec<_> = tokens.into_iter().collect();
    ensure(
        !attribute
            || !tokens.windows(2).any(|w| {
                matches!(&w[0], TokenTree::Ident(id) if id == "path")
                    && matches!(&w[1], TokenTree::Punct(p) if p.as_char() == '=')
            }),
        "explicit module path unsupported",
    )?;
    for (i, token) in tokens.iter().enumerate() {
        if let TokenTree::Group(group) = token {
            if group.delimiter() == Delimiter::Bracket {
                let attribute = group.stream().into_iter().collect::<Vec<_>>();
                ensure(
                    !attribute.windows(2).any(|w| {
                        matches!(&w[0], TokenTree::Ident(id) if id == "path")
                            && matches!(&w[1], TokenTree::Punct(p) if p.as_char() == '=')
                    }),
                    "explicit module path unsupported",
                )?;
            }
            included_paths(
                group.stream(),
                out,
                attribute || group.delimiter() == Delimiter::Bracket,
            )?;
        }
        if let TokenTree::Ident(name) = token
            && matches!(tokens.get(i + 1), Some(TokenTree::Punct(p)) if p.as_char() == '!')
        {
            ensure(
                name != "include",
                "unresolved generated Rust include dependency",
            )?;
            if name == "include_str" || name == "include_bytes" {
                let Some(TokenTree::Group(group)) = tokens.get(i + 2) else {
                    return Err("include arguments".into());
                };
                // Only one direct Rust string literal is supported; no computed paths.
                let literal: syn::LitStr = syn::parse2(group.stream())
                    .map_err(|_| "unresolved include macro source dependency")?;
                out.push(literal.value());
                ensure(out.len() <= 8192, "include count")?;
            }
        }
    }
    Ok(())
}
fn scan(root: &Path, dir: &Path, out: &mut Vec<SourceRecord>) -> Result<()> {
    for e in fs::read_dir(dir)? {
        let e = e?;
        let p = e.path();
        let m = fs::symlink_metadata(&p)?;
        ensure(!m.file_type().is_symlink(), "source link")?;
        #[cfg(windows)]
        {
            use std::os::windows::fs::MetadataExt;
            ensure(m.file_attributes() & 0x400 == 0, "source reparse")?;
        }
        let name = e.file_name();
        let name = name.to_str().ok_or("non UTF8 source")?;
        if m.is_dir() {
            if !["target", "artifacts", ".git"].contains(&name) && !name.starts_with("target-") {
                scan(root, &p, out)?;
            }
        } else if name.ends_with(".rs") || name == "Cargo.toml" || name == "Cargo.lock" {
            let b = stable_read(&p, MAX_FILE)?;
            if name.ends_with(".rs") {
                let content = std::str::from_utf8(&b)?;
                let tokens = content.parse().map_err(|_| "invalid Rust source tokens")?;
                let mut includes = Vec::new();
                included_paths(tokens, &mut includes, false)?;
                for relative in includes {
                    let included = p.parent().ok_or("include parent")?.join(relative);
                    regular(&included)?;
                    let included = included.canonicalize()?;
                    ensure(
                        included.starts_with(root),
                        "include outside explicit workspace",
                    )?;
                    let data = stable_read(&included, MAX_FILE)?;
                    out.push(SourceRecord {
                        path: included
                            .strip_prefix(root)?
                            .to_str()
                            .ok_or("include path")?
                            .replace('\\', "/"),
                        size: data.len() as u64,
                        sha256: hash(&data),
                    });
                }
            }
            out.push(SourceRecord {
                path: p
                    .strip_prefix(root)?
                    .to_str()
                    .ok_or("source path")?
                    .replace('\\', "/"),
                size: b.len() as u64,
                sha256: hash(&b),
            });
            ensure(out.len() <= 8192, "source count")?;
        }
    }
    Ok(())
}
/// Derives an own-format inventory, including inactive lock entries. Requires cached Cargo metadata.
pub fn inventory(
    workspace: &Path,
    manifest: &Path,
) -> Result<(Vec<SourceRecord>, Vec<Dependency>, Vec<String>)> {
    inventory_checked(workspace, manifest, false)
}
fn inventory_checked(
    workspace: &Path,
    manifest: &Path,
    reject_build_scripts: bool,
) -> Result<(Vec<SourceRecord>, Vec<Dependency>, Vec<String>)> {
    let (w, m) = rooted(workspace, manifest)?;
    check_manifest_links(&m)?;
    let mut cmd = Command::new("cargo");
    cmd.args([
        "metadata",
        "--locked",
        "--offline",
        "--format-version",
        "1",
        "--manifest-path",
    ])
    .arg(&m);
    let (status, stdout, stderr) = captured(cmd, 16 * 1024 * 1024, Duration::from_secs(60))?;
    ensure(
        status.success(),
        &format!("metadata failed: {}", String::from_utf8_lossy(&stderr)),
    )?;
    let meta: serde_json::Value = serde_json::from_slice(&stdout)?;
    let workspace_root = PathBuf::from(meta["workspace_root"].as_str().ok_or("workspace root")?);
    no_links(&workspace_root)?;
    let workspace_root = workspace_root.canonicalize()?;
    check_manifest_links(&workspace_root.join("Cargo.toml"))?;
    ensure(
        workspace_root.starts_with(&w),
        "workspace root outside explicit workspace",
    )?;
    let lockpath = workspace_root.join("Cargo.lock");
    let lockbytes = stable_read(&lockpath, MAX_REQUEST)?;
    let lock: toml::Value = toml::from_str(std::str::from_utf8(&lockbytes)?)?;
    ensure(
        matches!(
            lock.get("version").and_then(toml::Value::as_integer),
            Some(3 | 4)
        ),
        "unknown Cargo.lock version",
    )?;
    let packages = meta["packages"].as_array().ok_or("metadata packages")?;
    let mut sources = Vec::new();
    let mut roots = BTreeSet::new();
    for p in packages {
        if p["source"].is_null() {
            let path = PathBuf::from(p["manifest_path"].as_str().ok_or("package manifest")?);
            regular(&path)?;
            check_manifest_links(&path)?;
            let path = path.canonicalize()?;
            ensure(
                path.starts_with(&w),
                "unresolved path dependency outside workspace",
            )?;
            let package_root = path.parent().ok_or("manifest parent")?;
            for target in p["targets"].as_array().ok_or("package targets")? {
                let kinds = target["kind"].as_array().ok_or("target kinds")?;
                ensure(
                    !reject_build_scripts || !kinds.iter().any(|k| k == "custom-build"),
                    "build scripts require additional environment/input qualification",
                )?;
                let source = PathBuf::from(target["src_path"].as_str().ok_or("target source")?);
                regular(&source)?;
                let source = source.canonicalize()?;
                ensure(
                    source.extension().is_some_and(|e| e == "rs"),
                    "non-Rust-extension target unsupported",
                )?;
                let relative = source
                    .strip_prefix(package_root)
                    .map_err(|_| "target outside package root")?;
                ensure(
                    !relative.components().any(|c| {
                        c.as_os_str().to_str().is_none_or(|s| {
                            ["target", "artifacts", ".git"].contains(&s) || s.starts_with("target-")
                        })
                    }),
                    "target outside scanned source tree",
                )?;
            }
            roots.insert(package_root.to_path_buf());
        }
    }
    for r in roots {
        scan(&w, &r, &mut sources)?;
    }
    let lockrel = lockpath
        .strip_prefix(&w)?
        .to_str()
        .ok_or("lock path")?
        .replace('\\', "/");
    if !sources.iter().any(|s| s.path == lockrel) {
        sources.push(SourceRecord {
            path: lockrel,
            size: lockbytes.len() as u64,
            sha256: hash(&lockbytes),
        });
    }
    for path in [
        workspace_root.join("Cargo.toml"),
        w.join(".cargo/config"),
        w.join(".cargo/config.toml"),
    ] {
        if path.exists() {
            let b = stable_read(&path, MAX_REQUEST)?;
            sources.push(SourceRecord {
                path: path
                    .strip_prefix(&w)?
                    .to_str()
                    .ok_or("workspace input path")?
                    .replace('\\', "/"),
                size: b.len() as u64,
                sha256: hash(&b),
            });
        }
    }
    sources.sort_by(|a, b| a.path.cmp(&b.path));
    sources.dedup();
    let (deps, issues) = dependency_inventory(&lock, packages, &w)?;
    Ok((sources, deps, issues))
}
// Cargo may canonicalize a dependency before returning metadata. Inspect the
// declared local paths too, before that information is lost.
fn check_manifest_links(manifest: &Path) -> Result<()> {
    fn walk(value: &toml::Value, base: &Path, dependency: bool) -> Result<()> {
        if let Some(table) = value.as_table() {
            if dependency && let Some(path) = table.get("path").and_then(toml::Value::as_str) {
                no_links(&base.join(path))?;
            }
            for (key, value) in table {
                walk(
                    value,
                    base,
                    dependency
                        || matches!(
                            key.as_str(),
                            "dependencies"
                                | "dev-dependencies"
                                | "build-dependencies"
                                | "patch"
                                | "replace"
                        ),
                )?;
            }
        }
        Ok(())
    }
    let bytes = stable_read(manifest, MAX_REQUEST)?;
    let value: toml::Value = toml::from_str(std::str::from_utf8(&bytes)?)?;
    walk(&value, manifest.parent().ok_or("manifest parent")?, false)
}
/// Parses the complete lock inventory, preserving inactive registry/git records without inventing metadata.
pub fn dependency_inventory(
    lock: &toml::Value,
    packages: &[serde_json::Value],
    w: &Path,
) -> Result<(Vec<Dependency>, Vec<String>)> {
    let mut deps = Vec::new();
    let mut issues = Vec::new();
    for p in lock["package"].as_array().ok_or("lock packages")? {
        let name = p["name"].as_str().ok_or("lock name")?.to_owned();
        let ver = p["version"].as_str().ok_or("lock version")?.to_owned();
        let source = p.get("source").and_then(|v| v.as_str()).map(str::to_owned);
        let matching = packages.iter().find(|x| {
            x["name"].as_str() == Some(&name)
                && x["version"].as_str() == Some(&ver)
                && x["source"].as_str() == source.as_deref()
        });
        if source.is_none() {
            ensure(matching.is_some(), "unresolved local dependency")?;
        }
        let license = matching
            .and_then(|x| x["license"].as_str())
            .map(str::to_owned);
        if license.is_none() {
            issues.push(format!("missing license metadata: {name}@{ver}"));
        }
        if matching.is_none() {
            issues.push(format!("inactive or unresolved metadata: {name}@{ver}"));
        }
        deps.push(Dependency {
            name,
            version: ver,
            source: source.clone(),
            checksum: p
                .get("checksum")
                .and_then(|v| v.as_str())
                .map(str::to_owned),
            license,
            manifest_path: if source.is_none() {
                Some(
                    Path::new(
                        matching.ok_or("local metadata")?["manifest_path"]
                            .as_str()
                            .ok_or("local manifest")?,
                    )
                    .canonicalize()?
                    .strip_prefix(w)?
                    .to_str()
                    .ok_or("manifest UTF8")?
                    .replace('\\', "/"),
                )
            } else {
                None
            },
        });
    }
    deps.sort_by(|a, b| (&a.name, &a.version, &a.source).cmp(&(&b.name, &b.version, &b.source)));
    issues.sort();
    Ok((deps, issues))
}
fn hex(bytes: &[u8]) -> String {
    const H: &[u8] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(H[(b >> 4) as usize] as char);
        s.push(H[(b & 15) as usize] as char);
    }
    s
}
fn unhex(s: &str) -> Result<Vec<u8>> {
    ensure(
        s.len().is_multiple_of(2) && s.len() as u64 <= MAX_FILE * 2,
        "hex size",
    )?;
    s.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|p| {
            let digit = |b| -> Result<u8> {
                match b {
                    b'0'..=b'9' => Ok(b - b'0'),
                    b'a'..=b'f' => Ok(b - b'a' + 10),
                    _ => Err("invalid hex".into()),
                }
            };
            Ok(digit(p[0])? * 16 + digit(p[1])?)
        })
        .collect()
}
pub fn verify_bytes(bytes: &[u8]) -> Result<ReleaseManifest> {
    ensure(bytes.len() as u64 <= MAX_ARCHIVE, "archive bound")?;
    let a: Archive = serde_json::from_slice(bytes)?;
    a.manifest.validate()?;
    ensure(
        a.files.len() == a.manifest.files.len(),
        "unexpected/missing files",
    )?;
    for f in &a.manifest.files {
        let b = unhex(a.files.get(&f.path).ok_or("missing file")?)?;
        ensure(
            b.len() as u64 == f.size && hash(&b) == f.sha256,
            "artifact integrity mismatch",
        )?;
    }
    Ok(a.manifest)
}
pub fn verify(path: &Path) -> Result<ReleaseManifest> {
    verify_bytes(&stable_read(path, MAX_ARCHIVE)?)
}
pub fn create(req: &CreateRequest, output: &Path) -> Result<ReleaseManifest> {
    ensure(
        !req.files.is_empty() && req.files.len() <= 128,
        "input count",
    )?;
    let (w, m) = rooted(&req.workspace, &req.manifest)?;
    let (sources, dependencies, mut issues) = inventory(&w, &m)?;
    let parent = output
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let stage = unique_dir(parent)?;
    let mut files = BTreeMap::new();
    let mut records = Vec::new();
    let mut inputs = Vec::new();
    let mut total_bytes = 0u64;
    for input in &req.files {
        portable_path(&input.path)?;
        let source = if input.source.is_absolute() {
            input.source.clone()
        } else {
            w.join(&input.source)
        };
        let bytes = stable_read(&source, MAX_FILE)?;
        total_bytes = total_bytes
            .checked_add(bytes.len() as u64)
            .ok_or("artifact total overflow")?;
        ensure(total_bytes <= MAX_FILE, "total artifact input bound")?;
        artifact_role(&input.path, &input.role)?;
        let destination = stage.join(&input.path);
        if let Some(p) = destination.parent() {
            fs::create_dir_all(p)?;
        }
        write_new(&destination, &bytes)?;
        records.push(FileRecord {
            path: input.path.clone(),
            size: bytes.len() as u64,
            sha256: hash(&bytes),
            role: input.role.clone(),
        });
        ensure(
            files.insert(input.path.clone(), hex(&bytes)).is_none(),
            "duplicate input",
        )?;
        inputs.push((source, bytes));
    }
    if !records.iter().any(|r| r.role == Role::License) {
        issues.push("no product license included; terms not selected by this tool".to_owned());
    }
    issues.sort();
    records.sort_by(|a, b| a.path.cmp(&b.path));
    let manifest = ReleaseManifest {
        format: "giggles-release-json-v1".into(),
        version: 1,
        release: req.release.clone(),
        target: req.target.clone(),
        profile: req.profile.clone(),
        rustc: version("rustc")?,
        cargo: version("cargo")?,
        source_revision: req.source_revision.clone(),
        dirty: req.dirty,
        sources,
        dependencies,
        files: records,
        readiness_issues: issues,
        build: req
            .build_receipt
            .as_ref()
            .map(|p| -> Result<BuildReceipt> {
                Ok(serde_json::from_slice(&stable_read(p, 4 * MAX_REQUEST)?)?)
            })
            .transpose()?,
    };
    manifest.validate()?;
    for (p, b) in inputs {
        ensure(stable_read(&p, MAX_FILE)? == b, "artifact source changed")?;
    }
    ensure(
        inventory(&w, &m)?.0 == manifest.sources,
        "source closure changed",
    )?;
    let archive = serde_json::to_vec(&Archive {
        manifest: manifest.clone(),
        files,
    })?;
    verify_bytes(&archive)?;
    let staged = stage.join("complete.ggr");
    write_new(&staged, &archive)?;
    verify(&staged)?;
    fs::hard_link(&staged, output)?;
    Ok(manifest)
}
/// Runs only the local Cargo toolchain, never any executable from a bundle.
pub fn build(req: &BuildRequest, receipt_path: &Path) -> Result<BuildReceipt> {
    ensure(
        (1..=3600).contains(&req.timeout_seconds),
        "build timeout bound",
    )?;
    portable_path(&req.package)?;
    portable_path(&req.binary)?;
    for name in [&req.package, &req.binary] {
        ensure(
            name.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
                && name
                    .as_bytes()
                    .first()
                    .is_some_and(u8::is_ascii_alphanumeric),
            "invalid Cargo selector",
        )?;
    }
    let (w, m) = rooted(&req.request.workspace, &req.request.manifest)?;
    ensure(
        req.target_dir.is_absolute() && !req.target_dir.exists(),
        "new absolute private target required",
    )?;
    ensure(
        matches!(req.request.profile.as_str(), "dev" | "release")
            && !req.request.target.starts_with('-'),
        "invalid build profile/target",
    )?;
    text(&req.request.target)?;
    let before = inventory_checked(&w, &m, true)?.0;
    ensure(
        !before.iter().any(|s| s.path.ends_with("build.rs")),
        "build scripts require additional environment/input qualification",
    )?;
    let rustc = version("rustc")?;
    let cargo = version("cargo")?;
    let args = vec![
        "build".to_owned(),
        "--locked".into(),
        "--offline".into(),
        "--manifest-path".into(),
        m.to_string_lossy().into_owned(),
        "--package".into(),
        req.package.clone(),
        "--bin".into(),
        req.binary.clone(),
        "--profile".into(),
        req.request.profile.clone(),
        "--target".into(),
        req.request.target.clone(),
        "-j1".into(),
    ];
    let stage = unique_dir(receipt_path.parent().unwrap_or(Path::new(".")))?;
    let log = stage.join("build.log");
    let logf = OpenOptions::new().create_new(true).write(true).open(&log)?;
    // Reserve the private directory atomically; two builds cannot share a fresh target.
    fs::create_dir(&req.target_dir)?;
    let mut child = Command::new("cargo")
        .args(&args)
        .current_dir(&w)
        .env("CARGO_TARGET_DIR", &req.target_dir)
        .env("CARGO_BUILD_JOBS", "1")
        .stdout(Stdio::from(logf.try_clone()?))
        .stderr(Stdio::from(logf))
        .spawn()?;
    let start = Instant::now();
    let status = loop {
        if let Some(s) = child.try_wait()? {
            break Some(s);
        }
        if start.elapsed() > Duration::from_secs(req.timeout_seconds)
            || fs::metadata(&log)?.len() > MAX_REQUEST
        {
            child.kill()?;
            child.wait()?;
            break None;
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    let after_result = inventory_checked(&w, &m, true);
    let after = after_result
        .as_ref()
        .map(|v| v.0.clone())
        .unwrap_or_default();
    let mut logbytes = Vec::new();
    File::open(&log)?
        .take(MAX_REQUEST)
        .read_to_end(&mut logbytes)?;
    let output = String::from_utf8_lossy(&logbytes).into_owned();
    let artifact = if status.is_some_and(|s| s.success()) {
        let profile = if req.request.profile == "dev" {
            "debug"
        } else {
            "release"
        };
        let suffix = if req.request.target.contains("windows") {
            ".exe"
        } else {
            ""
        };
        let path = req
            .target_dir
            .join(&req.request.target)
            .join(profile)
            .join(format!("{}{suffix}", req.binary));
        match stable_read(&path, MAX_FILE) {
            Ok(bytes) => Some(FileRecord {
                path: format!("bin/{}{suffix}", req.binary),
                size: bytes.len() as u64,
                sha256: hash(&bytes),
                role: Role::Binary,
            }),
            Err(_) => None,
        }
    } else {
        None
    };
    let receipt = BuildReceipt {
        target: req.request.target.clone(),
        profile: req.request.profile.clone(),
        source_revision: req.request.source_revision.clone(),
        dirty: req.request.dirty,
        command: std::iter::once("cargo".to_owned()).chain(args).collect(),
        rustc,
        cargo,
        success: artifact.is_some()
            && status.is_some_and(|s| s.success())
            && before == after
            && after_result.is_ok(),
        exit_code: status.and_then(|s| s.code()),
        output,
        sources_before: before,
        sources_after: after,
        artifact,
    };
    write_new(receipt_path, &serde_json::to_vec_pretty(&receipt)?)?;
    Ok(receipt)
}
/// Explicit local-only smoke action. Caller must trust this locally built executable and receipt.
/// Bundle verification never calls this function.
pub fn smoke_local(
    receipt_path: &Path,
    executable: &Path,
    scratch_parent: &Path,
) -> Result<serde_json::Value> {
    let receipt: BuildReceipt =
        serde_json::from_slice(&stable_read(receipt_path, 4 * MAX_REQUEST)?)?;
    ensure(
        receipt.success
            && receipt.exit_code == Some(0)
            && receipt.sources_before == receipt.sources_after,
        "unsuccessful local build receipt",
    )?;
    let artifact = receipt.artifact.as_ref().ok_or("missing built artifact")?;
    let bytes = stable_read(executable, MAX_FILE)?;
    ensure(
        bytes.len() as u64 == artifact.size && hash(&bytes) == artifact.sha256,
        "local executable does not match build receipt",
    )?;
    let executable = executable.canonicalize()?;
    let scratch = unique_dir(scratch_parent)?;
    let database = scratch.join("smoke-db");
    let db = database.to_str().ok_or("database UTF8")?.to_owned();
    let mut commands = vec![vec!["create".to_owned(), db.clone()]];
    for sql in [
        "CREATE TABLE release_smoke (id BIGINT PRIMARY KEY, body TEXT NOT NULL)",
        "INSERT INTO release_smoke VALUES(1,'hello-release')",
        "SELECT body FROM release_smoke WHERE id = 1",
    ] {
        commands.push(vec![
            "admin".to_owned(),
            db.clone(),
            serde_json::json!({"command":"sql","sql":sql,"params":[],"expected_version":null})
                .to_string(),
        ]);
    }
    // Each operation is a distinct process, so the SELECT proves reopen/read of durable data.
    commands.push(vec!["inspect".into(), db]);
    let mut results = Vec::new();
    for args in &commands {
        let mut cmd = Command::new(&executable);
        cmd.args(args);
        let (status, stdout, stderr) = captured(cmd, MAX_REQUEST, Duration::from_secs(30))?;
        let response: serde_json::Value = serde_json::from_slice(&stdout)?;
        let result = serde_json::json!({"args":args,"status":status.code(),"response":response,"stderr":String::from_utf8_lossy(&stderr)});
        write_new(
            &scratch.join(format!("response-{}.json", results.len())),
            &serde_json::to_vec_pretty(&result)?,
        )?;
        ensure(
            status.success() && response["code"] == 0,
            "local smoke command failed; retained response",
        )?;
        if args.iter().any(|a| a.contains("SELECT body")) {
            ensure(
                smoke_query_matches(&response),
                "smoke query missing expected value",
            )?;
        }
        results.push(result);
    }
    Ok(
        serde_json::json!({"database":database,"executable_sha256":artifact.sha256,"results":results}),
    )
}

fn smoke_query_matches(response: &serde_json::Value) -> bool {
    let data = &response["data"];
    data["columns"] == serde_json::json!(["body"])
        && data["affected"] == 0
        && data["rows"].as_array().is_some_and(|rows| {
            rows.len() == 1
                && rows[0]["values"] == serde_json::json!([{"Text":"hello-release"}])
                && rows[0]["version"]
                    .as_u64()
                    .is_some_and(|version| version > 0)
        })
}

#[cfg(test)]
mod smoke_tests {
    use super::*;
    #[test]
    fn exact_row_required_instead_of_diagnostic_substring() {
        let good = serde_json::json!({"data":{"columns":["body"],"affected":0,"rows":[{"values":[{"Text":"hello-release"}],"version":1}]}});
        assert!(smoke_query_matches(&good));
        for bad in [
            serde_json::json!({"data":{"rows":[],"message":"hello-release"}}),
            serde_json::json!({"data":{"columns":["body"],"affected":0,"rows":[{"values":[{"Text":"wrong"}],"version":1}],"message":"hello-release"}}),
        ] {
            assert!(!smoke_query_matches(&bad));
        }
        let mut duplicate = good.clone();
        duplicate["data"]["rows"]
            .as_array_mut()
            .unwrap()
            .push(good["data"]["rows"][0].clone());
        assert!(!smoke_query_matches(&duplicate));
        let mut synthetic = good;
        synthetic["data"]["rows"][0]["version"] = 0.into();
        assert!(!smoke_query_matches(&synthetic));
    }
}
