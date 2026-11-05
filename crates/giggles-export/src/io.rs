use super::*;
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::PathBuf,
};
pub(crate) fn read(path: &Path, b: Bounds) -> Result<Vec<u8>> {
    b.check()?;
    let f = File::open(path)?;
    if f.metadata()?.len() > b.file_bytes as u64 {
        return Err(Error::Limit);
    }
    let mut out = Vec::new();
    f.take(b.file_bytes as u64 + 1).read_to_end(&mut out)?;
    if out.len() > b.file_bytes {
        return Err(Error::Limit);
    }
    Ok(out)
}
fn parent(path: &Path) -> &Path {
    path.parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
}
fn sync_parent(path: &Path) -> Result<()> {
    #[cfg(unix)]
    File::open(parent(path))?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}
fn stage(path: &Path) -> Result<(PathBuf, File)> {
    let mut id = [0u8; 16];
    getrandom::fill(&mut id).map_err(|_| invalid("random staging id"))?;
    let name = path
        .file_name()
        .ok_or_else(|| invalid("destination filename"))?
        .to_string_lossy();
    let p = parent(path).join(format!(".{name}.{}.staging", format::hex(&id)));
    let f = OpenOptions::new().write(true).create_new(true).open(&p)?;
    Ok((p, f))
}
pub(crate) fn publish(path: &Path, bytes: &[u8], b: Bounds, hook: Hook<'_>) -> Result<()> {
    if path.try_exists()? {
        return Err(Error::Exists);
    }
    let (staged, mut f) = stage(path)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    drop(f);
    // A retained staging file is never interpreted as a published export.
    let check = read(&staged, b)?;
    decode(&check, b)?;
    if check != bytes {
        return Err(Error::Integrity);
    }
    hook(Point::StageSynced)?;
    fs::hard_link(&staged, path)?;
    sync_parent(path)?;
    hook(Point::ExportPublished)?;
    Ok(())
}
pub(crate) fn marker(target: &Path, report: &Report) -> Result<()> {
    let path = target.join("logical-import-complete.json");
    let (staged, mut f) = stage(&path)?;
    let bytes = serde_json::to_vec(report).map_err(|_| invalid("marker JSON"))?;
    f.write_all(&bytes)?;
    f.sync_all()?;
    drop(f);
    fs::hard_link(staged, &path)?;
    sync_parent(&path)
}
