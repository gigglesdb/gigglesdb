use crate::{OpsError, Result};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Component, Path, PathBuf},
};

fn reparse(m: &fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        m.file_attributes() & 0x400 != 0
    }
    #[cfg(not(windows))]
    {
        let _ = m;
        false
    }
}
pub(crate) fn checked(path: &Path) -> Result<PathBuf> {
    let abs = std::path::absolute(path)?;
    let mut p = PathBuf::new();
    for c in abs.components() {
        if matches!(c, Component::ParentDir) {
            return Err(OpsError::UnsafePath);
        }
        p.push(c);
        let m = fs::symlink_metadata(&p)?;
        if m.file_type().is_symlink() || reparse(&m) {
            return Err(OpsError::UnsafePath);
        }
    }
    Ok(abs)
}
pub(crate) fn absent(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => Err(OpsError::Exists),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            checked(path.parent().ok_or(OpsError::UnsafePath)?)?;
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}
pub(crate) fn open(path: &Path) -> Result<File> {
    checked(path)?;
    if !fs::symlink_metadata(path)?.is_file() {
        return Err(OpsError::UnsafePath);
    }
    let mut o = OpenOptions::new();
    o.read(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        o.share_mode(0).custom_flags(0x00200000);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        o.custom_flags(libc::O_NOFOLLOW);
    }
    let f = o.open(path)?;
    if !f.metadata()?.is_file() || reparse(&f.metadata()?) {
        return Err(OpsError::UnsafePath);
    }
    Ok(f)
}
pub(crate) fn admission(root: &Path) -> Result<File> {
    let path = root.join(".ops-admission");
    if fs::symlink_metadata(&path).is_ok() {
        checked(&path)?;
        if !fs::symlink_metadata(&path)?.is_file() {
            return Err(OpsError::UnsafePath);
        }
    } else {
        absent(&path)?;
    }
    let mut o = OpenOptions::new();
    o.read(true).write(true).create(true).truncate(false);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        o.custom_flags(0x00200000);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        o.custom_flags(libc::O_NOFOLLOW);
    }
    let f = o.open(&path)?;
    if !f.metadata()?.is_file() || reparse(&f.metadata()?) {
        return Err(OpsError::UnsafePath);
    }
    f.try_lock().map_err(|_| OpsError::Busy)?;
    Ok(f)
}
pub(crate) fn read(path: &Path, max: u64) -> Result<Vec<u8>> {
    let f = open(path)?;
    if f.metadata()?.len() > max {
        return Err(OpsError::Limit);
    }
    let mut b = Vec::new();
    f.take(max + 1).read_to_end(&mut b)?;
    if b.len() as u64 > max {
        return Err(OpsError::Limit);
    }
    Ok(b)
}
pub(crate) fn write_json(path: &Path, value: &impl serde::Serialize) -> Result<()> {
    absent(path)?;
    let b = serde_json::to_vec_pretty(value).map_err(|_| OpsError::Invalid)?;
    if b.len() as u64 > crate::MAX_MANIFEST {
        return Err(OpsError::Limit);
    }
    let mut f = OpenOptions::new().write(true).create_new(true).open(path)?;
    f.write_all(&b)?;
    f.sync_all()?;
    Ok(())
}
pub(crate) fn hash(path: &Path, max: u64) -> Result<(u64, String, u32)> {
    let mut f = open(path)?;
    let len = f.metadata()?.len();
    if len > max {
        return Err(OpsError::Limit);
    }
    if len < 52 {
        return Err(OpsError::Invalid);
    }
    let mut header = [0u8; 12];
    f.read_exact(&mut header)?;
    if &header[..8] != b"GDEXP001" {
        return Err(OpsError::Invalid);
    }
    let version = u32::from_be_bytes(header[8..12].try_into().expect("fixed header"));
    let mut h = Sha256::new();
    h.update(header);
    let mut total = 12u64;
    let mut buf = [0u8; 8192];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        total += n as u64;
        if total > max {
            return Err(OpsError::Limit);
        }
        h.update(&buf[..n]);
    }
    if total != len {
        return Err(OpsError::Invalid);
    }
    Ok((len, format!("{:x}", h.finalize()), version))
}
pub(crate) fn nonce() -> Result<String> {
    let mut b = [0u8; 16];
    getrandom::fill(&mut b).map_err(|_| OpsError::Invalid)?;
    Ok(format!("{:032x}", u128::from_be_bytes(b)))
}
