use super::manifest::{Manifest, corrupt, hash_dir};
use crate::error::{Error, Result};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

static NEXT: AtomicU64 = AtomicU64::new(0);
pub struct SnapshotStore {
    root: PathBuf,
    cluster: [u8; 16],
    state: Arc<Mutex<Option<Manifest>>>,
}

fn key(m: &Manifest) -> (u64, u64) {
    (m.index, m.term)
}
fn label(index: u64, term: u64) -> String {
    format!("{index}-{term}")
}
fn parse(name: &str) -> Option<(u64, u64)> {
    let (i, t) = name.split_once('-')?;
    let pair = (i.parse().ok()?, t.parse().ok()?);
    (label(pair.0, pair.1) == name).then_some(pair)
}
fn publish(dir: &Path, m: &Manifest) -> Result<()> {
    let tmp = dir.join("MANIFEST.tmp");
    let mut f = fs::File::create(&tmp)?;
    f.write_all(&m.encode())?;
    f.sync_all()?;
    drop(f);
    fs::rename(tmp, dir.join("MANIFEST"))?;
    Ok(())
}

impl SnapshotStore {
    pub fn open(snap_dir: &Path, cluster: [u8; 16]) -> Result<Self> {
        fs::create_dir_all(snap_dir.join("incoming"))?;
        for e in fs::read_dir(snap_dir)? {
            let e = e?;
            if e.file_type()?.is_dir()
                && ["tmp-", "stage-"]
                    .iter()
                    .any(|prefix| e.file_name().to_string_lossy().starts_with(prefix))
            {
                fs::remove_dir_all(e.path())?;
            }
        }
        let s = Self {
            root: snap_dir.into(),
            cluster,
            state: Arc::new(Mutex::new(None)),
        };
        *s.state.lock().unwrap() = s.built()?.pop().map(|(_, m)| m);
        Ok(s)
    }

    fn built(&self) -> Result<Vec<(PathBuf, Manifest)>> {
        let mut found = Vec::new();
        for e in fs::read_dir(&self.root)? {
            let e = e?;
            let Some(k) = parse(&e.file_name().to_string_lossy()) else {
                continue;
            };
            if !e.file_type()?.is_dir() || !e.path().join("MANIFEST").exists() {
                continue;
            }
            let m = Manifest::decode(self.cluster, &fs::read(e.path().join("MANIFEST"))?)?;
            if key(&m) != k {
                return Err(corrupt());
            }
            found.push((e.path(), m));
        }
        found.sort_by_key(|(_, m)| key(m));
        Ok(found)
    }

    pub fn build(
        &self,
        index: u64,
        term: u64,
        conf: &raft::eraftpb::ConfState,
        membership: &[u8],
        checkpoint: impl FnOnce(&Path) -> Result<()>,
    ) -> Result<Manifest> {
        let dest = self.root.join(label(index, term));
        {
            let _guard = self.state.lock().unwrap();
            if dest.join("MANIFEST").exists() {
                return Manifest::decode(self.cluster, &fs::read(dest.join("MANIFEST"))?);
            }
        }
        let tmp = self.root.join(format!(
            "tmp-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&tmp)?;
        let result = (|| {
            checkpoint(&tmp.join("data"))?;
            let m = Manifest {
                cluster: self.cluster,
                index,
                term,
                conf: conf.clone(),
                membership: membership.to_vec(),
                files: hash_dir(&tmp.join("data"))?,
            };
            // Apply the same size and name rules to locally produced manifests.
            Manifest::decode(self.cluster, &m.encode())?;
            publish(&tmp, &m)?;
            let mut latest = self.state.lock().unwrap();
            // A concurrent builder may have published this position while we hashed.
            if dest.join("MANIFEST").exists() {
                let existing = Manifest::decode(self.cluster, &fs::read(dest.join("MANIFEST"))?)?;
                fs::remove_dir_all(&tmp)?;
                return Ok(existing);
            }
            if dest.exists() {
                fs::remove_dir_all(&dest)?;
            }
            fs::rename(&tmp, &dest)?;
            let built = self.built()?;
            *latest = built.last().map(|(_, m)| m.clone());
            for (path, _) in built.iter().take(built.len().saturating_sub(2)) {
                fs::remove_dir_all(path)?;
            }
            Ok(m)
        })();
        if result.is_err() && tmp.exists() {
            let _ = fs::remove_dir_all(tmp);
        }
        result
    }

    /// Checkpoint staging location, cleaned on the next open after a crash.
    pub fn staging_dir(&self, index: u64, term: u64) -> PathBuf {
        self.root.join(format!("stage-{index}-{term}"))
    }

    pub fn latest(&self) -> Option<Manifest> {
        self.state.lock().unwrap().clone()
    }
    pub fn latest_snapshot(&self) -> Option<raft::eraftpb::Snapshot> {
        self.latest().map(|m| m.to_raft())
    }
    pub fn built_data_dir(&self, m: &Manifest) -> PathBuf {
        self.root.join(label(m.index, m.term)).join("data")
    }

    pub fn incoming(&self, m: &Manifest) -> Result<IncomingWriter> {
        let _guard = self.state.lock().unwrap();
        Manifest::decode(self.cluster, &m.encode())?;
        let dir = self.root.join("incoming").join(label(m.index, m.term));
        if dir.join("MANIFEST").exists()
            && Manifest::decode(self.cluster, &fs::read(dir.join("MANIFEST"))?)? != *m
        {
            return Err(corrupt());
        }
        fs::create_dir_all(dir.join("data"))?;
        Ok(IncomingWriter {
            dir,
            manifest: m.clone(),
            state: self.state.clone(),
        })
    }

    pub fn take_incoming(&self, index: u64, term: u64) -> Option<PathBuf> {
        let _guard = self.state.lock().unwrap();
        let dir = self.root.join("incoming").join(label(index, term));
        (dir.join("MANIFEST").is_file() && dir.join("data").is_dir()).then(|| dir.join("data"))
    }

    pub fn discard_incoming_through(&self, index: u64) {
        let _guard = self.state.lock().unwrap();
        let Ok(entries) = fs::read_dir(self.root.join("incoming")) else {
            return;
        };
        for e in entries.flatten() {
            if parse(&e.file_name().to_string_lossy()).is_some_and(|(i, _)| i <= index) {
                let _ = fs::remove_dir_all(e.path());
            }
        }
    }
}

pub struct IncomingWriter {
    dir: PathBuf,
    manifest: Manifest,
    state: Arc<Mutex<Option<Manifest>>>,
}
impl IncomingWriter {
    pub fn resume_point(&self) -> (u32, u64) {
        let _guard = self.state.lock().unwrap();
        for (i, f) in self.manifest.files.iter().enumerate() {
            let path = self.dir.join("data").join(&f.name);
            let len = fs::metadata(&path).map_or(0, |m| m.len());
            if len > f.size {
                if let Ok(file) = OpenOptions::new().write(true).open(path) {
                    let _ = file.set_len(0);
                }
                return (i as u32, 0);
            }
            if len < f.size {
                return (i as u32, len);
            }
        }
        (self.manifest.files.len() as u32, 0)
    }

    pub fn write_chunk(&mut self, file: u32, offset: u64, data: &[u8]) -> Result<()> {
        let _guard = self.state.lock().unwrap();
        let f = self.manifest.files.get(file as usize).ok_or_else(corrupt)?;
        if self.dir.join("MANIFEST").exists() {
            return Err(corrupt());
        }
        let path = self.dir.join("data").join(&f.name);
        let len = match fs::metadata(&path) {
            Ok(m) => m.len(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
            Err(e) => return Err(e.into()),
        };
        if offset != len
            || offset
                .checked_add(data.len() as u64)
                .is_none_or(|n| n > f.size)
        {
            return Err(corrupt());
        }
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?
            .write_all(data)?;
        Ok(())
    }

    pub fn finish(self) -> Result<PathBuf> {
        let _guard = self.state.lock().unwrap();
        let data = self.dir.join("data");
        let result = (|| {
            // Empty files may never receive a chunk.
            for f in &self.manifest.files {
                let path = data.join(&f.name);
                if f.size == 0 && !path.exists() {
                    fs::File::create(&path)?;
                }
            }
            if hash_dir(&data)? != self.manifest.files {
                return Err(corrupt());
            }
            for f in &self.manifest.files {
                OpenOptions::new()
                    .write(true)
                    .open(data.join(&f.name))?
                    .sync_all()?;
            }
            if !self.dir.join("MANIFEST").exists() {
                publish(&self.dir, &self.manifest)?;
            }
            Ok(data)
        })();
        if result.is_err() {
            fs::remove_dir_all(&self.dir)?;
            return Err(Error::Corrupt(
                "snapshot transfer verification failed".into(),
            ));
        }
        result
    }
}
