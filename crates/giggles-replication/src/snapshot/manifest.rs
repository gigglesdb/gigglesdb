use crate::error::{Error, Result};
use raft::eraftpb::Snapshot;
use sha2::{Digest, Sha256};
use std::{fs, io::Read, path::Path};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileEntry {
    pub name: String,
    pub size: u64,
    pub sha256: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
    pub cluster: [u8; 16],
    pub index: u64,
    pub term: u64,
    pub voters: Vec<u64>,
    pub files: Vec<FileEntry>,
}

pub(super) fn corrupt() -> Error {
    Error::Corrupt("invalid snapshot manifest or data".into())
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 255
        && name != "."
        && !name.contains(['/', '\\', '\0', ':'])
        && !name.contains("..")
}

impl Manifest {
    pub fn encode(&self) -> Vec<u8> {
        let mut b = b"GDSNAP01".to_vec();
        b.extend(self.cluster);
        b.extend(self.index.to_be_bytes());
        b.extend(self.term.to_be_bytes());
        b.extend((self.voters.len() as u32).to_be_bytes());
        for v in &self.voters {
            b.extend(v.to_be_bytes());
        }
        b.extend((self.files.len() as u32).to_be_bytes());
        for f in &self.files {
            b.extend((f.name.len() as u16).to_be_bytes());
            b.extend(f.name.as_bytes());
            b.extend(f.size.to_be_bytes());
            b.extend(f.sha256);
        }
        b.extend_from_slice(&Sha256::digest(&b));
        b
    }

    pub fn decode(cluster: [u8; 16], bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 80 || bytes.len() > 65536 {
            return Err(corrupt());
        }
        let (body, checksum) = bytes.split_at(bytes.len() - 32);
        if &body[..8] != b"GDSNAP01" || Sha256::digest(body)[..] != *checksum {
            return Err(corrupt());
        }
        let mut r = Reader(body);
        r.take(8)?;
        if r.take(16)? != cluster {
            return Err(Error::Identity("snapshot cluster mismatch".into()));
        }
        let index = r.u64()?;
        let term = r.u64()?;
        let mut voters = Vec::new();
        for _ in 0..r.u32()? {
            voters.push(r.u64()?);
        }
        let mut files: Vec<FileEntry> = Vec::new();
        for _ in 0..r.u32()? {
            let len = u16::from_be_bytes(r.take(2)?.try_into().unwrap()) as usize;
            let name = std::str::from_utf8(r.take(len)?)
                .map_err(|_| corrupt())?
                .to_owned();
            if !valid_name(&name) || files.last().is_some_and(|f| f.name >= name) {
                return Err(corrupt());
            }
            files.push(FileEntry {
                name,
                size: r.u64()?,
                sha256: r.take(32)?.try_into().unwrap(),
            });
        }
        if !r.0.is_empty() {
            return Err(corrupt());
        }
        Ok(Self {
            cluster,
            index,
            term,
            voters,
            files,
        })
    }

    pub fn to_raft(&self) -> Snapshot {
        let mut s = Snapshot::default();
        s.mut_metadata().set_index(self.index);
        s.mut_metadata().set_term(self.term);
        s.mut_metadata()
            .mut_conf_state()
            .set_voters(self.voters.clone());
        s.set_data(self.encode().into());
        s
    }

    pub fn from_raft(cluster: [u8; 16], s: &Snapshot) -> Result<Self> {
        let m = Self::decode(cluster, s.get_data())?;
        let meta = s.get_metadata();
        if m.index != meta.index || m.term != meta.term || m.voters != meta.get_conf_state().voters
        {
            return Err(corrupt());
        }
        Ok(m)
    }
}

struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if n > self.0.len() {
            return Err(corrupt());
        }
        let (v, tail) = self.0.split_at(n);
        self.0 = tail;
        Ok(v)
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
}

pub(super) fn hash_file(path: &Path) -> Result<[u8; 32]> {
    let mut f = fs::File::open(path)?;
    let mut hash = Sha256::new();
    let mut buf = [0; 65536];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hash.update(&buf[..n]);
    }
    Ok(hash.finalize().into())
}

pub fn hash_dir(dir: &Path) -> Result<Vec<FileEntry>> {
    let mut files = Vec::new();
    for e in fs::read_dir(dir)? {
        let e = e?;
        let name = e.file_name().into_string().map_err(|_| corrupt())?;
        if !e.file_type()?.is_file() || !valid_name(&name) {
            return Err(corrupt());
        }
        files.push(FileEntry {
            name,
            size: e.metadata()?.len(),
            sha256: hash_file(&e.path())?,
        });
    }
    files.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(files)
}
