use crate::error::{Error, Result};
use raft::eraftpb::{ConfState, Snapshot};
use sha2::{Digest, Sha256};
use std::{fs, io::Read, path::Path};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileEntry {
    pub name: String,
    pub size: u64,
    pub sha256: [u8; 32],
}

#[derive(Clone, Debug, PartialEq)]
pub struct Manifest {
    pub cluster: [u8; 16],
    pub index: u64,
    pub term: u64,
    pub conf: ConfState,
    pub membership: Vec<u8>,
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
        let mut b = b"GDSNAP02".to_vec();
        b.extend(self.cluster);
        b.extend(self.index.to_be_bytes());
        b.extend(self.term.to_be_bytes());
        for ids in [
            &self.conf.voters,
            &self.conf.learners,
            &self.conf.voters_outgoing,
            &self.conf.learners_next,
        ] {
            b.extend((ids.len() as u32).to_be_bytes());
            for id in ids {
                b.extend(id.to_be_bytes());
            }
        }
        b.push(u8::from(self.conf.auto_leave));
        b.extend((self.membership.len() as u32).to_be_bytes());
        b.extend(&self.membership);
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
        if &body[..8] != b"GDSNAP02" || Sha256::digest(body)[..] != *checksum {
            return Err(corrupt());
        }
        let mut r = Reader(body);
        r.take(8)?;
        if r.take(16)? != cluster {
            return Err(Error::Identity("snapshot cluster mismatch".into()));
        }
        let index = r.u64()?;
        let term = r.u64()?;
        let mut conf = ConfState::default();
        conf.set_voters(r.ids()?);
        conf.set_learners(r.ids()?);
        conf.set_voters_outgoing(r.ids()?);
        conf.set_learners_next(r.ids()?);
        conf.auto_leave = match r.take(1)?[0] {
            0 => false,
            1 => true,
            _ => return Err(corrupt()),
        };
        let len = r.u32()? as usize;
        if len > 16384 {
            return Err(corrupt());
        }
        let membership = r.take(len)?.to_vec();
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
            conf,
            membership,
            files,
        })
    }

    pub fn to_raft(&self) -> Snapshot {
        let mut s = Snapshot::default();
        s.mut_metadata().set_index(self.index);
        s.mut_metadata().set_term(self.term);
        s.mut_metadata().set_conf_state(self.conf.clone());
        s.set_data(self.encode().into());
        s
    }

    pub fn from_raft(cluster: [u8; 16], s: &Snapshot) -> Result<Self> {
        let m = Self::decode(cluster, s.get_data())?;
        let meta = s.get_metadata();
        if m.index != meta.index
            || m.term != meta.term
            || m.conf.voters != meta.get_conf_state().voters
            || m.conf.learners != meta.get_conf_state().learners
            || m.conf.voters_outgoing != meta.get_conf_state().voters_outgoing
            || m.conf.learners_next != meta.get_conf_state().learners_next
            || m.conf.auto_leave != meta.get_conf_state().auto_leave
        {
            return Err(corrupt());
        }
        Ok(m)
    }
}

struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn ids(&mut self) -> Result<Vec<u64>> {
        let count = self.u32()?;
        if count > 64 {
            return Err(corrupt());
        }
        (0..count)
            .map(|_| {
                let id = self.u64()?;
                if id == 0 { Err(corrupt()) } else { Ok(id) }
            })
            .collect()
    }
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
