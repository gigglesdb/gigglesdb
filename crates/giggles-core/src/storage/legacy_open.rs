//! Keep redb's recovery/header/Drop maintenance off disk until admission succeeds.
//! The original FileBackend holds its exclusive lock throughout; no reopen race.
use redb::{StorageBackend, backends::FileBackend};
use std::{
    io,
    sync::{Arc, Mutex},
};

#[derive(Debug)]
enum Op {
    Resize(u64),
    Write(u64, Vec<u8>),
    Sync,
}
#[derive(Debug)]
struct Pending {
    len: u64,
    original_len: u64,
    ops: Vec<Op>,
}
#[derive(Debug, Clone)]
pub(super) struct Admission {
    file: Arc<FileBackend>,
    pending: Arc<Mutex<Option<Pending>>>,
}
impl Admission {
    pub(super) fn new(file: std::fs::File) -> Result<Self, redb::DatabaseError> {
        let file = Arc::new(FileBackend::new(file)?);
        let len = file.len()?;
        // create_with_backend must never initialize an empty refused database.
        if len == 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "empty legacy database").into());
        }
        Ok(Self {
            file,
            pending: Arc::new(Mutex::new(Some(Pending {
                len,
                original_len: len,
                ops: vec![],
            }))),
        })
    }
    pub(super) fn accept(&self) -> io::Result<()> {
        let mut guard = self.pending.lock().unwrap();
        let pending = guard.as_ref().unwrap();
        // Preserve write and durability ordering, including recovery commits.
        for op in &pending.ops {
            match op {
                Op::Resize(len) => self.file.set_len(*len)?,
                Op::Write(offset, bytes) => self.file.write(*offset, bytes)?,
                Op::Sync => self.file.sync_data()?,
            }
        }
        *guard = None;
        Ok(())
    }
}
impl StorageBackend for Admission {
    fn len(&self) -> io::Result<u64> {
        match self.pending.lock().unwrap().as_ref() {
            Some(p) => Ok(p.len),
            None => self.file.len(),
        }
    }
    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        let guard = self.pending.lock().unwrap();
        let Some(p) = guard.as_ref() else {
            return self.file.read(offset, out);
        };
        let end = offset
            .checked_add(out.len() as u64)
            .filter(|end| *end <= p.len)
            .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))?;
        out.fill(0);
        let n = p.original_len.min(end).saturating_sub(offset) as usize;
        if n != 0 {
            self.file.read(offset, &mut out[..n])?;
        }
        for op in &p.ops {
            match op {
                Op::Resize(len) if *len < end => {
                    out[len.saturating_sub(offset) as usize..].fill(0);
                }
                Op::Write(start, bytes) => {
                    let lo = offset.max(*start);
                    let hi = end.min(*start + bytes.len() as u64);
                    if lo < hi {
                        out[(lo - offset) as usize..(hi - offset) as usize]
                            .copy_from_slice(&bytes[(lo - start) as usize..(hi - start) as usize]);
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        match self.pending.lock().unwrap().as_mut() {
            Some(p) => {
                p.len = len;
                p.ops.push(Op::Resize(len));
                Ok(())
            }
            None => self.file.set_len(len),
        }
    }
    fn write(&self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        match self.pending.lock().unwrap().as_mut() {
            Some(p) => {
                let end = offset
                    .checked_add(bytes.len() as u64)
                    .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
                p.len = p.len.max(end);
                p.ops.push(Op::Write(offset, bytes.to_vec()));
                Ok(())
            }
            None => self.file.write(offset, bytes),
        }
    }
    fn sync_data(&self) -> io::Result<()> {
        match self.pending.lock().unwrap().as_mut() {
            Some(p) => {
                p.ops.push(Op::Sync);
                Ok(())
            }
            None => self.file.sync_data(),
        }
    }
    fn close(&self) -> io::Result<()> {
        self.file.close()
    }
}
