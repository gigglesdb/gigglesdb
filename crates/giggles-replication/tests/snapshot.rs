use giggles_replication::{
    error::Error,
    snapshot::{Manifest, SnapshotStore, hash_dir},
};
use sha2::{Digest, Sha256};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

const CLUSTER: [u8; 16] = [74; 16];
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Root(PathBuf);
impl Root {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "snapshot-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}
impl Drop for Root {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
fn checkpoint(p: &Path) -> giggles_replication::error::Result<()> {
    assert!(!p.exists());
    fs::create_dir(p)?;
    fs::write(p.join("a"), b"0123456789abcdefghijklmnop")?;
    fs::write(p.join("b"), b"another snapshot file")?;
    fs::write(p.join("c"), b"")?;
    Ok(())
}
fn built(root: &Root) -> (SnapshotStore, Manifest) {
    let s = SnapshotStore::open(&root.0.join("built"), CLUSTER).unwrap();
    let m = s.build(10, 2, &conf(&[1, 2, 3]), &[], checkpoint).unwrap();
    (s, m)
}

#[test]
fn manifest_validation() {
    let root = Root::new();
    let (_, m) = built(&root);
    let b = m.encode();
    assert_eq!(Manifest::decode(CLUSTER, &b).unwrap(), m);
    assert!(matches!(
        Manifest::decode([0; 16], &b),
        Err(Error::Identity(_))
    ));
    let mut bad = b.clone();
    bad[30] ^= 1;
    assert!(matches!(
        Manifest::decode(CLUSTER, &bad),
        Err(Error::Corrupt(_))
    ));
    for name in ["a/b", "a\\b", "..", "", "a\0b", ".", "C:foo"] {
        let mut bad = m.clone();
        bad.files[0].name = name.into();
        assert!(matches!(
            Manifest::decode(CLUSTER, &bad.encode()),
            Err(Error::Corrupt(_))
        ));
    }
    let mut bad = m.clone();
    bad.files[1].name = bad.files[0].name.clone();
    assert!(Manifest::decode(CLUSTER, &bad.encode()).is_err());
    bad = m.clone();
    bad.files.swap(0, 1);
    assert!(Manifest::decode(CLUSTER, &bad.encode()).is_err());
    let mut trailing = b[..b.len() - 32].to_vec();
    trailing.push(0);
    trailing.extend_from_slice(&Sha256::digest(&trailing));
    assert!(Manifest::decode(CLUSTER, &trailing).is_err());
    for n in 0..b.len() {
        assert!(Manifest::decode(CLUSTER, &b[..n]).is_err());
    }
    let mut s = m.to_raft();
    assert_eq!(Manifest::from_raft(CLUSTER, &s).unwrap(), m);
    s.mut_metadata().set_index(11);
    assert!(Manifest::from_raft(CLUSTER, &s).is_err());
    s = m.to_raft();
    s.mut_metadata().set_term(3);
    assert!(Manifest::from_raft(CLUSTER, &s).is_err());
    s = m.to_raft();
    s.mut_metadata().mut_conf_state().set_voters(vec![1, 2]);
    assert!(Manifest::from_raft(CLUSTER, &s).is_err());
}

#[test]
fn build_reopen_prune_and_cleanup() {
    let root = Root::new();
    let (s, m) = built(&root);
    assert_eq!(m.files, hash_dir(&s.built_data_dir(&m)).unwrap());
    assert_eq!(s.latest(), Some(m.clone()));
    assert_eq!(s.latest_snapshot(), Some(m.to_raft()));
    assert_eq!(
        s.build(10, 2, &conf(&[]), &[], |_| panic!("already built"))
            .unwrap(),
        m
    );
    s.build(11, 2, &conf(&[1, 2, 3]), &[], checkpoint).unwrap();
    let newest = s.build(12, 2, &conf(&[1, 2, 3]), &[], checkpoint).unwrap();
    assert!(!s.built_data_dir(&m).exists());
    fs::create_dir_all(root.0.join("built/tmp-stale/data")).unwrap();
    fs::create_dir_all(root.0.join("built/stage-stale/data")).unwrap();
    let reopened = SnapshotStore::open(&root.0.join("built"), CLUSTER).unwrap();
    assert_eq!(reopened.latest(), Some(newest));
    assert!(!root.0.join("built/tmp-stale").exists());
    assert!(!root.0.join("built/stage-stale").exists());
    let failed = s.build(20, 3, &conf(&[1]), &[], |p| {
        fs::create_dir_all(p.join("nested"))?;
        Ok(())
    });
    assert!(failed.is_err());
}

#[test]
fn resumable_transfer_and_corruption() {
    let root = Root::new();
    let (source, m) = built(&root);
    let dest = SnapshotStore::open(&root.0.join("dest"), CLUSTER).unwrap();
    let mut w = dest.incoming(&m).unwrap();
    assert!(dest.take_incoming(m.index, m.term).is_none());
    assert!(w.write_chunk(3, 0, b"x").is_err());
    assert!(w.write_chunk(0, 1, b"x").is_err());
    let first = fs::read(source.built_data_dir(&m).join("a")).unwrap();
    for (i, chunk) in first[..21].chunks(7).enumerate() {
        w.write_chunk(0, (i * 7) as u64, chunk).unwrap();
    }
    drop(w);
    let mut w = dest.incoming(&m).unwrap();
    assert_eq!(w.resume_point(), (0, 21));
    let (file, offset) = w.resume_point();
    for (i, f) in m.files.iter().enumerate().skip(file as usize) {
        let bytes = fs::read(source.built_data_dir(&m).join(&f.name)).unwrap();
        let start = if i == file as usize {
            offset as usize
        } else {
            0
        };
        for (n, chunk) in bytes[start..].chunks(7).enumerate() {
            w.write_chunk(i as u32, (start + n * 7) as u64, chunk)
                .unwrap();
        }
    }
    let data = w.finish().unwrap();
    assert_eq!(dest.take_incoming(m.index, m.term), Some(data.clone()));
    for f in &m.files {
        assert_eq!(
            fs::read(data.join(&f.name)).unwrap(),
            fs::read(source.built_data_dir(&m).join(&f.name)).unwrap()
        );
    }
    let complete = dest.incoming(&m).unwrap();
    assert_eq!(complete.resume_point(), (3, 0));
    assert_eq!(complete.finish().unwrap(), data);
    dest.discard_incoming_through(m.index);
    let mut bad = dest.incoming(&m).unwrap();
    for (i, f) in m.files.iter().enumerate() {
        let mut bytes = fs::read(source.built_data_dir(&m).join(&f.name)).unwrap();
        if i == 0 {
            bytes[0] ^= 1;
        }
        bad.write_chunk(i as u32, 0, &bytes).unwrap();
    }
    assert!(matches!(bad.finish(), Err(Error::Corrupt(_))));
    assert!(!root.0.join("dest/incoming/10-2").exists());
    assert!(dest.take_incoming(10, 2).is_none());
    let w = dest.incoming(&m).unwrap();
    fs::write(root.0.join("dest/incoming/10-2/data/a"), vec![0; 100]).unwrap();
    assert_eq!(w.resume_point(), (0, 0));
    assert_eq!(
        fs::metadata(root.0.join("dest/incoming/10-2/data/a"))
            .unwrap()
            .len(),
        0
    );
    let mut other = m.clone();
    other.cluster = [0; 16];
    assert!(matches!(dest.incoming(&other), Err(Error::Identity(_))));
}

#[test]
fn readers_and_duplicate_build_remain_responsive_during_checkpoint() {
    use std::{
        sync::{Arc, mpsc},
        thread,
        time::Duration,
    };
    let root = Root::new();
    let (store, old) = built(&root);
    let store = Arc::new(store);
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let builder = store.clone();
    let job = thread::spawn(move || {
        builder.build(11, 2, &conf(&[1, 2, 3]), &[], |dir| {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            checkpoint(dir)
        })
    });
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let reader = store.clone();
    let (read_tx, read_rx) = mpsc::channel();
    let expected_old = old.clone();
    let read_job = thread::spawn(move || {
        assert_eq!(reader.latest(), Some(expected_old.clone()));
        let incoming = reader.incoming(&expected_old).unwrap();
        assert!(reader.take_incoming(10, 2).is_none());
        drop(incoming);
        // Both builds started before either published the same position.
        read_tx
            .send(reader.build(11, 2, &conf(&[1, 2, 3]), &[], checkpoint))
            .unwrap();
    });
    let observed = read_rx.recv_timeout(Duration::from_secs(5));
    // Always release the builder before asserting, including on regression failure.
    release_tx.send(()).unwrap();
    let result = job.join().unwrap().unwrap();
    read_job.join().unwrap();
    assert_eq!(observed.unwrap().unwrap(), result);
    assert_eq!(store.latest(), Some(result));
}

fn conf(v: &[u64]) -> raft::eraftpb::ConfState {
    let mut cs = raft::eraftpb::ConfState::default();
    cs.set_voters(v.to_vec());
    cs
}

#[test]
fn joint_configuration_manifest_and_bounds() {
    let root = Root::new();
    let (_, mut m) = built(&root);
    m.conf.set_learners(vec![4]);
    m.conf.set_voters_outgoing(vec![1, 2, 5]);
    m.conf.set_learners_next(vec![5]);
    m.conf.auto_leave = true;
    m.membership = b"opaque membership".to_vec();
    let b = m.encode();
    assert_eq!(&b[..8], b"GDSNAP02");
    assert_eq!(Manifest::decode(CLUSTER, &b).unwrap(), m);
    assert_eq!(Manifest::from_raft(CLUSTER, &m.to_raft()).unwrap(), m);
    let mut snap = m.to_raft();
    snap.mut_metadata().mut_conf_state().set_learners(vec![6]);
    assert!(Manifest::from_raft(CLUSTER, &snap).is_err());
    let rehash = |mut b: Vec<u8>| {
        b.truncate(b.len() - 32);
        b.extend_from_slice(&Sha256::digest(&b));
        b
    };
    let mut bad = b.clone();
    bad[..8].copy_from_slice(b"GDSNAP01");
    assert!(Manifest::decode(CLUSTER, &rehash(bad)).is_err());
    let mut bad = b;
    let auto_offset = 40
        + 16
        + 8 * (m.conf.voters.len()
            + m.conf.learners.len()
            + m.conf.voters_outgoing.len()
            + m.conf.learners_next.len());
    bad[auto_offset] = 2;
    assert!(Manifest::decode(CLUSTER, &rehash(bad)).is_err());
    m.membership = vec![0; 16384];
    assert_eq!(Manifest::decode(CLUSTER, &m.encode()).unwrap(), m);
    m.membership.push(0);
    assert!(Manifest::decode(CLUSTER, &m.encode()).is_err());
    m.membership.clear();
    for field in 0..4 {
        let mut bad = m.clone();
        let ids = match field {
            0 => &mut bad.conf.voters,
            1 => &mut bad.conf.learners,
            2 => &mut bad.conf.voters_outgoing,
            _ => &mut bad.conf.learners_next,
        };
        *ids = vec![1; 65];
        assert!(Manifest::decode(CLUSTER, &bad.encode()).is_err());
        match field {
            0 => bad.conf.set_voters(vec![0]),
            1 => bad.conf.set_learners(vec![0]),
            2 => bad.conf.set_voters_outgoing(vec![0]),
            _ => bad.conf.set_learners_next(vec![0]),
        }
        assert!(Manifest::decode(CLUSTER, &bad.encode()).is_err());
    }
}
