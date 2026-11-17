use super::common::net_cluster::{NetCluster, until};
use giggles_core::relational::Value;
use giggles_replication::{failpoint, log_store::LogStore, sink::SnapshotPolicy};
use rocksdb::{DB, IteratorMode, Options};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::Write,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    path::Path,
    time::{Duration, Instant},
};

const WRITE: &[&str] = &[
    "log.persist.before",
    "log.persist.after",
    "node.commit.persisted",
    "node.apply.before",
    "node.apply.after",
    "log.compact.before",
    "snapshot.build.data",
    "snapshot.build.manifest",
];
const INSTALL: &[&str] = &[
    "snapshot.receive.chunk",
    "snapshot.receive.finish",
    "snapshot.install.pending",
    "slot.install.old",
    "slot.install.new",
    "snapshot.install.state",
];

fn store_digest(data_dir: &Path) -> (BTreeMap<String, u64>, [u8; 32]) {
    let options = Options::default();
    let mut families = DB::list_cf(&options, data_dir).unwrap();
    families.sort();
    let db = DB::open_cf_for_read_only(&options, data_dir, &families, false).unwrap();
    let mut counts = BTreeMap::new();
    let mut hash = Sha256::new();
    for name in families {
        hash.update(name.as_bytes());
        let cf = db.cf_handle(&name).unwrap();
        let mut count = 0;
        for entry in db.iterator_cf(cf, IteratorMode::Start) {
            let (key, value) = entry.unwrap();
            hash.update((key.len() as u64).to_be_bytes());
            hash.update(&key);
            hash.update((value.len() as u64).to_be_bytes());
            hash.update(&value);
            count += 1;
        }
        counts.insert(name, count);
    }
    (counts, hash.finalize().into())
}

fn emit(line: &str) {
    println!("{line}");
    if let Some(path) = std::env::var_os("R4_RECOVERY_OUT") {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        writeln!(file, "{line}").unwrap();
    }
}

fn insert(c: &NetCluster, next: &mut i64, acknowledged: &mut BTreeSet<i64>) {
    c.execute(&format!(
        "INSERT INTO t VALUES ({next}) ON CONFLICT DO NOTHING"
    ));
    acknowledged.insert(*next);
    *next += 1;
}

fn run(group: u8, points: &[&'static str]) {
    let mut missed = Vec::new();
    for (index, &point) in points.iter().enumerate() {
        let started = Instant::now();
        let mut cluster = [0; 16];
        cluster[0] = 0xC0 + group;
        cluster[1] = index as u8;
        let mut c = NetCluster::new(
            "recovery-matrix",
            cluster,
            SnapshotPolicy {
                every_entries: 20,
                margin: 5,
            },
        );
        let mut crashed = None;
        let mut never_fired = false;
        let mut unidentified = false;
        let mut leader_fallback = false;
        let mut acknowledged = BTreeSet::new();
        let mut next = 0;
        let mut count = None;
        let mut digest = None;
        let result = catch_unwind(AssertUnwindSafe(|| {
            c.execute("CREATE TABLE IF NOT EXISTS t(id bigint PRIMARY KEY)");
            let follower = if group == 1 {
                for _ in 0..60 {
                    insert(&c, &mut next, &mut acknowledged);
                }
                c.converged();
                let (leader, engine) = c.leader();
                drop(engine);
                until(|| {
                    fs::read_dir(&c.configs[leader].snap_dir)
                        .unwrap()
                        .flatten()
                        .any(|e| {
                            !e.file_name().to_string_lossy().starts_with("tmp-")
                                && e.path().join("MANIFEST").is_file()
                        })
                });
                let _ = c.nodes[leader].as_ref().unwrap().handle.read_barrier();
                c.stop(leader);
                let config = &c.configs[leader];
                let log =
                    LogStore::open(&config.log_dir, cluster, config.transport.node_id).unwrap();
                assert!(log.snapshot_meta().unwrap().0 > 0, "{point}: no compaction");
                drop(log);
                c.start(leader);
                c.converged();
                let (leader, engine) = c.leader();
                drop(engine);
                let f = (leader + 1) % 3;
                c.stop(f);
                let config = &c.configs[f];
                for dir in [&config.data_dir, &config.log_dir, &config.snap_dir] {
                    fs::remove_dir_all(dir).unwrap();
                }
                Some(f)
            } else {
                None
            };
            failpoint::arm(cluster, point, if follower.is_some() { 1 } else { 3 });
            if let Some(f) = follower {
                c.start(f);
            }
            let deadline = Instant::now() + Duration::from_secs(60);
            while failpoint::fired(cluster).is_empty() {
                if Instant::now() >= deadline {
                    never_fired = true;
                    panic!(
                        "{point}: did not fire within 60s; hits={}",
                        failpoint::hits(cluster, point)
                    );
                }
                insert(&c, &mut next, &mut acknowledged);
            }
            let fired = failpoint::fired(cluster);
            assert_eq!(fired[0].0, point);
            let snapshot_build = point.starts_with("snapshot.build.");
            let leader_at_fire = snapshot_build.then(|| c.leader().0);
            let staging_node = snapshot_build
                .then(|| {
                    c.configs.iter().position(|config| {
                        fs::read_dir(&config.snap_dir).unwrap().any(|entry| {
                            let entry = entry.unwrap();
                            let name = entry.file_name();
                            let name = name.to_string_lossy();
                            let built = name.split_once('-').is_some_and(|(index, term)| {
                                index.parse::<u64>().is_ok() && term.parse::<u64>().is_ok()
                            });
                            entry.file_type().unwrap().is_dir()
                                && (name.starts_with("tmp-")
                                    || (point == "snapshot.build.data"
                                        && built
                                        && !entry.path().join("MANIFEST").is_file()))
                        })
                    })
                })
                .flatten();
            let victim = fired[0]
                .1
                .map(|id| id as usize - 1)
                .or_else(|| {
                    c.nodes
                        .iter()
                        .position(|n| n.as_ref().is_some_and(|n| n.handle.is_stopped()))
                })
                .or(staging_node)
                .or_else(|| leader_at_fire.inspect(|_| leader_fallback = true))
                .or(follower)
                .unwrap_or_else(|| {
                    unidentified = true;
                    panic!("{point}: hook fired without a node id or stopped replica");
                });
            crashed = Some(victim + 1);
            if let Some(f) = follower {
                assert_eq!(victim, f, "{point}: wrong receiver");
            }
            failpoint::disarm(cluster);
            c.kill(victim);
            c.start(victim);
            for _ in 0..20 {
                insert(&c, &mut next, &mut acknowledged);
            }
            c.converged();
            let leader = {
                let (leader, engine) = c.leader();
                let mut session = engine.admin().sql_session();
                let result = session
                    .execute("SELECT count(*) FROM t", &[], None)
                    .unwrap();
                count = Some(result.rows[0].values[0].clone());
                let result = session
                    .execute("SELECT id FROM t ORDER BY id", &[], None)
                    .unwrap();
                let present: BTreeSet<_> = result
                    .rows
                    .iter()
                    .map(|row| {
                        let Value::Int(id) = row.values[0] else {
                            panic!("non-integer id")
                        };
                        id
                    })
                    .collect();
                assert!(
                    acknowledged.is_subset(&present),
                    "{point}: acknowledged row lost"
                );
                leader
            };
            // Stop followers first so shutdown cannot commit a new election no-op.
            for i in (0..3)
                .filter(|i| *i != leader)
                .chain(std::iter::once(leader))
            {
                c.stop(i);
            }
            if snapshot_build {
                assert!(
                    fs::read_dir(&c.configs[victim].snap_dir)
                        .unwrap()
                        .all(|entry| !entry
                            .unwrap()
                            .file_name()
                            .to_string_lossy()
                            .starts_with("tmp-")),
                    "{point}: staging directory remains after restart and convergence"
                );
            }
            let stores: Vec<_> = c
                .configs
                .iter()
                .map(|config| store_digest(&config.data_dir))
                .collect();
            digest = Some(stores[0].clone());
            assert_eq!(stores[0], stores[1], "{point}: stores 1 and 2 differ");
            assert_eq!(stores[0], stores[2], "{point}: stores 1 and 3 differ");
        }));
        failpoint::disarm(cluster);
        let (counts, prefix) = digest.map_or_else(
            || ("unavailable".into(), "unavailable".into()),
            |(counts, hash)| (format!("{counts:?}"), hex::encode(&hash[..8])),
        );
        emit(&format!(
            "{point} node={crashed:?} leader_fallback={leader_fallback} acknowledged={} count={count:?} cf={counts} sha256={prefix} {} elapsed={:.2}s",
            acknowledged.len(),
            if result.is_ok() { "PASS" } else { "FAIL" },
            started.elapsed().as_secs_f64()
        ));
        if let Err(error) = result {
            if never_fired || unidentified {
                missed.push(point);
            } else {
                resume_unwind(error);
            }
        }
    }
    assert!(missed.is_empty(), "failpoints not exercised: {missed:?}");
}

#[test]
fn write_path() {
    run(0, WRITE);
}
#[test]
fn snapshot_receive_install() {
    run(1, INSTALL);
}
#[test]
fn every_non_membership_point_is_covered() {
    for &point in failpoint::POINTS {
        assert!(
            WRITE.contains(&point)
                || INSTALL.contains(&point)
                || point.starts_with("node.conf.")
                || super::membership_crash::ADMIN.contains(&point) // membership_crash.rs
                || point.starts_with("range.split."), // split_lifecycle.rs
            "uncovered failpoint: {point}"
        );
    }
    for &point in WRITE.iter().chain(INSTALL) {
        assert!(failpoint::POINTS.contains(&point));
    }
}
