//! Gateways running SQL through one in-process cluster (GD-0075 R5-43, R5-51/R5-56 wiring).
use super::local::LocalCluster;
use super::*;
use crate::relational::{StatementClock, Value};
use crate::storage::range_api::{RangeSpace, range_key};
use std::collections::BTreeSet;
use std::time::Duration;

fn clock() -> StatementClock {
    StatementClock {
        unavailable: None,
        transaction_us: 100,
        statement_us: 100,
        zone: Some(giggles_pg_types::temporal::SessionZone::Fixed(0)),
        wall: None,
    }
}

/// Splits so each of the first tables' rows and index entries sit in their own ranges.
fn splits() -> Vec<Vec<u8>> {
    let mut splits = Vec::new();
    for space in [RangeSpace::Rows, RangeSpace::Index] {
        for id in 1..=12_u64 {
            splits.push(range_key(space, &id.to_be_bytes()));
        }
    }
    splits.sort();
    splits
}

fn gateway(cluster: &Arc<LocalCluster>, node: u16) -> Gateway {
    let config = ClusterConfig {
        node,
        heartbeat: Duration::from_millis(200),
        ..ClusterConfig::default()
    };
    let deadlock = DeadlockConfig {
        deadlock_timeout: Duration::from_millis(200),
        lock_wait_backstop: Duration::from_secs(20),
        tick: Duration::from_millis(50),
    };
    let gateway = attach(
        cluster.clone(),
        config,
        Some((cluster.clone(), deadlock)),
        [9; 16],
        Limits::default(),
    )
    .unwrap();
    cluster.register_gateway(u64::from(node), gateway.services.gateway_service());
    gateway
}

fn ints(rows: &crate::relational::SqlResult) -> Vec<Vec<i64>> {
    rows.rows
        .iter()
        .map(|row| {
            row.values
                .iter()
                .map(|v| match v {
                    Value::Int(i) => *i,
                    other => panic!("not an int: {other:?}"),
                })
                .collect()
        })
        .collect()
}

#[test]
fn two_gateways_share_tables_and_never_repeat_ids_or_sequence_values() {
    let cluster = LocalCluster::new(&splits());
    let a = gateway(&cluster, 1);
    let mut sa = a.admin.sql_session();
    sa.execute_at(
        "CREATE TABLE t(id SERIAL PRIMARY KEY, g INT)",
        &[],
        None,
        &clock(),
    )
    .unwrap();
    sa.execute_at("CREATE TABLE h(g INT, n INT)", &[], None, &clock())
        .unwrap();
    // B attaches after A's DDL and loads the catalog from the system range.
    let b = gateway(&cluster, 2);
    let mut sb = b.admin.sql_session();
    for n in 0..20 {
        sa.execute_at("INSERT INTO t(g) VALUES (1)", &[], None, &clock())
            .unwrap();
        sb.execute_at("INSERT INTO t(g) VALUES (2)", &[], None, &clock())
            .unwrap();
        sa.execute_at(
            &format!("INSERT INTO h VALUES (1, {n})"),
            &[],
            None,
            &clock(),
        )
        .unwrap();
        sb.execute_at(
            &format!("INSERT INTO h VALUES (2, {n})"),
            &[],
            None,
            &clock(),
        )
        .unwrap();
    }
    for session in [&mut sa, &mut sb] {
        let rows = session
            .execute_at("SELECT id, g FROM t ORDER BY id", &[], None, &clock())
            .unwrap();
        let rows = ints(&rows);
        assert_eq!(rows.len(), 40);
        // nextval is evaluated on the system range leader: one global order, no gaps here.
        assert_eq!(
            rows.iter().map(|r| r[0]).collect::<Vec<_>>(),
            (1..=40).collect::<Vec<_>>()
        );
        // Alternating gateways got alternating values.
        assert!(
            rows.iter()
                .all(|r| r[1] == if r[0] % 2 == 1 { 1 } else { 2 })
        );
        // Heap rows from both gateways: distinct row ids, so nothing was overwritten.
        let heap = ints(
            &session
                .execute_at("SELECT g, n FROM h ORDER BY g, n", &[], None, &clock())
                .unwrap(),
        );
        assert_eq!(heap.len(), 40);
        assert_eq!(heap.iter().collect::<BTreeSet<_>>().len(), 40);
    }
}

#[test]
fn a_transaction_over_several_ranges_commits_or_rolls_back_as_one() {
    let cluster = LocalCluster::new(&splits());
    let a = gateway(&cluster, 1);
    let mut sa = a.admin.sql_session();
    sa.execute_at(
        "CREATE TABLE x(id INT PRIMARY KEY, v INT)",
        &[],
        None,
        &clock(),
    )
    .unwrap();
    sa.execute_at(
        "CREATE TABLE y(id INT PRIMARY KEY, v INT UNIQUE)",
        &[],
        None,
        &clock(),
    )
    .unwrap();
    let b = gateway(&cluster, 2);
    let mut sb = b.admin.sql_session();
    for statement in [
        "BEGIN",
        "INSERT INTO x VALUES (1, 10)",
        "INSERT INTO y VALUES (1, 10)",
        "COMMIT",
        "BEGIN",
        "INSERT INTO x VALUES (2, 20)",
        "INSERT INTO y VALUES (2, 20)",
        "ROLLBACK",
    ] {
        sa.execute_at(statement, &[], None, &clock()).unwrap();
    }
    for table in ["x", "y"] {
        let rows = sb
            .execute_at(
                &format!("SELECT id, v FROM {table} ORDER BY id"),
                &[],
                None,
                &clock(),
            )
            .unwrap();
        assert_eq!(ints(&rows), vec![vec![1, 10]], "{table}");
    }
    // The unique index entry of y (another range than its row) is enforced across gateways.
    let error = sb
        .execute_at("INSERT INTO y VALUES (3, 10)", &[], None, &clock())
        .unwrap_err();
    assert!(format!("{error:?}").contains("Duplicate"), "{error:?}");
}

#[test]
fn concurrent_updates_through_two_gateways_lose_nothing() {
    let cluster = LocalCluster::new(&splits());
    let a = gateway(&cluster, 1);
    let mut sa = a.admin.sql_session();
    sa.execute_at(
        "CREATE TABLE c(id INT PRIMARY KEY, n INT)",
        &[],
        None,
        &clock(),
    )
    .unwrap();
    sa.execute_at("INSERT INTO c VALUES (1, 0)", &[], None, &clock())
        .unwrap();
    let b = gateway(&cluster, 2);
    std::thread::scope(|scope| {
        for gateway in [&a, &b] {
            scope.spawn(move || {
                let mut session = gateway.admin.sql_session();
                for _ in 0..25 {
                    session
                        .execute_at("UPDATE c SET n = n + 1 WHERE id = 1", &[], None, &clock())
                        .unwrap();
                }
            });
        }
    });
    let rows = sa
        .execute_at("SELECT n FROM c", &[], None, &clock())
        .unwrap();
    assert_eq!(ints(&rows), vec![vec![50]]);
}

#[test]
fn a_lock_cycle_across_ranges_and_gateways_ends_with_one_deadlock() {
    let cluster = LocalCluster::new(&splits());
    let a = gateway(&cluster, 1);
    let mut setup = a.admin.sql_session();
    setup
        .execute_at(
            "CREATE TABLE p(id INT PRIMARY KEY, n INT)",
            &[],
            None,
            &clock(),
        )
        .unwrap();
    setup
        .execute_at(
            "CREATE TABLE q(id INT PRIMARY KEY, n INT)",
            &[],
            None,
            &clock(),
        )
        .unwrap();
    setup
        .execute_at("INSERT INTO p VALUES (1, 0)", &[], None, &clock())
        .unwrap();
    setup
        .execute_at("INSERT INTO q VALUES (1, 0)", &[], None, &clock())
        .unwrap();
    let b = gateway(&cluster, 2);
    let barrier = std::sync::Barrier::new(2);
    let results: Vec<Result<(), String>> = std::thread::scope(|scope| {
        let handles: Vec<_> = [(&a, "p", "q"), (&b, "q", "p")]
            .into_iter()
            .map(|(gateway, first, second)| {
                let barrier = &barrier;
                scope.spawn(move || {
                    let mut s = gateway.admin.sql_session();
                    s.execute_at("BEGIN", &[], None, &clock()).unwrap();
                    s.execute_at(
                        &format!("UPDATE {first} SET n = n + 1 WHERE id = 1"),
                        &[],
                        None,
                        &clock(),
                    )
                    .unwrap();
                    barrier.wait();
                    let second = s.execute_at(
                        &format!("UPDATE {second} SET n = n + 1 WHERE id = 1"),
                        &[],
                        None,
                        &clock(),
                    );
                    match second {
                        Ok(_) => {
                            s.execute_at("COMMIT", &[], None, &clock()).unwrap();
                            Ok(())
                        }
                        Err(error) => {
                            let _ = s.execute_at("ROLLBACK", &[], None, &clock());
                            Err(format!("{error:?}"))
                        }
                    }
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let failed: Vec<_> = results.iter().filter_map(|r| r.as_ref().err()).collect();
    assert_eq!(failed.len(), 1, "{results:?}");
    assert!(failed[0].contains("Deadlock"), "{failed:?}");
    let p = ints(
        &setup
            .execute_at("SELECT n FROM p", &[], None, &clock())
            .unwrap(),
    );
    let q = ints(
        &setup
            .execute_at("SELECT n FROM q", &[], None, &clock())
            .unwrap(),
    );
    assert_eq!((p, q), (vec![vec![1]], vec![vec![1]]));
}

/// R5-45: a gateway streams range pages, so an aggregate that keeps no rows runs over any
/// amount of data, and one whose grouped input passes the 64 MiB retention budget spills to
/// temp files like on one node (R2-PGLIMITS-4, PostgreSQL HashAgg spill), never exhausting
/// memory and never failing with 54000.
#[test]
fn a_gateway_aggregate_over_the_retention_budget_spills() {
    let cluster = LocalCluster::new(&splits());
    let a = gateway(&cluster, 1);
    let mut s = a.admin.sql_session();
    s.execute_at(
        "CREATE TABLE big(id INT PRIMARY KEY, payload TEXT)",
        &[],
        None,
        &clock(),
    )
    .unwrap();
    // 17 x 1,000 rows of ~4 KiB: about 68 MiB of distinct payloads.
    for batch in 0..17 {
        let pad = "x".repeat(4000);
        let values: Vec<String> = (batch * 1000 + 1..=batch * 1000 + 1000)
            .map(|id| format!("({id}, '{pad}{id}')"))
            .collect();
        s.execute_at(
            &format!("INSERT INTO big VALUES {}", values.join(",")),
            &[],
            None,
            &clock(),
        )
        .unwrap();
    }
    let count = s
        .execute_at("SELECT count(*) FROM big", &[], None, &clock())
        .unwrap();
    assert_eq!(ints(&count), vec![vec![17_000]]);
    let duplicates = s
        .execute_at(
            "SELECT payload FROM big GROUP BY payload HAVING count(*) > 1",
            &[],
            None,
            &clock(),
        )
        .unwrap();
    assert!(duplicates.rows.is_empty());
    let groups = s
        .execute_at(
            "SELECT count(DISTINCT payload) FROM big",
            &[],
            None,
            &clock(),
        )
        .unwrap();
    assert_eq!(ints(&groups), vec![vec![17_000]]);
}

fn run(session: &mut crate::storage::SqlSession, sql: &str) -> crate::relational::SqlResult {
    session.execute_at(sql, &[], None, &clock()).unwrap()
}

/// P7-1: a gateway attached before another gateway's DDL serves that DDL (the catalog-generation
/// record tells it its cached catalog is stale).
#[test]
fn a_gateway_sees_ddl_committed_through_another_gateway() {
    let cluster = LocalCluster::new(&splits());
    let a = gateway(&cluster, 1);
    let b = gateway(&cluster, 2);
    let (mut sa, mut sb) = (a.admin.sql_session(), b.admin.sql_session());
    run(&mut sa, "CREATE TABLE t(id INT PRIMARY KEY, v INT)");
    run(&mut sa, "CREATE TABLE gone(id INT PRIMARY KEY)");
    run(&mut sa, "INSERT INTO t VALUES (1, 10)");
    assert_eq!(ints(&run(&mut sb, "SELECT id, v FROM t")), vec![vec![1, 10]]);
    run(&mut sb, "INSERT INTO t VALUES (2, 20)");
    assert_eq!(
        ints(&run(&mut sa, "SELECT id, v FROM t ORDER BY id")),
        vec![vec![1, 10], vec![2, 20]]
    );
    run(&mut sa, "ALTER TABLE t ADD COLUMN w INT");
    run(&mut sa, "DROP TABLE gone");
    run(&mut sb, "UPDATE t SET w = 7 WHERE id = 2");
    assert_eq!(
        ints(&run(&mut sb, "SELECT id, w FROM t WHERE w IS NOT NULL")),
        vec![vec![2, 7]]
    );
    let error = sb
        .execute_at("SELECT * FROM gone", &[], None, &clock())
        .unwrap_err();
    assert!(format!("{error:?}").contains("UnknownTable"), "{error:?}");
    assert_eq!(
        ints(&run(&mut sa, "SELECT id, w FROM t WHERE w IS NOT NULL")),
        vec![vec![2, 7]]
    );
}

/// P7-2: the PostgreSQL wire path on a gateway. SELECTs run through cursors (buffered on a
/// gateway), and two metadata reads share an epoch while no catalog change is published
/// between them, although each read draws a fresh oracle timestamp; DDL changes the epoch.
#[test]
fn a_gateway_serves_cursors_and_stable_metadata_epochs() {
    let cluster = LocalCluster::new(&splits());
    let a = gateway(&cluster, 1);
    let mut s = a.admin.sql_session();
    run(&mut s, "CREATE TABLE t(id INT PRIMARY KEY, v INT)");
    run(&mut s, "INSERT INTO t VALUES (1, 10), (2, 20)");
    for (sql, want) in [
        ("SELECT id, v FROM t ORDER BY id", vec![vec![1, 10], vec![2, 20]]),
        ("SELECT count(*) FROM t", vec![vec![2]]),
        ("SELECT 1", vec![vec![1]]),
    ] {
        let p = s.prepare(sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        s.describe(&p).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        let mut cursor = s.open_cursor(&p, &[]).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        let rows = crate::relational::SqlResult {
            rows: cursor.fetch(100, 1 << 20).unwrap(),
            ..Default::default()
        };
        assert_eq!(ints(&rows), want, "{sql}");
    }
    let epoch = s.catalog_metadata().unwrap().presentation.epoch;
    run(&mut s, "INSERT INTO t VALUES (3, 30)");
    assert_eq!(s.catalog_metadata().unwrap().presentation.epoch, epoch);
    run(&mut s, "ALTER TABLE t ADD COLUMN w INT");
    assert_ne!(s.catalog_metadata().unwrap().presentation.epoch, epoch);
}

/// P7-1: roles are read from the system range on every lookup, so a role created through one
/// gateway is visible on another; role writers serialize across gateways, so two creates of
/// one name leave exactly one role and distinct names get distinct OIDs.
#[test]
fn role_writes_through_two_gateways_serialize() {
    use crate::storage::roles::{NewRole, RoleError};
    let cluster = LocalCluster::new(&splits());
    let a = gateway(&cluster, 1);
    let b = gateway(&cluster, 2);
    let role = |name: &str| NewRole {
        name: name.into(),
        ..NewRole::default()
    };
    a.admin.roles().create(role("early")).unwrap();
    assert!(b.admin.roles().get("early").unwrap().is_some());
    for round in 0..5 {
        let barrier = std::sync::Barrier::new(2);
        let same = format!("same{round}");
        let results: Vec<_> = std::thread::scope(|scope| {
            let handles: Vec<_> = [&a, &b]
                .into_iter()
                .map(|gateway| {
                    let (barrier, same) = (&barrier, &same);
                    scope.spawn(move || {
                        barrier.wait();
                        gateway.admin.roles().create(role(same))
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let duplicates = results
            .iter()
            .filter(|r| matches!(r, Err(RoleError::Duplicate)))
            .count();
        assert_eq!((results.len() - duplicates, duplicates), (1, 1), "{results:?}");
        let distinct: Vec<_> = std::thread::scope(|scope| {
            let handles: Vec<_> = [(&a, "x"), (&b, "y")]
                .into_iter()
                .map(|(gateway, prefix)| {
                    let name = format!("{prefix}{round}");
                    scope.spawn(move || gateway.admin.roles().create(role(&name)).unwrap().oid)
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        assert_ne!(distinct[0], distinct[1]);
    }
    let names = |admin: &crate::storage::Admin| -> Vec<(String, u32)> {
        let mut roles: Vec<_> = admin
            .roles()
            .list()
            .unwrap()
            .into_iter()
            .map(|r| (r.name, r.oid))
            .collect();
        roles.sort();
        roles
    };
    let listed = names(&a.admin);
    assert_eq!(listed, names(&b.admin));
    assert_eq!(listed.len(), 16);
    assert_eq!(listed.iter().map(|r| r.1).collect::<BTreeSet<_>>().len(), 16);
}

/// Raw Rows user keys (version suffix stripped) through `client` at a fresh timestamp.
fn row_keys(cluster: &Arc<LocalCluster>) -> BTreeSet<Vec<u8>> {
    use crate::storage::cluster::read::RoutedKvRead;
    use crate::storage::engine::{
        codec,
        hlc::Ts,
        kv::{Direction, Keyspace, KvRead},
    };
    use crate::storage::range_api::RangeClient;
    use std::ops::Bound;
    let at = cluster.oracle().next(1).unwrap();
    let reader = RoutedKvRead::new(cluster.clone(), Ts(at.0), 1 << 20);
    reader
        .range(
            Keyspace::Rows,
            Bound::Unbounded,
            Bound::Unbounded,
            Direction::Forward,
        )
        .unwrap()
        .map(|pair| {
            let (key, _) = pair.unwrap();
            codec::split_versioned(&key).unwrap().0.to_vec()
        })
        .collect()
}

/// GD-0075 P8-0: a prepared write whose outcome stays open (its gateway died mid-commit) holds
/// up only reads of its own key. Point reads and bounded scans below it answer at once through
/// another gateway; a read of that key waits and then sees the outcome.
#[test]
fn a_prepared_write_above_a_row_does_not_delay_point_reads_through_a_gateway() {
    use crate::storage::range_api::{
        ClusterTxnId, HomeOp, HomeReply, LockRequest, LockTarget, Outcome, RangeClient,
        RangeLockMode, RangeWrite,
    };
    use std::time::Instant;
    let cluster = LocalCluster::new(&splits());
    let a = gateway(&cluster, 1);
    let mut sa = a.admin.sql_session();
    run(&mut sa, "CREATE TABLE p(id INT PRIMARY KEY, v INT)");
    run(&mut sa, "INSERT INTO p VALUES (1, 10), (2, 20)");
    let before = row_keys(&cluster);
    run(&mut sa, "INSERT INTO p VALUES (9, 90)");
    let added: Vec<Vec<u8>> = row_keys(&cluster).difference(&before).cloned().collect();
    let [key9] = added.as_slice() else {
        panic!("one new row key, got {added:?}");
    };
    // A transaction of a dead gateway: home record open (never decided), key 9 prepared.
    let range = cluster.range_of(RangeSpace::Rows, key9);
    let epoch = cluster
        .descriptor_for(&range_key(RangeSpace::Rows, key9))
        .unwrap()
        .epoch;
    let txn = ClusterTxnId::new(77, 1);
    let opened = cluster.home(range, epoch, HomeOp::Open { txn }).unwrap();
    assert!(matches!(opened, HomeReply::Opened { .. }), "{opened:?}");
    let grant = cluster
        .lock(LockRequest {
            range,
            epoch,
            txn,
            priority: crate::storage::range_api::Timestamp(0),
            target: LockTarget::Key {
                space: RangeSpace::Rows,
                key: key9.clone(),
            },
            mode: RangeLockMode::Exclusive,
            timeout_ms: Some(1000),
        })
        .unwrap();
    let write = RangeWrite {
        space: RangeSpace::Rows,
        key: key9.clone(),
        value: Some(b"never applied".to_vec()),
    };
    cluster
        .prepare(
            range,
            epoch,
            txn,
            range,
            vec![write],
            Some(grant.generation),
        )
        .unwrap();

    // Another gateway: reads below key 9 answer well inside read_wait (2 s).
    let b = gateway(&cluster, 2);
    let mut sb = b.admin.sql_session();
    for (sql, want) in [
        ("SELECT v FROM p WHERE id = 1", vec![vec![10]]),
        ("SELECT v FROM p WHERE id = 2", vec![vec![20]]),
        ("SELECT v FROM p WHERE id = 5", vec![]),
        (
            "SELECT id FROM p WHERE id >= 1 AND id < 3 ORDER BY id",
            vec![vec![1], vec![2]],
        ),
    ] {
        let started = Instant::now();
        let rows = sb
            .execute_at(sql, &[], None, &clock())
            .unwrap_or_else(|e| panic!("{sql}: {e:?} after {:?}", started.elapsed()));
        assert_eq!(ints(&rows), want, "{sql}");
        assert!(
            started.elapsed() < Duration::from_millis(1000),
            "{sql} took {:?}",
            started.elapsed()
        );
    }
    // A read of key 9 waits for the outcome; the participant's abort releases it.
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        let b = &b;
        scope.spawn(move || {
            let mut session = b.admin.sql_session();
            let _ = tx.send(ints(&run(&mut session, "SELECT v FROM p WHERE id = 9")));
        });
        assert!(
            rx.recv_timeout(Duration::from_millis(500)).is_err(),
            "a read of the prepared key must wait"
        );
        cluster.finalize(range, txn, Outcome::Abort).unwrap();
        let rows = rx.recv_timeout(Duration::from_secs(10)).unwrap();
        assert_eq!(rows, vec![vec![90]]);
    });
}
