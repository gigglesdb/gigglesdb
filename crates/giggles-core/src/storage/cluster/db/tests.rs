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
    if let Some(service) = gateway.services.gateway_service() {
        cluster.register_gateway(u64::from(node), service);
    }
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
/// amount of data, and one whose retained input passes the 64 MiB budget fails with the same
/// limit error as on one node, never by exhausting memory.
#[test]
fn a_gateway_aggregate_over_the_retention_budget_fails_with_the_limit_error() {
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
    let error = s
        .execute_at(
            "SELECT payload FROM big GROUP BY payload HAVING count(*) > 1",
            &[],
            None,
            &clock(),
        )
        .unwrap_err();
    assert_eq!(
        error,
        crate::CoreError::Sql(crate::relational::SqlError::Limit)
    );
}
