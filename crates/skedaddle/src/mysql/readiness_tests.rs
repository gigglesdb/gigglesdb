use super::*;
const UUID: &str = "426f5bf9-e122-11f1-a0ab-0242ac120002";
fn status() -> MysqlStatus {
    MysqlStatus {
        file: "bin.000003".into(),
        offset: 100,
        server_uuid: UUID.into(),
        executed: format!("{UUID}:1-10").parse().unwrap(),
        purged: GtidSet::default(),
        gtid_on: true,
        binary_logs: vec![
            ("bin.000001".into(), 1000),
            ("bin.000002".into(), 500),
            ("bin.000003".into(), 100),
        ],
    }
}
fn file(seq: u64, offset: u64) -> MysqlPosition {
    MysqlPosition::File {
        server_uuid: UUID.into(),
        file_seq: seq,
        offset,
    }
}
fn probe(gtid: bool) -> MysqlReadinessProbe {
    let mut status = status();
    status.gtid_on = gtid;
    let source = SourceIdentity {
        engine: "mysql".into(),
        system_id: "cluster".into(),
        lineage: "one".into(),
        db_name: "db".into(),
        stream: "stream".into(),
    };
    let order = if gtid {
        MysqlOrder::gtid("cluster").unwrap()
    } else {
        MysqlOrder::file(UUID).unwrap()
    };
    let native = if gtid {
        MysqlPosition::Gtid {
            set: status.executed.clone(),
            server_uuid: UUID.into(),
            file: status.file.clone(),
            offset: status.offset,
        }
    } else {
        file(3, 100)
    };
    let logs = [0, 4, 8]
        .into_iter()
        .enumerate()
        .map(|(i, n)| LogEvidence {
            file: format!("bin.{:06}", i + 1),
            created_us: Some(1_000_000 + i as i64 * 1_000_000),
            previous: if n == 0 {
                GtidSet::default()
            } else {
                format!("{UUID}:1-{n}").parse().unwrap()
            },
        })
        .collect();
    MysqlReadinessProbe {
        source,
        tip: Position::new(order.domain(), native.encode().unwrap()),
        order,
        status,
        clock_us: 10_000_000,
        clock_at: Instant::now(),
        started: Instant::now(),
        expiry_seconds: 100,
        logs: Some(logs),
        retention_issue: None,
    }
}
fn applied(p: &MysqlReadinessProbe, n: u64) -> Position {
    let native = if p.status.gtid_on {
        MysqlPosition::Gtid {
            set: format!("{UUID}:1-{n}").parse().unwrap(),
            server_uuid: UUID.into(),
            file: "bin.000001".into(),
            offset: 900,
        }
    } else {
        file(n, 100)
    };
    Position::new(p.order.domain(), native.encode().unwrap())
}
#[test]
fn bytes_sum_rotations_and_fail_closed() {
    let s = status();
    assert_eq!(bytes_behind(&file(1, 900), &s), Some(700));
    assert_eq!(bytes_behind(&file(2, 400), &s), Some(200));
    assert_eq!(bytes_behind(&file(3, 100), &s), Some(0));
    assert_eq!(bytes_behind(&file(3, 101), &s), None);
    assert_eq!(bytes_behind(&file(1, 1001), &s), None);
    let mut s = status();
    s.binary_logs.remove(1);
    assert_eq!(bytes_behind(&file(1, 900), &s), None);
    assert_eq!(bytes_behind(&file(2, 100), &s), None);
    let mut p = file(1, 900);
    if let MysqlPosition::File { server_uuid, .. } = &mut p {
        *server_uuid = "00000000-0000-0000-0000-000000000000".into();
    }
    assert_eq!(bytes_behind(&p, &s), None);
    let mut s = status();
    s.binary_logs[0].1 = u64::MAX;
    assert_eq!(bytes_behind(&file(1, 4), &s), None);
}
#[test]
fn gtid_count_uses_intervals_and_detects_tags_overflow() {
    let s: GtidSet = format!("{UUID}:1-1000000000:1000000002-1000000005")
        .parse()
        .unwrap();
    assert_eq!(count(&s), Some(1_000_000_004));
    assert_eq!(count(&GtidSet::default()), Some(0));
    let s: GtidSet = format!("{UUID}:tag:1-3").parse().unwrap();
    assert_eq!(count(&s), None);
    let s:GtidSet=format!("{UUID}:1-9223372036854775807,00000000-0000-0000-0000-000000000001:1-9223372036854775807,00000000-0000-0000-0000-000000000002:1-2").parse().unwrap();
    assert_eq!(count(&s), None);
}
#[test]
fn age_uses_source_clock_independent_of_local_wall_clock() {
    // Simulated wall-clock offsets are deliberately absent from source arithmetic.
    for _local_wall_us in [i64::MIN, 0, i64::MAX] {
        assert_eq!(commit_age(10_000_001, Some(Some(9_000_000))), Some(1001));
    }
    assert_eq!(commit_age(10, Some(Some(11))), None);
    assert_eq!(
        commit_age(i64::MAX, Some(Some(1))),
        Some((i64::MAX as u64 - 1).div_ceil(1000))
    );
    assert_eq!(commit_age(10, Some(None)), None);
    assert_eq!(commit_age(10, Some(Some(0))), None);
    assert_eq!(commit_age(10, None), Some(0));
}
#[test]
fn samples_backlog_retention_and_unknowns() {
    let mut p = probe(true);
    let a = applied(&p, 4);
    let s = p.sample(&a, Some(Some(8_000_000))).unwrap();
    assert_eq!(
        s.lag,
        Some(LagMeasure {
            value: 6,
            unit: "transactions"
        })
    );
    assert!(s.oldest_unapplied_age_ms.unwrap() >= 2000);
    assert!(s.retention.unwrap().remaining.value <= 92);
    assert_eq!(
        p.retention(&a, &MysqlPosition::decode(a.bytes()).unwrap(), 10_000_000)
            .unwrap()
            .remaining
            .value,
        92
    ); // first unapplied is file 2
    assert!(s.schema.is_none());
    let a = applied(&p, 10);
    let s = p.sample(&a, None).unwrap();
    assert_eq!(s.lag.unwrap().value, 0);
    assert_eq!(s.oldest_unapplied_age_ms, Some(0));
    assert!(s.retention.unwrap().remaining.value <= 93);
    assert_eq!(
        p.retention(&a, &MysqlPosition::decode(a.bytes()).unwrap(), 10_000_000)
            .unwrap()
            .remaining
            .value,
        93
    );
    assert!(
        p.sample(&a, Some(None))
            .unwrap()
            .oldest_unapplied_age_ms
            .is_none()
    );
    p.logs.as_mut().unwrap()[2].created_us = None;
    assert!(p.sample(&a, None).unwrap().retention.is_none());
    p.logs = None;
    assert!(p.sample(&a, None).unwrap().retention.is_none());
    assert!(p.sample(&Position::new("wrong", a.bytes()), None).is_err());
    assert!(p.sample(&applied(&p, 11), None).is_err());
    let mut p = probe(false);
    let a = applied(&p, 1);
    assert_eq!(p.sample(&a, None).unwrap().lag.unwrap().unit, "bytes");
    p.status.binary_logs.remove(0);
    let s = p.sample(&a, None).unwrap();
    assert!(s.lag.is_none());
    assert!(!s.retention.unwrap().history_available);
}
#[test]
fn retention_unknown_future_expired_disabled_and_uncovered() {
    let mut p = probe(true);
    let a = applied(&p, 4);
    let native = MysqlPosition::decode(a.bytes()).unwrap();
    assert!(p.retention(&a, &native, 1).is_none());
    p.expiry_seconds = 1;
    assert_eq!(
        p.retention(&a, &native, 10_000_000)
            .unwrap()
            .remaining
            .value,
        0
    );
    p.expiry_seconds = 0;
    assert!(p.retention(&a, &native, 10_000_000).is_none());
    p.expiry_seconds = 100;
    p.logs.as_mut().unwrap().remove(0);
    let a = applied(&p, 1);
    let native = MysqlPosition::decode(a.bytes()).unwrap();
    assert!(p.retention(&a, &native, 10_000_000).is_none());
}
#[test]
fn four_real_fixture_gtid_timestamps_present_including_payloads() {
    use crate::mysql::binlog::{BinlogDecoder, MysqlEvent};
    fn collect_times(e: &MysqlEvent, times: &mut Vec<i64>) {
        match e {
            MysqlEvent::Gtid { commit_time, .. }
            | MysqlEvent::AnonymousGtid { commit_time, .. } => {
                times.push(commit_time.expect("8.0/8.4 commit timestamp"))
            }
            MysqlEvent::TransactionPayload(events) => {
                for e in events {
                    collect_times(e, times)
                }
            }
            _ => {}
        }
    }
    for variant in [
        "mysql80-gtid-on",
        "mysql80-gtid-off",
        "mysql84-gtid-on",
        "mysql84-gtid-off",
    ] {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/mysql")
            .join(variant);
        let path = std::fs::read_dir(&root)
            .unwrap()
            .map(|p| p.unwrap().path())
            .find(|p| p.extension().is_some_and(|x| x == "binlog"))
            .unwrap();
        let bytes = std::fs::read(path).unwrap();
        let mut input = std::io::Cursor::new(&bytes[4..]);
        let mut decoder = BinlogDecoder::new(16 * 1024 * 1024);
        let mut found = Vec::new();
        while let Some(e) = decoder.read(&mut input).unwrap() {
            collect_times(&e, &mut found);
        }
        assert!(found.len() >= 3, "{variant}");
        assert!(found.iter().all(|n| *n > 1_700_000_000_000_000));
    }
}
