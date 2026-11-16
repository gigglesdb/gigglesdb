use super::*;
use crate::mysql::{capture::ReconnectPolicy, client::MysqlError};
use std::time::Duration;

const A: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
const B: &str = "24bc7850-2c16-11e6-a073-0242ac110002";
fn set(a: &str, b: &str) -> GtidSet {
    [(A, a), (B, b)]
        .into_iter()
        .filter(|(_, s)| !s.is_empty())
        .map(|(id, s)| format!("{id}:{s}"))
        .collect::<Vec<_>>()
        .join(",")
        .parse()
        .unwrap()
}
fn observation() -> ServerObservation {
    ServerObservation {
        file: "mysql-bin.000013".into(),
        offset: 200,
        server_uuid: A.into(),
        executed: set("1-50", "1-20"),
        purged: set("1-10", "1-3"),
        gtid_on: true,
        binary_logs: vec![
            ("mysql-bin.000012".into(), 400),
            ("mysql-bin.000013".into(), 200),
        ],
    }
}
fn durable() -> MysqlPosition {
    MysqlPosition::Gtid {
        set: set("1-40:46-50", "1-8:12-15"),
        server_uuid: A.into(),
        file: "mysql-bin.000012".into(),
        offset: 100,
    }
}
fn file() -> MysqlPosition {
    MysqlPosition::File {
        server_uuid: A.into(),
        file_seq: 12,
        offset: 100,
    }
}

#[test]
fn equal_and_superset_gtid_history_resume_on_same_or_different_server() {
    for server in [A, B] {
        for equal in [false, true] {
            let mut o = observation();
            o.server_uuid = server.into();
            if equal {
                let MysqlPosition::Gtid { set, .. } = durable() else {
                    unreachable!()
                };
                o.executed = set;
            }
            let plan = check(&durable(), &o).unwrap();
            let MysqlPosition::Gtid { set, .. } = durable() else {
                unreachable!()
            };
            assert_eq!(
                plan,
                ResumePlan::Gtid {
                    set,
                    reconnect_hint: ("mysql-bin.000012".into(), 100)
                }
            );
        }
    }
}

#[test]
fn subset_incomparable_and_foreign_gtid_histories_block_with_exact_difference() {
    for executed in [
        set("1-39:46-50", "1-8:12-15"),
        set("1-40:46-51", "1-8:12-14"),
        set("1-50", ""),
    ] {
        for server in [A, B] {
            let mut o = observation();
            o.executed = executed.clone();
            o.server_uuid = server.into();
            let MysqlPosition::Gtid { set, .. } = durable() else {
                unreachable!()
            };
            assert_eq!(
                check(&durable(), &o),
                Err(ResumeBlocker::Divergent {
                    server: server.into(),
                    missing: set.difference(&executed),
                })
            );
        }
    }
}

#[test]
fn purged_equal_or_subset_is_safe_but_gaps_and_foreign_purges_are_history_loss() {
    let MysqlPosition::Gtid { set: captured, .. } = durable() else {
        unreachable!()
    };
    for purged in [
        GtidSet::default(),
        captured.clone(),
        set("1-3:46-47", "1-2:12"),
    ] {
        let mut o = observation();
        o.purged = purged;
        assert!(check(&durable(), &o).is_ok());
    }
    for purged in [set("1-41", "1-3"), set("1-10", "1-9"), set("", "16-17")] {
        let mut o = observation();
        o.purged = purged.clone();
        let error = check(&durable(), &o).unwrap_err();
        assert_eq!(
            error,
            ResumeBlocker::HistoryLost {
                missing: purged.difference(&captured)
            }
        );
        assert!(
            error
                .to_string()
                .contains("restart the migration with a new copy")
        );
    }
}

#[test]
fn nonempty_gtid_file_coordinates_are_hints_only() {
    let mut o = observation();
    o.binary_logs.clear();
    // An obsolete filename (or an unrepresentable offset) cannot constrain GTID.
    let mut d = durable();
    if let MysqlPosition::Gtid { file, offset, .. } = &mut d {
        file.clear();
        *offset = u64::MAX;
    }
    assert!(matches!(check(&d, &o), Ok(ResumePlan::Gtid { .. })));
}

#[test]
fn empty_gtid_uses_durable_file_not_current_status_and_checks_retention() {
    let mut o = observation();
    o.purged = GtidSet::default();
    let mut d = durable();
    if let MysqlPosition::Gtid { set, .. } = &mut d {
        *set = GtidSet::default();
    }
    assert_eq!(
        check(&d, &o),
        Ok(ResumePlan::File {
            file: "mysql-bin.000012".into(),
            offset: 100
        })
    );
    o.binary_logs.remove(0);
    assert!(matches!(
        check(&d, &o),
        Err(ResumeBlocker::FilePurged { .. })
    ));
    o = observation();
    assert!(matches!(
        check(&d, &o),
        Err(ResumeBlocker::HistoryLost { .. })
    ));
    o.purged = GtidSet::default();
    o.server_uuid = B.into();
    assert!(matches!(
        check(&d, &o),
        Err(ResumeBlocker::SourceMismatch { .. })
    ));
}

#[test]
fn completely_empty_gtid_history_and_file_mode_with_gtid_off() {
    let mut o = observation();
    o.executed = GtidSet::default();
    o.purged = GtidSet::default();
    let mut d = durable();
    if let MysqlPosition::Gtid { set, .. } = &mut d {
        *set = GtidSet::default();
    }
    assert!(matches!(
        check(&d, &o),
        Ok(ResumePlan::File { offset: 100, .. })
    ));
    o.gtid_on = false;
    assert!(matches!(check(&file(), &o), Ok(ResumePlan::File { .. })));
}

#[test]
fn disabled_and_tagged_gtids_block() {
    let mut o = observation();
    o.gtid_on = false;
    assert_eq!(check(&durable(), &o), Err(ResumeBlocker::GtidDisabled));
    for target in 0..3 {
        let mut d = durable();
        let mut o = observation();
        let tagged: GtidSet = format!("{A}:blue:1-3").parse().unwrap();
        match target {
            0 => {
                if let MysqlPosition::Gtid { set, .. } = &mut d {
                    *set = tagged
                }
            }
            1 => o.executed = tagged,
            _ => o.purged = tagged,
        }
        assert_eq!(check(&d, &o), Err(ResumeBlocker::TaggedGtid));
    }
}

#[test]
fn file_uuid_mismatch_precedes_retention_checks() {
    let mut o = observation();
    o.server_uuid = B.into();
    o.binary_logs.clear();
    assert_eq!(
        check(&file(), &o),
        Err(ResumeBlocker::SourceMismatch {
            durable: A.into(),
            observed: B.into()
        })
    );
}

#[test]
fn file_retention_and_inclusive_size_boundary() {
    let mut o = observation();
    for offset in [4, 100, 400] {
        let mut d = file();
        if let MysqlPosition::File { offset: v, .. } = &mut d {
            *v = offset;
        }
        assert_eq!(
            check(&d, &o),
            Ok(ResumePlan::File {
                file: "mysql-bin.000012".into(),
                offset
            })
        );
    }
    for offset in [0, 3, 401, u64::from(u32::MAX) + 1] {
        let mut d = file();
        if let MysqlPosition::File { offset: v, .. } = &mut d {
            *v = offset;
        }
        assert!(matches!(
            check(&d, &o),
            Err(ResumeBlocker::OffsetOutsideFile { .. })
        ));
    }
    o.binary_logs.remove(0);
    let error = check(&file(), &o).unwrap_err();
    assert_eq!(
        error,
        ResumeBlocker::FilePurged {
            file: "mysql-bin.000012".into()
        }
    );
    assert!(error.to_string().contains("history lost"));
}

#[test]
fn file_dump_upper_bound_is_inclusive_even_when_log_is_larger() {
    let mut o = observation();
    o.binary_logs[0].1 = u64::MAX;
    let mut d = file();
    if let MysqlPosition::File { offset, .. } = &mut d {
        *offset = u64::from(u32::MAX);
    }
    assert!(check(&d, &o).is_ok());
    if let MysqlPosition::File { offset, .. } = &mut d {
        *offset += 1;
    }
    assert!(matches!(
        check(&d, &o),
        Err(ResumeBlocker::OffsetOutsideFile { .. })
    ));
}

#[test]
fn transport_retry_classification_excludes_semantic_auth_and_tls_failures() {
    for kind in [
        std::io::ErrorKind::ConnectionReset,
        std::io::ErrorKind::TimedOut,
        std::io::ErrorKind::ConnectionRefused,
        std::io::ErrorKind::UnexpectedEof,
    ] {
        assert!(MysqlError::Io(kind.into()).is_transient());
        assert!(MysqlError::Driver(std::io::Error::from(kind).into()).is_transient());
    }
    assert!(MysqlError::StreamEnded.is_transient());
    for code in [1053, 1077, 1079, 1045, 1236] {
        let error = MysqlError::Driver(
            mysql_async::ServerError {
                code,
                message: String::new(),
                state: "HY000".into(),
            }
            .into(),
        );
        assert_eq!(error.is_transient(), matches!(code, 1053 | 1077 | 1079));
    }
    assert!(!MysqlError::Invalid("codec").is_transient());
    assert!(!MysqlError::Resume(ResumeBlocker::GtidDisabled).is_transient());
    let policy = ReconnectPolicy::default();
    assert_eq!(policy.attempts, 5);
    assert_eq!(policy.budget, Duration::from_secs(30));
}
