mod support;

use support::{
    oracle::{Anomaly, Oracle, read_destination},
    workload::{self, Config, HistoryEntry, Operation, Outcome, Row, Statement, TABLES, Toast},
};

fn row(key: i64, counter: i64) -> Row {
    Row {
        key,
        counter,
        numeric: "999999999999999999999999999999.9999999999".into(),
        timestamp: "9999-12-31 23:59:59.999999+00".into(),
        toast: Toast {
            text: "large payload".into(),
            bytes: "00ff80".into(),
        },
    }
}

fn insert(table: &str, key: i64, counter: i64) -> Operation {
    Operation::Insert {
        table: table.into(),
        row: row(key, counter),
    }
}

fn update(table: &str, key: i64, new_key: Option<i64>, delta: i64) -> Operation {
    Operation::Update {
        table: table.into(),
        key,
        new_key,
        delta,
        toast: None,
    }
}

fn committed(id: u64, lsn: u64, operations: Vec<Operation>) -> HistoryEntry {
    HistoryEntry {
        txn_id: id,
        statements: operations.into_iter().map(Statement::new).collect(),
        outcome: Outcome::Committed,
        commit_lsn: lsn,
    }
}

fn small_config(seed: u64) -> Config {
    Config {
        seed,
        toast_bytes: 64,
        ..Config::default()
    }
}

#[test]
fn seeded_generator_is_deterministic_and_seed_sensitive() {
    assert_eq!(
        workload::generate(&small_config(123)),
        workload::generate(&small_config(123))
    );
    assert_ne!(
        workload::generate(&small_config(123)),
        workload::generate(&small_config(124))
    );
}

#[test]
fn all_operation_classes_appear_within_twelve_transactions() {
    let generated = workload::generate(&Config {
        transactions: 12,
        toast_bytes: 8192,
        ..Config::default()
    });
    let mut classes = std::collections::BTreeSet::new();
    let mut tables = std::collections::BTreeSet::new();
    let mut amounts = std::collections::BTreeSet::new();
    let mut times = std::collections::BTreeSet::new();
    for transaction in &generated {
        if transaction.outcome == Outcome::RolledBack {
            classes.insert("rollback");
        }
        let mut touched = std::collections::BTreeSet::new();
        for statement in &transaction.statements {
            match &statement.operation {
                Operation::Insert { table, row } => {
                    classes.insert("insert");
                    touched.insert(table);
                    amounts.insert(&row.numeric);
                    times.insert(&row.timestamp);
                    assert!(row.toast.text.len() >= 8192);
                    assert!(row.toast.bytes.len() >= 16_384);
                }
                Operation::Update {
                    table,
                    new_key,
                    toast,
                    ..
                } => {
                    classes.insert("update");
                    touched.insert(table);
                    if new_key.is_some() {
                        classes.insert("key-change");
                    }
                    classes.insert(if toast.is_some() {
                        "toast-update"
                    } else {
                        "toast-untouched"
                    });
                }
                Operation::Delete { table, .. } => {
                    classes.insert("delete");
                    touched.insert(table);
                }
                Operation::Truncate { tables } => {
                    classes.insert("truncate");
                    if tables.len() > 1 {
                        classes.insert("multi-truncate");
                    }
                    touched.extend(tables);
                }
            }
        }
        if touched.len() > 1 {
            classes.insert("multi-table");
        }
        tables.extend(touched);
    }
    assert_eq!(
        classes,
        [
            "delete",
            "insert",
            "key-change",
            "multi-table",
            "multi-truncate",
            "rollback",
            "toast-untouched",
            "toast-update",
            "truncate",
            "update"
        ]
        .into_iter()
        .collect()
    );
    assert_eq!(tables.len(), 3);
    assert_eq!(amounts.len(), 4);
    assert_eq!(times.len(), 4);
}

#[test]
fn hot_range_knob_concentrates_all_keys_inside_chunk_window() {
    let config = Config {
        transactions: 240,
        hot_range: -3..=-2,
        hot_percent: 100,
        ..small_config(55)
    };
    for transaction in workload::generate(&config) {
        for statement in transaction.statements {
            match statement.operation {
                Operation::Insert { row, .. } => assert!(config.hot_range.contains(&row.key)),
                Operation::Update { key, new_key, .. } => {
                    assert!(config.hot_range.contains(&key));
                    if let Some(key) = new_key {
                        assert!(config.hot_range.contains(&key));
                    }
                }
                Operation::Delete { key, .. } => assert!(config.hot_range.contains(&key)),
                Operation::Truncate { .. } => {}
            }
        }
    }
    let cold = workload::generate(&Config {
        hot_percent: 0,
        ..config.clone()
    });
    assert!(cold.iter().flat_map(|t| &t.statements).any(|s| matches!(&s.operation, Operation::Insert { row, .. } if !config.hot_range.contains(&row.key))));
}

#[test]
fn toast_untouched_update_omits_toast_columns_and_literals_are_escaped() {
    let sql = Statement::new(update("t", 1, None, 1)).sql;
    assert!(!sql.contains("body"));
    assert!(!sql.contains("payload"));
    let mut value = row(1, 0);
    value.toast.text = "it's a backslash \\ and newline\n".into();
    assert!(
        Statement::new(Operation::Insert {
            table: "t".into(),
            row: value
        })
        .sql
        .contains("it''s")
    );
}

#[test]
fn generator_mutates_preexisting_rows_across_transactions() {
    let generated = workload::generate(&Config {
        hot_percent: 100,
        ..small_config(1)
    });
    assert!(generated.iter().any(|transaction| matches!(
        transaction.statements.first().map(|s| &s.operation),
        Some(Operation::Update { .. } | Operation::Delete { .. })
    )));
}

#[test]
fn oracle_preserves_and_replaces_toast_as_requested() {
    let changed = Toast {
        text: "replacement".into(),
        bytes: "abcd".into(),
    };
    let oracle = Oracle::replay(&[
        committed(1, 10, vec![insert("a", 1, 10)]),
        committed(2, 20, vec![update("a", 1, None, 2)]),
    ]);
    assert_eq!(oracle.expected[&("a".into(), 1)].toast, row(1, 10).toast);
    let oracle = Oracle::replay(&[
        committed(1, 10, vec![insert("a", 1, 10)]),
        committed(
            2,
            20,
            vec![Operation::Update {
                table: "a".into(),
                key: 1,
                new_key: None,
                delta: 2,
                toast: Some(changed.clone()),
            }],
        ),
    ]);
    assert_eq!(oracle.expected[&("a".into(), 1)].toast, changed);
}

#[test]
fn replay_orders_lsn_ignores_rollbacks_and_handles_key_changes_and_truncates() {
    let first = committed(
        1,
        10,
        vec![insert("a", 1, 10), insert("b", 1, 20), insert("c", 1, 30)],
    );
    let second = committed(
        2,
        20,
        vec![
            update("a", 1, Some(2), 7),
            Operation::Delete {
                table: "b".into(),
                key: 1,
            },
        ],
    );
    let mut rollback = committed(
        3,
        30,
        vec![Operation::Truncate {
            tables: vec!["a".into(), "c".into()],
        }],
    );
    rollback.outcome = Outcome::RolledBack;
    let fourth = committed(
        4,
        40,
        vec![
            Operation::Truncate {
                tables: vec!["b".into(), "c".into()],
            },
            insert("b", 5, 50),
        ],
    );
    let oracle = Oracle::replay(&[fourth, second, rollback, first]);
    let expected = [(("a".into(), 2), row(2, 17)), (("b".into(), 5), row(5, 50))]
        .into_iter()
        .collect();
    assert_eq!(oracle.expected, expected);
    assert!(oracle.compare(&expected).is_empty());
    assert!(oracle.compare(&expected).anomalies.is_empty());
}

#[test]
fn equal_lsn_preserves_acknowledged_history_order() {
    let oracle = Oracle::replay(&[
        committed(9, 10, vec![insert("a", 1, 1)]),
        committed(1, 10, vec![update("a", 1, None, 2)]),
    ]);
    assert_eq!(oracle.expected[&("a".into(), 1)].counter, 3);
}

#[test]
fn detects_missing_transaction() {
    let oracle = Oracle::replay(&[committed(
        1,
        10,
        vec![insert("a", 1, 10), insert("b", 2, 20)],
    )]);
    let diff = oracle.compare(&Default::default());
    assert_eq!(diff.rows.len(), 2);
    assert!(
        diff.anomalies
            .contains(&Anomaly::MissingTransaction { txn_id: 1 })
    );
}

#[test]
fn detects_duplicated_additive_effect() {
    let oracle = Oracle::replay(&[
        committed(1, 10, vec![insert("a", 1, 10)]),
        committed(
            2,
            20,
            vec![update("a", 1, None, 3), update("a", 1, None, 4)],
        ),
    ]);
    let mut actual = oracle.expected.clone();
    actual.get_mut(&("a".into(), 1)).unwrap().counter += 7;
    assert!(
        oracle
            .compare(&actual)
            .anomalies
            .contains(&Anomaly::DuplicatedEffect {
                txn_id: 2,
                table: "a".into(),
                key: 1
            })
    );
}

#[test]
fn detects_partial_multi_table_transaction() {
    let oracle = Oracle::replay(&[committed(
        1,
        10,
        vec![insert("a", 1, 10), insert("b", 1, 20)],
    )]);
    let mut actual = oracle.expected.clone();
    actual.remove(&("b".into(), 1));
    assert!(
        oracle
            .compare(&actual)
            .anomalies
            .contains(&Anomaly::PartialTransaction { txn_id: 1 })
    );
}

#[test]
fn detects_resurrected_delete_and_truncate() {
    for operation in [
        Operation::Delete {
            table: "a".into(),
            key: 1,
        },
        Operation::Truncate {
            tables: vec!["a".into()],
        },
    ] {
        let oracle = Oracle::replay(&[
            committed(1, 10, vec![insert("a", 1, 10)]),
            committed(2, 20, vec![operation]),
        ]);
        let actual = [(("a".into(), 1), row(1, 10))].into_iter().collect();
        assert!(
            oracle
                .compare(&actual)
                .anomalies
                .contains(&Anomaly::ResurrectedDelete {
                    txn_id: 2,
                    table: "a".into(),
                    key: 1
                })
        );
    }
}

#[test]
fn detects_stale_overwrite() {
    let oracle = Oracle::replay(&[
        committed(1, 10, vec![insert("a", 1, 10)]),
        committed(2, 20, vec![update("a", 1, None, 5)]),
    ]);
    let actual = [(("a".into(), 1), row(1, 10))].into_iter().collect();
    assert!(
        oracle
            .compare(&actual)
            .anomalies
            .contains(&Anomaly::StaleOverwrite {
                txn_id: 2,
                table: "a".into(),
                key: 1
            })
    );
}

#[test]
fn unknown_corruption_still_produces_a_row_diff() {
    let oracle = Oracle::replay(&[committed(1, 10, vec![insert("a", 1, 10)])]);
    let mut actual = oracle.expected.clone();
    actual.get_mut(&("a".into(), 1)).unwrap().toast.text = "unexplained corruption".into();
    assert_eq!(oracle.compare(&actual).rows.len(), 1);
}

#[test]
fn history_json_lines_round_trip_and_lsn_parsing() {
    let path = support::scratch("campaign-history").join("history.jsonl");
    let history = vec![committed(
        1,
        workload::parse_lsn("16/B374D848").unwrap(),
        vec![insert("a", 1, 10)],
    )];
    let text = history
        .iter()
        .map(|entry| serde_json::to_string(entry).unwrap() + "\n")
        .collect::<String>();
    std::fs::write(&path, text).unwrap();
    assert_eq!(workload::read_history(&path).unwrap(), history);
    assert_eq!(
        workload::parse_lsn("16/B374D848").unwrap(),
        0x0016_b374_d848
    );
    for malformed in ["1", "GG/1", "1/100000000", "100000000/0"] {
        assert!(workload::parse_lsn(malformed).is_err());
    }
}

#[test]
fn oracle_has_no_implementation_imports() {
    let source = include_str!("support/oracle.rs");
    for forbidden in ["skedaddle::", "crate::"] {
        assert!(
            !source.contains(forbidden),
            "oracle must remain independent: {forbidden}"
        );
    }
}

/// Support smoke test only: a second PG connection reads the workload database
/// as the destination. Replication and fault injection belong to later items.
#[tokio::test]
async fn live_generator_and_independent_sql_oracle_round_trip() {
    let Some(url) = workload::harness_url("SKEDADDLE_CAMPAIGN_DATABASE_URL") else {
        return;
    };
    let (mut writer, write_connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();
    let writer_task = tokio::spawn(write_connection);
    let (mut reader, read_connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();
    let reader_task = tokio::spawn(read_connection);
    let schema = format!(
        "campaign_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    writer.batch_execute(&format!("CREATE SCHEMA {schema}; SET search_path TO {schema}; SET standard_conforming_strings=on")).await.unwrap();
    let result: Result<(), Box<dyn std::error::Error>> = async {
        reader
            .batch_execute(&format!("SET search_path TO {schema}"))
            .await?;
        workload::create_tables(&writer).await?;
        let path = support::scratch("campaign-live").join("history.jsonl");
        let generated = workload::generate(&Config {
            transactions: 17,
            ..Config::default()
        });
        let history = workload::execute(&mut writer, &generated, &path).await?;
        if history != workload::read_history(&path)? {
            return Err("persisted history differs from acknowledged history".into());
        }
        let oracle = Oracle::replay(&history);
        let actual = read_destination(&mut reader, &TABLES).await?;
        let diff = oracle.compare(&actual);
        if !diff.is_empty() {
            return Err(format!("independent SQL round trip differs: {diff:?}").into());
        }
        Ok(())
    }
    .await;
    let cleanup = writer
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await;
    drop(writer);
    drop(reader);
    writer_task.abort();
    reader_task.abort();
    result.unwrap();
    cleanup.unwrap();
}
