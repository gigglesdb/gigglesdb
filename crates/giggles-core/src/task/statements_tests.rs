use super::definition::NewTask;
use super::definition::{DisclosureCaps, LoadLimits, MutationCaps, PredicateText, TaskPrivileges};
use super::statements::{command_tag, parse, TaskStatement};
use crate::relational::Value;
use std::collections::BTreeSet;

const INC_436: &str = r#"CREATE TASK "inc-436"
  EXECUTOR repair_agent APPROVER oncall_lead
  ON shipments
  VISIBLE ROWS WHERE (merchant_id = 42)
  READABLE (id, status, failed_at, attempt_count)
  MASKED (customer_email)
  ALLOW UPDATE (status)
    OLD ROW WHERE (status = 'failed' AND failed_at >= '2026-11-05 17:00:00+00'
                   AND failed_at < '2026-11-05 17:07:00+00')
    NEW ROW WHERE (status = 'queued')
    AT MOST 25 DISTINCT ROWS AND 25 TRANSITIONS
    APPROVE ABOVE 5
  DISCLOSE AT MOST 1000 ROWS READ, 2097152 BYTES RETURNED
  WITH (statement_ms = 2000, rows_scanned = 100000, concurrent_statements = 2)
  EXPIRES '2026-11-05 17:30:00+00';"#;

const MINIMAL: &str = "CREATE TASK t EXECUTOR a ON tbl VISIBLE ROWS WHERE (true) \
     DISCLOSE AT MOST 10 ROWS READ, 100 BYTES RETURNED EXPIRES '2030-01-01 00:00:00+00'";

fn create(sql: &str) -> NewTask {
    match parse(sql) {
        Some(TaskStatement::Create(t)) => *t,
        other => panic!("expected CREATE TASK, got {other:?}"),
    }
}

fn error(sql: &str) -> &'static str {
    match parse(sql) {
        Some(TaskStatement::Error(code)) => code,
        other => panic!("expected an error for {sql:?}, got {other:?}"),
    }
}

fn names(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

#[test]
fn parses_inc_436_worked_example() {
    let stmt = parse(INC_436).expect("task statement");
    assert_eq!(command_tag(&stmt), "CREATE TASK");
    let t = create(INC_436);
    assert_eq!(t.name, "inc-436");
    assert_eq!(t.executor, "repair_agent");
    assert_eq!(t.approver.as_deref(), Some("oncall_lead"));
    assert_eq!(t.schema, None);
    assert_eq!(t.table, "shipments");
    assert_eq!(t.visible_rows, PredicateText("merchant_id = 42".into()));
    assert_eq!(
        t.readable,
        names(&["id", "status", "failed_at", "attempt_count"])
    );
    assert_eq!(t.masked, vec![("customer_email".to_string(), None)]);
    assert!(t.untrusted_text.is_empty());
    let w = t.allowed_write.as_ref().expect("allowed write");
    assert_eq!(w.columns, BTreeSet::from(["status".to_string()]));
    // The exact source text, newline and indentation included.
    let old_start = INC_436.find("status = 'failed'").unwrap();
    let old_end = INC_436.find("17:07:00+00'").unwrap() + "17:07:00+00'".len();
    assert_eq!(w.old_row, PredicateText(INC_436[old_start..old_end].into()));
    assert_eq!(
        w.old_row.0.split_whitespace().collect::<Vec<_>>().join(" "),
        "status = 'failed' AND failed_at >= '2026-11-05 17:00:00+00' \
         AND failed_at < '2026-11-05 17:07:00+00'"
    );
    assert_eq!(w.new_row, PredicateText("status = 'queued'".into()));
    assert_eq!(
        t.mutation_caps,
        MutationCaps {
            distinct_rows: 25,
            transitions: 25
        }
    );
    assert_eq!(t.approval_threshold, 5);
    assert_eq!(
        t.disclosure_caps,
        DisclosureCaps {
            rows_read: 1000,
            rows_returned: 1000,
            bytes_returned: 2_097_152
        }
    );
    assert_eq!(
        t.limits,
        LoadLimits {
            statement_ms: 2000,
            rows_scanned: 100_000,
            concurrent_statements: 2,
            ..LoadLimits::default()
        }
    );
    assert_eq!(t.expires, "2026-11-05 17:30:00+00");

    assert_eq!(
        parse("SHOW TASK \"inc-436\";"),
        Some(TaskStatement::Show(Some("inc-436".into())))
    );
    let revoke = parse("REVOKE TASK \"inc-436\";").unwrap();
    assert_eq!(revoke, TaskStatement::Revoke("inc-436".into()));
    assert_eq!(command_tag(&revoke), "REVOKE TASK");
}

#[test]
fn quoted_and_folded_names() {
    let t = create(
        "create task Inc_1 executor Repair_Agent approver \"OnCall\" on Public.\"Shipments\" \
         visible rows where (true) readable (ID, \"Mixed\") \
         disclose at most 1 rows read, 1 bytes returned expires '2030-01-01 00:00:00+00'",
    );
    assert_eq!(t.name, "inc_1");
    assert_eq!(t.executor, "repair_agent");
    assert_eq!(t.approver.as_deref(), Some("OnCall"));
    assert_eq!(t.schema.as_deref(), Some("public"));
    assert_eq!(t.table, "Shipments");
    assert_eq!(t.readable, names(&["id", "Mixed"]));

    assert_eq!(
        parse("show task \"A\"\"b\""),
        Some(TaskStatement::Show(Some("A\"b".into())))
    );
    assert_eq!(parse("SHOW TASKS"), Some(TaskStatement::Show(None)));
    let g = parse("GRANT undo, INSPECT ON TASK \"inc-436\" TO Oncall_Lead, \"Auditor\"").unwrap();
    assert_eq!(
        g,
        TaskStatement::Grant {
            grant: true,
            bits: TaskPrivileges::UNDO | TaskPrivileges::INSPECT,
            task: "inc-436".into(),
            roles: names(&["oncall_lead", "Auditor"]),
        }
    );
    assert_eq!(command_tag(&g), "GRANT");
    let r = parse("revoke inspect on task t from x;").unwrap();
    assert_eq!(
        r,
        TaskStatement::Grant {
            grant: false,
            bits: TaskPrivileges::INSPECT,
            task: "t".into(),
            roles: names(&["x"]),
        }
    );
    assert_eq!(command_tag(&r), "REVOKE");
    assert_eq!(command_tag(&TaskStatement::Show(None)), "SHOW");
    // GRANT uses TO, REVOKE uses FROM.
    assert_eq!(error("grant undo on task t from x"), "42601");
}

#[test]
fn rows_returned_defaults_to_rows_read() {
    let t = create(MINIMAL);
    assert_eq!(
        t.disclosure_caps,
        DisclosureCaps {
            rows_read: 10,
            rows_returned: 10,
            bytes_returned: 100
        }
    );
    let t = create(&MINIMAL.replace(
        "10 ROWS READ, 100 BYTES",
        "10 ROWS READ, 4 ROWS RETURNED, 100 BYTES",
    ));
    assert_eq!(
        t.disclosure_caps,
        DisclosureCaps {
            rows_read: 10,
            rows_returned: 4,
            bytes_returned: 100
        }
    );
    assert_eq!(t.limits, LoadLimits::default());
}

#[test]
fn read_only_task_without_allow_update() {
    let t = create(MINIMAL);
    assert_eq!(t.name, "t");
    assert_eq!(t.executor, "a");
    assert_eq!(t.approver, None);
    assert_eq!(t.visible_rows, PredicateText("true".into()));
    assert_eq!(t.allowed_write, None);
    assert_eq!(t.mutation_caps, MutationCaps::default());
    assert_eq!(t.approval_threshold, 0);
    assert!(t.readable.is_empty() && t.masked.is_empty() && t.untrusted_text.is_empty());
    assert_eq!(t.expires, "2030-01-01 00:00:00+00");
}

#[test]
fn masked_with_constant() {
    let t = create(&MINIMAL.replace(
        "DISCLOSE",
        "MASKED (email = 'x@example.com', n = -7, flag = true, plain, z = 0) \
         UNTRUSTED TEXT (notes) DISCLOSE",
    ));
    assert_eq!(
        t.masked,
        vec![
            (
                "email".to_string(),
                Some(Value::Text("x@example.com".into()))
            ),
            ("n".to_string(), Some(Value::Int(-7))),
            ("flag".to_string(), Some(Value::Bool(true))),
            ("plain".to_string(), None),
            ("z".to_string(), Some(Value::Int(0))),
        ]
    );
    assert_eq!(t.untrusted_text, names(&["notes"]));
    assert_eq!(
        error(&MINIMAL.replace("DISCLOSE", "MASKED (email = ) DISCLOSE")),
        "42601"
    );
}

#[test]
fn with_limits_unknown_name_42601() {
    assert_eq!(
        error(&MINIMAL.replace("EXPIRES", "WITH (statement_msx = 5) EXPIRES")),
        "42601"
    );
    assert_eq!(
        error(&MINIMAL.replace(
            "EXPIRES",
            "WITH (statement_ms = 5, statement_ms = 6) EXPIRES"
        )),
        "42601"
    );
    let t = create(&MINIMAL.replace(
        "EXPIRES",
        "WITH (lock_wait_ms = 5, pending_write_bytes = 6, stored_previews = 7, preview_ttl_s = 8) EXPIRES",
    ));
    assert_eq!(t.limits.lock_wait_ms, 5);
    assert_eq!(t.limits.pending_write_bytes, 6);
    assert_eq!(t.limits.stored_previews, 7);
    assert_eq!(t.limits.preview_ttl_s, 8);
}

#[test]
fn duplicate_clause_42601() {
    assert_eq!(
        error(&MINIMAL.replace("ON tbl", "ON tbl READABLE (a) READABLE (b)")),
        "42601"
    );
    assert_eq!(
        error(&MINIMAL.replace("EXECUTOR a", "EXECUTOR a EXECUTOR b")),
        "42601"
    );
    // Missing mandatory clauses.
    assert_eq!(
        error(&MINIMAL.replace(" EXPIRES '2030-01-01 00:00:00+00'", "")),
        "42601"
    );
    assert_eq!(error(&MINIMAL.replace("EXECUTOR a ", "")), "42601");
    assert_eq!(
        error(&MINIMAL.replace("VISIBLE ROWS WHERE (true) ", "")),
        "42601"
    );
    // Unbalanced or empty predicate.
    assert_eq!(error(&MINIMAL.replace("(true)", "((true)")), "42601");
    assert_eq!(error(&MINIMAL.replace("(true)", "()")), "42601");
}

#[test]
fn trailing_statement_42601() {
    assert_eq!(error("REVOKE TASK x; SELECT 1"), "42601");
    assert_eq!(error("SHOW TASKS extra"), "42601");
    assert_eq!(error(&format!("{MINIMAL}; DROP TABLE t")), "42601");
    assert_eq!(
        error("GRANT UNDO ON TASK t TO r; GRANT UNDO ON TASK t TO s"),
        "42601"
    );
}

#[test]
#[allow(non_snake_case)]
fn grant_approve_0A000() {
    assert_eq!(error("GRANT APPROVE ON TASK t TO r"), "0A000");
    assert_eq!(error("grant undo, approve on task t to r"), "0A000");
    assert_eq!(error("REVOKE APPROVE ON TASK t FROM r"), "0A000");
}

#[test]
fn non_task_statements_return_none() {
    for sql in [
        "REVOKE SELECT ON t FROM r",
        "SHOW search_path",
        "CREATE TABLE task (x int)",
        "GRANT SELECT ON task TO r",
        "SELECT 1",
        "",
        "CREATE ROLE task",
        "REVOKE task FROM bob",
        "REVOKE task, other FROM bob",
    ] {
        assert_eq!(parse(sql), None, "{sql}");
    }
}

// R3-707: the six D7.1 forms.
mod change_statements {
    use super::super::statements::{parse_change_statement, ChangeStatement};
    use crate::relational::SqlError;

    fn hex() -> String {
        "0123456789abcdef".repeat(4)
    }

    fn digest() -> [u8; 32] {
        let mut out = [0u8; 32];
        for (i, b) in out.iter_mut().enumerate() {
            *b = [0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef][i % 8];
        }
        out
    }

    #[test]
    fn six_forms_parse() {
        let h = hex();
        let cases = [
            (
                "PREVIEW UPDATE shipments SET status = $1 WHERE id = $2".to_owned(),
                ChangeStatement::Preview {
                    update: "UPDATE shipments SET status = $1 WHERE id = $2".into(),
                },
            ),
            (
                format!("apply change 7 digest '{}';", h.to_ascii_uppercase()),
                ChangeStatement::Apply {
                    change: 7,
                    digest: digest(),
                },
            ),
            (
                "SHOW CHANGES OF TASK \"inc-436\"".to_owned(),
                ChangeStatement::ShowChanges {
                    task: "inc-436".into(),
                },
            ),
            (
                "show change 3 of task Inc".to_owned(),
                ChangeStatement::ShowChange {
                    change: 3,
                    task: "inc".into(),
                },
            ),
            (
                format!("APPROVE CHANGE 3 OF TASK \"inc-436\" DIGEST '{h}'"),
                ChangeStatement::Approve {
                    change: 3,
                    task: "inc-436".into(),
                    digest: digest(),
                },
            ),
            (
                "REVOKE APPROVAL OF CHANGE 3 OF TASK \"inc-436\" ;".to_owned(),
                ChangeStatement::RevokeApproval {
                    change: 3,
                    task: "inc-436".into(),
                },
            ),
        ];
        for (sql, expected) in cases {
            assert_eq!(parse_change_statement(&sql), Some(Ok(expected.clone())), "{sql}");
            assert_eq!(
                expected.is_approver_form(),
                !matches!(expected, ChangeStatement::Preview { .. } | ChangeStatement::Apply { .. })
            );
        }
    }

    #[test]
    fn other_statements_keep_their_path() {
        for sql in [
            "SHOW TASK \"inc-436\"",
            "SHOW search_path",
            "REVOKE approval FROM alice",
            "REVOKE task FROM alice",
            "PREVIEW SELECT 1",
            "APPLY 1",
            "APPROVE 1",
            "UPDATE shipments SET status = 'x'",
            "",
        ] {
            assert_eq!(parse_change_statement(sql), None, "{sql}");
        }
    }

    #[test]
    fn malformed_forms_are_42601_and_bad_digests_22023() {
        let h = hex();
        for sql in [
            "SHOW CHANGES OF TASK inc-436".to_owned(),
            "SHOW CHANGES TASK x".to_owned(),
            "SHOW CHANGE x OF TASK t".to_owned(),
            "SHOW CHANGE -1 OF TASK t".to_owned(),
            "SHOW CHANGE 1x OF TASK t".to_owned(),
            format!("APPROVE CHANGE 1 OF TASK t DIGEST {h}"),
            format!("APPROVE CHANGE 1 OF TASK t '{h}'"),
            format!("APPROVE CHANGE 1 OF TASK t DIGEST '{h}' extra"),
            "APPROVE CHANGE 1 OF TASK t".to_owned(),
            "REVOKE APPROVAL OF CHANGE 1".to_owned(),
            format!("APPLY CHANGE 99999999999999999999 DIGEST '{h}'"),
            "APPLY CHANGE 1 DIGEST 'abc".to_owned(),
        ] {
            assert_eq!(parse_change_statement(&sql), Some(Err(SqlError::Syntax)), "{sql}");
        }
        let short = &h[..63];
        let odd = format!("{}g", &h[..63]);
        for bad in [short.to_owned(), format!("{h}0"), odd, String::new(), format!("\\x{}", &h[..62])] {
            for sql in [
                format!("APPLY CHANGE 1 DIGEST '{bad}'"),
                format!("APPROVE CHANGE 1 OF TASK t DIGEST '{bad}'"),
            ] {
                assert_eq!(
                    parse_change_statement(&sql),
                    Some(Err(SqlError::InvalidParameterValue)),
                    "{sql}"
                );
            }
        }
    }

    // R3-804: the three undo forms.
    #[test]
    fn undo_forms_parse() {
        use crate::task::{OperationId, UndoMode};
        let h = hex();
        let cases = [
            (
                "PREVIEW UNDO OF TASK \"inc-436\"".to_owned(),
                ChangeStatement::PreviewUndo { task: "inc-436".into(), operation: None },
            ),
            (
                "preview undo of task t operation 'a1';".to_owned(),
                ChangeStatement::PreviewUndo {
                    task: "t".into(),
                    operation: OperationId::new("a1"),
                },
            ),
            (
                "SHOW UNDO 4 OF TASK \"inc-436\"".to_owned(),
                ChangeStatement::ShowUndo { undo: 4, task: "inc-436".into() },
            ),
            (
                format!("APPROVE UNDO 4 OF TASK t DIGEST '{h}' MODE COMPLETE"),
                ChangeStatement::ApproveUndo {
                    undo: 4,
                    task: "t".into(),
                    digest: digest(),
                    mode: UndoMode::Complete,
                },
            ),
            (
                format!("approve undo 4 of task t digest '{h}' mode partial ;"),
                ChangeStatement::ApproveUndo {
                    undo: 4,
                    task: "t".into(),
                    digest: digest(),
                    mode: UndoMode::Partial,
                },
            ),
        ];
        for (sql, expected) in cases {
            assert_eq!(parse_change_statement(&sql), Some(Ok(expected.clone())), "{sql}");
            assert!(expected.is_approver_form() && expected.is_undo_form(), "{sql}");
        }
    }

    #[test]
    fn undo_mode_keyword_is_required() {
        let h = hex();
        for sql in [
            format!("APPROVE UNDO 4 OF TASK t DIGEST '{h}'"),
            format!("APPROVE UNDO 4 OF TASK t DIGEST '{h}' COMPLETE"),
            format!("APPROVE UNDO 4 OF TASK t DIGEST '{h}' MODE"),
            format!("APPROVE UNDO 4 OF TASK t DIGEST '{h}' MODE FULL"),
            format!("APPROVE UNDO 4 OF TASK t MODE COMPLETE DIGEST '{h}'"),
            format!("APPROVE UNDO 4 OF TASK t DIGEST '{h}' MODE PARTIAL extra"),
            "PREVIEW UNDO OF TASK".to_owned(),
            "PREVIEW UNDO TASK t".to_owned(),
            "PREVIEW UNDO OF TASK t OPERATION".to_owned(),
            "PREVIEW UNDO OF TASK t OPERATION ''".to_owned(),
            "PREVIEW UNDO OF TASK t OPERATION a1".to_owned(),
            format!("PREVIEW UNDO OF TASK t OPERATION '{}'", "x".repeat(129)),
            "SHOW UNDO OF TASK t".to_owned(),
            "SHOW UNDO 1 OF TASK".to_owned(),
        ] {
            assert_eq!(parse_change_statement(&sql), Some(Err(SqlError::Syntax)), "{sql}");
        }
        // The digest is checked once the form has parsed.
        assert_eq!(
            parse_change_statement("APPROVE UNDO 4 OF TASK t DIGEST 'xyz' MODE COMPLETE"),
            Some(Err(SqlError::InvalidParameterValue))
        );
    }
}

#[test]
fn credential_parse_tags_and_description() {
    use super::statements::description;
    use crate::relational::DataType;
    for (sql, task, expires) in [
        ("ISSUE CREDENTIAL FOR TASK t", "t", None),
        (
            "ISSUE CREDENTIAL FOR TASK \"Mixed task\" EXPIRES '2030-01-01 00:00:00+00'",
            "Mixed task",
            Some("2030-01-01 00:00:00+00"),
        ),
    ] {
        let stmt = parse(sql).unwrap();
        assert_eq!(
            stmt,
            TaskStatement::IssueCredential {
                task: task.into(),
                expires: expires.map(str::to_owned)
            }
        );
        assert_eq!(command_tag(&stmt), "ISSUE CREDENTIAL");
        let d = description(&stmt);
        assert_eq!(
            d.columns
                .iter()
                .map(|c| (c.name.as_str(), c.data_type.clone()))
                .collect::<Vec<_>>(),
            vec![
                ("credential_id", DataType::BigInt),
                ("task", DataType::Text),
                ("secret", DataType::Text),
                ("expires_at", DataType::TimestampTz)
            ]
        );
    }
    let revoke = parse("REVOKE CREDENTIAL 42;").unwrap();
    assert_eq!(revoke, TaskStatement::RevokeCredential(42));
    assert_eq!(command_tag(&revoke), "REVOKE CREDENTIAL");
    assert!(description(&revoke).columns.is_empty());
    assert_eq!(parse("REVOKE credential FROM r"), None);
    for sql in [
        "ISSUE CREDENTIAL",
        "ISSUE CREDENTIAL FOR TASK t EXPIRES 42",
        "ISSUE CREDENTIAL FOR TASK t garbage",
        "REVOKE CREDENTIAL -1",
        "REVOKE CREDENTIAL 18446744073709551616",
        "REVOKE CREDENTIAL 1.5",
        "REVOKE CREDENTIAL 42 garbage",
    ] {
        assert_eq!(error(sql), "42601");
    }
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn credential_execute_show_and_revoke() {
    use super::{
        definition::WallClockMs,
        statements::{TaskOutcome, execute},
    };
    use crate::{
        Admin, Engine, Limits,
        roles::{NewRole, TablePrivilege},
    };
    let dir = std::env::temp_dir().join(format!("credential-statements-{}", std::process::id()));
    let admin = Admin::create_engine(&dir, Limits::default(), Engine::V2).unwrap();
    admin
        .sql_session()
        .execute("CREATE TABLE tbl (id bigint primary key)", &[], None)
        .unwrap();
    for name in ["issuer", "other", "a"] {
        admin
            .roles()
            .create(NewRole {
                name: name.into(),
                login: true,
                ..NewRole::default()
            })
            .unwrap();
    }
    admin.roles().set_task_only("a", true).unwrap();
    admin
        .roles()
        .grant_table("tbl", "issuer", &[TablePrivilege::Select])
        .unwrap();
    let issuer = admin.roles().get("issuer").unwrap().unwrap();
    let other = admin.roles().get("other").unwrap().unwrap();
    let now = WallClockMs(1_793_898_000_000);
    execute(&admin, &issuer, &parse(MINIMAL).unwrap(), now).unwrap();
    let issue = parse("ISSUE CREDENTIAL FOR TASK t").unwrap();
    assert_eq!(
        execute(&admin, &other, &issue, now).unwrap_err().sqlstate,
        "42501"
    );
    assert_eq!(
        execute(
            &admin,
            &issuer,
            &parse("ISSUE CREDENTIAL FOR TASK t EXPIRES 'bad'").unwrap(),
            now
        )
        .unwrap_err()
        .sqlstate,
        "22007"
    );
    for expiry in [None, Some("2029-01-01 00:00:00+00")] {
        let sql = expiry.map_or_else(
            || "ISSUE CREDENTIAL FOR TASK t".to_owned(),
            |ts| format!("ISSUE CREDENTIAL FOR TASK t EXPIRES '{ts}'"),
        );
        let outcome = execute(&admin, &issuer, &parse(&sql).unwrap(), now).unwrap();
        let debug = format!("{outcome:?}");
        let TaskOutcome::Rows(_, issued) = outcome else {
            panic!("expected rows")
        };
        let Value::Int(id) = issued.rows[0].values[0] else {
            panic!("expected id")
        };
        let Value::Text(secret) = &issued.rows[0].values[2] else {
            panic!("expected secret")
        };
        assert!(secret.starts_with("ggtc1_"));
        assert!(!debug.contains(secret));
        assert_eq!(issued.rows[0].values[3] == Value::Null, expiry.is_none());
        let show = parse("SHOW TASK t").unwrap();
        let TaskOutcome::Rows(d, shown) = execute(&admin, &issuer, &show, now).unwrap() else {
            panic!("expected rows")
        };
        assert_eq!(d.columns.len(), 26);
        assert_eq!(shown.columns[25], "credentials");
        let Value::Text(list) = &shown.rows[0].values[25] else {
            panic!("expected list")
        };
        assert!(list.starts_with(&format!("{id} expires ")));
        assert!(!list.contains(secret));
        assert!(!format!("{shown:?}").contains(secret));
        let revoke = TaskStatement::RevokeCredential(id as u64);
        assert_eq!(
            execute(&admin, &other, &revoke, now).unwrap_err().sqlstate,
            "42501"
        );
        execute(&admin, &issuer, &revoke, now).unwrap();
        let TaskOutcome::Rows(_, shown) = execute(&admin, &issuer, &show, now).unwrap() else {
            panic!("expected rows")
        };
        assert_eq!(shown.rows[0].values[25], Value::Text(String::new()));
    }
    drop(admin);
    std::fs::remove_dir_all(dir).unwrap();
}
