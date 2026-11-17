//! R3-207 (GD-0061 design-2 section 5, coordinator ruling R2): the SQL surface
//! for operation IDs, preview, apply and outcome lookup on a task session. The
//! forms parse and are admitted by class. Since R3-626 `Update` and
//! `OutcomeLookup` are enabled: `OPERATION '<id>' UPDATE ...` runs the protected
//! commit and `SHOW OPERATION` returns its outcome. Preview and apply (Phase 7)
//! still refuse with 0A000 and their class message.
#![cfg(feature = "engine-rocksdb")]
mod agent_support;

use agent_support::{
    AGENT_STARTUP, AgentServer, DAY_MS, Msg, Wire, agent_wire, binding, certificate, connect_raw,
    error_of, now_ms, ordinary_wire, scratch, timestamptz,
};
use giggles_core::{
    Admin, Engine, Limits,
    relational::{DataType, Prepared, Value},
    roles::{NewRole, TablePrivilege},
    task::{
        DenyRule, StatementClass, TaskError,
        definition::{
            AllowedWrite, DisclosureCaps, LoadLimits, MutationCaps, NewTask, PredicateText,
            WallClockMs,
        },
    },
};
use giggles_server::agent::statement::{OperationId, ProtectedHead, description, parse};

fn not_allowed(class: StatementClass) -> (String, String) {
    (
        "0A000".to_owned(),
        TaskError::NotAllowedInTaskSession(class).message().to_owned(),
    )
}

#[track_caller]
fn assert_error(messages: &[Msg], code: &str, sql: &str) {
    assert_eq!(
        error_of(messages).map(|e| e.0),
        Some(code.to_owned()),
        "{sql}: {messages:?}"
    );
    assert_eq!(messages.last(), Some(&Msg::Ready(b'I')), "{sql}: {messages:?}");
}

fn digest() -> String {
    "0123456789abcdef".repeat(4)
}

#[tokio::test]
async fn bare_update_requires_operation_id() {
    let (_server, _agent, _ordinary, mut wire) = agent_wire().await;
    for sql in [
        "UPDATE shipments SET status = 'retry' WHERE id = 1",
        "update shipments set status = 'retry' where id = 1 returning id",
        "UPDATE shipments SET status = $1 WHERE id = $2",
    ] {
        let params: &[Option<&str>] = if sql.contains("$2") {
            &[Some("retry"), Some("1")]
        } else {
            &[]
        };
        for messages in [wire.simple(sql).await, wire.extended(sql, params, 0).await] {
            assert_eq!(
                error_of(&messages),
                Some((
                    "42T04".to_owned(),
                    TaskError::OperationIdRequired.message().to_owned()
                )),
                "{sql}: {messages:?}"
            );
        }
    }
}

#[tokio::test]
async fn operation_prefix_on_select_is_syntax_error() {
    let (_server, _agent, _ordinary, mut wire) = agent_wire().await;
    for sql in [
        "OPERATION 'a1' SELECT id FROM shipments",
        "OPERATION 'a1' SELECT 1",
        "OPERATION 'a1' DELETE FROM shipments WHERE id = 1",
        "OPERATION 'a1' SHOW application_name",
        "OPERATION 'a1' OPERATION 'a2' UPDATE shipments SET status = 'x' WHERE id = 1",
        "OPERATION 'a1'",
        "PREVIEW SELECT id FROM shipments",
    ] {
        assert_error(&wire.simple(sql).await, "42601", sql);
        assert_error(&wire.extended(sql, &[], 0).await, "42601", sql);
    }
}

#[tokio::test]
async fn operation_id_charset_and_length_bounds() {
    let (_server, mut wire) = writable_wire().await;
    let longest = "a".repeat(128);
    for id in [
        "a",
        "A-z.0_9:x",
        "inc-436:retry.1",
        longest.as_str(),
    ] {
        assert!(OperationId::new(id).is_some(), "{id}");
        let sql = format!(
            "OPERATION '{id}' UPDATE shipments SET status = 'queued' \
             WHERE id = 1 AND status = 'failed'"
        );
        // It parses and commits; the resend under the same ID replays.
        for messages in [wire.simple(&sql).await, wire.extended(&sql, &[], 0).await] {
            assert_eq!(error_of(&messages), None, "{id}: {messages:?}");
            assert!(
                messages
                    .iter()
                    .any(|m| matches!(m, Msg::Complete(tag) if tag.starts_with("UPDATE "))),
                "{id}: {messages:?}"
            );
        }
    }
    let too_long = "a".repeat(129);
    for id in ["", "a b", "a/b", "a'b", "é", "a;b", "a\"b", too_long.as_str()] {
        assert!(OperationId::new(id).is_none(), "{id}");
        let quoted = id.replace('\'', "''");
        let sql = format!("OPERATION '{quoted}' UPDATE shipments SET status = 'x' WHERE id = 1");
        assert_error(&wire.simple(&sql).await, "42601", &sql);
        assert_error(&wire.extended(&sql, &[], 0).await, "42601", &sql);
    }
    // The ID is a string literal only.
    for sql in [
        "OPERATION a1 UPDATE shipments SET status = 'x' WHERE id = 1",
        "OPERATION \"a1\" UPDATE shipments SET status = 'x' WHERE id = 1",
        "OPERATION E'a1' UPDATE shipments SET status = 'x' WHERE id = 1",
        "OPERATION $1 UPDATE shipments SET status = 'x' WHERE id = 1",
    ] {
        assert_error(&wire.simple(sql).await, "42601", sql);
    }
}

#[tokio::test]
async fn prefix_survives_extended_parse_with_parameters() {
    let sql = "OPERATION 'a1' UPDATE shipments SET status = $1 WHERE id = $2 RETURNING $1::text";
    let (head, wrapped) = parse(sql).unwrap().unwrap();
    assert_eq!(
        head,
        ProtectedHead::Direct {
            op: OperationId::new("a1").unwrap()
        }
    );
    // The wrapped statement is a suffix of the Parse text: `$n` numbering is the client's.
    assert_eq!(
        wrapped,
        "UPDATE shipments SET status = $1 WHERE id = $2 RETURNING $1::text"
    );
    assert!(sql.ends_with(wrapped));
    assert_eq!(
        Prepared::task_class_of_sql(wrapped).unwrap(),
        StatementClass::Update
    );
    let preview = "PREVIEW UPDATE shipments SET status = $1 WHERE id = $2";
    assert_eq!(
        parse(preview).unwrap().unwrap().1,
        "UPDATE shipments SET status = $1 WHERE id = $2"
    );
    // Over the wire the prefixed Parse binds the client's `$n` (not a
    // parameter-count or syntax error), with or without declared types.
    let (server, mut wire) = writable_wire().await;
    let plain =
        "OPERATION 'p1' UPDATE shipments SET status = $1 WHERE id = $2 AND status = 'failed'";
    let bound = wire.extended(plain, &[Some("queued"), Some("1")], 0).await;
    assert_complete(&bound, "UPDATE 1", plain);
    assert_eq!(statuses(&server.admin)[0], "queued");
    let declared = wire.parse_describe(plain, &[25, 20]).await;
    assert_eq!(error_of(&declared), None, "{declared:?}");
    assert!(declared.contains(&Msg::Parameters(vec![25, 20])), "{declared:?}");
    let previewed = wire.parse_describe(preview, &[25, 20]).await;
    assert_eq!(error_of(&previewed), Some(not_allowed(StatementClass::Preview)));
}

#[tokio::test]
async fn preview_apply_outcome_forms_parse_and_describe() {
    let hex = digest();
    let apply = format!("OPERATION 'a2' APPLY CHANGE 17 DIGEST '{hex}'");
    let cases = [
        (
            "PREVIEW UPDATE shipments SET status = 'retry' WHERE id = 1".to_owned(),
            StatementClass::Preview,
            vec!["change_id", "digest", "row_count", "expires_at"],
        ),
        (apply.clone(), StatementClass::Apply, vec![]),
        (
            "SHOW OPERATION 'a2'".to_owned(),
            StatementClass::OutcomeLookup,
            vec![
                "operation_id",
                "decision",
                "row_count",
                "receipt_seq",
                "effects_digest",
            ],
        ),
    ];
    for (sql, class, columns) in &cases {
        let (head, _) = parse(sql).unwrap().unwrap();
        assert_eq!(head.class(), *class, "{sql}");
        let shape = description(&head, None);
        let names: Vec<_> = shape.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(&names, columns, "{sql}");
        assert_eq!(shape.typmods.len(), names.len());
    }
    let (head, _) = parse(&apply).unwrap().unwrap();
    let mut expected = [0u8; 32];
    hex::decode_to_slice(&hex, &mut expected).unwrap();
    assert_eq!(
        head,
        ProtectedHead::Apply {
            op: OperationId::new("a2").unwrap(),
            change: 17,
            digest: expected,
        }
    );
    let preview_shape = description(&ProtectedHead::Preview, None);
    let types: Vec<_> = preview_shape
        .columns
        .iter()
        .map(|c| c.data_type.clone())
        .collect();
    assert_eq!(
        types,
        [
            DataType::BigInt,
            DataType::Text,
            DataType::BigInt,
            DataType::TimestampTz
        ]
    );
    // On the agent listener each form reaches admission. Preview and apply are
    // refused by their class in both protocols, and Describe is never reached;
    // the outcome lookup is enabled and answers its columns (no row: unknown ID).
    let (_server, _agent, _ordinary, mut wire) = agent_wire().await;
    for (sql, class, columns) in &cases {
        for messages in [
            wire.simple(sql).await,
            wire.extended(sql, &[], 0).await,
            wire.parse_describe(sql, &[]).await,
        ] {
            if *class == StatementClass::OutcomeLookup {
                assert_eq!(error_of(&messages), None, "{sql}: {messages:?}");
                let names = columns.iter().map(|c| c.to_string()).collect();
                assert!(messages.contains(&Msg::Columns(names)), "{sql}: {messages:?}");
                assert!(!messages.iter().any(|m| matches!(m, Msg::Row(_))), "{sql}");
            } else {
                assert_eq!(error_of(&messages), Some(not_allowed(*class)), "{sql}");
            }
            assert_eq!(messages.last(), Some(&Msg::Ready(b'I')), "{sql}");
        }
    }
    // Ordinary SHOW still works.
    let show = wire.simple("SHOW application_name").await;
    assert_eq!(error_of(&show), None);
}

async fn run(wire: &mut agent_support::Wire, sql: &str, extended: bool) -> Vec<Msg> {
    if extended {
        wire.extended(sql, &[], 0).await
    } else {
        wire.simple(sql).await
    }
}

#[tokio::test]
async fn forms_are_syntax_errors_on_postgresql_listener() {
    let (server, _agent, ordinary, _wire) = agent_wire().await;
    let (listener, mut wire) = ordinary_wire(&server, &ordinary).await;
    let hex = digest();
    // The ordinary grammar is unchanged: each form is rejected exactly as the
    // same text with an unknown word is (this server answers 0A000 for an
    // unknown statement, 42601 for a malformed one and 42704 for an unknown
    // SHOW parameter), never as a task error.
    for (sql, unknown) in [
        (
            "OPERATION 'a1' UPDATE shipments SET status = 'x' WHERE id = 1".to_owned(),
            "FROBNICATE 'a1' UPDATE shipments SET status = 'x' WHERE id = 1".to_owned(),
        ),
        (
            "PREVIEW UPDATE shipments SET status = 'x' WHERE id = 1".to_owned(),
            "FROBNICATE UPDATE shipments SET status = 'x' WHERE id = 1".to_owned(),
        ),
        (
            format!("OPERATION 'a2' APPLY CHANGE 1 DIGEST '{hex}'"),
            format!("FROBNICATE 'a2' APPLY CHANGE 1 DIGEST '{hex}'"),
        ),
        (
            "SHOW OPERATION 'a2'".to_owned(),
            "SHOW FROBNICATE 'a2'".to_owned(),
        ),
    ] {
        for extended in [false, true] {
            let form = run(&mut wire, &sql, extended).await;
            let baseline = run(&mut wire, &unknown, extended).await;
            let code = error_of(&form).map(|e| e.0);
            assert!(
                matches!(code.as_deref(), Some("42601" | "0A000" | "42704")),
                "{sql}: {form:?}"
            );
            assert_eq!(form, baseline, "{sql}");
        }
    }
    listener.shutdown().await.unwrap();
}

#[tokio::test]
async fn apply_digest_must_be_64_hex() {
    let (_server, _agent, _ordinary, mut wire) = agent_wire().await;
    let hex = digest();
    let upper = hex.to_ascii_uppercase();
    for ok in [hex.as_str(), upper.as_str()] {
        let sql = format!("OPERATION 'a3' APPLY CHANGE 5 DIGEST '{ok}'");
        assert!(parse(&sql).unwrap().is_some());
        assert_eq!(
            error_of(&wire.simple(&sql).await),
            Some(not_allowed(StatementClass::Apply))
        );
    }
    let short = &hex[..63];
    let long = format!("{hex}0");
    let not_hex = format!("{}g", &hex[..63]);
    let spaced = format!("{} {}", &hex[..32], &hex[32..62]);
    for bad in [
        format!("OPERATION 'a3' APPLY CHANGE 5 DIGEST '{short}'"),
        format!("OPERATION 'a3' APPLY CHANGE 5 DIGEST '{long}'"),
        format!("OPERATION 'a3' APPLY CHANGE 5 DIGEST '{not_hex}'"),
        format!("OPERATION 'a3' APPLY CHANGE 5 DIGEST '{spaced}'"),
        format!("OPERATION 'a3' APPLY CHANGE 5 DIGEST {hex}"),
        format!("OPERATION 'a3' APPLY CHANGE 5 DIGEST '\\x{}'", &hex[..62]),
        "OPERATION 'a3' APPLY CHANGE 5 DIGEST ''".to_owned(),
        format!("OPERATION 'a3' APPLY CHANGE 5 '{hex}'"),
        format!("OPERATION 'a3' APPLY 5 DIGEST '{hex}'"),
    ] {
        assert_eq!(parse(&bad), Err("42601"), "{bad}");
        assert_error(&wire.simple(&bad).await, "42601", &bad);
        assert_error(&wire.extended(&bad, &[], 0).await, "42601", &bad);
    }
}

// R3-626: the direct write and the outcome lookup over a real agent connection.

const REQUEUE: &str =
    "UPDATE shipments SET status = 'queued' WHERE merchant_id = 42 AND status = 'failed'";
const BEFORE: [&str; 6] = ["failed", "failed", "failed", "failed", "delivered", "failed"];
const AFTER: [&str; 6] = ["queued", "queued", "queued", "queued", "delivered", "failed"];

/// The worked example with a writable task: `inc-436` may set `status` from
/// 'failed' to 'queued' on merchant 42's rows (1-5; row 5 is delivered), at
/// most 25 rows. Row 6 belongs to merchant 7.
fn writable_admin(readable: &[&str]) -> Admin {
    let path = scratch("agent-pg-write");
    let admin = Admin::create_engine(path.join("db.redb"), Limits::default(), Engine::V2).unwrap();
    for statement in [
        "CREATE TABLE shipments (id bigint primary key, merchant_id bigint, status text, \
         attempt_count int)",
        "INSERT INTO shipments VALUES (1, 42, 'failed', 1), (2, 42, 'failed', 2), \
         (3, 42, 'failed', 1), (4, 42, 'failed', 3), (5, 42, 'delivered', 0), (6, 7, 'failed', 1)",
    ] {
        admin.sql_session().execute(statement, &[], None).unwrap();
    }
    let roles = admin.roles();
    for (name, task_only) in [
        ("incident_controller", false),
        ("oncall_lead", false),
        ("agent_exec", true),
    ] {
        roles
            .create(NewRole {
                name: name.into(),
                login: true,
                ..NewRole::default()
            })
            .unwrap();
        if task_only {
            roles.set_task_only(name, true).unwrap();
        }
    }
    for name in ["incident_controller", "agent_exec"] {
        roles
            .grant_table(
                "shipments",
                name,
                &[TablePrivilege::Select, TablePrivilege::Update],
            )
            .unwrap();
    }
    let issuer = roles.get("incident_controller").unwrap().unwrap();
    admin
        .tasks()
        .create(
            &issuer,
            NewTask {
                name: "inc-436".into(),
                executor: "agent_exec".into(),
                approver: None,
                schema: None,
                table: "shipments".into(),
                visible_rows: PredicateText("merchant_id = 42".into()),
                readable: readable
                    .iter()
                    .map(|c| c.to_string())
                    .collect(),
                masked: Vec::new(),
                untrusted_text: Vec::new(),
                allowed_write: Some(AllowedWrite {
                    columns: ["status".to_string()].into_iter().collect(),
                    old_row: PredicateText("status = 'failed'".into()),
                    new_row: PredicateText("status = 'queued'".into()),
                }),
                mutation_caps: MutationCaps {
                    distinct_rows: 25,
                    transitions: 25,
                },
                disclosure_caps: DisclosureCaps {
                    rows_read: 1000,
                    rows_returned: 1000,
                    bytes_returned: 2_097_152,
                },
                limits: LoadLimits::default(),
                approval_threshold: 25,
                expires: timestamptz(now_ms() + DAY_MS),
            },
            WallClockMs(now_ms()),
        )
        .unwrap();
    admin
}

/// A started agent server for [`writable_admin`] and a raw session on its
/// agent listener.
async fn writable_wire() -> (AgentServer, Wire) {
    writable_wire_reading(&["id", "merchant_id", "status", "attempt_count"]).await
}

/// [`writable_wire`] with only `readable` readable (the others hidden).
async fn writable_wire_reading(readable: &[&str]) -> (AgentServer, Wire) {
    let ca = certificate(None, false);
    let agent = certificate(Some(&ca), false);
    let server = AgentServer::start(
        writable_admin(readable),
        ca,
        vec![],
        vec![binding(&agent, "agent_exec", &["inc-436"])],
    )
    .await;
    let wire = connect_raw(server.agent.address, &server.ca, &agent, AGENT_STARTUP).await;
    (server, wire)
}

fn statuses(admin: &Admin) -> Vec<String> {
    admin
        .sql_session()
        .execute("SELECT status FROM shipments ORDER BY id", &[], None)
        .unwrap()
        .rows
        .into_iter()
        .map(|row| match &row.values[0] {
            Value::Text(text) => text.clone(),
            other => panic!("{other:?}"),
        })
        .collect()
}

/// The reply is exactly `tag` with no rows and no error, and the session is idle.
#[track_caller]
fn assert_complete(messages: &[Msg], tag: &str, sql: &str) {
    assert_eq!(error_of(messages), None, "{sql}: {messages:?}");
    assert!(
        messages.contains(&Msg::Complete(tag.into())),
        "{sql}: {messages:?}"
    );
    assert!(
        !messages.iter().any(|m| matches!(m, Msg::Row(_))),
        "{sql}: {messages:?}"
    );
    assert_eq!(messages.last(), Some(&Msg::Ready(b'I')), "{sql}: {messages:?}");
}

/// The single row of `SHOW OPERATION`.
fn outcome_row(messages: &[Msg]) -> Vec<Option<String>> {
    let rows: Vec<_> = messages
        .iter()
        .filter_map(|m| match m {
            Msg::Row(values) => Some(values.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(rows.len(), 1, "{messages:?}");
    rows.into_iter().next().unwrap()
}

#[tokio::test]
async fn direct_update_commits_replays_and_shows_its_outcome() {
    for extended in [false, true] {
        let (server, mut wire) = writable_wire().await;
        let sql = format!("OPERATION 'a1' {REQUEUE}");
        assert_complete(&run(&mut wire, &sql, extended).await, "UPDATE 4", &sql);
        assert_eq!(statuses(&server.admin), AFTER);
        // Make a row match again: the resend replays the receipt and runs nothing.
        server
            .admin
            .sql_session()
            .execute("UPDATE shipments SET status = 'failed' WHERE id = 1", &[], None)
            .unwrap();
        assert_complete(&run(&mut wire, &sql, extended).await, "UPDATE 4", &sql);
        assert_eq!(statuses(&server.admin)[0], "failed");
        // The same ID with another request is 42T03 and writes nothing.
        let other = "OPERATION 'a1' UPDATE shipments SET status = 'queued' WHERE id = 1";
        let conflict = run(&mut wire, other, extended).await;
        assert_eq!(
            error_of(&conflict),
            Some((
                "42T03".to_owned(),
                TaskError::OperationConflict.message().to_owned()
            )),
            "{conflict:?}"
        );
        assert_eq!(statuses(&server.admin)[0], "failed");
        // SHOW OPERATION returns the committed row.
        let shown = run(&mut wire, "SHOW OPERATION 'a1'", extended).await;
        assert_eq!(error_of(&shown), None, "{shown:?}");
        let names = ["operation_id", "decision", "row_count", "receipt_seq", "effects_digest"];
        assert!(
            shown.contains(&Msg::Columns(names.map(String::from).to_vec())),
            "{shown:?}"
        );
        let row = outcome_row(&shown);
        assert_eq!(
            row[..4],
            [
                Some("a1".to_owned()),
                Some("committed".to_owned()),
                Some("4".to_owned()),
                Some("1".to_owned())
            ]
        );
        let digest = row[4].as_deref().unwrap();
        assert!(digest.len() == 64 && digest.bytes().all(|b| b.is_ascii_hexdigit()));
        // An unknown ID: the columns and no row.
        let unknown = run(&mut wire, "SHOW OPERATION 'never'", extended).await;
        assert_eq!(error_of(&unknown), None);
        assert!(
            !unknown.iter().any(|m| matches!(m, Msg::Row(_))),
            "{unknown:?}"
        );
    }
}

#[tokio::test]
async fn out_of_scope_rows_do_not_match() {
    let (server, mut wire) = writable_wire().await;
    // Row 6 belongs to merchant 7: outside the visible rows, zero rows match.
    for (n, extended) in [false, true].into_iter().enumerate() {
        let sql = format!(
            "OPERATION 'scope-{n}' UPDATE shipments SET status = 'queued' \
             WHERE id = 6 AND status = 'failed'"
        );
        assert_complete(&run(&mut wire, &sql, extended).await, "UPDATE 0", &sql);
    }
    assert_eq!(statuses(&server.admin), BEFORE);
}

#[tokio::test]
async fn column_outside_allowed_write_is_refused_and_writes_nothing() {
    let (server, mut wire) = writable_wire().await;
    let denied = TaskError::Denied(DenyRule::ColumnNotWritable);
    for (n, extended) in [false, true].into_iter().enumerate() {
        let sql = format!(
            "OPERATION 'col-{n}' UPDATE shipments SET status = 'queued', attempt_count = 9 \
             WHERE id = 1"
        );
        let messages = run(&mut wire, &sql, extended).await;
        assert_eq!(
            error_of(&messages),
            Some((denied.sqlstate().to_owned(), denied.message().to_owned())),
            "{sql}: {messages:?}"
        );
        assert_eq!(messages.last(), Some(&Msg::Ready(b'I')), "{sql}");
    }
    assert_eq!(statuses(&server.admin), BEFORE);
    let attempts = server
        .admin
        .sql_session()
        .execute("SELECT attempt_count FROM shipments WHERE id = 1", &[], None)
        .unwrap();
    assert_eq!(attempts.rows[0].values, [Value::Int(1)]);
    // The session still works.
    let sql = format!("OPERATION 'ok' {REQUEUE}");
    assert_complete(&wire.simple(&sql).await, "UPDATE 4", &sql);
}

#[tokio::test]
async fn bare_update_and_preview_apply_stay_refused_on_a_writable_task() {
    let (server, mut wire) = writable_wire().await;
    let hex = digest();
    for extended in [false, true] {
        let bare = run(&mut wire, REQUEUE, extended).await;
        assert_eq!(
            error_of(&bare),
            Some((
                "42T04".to_owned(),
                TaskError::OperationIdRequired.message().to_owned()
            )),
            "{bare:?}"
        );
        let preview = format!("PREVIEW {REQUEUE}");
        assert_eq!(
            error_of(&run(&mut wire, &preview, extended).await),
            Some(not_allowed(StatementClass::Preview))
        );
        let apply = format!("OPERATION 'ap' APPLY CHANGE 1 DIGEST '{hex}'");
        assert_eq!(
            error_of(&run(&mut wire, &apply, extended).await),
            Some(not_allowed(StatementClass::Apply))
        );
    }
    assert_eq!(statuses(&server.admin), BEFORE);
}

#[tokio::test]
async fn hidden_column_in_update_is_refused_with_the_rule_before_running() {
    // merchant_id is hidden here: a filter, a SET source or a RETURNING item
    // on it is refused by the column rules before the statement runs, with
    // the same error as a column that does not exist, over both protocols.
    let (server, mut wire) = writable_wire_reading(&["id", "status", "attempt_count"]).await;
    let denied = TaskError::Denied(DenyRule::ColumnNotAvailable);
    let expected = Some((denied.sqlstate().to_owned(), denied.message().to_owned()));
    for (n, extended) in [false, true].into_iter().enumerate() {
        for (m, wrapped) in [
            REQUEUE,
            "UPDATE shipments SET status = 'queued' WHERE 1 / (merchant_id - 42) = 0",
            "UPDATE shipments SET status = CASE WHEN merchant_id = 42 THEN 'queued'              ELSE status END WHERE status = 'failed'",
            "UPDATE shipments SET status = 'queued' WHERE id = 1 RETURNING merchant_id",
            "UPDATE shipments SET status = 'queued' WHERE no_such_column = 42",
        ]
        .into_iter()
        .enumerate()
        {
            let sql = format!("OPERATION 'hidden-{n}-{m}' {wrapped}");
            let messages = run(&mut wire, &sql, extended).await;
            assert_eq!(error_of(&messages), expected, "{sql}: {messages:?}");
            assert_eq!(messages.last(), Some(&Msg::Ready(b'I')), "{sql}");
        }
    }
    assert_eq!(statuses(&server.admin), BEFORE);
    // A readable filter still commits on the same session.
    let sql = "OPERATION 'ok' UPDATE shipments SET status = 'queued'                WHERE status = 'failed' AND attempt_count = 1";
    assert_complete(&wire.simple(sql).await, "UPDATE 2", sql);
}
