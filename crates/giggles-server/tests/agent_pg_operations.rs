//! R3-207 (GD-0061 design-2 section 5, coordinator ruling R2): the SQL surface
//! for operation IDs, preview, apply and outcome lookup on a task session. The
//! forms parse and are admitted by class; in this build none of their classes
//! is enabled, so each lands refusing with 0A000 and its class message.
#![cfg(feature = "engine-rocksdb")]
mod agent_support;

use agent_support::{Msg, agent_wire, error_of, ordinary_wire};
use giggles_core::{
    relational::{DataType, Prepared},
    task::{StatementClass, TaskError},
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
    let (_server, _agent, _ordinary, mut wire) = agent_wire().await;
    let longest = "a".repeat(128);
    for id in [
        "a",
        "A-z.0_9:x",
        "inc-436:retry.1",
        longest.as_str(),
    ] {
        assert!(OperationId::new(id).is_some(), "{id}");
        let sql = format!("OPERATION '{id}' UPDATE shipments SET status = 'x' WHERE id = 1");
        // It parses, and is then refused by class: Update is not enabled yet.
        for messages in [wire.simple(&sql).await, wire.extended(&sql, &[], 0).await] {
            assert_eq!(
                error_of(&messages),
                Some(not_allowed(StatementClass::Update)),
                "{id}"
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
    // Over the wire the prefixed Parse reaches admission (not a parameter-count
    // or syntax error), with or without declared types.
    let (_server, _agent, _ordinary, mut wire) = agent_wire().await;
    let bound = wire.extended(sql, &[Some("retry"), Some("1")], 0).await;
    assert_eq!(error_of(&bound), Some(not_allowed(StatementClass::Update)));
    let declared = wire.parse_describe(sql, &[25, 20]).await;
    assert_eq!(error_of(&declared), Some(not_allowed(StatementClass::Update)));
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
    // On the agent listener each form reaches admission and is refused by its
    // class, in both protocols; Describe is never reached.
    let (_server, _agent, _ordinary, mut wire) = agent_wire().await;
    for (sql, class, _) in &cases {
        for messages in [
            wire.simple(sql).await,
            wire.extended(sql, &[], 0).await,
            wire.parse_describe(sql, &[]).await,
        ] {
            assert_eq!(error_of(&messages), Some(not_allowed(*class)), "{sql}");
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
