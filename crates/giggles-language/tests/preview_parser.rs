//! GD-0061 R3-708: the native change forms of profile 0.22 (design-7 D7.1).
use giggles_language::{
    ChangeStatement, Error, Intent, LanguageVersion, Statement, Value, ValueExpr, lower_preview_update,
    parse_change_with_version,
};

const V: LanguageVersion = LanguageVersion::Embedded022;
const DIGEST: &str = "\"00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff\"";

fn text(s: &str) -> ValueExpr {
    ValueExpr::Literal(Value::Text(s.into()))
}

#[test]
fn preview_block_parses() {
    let parsed = parse_change_with_version(
        "preview { update many shipments where status = \"failed\" and id >= $lo and id <= 12 \
         set status = \"queued\" at most 25 } using task \"inc-436\";",
        V,
    )
    .unwrap();
    let ChangeStatement::Preview { update, task } = parsed else {
        panic!("not a preview: {parsed:?}");
    };
    assert_eq!(task, text("inc-436"));
    let Statement::Update { table, intent, .. } = &update else {
        panic!("not an update: {update:?}");
    };
    assert_eq!(table, "shipments");
    assert!(matches!(intent, Intent::Many { .. }));
    let bindings = [("lo".to_owned(), Value::Int(1))].into_iter().collect();
    let lowered = lower_preview_update(&update, &bindings).unwrap();
    assert_eq!(
        lowered.sql,
        "UPDATE shipments SET status = 'queued' WHERE status = 'failed' AND id >= $1 AND id <= 12"
    );
    assert_eq!(lowered.params, vec![Value::Int(1)]);
    // A missing binding is a bind error.
    assert_eq!(
        lower_preview_update(&update, &Default::default()),
        Err(Error::Bind("lo".into()))
    );

    // Computed assignments, a single-row update, a word or parameter task
    // name.
    let parsed = parse_change_with_version(
        "preview { update shipments where id = 7 set attempt_count = attempt_count + 1, status = -1 } \
         using task nightly",
        V,
    )
    .unwrap();
    let ChangeStatement::Preview { update, task } = parsed else { panic!() };
    assert_eq!(task, text("nightly"));
    assert_eq!(
        lower_preview_update(&update, &Default::default()).unwrap().sql,
        "UPDATE shipments SET attempt_count = attempt_count + 1, status = (-1) WHERE id = 7"
    );
    let parsed = parse_change_with_version(
        "preview { update app.shipments where id = $id set status = $s } using task $t",
        V,
    )
    .unwrap();
    let ChangeStatement::Preview { update, task } = parsed else { panic!() };
    assert_eq!(task, ValueExpr::Parameter("t".into()));
    let bindings = [
        ("id".to_owned(), Value::Int(3)),
        ("s".to_owned(), Value::Text("queued".into())),
    ]
    .into_iter()
    .collect();
    let lowered = lower_preview_update(&update, &bindings).unwrap();
    assert_eq!(lowered.sql, "UPDATE app.shipments SET status = $1 WHERE id = $2");
    assert_eq!(lowered.params, vec![Value::Text("queued".into()), Value::Int(3)]);

    // Only one guard-free update fits in the block.
    for unsupported in [
        "preview { update shipments where id = 1 when status = \"failed\" set status = \"queued\" } using task t",
        "preview { delete shipments where id = 1 } using task t",
        "preview { work where id = 1 select id } using task t",
        "preview { claim shipments where status = \"failed\" order by id set status = \"x\" } using task t",
    ] {
        assert_eq!(parse_change_with_version(unsupported, V), Err(Error::Unsupported), "{unsupported}");
    }
    for malformed in [
        "preview update shipments where id = 1 set status = \"q\" using task t",
        "preview { update shipments where id = 1 set status = \"q\" }",
        "preview { update shipments where id = 1 set status = \"q\" } using t",
        "preview { update shipments where id = 1 set status = \"q\" } using task t extra",
    ] {
        assert!(
            matches!(parse_change_with_version(malformed, V), Err(Error::Syntax { .. })),
            "{malformed}"
        );
    }
    // Only profile 0.22 has these forms.
    for version in [LanguageVersion::Embedded020, LanguageVersion::Embedded021] {
        assert_eq!(
            parse_change_with_version(
                "preview { update shipments where id = 1 set status = \"q\" } using task t",
                version
            ),
            Err(Error::Unsupported)
        );
    }
}

#[test]
fn apply_change_requires_once() {
    let full = format!("apply change 3 digest {DIGEST} once \"op-1\" for 1h");
    assert_eq!(
        parse_change_with_version(&full, V).unwrap(),
        ChangeStatement::Apply {
            change: ValueExpr::Literal(Value::Int(3)),
            digest: text(&DIGEST[1..65]),
            once: text("op-1"),
            retention: giggles_language::DurationLiteral { microseconds: 3_600_000_000 },
        }
    );
    assert!(matches!(
        parse_change_with_version("apply change $c digest $d once $op for 30s", V),
        Ok(ChangeStatement::Apply { .. })
    ));
    for missing in [
        format!("apply change 3 digest {DIGEST}"),
        format!("apply change 3 digest {DIGEST} for 1h"),
        format!("apply change 3 digest {DIGEST} once \"op-1\""),
        "apply change 3 once \"op-1\" for 1h".to_owned(),
    ] {
        assert!(
            matches!(parse_change_with_version(&missing, V), Err(Error::Syntax { .. })),
            "{missing}"
        );
    }
    // The retention is bounded (and then ignored).
    assert_eq!(
        parse_change_with_version(&format!("apply change 3 digest {DIGEST} once \"op-1\" for 8d"), V),
        Err(Error::Limit)
    );
    assert_eq!(
        parse_change_with_version(&full, LanguageVersion::Embedded021),
        Err(Error::Unsupported)
    );
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn using_task_must_match_session() {
    use crate::preview_parity::{TASK, context, native, queued, setup};
    use giggles_language::ChangeOutcome;
    let db = setup();
    let ctx = context(&db.admin);
    let block = "preview { update many shipments where status = \"failed\" and id <= 3 \
                 set status = \"queued\" at most 25 } using task";
    for other in ["\"inc-437\"", "nightly", "$t"] {
        let bindings = [("t".to_owned(), Value::Text("other".into()))].into_iter().collect();
        assert_eq!(
            native(&db.admin, &ctx, &format!("{block} {other}"), &bindings),
            Err(Error::Sql { sqlstate: "42501".into() }),
            "{other}"
        );
    }
    // Nothing was stored for the refused previews.
    assert!(db.admin.task_previews(ctx.task).unwrap().previews.is_empty());
    let bindings = [("t".to_owned(), Value::Text(TASK.into()))].into_iter().collect();
    let outcome = native(&db.admin, &ctx, &format!("{block} $t"), &bindings).unwrap();
    assert!(matches!(outcome, ChangeOutcome::Preview(summary) if summary.rows == 3));
    assert_eq!(db.admin.task_previews(ctx.task).unwrap().previews.len(), 1);
    assert_eq!(queued(&db.admin), Value::Int(0));
    // A single-row update must match exactly one row.
    assert_eq!(
        native(
            &db.admin,
            &ctx,
            "preview { update shipments where status = \"failed\" and id <= 3 set status = \"queued\" } \
             using task \"inc-436\"",
            &Default::default()
        ),
        Err(Error::AmbiguousMatch)
    );
}

#[test]
fn native_approve_is_unsupported() {
    let text_form = format!("approve change 3 of task \"inc-436\" digest {DIGEST}");
    assert_eq!(
        parse_change_with_version(&text_form, V).unwrap(),
        ChangeStatement::Approve {
            change: ValueExpr::Literal(Value::Int(3)),
            task: Some(text("inc-436")),
            digest: text(&DIGEST[1..65]),
        }
    );
    assert!(matches!(
        parse_change_with_version("approve change $c digest $d", V),
        Ok(ChangeStatement::Approve { task: None, .. })
    ));
    assert_eq!(
        parse_change_with_version(&text_form, LanguageVersion::Embedded021),
        Err(Error::Unsupported)
    );
    approve_runs_as_0a000(&text_form);
}

/// Run in a task session, `approve change` is 0A000 (no native approver forms).
#[cfg(feature = "engine-rocksdb")]
fn approve_runs_as_0a000(text_form: &str) {
    use crate::preview_parity::{context, native, setup};
    let db = setup();
    let ctx = context(&db.admin);
    assert_eq!(
        native(&db.admin, &ctx, text_form, &Default::default()),
        Err(Error::Sql { sqlstate: "0A000".into() })
    );
    assert!(db.admin.task_previews(ctx.task).unwrap().previews.is_empty());
}

#[cfg(not(feature = "engine-rocksdb"))]
fn approve_runs_as_0a000(_: &str) {}
