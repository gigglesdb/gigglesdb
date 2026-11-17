//! GD-0061 R3-304: column rules for native requests (design-3 section 2).
//! `deliveries` is a reserved legacy name, so the worked example's columns
//! live in `shipments`.
mod common;
use giggles_core::{
    Admin, Engine, Limits,
    operations::{Arithmetic, Expr, Intent, Projection, Request, ScalarFunction, Sort, ValueExpr},
    relational::{TableDef, Value},
    task::{
        DenyRule, TaskError,
        columns::{ColumnMarks, OutputMode, Position, apply_native, apply_sql_text, native_shape},
        definition::{
            AllowedWrite, ColumnMode, DisclosureCaps, LoadLimits, MutationCaps, PredicateText,
            TaskDef, TaskName, TaskObjectId, WallClockMs,
        },
    },
};
use std::collections::{BTreeMap, BTreeSet};

const TABLE: &str = "CREATE TABLE shipments (id bigint PRIMARY KEY, merchant_id bigint, \
    status text, failed_at text, attempt_count int, customer_email text, card_token text, \
    note text)";

#[cfg(feature = "engine-rocksdb")]
const ENGINE: Engine = Engine::V2;
#[cfg(not(feature = "engine-rocksdb"))]
const ENGINE: Engine = Engine::Legacy;

fn table() -> TableDef {
    let path = common::fresh("task-columns-native");
    let admin = Admin::create_engine(&path, Limits::default(), ENGINE).unwrap();
    admin.sql_session().execute(TABLE, &[], None).unwrap();
    let table = admin
        .catalog()
        .unwrap()
        .1
        .into_iter()
        .find(|t| t.name == "shipments")
        .unwrap();
    drop(admin);
    let _ = std::fs::remove_dir_all(&path);
    table
}

/// Readable id, status, failed_at, attempt_count; masked customer_email;
/// status writable; card_token, note and merchant_id hidden.
fn def(table: &TableDef) -> TaskDef {
    let mut columns: BTreeMap<String, ColumnMode> = ["id", "status", "failed_at", "attempt_count"]
        .iter()
        .map(|c| (c.to_string(), ColumnMode::Readable))
        .collect();
    columns.insert(
        "customer_email".into(),
        ColumnMode::Masked { constant: None },
    );
    TaskDef {
        id: TaskObjectId(1),
        name: TaskName::new("inc-436").unwrap(),
        def_version: 1,
        issuer: "incident_controller".into(),
        executor: "repair_agent".into(),
        approver: None,
        table: table.id,
        visible_rows: PredicateText("merchant_id = 42".into()),
        columns,
        untrusted_allowed: BTreeSet::new(),
        allowed_write: Some(AllowedWrite {
            columns: BTreeSet::from(["status".to_string()]),
            old_row: PredicateText("status = 'failed'".into()),
            new_row: PredicateText("status = 'queued'".into()),
        }),
        mutation_caps: MutationCaps::default(),
        disclosure_caps: DisclosureCaps::default(),
        limits: LoadLimits::default(),
        approval_threshold: 0,
        expires_at: WallClockMs(i64::MAX),
        revoked: false,
        grants: BTreeMap::new(),
    }
}

fn field(c: &str) -> Expr {
    Expr::Field(c.into())
}
fn text(s: &str) -> Expr {
    Expr::Value(ValueExpr::Literal(Value::Text(s.into())))
}
fn int(i: i64) -> Expr {
    Expr::Value(ValueExpr::Literal(Value::Int(i)))
}
fn eq(a: Expr, b: Expr) -> Expr {
    Expr::Compare(
        Box::new(a),
        giggles_core::operations::Compare::Eq,
        Box::new(b),
    )
}
fn item(e: Expr, alias: Option<&str>) -> Projection {
    Projection {
        expression: e,
        alias: alias.map(Into::into),
    }
}
fn sort(c: &str) -> Vec<Sort> {
    vec![Sort {
        field: c.into(),
        descending: false,
    }]
}
fn read(projection: &[&str], filter: Option<Expr>, order: Vec<Sort>) -> Request {
    Request::Read {
        table: "shipments".into(),
        filter,
        order,
        limit: None,
        projection: projection.iter().map(|c| c.to_string()).collect(),
    }
}
fn read_exprs(projection: Vec<Projection>) -> Request {
    Request::ReadExpressions {
        table: "shipments".into(),
        filter: None,
        order: Vec::new(),
        limit: None,
        projection,
    }
}
fn update(values: &[(&str, Value)], filter: Expr, guard: Option<Expr>) -> Request {
    Request::Update {
        table: "shipments".into(),
        filter,
        guard,
        values: values
            .iter()
            .map(|(c, v)| (c.to_string(), ValueExpr::Literal(v.clone())))
            .collect(),
        intent: Intent::One,
    }
}
fn update_exprs(values: Vec<(&str, Expr)>, filter: Expr) -> Request {
    Request::UpdateExpressions {
        table: "shipments".into(),
        filter,
        guard: None,
        values: values
            .into_iter()
            .map(|(c, e)| (c.to_string(), e))
            .collect(),
        intent: Intent::One,
    }
}
fn returning(request: Request, projection: Vec<Projection>) -> Request {
    Request::Returning {
        request: Box::new(request),
        projection,
    }
}

fn denied(r: Result<impl std::fmt::Debug, TaskError>, rule: DenyRule, what: &str) {
    match r {
        Err(TaskError::Denied(got)) => assert_eq!(got, rule, "{what}"),
        other => panic!("{what}: expected {rule:?}, got {other:?}"),
    }
}

#[test]
fn read_projection_modes() {
    let t = table();
    let d = def(&t);
    let marks = ColumnMarks::default();
    // Readable and masked plain outputs.
    let (_, plan) = apply_native(
        &d,
        1,
        &marks,
        &t,
        &read(&["id", "customer_email"], None, vec![]),
    )
    .unwrap();
    assert_eq!(
        plan.outputs,
        [OutputMode::Plain, OutputMode::Mask(Value::Null)]
    );
    // An empty projection expands to readable and masked columns in table order.
    let (expanded, plan) = apply_native(&d, 1, &marks, &t, &read(&[], None, vec![])).unwrap();
    let Request::Read { projection, .. } = &expanded else {
        panic!("{expanded:?}")
    };
    assert_eq!(
        projection,
        &[
            "id",
            "status",
            "failed_at",
            "attempt_count",
            "customer_email"
        ]
    );
    assert_eq!(plan.outputs.len(), 5);
    assert_eq!(plan.outputs[4], OutputMode::Mask(Value::Null));
    // Idempotent.
    assert_eq!(
        apply_native(&d, 1, &marks, &t, &expanded).unwrap(),
        (expanded.clone(), plan)
    );
    // ReadExpressions: a bare field is Output, anything else Expression.
    let r = read_exprs(vec![
        item(field("customer_email"), Some("e")),
        item(
            Expr::Call {
                function: ScalarFunction::Length,
                arguments: vec![field("status")],
            },
            Some("n"),
        ),
    ]);
    let shape = native_shape(&r);
    assert!(
        shape
            .refs
            .iter()
            .any(|x| x.column == "customer_email" && x.position == Position::Output)
    );
    assert!(
        shape
            .refs
            .iter()
            .any(|x| x.column == "status" && x.position == Position::Expression)
    );
    let (_, plan) = apply_native(&d, 1, &marks, &t, &r).unwrap();
    assert_eq!(
        plan.outputs,
        [OutputMode::Mask(Value::Null), OutputMode::Plain]
    );
    // A masked column under an expression is refused.
    denied(
        apply_native(
            &d,
            1,
            &marks,
            &t,
            &read_exprs(vec![item(
                Expr::Call {
                    function: ScalarFunction::Length,
                    arguments: vec![field("customer_email")],
                },
                Some("n"),
            )]),
        ),
        DenyRule::MaskedColumnPosition,
        "length(customer_email)",
    );
    // Hidden and unknown columns are the same error.
    for c in ["card_token", "no_such", "ctid"] {
        denied(
            apply_native(&d, 1, &marks, &t, &read(&[c], None, vec![])),
            DenyRule::ColumnNotAvailable,
            c,
        );
    }
    // Untrusted marks: withheld unless allowed, then counted.
    let marks = ColumnMarks::new(["status"]);
    let (_, plan) = apply_native(&d, 1, &marks, &t, &read(&["status"], None, vec![])).unwrap();
    assert_eq!(plan.outputs, [OutputMode::Withhold]);
    let mut allowed = d.clone();
    allowed.untrusted_allowed.insert("status".into());
    let (_, plan) =
        apply_native(&allowed, 1, &marks, &t, &read(&["status"], None, vec![])).unwrap();
    assert_eq!(plan.outputs, [OutputMode::CountUntrusted]);
    // A changed definition refuses everything.
    denied(
        apply_native(
            &d,
            2,
            &ColumnMarks::default(),
            &t,
            &read(&["id"], None, vec![]),
        ),
        DenyRule::DefinitionChanged,
        "def version",
    );
}

#[test]
fn filter_guard_order_masked_refused() {
    let t = table();
    let d = def(&t);
    let m = ColumnMarks::default();
    let masked = eq(field("customer_email"), text("a"));
    denied(
        apply_native(&d, 1, &m, &t, &read(&["id"], Some(masked.clone()), vec![])),
        DenyRule::MaskedColumnPosition,
        "filter",
    );
    denied(
        apply_native(&d, 1, &m, &t, &read(&["id"], None, sort("customer_email"))),
        DenyRule::MaskedColumnPosition,
        "order",
    );
    denied(
        apply_native(
            &d,
            1,
            &m,
            &t,
            &update(
                &[("status", Value::Text("queued".into()))],
                eq(field("id"), int(1)),
                Some(masked),
            ),
        ),
        DenyRule::MaskedColumnPosition,
        "guard",
    );
    let claim = Request::Update {
        table: "shipments".into(),
        filter: eq(field("id"), int(1)),
        guard: None,
        values: vec![(
            "status".into(),
            ValueExpr::Literal(Value::Text("queued".into())),
        )],
        intent: Intent::Claim {
            order: sort("customer_email"),
        },
    };
    denied(
        apply_native(&d, 1, &m, &t, &claim),
        DenyRule::MaskedColumnPosition,
        "claim order",
    );
    denied(
        apply_native(
            &d,
            1,
            &m,
            &t,
            &read(&["id"], Some(eq(field("note"), text("x"))), vec![]),
        ),
        DenyRule::ColumnNotAvailable,
        "hidden filter",
    );
    // Readable filter and order are fine.
    assert!(
        apply_native(
            &d,
            1,
            &m,
            &t,
            &read(
                &["id"],
                Some(eq(field("status"), text("failed"))),
                sort("attempt_count")
            )
        )
        .is_ok()
    );
    // Refused classes.
    let insert = Request::Insert {
        table: "shipments".into(),
        values: vec![("id".into(), ValueExpr::Literal(Value::Int(1)))],
    };
    assert_eq!(
        apply_native(&d, 1, &m, &t, &insert).unwrap_err(),
        TaskError::NotAllowedInTaskSession(giggles_core::task::StatementClass::Insert)
    );
    let delete = Request::Delete {
        table: "shipments".into(),
        filter: eq(field("id"), int(1)),
        guard: None,
        cap: None,
    };
    assert_eq!(
        apply_native(&d, 1, &m, &t, &delete).unwrap_err(),
        TaskError::NotAllowedInTaskSession(giggles_core::task::StatementClass::Delete)
    );
    let upsert = Request::OnConflict {
        request: Box::new(insert),
        target: vec!["id".into()],
        action: giggles_core::operations::ConflictAction::Nothing,
    };
    assert_eq!(
        apply_native(&d, 1, &m, &t, &upsert).unwrap_err(),
        TaskError::NotAllowedInTaskSession(giggles_core::task::StatementClass::Insert)
    );
}

#[test]
fn update_values_source_and_target() {
    let t = table();
    let d = def(&t);
    let m = ColumnMarks::default();
    let by_id = eq(field("id"), int(1));
    assert!(
        apply_native(
            &d,
            1,
            &m,
            &t,
            &update(
                &[("status", Value::Text("queued".into()))],
                by_id.clone(),
                None
            )
        )
        .is_ok()
    );
    denied(
        apply_native(
            &d,
            1,
            &m,
            &t,
            &update(&[("attempt_count", Value::Int(0))], by_id.clone(), None),
        ),
        DenyRule::ColumnNotWritable,
        "readable target",
    );
    denied(
        apply_native(
            &d,
            1,
            &m,
            &t,
            &update(&[("note", Value::Text("x".into()))], by_id.clone(), None),
        ),
        DenyRule::ColumnNotAvailable,
        "hidden target",
    );
    denied(
        apply_native(
            &d,
            1,
            &m,
            &t,
            &update(&[("customer_email", Value::Null)], by_id.clone(), None),
        ),
        DenyRule::MaskedColumnPosition,
        "masked target",
    );
    // Sources: readable ok, masked and hidden refused.
    let shape = native_shape(&update_exprs(
        vec![("status", field("failed_at"))],
        by_id.clone(),
    ));
    assert!(
        shape
            .refs
            .iter()
            .any(|r| r.column == "status" && r.position == Position::AssignTarget)
    );
    assert!(
        shape
            .refs
            .iter()
            .any(|r| r.column == "failed_at" && r.position == Position::AssignSource)
    );
    assert!(
        apply_native(
            &d,
            1,
            &m,
            &t,
            &update_exprs(vec![("status", field("failed_at"))], by_id.clone())
        )
        .is_ok()
    );
    denied(
        apply_native(
            &d,
            1,
            &m,
            &t,
            &update_exprs(vec![("status", field("customer_email"))], by_id.clone()),
        ),
        DenyRule::MaskedColumnPosition,
        "masked source",
    );
    denied(
        apply_native(
            &d,
            1,
            &m,
            &t,
            &update_exprs(vec![("status", field("card_token"))], by_id.clone()),
        ),
        DenyRule::ColumnNotAvailable,
        "hidden source",
    );
    denied(
        apply_native(
            &d,
            1,
            &m,
            &t,
            &update_exprs(
                vec![(
                    "attempt_count",
                    Expr::Arithmetic(
                        Box::new(field("attempt_count")),
                        Arithmetic::Add,
                        Box::new(int(1)),
                    ),
                )],
                by_id,
            ),
        ),
        DenyRule::ColumnNotWritable,
        "readable expression target",
    );
}

#[test]
fn return_star_readable_only() {
    let t = table();
    let d = def(&t);
    let m = ColumnMarks::default();
    let base = update(
        &[("status", Value::Text("queued".into()))],
        eq(field("id"), int(1)),
        None,
    );
    let (expanded, plan) = apply_native(&d, 1, &m, &t, &returning(base.clone(), vec![])).unwrap();
    let Request::Returning { projection, .. } = &expanded else {
        panic!("{expanded:?}")
    };
    let names: Vec<_> = projection
        .iter()
        .map(|p| match &p.expression {
            Expr::Field(c) => c.as_str(),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(names, ["id", "status", "failed_at", "attempt_count"]);
    assert_eq!(plan.outputs, vec![OutputMode::Plain; 4]);
    assert_eq!(apply_native(&d, 1, &m, &t, &expanded).unwrap().0, expanded);
    assert!(
        apply_native(
            &d,
            1,
            &m,
            &t,
            &returning(base.clone(), vec![item(field("status"), None)])
        )
        .is_ok()
    );
    denied(
        apply_native(
            &d,
            1,
            &m,
            &t,
            &returning(base.clone(), vec![item(field("customer_email"), None)]),
        ),
        DenyRule::MaskedColumnPosition,
        "returning masked",
    );
    denied(
        apply_native(
            &d,
            1,
            &m,
            &t,
            &returning(
                base.clone(),
                vec![item(
                    Expr::Call {
                        function: ScalarFunction::Length,
                        arguments: vec![field("customer_email")],
                    },
                    Some("n"),
                )],
            ),
        ),
        DenyRule::MaskedColumnPosition,
        "returning masked expression",
    );
    // An untrusted, withheld column is not in `return *`.
    let marks = ColumnMarks::new(["failed_at"]);
    let (expanded, _) = apply_native(&d, 1, &marks, &t, &returning(base, vec![])).unwrap();
    let Request::Returning { projection, .. } = &expanded else {
        panic!()
    };
    assert_eq!(projection.len(), 3);
}

/// R3-303's SQL cases (task/columns_tests.rs) with their native equivalents.
/// Statements with no native form (GROUP BY, aggregates, joins, subqueries,
/// sequences, other SQL functions) have nothing to compare.
#[test]
fn same_denials_as_sql() {
    let t = table();
    let d = def(&t);
    let m = ColumnMarks::default();
    let by_id = || eq(field("id"), int(1));
    let queued = || Value::Text("queued".into());
    let len = |c: &str| Expr::Call {
        function: ScalarFunction::Length,
        arguments: vec![field(c)],
    };
    let cases: Vec<(&str, Request)> = vec![
        ("SELECT * FROM shipments", read(&[], None, vec![])),
        (
            "SELECT id, status FROM shipments",
            read(&["id", "status"], None, vec![]),
        ),
        (
            "SELECT card_token FROM shipments",
            read(&["card_token"], None, vec![]),
        ),
        (
            "SELECT no_such FROM shipments",
            read(&["no_such"], None, vec![]),
        ),
        ("SELECT ctid FROM shipments", read(&["ctid"], None, vec![])),
        (
            "SELECT id FROM shipments WHERE note = 'x'",
            read(&["id"], Some(eq(field("note"), text("x"))), vec![]),
        ),
        (
            "SELECT id FROM shipments WHERE customer_email = 'a'",
            read(
                &["id"],
                Some(eq(field("customer_email"), text("a"))),
                vec![],
            ),
        ),
        (
            "SELECT id FROM shipments ORDER BY customer_email",
            read(&["id"], None, sort("customer_email")),
        ),
        (
            "SELECT length(customer_email) AS n FROM shipments",
            read_exprs(vec![item(len("customer_email"), Some("n"))]),
        ),
        (
            "SELECT length(status) AS n FROM shipments",
            read_exprs(vec![item(len("status"), Some("n"))]),
        ),
        (
            "SELECT customer_email AS e FROM shipments",
            read_exprs(vec![item(field("customer_email"), Some("e"))]),
        ),
        ("SELECT id FROM other_table", {
            let mut r = read(&["id"], None, vec![]);
            if let Request::Read { table, .. } = &mut r {
                *table = "other_table".into();
            }
            r
        }),
        (
            "UPDATE shipments SET status = 'queued' WHERE id = 1",
            update(&[("status", queued())], by_id(), None),
        ),
        (
            "UPDATE shipments SET attempt_count = 0 WHERE id = 1",
            update(&[("attempt_count", Value::Int(0))], by_id(), None),
        ),
        (
            "UPDATE shipments SET note = 'x' WHERE id = 1",
            update(&[("note", Value::Text("x".into()))], by_id(), None),
        ),
        (
            "UPDATE shipments SET status = customer_email WHERE id = 1",
            update_exprs(vec![("status", field("customer_email"))], by_id()),
        ),
        (
            "UPDATE shipments SET status = failed_at WHERE id = 1",
            update_exprs(vec![("status", field("failed_at"))], by_id()),
        ),
        (
            "UPDATE shipments SET status = 'queued' WHERE id = 1 RETURNING id, status",
            returning(
                update(&[("status", queued())], by_id(), None),
                vec![item(field("id"), None), item(field("status"), None)],
            ),
        ),
        (
            "UPDATE shipments SET status = 'queued' WHERE id = 1 RETURNING customer_email",
            returning(
                update(&[("status", queued())], by_id(), None),
                vec![item(field("customer_email"), None)],
            ),
        ),
        (
            "UPDATE shipments SET status = 'queued' WHERE id = 1 RETURNING *",
            returning(update(&[("status", queued())], by_id(), None), vec![]),
        ),
        (
            "UPDATE shipments SET status = 'queued' WHERE customer_email = 'a'",
            update(
                &[("status", queued())],
                eq(field("customer_email"), text("a")),
                None,
            ),
        ),
        (
            "INSERT INTO shipments (id) VALUES (1)",
            Request::Insert {
                table: "shipments".into(),
                values: vec![("id".into(), ValueExpr::Literal(Value::Int(1)))],
            },
        ),
        (
            "DELETE FROM shipments WHERE id = 1",
            Request::Delete {
                table: "shipments".into(),
                filter: by_id(),
                guard: None,
                cap: None,
            },
        ),
    ];
    let mut denials = 0;
    for (sql, native) in &cases {
        let from_sql = apply_sql_text(&d, 1, &m, &t, sql).map(|_| ());
        let from_native = apply_native(&d, 1, &m, &t, native).map(|_| ());
        assert_eq!(from_native, from_sql, "{sql}");
        denials += usize::from(from_sql.is_err());
    }
    assert!(denials >= 12, "{denials}");
    // The untrusted mark gives the same plan on both surfaces.
    let marks = ColumnMarks::new(["status"]);
    let sql = apply_sql_text(&d, 1, &marks, &t, "SELECT status FROM shipments").unwrap();
    let (_, native) = apply_native(&d, 1, &marks, &t, &read(&["status"], None, vec![])).unwrap();
    assert_eq!(sql.outputs, native.outputs);
}

/// The rules run in `bind_request` for a session bound to a task.
#[cfg(feature = "engine-rocksdb")]
#[test]
fn bound_session_applies_rules() {
    use giggles_core::{
        roles::{NewRole, TablePrivilege},
        task::{context::open_context, definition::NewTask},
    };
    let path = common::fresh("task-columns-native-session");
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    let mut s = admin.sql_session();
    s.execute(TABLE, &[], None).unwrap();
    s.execute(
        "INSERT INTO shipments VALUES (1, 42, 'failed', 'x', 3, 'a@b', 'tok', 'n')",
        &[],
        None,
    )
    .unwrap();
    let roles = admin.roles();
    for (name, task_only) in [("incident_controller", false), ("repair_agent", true)] {
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
    roles
        .grant_table(
            "shipments",
            "incident_controller",
            &[TablePrivilege::Select, TablePrivilege::Update],
        )
        .unwrap();
    let issuer = roles.get("incident_controller").unwrap().unwrap();
    let now = WallClockMs(1_793_898_000_000);
    admin
        .tasks()
        .create(
            &issuer,
            NewTask {
                name: "inc-436".into(),
                executor: "repair_agent".into(),
                approver: None,
                schema: None,
                table: "shipments".into(),
                visible_rows: PredicateText("merchant_id = 42".into()),
                readable: ["id", "status"].map(String::from).to_vec(),
                masked: vec![("customer_email".into(), None)],
                untrusted_text: Vec::new(),
                allowed_write: None,
                mutation_caps: MutationCaps::default(),
                disclosure_caps: DisclosureCaps {
                    rows_read: 10,
                    rows_returned: 10,
                    bytes_returned: 4096,
                },
                limits: LoadLimits::default(),
                approval_threshold: 0,
                expires: "2026-11-05 17:30:00+00".into(),
            },
            now,
        )
        .unwrap();
    let mut agent = admin.sql_session();
    let ctx = open_context(
        &agent,
        &TaskName::new("inc-436").unwrap(),
        &"repair_agent".to_string(),
        [1; 32],
        now,
        0,
    )
    .unwrap();
    agent.bind_task(ctx).unwrap();
    let none = BTreeMap::new();
    let refused = |agent: &mut giggles_core::SqlSession, r: &Request, state: &str| {
        let e = agent.execute_native_unreleased(r, &none).unwrap_err();
        assert_eq!(e.sqlstate(), Some(state), "{r:?}: {e:?}");
    };
    refused(&mut agent, &read(&["card_token"], None, vec![]), "42501");
    refused(
        &mut agent,
        &read(
            &["id"],
            Some(eq(field("customer_email"), text("a"))),
            vec![],
        ),
        "42501",
    );
    let mut other = read(&["id"], None, vec![]);
    if let Request::Read { table, .. } = &mut other {
        *table = "no_such_table".into();
    }
    refused(&mut agent, &other, "42501");
    // Allowed reads bind and run; the page is a task page.
    for r in [
        read(&["id", "customer_email"], None, vec![]),
        read(&[], None, vec![]),
    ] {
        let page = agent.execute_native_unreleased(&r, &none).unwrap();
        assert!(page.is_task());
    }
    drop(agent);
    drop(s);
    drop(admin);
    let _ = std::fs::remove_dir_all(&path);
}
