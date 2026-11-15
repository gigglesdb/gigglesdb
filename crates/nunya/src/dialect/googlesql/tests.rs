use super::*;
use crate::{
    policy::{DomainName, ReadLogSink},
    schema::{Column, Relation},
};
fn fixture() -> (Snapshot, Policy) {
    let mut p = Policy {
        columns: BTreeMap::new(),
        domains: BTreeMap::new(),
        snapshot_max_age_ms: 0,
        readlog: ReadLogSink::Stdout,
    };
    let mut rs = vec![];
    for (t, cs) in [
        (
            "Customers",
            vec!["Id", "Email", "Country", "Phone", "FailedPayments"],
        ),
        ("Outbox", vec!["Id", "ToAddress", "Template"]),
        ("Notes", vec!["Id", "Body"]),
    ] {
        let mut columns = vec![];
        for c in cs {
            columns.push(Column {
                name: c.into(),
                ty: if matches!(c, "Id" | "FailedPayments") {
                    ColumnType::Int8
                } else {
                    ColumnType::Text
                },
                nullable: true,
                generated: None,
                default: None,
            });
            let col = ColumnRef {
                schema: "".into(),
                table: t.into(),
                column: c.into(),
            };
            if matches!(c, "Email" | "ToAddress" | "Phone") {
                p.columns.insert(
                    col,
                    Treatment::Blind {
                        domain: DomainName(if c == "Phone" { "phone" } else { "email" }.into()),
                    },
                );
            } else if c == "Country" {
                p.columns
                    .insert(col, Treatment::Masked { placeholder: None });
            }
        }
        rs.push(Relation {
            schema: "".into(),
            name: t.into(),
            columns,
            kind: RelationKind::Table,
            view_definition: None,
            primary_key: vec![],
        });
    }
    (Snapshot::new(rs, vec![], vec!["".into()], 0), p)
}
fn lower(sql: &str) -> ResolvedStatement {
    let (s, p) = fixture();
    lower_gsql(sql, &s, &p).unwrap().0
}
fn has(s: &ResolvedStatement, k: UseKind) -> bool {
    s.uses.iter().any(|u| u.kind == k)
}
#[test]
fn selects_and_names() {
    let s = lower("SELECT Id, Email FROM Customers WHERE FailedPayments >= 2");
    assert_eq!(s.outputs[0].label, Label::Plain);
    assert!(matches!(s.outputs[1].label, Label::Blind(_)));
    let (sn, p) = fixture();
    let (s, names) = lower_gsql("SELECT id FROM customers WHERE email = @e", &sn, &p).unwrap();
    assert_eq!(names, ["e"]);
    assert!(has(
        &s,
        UseKind::Equality {
            other: ValueSource::Param(1)
        }
    ));
    assert_eq!(s.uses[0].column.column, "Email");
}
#[test]
fn literals_never_decrypt() {
    let s = lower("SELECT Id FROM Customers WHERE Email = 'tok_x'");
    assert!(has(
        &s,
        UseKind::Equality {
            other: ValueSource::Computed(Label::Plain)
        }
    ));
    let (sn, p) = fixture();
    assert!(crate::ir::check::check(&s, &p, &sn).is_err());
}
#[test]
fn join_group_pattern_order() {
    assert!(has(
        &lower("SELECT c.Id FROM Customers c JOIN Outbox o ON c.Email = o.ToAddress"),
        UseKind::Equality {
            other: ValueSource::Column {
                column: ColumnRef {
                    schema: "".into(),
                    table: "Outbox".into(),
                    column: "ToAddress".into()
                },
                label: Label::Blind(DomainName("email".into()))
            }
        }
    ));
    let s = lower("SELECT Email, COUNT(DISTINCT Email) FROM Customers GROUP BY Email");
    assert!(has(&s, UseKind::CountDistinct));
    assert!(has(&s, UseKind::GroupBy));
    assert!(has(
        &lower("SELECT Id FROM Customers WHERE Email LIKE 'a%'"),
        UseKind::Pattern
    ));
    assert!(has(
        &lower("SELECT Email FROM Customers ORDER BY Email"),
        UseKind::Order
    ));
}
#[test]
fn functions_and_set() {
    let s = lower("SELECT UPPER(Email) FROM Customers");
    assert_eq!(s.outputs[0].label, Label::Tainted);
    assert!(has(
        &s,
        UseKind::Function {
            name: "upper".into()
        }
    ));
    assert_eq!(
        lower("SELECT Email FROM Customers UNION ALL SELECT Body FROM Notes").outputs[0].label,
        Label::Tainted
    );
}
#[test]
fn writes() {
    let s = lower("INSERT INTO Outbox (Id, ToAddress, Template) VALUES (1, @to, 'r')");
    assert_eq!(s.writes[1].value, ValueSource::Param(1));
    let s = lower("UPDATE Notes SET Body = @b WHERE Id = 1");
    assert_eq!(s.plain_params, [1]);
    assert_eq!(
        lower("DELETE FROM Customers WHERE Email = @e").kind,
        StatementKind::Delete
    );
}
#[test]
fn refusals() {
    let (s, p) = fixture();
    for sql in [
        "SELECT * FROM INFORMATION_SCHEMA.TABLES",
        "SELECT SOME_UNKNOWN_FN(1)",
        "SELECT 1; SELECT 2",
        "",
        "SELECT Email & 1 FROM Customers",
    ] {
        assert_eq!(
            lower_gsql(sql, &s, &p).unwrap_err().code,
            NyCode::NotAnalysable
        )
    }
    assert_eq!(
        lower("CREATE TABLE x (a INT64)").kind,
        StatementKind::Refused(NyCode::StatementNotAllowed)
    );
}
#[test]
fn parameter_order_repetition_cast() {
    let (s, p) = fixture();
    let (q, n) = lower_gsql(
        "SELECT @first FROM (SELECT @second AS x) q WHERE @first = @third",
        &s,
        &p,
    )
    .unwrap();
    assert_eq!(n, ["first", "second", "third"]);
    assert_eq!(q.param_count, 3);
    let q = lower("SELECT Id FROM Customers WHERE Email = CAST(@e AS STRING)");
    assert!(has(
        &q,
        UseKind::Equality {
            other: ValueSource::Param(1)
        }
    ));
    assert!(q.plain_params.is_empty());
}
#[test]
fn joined_passthrough_and_plain_write() {
    let s = lower("SELECT Email FROM Customers a RIGHT JOIN Customers b USING (Email)");
    assert_eq!(s.outputs[0].source.as_ref().unwrap().table, "Customers");
    assert!(matches!(s.outputs[0].label, Label::Blind(_)));
    let s = lower("SELECT Email FROM Customers a FULL JOIN Customers b USING (Email)");
    assert_eq!(s.outputs[0].label, Label::Tainted);
    assert!(s.outputs[0].source.is_none());
    let s = lower("SELECT * FROM Customers a NATURAL JOIN Customers b");
    assert_eq!(s.outputs.len(), 5);
    let s = lower("UPDATE Customers SET Email = Email WHERE Id = 1");
    let (sn, p) = fixture();
    assert!(crate::ir::check::check(&s, &p, &sn).is_ok());
    assert!(
        lower_gsql(
            "SELECT Email FROM Customers a JOIN Customers b USING (Email, Email)",
            &sn,
            &p
        )
        .is_err()
    );
}
#[test]
fn native_scalar_and_plain_constructors() {
    for sql in [
        "SELECT TRIM(Email) FROM Customers",
        "SELECT CEIL(FailedPayments) FROM Customers",
        "SELECT FLOOR(FailedPayments) FROM Customers",
        "SELECT IFNULL(Email, 'x') FROM Customers",
        "SELECT CASE WHEN Id = 1 THEN Email ELSE 'x' END FROM Customers",
    ] {
        let s = lower(sql);
        if sql.contains("Email") {
            assert_eq!(s.outputs[0].label, Label::Tainted)
        }
    }
    let (sn, p) = fixture();
    for sql in [
        "SELECT STRUCT(Email) FROM Customers",
        "SELECT [Email] FROM Customers",
        "SELECT (STRUCT(Email AS x)).x FROM Customers",
    ] {
        assert!(lower_gsql(sql, &sn, &p).is_err())
    }
    assert_eq!(
        lower("SELECT (STRUCT(1 AS x)).x").outputs[0].label,
        Label::Plain
    );
    assert_eq!(
        lower("SELECT x FROM UNNEST([1, 2]) x").outputs[0].label,
        Label::Plain
    );
}
#[test]
fn ctes_views_and_parameter_refusal() {
    let s = lower("WITH x AS (SELECT Email FROM Customers) SELECT Email FROM x");
    assert!(matches!(s.outputs[0].label, Label::Blind(_)));
    assert!(s.outputs[0].source.is_some());
    let (mut sn, p) = fixture();
    let cols = sn.relations[0].columns[1..2].to_vec();
    sn.relations.push(Relation {
        schema: "".into(),
        name: "V".into(),
        kind: RelationKind::View,
        columns: cols,
        view_definition: Some("SELECT Email FROM Customers WHERE Email = 'tok_x'".into()),
        primary_key: vec![],
    });
    let (s, _) = lower_gsql("SELECT * FROM V", &sn, &p).unwrap();
    assert!(has(
        &s,
        UseKind::Equality {
            other: ValueSource::Computed(Label::Plain)
        }
    ));
    sn.relations.last_mut().unwrap().view_definition =
        Some("SELECT Email FROM Customers WHERE Email = @e".into());
    assert!(lower_gsql("SELECT @e FROM V", &sn, &p).is_err());
}
#[test]
fn qualified_star_order_and_ambiguity() {
    let s = lower("SELECT c.*, n.* FROM Customers c JOIN Notes n USING (Id)");
    assert_eq!(
        s.outputs
            .iter()
            .map(|o| o.name.as_str())
            .collect::<Vec<_>>(),
        [
            "Id",
            "Email",
            "Country",
            "Phone",
            "FailedPayments",
            "Id",
            "Body"
        ]
    );
    assert_eq!(s.outputs[0].label, Label::Plain);
    assert!(matches!(s.outputs[1].label, Label::Blind(_)));
    let (sn, p) = fixture();
    assert!(
        lower_gsql(
            "SELECT c.* FROM Customers c JOIN Notes c USING(Id)",
            &sn,
            &p
        )
        .is_err()
    );
    assert_eq!(
        lower("SELECT STRUCT<INT64>(1)").outputs[0].label,
        Label::Plain
    );
}
#[test]
fn native_dates_and_plain_field() {
    for sql in [
        "SELECT DATE_TRUNC(DATE '2026-11-15', MONTH)",
        "SELECT TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)",
        "SELECT EXTRACT(YEAR FROM DATE '2026-11-15')",
    ] {
        assert_eq!(lower(sql).outputs[0].label, Label::Plain)
    }
    let (sn, p) = fixture();
    let s = lower_gsql("SELECT f.a FROM (SELECT STRUCT(1 AS a) AS f) q", &sn, &p)
        .unwrap()
        .0;
    assert_eq!(s.outputs[0].label, Label::Plain);
    assert!(s.outputs[0].source.is_none());
    assert!(lower_gsql("SELECT Email.a FROM Customers", &sn, &p).is_err());
}
#[test]
fn named_schema_shadowing_and_conflicting_parameter() {
    let (mut sn, mut p) = fixture();
    let mut r = sn.relations[0].clone();
    r.schema = "Named".into();
    sn.relations.push(r);
    let col = ColumnRef {
        schema: "Named".into(),
        table: "Customers".into(),
        column: "Email".into(),
    };
    p.columns.insert(
        col.clone(),
        Treatment::Blind {
            domain: DomainName("email".into()),
        },
    );
    let s = lower_gsql("SELECT email FROM named.customers", &sn, &p)
        .unwrap()
        .0;
    assert_eq!(s.outputs[0].source, Some(col));
    sn.user_routines = vec!["upper".into()];
    assert!(lower_gsql("SELECT UPPER(Email) FROM Customers", &sn, &p).is_err());
    let s = lower_gsql("SELECT @e FROM Customers WHERE Email = @e", &sn, &p)
        .unwrap()
        .0;
    assert_eq!(s.param_count, 1);
    assert_eq!(s.plain_params, [1]);
    assert!(crate::ir::check::check(&s, &p, &sn).is_err());
}
#[test]
fn view_nesting_bound() {
    let (mut sn, p) = fixture();
    let cols = sn.relations[0].columns[1..2].to_vec();
    for i in 0..9 {
        sn.relations.push(Relation {
            schema: "".into(),
            name: format!("V{i}"),
            kind: RelationKind::View,
            columns: cols.clone(),
            view_definition: Some(if i == 0 {
                "SELECT Email FROM Customers".into()
            } else {
                format!("SELECT Email FROM V{}", i - 1)
            }),
            primary_key: vec![],
        })
    }
    assert!(lower_gsql("SELECT Email FROM V7", &sn, &p).is_ok());
    assert!(lower_gsql("SELECT Email FROM V8", &sn, &p).is_err());
}

#[test]
fn spanner_primary_key_ddl_refusal() {
    assert_eq!(
        lower("CREATE TABLE x (a INT64) PRIMARY KEY (a)").kind,
        StatementKind::Refused(NyCode::StatementNotAllowed)
    );
}
#[test]
fn inner_sensitive_field_prefix_shadows_outer_alias() {
    let (sn, p) = fixture();
    assert!(
        lower_gsql(
            "SELECT (SELECT x.Id FROM (SELECT Email AS x FROM Customers) inner_q) FROM Notes x",
            &sn,
            &p
        )
        .is_err()
    );
}

#[test]
fn unparsable_ddl_classifier_stays_fail_closed() {
    let (s, p) = fixture();
    for sql in [
        "CREATE TABLE x (a INT64) PRIMARY KEY (a); SELECT 1",
        "SELECT FROM WHERE",
    ] {
        assert_eq!(
            lower_gsql(sql, &s, &p).unwrap_err().code,
            NyCode::NotAnalysable
        );
    }
}
