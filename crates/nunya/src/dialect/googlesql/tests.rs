use super::*;
use crate::{
    policy::{DomainName, ReadLogSink},
    schema::{Column, Relation},
};
fn fixture() -> (Snapshot, Policy) {
    let mut p = Policy {
        json_filters: Default::default(),
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
    let snapshot = Snapshot::new(rs, vec![], vec!["".into()], 0);
    let p = crate::policy::effective::legacy_policy(p, &snapshot);
    (snapshot, p)
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

#[test]
fn exact_tokenize_shape() {
    let (snapshot, mut policy) = fixture();
    let domain = DomainName("email".into());
    for sql in [
        "SELECT nunya_tokenize('email', @v)",
        "SELECT `nunya_tokenize`('email', @v)",
        "SELECT nunya_tokenize('email', @v);",
    ] {
        let (stmt, names) = lower_gsql(sql, &snapshot, &policy).unwrap();
        let arg = ValueSource::Param(1);
        assert_eq!(
            stmt.kind,
            StatementKind::Tokenize {
                domain: domain.clone(),
                arg: arg.clone()
            },
            "{sql}"
        );
        assert_eq!(stmt.plain_params, [1], "{sql}");
        assert_eq!(stmt.param_count, 1, "{sql}");
        assert_eq!(names, ["v"], "{sql}");
        assert!(
            stmt.outputs.is_empty() && stmt.uses.is_empty() && stmt.writes.is_empty(),
            "{sql}"
        );
        policy.domains.insert(
            domain.clone(),
            crate::policy::DomainSettings {
                tokenize: crate::policy::TokenizeSetting::On {
                    limit_per_session: 10,
                    limit_per_principal: 10,
                    principal_window: std::time::Duration::from_secs(60),
                },
                range: None,
            },
        );
        assert_eq!(
            crate::ir::check::check(&stmt, &policy, &snapshot)
                .unwrap()
                .tokenize,
            Some((domain.clone(), arg))
        );
        policy.domains.get_mut(&domain).unwrap().tokenize = crate::policy::TokenizeSetting::Off;
        assert_eq!(
            crate::ir::check::check(&stmt, &policy, &snapshot)
                .unwrap_err()
                .code,
            NyCode::StatementNotAllowed
        );
    }
    for (sql, domain_text, text) in [
        (
            r#"select NUNYA_TOKENIZE("email", 'bob@x.com')"#,
            "email",
            "bob@x.com",
        ),
        (
            r#"SELECT nunya_tokenize('Email', "bob@x.com")"#,
            "Email",
            "bob@x.com",
        ),
        (r#"SELECT nunya_tokenize('email', "a'b")"#, "email", "a'b"),
        (r#"SELECT nunya_tokenize('email', 'a"b')"#, "email", "a\"b"),
        (r#"SELECT nunya_tokenize("em'ail", 'x')"#, "em'ail", "x"),
        (r#"SELECT nunya_tokenize('em"ail', 'x')"#, "em\"ail", "x"),
    ] {
        let (stmt, names) = lower_gsql(sql, &snapshot, &policy).unwrap();
        assert_eq!(
            stmt.kind,
            StatementKind::Tokenize {
                domain: DomainName(domain_text.into()),
                arg: ValueSource::Literal {
                    span: Span {
                        start: 0,
                        end: sql.len()
                    },
                    text: text.into()
                },
            },
            "{sql}"
        );
        assert!(stmt.plain_params.is_empty() && names.is_empty(), "{sql}");
        assert_eq!(stmt.param_count, 0);
        assert!(stmt.outputs.is_empty() && stmt.uses.is_empty() && stmt.writes.is_empty());
    }
}

#[test]
fn tokenize_unsupported_shapes_fail_closed() {
    let (snapshot, policy) = fixture();
    for sql in [
        "SELECT nunya_tokenize('email', @v) AS t",
        "SELECT nunya_tokenize('email', @v) AS `t`",
        "SELECT nunya_tokenize('email', 'a''b')",
        "SELECT nunya_tokenize('em''ail', @v)",
        r#"SELECT nunya_tokenize('email', "a""b")"#,
        r#"SELECT nunya_tokenize("em""ail", @v)"#,
        "SELECT nunya_tokenize('email', 'a\nb')",
        "SELECT nunya_tokenize('email', 'a\rb')",
        "SELECT nunya_tokenize('em\nail', @v)",
        "SELECT nunya_tokenize('em\rail', @v)",
        "SELECT nunya_tokenize('email', \"a\nb\")",
        "SELECT nunya_tokenize('email', \"a\rb\")",
        "SELECT nunya_tokenize(\"em\nail\", @v)",
        "SELECT nunya_tokenize(\"em\rail\", @v)",
        "SELECT nunya.nunya_tokenize('email',@v)",
        "SELECT nunya_tokenize('email')",
        "SELECT nunya_tokenize('email',@v,@w)",
        "SELECT nunya_tokenize('email',1)",
        "SELECT nunya_tokenize('email',NULL)",
        "SELECT nunya_tokenize('email',TRUE)",
        "SELECT nunya_tokenize('email',DATE '2020-01-01')",
        "SELECT nunya_tokenize('email',r'x')",
        "SELECT nunya_tokenize('email',b'x')",
        "SELECT nunya_tokenize('email','''x''')",
        r#"SELECT nunya_tokenize('email',"""x""")"#,
        "SELECT nunya_tokenize('email',@a || 'x')",
        "SELECT nunya_tokenize('email',CONCAT('a','b'))",
        "SELECT nunya_tokenize('email',?)",
        "SELECT nunya_tokenize(email,@v)",
        "SELECT nunya_tokenize(@d,@v)",
        "SELECT nunya_tokenize(1,@v)",
        "SELECT nunya_tokenize(r'email',@v)",
        "SELECT nunya_tokenize(b'email',@v)",
        "SELECT nunya_tokenize('''email''',@v)",
        r"SELECT nunya_tokenize('email','a\nb')",
        r"SELECT nunya_tokenize('em\ail',@v)",
        r"SELECT nunya_tokenize('email',@v) /* \ */",
        "SELECT nunya_tokenize('email',@v) FROM Customers",
        "SELECT nunya_tokenize('email',@v) WHERE TRUE",
        "SELECT nunya_tokenize('email',@v) ORDER BY 1",
        "SELECT nunya_tokenize('email',@v) LIMIT 1",
        "SELECT nunya_tokenize('email',@v) LIMIT 1 OFFSET 1",
        "SELECT DISTINCT nunya_tokenize('email',@v)",
        "SELECT AS STRUCT nunya_tokenize('email',@v)",
        "SELECT AS VALUE nunya_tokenize('email',@v)",
        "SELECT nunya_tokenize('email',@v), 1",
        "SELECT nunya_tokenize('email',@v) UNION ALL SELECT 1",
        "WITH x AS (SELECT 1) SELECT nunya_tokenize('email',@v)",
        "SELECT UPPER(nunya_tokenize('email',@v))",
        "SELECT (SELECT nunya_tokenize('email',@v))",
        "SELECT Id FROM Customers WHERE Email = nunya_tokenize('email',@v)",
        "INSERT INTO Notes (Id, Body) VALUES (1, nunya_tokenize('email',@v))",
        "WITH x AS (SELECT nunya_tokenize('email',@v)) SELECT * FROM x",
        "SELECT COUNT(nunya_tokenize('email',@v))",
        "SELECT nunya_tokenize(DISTINCT 'email',@v)",
        "SELECT nunya_tokenize(ALL 'email',@v)",
        "SELECT nunya_tokenize('email',@v) OVER ()",
        "SELECT nunya_tokenize('email',@v) FILTER (WHERE TRUE)",
        "SELECT nunya_tokenize('email',@v) WITHIN GROUP (ORDER BY 1)",
        "SELECT nunya_tokenize('email',@v) GROUP BY 1",
        "SELECT nunya_tokenize('email',@v) HAVING TRUE",
        "SELECT nunya_tokenize('email',@v) QUALIFY TRUE",
        "SELECT nunya_tokenize('email',@v) WINDOW w AS ()",
    ] {
        assert_eq!(
            lower_gsql(sql, &snapshot, &policy).unwrap_err().code,
            NyCode::NotAnalysable,
            "{sql}"
        );
    }
}

#[test]
fn tokenize_in_view_definition_fails_closed() {
    let (mut snapshot, policy) = fixture();
    snapshot.relations.push(Relation {
        schema: String::new(),
        name: "TokenView".into(),
        kind: RelationKind::View,
        columns: vec![Column {
            name: "Value".into(),
            ty: ColumnType::Text,
            nullable: false,
            generated: None,
            default: None,
        }],
        view_definition: Some("SELECT nunya_tokenize('email','x') AS Value".into()),
        primary_key: vec![],
    });
    assert_eq!(
        lower_gsql("SELECT * FROM TokenView", &snapshot, &policy)
            .unwrap_err()
            .code,
        NyCode::NotAnalysable,
    );
}

#[test]
fn missing_policy_column_never_becomes_plain() {
    let (snapshot, mut policy) = fixture();
    let missing = ColumnRef {
        schema: "".into(),
        table: "Notes".into(),
        column: "Body".into(),
    };
    policy.columns.remove(&missing);
    let lower = |sql: &str| lower_gsql(sql, &snapshot, &policy).map(|s| s.0);
    for sql in [
        "SELECT Body FROM Notes",
        "SELECT * FROM Notes",
        "SELECT Body AS renamed FROM Notes",
        "SELECT d.Body FROM (SELECT Body FROM Notes) d",
        "WITH d AS (SELECT Body FROM Notes) SELECT Body FROM d",
        "DELETE FROM Notes WHERE TRUE THEN RETURN Body",
        "SELECT Id FROM Notes WHERE Body = @p",
        "SELECT n.Id FROM Notes n JOIN Notes m ON n.Body = m.Body",
        "SELECT Body FROM Notes GROUP BY Body",
        "SELECT Id FROM Notes ORDER BY Body",
    ] {
        if let Ok(stmt) = lower(sql) {
            let outputs = stmt
                .outputs
                .iter()
                .filter(|o| o.source.as_ref() == Some(&missing))
                .collect::<Vec<_>>();
            let uses = stmt
                .uses
                .iter()
                .filter(|u| u.column == missing)
                .collect::<Vec<_>>();
            assert!(!outputs.is_empty() || !uses.is_empty(), "{sql}");
            assert!(outputs.iter().all(|o| o.label == Label::Masked), "{sql}");
            assert!(uses.iter().all(|u| u.label == Label::Masked), "{sql}");
            assert!(
                crate::ir::check::check(&stmt, &policy, &snapshot).is_err(),
                "{sql}"
            );
        }
    }
    let direct = lower("SELECT Body FROM Notes").unwrap();
    assert_eq!(direct.outputs[0].label, Label::Masked);
    assert_eq!(
        crate::ir::check::check(&direct, &policy, &snapshot)
            .unwrap_err()
            .code,
        NyCode::NotAnalysable
    );
    let predicate = lower("SELECT Id FROM Notes WHERE Body = @p").unwrap();
    assert_eq!(
        crate::ir::check::check(&predicate, &policy, &snapshot)
            .unwrap_err()
            .code,
        NyCode::MaskedExpression
    );
}

#[test]
fn compiled_tokenize_policy_drives_output_rewrites_and_predicate_refusals() {
    let (snapshot, policy, domain) =
        crate::policy::effective::tests::compiled_tokenize_fixture("", "types");
    let lower = |sql: &str| lower_gsql(sql, &snapshot, &policy).map(|s| s.0);
    let stmt = lower("SELECT text, small, flag, kind FROM types").unwrap();
    let plan = crate::ir::check::check(&stmt, &policy, &snapshot).unwrap();
    assert_eq!(
        plan.outputs,
        vec![
            OutputRewrite::Tokenize {
                domain,
                ty: ColumnType::Text
            },
            OutputRewrite::Mask { placeholder: None },
            OutputRewrite::Mask { placeholder: None },
            OutputRewrite::Mask { placeholder: None },
        ]
    );
    for name in ["small", "flag", "kind"] {
        let sql = format!("SELECT text FROM types WHERE {name} IS NULL");
        let stmt = lower(&sql).unwrap();
        assert_eq!(
            crate::ir::check::check(&stmt, &policy, &snapshot)
                .unwrap_err()
                .code,
            NyCode::MaskedExpression
        );
    }
}
