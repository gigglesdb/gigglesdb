use super::*;
use crate::{
    policy::{DomainName, ReadLogSink},
    schema::{Column, Relation},
};
use std::collections::BTreeMap;
fn fixture() -> (Snapshot, Policy) {
    let mut policy = Policy {
        retired_domains: Default::default(),
        json_filters: Default::default(),
        columns: BTreeMap::new(),
        domains: BTreeMap::new(),
        snapshot_max_age_ms: 0,
        readlog: ReadLogSink::Stdout,
    };
    let mut relations = vec![];
    for (table, cols) in [
        (
            "customers",
            vec!["id", "email", "country", "phone", "failed_payments"],
        ),
        ("outbox", vec!["id", "to_address", "template"]),
        ("notes", vec!["id", "body"]),
    ] {
        let mut columns = vec![];
        for name in cols {
            columns.push(Column {
                name: name.into(),
                ty: if name == "id" || name == "failed_payments" {
                    ColumnType::Int4
                } else {
                    ColumnType::Text
                },
                nullable: true,
                generated: None,
                default: None,
            });
            let col = ColumnRef {
                schema: "public".into(),
                table: table.into(),
                column: name.into(),
            };
            if matches!(name, "email" | "phone" | "to_address") {
                policy.columns.insert(
                    col,
                    Treatment::Blind {
                        domain: DomainName(if name == "phone" { "phone" } else { "email" }.into()),
                    },
                );
            } else if name == "country" {
                policy
                    .columns
                    .insert(col, Treatment::Masked { placeholder: None });
            }
        }
        relations.push(Relation {
            schema: "public".into(),
            name: table.into(),
            kind: RelationKind::Table,
            columns,
            view_definition: None,
            primary_key: vec![],
        });
    }
    relations.push(Relation {
        schema: "public".into(),
        name: "v_emails".into(),
        kind: RelationKind::View,
        columns: relations[0].columns[..2].to_vec(),
        view_definition: Some("SELECT id, email FROM public.customers".into()),
        primary_key: vec![],
    });
    let snapshot = Snapshot::new(relations, vec![], vec!["public".into()], 0);
    let policy = crate::policy::effective::legacy_policy(policy, &snapshot);
    (snapshot, policy)
}
fn lower(sql: &str) -> ResolvedStatement {
    let (s, p) = fixture();
    PgLowering.lower(sql, &s, &p, &[]).unwrap().remove(0)
}
fn blind() -> Label {
    Label::Blind(DomainName("email".into()))
}
fn has(s: &ResolvedStatement, kind: UseKind, context: Context) -> bool {
    s.uses
        .iter()
        .any(|u| u.kind == kind && u.context == context)
}
#[test]
fn case01_output_and_plain_comparison() {
    let s = lower("SELECT id, email FROM customers WHERE failed_payments >= 2");
    assert_eq!(s.kind, StatementKind::Select);
    assert_eq!(
        s.outputs
            .iter()
            .map(|o| o.label.clone())
            .collect::<Vec<_>>(),
        vec![Label::Plain, blind()]
    );
    assert_eq!(s.outputs[1].source.as_ref().unwrap().column, "email");
    assert_eq!(s.uses.len(), 1);
    assert!(has(&s, UseKind::Output, Context::SelectList));
}
#[test]
fn case02_equality_parameter() {
    let s = lower("SELECT id FROM customers WHERE email = $1");
    assert!(has(
        &s,
        UseKind::Equality {
            other: ValueSource::Param(1)
        },
        Context::Where
    ));
    assert!(s.plain_params.is_empty());
    assert_eq!(s.param_count, 1);
}
#[test]
fn case03_literal_span() {
    let sql = "SELECT id FROM customers WHERE email = 'tok_email_abc'";
    let s = lower(sql);
    match &s.uses[0].kind {
        UseKind::Equality {
            other: ValueSource::Literal { span, text },
        } => {
            assert_eq!(&sql[span.start..span.end], "'tok_email_abc'");
            assert_eq!(text, "tok_email_abc");
        }
        _ => panic!("missing literal equality"),
    };
}
#[test]
fn case04_join_equality() {
    let s = lower("SELECT c.id FROM customers c JOIN outbox o ON c.email = o.to_address");
    assert_eq!(s.uses.len(), 2);
    for u in s.uses {
        assert_eq!(u.context, Context::JoinOn);
        assert!(matches!(
            u.kind,
            UseKind::Equality {
                other: ValueSource::Column { .. }
            }
        ));
    }
}
#[test]
fn case05_group_count() {
    let s = lower("SELECT email, count(*) FROM customers GROUP BY email");
    assert_eq!(s.outputs[1].label, Label::Plain);
    assert!(has(&s, UseKind::GroupBy, Context::GroupBy));
}
#[test]
fn case06_count_distinct() {
    let s = lower("SELECT count(DISTINCT email) FROM customers");
    assert_eq!(s.outputs[0].label, Label::Plain);
    assert!(has(&s, UseKind::CountDistinct, Context::SelectList));
}
#[test]
fn case07_null() {
    assert!(has(
        &lower("SELECT id FROM customers WHERE phone IS NULL"),
        UseKind::IsNull,
        Context::Where
    ));
}
#[test]
fn case08_pattern() {
    assert!(has(
        &lower("SELECT email FROM customers WHERE email LIKE 'a%'"),
        UseKind::Pattern,
        Context::Where
    ));
}
#[test]
fn case09_order() {
    for suffix in ["email", "1"] {
        assert!(has(
            &lower(&format!("SELECT email FROM customers ORDER BY {suffix}")),
            UseKind::Order,
            Context::OrderBy
        ));
    }
}
#[test]
fn case10_function_taint() {
    let s = lower("SELECT upper(email) FROM customers");
    assert_eq!(s.outputs[0].label, Label::Tainted);
    assert!(s.outputs[0].source.is_none());
    assert!(has(
        &s,
        UseKind::Function {
            name: "upper".into()
        },
        Context::SelectList
    ));
}
#[test]
fn case11_cast() {
    let s = lower("SELECT email::text FROM customers");
    assert_eq!(s.outputs[0].label, Label::Tainted);
    assert!(has(&s, UseKind::Cast, Context::SelectList));
}
#[test]
fn case12_occurrence_sensitive_params() {
    let s = lower("SELECT id FROM customers WHERE email = $1 AND id = $1");
    assert_eq!(s.plain_params, vec![1]);
    assert!(
        lower("SELECT id FROM customers WHERE email = $1::text")
            .plain_params
            .is_empty()
    );
}
#[test]
fn refused_shapes() {
    let (s, p) = fixture();
    for sql in [
        "SELECT pg_read_file('x')",
        "SELECT * FROM pg_catalog.pg_stats",
        "SELECT email[1] FROM customers",
        "SELECT email::public.unknown_type FROM customers",
        "SELECT id FROM customers WHERE email = E'x'",
        "SELECT id FROM customers WHERE email = 'a'\n'b'",
    ] {
        assert!(
            PgLowering.lower(sql, &s, &p, &[]).is_err(),
            "unexpected acceptance"
        );
    }
}
#[test]
fn routing_and_spans() {
    let (s, p) = fixture();
    let sql = "BEGIN; SELECT 1; COMMIT";
    let stmts = PgLowering.lower(sql, &s, &p, &[]).unwrap();
    assert_eq!(stmts.len(), 3);
    assert_eq!(stmts[0].kind, StatementKind::Begin);
    assert_eq!(stmts[2].kind, StatementKind::Commit);
    assert_eq!(
        &sql[stmts[1].source_span.start..stmts[1].source_span.end],
        "SELECT 1"
    );
    for sql in [
        "CREATE TABLE t(x int)",
        "COPY customers TO STDOUT",
        "SET search_path = x",
        "EXPLAIN SELECT 1",
        "PREPARE TRANSACTION 'x'",
    ] {
        assert_eq!(
            lower(sql).kind,
            StatementKind::Refused(NyCode::StatementNotAllowed)
        );
    }
}
#[test]
fn in_literals_and_plain_parameters() {
    let s = lower("SELECT id FROM customers WHERE email IN ($1,$2)");
    assert_eq!(s.uses.len(), 2);
    assert!(s.plain_params.is_empty());
    let s = lower("SELECT id FROM customers WHERE email = $$tok$$ LIMIT $2");
    assert_eq!(s.plain_params, vec![2]);
    assert!(matches!(
        s.uses[0].kind,
        UseKind::Equality {
            other: ValueSource::Literal { .. }
        }
    ));
}

#[test]
fn group_input_precedes_alias() {
    let s = lower("SELECT id AS email FROM customers GROUP BY email");
    assert!(has(&s, UseKind::GroupBy, Context::GroupBy));
}
#[test]
fn computed_count_refuses() {
    let (s, p) = fixture();
    assert!(
        PgLowering
            .lower("SELECT count(upper(email)) FROM customers", &s, &p, &[])
            .is_err()
    );
}
#[test]
fn computed_order_records_provenance() {
    let s = lower("SELECT email FROM customers ORDER BY upper(email)");
    assert!(has(&s, UseKind::Order, Context::OrderBy));
    assert!(has(
        &s,
        UseKind::Function {
            name: "upper".into()
        },
        Context::OrderBy
    ));
}

#[test]
fn exact_tokenize_shape() {
    let s = lower("SELECT nunya_tokenize('email', $1)");
    assert!(
        matches!(s.kind,StatementKind::Tokenize { domain, arg: ValueSource::Param(1) } if domain == DomainName("email".into()))
    );
    assert_eq!(s.plain_params, vec![1]);
    let (s, p) = fixture();
    for sql in [
        "SELECT nunya_tokenize('email',$1), 1",
        "SELECT nunya_tokenize('email',$1) WHERE true",
        "SELECT upper(nunya_tokenize('email',$1))",
    ] {
        assert!(PgLowering.lower(sql, &s, &p, &[]).is_err());
    }
}

#[test]
fn case18_masked_equality_is_recorded() {
    let s = lower("SELECT country FROM customers WHERE country = 'US'");
    assert_eq!(s.outputs[0].label, Label::Masked);
    assert!(s.uses.iter().any(|u| u.label == Label::Masked
        && u.context == Context::Where
        && matches!(u.kind, UseKind::Equality { .. })));
}
#[test]
fn unsupported_relational_shapes_fail_closed() {
    let (s, p) = fixture();
    for sql in [
        "WITH RECURSIVE r AS (SELECT 1) SELECT * FROM r",
        "SELECT * FROM customers c, LATERAL (SELECT c.email) d",
        "SELECT * FROM generate_series(1,2)",
        "SELECT * FROM (customers JOIN outbox ON true) j",
    ] {
        assert_eq!(
            PgLowering.lower(sql, &s, &p, &[]).unwrap_err().code,
            NyCode::NotAnalysable
        );
    }
}

#[test]
fn literal_lexer_boundaries() {
    for (sql, text) in [
        ("'a''b'", "a'b"),
        ("$$tok$$", "tok"),
        ("$x$tok$x$", "tok"),
        ("-1.25e+2", "-1.25e+2"),
    ] {
        let (span, value) = lexer::literal_extent(sql, 0).unwrap();
        assert_eq!(
            span,
            Span {
                start: 0,
                end: sql.len()
            }
        );
        assert_eq!(value, text);
    }
    for sql in [
        "E'x'",
        "U&'x'",
        "B'1'",
        "X'ab'",
        "'a'\n'b'",
        "'a' /*c*/ 'b'",
        "'unterminated",
        "$t$x",
        "1e+",
    ] {
        assert!(lexer::literal_extent(sql, 0).is_none());
    }
}
#[test]
fn counterpart_parameters_are_per_occurrence() {
    for sql in [
        "SELECT $1 FROM customers WHERE email = $1",
        "SELECT id FROM customers WHERE email = $1 LIMIT $1",
        "SELECT id FROM customers WHERE email = $1 AND country = $1",
        "SELECT id FROM customers WHERE email IN ($1,$2) AND id = $2",
    ] {
        assert!(!lower(sql).plain_params.is_empty());
    }
    assert_eq!(
        lower("SELECT id FROM customers WHERE email = CAST($3 AS text)").param_count,
        3
    );
}

#[test]
fn case13_view_passthrough() {
    let s = lower("SELECT * FROM v_emails");
    assert_eq!(s.outputs.len(), 2);
    assert_eq!(s.outputs[0].label, Label::Plain);
    assert_eq!(s.outputs[1].label, blind());
    assert_eq!(s.outputs[1].source.as_ref().unwrap().table, "customers");
    assert!(
        s.uses
            .iter()
            .filter(|u| u.context == Context::SelectList && u.kind == UseKind::Output)
            .count()
            >= 2
    );
}
#[test]
fn case14_cte_passthrough_and_ordered_aliases() {
    let s = lower("WITH x AS (SELECT email FROM customers) SELECT email FROM x");
    assert_eq!(s.outputs[0].label, blind());
    assert_eq!(s.outputs[0].source.as_ref().unwrap().column, "email");
    for modifier in ["MATERIALIZED", "NOT MATERIALIZED"] {
        let s = lower(&format!(
            "WITH x(e) AS {modifier} (SELECT email FROM customers), y(f) AS (SELECT e FROM x) SELECT f FROM y"
        ));
        assert_eq!(s.outputs[0].name, "f");
        assert_eq!(s.outputs[0].label, blind());
        assert_eq!(s.outputs[0].source.as_ref().unwrap().table, "customers");
    }
}
#[test]
fn case15_set_mixed_leaves_its_domain() {
    let (snapshot, policy) = fixture();
    for op in ["UNION", "UNION ALL", "INTERSECT", "EXCEPT"] {
        for (right, code) in [
            ("phone FROM customers", NyCode::TokenOutsideDomain),
            ("body FROM notes", NyCode::TokenOutsideDomain),
            ("country FROM customers", NyCode::MaskedExpression),
        ] {
            for (left, right) in [("email FROM customers", right), (right, "email FROM customers")] {
                let sql = format!("SELECT {left} {op} SELECT {right}");
                assert_eq!(
                    PgLowering.lower(&sql, &snapshot, &policy, &[]).unwrap_err().code,
                    code,
                    "{sql}"
                );
            }
        }
        let s = lower(&format!(
            "SELECT email FROM customers {op} SELECT to_address FROM outbox"
        ));
        assert!(matches!(
            crate::ir::check::check(&s, &policy, &snapshot).unwrap().outputs.as_slice(),
            [OutputRewrite::Tokenize { domain, ty: ColumnType::Text }] if domain.0 == "email"
        ));
    }
}
#[test]
fn case16_catalogs_and_case17_functions_refuse() {
    let (s, p) = fixture();
    for sql in [
        "SELECT * FROM pg_catalog.pg_stats",
        "SELECT * FROM pg_stats",
        "SELECT * FROM information_schema.tables",
        "SELECT pg_read_file('x')",
    ] {
        assert_eq!(
            PgLowering.lower(sql, &s, &p, &[]).unwrap_err().code,
            NyCode::NotAnalysable
        );
    }
}
#[test]
fn case24_correlated_exists() {
    let s = lower(
        "SELECT id FROM customers WHERE EXISTS (SELECT 1 FROM outbox o WHERE o.to_address = customers.email)",
    );
    assert_eq!(s.outputs[0].label, Label::Plain);
    assert_eq!(s.uses.len(), 2);
    assert!(s.uses.iter().all(|u| u.context == Context::Where
        && matches!(
            u.kind,
            UseKind::Equality {
                other: ValueSource::Column { .. }
            }
        )));
}
#[test]
fn case25_recursive_and_dml_cte_refusals() {
    let (s, p) = fixture();
    assert_eq!(
        PgLowering
            .lower(
                "WITH RECURSIVE r AS (SELECT 1) SELECT * FROM r",
                &s,
                &p,
                &[]
            )
            .unwrap_err()
            .code,
        NyCode::NotAnalysable
    );
    let s = lower("WITH y AS (SELECT $1), x AS (DELETE FROM notes RETURNING id) SELECT * FROM x");
    assert_eq!(s.kind, StatementKind::Refused(NyCode::StatementNotAllowed));
    assert!(s.outputs.is_empty() && s.uses.is_empty() && s.plain_params.is_empty());
}
#[test]
fn derived_columns_and_scalar_sublink_pass_through() {
    let s = lower("SELECT e FROM (SELECT email FROM customers) AS d(e)");
    assert_eq!(s.outputs[0].label, blind());
    assert_eq!(s.outputs[0].source.as_ref().unwrap().column, "email");
    let s = lower("SELECT (SELECT d.*) FROM (SELECT email FROM customers) d");
    assert_eq!(s.outputs[0].label, blind());
    assert_eq!(s.outputs[0].source.as_ref().unwrap().table, "customers");
    let (s, p) = fixture();
    for sql in [
        "SELECT (SELECT id,email FROM customers)",
        "SELECT * FROM customers c, (SELECT c.email) d",
        "SELECT * FROM customers c, LATERAL (SELECT c.email) d",
    ] {
        assert!(PgLowering.lower(sql, &s, &p, &[]).is_err());
    }
}
#[test]
fn correlation_shadowing_and_true_outer_levels() {
    let (s, p) = fixture();
    for sql in [
        "SELECT (SELECT customers.email FROM notes AS customers) FROM customers",
        "SELECT (SELECT email FROM customers a, customers b) FROM customers",
    ] {
        assert!(PgLowering.lower(sql, &s, &p, &[]).is_err());
    }
    let s = lower("SELECT (SELECT e FROM (SELECT c.email AS e) d) FROM customers c");
    assert_eq!(s.outputs[0].label, blind());
}
#[test]
fn sublink_any_domains_and_array() {
    let s = lower("SELECT id FROM customers WHERE email IN (SELECT to_address FROM outbox)");
    assert!(has(
        &s,
        UseKind::Equality {
            other: ValueSource::Column {
                column: ColumnRef {
                    schema: "public".into(),
                    table: "outbox".into(),
                    column: "to_address".into()
                },
                label: blind()
            }
        },
        Context::Where
    ));
    for sql in [
        "SELECT id FROM customers WHERE phone IN (SELECT to_address FROM outbox)",
        "SELECT id FROM customers WHERE email <> ANY (SELECT to_address FROM outbox)",
    ] {
        assert!(has(&lower(sql), UseKind::Comparison, Context::Where));
    }
    let s = lower("SELECT ARRAY(SELECT email FROM customers)");
    assert_eq!(s.outputs[0].label, Label::Tainted);
    assert!(has(
        &s,
        UseKind::Function {
            name: "array".into()
        },
        Context::SelectList
    ));
    let (s, p) = fixture();
    assert!(
        PgLowering
            .lower(
                "SELECT id FROM customers WHERE email = ALL (SELECT to_address FROM outbox)",
                &s,
                &p,
                &[]
            )
            .is_err()
    );
}
#[test]
fn scalar_sublink_is_not_bare_counterpart() {
    let s = lower("SELECT id FROM customers WHERE email = (SELECT to_address FROM outbox)");
    assert!(s.uses.iter().any(
        |u| matches!(&u.kind,UseKind::Equality {other:ValueSource::Computed(l)} if *l==blind())
    ));
    let s = lower("SELECT id FROM customers WHERE email = (SELECT $1)");
    assert_eq!(s.plain_params, vec![1]);
}
#[test]
fn view_literals_params_isolation_and_metadata_names() {
    let (mut snapshot, policy) = fixture();
    let view = snapshot
        .relations
        .iter_mut()
        .find(|r| r.name == "v_emails")
        .unwrap();
    view.view_definition =
        Some("SELECT id, email FROM public.customers WHERE email = 'fixed'::text".into());
    view.columns[1].name = "renamed".into();
    let s = PgLowering
        .lower("SELECT renamed FROM v_emails", &snapshot, &policy, &[])
        .unwrap()
        .remove(0);
    assert_eq!(s.outputs[0].name, "renamed");
    assert_eq!(s.outputs[0].label, blind());
    assert!(s.uses.iter().any(|u| matches!(
        u.kind,
        UseKind::Equality {
            other: ValueSource::Computed(Label::Plain)
        }
    )));
    assert!(!s.uses.iter().any(|u| matches!(
        u.kind,
        UseKind::Equality {
            other: ValueSource::Literal { .. }
        }
    )));
    let view = snapshot
        .relations
        .iter_mut()
        .find(|r| r.name == "v_emails")
        .unwrap();
    view.view_definition = Some("SELECT id,$1 FROM customers".into());
    assert!(
        PgLowering
            .lower("SELECT * FROM v_emails", &snapshot, &policy, &[])
            .is_err()
    );
    let (mut snapshot, policy) = fixture();
    snapshot
        .relations
        .iter_mut()
        .find(|r| r.name == "v_emails")
        .unwrap()
        .view_definition = Some("SELECT id,email FROM customers".into());
    let s = PgLowering
        .lower(
            "WITH customers AS (SELECT id,body AS email FROM notes) SELECT * FROM v_emails",
            &snapshot,
            &policy,
            &[],
        )
        .unwrap()
        .remove(0);
    assert_eq!(s.outputs[1].label, blind());
}
#[test]
fn view_recursion_is_bounded() {
    let (mut snapshot, policy) = fixture();
    let base = snapshot
        .relations
        .iter()
        .find(|r| r.name == "v_emails")
        .unwrap()
        .clone();
    for n in 1..=8 {
        let mut v = base.clone();
        v.name = format!("v{n}");
        v.view_definition = Some(if n == 1 {
            "SELECT id,email FROM customers".into()
        } else {
            format!("SELECT * FROM v{}", n - 1)
        });
        snapshot.relations.push(v);
    }
    assert!(
        PgLowering
            .lower("SELECT * FROM v8", &snapshot, &policy, &[])
            .is_ok()
    );
    let mut ninth = base;
    ninth.name = "v9".into();
    ninth.view_definition = Some("SELECT * FROM v8".into());
    snapshot.relations.push(ninth);
    assert!(
        PgLowering
            .lower("SELECT * FROM v9", &snapshot, &policy, &[])
            .is_err()
    );
}
#[test]
fn using_and_natural_visibility_and_order() {
    let s = lower("SELECT * FROM customers c JOIN outbox o USING (id)");
    assert_eq!(
        s.outputs
            .iter()
            .map(|o| o.name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "id",
            "email",
            "country",
            "phone",
            "failed_payments",
            "to_address",
            "template"
        ]
    );
    let s = lower("SELECT c.* FROM customers c JOIN outbox o USING (id)");
    assert_eq!(
        s.outputs
            .iter()
            .map(|o| o.name.as_str())
            .collect::<Vec<_>>(),
        vec!["id", "email", "country", "phone", "failed_payments"]
    );
    let s = lower("SELECT * FROM customers c NATURAL JOIN outbox o");
    assert_eq!(s.outputs.len(), 7);
    assert_eq!(s.outputs[0].name, "id");
    let s = lower("SELECT email FROM customers c JOIN outbox o(id,email,template) USING (email)");
    assert_eq!(s.outputs[0].label, blind());
    assert_eq!(
        s.uses
            .iter()
            .filter(|u| u.context == Context::JoinOn && matches!(u.kind, UseKind::Equality { .. }))
            .count(),
        2
    );
    let s = lower("SELECT * FROM customers c JOIN outbox o(id,email,template) USING (email,id)");
    assert_eq!(s.outputs[0].name, "email");
    assert_eq!(s.outputs[1].name, "id");
}
#[test]
fn full_and_right_using_provenance() {
    let s = lower(
        "SELECT email,c.email,o.email FROM customers c FULL JOIN outbox o(id,email,template) USING (email)",
    );
    assert_eq!(s.outputs[0].label, Label::Tainted);
    assert!(s.outputs[0].source.is_none());
    assert_eq!(s.outputs[1].label, blind());
    assert_eq!(s.outputs[2].source.as_ref().unwrap().table, "outbox");
    assert!(
        s.uses
            .iter()
            .any(|u| u.kind == UseKind::Output && u.label == Label::Tainted)
    );
    let s =
        lower("SELECT email FROM customers c RIGHT JOIN outbox o(id,email,template) USING (email)");
    assert_eq!(s.outputs[0].source.as_ref().unwrap().table, "outbox");
}
#[test]
fn set_label_rules_and_outer_clauses() {
    for op in ["UNION", "UNION ALL", "INTERSECT", "EXCEPT"] {
        let s = lower(&format!(
            "SELECT email FROM customers {op} SELECT to_address FROM outbox ORDER BY 1 LIMIT $1 OFFSET $2"
        ));
        assert_eq!(s.outputs[0].label, blind());
        assert_eq!(s.outputs[0].source.as_ref().unwrap().table, "customers");
        assert_eq!(s.plain_params, vec![1, 2]);
        assert!(has(&s, UseKind::Order, Context::OrderBy));
    }
    for sql in [
        "SELECT country FROM customers UNION SELECT body FROM notes",
        "SELECT body FROM notes UNION SELECT country FROM customers",
    ] {
        assert_eq!(lower(sql).outputs[0].label, Label::Masked);
    }
    let (s, p) = fixture();
    assert!(
        PgLowering
            .lower(
                "SELECT id,email FROM customers UNION SELECT body FROM notes",
                &s,
                &p,
                &[]
            )
            .is_err()
    );
}
#[test]
fn values_plain_and_parameters() {
    let s = lower("SELECT * FROM (VALUES (1,$2),(3,4)) AS d(a,b)");
    assert_eq!(
        s.outputs
            .iter()
            .map(|o| o.name.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "b"]
    );
    assert!(
        s.outputs
            .iter()
            .all(|o| o.label == Label::Plain && o.source.is_none())
    );
    assert_eq!(s.plain_params, vec![2]);
    assert_eq!(lower("VALUES (1),(2)").outputs[0].name, "column1");
}
#[test]
fn advanced_expressions_preserve_provenance() {
    for (expression, name) in [
        ("CASE WHEN id=1 THEN email ELSE phone END", "case"),
        ("coalesce(email,'x')", "coalesce"),
        ("nullif(email,'x')", "nullif"),
        ("ROW(email,phone)", "row"),
    ] {
        let s = lower(&format!("SELECT {expression} FROM customers"));
        assert_eq!(s.outputs[0].label, Label::Tainted);
        assert!(has(
            &s,
            UseKind::Function { name: name.into() },
            Context::SelectList
        ));
    }
    let s = lower("SELECT CASE WHEN email=$1 THEN 1 ELSE 0 END FROM customers");
    assert!(has(
        &s,
        UseKind::Equality {
            other: ValueSource::Param(1)
        },
        Context::SelectList
    ));
    let s = lower("SELECT greatest(email,phone),least(email,'x') FROM customers");
    assert!(s.outputs.iter().all(|o| o.label == Label::Tainted));
    assert!(has(&s, UseKind::Order, Context::SelectList));
    let s = lower("SELECT x FROM (SELECT upper(email) AS x FROM customers) d ORDER BY x");
    assert_eq!(s.outputs[0].label, Label::Tainted);
    assert!(
        s.uses
            .iter()
            .any(|u| u.kind == UseKind::Output && u.label == Label::Tainted)
    );
    assert!(
        s.uses
            .iter()
            .any(|u| u.kind == UseKind::Order && u.label == Label::Tainted)
    );
}
#[test]
fn date_time_values_and_plain_arrays() {
    let s = lower(
        "SELECT CURRENT_DATE,CURRENT_TIMESTAMP,LOCALTIMESTAMP,CURRENT_TIME,LOCALTIME,CURRENT_TIMESTAMP(2),ARRAY[1,$3]",
    );
    assert!(s.outputs.iter().all(|o| o.label == Label::Plain));
    assert_eq!(s.plain_params, vec![3]);
    let (s, p) = fixture();
    for sql in ["SELECT CURRENT_USER", "SELECT ARRAY[email] FROM customers"] {
        assert!(PgLowering.lower(sql, &s, &p, &[]).is_err());
    }
}
#[test]
fn tokenize_never_inside_relational_layers() {
    let (s, p) = fixture();
    for sql in [
        "WITH x AS (SELECT 1) SELECT nunya_tokenize('email',$1)",
        "WITH x AS (DELETE FROM notes RETURNING id) SELECT nunya_tokenize('email',$1)",
        "SELECT * FROM (SELECT nunya_tokenize('email',$1)) d",
        "WITH x AS (SELECT nunya_tokenize('email',$1)) SELECT * FROM x",
        "SELECT (SELECT nunya_tokenize('email',$1))",
    ] {
        assert_eq!(
            PgLowering.lower(sql, &s, &p, &[]).unwrap_err().code,
            NyCode::NotAnalysable
        );
    }
}
#[test]
fn builtin_cast_modifiers_and_patterns() {
    let s = lower("SELECT id FROM customers WHERE email = $1::varchar(20)");
    assert!(s.plain_params.is_empty());
    let s = lower("SELECT ARRAY[1,2]::int4[]");
    assert_eq!(s.outputs[0].label, Label::Plain);
    for predicate in [
        "email LIKE 'a%' ESCAPE '!'",
        "email ILIKE 'a%' ESCAPE '!'",
        "email SIMILAR TO 'a%'",
        "email NOT SIMILAR TO 'a%' ESCAPE '!' ",
    ] {
        assert!(has(
            &lower(&format!("SELECT id FROM customers WHERE {predicate}")),
            UseKind::Pattern,
            Context::Where
        ));
    }
    let (s, p) = fixture();
    assert!(
        PgLowering
            .lower("SELECT pg_catalog.like_escape('x','!')", &s, &p, &[])
            .is_err()
    );
}

#[test]
fn materialized_view_and_nested_tokenize_refusal() {
    let (mut snapshot, policy) = fixture();
    let view = snapshot
        .relations
        .iter_mut()
        .find(|r| r.name == "v_emails")
        .unwrap();
    view.kind = RelationKind::MaterializedView;
    let s = PgLowering
        .lower("SELECT * FROM v_emails", &snapshot, &policy, &[])
        .unwrap()
        .remove(0);
    assert_eq!(s.outputs[1].label, blind());
    let view = snapshot
        .relations
        .iter_mut()
        .find(|r| r.name == "v_emails")
        .unwrap();
    view.view_definition = Some("SELECT nunya_tokenize('email','x')".into());
    assert_eq!(
        PgLowering
            .lower("SELECT * FROM v_emails", &snapshot, &policy, &[])
            .unwrap_err()
            .code,
        NyCode::NotAnalysable
    );
}
#[test]
fn explicit_pattern_helpers_refuse_even_as_operands() {
    let (s, p) = fixture();
    for sql in [
        "SELECT id FROM customers WHERE email LIKE pg_catalog.like_escape('a%','!')",
        "SELECT id FROM customers WHERE email SIMILAR TO pg_catalog.similar_to_escape('a%')",
    ] {
        assert!(PgLowering.lower(sql, &s, &p, &[]).is_err());
    }
}

#[test]
fn values_do_not_claim_first_row_column_source() {
    let s = lower("VALUES ((SELECT id FROM notes)),(1)");
    assert_eq!(s.outputs[0].label, Label::Plain);
    assert!(s.outputs[0].source.is_none());
}

#[test]
fn negative_membership_compares_without_reclassifying_inner_equalities() {
    for predicate in [
        "c.email NOT IN (SELECT o.to_address FROM outbox o WHERE o.to_address=$1)",
        "NOT (c.email IN (SELECT o.to_address FROM outbox o WHERE o.to_address=$1))",
    ] {
        let s = lower(&format!("SELECT c.id FROM customers c WHERE {predicate}"));
        assert_eq!(
            s.uses
                .iter()
                .filter(|u| u.context == Context::Where && u.kind == UseKind::Comparison)
                .count(),
            2
        );
        assert!(has(
            &s,
            UseKind::Equality {
                other: ValueSource::Param(1)
            },
            Context::Where
        ));
        assert!(!s.uses.iter().any(|u| matches!(
            u.kind,
            UseKind::Equality {
                other: ValueSource::Column { .. }
            }
        )));
        assert!(s.plain_params.is_empty());
    }
    let s = lower(
        "SELECT c.id FROM customers c WHERE c.email IN (SELECT o.to_address FROM outbox o WHERE o.to_address=$1)",
    );
    assert!(s.uses.iter().any(|u| matches!(
        u.kind,
        UseKind::Equality {
            other: ValueSource::Column { .. }
        }
    )));
    let s = lower("SELECT id FROM customers WHERE NOT (email=$1)");
    assert!(has(
        &s,
        UseKind::Equality {
            other: ValueSource::Param(1)
        },
        Context::Where
    ));
}

#[test]
fn dml_writes_and_parameter_occurrences() {
    let s = lower("INSERT INTO outbox (to_address, template) VALUES ($1, 'reminder')");
    assert_eq!(s.kind, StatementKind::Insert);
    assert_eq!(s.writes[0].column.column, "to_address");
    assert_eq!(s.writes[0].value, ValueSource::Param(1));
    assert!(
        matches!(s.writes[1].value, ValueSource::Literal { ref text, .. } if text == "reminder")
    );
    assert!(s.plain_params.is_empty());
    let s = lower("INSERT INTO outbox (to_address, template) VALUES ('tok_email_abc', $1)");
    assert!(matches!(s.writes[0].value, ValueSource::Literal { .. }));
    assert_eq!(s.plain_params, vec![1]);
    let s = lower("UPDATE notes SET body = $1 WHERE id = 3");
    assert_eq!(s.kind, StatementKind::Update);
    assert_eq!(s.writes[0].label, Label::Plain);
    assert_eq!(s.writes[0].value, ValueSource::Param(1));
    assert_eq!(s.plain_params, vec![1]);
    let s = lower("INSERT INTO outbox (to_address,template) VALUES ($1,$1)");
    assert_eq!(s.plain_params, vec![1]);
    let s = lower("INSERT INTO outbox (to_address) VALUES ($1::text),($2)");
    assert_eq!(s.writes.len(), 2);
    assert!(s.plain_params.is_empty());
    assert_eq!(lower("INSERT INTO notes VALUES (1,'x')").writes.len(), 2);
    let s = lower("INSERT INTO notes(body) VALUES (TRUE),(false)");
    assert!(matches!(&s.writes[0].value, ValueSource::Literal { text, .. } if text == "true"));
    assert!(matches!(&s.writes[1].value, ValueSource::Literal { text, .. } if text == "false"));
}
#[test]
fn dml_column_sources_and_computed_values() {
    let s = lower("UPDATE notes SET body = c.email FROM customers c WHERE c.id = notes.id");
    assert!(
        matches!(&s.writes[0].value, ValueSource::Column { column, label } if column.table == "customers" && *label == blind())
    );
    let s = lower(
        "INSERT INTO outbox (to_address) SELECT email FROM customers WHERE failed_payments > 1",
    );
    assert!(matches!(&s.writes[0].value, ValueSource::Column { label, .. } if *label == blind()));
    assert!(!s.uses.iter().any(|u| u.kind == UseKind::Output));
    let s = lower("UPDATE customers SET email = upper(email)");
    assert_eq!(s.writes[0].value, ValueSource::Computed(Label::Tainted));
    assert!(has(
        &s,
        UseKind::Function {
            name: "upper".into()
        },
        Context::SetClause
    ));
    let s = lower(
        "INSERT INTO customers (id, email) VALUES (1, $1) ON CONFLICT (email) DO UPDATE SET email = excluded.email",
    );
    assert_eq!(s.writes.len(), 3);
    assert!(matches!(&s.writes[2].value, ValueSource::Column { label, .. } if *label == blind()));
    assert!(s.plain_params.is_empty());
    let s = lower(
        "WITH x AS (SELECT email FROM customers) INSERT INTO outbox(to_address) SELECT email FROM x",
    );
    assert!(!s.uses.iter().any(|u| u.kind == UseKind::Output));
    let s = lower("INSERT INTO customers(email) VALUES ($1) ON CONFLICT ((email)) DO NOTHING");
    assert_eq!(s.writes.len(), 1);
}
#[test]
fn dml_returning_and_scopes() {
    let s = lower("DELETE FROM customers WHERE email = $1 RETURNING id, email");
    assert_eq!(s.kind, StatementKind::Delete);
    assert!(has(
        &s,
        UseKind::Equality {
            other: ValueSource::Param(1)
        },
        Context::Where
    ));
    assert_eq!(s.outputs[0].label, Label::Plain);
    assert_eq!(s.outputs[1].label, blind());
    assert_eq!(s.outputs[1].source.as_ref().unwrap().column, "email");
    assert!(has(&s, UseKind::Output, Context::Returning));
    for sql in [
        "UPDATE notes n SET body='x' FROM customers c WHERE n.id=c.id RETURNING *",
        "DELETE FROM notes n USING customers c WHERE n.id=c.id RETURNING *",
    ] {
        let s = lower(sql);
        assert_eq!(
            s.outputs
                .iter()
                .map(|o| o.name.as_str())
                .collect::<Vec<_>>(),
            [
                "id",
                "body",
                "id",
                "email",
                "country",
                "phone",
                "failed_payments"
            ]
        );
        for (i, output) in s.outputs.iter().enumerate() {
            assert_eq!(
                output.source.as_ref().unwrap().table,
                if i < 2 { "notes" } else { "customers" }
            );
        }
        assert_eq!(s.outputs[3].label, blind());
    }
    assert_eq!(
        lower("DELETE FROM ONLY notes n USING customers c WHERE n.id=c.id RETURNING n.*")
            .outputs
            .len(),
        2
    );
    assert_eq!(lower("UPDATE notes SET (id,body)=(1,'x')").writes.len(), 2);
    assert!(
        lower("INSERT INTO notes DEFAULT VALUES RETURNING *")
            .writes
            .is_empty()
    );
    assert_eq!(
        lower("INSERT INTO notes (body) VALUES (DEFAULT)").writes[0].value,
        ValueSource::Null
    );
    assert_eq!(
        lower("INSERT INTO customers (country) VALUES (DEFAULT)").writes[0].value,
        ValueSource::Null
    );
    assert_eq!(
        lower("INSERT INTO notes VALUES (1,'x') ON CONFLICT DO NOTHING")
            .writes
            .len(),
        2
    );
    assert_eq!(
        lower("WITH x AS (SELECT body FROM notes) INSERT INTO notes(body) SELECT body FROM x")
            .writes
            .len(),
        1
    );
}
#[test]
fn dml_and_operator_refusals() {
    let (s, p) = fixture();
    for sql in [
        "INSERT INTO v_emails VALUES (1, 'x')",
        "INSERT INTO outbox (to_address) VALUES (DEFAULT)",
        "INSERT INTO notes (id,id) VALUES (1,2)",
        "INSERT INTO notes (missing) VALUES (1)",
        "INSERT INTO notes (body) VALUES (1,2)",
        "INSERT INTO notes (body) SELECT id,body FROM notes",
        "INSERT INTO notes OVERRIDING SYSTEM VALUE VALUES (1,'x')",
        "INSERT INTO notes VALUES (1,'x') ON CONFLICT ((lower(body))) DO NOTHING",
        "UPDATE notes SET (id,body)=(SELECT id,body FROM notes)",
        "UPDATE notes SET body[1]='x'",
        "UPDATE notes SET body='x' WHERE CURRENT OF cur",
        "DELETE FROM notes WHERE CURRENT OF cur",
        "DELETE FROM notes RETURNING OLD.*",
        "INSERT INTO notes VALUES (1,'x') RETURNING NEW.*",
        "SELECT 1 OPERATOR(pg_catalog.+) 2",
        "SELECT 1 @@@ 2",
        "SELECT 1 OPERATOR(pg_catalog.=) ANY (SELECT 1)",
        "SELECT 1 @@@ ANY (SELECT 1)",
    ] {
        assert_eq!(
            PgLowering.lower(sql, &s, &p, &[]).unwrap_err().code,
            NyCode::NotAnalysable,
            "{sql}"
        );
    }
    for sql in [
        "WITH d AS (DELETE FROM notes RETURNING id) SELECT * FROM d",
        "WITH d AS (DELETE FROM notes RETURNING id) INSERT INTO notes SELECT * FROM d",
    ] {
        assert_eq!(
            lower(sql).kind,
            StatementKind::Refused(NyCode::StatementNotAllowed)
        );
    }
    assert_eq!(lower("SELECT 1 + 2").outputs.len(), 1);
}

#[test]
fn between_operator_names_match_kinds() {
    for predicate in [
        "id BETWEEN 1 AND 3",
        "id NOT BETWEEN 1 AND 3",
        "id BETWEEN SYMMETRIC 3 AND 1",
        "id NOT BETWEEN SYMMETRIC 3 AND 1",
    ] {
        let s = lower(&format!("SELECT id FROM notes WHERE {predicate}"));
        assert_eq!(s.kind, StatementKind::Select);
        assert_eq!(s.outputs[0].name, "id");
    }
    let s = lower("SELECT id FROM customers WHERE email BETWEEN 'a' AND 'z'");
    assert!(has(&s, UseKind::Comparison, Context::Where));
    let (snapshot, policy) = fixture();
    let kinds = [
        p::AExprKind::AexprBetween,
        p::AExprKind::AexprNotBetween,
        p::AExprKind::AexprBetweenSym,
        p::AExprKind::AexprNotBetweenSym,
        p::AExprKind::AexprOp,
    ];
    for (expected, name) in kinds[..4].iter().zip([
        "BETWEEN",
        "NOT BETWEEN",
        "BETWEEN SYMMETRIC",
        "NOT BETWEEN SYMMETRIC",
    ]) {
        for kind in kinds {
            let mut parsed = pg_query::parse("SELECT 1 BETWEEN 1 AND 3", 0).unwrap();
            let NodeEnum::SelectStmt(select) = parsed.protobuf.stmts[0]
                .stmt
                .as_mut()
                .unwrap()
                .node
                .as_mut()
                .unwrap()
            else {
                panic!()
            };
            let NodeEnum::ResTarget(target) = select.target_list[0].node.as_mut().unwrap() else {
                panic!()
            };
            let n = target.val.as_mut().unwrap();
            let NodeEnum::AExpr(a) = n.node.as_mut().unwrap() else {
                panic!()
            };
            a.kind = kind as i32;
            let NodeEnum::String(op) = a.name[0].node.as_mut().unwrap() else {
                panic!()
            };
            op.sval = name.into();
            let mut state = State {
                sql: "SELECT 1 BETWEEN 1 AND 3".into(),
                snapshot: &snapshot,
                policy: &policy,
                statement: empty(Span { start: 0, end: 0 }),
                plain: BTreeSet::new(),
                ctes: BTreeMap::new(),
                outers: vec![],
                depth: 0,
                view_depth: 0,
                view_body: false,
                extract_allowed: false,
                extracts: vec![],
                replacements: vec![],
            };
            assert_eq!(
                state.expression(n, &vec![], &Context::Where).is_ok(),
                kind == *expected
            );
        }
    }
}

#[test]
fn shadowed_allowlisted_functions_are_refused_on_plain_inputs() {
    let (snapshot, policy) = fixture();
    let snapshot = snapshot.with_user_routines(vec!["lower".into(), "count".into()]);
    for sql in [
        "SELECT lower('a')",
        "SELECT LOWER('a')",
        "SELECT pg_catalog.lower('a')",
        "SELECT upper(lower('a'))",
        "SELECT count(*)",
    ] {
        assert_eq!(
            PgLowering
                .lower(sql, &snapshot, &policy, &[])
                .unwrap_err()
                .code,
            NyCode::NotAnalysable,
            "{sql}"
        );
    }
    assert!(
        PgLowering
            .lower("SELECT upper('a')", &snapshot, &policy, &[])
            .is_ok()
    );
}

#[test]
fn shadowed_allowlisted_operators_are_refused_on_plain_inputs() {
    let (snapshot, policy) = fixture();
    let snapshot = snapshot.with_user_routines(vec!["+".into(), "=".into()]);
    for sql in [
        "SELECT 1 + 2",
        "SELECT +1",
        "SELECT 1 = 2",
        "SELECT 1 = ANY (ARRAY[1, 2])",
        "SELECT 1 = ANY (SELECT 1)",
        "SELECT 1 IN (SELECT 1)",
    ] {
        assert_eq!(
            PgLowering
                .lower(sql, &snapshot, &policy, &[])
                .unwrap_err()
                .code,
            NyCode::NotAnalysable,
            "{sql}"
        );
    }
    assert!(
        PgLowering
            .lower("SELECT 1 - 2", &snapshot, &policy, &[])
            .is_ok()
    );
}

#[test]
fn shadowed_between_comparisons_are_refused() {
    let (snapshot, policy) = fixture();
    for (operator, sql) in [
        (">=", "SELECT 1 BETWEEN 0 AND 2"),
        ("<=", "SELECT 1 BETWEEN SYMMETRIC 0 AND 2"),
        ("<", "SELECT 1 NOT BETWEEN 0 AND 2"),
        (">", "SELECT 1 NOT BETWEEN SYMMETRIC 0 AND 2"),
    ] {
        let snapshot = snapshot.clone().with_user_routines(vec![operator.into()]);
        assert_eq!(
            PgLowering
                .lower(sql, &snapshot, &policy, &[])
                .unwrap_err()
                .code,
            NyCode::NotAnalysable,
            "{sql}"
        );
    }
}

#[test]
fn driver_session_settings_are_exact_and_fail_closed() {
    let (snapshot, policy) = fixture();
    for sql in [
        "SET extra_float_digits = 3",
        "SET extra_float_digits = '3'",
        "SET application_name = 'PostgreSQL JDBC Driver'",
        "SET SESSION client_encoding = 'UTF-8'",
        "SET LOCAL timezone = ' Etc/UTC '",
        "SET datestyle = ISO, MDY",
        "SET datestyle = 'ISO, MDY'",
        "SET datestyle = 'ISO'",
        "SET intervalstyle = postgres",
        "SET standard_conforming_strings = on",
    ] {
        let statements = PgLowering.lower(sql, &snapshot, &policy, &[]).unwrap();
        assert_eq!(
            statements[0].kind,
            StatementKind::SessionNoop { tag: "SET" },
            "{sql}"
        );
    }
    for name in ["x".repeat(64), "\u{e9}".repeat(32)] {
        let sql = format!("SET application_name = '{name}'");
        assert_eq!(
            PgLowering.lower(&sql, &snapshot, &policy, &[]).unwrap()[0].kind,
            StatementKind::SessionNoop { tag: "SET" }
        );
    }
    let long_name = format!("SET application_name = '{}'", "x".repeat(65));
    let multibyte_long = format!("SET application_name = '{}'", "\u{e9}".repeat(33));
    for sql in [
        &multibyte_long,
        "SET extra_float_digits = 2",
        "SET search_path = x",
        "SET ROLE x",
        "SET timezone = 'Europe/London'",
        "SET client_encoding = 'LATIN1'",
        "SET datestyle = SQL",
        "SET standard_conforming_strings = off",
        "SET application_name = 3",
        "SET timezone FROM CURRENT",
        "SET SESSION AUTHORIZATION x",
        "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY",
        "SET TRANSACTION DEFERRABLE",
        "SET extra_float_digits = 3, 3",
        &long_name,
    ] {
        let statements = PgLowering.lower(sql, &snapshot, &policy, &[]).unwrap();
        assert_eq!(
            statements[0].kind,
            StatementKind::Refused(NyCode::StatementNotAllowed),
            "{sql}"
        );
    }
    for sql in [
        "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        "SET TRANSACTION READ ONLY",
        "SET TRANSACTION READ WRITE",
    ] {
        assert_eq!(
            PgLowering.lower(sql, &snapshot, &policy, &[]).unwrap()[0].kind,
            StatementKind::Savepoint
        );
    }
}

#[test]
fn prepared_statement_housekeeping_classification() {
    for (sql, name) in [
        ("DEALLOCATE x", Some("x")),
        ("DEALLOCATE PREPARE x", Some("x")),
        (r#"DEALLOCATE "Case Sensitive""#, Some("Case Sensitive")),
        (r#"DEALLOCATE "ALL""#, Some("ALL")),
        ("DEALLOCATE ALL", None),
        ("DEALLOCATE PREPARE ALL", None),
    ] {
        let statement = lower(sql);
        assert_eq!(
            statement.kind,
            StatementKind::Deallocate {
                name: name.map(str::to_owned),
            },
            "{sql}"
        );
        assert_eq!(statement.canonical_sql, None, "{sql}");
    }
    let discard = lower("DISCARD ALL");
    assert_eq!(discard.kind, StatementKind::DiscardAll);
    assert_eq!(discard.canonical_sql, Some("DEALLOCATE ALL"));
    assert_eq!(
        lower("DISCARD PLANS").kind,
        StatementKind::SessionNoop {
            tag: "DISCARD PLANS"
        }
    );
    for sql in ["DISCARD SEQUENCES", "DISCARD TEMP", "DISCARD TEMPORARY"] {
        assert_eq!(
            lower(sql).kind,
            StatementKind::Refused(NyCode::StatementNotAllowed),
            "{sql}"
        );
    }
}

#[test]
fn reset_housekeeping_classification() {
    for sql in [
        "RESET ALL",
        "RESET extra_float_digits",
        "RESET application_name",
        "RESET client_encoding",
        "RESET TIME ZONE",
        "RESET DateStyle",
        "RESET intervalstyle",
        "RESET standard_conforming_strings",
    ] {
        assert_eq!(
            lower(sql).kind,
            StatementKind::SessionNoop { tag: "RESET" },
            "{sql}"
        );
    }
    for sql in [
        "RESET statement_timeout",
        "RESET search_path",
        "RESET role",
        "SET application_name TO DEFAULT",
    ] {
        assert_eq!(
            lower(sql).kind,
            StatementKind::Refused(NyCode::StatementNotAllowed),
            "{sql}"
        );
    }

    // Lowering classifies each statement. The simple-query route refuses
    // messages that mix local housekeeping with a forwarded statement.
    let (snapshot, policy) = fixture();
    for (sql, expected) in [
        (
            "RESET search_path; SELECT 1",
            StatementKind::Refused(NyCode::StatementNotAllowed),
        ),
        (
            "RESET role; SELECT 1",
            StatementKind::Refused(NyCode::StatementNotAllowed),
        ),
        (
            "RESET ALL; SELECT 1",
            StatementKind::SessionNoop { tag: "RESET" },
        ),
    ] {
        let statements = PgLowering.lower(sql, &snapshot, &policy, &[]).unwrap();
        let [reset, select] = statements.as_slice() else {
            panic!("expected two statements: {sql}");
        };
        assert_eq!(reset.kind, expected, "{sql}");
        assert_eq!(select.kind, StatementKind::Select, "{sql}");
    }
}

#[test]
fn session_metadata_allowlist() {
    for sql in ["SHOW TRANSACTION ISOLATION LEVEL", "show transaction isolation level",
        "SHOW transaction_isolation", "select current_schema()", "SELECT current_schema()",
        "select current_catalog", "SELECT current_database()"] {
        let s = lower(sql);
        assert_eq!(s.kind, StatementKind::Select, "{sql}");
        assert_eq!(s.outputs.len(), 1, "{sql}");
        assert_eq!(s.outputs[0].label, Label::Plain);
        assert_eq!(s.outputs[0].ty, Some(ColumnType::Text));
        assert!(s.outputs[0].source.is_none());
        assert!(s.uses.is_empty());
    }
}
#[test]
fn session_metadata_near_misses() {
    let (snapshot, policy) = fixture();
    for (sql, code) in [
        ("SHOW search_path", NyCode::StatementNotAllowed),
        ("select current_schema() as s", NyCode::NotAnalysable),
        ("select current_schema() s", NyCode::NotAnalysable),
        ("select current_catalog as c", NyCode::NotAnalysable),
        ("select pg_catalog.current_schema()", NyCode::NotAnalysable),
        ("select pg_catalog.current_schema() as schema", NyCode::NotAnalysable),
        ("select pg_catalog.current_database()", NyCode::NotAnalysable),
        ("select current_schema", NyCode::NotAnalysable),

        ("SHOW ALL", NyCode::StatementNotAllowed),
        ("SHOW server_version", NyCode::StatementNotAllowed),
        ("SHOW transaction_read_only", NyCode::StatementNotAllowed),
        ("select current_schema() from customers", NyCode::NotAnalysable),
        ("select current_schema(), email from customers", NyCode::NotAnalysable),
        ("select current_schema() where exists (select 1 from customers)", NyCode::NotAnalysable),
        ("select current_schema() union select email from customers", NyCode::NotAnalysable),
        ("select current_database() limit 1", NyCode::NotAnalysable),
        ("select current_database() limit all", NyCode::NotAnalysable),
        ("with x as (select 1) select current_schema()", NyCode::NotAnalysable),
        ("select current_schemas(true)", NyCode::NotAnalysable),
        ("select current_user", NyCode::NotAnalysable),
        ("select current_schema() into t", NyCode::StatementNotAllowed),
        ("select private.current_schema()", NyCode::NotAnalysable),
        ("select current_schema(1)", NyCode::NotAnalysable),
        ("select current_schema() over ()", NyCode::NotAnalysable),
        ("select distinct current_schema()", NyCode::NotAnalysable),
        ("select current_catalog order by 1", NyCode::NotAnalysable),
    ] {
        let actual = match PgLowering.lower(sql, &snapshot, &policy, &[]) {
            Err(e) => e.code,
            Ok(s) => match s[0].kind { StatementKind::Refused(c) => c, _ => panic!("accepted {sql}") },
        };
        assert_eq!(actual, code, "{sql}");
    }
    let statements = PgLowering.lower("SHOW TRANSACTION ISOLATION LEVEL; SELECT email FROM customers", &snapshot, &policy, &[]).unwrap();
    assert_eq!(statements.len(), 2);
    assert_eq!(statements[1].outputs[0].label, blind());
    assert!(matches!(crate::ir::check::check(&statements[1], &policy, &snapshot).unwrap().outputs[0], OutputRewrite::Tokenize { .. }));
}

#[test]
fn missing_policy_column_never_becomes_plain() {
    let (snapshot, mut policy) = fixture();
    let missing = ColumnRef {
        schema: "public".into(),
        table: "notes".into(),
        column: "body".into(),
    };
    policy.columns.remove(&missing);
    let lower = |sql: &str| {
        PgLowering
            .lower(sql, &snapshot, &policy, &[])
            .map(|mut s| s.remove(0))
    };
    for sql in [
        "SELECT body FROM notes",
        "SELECT * FROM notes",
        "SELECT body AS renamed FROM notes",
        "SELECT d.body FROM (SELECT body FROM notes) d",
        "WITH d AS (SELECT body FROM notes) SELECT body FROM d",
        "DELETE FROM notes RETURNING body",
        "SELECT id FROM notes WHERE body = $1",
        "SELECT n.id FROM notes n JOIN notes m ON n.body = m.body",
        "SELECT body FROM notes GROUP BY body",
        "SELECT id FROM notes ORDER BY body",
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
    let direct = lower("SELECT body FROM notes").unwrap();
    assert_eq!(direct.outputs[0].label, Label::Masked);
    assert_eq!(
        crate::ir::check::check(&direct, &policy, &snapshot)
            .unwrap_err()
            .code,
        NyCode::NotAnalysable
    );
    let predicate = lower("SELECT id FROM notes WHERE body = $1").unwrap();
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
        crate::policy::effective::tests::compiled_tokenize_fixture("public", "types");
    let lower = |sql: &str| {
        PgLowering
            .lower(sql, &snapshot, &policy, &[])
            .map(|mut s| s.remove(0))
    };
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

#[test]
fn distinct_uses_grouping_rules_for_every_output() {
    let (mut snapshot, mut policy) = fixture();
    for name in ["id", "email", "country"] {
        let stmt = PgLowering.lower(&format!("SELECT DISTINCT {name} FROM customers"), &snapshot, &policy, &[]).unwrap().remove(0);
        if name != "id" {
            assert!(has(&stmt, UseKind::GroupBy, Context::GroupBy));
        }
        let result = crate::ir::check::check(&stmt, &policy, &snapshot);
        if name == "country" {
            assert_eq!(result.unwrap_err().code, NyCode::MaskedExpression);
        } else {
            let grouped = PgLowering.lower(&format!("SELECT {name} FROM customers GROUP BY {name}"), &snapshot, &policy, &[]).unwrap().remove(0);
            assert_eq!(result.unwrap(), crate::ir::check::check(&grouped, &policy, &snapshot).unwrap());
        }
    }
    let column = ColumnRef { schema: "public".into(), table: "notes".into(), column: "body".into() };
    snapshot.relations.iter_mut().find(|r| r.name == "notes").unwrap().columns[1].ty = ColumnType::Json;
    policy.columns.insert(column.clone(), Treatment::Masked { placeholder: None });
    policy.json_filters.insert(column.clone(), crate::policy::JsonColumnFilter {
        mode: crate::json::filter::JsonMode::Show,
        rules: vec![crate::policy::JsonPathRule { column, path: vec![], treatment: crate::policy::JsonPathTreatment::Masked }],
    });
    let stmt = PgLowering.lower("SELECT DISTINCT body FROM notes", &snapshot, &policy, &[]).unwrap().remove(0);
    assert_eq!(crate::ir::check::check(&stmt, &policy, &snapshot).unwrap_err().code, NyCode::MaskedExpression);
}

fn extraction_fixture(mode: crate::json::filter::JsonMode) -> (Snapshot, Policy) {
    use crate::policy::{JsonColumnFilter, JsonPathRule, JsonPathTreatment, JsonLeafType, JsonTokenType, json_path::parse_path};
    let (mut snapshot, mut policy) = fixture();
    let column = ColumnRef { schema: "public".into(), table: "notes".into(), column: "body".into() };
    snapshot.relations.iter_mut().find(|r| r.name == "notes").unwrap().columns.iter_mut().find(|c| c.name == "body").unwrap().ty = ColumnType::Jsonb;
    policy.columns.insert(column.clone(), Treatment::Masked { placeholder: None });
    let rules = [
        ("$.show", JsonPathTreatment::Show { ty: JsonLeafType::String }),
        ("$.blind", JsonPathTreatment::Blind { domain: DomainName("email".into()), ty: JsonTokenType::Text }),
        ("$.masked", JsonPathTreatment::Masked),
        ("$.parent.child", JsonPathTreatment::Show { ty: JsonLeafType::String }),
        ("$.array[*].child", JsonPathTreatment::Show { ty: JsonLeafType::String }),
    ].into_iter().map(|(path, treatment)| JsonPathRule { column: column.clone(), path: parse_path(path).unwrap(), treatment }).collect();
    policy.json_filters.insert(column, JsonColumnFilter { rules, mode });
    (snapshot, policy)
}

#[test]
fn json_extraction_analyzer_matrix() {
    use crate::json::filter::JsonMode;
    for mode in [JsonMode::Show, JsonMode::Tokenize] {
        let (snapshot, policy) = extraction_fixture(mode);
        for (expr, expected) in [
            ("body->'show'", "filter"), ("body->'blind'", "filter"),
            ("body->'masked'", "mask"), ("body->'masked'->'child'", "mask"),
            ("body->'show'->'inside'", "mask"), ("body->'parent'", "filter"),
            ("body->'array'->0", "filter"), ("body->'array'->(-1)->'child'", "filter"),
            ("body#>'{parent,child}'", "filter"), ("body#>>'{parent,child}'", "text"),
            ("body->>'show'", "text"), ("body->>'blind'", "text"),
            ("body->'unknown'", if mode == JsonMode::Show { "pass" } else { "mask" }),
        ] {
            let sql = format!("SELECT {expr} FROM notes");
            let statements = PgLowering.lower(&sql, &snapshot, &policy, &[]).unwrap();
            let statement = &statements[0];
            assert!(statement.outputs[0].source.is_none());
            let plan = crate::ir::check::check(statement, &policy, &snapshot).unwrap();
            match expected {
                "filter" => assert!(matches!(plan.outputs[0], OutputRewrite::JsonExtract { text: false, .. }), "{expr}"),
                "text" => {
                    assert!(matches!(plan.outputs[0], OutputRewrite::JsonExtract { text: true, .. }));
                    let InputRewrite::ReplaceSpan { text, .. } = &plan.inputs[0] else { panic!("missing replacement") };
                    assert!(text.ends_with(" AS \"?column?\""));
                    assert!(!text.contains("->>") && !text.contains("#>>"));
                }
                "mask" => assert_eq!(plan.outputs[0], OutputRewrite::Mask { placeholder: None }),
                "pass" => assert_eq!(plan.outputs[0], OutputRewrite::Pass),
                _ => panic!("invalid expectation"),
            }
        }
    }
}

#[test]
fn json_extraction_refused_forms_and_contexts_are_data_free() {
    use crate::json::filter::JsonMode;
    for mode in [JsonMode::Show, JsonMode::Tokenize] {
        let (snapshot, policy) = extraction_fixture(mode);
        let mut sqls: Vec<String> = ["E'secret'", "U&'secret'", "$$secret$$", "'secret'::text", "$1", "NULL", "1+2", "1.2", "'secret'\n'other'"]
            .into_iter().map(|rhs| format!("SELECT body->{rhs} FROM notes")).collect();
        sqls.extend([
            "SELECT body#>'{NULL}' FROM notes", "SELECT body#>'{}' FROM notes",
            "SELECT id FROM notes WHERE body->'masked' = 'secret'",
            "SELECT length(body->>'show') FROM notes",
            "SELECT (body->'show')::text FROM notes",
            "SELECT * FROM (SELECT body->'show' FROM notes) q",
            "WITH q AS (SELECT body->'show' FROM notes) SELECT * FROM q",
            "SELECT body->'show' FROM (SELECT body FROM notes) q",
            "SELECT body->'show' FROM notes UNION ALL SELECT body->'show' FROM notes",
            "UPDATE notes SET id = 1 RETURNING body->'show'",
        ].into_iter().map(str::to_owned));
        for sql in sqls {
            let result = PgLowering.lower(&sql, &snapshot, &policy, &[]).and_then(|statements| crate::ir::check::check(&statements[0], &policy, &snapshot));
            let error = result.expect_err(&sql);
            assert_eq!(error.code, NyCode::MaskedExpression, "{sql}");
            assert!(!format!("{error:?}").contains("secret"));
        }
    }
}

#[test]
fn json_show_leaf_group_order_and_distinct() {
    use crate::json::filter::JsonMode;
    for mode in [JsonMode::Show, JsonMode::Tokenize] {
        let (snapshot, policy) = extraction_fixture(mode);
        let plan = |sql: &str| {
            let statements = PgLowering.lower(sql, &snapshot, &policy, &[])?;
            crate::ir::check::check(&statements[0], &policy, &snapshot)
        };
        for op in ["->", "->>"] {
            let expected = format!(
                "(CASE WHEN pg_catalog.jsonb_typeof(body) = 'object' AND pg_catalog.jsonb_typeof(body->'show') = 'string' THEN body{op}'show' END)"
            );
            for (shape, expected_sql) in [
                (
                    "SELECT LEAF, count(*) FROM notes GROUP BY LEAF ORDER BY LEAF",
                    format!("SELECT {expected} AS \"?column?\", count(*) FROM notes GROUP BY {expected} ORDER BY {expected}"),
                ),
                (
                    "SELECT (LEAF), count(*) FROM notes GROUP BY 1 ORDER BY 1",
                    format!("SELECT (CASE WHEN pg_catalog.jsonb_typeof(body) = 'object' AND pg_catalog.jsonb_typeof((body->'show')) = 'string' THEN (body{op}'show') END) AS \"?column?\", count(*) FROM notes GROUP BY 1 ORDER BY 1"),
                ),
                (
                    "SELECT LEAF AS x, count(*) FROM notes GROUP BY LEAF ORDER BY x DESC",
                    format!("SELECT {expected} AS x, count(*) FROM notes GROUP BY {expected} ORDER BY x DESC"),
                ),
                ("SELECT id FROM notes ORDER BY LEAF", format!("SELECT id FROM notes ORDER BY {expected}")),
                ("SELECT LEAF FROM notes ORDER BY 1", format!("SELECT {expected} AS \"?column?\" FROM notes ORDER BY 1")),
                ("SELECT LEAF AS x FROM notes ORDER BY x", format!("SELECT {expected} AS x FROM notes ORDER BY x")),
                ("SELECT DISTINCT LEAF FROM notes", format!("SELECT DISTINCT {expected} AS \"?column?\" FROM notes")),
                (
                    "SELECT DISTINCT ON (LEAF) LEAF FROM notes ORDER BY LEAF",
                    format!("SELECT DISTINCT ON ({expected}) {expected} AS \"?column?\" FROM notes ORDER BY {expected}"),
                ),
            ] {
                let sql = shape.replace("LEAF", &format!("body{op}'show'"));
                let allowed = plan(&sql).unwrap();
                assert!(allowed.outputs.iter().all(|output| *output == OutputRewrite::Pass), "{sql}");
                let replacements: Vec<_> = allowed.inputs.into_iter().map(|input| match input {
                    InputRewrite::ReplaceSpan { span, text } => (span, text),
                    _ => panic!("unexpected input rewrite"),
                }).collect();
                let rewritten = crate::ir::rewrite::splice(&sql, &replacements).unwrap();
                assert_eq!(rewritten, expected_sql, "{sql}");
                for (path, reason) in [
                    ("blind", crate::error::JSON_GROUP_BLIND),
                    ("masked", crate::error::JSON_GROUP_MASKED),
                    ("unlisted", crate::error::JSON_GROUP_UNLISTED),
                    ("parent", crate::error::JSON_GROUP_NOT_LEAF),
                ] {
                    let sql = shape.replace("LEAF", &format!("body{op}'{path}'"));
                    if mode == JsonMode::Show && path == "unlisted" {
                        // Show mode passes unlisted paths through, so they group like any shown value.
                        plan(&sql).expect(&sql);
                        continue;
                    }
                    let error = plan(&sql).expect_err(&sql);
                    assert_eq!(error.code, NyCode::MaskedExpression, "{sql}");
                    assert_eq!(error.message(), reason, "{sql}");
                }
            }
        }
        for sql in [
            "SELECT body->>'show' FROM (SELECT body FROM notes) n GROUP BY 1",
            "SELECT * FROM (SELECT body->>'show' AS v FROM notes ORDER BY v) n",
            "SELECT id FROM (SELECT id, body FROM notes) n ORDER BY body->>'show'",
        ] {
            assert_eq!(plan(sql).expect_err(sql).code, NyCode::MaskedExpression);
        }
    }
}

#[test]
fn json_text_extraction_rewrites_preserve_parentheses_and_aliases() {
    let (snapshot, policy) = extraction_fixture(crate::json::filter::JsonMode::Show);
    for expression in ["body->>'show'", "(body->>'show')", "((body)->>'show')", "((body->'parent')->>'child')", "body#>>'{parent,child}'"] {
        for alias in ["", " AS named"] {
            let sql = format!("SELECT {expression}{alias} FROM notes");
            let statements = PgLowering.lower(&sql, &snapshot, &policy, &[]).unwrap();
            let plan = crate::ir::check::check(&statements[0], &policy, &snapshot).unwrap();
            let replacements: Vec<_> = plan.inputs.iter().map(|input| match input {
                InputRewrite::ReplaceSpan { span, text } => (span.clone(), text.clone()),
                _ => panic!("unexpected input rewrite"),
            }).collect();
            let rewritten = crate::ir::rewrite::splice(&sql, &replacements).unwrap();
            let parsed = pg_query::parse(&rewritten, 0).unwrap();
            let NodeEnum::SelectStmt(select) = node(parsed.protobuf.stmts[0].stmt.as_ref().unwrap()).unwrap() else { panic!("select") };
            let NodeEnum::ResTarget(target) = node(&select.target_list[0]).unwrap() else { panic!("target") };
            assert_eq!(target.name, if alias.is_empty() { "?column?" } else { "named" });
            assert!(matches!(node(target.val.as_ref().unwrap()).unwrap(), NodeEnum::TypeCast(_)));
            assert!(!rewritten.contains("->>") && !rewritten.contains("#>>"));
        }
    }
}

#[test]
fn json_extraction_through_views_is_masked_expression() {
    for definition in ["SELECT body FROM notes", "SELECT body->'show' FROM notes"] {
        let (mut snapshot, policy) = extraction_fixture(crate::json::filter::JsonMode::Show);
        let column = snapshot.relations.iter().find(|r| r.name == "notes").unwrap().columns.iter().find(|c| c.name == "body").unwrap().clone();
        snapshot.relations.push(Relation { schema: "public".into(), name: "json_view".into(), kind: RelationKind::View,
            columns: vec![column], view_definition: Some(definition.into()), primary_key: vec![] });
        let error = PgLowering.lower("SELECT body->'show' FROM json_view", &snapshot, &policy, &[]).unwrap_err();
        assert_eq!(error.code, NyCode::MaskedExpression);
    }
}


#[test]
fn json_predicate_guards_and_domains() {
    use crate::{json::filter::JsonMode, policy::{JsonPathTreatment, JsonTokenType, JsonLeafType, JsonPathRule, json_path::parse_path}};
    for mode in [JsonMode::Show, JsonMode::Tokenize] {
        let (mut snapshot, mut policy) = extraction_fixture(mode);
        let column = ColumnRef { schema: "public".into(), table: "notes".into(), column: "body".into() };
        policy.json_filters.get_mut(&column).unwrap().rules.push(JsonPathRule {
            column: column.clone(), path: parse_path("$.recipients[*].email").unwrap(),
            treatment: JsonPathTreatment::Blind { domain: DomainName("email".into()), ty: JsonTokenType::Text },
        });
        for base in [ColumnType::Json, ColumnType::Jsonb] {
            snapshot.relations.iter_mut().find(|r| r.name == "notes").unwrap().columns.iter_mut().find(|c| c.name == "body").unwrap().ty = base.clone();
            for ty in [JsonLeafType::String, JsonLeafType::Number, JsonLeafType::Boolean] {
                policy.json_filters.get_mut(&column).unwrap().rules[0].treatment = JsonPathTreatment::Show { ty };
                for predicate in ["body->>'show' = 'value'", "body->'show' IS NULL", "body->'show' IS NOT NULL", "body->'array'->0->>'child' = 'value'", "((body)->>'show') = 'value'", "((body->'parent')->>'child') IS NULL"] {
                    let sql = format!("SELECT id FROM notes WHERE {predicate}");
                    let statements = PgLowering.lower(&sql, &snapshot, &policy, &[]).unwrap();
                    let plan = crate::ir::check::check(&statements[0], &policy, &snapshot).unwrap();
                    let InputRewrite::ReplaceSpan { text, .. } = &plan.inputs[0] else { panic!("guard") };
                    let function = if base == ColumnType::Json { "json_typeof" } else { "jsonb_typeof" };
                    assert!(text.contains(function));
                    assert!(text.contains("CASE WHEN") && text.contains(" END)"));
                    let kind = if predicate.contains("array") || predicate.contains("parent") { "string" } else { match ty { JsonLeafType::String => "string", JsonLeafType::Number => "number", JsonLeafType::Boolean => "boolean" } };
                    assert!(text.contains(&format!("= '{kind}' THEN")));
                    let replacements = plan.inputs.iter().filter_map(|i| match i { InputRewrite::ReplaceSpan { span, text } => Some((span.clone(), text.clone())), _ => None }).collect::<Vec<_>>();
                    pg_query::parse(&crate::ir::rewrite::splice(&sql, &replacements).unwrap(), 0).unwrap();
                }
            }
            for predicate in ["body->>'blind' = $1", "body->>'blind' = 'token'", "body->>'blind' IS NULL", "body->>'blind' IS NOT NULL", "body->>'blind' = body->>'blind'"] {
                let sql = format!("SELECT id FROM notes WHERE {predicate}");
                let statements = PgLowering.lower(&sql, &snapshot, &policy, &[]).unwrap();
                let plan = crate::ir::check::check(&statements[0], &policy, &snapshot).unwrap();
                assert!(plan.inputs.iter().any(|i| matches!(i, InputRewrite::ReplaceSpan { text, .. } if text.contains("= 'string' THEN"))));
                if predicate.contains("$1") { assert!(plan.inputs.contains(&InputRewrite::DecryptParam { index: 1, domain: DomainName("email".into()), ty: ColumnType::Text })); }
                if predicate.contains("token") { assert!(plan.inputs.iter().any(|i| matches!(i, InputRewrite::DecryptLiteral { domain, ty: ColumnType::Text, .. } if domain.0 == "email"))); }
            }
            let sql = "SELECT e.id FROM notes e JOIN customers c ON e.body #>> '{recipients,0,email}' = c.email";
            let statements = PgLowering.lower(sql, &snapshot, &policy, &[]).unwrap();
            let plan = crate::ir::check::check(&statements[0], &policy, &snapshot).unwrap();
            assert!(plan.inputs.iter().any(|i| matches!(i, InputRewrite::ReplaceSpan { text, .. } if text.contains("= 'array'") && text.contains("= 'string'"))));
        }
        for ty in [JsonTokenType::Int4, JsonTokenType::Int8, JsonTokenType::Uuid, JsonTokenType::Date, JsonTokenType::Timestamp, JsonTokenType::Timestamptz] {
            policy.json_filters.get_mut(&column).unwrap().rules[1].treatment = JsonPathTreatment::Blind { domain: DomainName("email".into()), ty };
            let result = PgLowering.lower("SELECT id FROM notes WHERE body->>'blind' = $1", &snapshot, &policy, &[]);
            assert_eq!(result.unwrap_err().code, NyCode::MaskedExpression);
        }
    }
}

#[test]
fn json_predicate_refusal_matrix() {
    for mode in [crate::json::filter::JsonMode::Show, crate::json::filter::JsonMode::Tokenize] {
        let (snapshot, policy) = extraction_fixture(mode);
        let mut sqls: Vec<String> = [
            "body->'masked' IS NULL", "body->'masked' IS NOT NULL", "body->'parent' IS NULL",
            "body->'blind' = $1", "body#>'{blind}' = $1", "body->>'blind' <> $1",
            "body->>'blind' LIKE $1", "(body->>'blind')::int = $1", "body->>'blind' IN ($1)",
            "body->>'blind' = NULL", "body->>'blind' = id", "body->>'blind' = $1::int", "body->>'blind' = 'token'::int",
            "body @> '{}'", "body <@ '{}'", "body ? 'show'", "body ?| ARRAY['show']", "body ?& ARRAY['show']",
            "body @? '$.show'", "body @@ '$.show'", "body->$1 IS NULL", "body#>$1 IS NULL", "body->>$1 IS NULL", "body#>>$1 IS NULL",
            "jsonb_path_exists(body, '$.show')", "jsonb_path_query_first(body, '$.show') IS NULL",
            "jsonb_extract_path(body, 'show') IS NULL", "json_extract_path(body, 'show') IS NULL",
            "jsonb_extract_path_text(body, 'show') IS NULL", "json_extract_path_text(body, 'show') IS NULL",
            "jsonb_each(body) IS NULL", "json_each(body) IS NULL", "jsonb_each_text(body) IS NULL", "json_each_text(body) IS NULL",
        ].into_iter().map(|p| format!("SELECT id FROM notes WHERE {p}")).collect();
        sqls.extend([
            "UPDATE notes SET id = 1 WHERE body->>'show' IS NULL",
            "DELETE FROM notes WHERE body->>'show' IS NULL",
            "INSERT INTO notes (id) SELECT id FROM notes WHERE body->>'show' IS NULL",
            "SELECT id FROM (SELECT id, body FROM notes) q WHERE body->>'show' IS NULL",
            "WITH q AS (SELECT id, body FROM notes) SELECT id FROM q WHERE body->>'blind' IS NULL",
        ].into_iter().map(str::to_owned));
        for sql in sqls {
            let result = PgLowering.lower(&sql, &snapshot, &policy, &[]).and_then(|s| crate::ir::check::check(&s[0], &policy, &snapshot));
            assert_eq!(result.expect_err(&sql).code, NyCode::MaskedExpression, "{sql}");
        }
    }
}


#[test]
fn json_predicate_plain_paths_and_views() {
    let (mut snapshot, policy) = extraction_fixture(crate::json::filter::JsonMode::Show);
    let sql = "SELECT id FROM notes WHERE body->>'unrelated' = 'value'";
    let statements = PgLowering.lower(sql, &snapshot, &policy, &[]).unwrap();
    assert!(crate::ir::check::check(&statements[0], &policy, &snapshot).unwrap().inputs.is_empty());
    let column = snapshot.relations.iter().find(|r| r.name == "notes").unwrap().columns.iter().find(|c| c.name == "body").unwrap().clone();
    snapshot.relations.push(Relation { schema: "public".into(), name: "json_view".into(), kind: RelationKind::View,
        columns: vec![column], view_definition: Some("SELECT body FROM notes".into()), primary_key: vec![] });
    for predicate in ["body->>'show' = 'value'", "body->>'blind' IS NULL"] {
        let sql = format!("SELECT 1 FROM json_view WHERE {predicate}");
        assert_eq!(PgLowering.lower(&sql, &snapshot, &policy, &[]).unwrap_err().code, NyCode::MaskedExpression);
    }
}

#[test]
fn f1_numeric_path_guards_plain_projection_predicate_and_blind_projection() {
    use crate::{json::filter::JsonMode, policy::{JsonPathRule, JsonPathTreatment, JsonTokenType, json_path::parse_path}};
    let (snapshot, mut policy) = extraction_fixture(JsonMode::Show);
    let column = ColumnRef { schema: "public".into(), table: "notes".into(), column: "body".into() };
    let filter = policy.json_filters.get_mut(&column).unwrap();
    filter.rules = vec![JsonPathRule { column: column.clone(), path: parse_path("$.items[*].card").unwrap(), treatment: JsonPathTreatment::Masked },
        JsonPathRule { column, path: parse_path("$.recipients[*].email").unwrap(), treatment: JsonPathTreatment::Blind { domain: DomainName("email".into()), ty: JsonTokenType::Text } }];
    for sql in ["SELECT body #>> '{items,0,cvv}' FROM notes", "SELECT id FROM notes WHERE body #>> '{items,0,cvv}' = '123'", "SELECT body #>> '{recipients,0,email}' FROM notes"] {
        let stmt = PgLowering.lower(sql, &snapshot, &policy, &[]).unwrap();
        let plan = crate::ir::check::check(&stmt[0], &policy, &snapshot).unwrap();
        let inputs = plan.inputs.iter().filter_map(|i| if let InputRewrite::ReplaceSpan { span, text } = i { Some((span.clone(), text.clone())) } else { None }).collect::<Vec<_>>();
        assert_eq!(inputs.len(), 1);
        assert!(inputs[0].1.contains("= 'array'"));
        let rewritten = crate::ir::rewrite::splice(sql, &inputs).unwrap();
        pg_query::parse(&rewritten, 0).unwrap();
        if sql.contains("recipients") { assert!(inputs[0].1.contains("::text")); assert!(matches!(plan.outputs[0], OutputRewrite::JsonExtract { .. })); }
        else { assert!(matches!(plan.outputs[0], OutputRewrite::Pass)); }
    }
}

#[test]
fn r1_pg_c_whitespace_paths_refuse_projection_and_predicate() {
    use crate::{json::filter::JsonMode, policy::{JsonPathTreatment, json_path::parse_path}};
    let (snapshot, mut policy) = extraction_fixture(JsonMode::Show);
    let filter = policy.json_filters.values_mut().next().unwrap();
    filter.rules.truncate(1);
    filter.rules[0].path = parse_path("$.a.named.b").unwrap();
    filter.rules[0].treatment = JsonPathTreatment::Masked;
    for whitespace in [' ', '\t', '\n', '\x0b', '\x0c', '\r'] {
        for op in ["#>", "#>>"] {
            let expression = format!("body {op} '{{a,\"{whitespace}0\"}}'");
            for sql in [format!("SELECT {expression} FROM notes"), format!("SELECT id FROM notes WHERE {expression} = 'x'")] {
                let error = PgLowering.lower(&sql, &snapshot, &policy, &[]).unwrap_err();
                assert_eq!(error.code, NyCode::MaskedExpression);
            }
        }
    }
}

#[test]
fn r2_pg_guarded_projection_preserves_default_and_explicit_names() {
    use crate::{json::filter::JsonMode, policy::{JsonPathTreatment, JsonTokenType, json_path::parse_path}};
    let (snapshot, mut policy) = extraction_fixture(JsonMode::Show);
    let filter = policy.json_filters.values_mut().next().unwrap();
    filter.rules.truncate(1);
    filter.rules[0].path = parse_path("$.items[*].card").unwrap();
    filter.rules[0].treatment = JsonPathTreatment::Masked;
    // Cover guarded Plain, Masked, and filtered text/JSON targets.
    for path in ["{items,0,cvv}", "{items,0,card}"] {
        for op in ["#>", "#>>"] {
            for alias in ["", " AS named"] {
                let sql = format!("SELECT body {op} '{path}'{alias} FROM notes");
                let statements = PgLowering.lower(&sql, &snapshot, &policy, &[]).unwrap();
                let plan = crate::ir::check::check(&statements[0], &policy, &snapshot).unwrap();
                let replacements = plan.inputs.iter().filter_map(|i| match i {
                    InputRewrite::ReplaceSpan { span, text } => Some((span.clone(), text.clone())),
                    _ => None,
                }).collect::<Vec<_>>();
                assert_eq!(replacements.len(), 1);
                assert!(replacements[0].1.contains("CASE WHEN"));
                assert_eq!(replacements[0].1.ends_with(" AS \"?column?\""), alias.is_empty());
                let rewritten = crate::ir::rewrite::splice(&sql, &replacements).unwrap();
                let parsed = pg_query::parse(&rewritten, 0).unwrap();
                let NodeEnum::SelectStmt(select) = node(parsed.protobuf.stmts[0].stmt.as_ref().unwrap()).unwrap() else { panic!("select") };
                let NodeEnum::ResTarget(target) = node(&select.target_list[0]).unwrap() else { panic!("target") };
                assert_eq!(target.name, if alias.is_empty() { "?column?" } else { "named" });
            }
        }
    }
    let filter = policy.json_filters.values_mut().next().unwrap();
    filter.rules[0].treatment = JsonPathTreatment::Blind { domain: DomainName("email".into()), ty: JsonTokenType::Text };
    let sql = "SELECT body #>> '{items,0,card}' FROM notes";
    let statements = PgLowering.lower(sql, &snapshot, &policy, &[]).unwrap();
    let plan = crate::ir::check::check(&statements[0], &policy, &snapshot).unwrap();
    assert!(matches!(&plan.inputs[0], InputRewrite::ReplaceSpan { text, .. } if text.ends_with(" AS \"?column?\"")));
}

fn analyser_shapes_fixture() -> (Snapshot, Policy) {
    let (snapshot, mut policy) = fixture();
    let mut relations = snapshot.relations;
    relations.push(Relation {
        schema: "public".into(),
        name: "tickets".into(),
        kind: RelationKind::Table,
        columns: [
            ("status", ColumnType::Text),
            ("plan", ColumnType::Text),
            ("amount", ColumnType::Int4),
            ("tax_id", ColumnType::Text),
            ("created_at", ColumnType::Timestamp),
            ("resolved_at", ColumnType::Timestamp),
            ("date_of_birth", ColumnType::Date),
            ("blind_at", ColumnType::Timestamp),
        ]
        .into_iter()
        .map(|(name, ty)| Column {
            name: name.into(),
            ty,
            nullable: true,
            generated: None,
            default: None,
        })
        .collect(),
        view_definition: None,
        primary_key: vec![],
    });
    for (column, treatment) in [
        ("tax_id", Treatment::Masked { placeholder: None }),
        ("date_of_birth", Treatment::Masked { placeholder: None }),
        ("blind_at", Treatment::Blind { domain: DomainName("email".into()) }),
    ] {
        policy.columns.insert(
            ColumnRef {
                schema: "public".into(),
                table: "tickets".into(),
                column: column.into(),
            },
            treatment,
        );
    }
    let snapshot = Snapshot::new(relations, vec![], vec!["public".into()], 0);
    let policy = crate::policy::effective::legacy_policy(policy, &snapshot);
    (snapshot, policy)
}

#[test]
fn same_domain_coalesce_outputs_use_a_real_source() {
    let (snapshot, policy) = analyser_shapes_fixture();
    for sql in [
        "SELECT coalesce(c.email, o.to_address) FROM customers c JOIN outbox o ON c.id = o.id",
        "UPDATE outbox SET template = 'r' RETURNING coalesce(to_address, to_address)",
    ] {
        let s = PgLowering.lower(sql, &snapshot, &policy, &[]).unwrap().remove(0);
        let source = s.outputs[0].source.as_ref().unwrap();
        assert!(matches!(source.column.as_str(), "email" | "to_address"));
        assert_eq!(policy.domain_of(source), Some(&DomainName("email".into())));
        assert_eq!(
            crate::ir::check::check(&s, &policy, &snapshot).unwrap().outputs,
            [OutputRewrite::Tokenize { domain: DomainName("email".into()), ty: ColumnType::Text }]
        );
    }
    for (sql, code) in [
        ("SELECT coalesce(email, phone) FROM customers", NyCode::BlindOperation),
        ("SELECT coalesce(email, id) FROM customers", NyCode::BlindOperation),
        ("SELECT coalesce(email, 'x') FROM customers", NyCode::BlindOperation),
        ("SELECT coalesce(email, $1) FROM customers", NyCode::BlindOperation),
        ("SELECT coalesce(email, country) FROM customers", NyCode::MaskedExpression),
        ("SELECT coalesce(lower(email), email) FROM customers", NyCode::BlindOperation),
        ("SELECT coalesce(email, blind_at) FROM customers, tickets", NyCode::BlindOperation),
        ("SELECT lower(coalesce(email, email)) FROM customers", NyCode::BlindOperation),
        (
            "INSERT INTO outbox (to_address, template) SELECT coalesce(email, email), 't' FROM customers",
            NyCode::BlindOperation,
        ),
        ("SELECT id FROM customers WHERE coalesce(email, email) = $1", NyCode::BlindOperation),
    ] {
        let result = PgLowering.lower(sql, &snapshot, &policy, &[]).and_then(|stmts| {
            crate::ir::check::check(&stmts[0], &policy, &snapshot)
        });
        assert_eq!(result.unwrap_err().code, code, "{sql}");
    }
}

#[test]
fn coalesce_result_reused_downstream_or_written_is_still_refused() {
    let (snapshot, policy) = analyser_shapes_fixture();
    let run = |sql: &str| {
        PgLowering.lower(sql, &snapshot, &policy, &[]).and_then(|stmts| {
            crate::ir::check::check(&stmts[0], &policy, &snapshot)
        })
    };
    for sql in [
        "SELECT coalesce(email, email) AS e FROM customers ORDER BY e",
        "SELECT x FROM (SELECT coalesce(c.email, o.to_address) x FROM customers c, outbox o) d WHERE x LIKE 'a%'",
        "WITH c AS (SELECT coalesce(email, email) x FROM customers) SELECT lower(x) FROM c",
        "SELECT x FROM (SELECT coalesce(email, email) x FROM customers) d ORDER BY x",
        "SELECT min(x) FROM (SELECT coalesce(email, email) x FROM customers) d",
    ] {
        assert_eq!(run(sql).unwrap_err().code, NyCode::BlindOperation, "{sql}");
    }
    for sql in [
        "WITH c AS (SELECT coalesce(email, email) x FROM customers) INSERT INTO outbox (to_address, template) SELECT x, 't' FROM c",
        "UPDATE outbox SET template = (SELECT coalesce(email, email) FROM customers LIMIT 1)",
    ] {
        assert!(run(sql).is_err(), "{sql}");
    }
}

#[test]
fn aggregate_filter_uses_where_policy_and_rewrite_spans() {
    let (snapshot, policy) = analyser_shapes_fixture();
    let plan = |sql| {
        let s = PgLowering.lower(sql, &snapshot, &policy, &[])?.remove(0);
        crate::ir::check::check(&s, &policy, &snapshot)
    };
    for sql in [
        "SELECT count(*) FILTER (WHERE status = 'open') FROM tickets",
        "SELECT sum(amount) FILTER (WHERE plan = 'pro') FROM tickets",
        "SELECT count(email) FILTER (WHERE id = 1) FROM customers",
    ] {
        assert_eq!(plan(sql).unwrap().outputs, [OutputRewrite::Pass]);
    }
    for (sql, code) in [
        ("SELECT count(*) FILTER (WHERE email LIKE 'a%') FROM customers", NyCode::BlindOperation),
        ("SELECT count(*) FILTER (WHERE tax_id = '1') FROM tickets", NyCode::MaskedExpression),
        ("SELECT min(email) FILTER (WHERE true) FROM customers", NyCode::BlindOperation),
        ("SELECT sum(email) FILTER (WHERE id = 1) FROM customers", NyCode::BlindOperation),
        ("SELECT lower('x') FILTER (WHERE true)", NyCode::NotAnalysable),
        ("SELECT $1, count(*) FILTER (WHERE email = $1) FROM customers", NyCode::BlindOperation),
    ] {
        assert_eq!(plan(sql).unwrap_err().code, code, "{sql}");
    }
    let domain = DomainName("email".into());
    let sql = "SELECT count(*) FILTER (WHERE email = 'tok_email_a') FROM customers";
    let result = plan(sql).unwrap();
    assert!(matches!(result.inputs.as_slice(),
        [InputRewrite::DecryptLiteral { span, domain: d, ty: ColumnType::Text }]
            if &sql[span.start..span.end] == "'tok_email_a'" && d == &domain));
    let sql = "SELECT count(*) FILTER (WHERE email = $1) FROM customers";
    assert_eq!(
        plan(sql).unwrap().inputs,
        [InputRewrite::DecryptParam { index: 1, domain, ty: ColumnType::Text }]
    );
}

#[test]
fn extract_accepts_plain_values_and_rejects_protected_inputs() {
    let (snapshot, policy) = analyser_shapes_fixture();
    let plan = |sql| {
        let s = PgLowering.lower(sql, &snapshot, &policy, &[])?.remove(0);
        crate::ir::check::check(&s, &policy, &snapshot)
    };
    for sql in [
        "SELECT extract(epoch FROM resolved_at - created_at) FROM tickets",
        "SELECT extract(year FROM created_at) FROM tickets",
        "SELECT pg_catalog.extract('year', created_at) FROM tickets",
        "SELECT date_part('year', created_at) FROM tickets",
    ] {
        assert_eq!(plan(sql).unwrap().outputs, [OutputRewrite::Pass], "{sql}");
    }
    for (sql, code) in [
        ("SELECT extract(year FROM date_of_birth) FROM tickets", NyCode::MaskedExpression),
        ("SELECT extract(year FROM blind_at) FROM tickets", NyCode::BlindOperation),
        ("SELECT extract(epoch FROM blind_at - created_at) FROM tickets", NyCode::BlindOperation),
        ("SELECT date_part('year', blind_at) FROM tickets", NyCode::BlindOperation),
    ] {
        assert_eq!(plan(sql).unwrap_err().code, code, "{sql}");
    }
}

#[test]
fn case_with_blind_branch_is_still_an_operation_refusal() {
    let (snapshot, policy) = fixture();
    let s = lower("SELECT CASE WHEN id > 0 THEN email ELSE 'x' END FROM customers");
    assert_eq!(
        crate::ir::check::check(&s, &policy, &snapshot).unwrap_err().code,
        NyCode::BlindOperation
    );
}
