use super::*;
use crate::{
    policy::{DomainName, ReadLogSink},
    schema::{Column, Relation},
};
use std::collections::BTreeMap;
fn fixture() -> (Snapshot, Policy) {
    let mut policy = Policy {
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
    (
        Snapshot::new(relations, vec![], vec!["public".into()], 0),
        policy,
    )
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
        "SELECT count(email) FILTER (WHERE id = 1) FROM customers",
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
fn case15_set_mixed_is_tainted() {
    let s = lower("SELECT email FROM customers UNION SELECT body FROM notes");
    assert_eq!(s.outputs[0].label, Label::Tainted);
    assert!(s.outputs[0].source.is_none());
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
    assert_eq!(
        lower("SELECT phone FROM customers UNION SELECT email FROM customers").outputs[0].label,
        Label::Tainted
    );
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
        "RESET ALL",
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
