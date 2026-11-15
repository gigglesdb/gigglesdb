//! Data-driven lowering and policy coverage; BUG entries retain intended outcomes.
use nunya::{
    dialect::{Lowering, pg::PgLowering},
    ir::{InputRewrite, OutputRewrite, RewritePlan, check::check},
};
use serde::Deserialize;
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Corpus {
    case: Vec<Case>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    sql: String,
    expect: String,
    plan: Option<Plan>,
    note: String,
    intended: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Plan {
    inputs: Vec<String>,
    outputs: Vec<String>,
}
fn summarize(plan: &RewritePlan) -> Plan {
    Plan {
        inputs: plan
            .inputs
            .iter()
            .map(|i| match i {
                InputRewrite::DecryptParam { index, domain, .. } => {
                    format!("param:{index}:{}", domain.0)
                }
                InputRewrite::DecryptLiteral { domain, .. } => format!("literal:{}", domain.0),
            })
            .collect(),
        outputs: plan
            .outputs
            .iter()
            .map(|o| match o {
                OutputRewrite::Pass => "pass".into(),
                OutputRewrite::Tokenize { domain, .. } => format!("tokenize:{}", domain.0),
                OutputRewrite::Mask { .. } => "mask".into(),
            })
            .collect(),
    }
}
#[test]
fn policy_corpus() {
    let (snapshot, policy) = crate::common::harness_fixture();
    let mut total = 0;
    let mut passed = 0;
    let mut bugs = 0;
    let mut failures = Vec::new();
    for (file, text) in [
        ("allowed", include_str!("corpus/allowed.toml")),
        ("refused", include_str!("corpus/refused.toml")),
        ("writes", include_str!("corpus/writes.toml")),
        ("routes", include_str!("corpus/routes.toml")),
        ("catalog", include_str!("corpus/catalog.toml")),
    ] {
        let corpus: Corpus =
            toml::from_str(text).unwrap_or_else(|e| panic!("invalid {file} corpus: {e}"));
        for (index, case) in corpus.case.into_iter().enumerate() {
            total += 1;
            assert!(!case.note.is_empty(), "missing note: {file}/{}", index + 1);
            let result = PgLowering
                .lower(&case.sql, &snapshot, &policy, &[])
                .and_then(|statements| {
                    assert_eq!(statements.len(), 1, "one statement per corpus case");
                    check(&statements[0], &policy, &snapshot)
                });
            let outcome = match &result {
                Ok(_) => "allow",
                Err(e) => e.code.sqlstate(),
            };
            if case.expect == "BUG" {
                bugs += 1;
                let intended = case
                    .intended
                    .as_deref()
                    .expect("BUG needs intended outcome");
                assert!(
                    intended == "allow"
                        || nunya::error::NyCode::ALL
                            .iter()
                            .any(|c| c.sqlstate() == intended)
                );
                eprintln!(
                    "BUG {file}/{} intended={intended} actual={outcome}: {}",
                    index + 1,
                    case.note
                );
                continue;
            }
            assert!(
                case.intended.is_none(),
                "intended belongs only on BUG entries"
            );
            let plan_matches = match (&case.plan, &result) {
                (Some(expected), Ok(actual)) => {
                    let actual = summarize(actual);
                    expected.inputs == actual.inputs && expected.outputs == actual.outputs
                }
                (Some(_), Err(_)) => false,
                (None, _) => true,
            };
            if outcome == case.expect && plan_matches {
                passed += 1;
            } else {
                failures.push(format!(
                    "{file}/{} expected={} actual={outcome} plan_matches={plan_matches}: {} SQL={}",
                    index + 1,
                    case.expect,
                    case.note,
                    case.sql
                ));
            }
        }
    }
    eprintln!(
        "corpus: cases={total} passed={passed} failed={} BUG-marked={bugs}",
        failures.len()
    );
    assert!(total >= 150, "corpus must cover at least 150 cases");
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn cross_domain_policy_rules() {
    use nunya::{
        error::NyCode,
        policy::{DomainName, DomainSettings, TokenizeSetting, Treatment},
        schema::ColumnRef,
    };
    let (snapshot, mut policy) = crate::common::harness_fixture();
    let phone = DomainName("phone".into());
    policy.columns.insert(
        ColumnRef {
            schema: "public".into(),
            table: "customers".into(),
            column: "phone".into(),
        },
        Treatment::Blind {
            domain: phone.clone(),
        },
    );
    policy.domains.insert(
        phone,
        DomainSettings {
            tokenize: TokenizeSetting::On { limit_per_task: 20 },
            range: None,
        },
    );
    for sql in [
        "SELECT id FROM customers WHERE email=phone",
        "UPDATE customers SET phone=email",
        "INSERT INTO outbox(to_address) SELECT phone FROM customers",
        "SELECT id FROM customers WHERE email=$1 AND phone=$1",
    ] {
        let statements = PgLowering
            .lower(sql, &snapshot, &policy, &[])
            .expect("lower synthetic two-domain case");
        assert_eq!(
            check(&statements[0], &policy, &snapshot).unwrap_err().code,
            NyCode::TokenOutsideDomain,
            "cross-domain case: {sql}"
        );
    }
}

#[test]
fn worked_plain_write_and_local_tokenize() {
    use nunya::ir::{Label, ValueSource};
    let (snapshot, policy) = crate::common::harness_fixture();
    let statements = PgLowering
        .lower(
            "UPDATE notes SET body='tok_email_x' WHERE id=3",
            &snapshot,
            &policy,
            &[],
        )
        .expect("lower worked write");
    assert_eq!(statements[0].writes.len(), 1);
    assert_eq!(statements[0].writes[0].label, Label::Plain);
    assert!(matches!(
        statements[0].writes[0].value,
        ValueSource::Literal { .. }
    ));
    assert!(check(&statements[0], &policy, &snapshot).is_ok());
    for (sql, param) in [
        ("SELECT nunya_tokenize('email',$1)", true),
        ("SELECT nunya_tokenize('email','hello')", false),
    ] {
        let statements = PgLowering
            .lower(sql, &snapshot, &policy, &[])
            .expect("lower local tokenize");
        let plan = check(&statements[0], &policy, &snapshot).expect("allow local tokenize");
        let (domain, arg) = plan.tokenize.expect("local tokenize plan");
        assert_eq!(domain.0, "email");
        assert!(if param {
            matches!(arg, ValueSource::Param(1))
        } else {
            matches!(arg, ValueSource::Literal { .. })
        });
        assert!(plan.inputs.is_empty() && plan.outputs.is_empty());
    }
}
