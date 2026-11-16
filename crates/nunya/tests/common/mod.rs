//! Harness fixtures shared by offline and opt-in network tests.
use nunya::{
    policy::{DomainName, DomainSettings, Policy, ReadLogSink, TokenizeSetting, Treatment},
    schema::{Column, ColumnRef, Relation, RelationKind, Snapshot},
    value::ColumnType,
};
use std::collections::BTreeMap;

/// Mirror harness/seed.sql and harness/policy.toml without a database connection.
pub fn harness_fixture() -> (Snapshot, Policy) {
    let mut relations = Vec::new();
    for (table, names) in [
        (
            "customers",
            vec!["id", "email", "country", "phone", "failed_payments"],
        ),
        ("outbox", vec!["id", "to_address", "template"]),
        ("notes", vec!["id", "body"]),
    ] {
        relations.push(Relation {
            schema: "public".into(),
            name: table.into(),
            kind: RelationKind::Table,
            columns: names
                .into_iter()
                .map(|name| Column {
                    name: name.into(),
                    ty: if matches!(name, "id" | "failed_payments") {
                        ColumnType::Int4
                    } else {
                        ColumnType::Text
                    },
                    nullable: name != "id",
                    generated: None,
                    default: if table == "outbox" && name == "id" {
                        Some("nextval('outbox_id_seq'::regclass)".into())
                    } else {
                        None
                    },
                })
                .collect(),
            view_definition: None,
            primary_key: vec!["id".into()],
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
    let column = |table: &str, name: &str| ColumnRef {
        schema: "public".into(),
        table: table.into(),
        column: name.into(),
    };
    let domain = DomainName("email".into());
    let policy = Policy {
        columns: BTreeMap::from([
            (
                column("customers", "email"),
                Treatment::Blind {
                    domain: domain.clone(),
                },
            ),
            (
                column("outbox", "to_address"),
                Treatment::Blind {
                    domain: domain.clone(),
                },
            ),
            (
                column("customers", "phone"),
                Treatment::Masked {
                    placeholder: Some("[hidden]".into()),
                },
            ),
        ]),
        domains: BTreeMap::from([(
            domain,
            DomainSettings {
                tokenize: TokenizeSetting::On {
                    limit_per_session: 20,
                    limit_per_principal: 200,
                    principal_window: std::time::Duration::from_secs(86400),
                },
                range: None,
            },
        )]),
        snapshot_max_age_ms: 60_000,
        readlog: ReadLogSink::Stdout,
    };
    (
        Snapshot::new(relations, vec![], vec!["public".into()], 0),
        policy,
    )
}
