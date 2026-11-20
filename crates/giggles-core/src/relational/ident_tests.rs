use super::*;
use std::collections::BTreeSet;
fn parser(sql: &str) -> Parser {
    Parser { privilege_seen: false, privilege_casts: false,
        tokens: super::super::lex(sql).unwrap(),
        at: 0,
        params: BTreeSet::new(),
        relations: Vec::new(),
    }
}
#[test]
fn lexical_folding_and_quoted_names() {
    let mut p = parser("\u{00c4}BC$z _X \"A\"\"B\" \"default\"");
    assert_eq!(p.name().unwrap(), "\u{00c4}bc$z");
    assert_eq!(p.name().unwrap(), "_x");
    assert_eq!(p.name().unwrap(), "A\"B");
    assert_eq!(p.name().unwrap(), "default");
}
#[test]
fn truncation_is_at_utf8_boundary_and_before_reserved_checks() {
    let long = format!("{}\u{00e9}z", "a".repeat(62));
    assert_eq!(parser(&long).name().unwrap(), "a".repeat(62));
    assert_eq!(
        parser(&format!("\"{}\"", long)).name().unwrap(),
        "a".repeat(62)
    );
    assert_eq!(
        parser(&format!("pg_{}", "a".repeat(80))).name(),
        Err(SqlError::Reserved)
    );
}
#[test]
fn schema_and_column_resolution() {
    assert_eq!(
        parser("PUBLIC.\"Mixed\"").table_name().unwrap(),
        "public\0Mixed"
    );
    let mut p = parser("other.t");
    assert_eq!(p.table_name().unwrap(), "other\0t");
    assert_eq!(p.relations[0].schema.as_deref(), Some("other"));
    let mut p = parser("other.t.c");
    assert_eq!(p.qualified_name().unwrap(), (Some("t".into()), "c".into()));
    assert_eq!(p.relations[0].schema.as_deref(), Some("other"));
}
#[test]
fn stored_identifiers_allow_exact_spelling_but_keep_limits_and_reservations() {
    assert!(ident("9 spaces.\"$\u{00c4}").is_ok());
    for value in ["", "a\0b"] {
        assert_eq!(ident(value), Err(SqlError::Syntax));
    }
    assert_eq!(ident(&"a".repeat(64)), Err(SqlError::Syntax));
    assert_eq!(ident("pg_x"), Err(SqlError::Reserved));
    assert!(ident("PG_x").is_ok());
}

#[test]
fn native_create_keeps_canonical_ascii_policy() {
    use super::super::{ColumnDef, DataType, Prepared, Relational};
    for name in ["Mixed", "with space", "_prefix", "cash$", "\u{00e9}"] {
        let sql = format!("\"{name}\"");
        assert!(parser(&sql).name().is_ok());
        let result = Prepared::create_engine(
            name.into(),
            vec![ColumnDef {
                id: 0,
                name: "id".into(),
                data_type: DataType::Integer,
                not_null: true,
            }],
            vec!["id".into()],
            1,
            &Relational::default(),
        );
        assert!(matches!(result, Err(SqlError::Syntax)), "{name}");
    }
}
#[test]
fn automatic_index_names_stay_in_utf8_byte_budget() {
    for table in ["t".repeat(63), "\u{00e9}".repeat(31)] {
        let sql = format!(
            "create index on \"{table}\" (\"{}\")",
            "\u{00e9}".repeat(31)
        );
        let index = super::super::sql_index::parse(&mut parser(&sql)).unwrap();
        assert!(index.name.len() <= 63);
        assert!(index.name.ends_with("_idx"));
    }
}
