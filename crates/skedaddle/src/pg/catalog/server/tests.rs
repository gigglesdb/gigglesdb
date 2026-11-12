use super::*;
use crate::pg::chunk::{Direction, KeyColumn, TableKey};
fn table(directions: &[Direction]) -> TableKey {
    TableKey {
        table: "quoted table".into(),
        relation: QualifiedName {
            schema: "s\"q".into(),
            name: "r WHERE ORDER BY".into(),
        },
        columns: directions
            .iter()
            .enumerate()
            .map(|(i, direction)| KeyColumn {
                name: format!("k{i}"),
                pg_type: PgType::Text {
                    encoding: "UTF8".into(),
                },
                sql_type: QualifiedName {
                    schema: "pg_catalog".into(),
                    name: "text".into(),
                },
                collation: Some(QualifiedName {
                    schema: "locale schema".into(),
                    name: "odd\"locale".into(),
                }),
                direction: *direction,
            })
            .collect(),
    }
}
fn text(value: &str) -> Value {
    Value::Text(crate::mapping::EncodedBytes {
        bytes: value.as_bytes().to_vec(),
        encoding: "UTF8".into(),
    })
}
fn bounded(table: &TableKey) -> ChunkRange {
    let samples = vec![
        vec![text("a"); table.columns.len()],
        vec![text("z"); table.columns.len()],
    ];
    ChunkPlan::new(table.clone(), 1, samples).unwrap().ranges[1].clone()
}
#[test]
fn membership_retains_exact_chunk_predicates_order_collations_and_casts() {
    for directions in [
        vec![Direction::Asc],
        vec![Direction::Desc],
        vec![Direction::Asc, Direction::Asc],
        vec![Direction::Desc, Direction::Desc],
        vec![Direction::Asc, Direction::Desc],
    ] {
        let table = table(&directions);
        let range = bounded(&table);
        let chunk = table.read_query(&range).unwrap();
        let key = vec![text("'); DELETE FROM rows; --"); directions.len()];
        let query = membership_query(&table, &range, &[key]).unwrap();
        let prefix = format!("SELECT * FROM {} WHERE ", table.relation.sql().unwrap());
        let suffix = chunk.sql.strip_prefix(&prefix).unwrap();
        let generated = if directions.iter().all(|d| *d == directions[0]) {
            explicit_rows(suffix, 4).unwrap()
        } else {
            suffix.to_string()
        };
        assert!(query.sql.ends_with(&format!("WHERE {generated}")));
        if directions.iter().all(|d| *d == directions[0]) {
            assert!(query.sql.contains("ROW("));
        } else {
            assert!(!query.sql.contains("ROW("));
        }
        assert_eq!(
            &query.parameters[..chunk.parameters.len()],
            &chunk.parameters
        );
        assert!(!query.sql.contains("DELETE"));
        assert!(query.parameters.iter().any(|p| p.contains("DELETE")));
        assert!(
            query
                .sql
                .contains("COLLATE \"locale schema\".\"odd\"\"locale\"")
        );
        assert!(query.sql.contains("::text::\"pg_catalog\".\"text\""));
        assert_eq!(
            query.sql.contains(" OR "),
            directions.len() == 2 && directions[0] != directions[1]
        );
        assert!(query.sql.contains("ASC") || query.sql.contains("DESC"));
    }
}
#[test]
fn membership_ordinals_are_bound_and_do_not_collide_with_key_identifiers() {
    let mut table = table(&[Direction::Asc]);
    table.columns[0].name = "__skedaddle_catalog_ordinal".into();
    let range = bounded(&table);
    let q = membership_query(&table, &range, &[vec![text("")], vec![text("A")]]).unwrap();
    assert!(q.sql.starts_with("SELECT \"__skedaddle_catalog_ordinal_\""));
    assert_eq!(q.parameters, vec!["a", "z", "0", "", "1", "A"]);
    assert!(q.sql.contains("$3::text::bigint"));
    assert!(q.sql.contains("$5::text::bigint"));
    assert!(membership_query(&table, &range, &[]).is_err());
    assert!(membership_query(&table, &range, &[vec![Value::Null]]).is_err());
}
fn evidence() -> CollationEvidence {
    CollationEvidence {
        provider: "i".into(),
        deterministic: true,
        encoding: -1,
        locale: Some("und".into()),
        lc_collate: None,
        lc_ctype: None,
        rules: None,
        recorded_version: Some("153.120".into()),
        actual_version: Some("153.120".into()),
        fixed_byte_order: false,
    }
}
#[test]
fn collation_versions_fail_closed_except_fixed_c_posix() {
    let e = evidence();
    assert!(qualify_version(&e).is_ok());
    for (recorded, actual) in [
        (None, None),
        (Some("1"), None),
        (None, Some("1")),
        (Some("1"), Some("2")),
        (Some(""), Some("")),
    ] {
        let mut e = evidence();
        e.recorded_version = recorded.map(str::to_string);
        e.actual_version = actual.map(str::to_string);
        assert!(qualify_version(&e).is_err());
    }
    let mut c = evidence();
    c.fixed_byte_order = true;
    c.recorded_version = None;
    c.actual_version = None;
    assert!(qualify_version(&c).is_ok());
    c.actual_version = Some("unexpected".into());
    assert!(qualify_version(&c).is_err());
}

#[test]
fn explicit_row_adapter_is_quote_aware_and_fails_closed() {
    let mut t = table(&[Direction::Asc, Direction::Asc]);
    t.columns[0].name = "key )(( \" ROW(".into();
    t.columns[0].collation.as_mut().unwrap().name = "collation )(( \" ROW(".into();
    let range = bounded(&t);
    let chunk = t.read_query(&range).unwrap();
    let prefix = format!("SELECT * FROM {} WHERE ", t.relation.sql().unwrap());
    let original = chunk.sql.strip_prefix(&prefix).unwrap();
    let adapted = explicit_rows(original, 4).unwrap();
    // Remove ROW tokens only outside quoted identifiers. This proves the
    // adaptation preserves the builder's entire predicate and ordering text.
    let mut unwrapped = String::new();
    let mut i = 0;
    let mut quoted = false;
    while i < adapted.len() {
        if adapted.as_bytes()[i] == b'"' {
            unwrapped.push('"');
            i += 1;
            if quoted && adapted.as_bytes().get(i) == Some(&b'"') {
                unwrapped.push('"');
                i += 1;
            } else {
                quoted = !quoted;
            }
        } else if !quoted && adapted[i..].starts_with("ROW(") {
            i += 3;
        } else {
            let c = adapted[i..].chars().next().unwrap();
            unwrapped.push(c);
            i += c.len_utf8();
        }
    }
    assert_eq!(unwrapped, original);
    let q = membership_query(&t, &range, &[vec![text("a"), text("b")]]).unwrap();
    assert!(q.sql.ends_with(&format!("WHERE {adapted}")));
    for broken in ["((a) >= (b)", "((a) >= (b)))", "((\"a) >= (b))"] {
        assert!(explicit_rows(broken, 2).is_err());
    }
    assert!(explicit_rows("((a) >= (b))", 4).is_err());
}
