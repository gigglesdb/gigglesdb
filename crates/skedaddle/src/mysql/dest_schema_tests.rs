use super::*;
use crate::mysql::schema::Collation as SourceCollation;
fn column(name: &str, ordinal: u64, ty: &str) -> ColumnDescriptor {
    let data_type = ty.split(['(', ' ']).next().unwrap();
    let text = matches!(
        data_type,
        "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" | "enum" | "set"
    );
    ColumnDescriptor {
        name: name.into(),
        ordinal,
        data_type: data_type.into(),
        column_type: ty.into(),
        character_set: text.then(|| "utf8mb4".into()),
        collation: text.then(|| SourceCollation {
            name: "utf8mb4_0900_ai_ci".into(),
            id: 255,
        }),
        nullable: name != "id",
        generated: false,
        column_default: None,
        default_generated: false,
        generation_expression: None,
    }
}
fn table(columns: Vec<ColumnDescriptor>) -> TableDescriptor {
    TableDescriptor {
        schema: "type_zoo".into(),
        table: "zoo".into(),
        columns,
        primary_key: vec!["id".into()],
    }
}
#[test]
fn deterministic_zoo_all_kinds() {
    let types = [
        "bigint",
        "tinyint",
        "tinyint unsigned",
        "smallint",
        "smallint unsigned",
        "mediumint",
        "mediumint unsigned",
        "int",
        "int unsigned",
        "bigint",
        "bigint unsigned",
        "decimal(65,30)",
        "float",
        "double",
        "bit(17)",
        "date",
        "datetime(6)",
        "timestamp(6)",
        "time(6)",
        "year",
        "char(8)",
        "varchar(128)",
        "text",
        "binary(8)",
        "blob",
        "enum('zero','one','two')",
        "set('a','b','c')",
        "json",
        "varchar(8)",
        "mediumtext",
    ];
    let mut t = table(
        types
            .iter()
            .enumerate()
            .map(|(i, ty)| column(if i == 0 { "id" } else { "unused" }, i as u64 + 1, ty))
            .collect(),
    );
    for (i, c) in t.columns.iter_mut().enumerate().skip(1) {
        c.name = format!("c{i}");
    }
    let schema = build(&[t.clone()]).unwrap();
    assert_eq!(schema.statements.len(), 3);
    assert_eq!(
        schema.statements[0],
        "CREATE COLLATION IF NOT EXISTS \"public\".\"skedaddle_und_ai_ci\" (provider = icu, locale = E'und-u-ks-level1', deterministic = false)"
    );
    assert_eq!(
        schema.statements[1],
        "CREATE SCHEMA IF NOT EXISTS \"type_zoo\""
    );
    assert!(schema.statements[2].starts_with(
        "CREATE TABLE IF NOT EXISTS \"type_zoo\".\"zoo\" (\"id\" pg_catalog.int8 NOT NULL"
    ));
    assert!(schema.statements[2].contains("\"c10\" pg_catalog.numeric(20,0)"));
    assert!(schema.statements[2].contains("CHECK (\"c25\" IN (E'zero', E'one', E'two'))"));
    assert_eq!(
        schema
            .digest
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>(),
        "4f9b074e5bb076fc61a7d59bc63d00574632b79c6a35bfcef3d4f3db5909f4f5"
    );
    let expected_types = [
        "int8",
        "int2",
        "int2",
        "int2",
        "int4",
        "int4",
        "int4",
        "int4",
        "int8",
        "int8",
        "numeric(20,0)",
        "numeric(65,30)",
        "float4",
        "float8",
        "bytea",
        "date",
        "timestamp(6)",
        "timestamptz(6)",
        "interval",
        "int2",
        "bpchar(8)",
        "varchar(128)",
        "text",
        "bytea",
        "bytea",
        "text",
        "text",
        "jsonb",
        "varchar(8)",
        "text",
    ];
    let fields = expected_types
        .iter()
        .enumerate()
        .map(|(i, ty)| {
            let name = if i == 0 { "id".into() } else { format!("c{i}") };
            let mut ddl = format!("\"{name}\" pg_catalog.{ty}");
            if matches!(i, 20 | 21 | 22 | 25 | 26 | 28 | 29) {
                ddl.push_str(" COLLATE \"public\".\"skedaddle_und_ai_ci\"");
            }
            if i == 0 {
                ddl.push_str(" NOT NULL");
            }
            if i == 25 {
                ddl.push_str(" CHECK (\"c25\" IN (E'zero', E'one', E'two'))");
            }
            ddl
        })
        .collect::<Vec<_>>()
        .join(", ");
    assert_eq!(
        schema.statements[2],
        format!("CREATE TABLE IF NOT EXISTS \"type_zoo\".\"zoo\" ({fields}, PRIMARY KEY (\"id\"))")
    );
    t.columns.reverse();
    assert_eq!(schema.statements, build(&[t.clone()]).unwrap().statements);
    assert_eq!(schema.digest, build(&[t.clone()]).unwrap().digest);
    let canonical = serde_json::to_vec(&(
        VERSION,
        &{
            let mut ts = vec![t.clone()];
            ts[0].columns.sort_by_key(|c| c.ordinal);
            ts
        },
        &schema.statements,
        &schema.omitted_defaults,
    ))
    .unwrap();
    assert_eq!(schema.digest, <[u8; 32]>::from(Sha256::digest(canonical)));
    t.columns[0].column_default = Some("different".into());
    assert_ne!(schema.digest, build(&[t]).unwrap().digest);
}
#[test]
fn remaining_kinds_and_collations() {
    for ty in [
        "integer",
        "numeric(10,2)",
        "real",
        "varbinary(9)",
        "tinyblob",
        "mediumblob",
        "longblob",
        "tinytext",
        "longtext",
        "decimal",
    ] {
        build(&[table(vec![column("id", 1, "bigint"), column("v", 2, ty)])]).unwrap();
    }
    for (source, dest) in [
        ("utf8mb4_bin", "pg_catalog\".\"C"),
        ("utf8mb4_general_ci", "public\".\"skedaddle_und_ci"),
        ("utf8mb4_0900_as_cs", "public\".\"skedaddle_und_as_cs"),
    ] {
        let mut t = table(vec![column("id", 1, "int"), column("v", 2, "text")]);
        t.columns[1].collation.as_mut().unwrap().name = source.into();
        assert!(
            build(&[t])
                .unwrap()
                .statements
                .last()
                .unwrap()
                .contains(dest)
        );
    }
}
#[test]
fn enum_identifier_and_literal_quoting() {
    let c = column("a'\"b", 2, "enum('it''s','a\\\\b','x); DROP TABLE zoo;--')");
    let ddl = build(&[table(vec![column("id", 1, "int"), c])]).unwrap();
    assert!(
        ddl.statements
            .last()
            .unwrap()
            .contains("CHECK (\"a'\"\"b\" IN (E'it''s', E'a\\\\b', E'x); DROP TABLE zoo;--'))")
    );
}
#[test]
fn defaults_and_generated_subset() {
    let mut t = table(vec![column("id", 1, "int"), column("v", 2, "int")]);
    t.columns[0].column_default = Some("42".into());
    t.columns[1].generated = true;
    t.columns[1].generation_expression = Some("`id`".into());
    let s = build(&[t.clone()]).unwrap();
    assert!(s.statements.last().unwrap().contains("DEFAULT E'42'"));
    assert!(
        s.statements
            .last()
            .unwrap()
            .contains("GENERATED ALWAYS AS (\"id\") STORED")
    );
    t.columns[1].generation_expression = Some("id + 1".into());
    assert_eq!(
        build(&[t.clone()]).unwrap_err().kind,
        BlockerKind::GeneratedExpression
    );
    t.columns[1].generation_expression = None;
    assert_eq!(
        build(&[t.clone()]).unwrap_err().kind,
        BlockerKind::GeneratedExpression
    );
    t.columns[1].generated = false;
    t.columns[0].default_generated = true;
    t.columns[1].column_default = Some("2147483648".into());
    assert_eq!(build(&[t]).unwrap().omitted_defaults.len(), 2);
}
#[test]
fn lossless_text_decimal_defaults_and_lossy_char_omission() {
    let mut t = table(vec![
        column("id", 1, "int"),
        column("v", 2, "varchar(30)"),
        column("d", 3, "decimal(5,2)"),
        column("c", 4, "char(8)"),
    ]);
    for (c, v) in t
        .columns
        .iter_mut()
        .skip(1)
        .zip(["a'\\b", "12.30", "trailing "])
    {
        c.column_default = Some(v.into());
    }
    let s = build(&[t]).unwrap();
    assert!(s.statements.last().unwrap().contains("DEFAULT E'a''\\\\b'"));
    assert!(s.statements.last().unwrap().contains("DEFAULT E'12.30'"));
    assert_eq!(s.omitted_defaults.len(), 1);
}
#[test]
fn malformed_descriptors_block() {
    for change in 0..5 {
        let mut t = table(vec![column("id", 1, "int"), column("v", 2, "int")]);
        match change {
            0 => t.columns[1].name = "id".into(),
            1 => t.columns[1].ordinal = 1,
            2 => t.primary_key.clear(),
            3 => t.columns[0].nullable = true,
            _ => t.schema = "x".repeat(64),
        }
        assert_eq!(build(&[t]).unwrap_err().kind, BlockerKind::Descriptor);
    }
}
#[test]
fn table_maps_ignore_default_and_generation_metadata() {
    use crate::mysql::{
        binlog::{MysqlColumn, MysqlTable},
        schema::compare_table_map,
    };
    let mut t = table(vec![column("id", 1, "int")]);
    let map = MysqlTable {
        id: 1,
        full_metadata: vec![],
        schema: b"type_zoo".to_vec(),
        table: b"zoo".to_vec(),
        columns: vec![MysqlColumn {
            name: b"id".to_vec(),
            column_type: 3,
            metadata: vec![],
            nullable: false,
            unsigned: Some(false),
            collation: None,
            labels: None,
        }],
    };
    assert!(compare_table_map(&t, &map).is_ok());
    t.columns[0].column_default = Some("42".into());
    t.columns[0].default_generated = true;
    t.columns[0].generation_expression = Some("id + 1".into());
    assert!(compare_table_map(&t, &map).is_ok());
}

#[test]
fn enum_set_literal_defaults_and_unsupported_defaults() {
    let mut t = table(vec![
        column("id", 1, "int"),
        column("e", 2, "enum('a','b')"),
        column("s", 3, "set('a','b')"),
        column("j", 4, "json"),
    ]);
    for (c, v) in t.columns.iter_mut().skip(1).zip(["b", "a,b", "{}"]) {
        c.column_default = Some(v.into());
    }
    let plan = build(&[t]).unwrap();
    assert!(plan.statements.last().unwrap().contains("DEFAULT E'b'"));
    assert!(plan.statements.last().unwrap().contains("DEFAULT E'a,b'"));
    assert_eq!(plan.omitted_defaults.len(), 1);
}

#[test]
fn generated_malformed_tokens_and_changed_domains_block() {
    let mut t = table(vec![column("id", 1, "int"), column("v", 2, "int")]);
    t.columns[1].generated = true;
    for expression in ["`", "id + 1", "(id)", "CAST(id AS SIGNED)", "`id` + `id`"] {
        t.columns[1].generation_expression = Some(expression.into());
        assert_eq!(
            build(&[t.clone()]).unwrap_err().kind,
            BlockerKind::GeneratedExpression
        );
    }
    t.columns[1].generation_expression = Some("id".into());
    t.columns[1].column_type = "bigint".into();
    t.columns[1].data_type = "bigint".into();
    assert_eq!(
        build(&[t]).unwrap_err().kind,
        BlockerKind::GeneratedExpression
    );
    assert_eq!(
        build(&[table(vec![
            column("id", 1, "int"),
            column("geo", 2, "geometry")
        ])])
        .unwrap_err()
        .kind,
        BlockerKind::Mapping
    );
}
#[test]
fn table_input_order_is_canonical() {
    let a = table(vec![column("id", 1, "int")]);
    let mut b = a.clone();
    b.table = "before".into();
    assert_eq!(
        build(&[a.clone(), b.clone()]).unwrap().digest,
        build(&[b, a]).unwrap().digest
    );
}
