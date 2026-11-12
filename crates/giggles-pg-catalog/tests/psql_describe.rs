use giggles_pg_catalog::*;

// Captured by executing every verbatim fixture block on PostgreSQL 18.6,
// image e3d24f6906d0, initdb --locale=C.UTF-8 --encoding=UTF8.
// These are raw query results, including the internal queries hidden by psql -E.
type OracleRow = &'static [Option<&'static str>];
type OracleResult = (&'static [&'static str], &'static [OracleRow]);
const ORACLE: &[OracleResult] = &[
    (
        &["Schema", "Name", "Type", "Owner"],
        &[&[
            Some("public"),
            Some("r0g_t"),
            Some("table"),
            Some("postgres"),
        ]],
    ),
    (
        &["Schema", "Name", "Type", "Owner"],
        &[&[
            Some("public"),
            Some("r0g_t"),
            Some("table"),
            Some("postgres"),
        ]],
    ),
    (
        &["Schema", "Name", "Type", "Owner"],
        &[&[
            Some("public"),
            Some("r0g_t"),
            Some("table"),
            Some("postgres"),
        ]],
    ),
    (
        &["Schema", "Name", "Type", "Owner"],
        &[&[
            Some("public"),
            Some("r0g_t"),
            Some("table"),
            Some("postgres"),
        ]],
    ),
    (
        &["oid", "nspname", "relname"],
        &[&[Some("16384"), Some("public"), Some("r0g_t")]],
    ),
    (
        &[
            "relchecks",
            "relkind",
            "relhasindex",
            "relhasrules",
            "relhastriggers",
            "relrowsecurity",
            "relforcerowsecurity",
            "relhasoids",
            "relispartition",
            "?column?",
            "reltablespace",
            "reloftype",
            "relpersistence",
            "relreplident",
            "amname",
        ],
        &[&[
            Some("0"),
            Some("r"),
            Some("t"),
            Some("f"),
            Some("f"),
            Some("f"),
            Some("f"),
            Some("f"),
            Some("f"),
            Some(""),
            Some("0"),
            Some(""),
            Some("p"),
            Some("d"),
            Some("heap"),
        ]],
    ),
    (
        &[
            "attname",
            "format_type",
            "pg_get_expr",
            "attnotnull",
            "attcollation",
            "attidentity",
            "attgenerated",
        ],
        &[
            &[
                Some("id"),
                Some("integer"),
                None,
                Some("t"),
                None,
                Some(""),
                Some(""),
            ],
            &[
                Some("name"),
                Some("text"),
                None,
                Some("t"),
                None,
                Some(""),
                Some(""),
            ],
        ],
    ),
    (
        &[
            "relname",
            "indisprimary",
            "indisunique",
            "indisclustered",
            "indisvalid",
            "pg_get_indexdef",
            "pg_get_constraintdef",
            "contype",
            "condeferrable",
            "condeferred",
            "indisreplident",
            "reltablespace",
            "conperiod",
        ],
        &[
            &[
                Some("r0g_t_pkey"),
                Some("t"),
                Some("t"),
                Some("f"),
                Some("t"),
                Some("CREATE UNIQUE INDEX r0g_t_pkey ON r0g_t USING btree (id)"),
                Some("PRIMARY KEY (id)"),
                Some("p"),
                Some("f"),
                Some("f"),
                Some("f"),
                Some("0"),
                Some("f"),
            ],
            &[
                Some("r0g_t_name_idx"),
                Some("f"),
                Some("f"),
                Some("f"),
                Some("t"),
                Some("CREATE INDEX r0g_t_name_idx ON r0g_t USING btree (name)"),
                None,
                None,
                None,
                None,
                Some("f"),
                Some("0"),
                None,
            ],
        ],
    ),
    (&["sametable", "conname", "condef", "ontable"], &[]),
    (&["conname", "ontable", "condef"], &[]),
    (
        &[
            "polname",
            "polpermissive",
            "array_to_string",
            "pg_get_expr",
            "pg_get_expr",
            "cmd",
        ],
        &[],
    ),
    (
        &[
            "oid",
            "stxrelid",
            "nsp",
            "stxname",
            "columns",
            "ndist_enabled",
            "deps_enabled",
            "mcv_enabled",
            "stxstattarget",
        ],
        &[],
    ),
    (&["pubname", "?column?", "?column?"], &[]),
    (&["oid"], &[]),
    (&["oid", "relkind", "inhdetachpending", "pg_get_expr"], &[]),
    (
        &["oid", "nspname", "relname"],
        &[&[Some("16384"), Some("public"), Some("r0g_t")]],
    ),
    (
        &[
            "relchecks",
            "relkind",
            "relhasindex",
            "relhasrules",
            "relhastriggers",
            "relrowsecurity",
            "relforcerowsecurity",
            "relhasoids",
            "relispartition",
            "?column?",
            "reltablespace",
            "reloftype",
            "relpersistence",
            "relreplident",
            "amname",
        ],
        &[&[
            Some("0"),
            Some("r"),
            Some("t"),
            Some("f"),
            Some("f"),
            Some("f"),
            Some("f"),
            Some("f"),
            Some("f"),
            Some(""),
            Some("0"),
            Some(""),
            Some("p"),
            Some("d"),
            Some("heap"),
        ]],
    ),
    (
        &[
            "attname",
            "format_type",
            "pg_get_expr",
            "attnotnull",
            "attcollation",
            "attidentity",
            "attgenerated",
        ],
        &[
            &[
                Some("id"),
                Some("integer"),
                None,
                Some("t"),
                None,
                Some(""),
                Some(""),
            ],
            &[
                Some("name"),
                Some("text"),
                None,
                Some("t"),
                None,
                Some(""),
                Some(""),
            ],
        ],
    ),
    (
        &[
            "relname",
            "indisprimary",
            "indisunique",
            "indisclustered",
            "indisvalid",
            "pg_get_indexdef",
            "pg_get_constraintdef",
            "contype",
            "condeferrable",
            "condeferred",
            "indisreplident",
            "reltablespace",
            "conperiod",
        ],
        &[
            &[
                Some("r0g_t_pkey"),
                Some("t"),
                Some("t"),
                Some("f"),
                Some("t"),
                Some("CREATE UNIQUE INDEX r0g_t_pkey ON r0g_t USING btree (id)"),
                Some("PRIMARY KEY (id)"),
                Some("p"),
                Some("f"),
                Some("f"),
                Some("f"),
                Some("0"),
                Some("f"),
            ],
            &[
                Some("r0g_t_name_idx"),
                Some("f"),
                Some("f"),
                Some("f"),
                Some("t"),
                Some("CREATE INDEX r0g_t_name_idx ON r0g_t USING btree (name)"),
                None,
                None,
                None,
                None,
                Some("f"),
                Some("0"),
                None,
            ],
        ],
    ),
    (&["sametable", "conname", "condef", "ontable"], &[]),
    (&["conname", "ontable", "condef"], &[]),
    (
        &[
            "polname",
            "polpermissive",
            "array_to_string",
            "pg_get_expr",
            "pg_get_expr",
            "cmd",
        ],
        &[],
    ),
    (
        &[
            "oid",
            "stxrelid",
            "nsp",
            "stxname",
            "columns",
            "ndist_enabled",
            "deps_enabled",
            "mcv_enabled",
            "stxstattarget",
        ],
        &[],
    ),
    (&["pubname", "?column?", "?column?"], &[]),
    (&["oid"], &[]),
    (&["oid", "relkind", "inhdetachpending", "pg_get_expr"], &[]),
    (
        &["Schema", "Name", "Type", "Owner", "Table"],
        &[
            &[
                Some("public"),
                Some("r0g_t_name_idx"),
                Some("index"),
                Some("postgres"),
                Some("r0g_t"),
            ],
            &[
                Some("public"),
                Some("r0g_t_pkey"),
                Some("index"),
                Some("postgres"),
                Some("r0g_t"),
            ],
        ],
    ),
    (
        &["Schema", "Name", "Type", "Owner", "Table"],
        &[
            &[
                Some("public"),
                Some("r0g_t_name_idx"),
                Some("index"),
                Some("postgres"),
                Some("r0g_t"),
            ],
            &[
                Some("public"),
                Some("r0g_t_pkey"),
                Some("index"),
                Some("postgres"),
                Some("r0g_t"),
            ],
        ],
    ),
    (
        &["Name", "Owner"],
        &[&[Some("public"), Some("pg_database_owner")]],
    ),
    (
        &["Name", "Owner"],
        &[&[Some("public"), Some("pg_database_owner")]],
    ),
    (
        &[
            "rolname",
            "rolsuper",
            "rolinherit",
            "rolcreaterole",
            "rolcreatedb",
            "rolcanlogin",
            "rolconnlimit",
            "rolvaliduntil",
            "rolreplication",
            "rolbypassrls",
        ],
        &[&[
            Some("postgres"),
            Some("t"),
            Some("t"),
            Some("t"),
            Some("t"),
            Some("t"),
            Some("-1"),
            None,
            Some("t"),
            Some("t"),
        ]],
    ),
    (
        &[
            "rolname",
            "rolsuper",
            "rolinherit",
            "rolcreaterole",
            "rolcreatedb",
            "rolcanlogin",
            "rolconnlimit",
            "rolvaliduntil",
            "rolreplication",
            "rolbypassrls",
        ],
        &[&[
            Some("postgres"),
            Some("t"),
            Some("t"),
            Some("t"),
            Some("t"),
            Some("t"),
            Some("-1"),
            None,
            Some("t"),
            Some("t"),
        ]],
    ),
    (
        &[
            "Name",
            "Owner",
            "Encoding",
            "Locale Provider",
            "Collate",
            "Ctype",
            "Locale",
            "ICU Rules",
            "Access privileges",
        ],
        &[
            &[
                Some("postgres"),
                Some("postgres"),
                Some("UTF8"),
                Some("libc"),
                Some("C.UTF-8"),
                Some("C.UTF-8"),
                None,
                None,
                None,
            ],
            &[
                Some("template0"),
                Some("postgres"),
                Some("UTF8"),
                Some("libc"),
                Some("C.UTF-8"),
                Some("C.UTF-8"),
                None,
                None,
                Some("=c/postgres\npostgres=CTc/postgres"),
            ],
            &[
                Some("template1"),
                Some("postgres"),
                Some("UTF8"),
                Some("libc"),
                Some("C.UTF-8"),
                Some("C.UTF-8"),
                None,
                None,
                Some("=c/postgres\npostgres=CTc/postgres"),
            ],
        ],
    ),
    (
        &[
            "Name",
            "Owner",
            "Encoding",
            "Locale Provider",
            "Collate",
            "Ctype",
            "Locale",
            "ICU Rules",
            "Access privileges",
        ],
        &[
            &[
                Some("postgres"),
                Some("postgres"),
                Some("UTF8"),
                Some("libc"),
                Some("C.UTF-8"),
                Some("C.UTF-8"),
                None,
                None,
                None,
            ],
            &[
                Some("template0"),
                Some("postgres"),
                Some("UTF8"),
                Some("libc"),
                Some("C.UTF-8"),
                Some("C.UTF-8"),
                None,
                None,
                Some("=c/postgres\npostgres=CTc/postgres"),
            ],
            &[
                Some("template1"),
                Some("postgres"),
                Some("UTF8"),
                Some("libc"),
                Some("C.UTF-8"),
                Some("C.UTF-8"),
                None,
                None,
                Some("=c/postgres\npostgres=CTc/postgres"),
            ],
        ],
    ),
];

fn snapshot() -> CatalogSnapshot {
    CatalogSnapshot {
        user: "postgres".into(),
        database: "postgres".into(),
        roles: vec![RoleDef {
            name: "postgres".into(),
            ..CatalogSnapshot::default().roles.remove(0)
        }],
        tables: vec![TableDef {
            id: 7,
            name: "r0g_t".into(),
            columns: vec![
                ColumnDef {
                    id: 11,
                    name: "id".into(),
                    data_type: ScalarType::Integer,
                    not_null: true,
                },
                ColumnDef {
                    id: 15,
                    name: "name".into(),
                    data_type: ScalarType::Text,
                    not_null: true,
                },
            ],
            primary_key: vec![0],
        }],
        indexes: vec![IndexDef {
            id: 19,
            table: 7,
            name: "r0g_t_name_idx".into(),
            columns: vec![15],
            unique: false,
        }],
        ..CatalogSnapshot::default()
    }
}
fn text_value(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Text(s) => Some(s.clone()),
        Value::Int(i) => Some(i.to_string()),
        Value::Bool(b) => Some(if *b { "t" } else { "f" }.into()),
        other => panic!("unexpected oracle value {other:?}"),
    }
}
#[test]
fn every_psql18_describe_query_matches_raw_postgres_results() {
    let fixture = include_str!("fixtures/psql18_describe_queries.sql");
    let blocks: Vec<_> = fixture
        .split("-- query ")
        .skip(1)
        .map(|b| {
            let (tag, sql) = b.split_once('\n').unwrap();
            (tag.split_once(": ").unwrap().1.trim(), sql)
        })
        .collect();
    assert_eq!(blocks.len(), 34);
    let snapshot = snapshot();
    let lookup = plan(blocks[4].1).unwrap().unwrap();
    let lookup = execute(&lookup, &snapshot, &[]).unwrap();
    let Value::Int(oid) = lookup.rows[0][0] else {
        panic!("lookup oid")
    };
    let mut failures = Vec::new();
    for (index, ((tag, sql), (columns, expected))) in blocks.iter().zip(ORACLE).enumerate() {
        let sql = sql.replace("'16384'", &format!("'{oid}'"));
        let result = plan(&sql).and_then(|p| execute(&p.expect("catalog query"), &snapshot, &[]));
        let checked = (|| -> Result<(), String> {
            let result = result.map_err(|e| format!("{e:?}"))?;
            let names: Vec<_> = result.fields.iter().map(|f| f.name.as_str()).collect();
            if names != *columns {
                return Err(format!("columns {names:?}, expected {columns:?}"));
            }
            let keep: Vec<_> = (0..columns.len()).collect();
            let actual: Vec<Vec<_>> = result
                .rows
                .iter()
                .map(|row| keep.iter().map(|i| text_value(&row[*i])).collect())
                .collect();
            let expected: Vec<Vec<_>> = expected
                .iter()
                .filter(|row| !tag.contains("\\l") || row[0] == Some("postgres"))
                .map(|row| {
                    keep.iter()
                        .map(|i| {
                            row[*i].map(|v| {
                                if *i == 0 && columns[0] == "oid" && v == "16384" {
                                    oid.to_string()
                                } else {
                                    v.to_owned()
                                }
                            })
                        })
                        .collect()
                })
                .collect();
            if tag.contains("\\du") {
                let row = &result.rows[0];
                let mut attributes = Vec::new();
                for (column, label) in [
                    (1, "Superuser"),
                    (3, "Create role"),
                    (4, "Create DB"),
                    (8, "Replication"),
                    (9, "Bypass RLS"),
                ] {
                    if row[column] == Value::Bool(true) {
                        attributes.push(label);
                    }
                }
                // Single rendered psql 18 row captured in g1b-psql-d/pg.out.
                if text_value(&row[0]).as_deref() != Some("postgres")
                    || attributes.join(", ")
                        != "Superuser, Create role, Create DB, Replication, Bypass RLS"
                {
                    return Err(format!(
                        "rendered role {:?}: {}",
                        row[0],
                        attributes.join(", ")
                    ));
                }
            }
            if actual != expected {
                return Err(format!("rows {actual:?}, expected {expected:?}"));
            }
            Ok(())
        })();
        match checked {
            Ok(()) => eprintln!("query {:02} {tag}: PASS", index + 1),
            Err(e) => {
                eprintln!("query {:02} {tag}: FAIL {e}", index + 1);
                failures.push(format!("query {:02} {tag}: {e}", index + 1));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn postgres18_collation_and_replica_identity_catalog_values() {
    let snapshot = snapshot();
    let run = |sql: &str| {
        execute(&plan(sql).unwrap().unwrap(), &snapshot, &[])
            .unwrap()
            .rows
    };
    assert_eq!(
        run("SELECT oid,typcollation FROM pg_type WHERE oid IN (19,25,1003,1009) ORDER BY oid"),
        vec![
            vec![Value::Int(19), Value::Int(950)],
            vec![Value::Int(25), Value::Int(100)],
            vec![Value::Int(1003), Value::Int(950)],
            vec![Value::Int(1009), Value::Int(100)],
        ]
    );
    assert_eq!(
        run(
            "SELECT a.attname,a.attcollation FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid WHERE c.relname='r0g_t' AND a.attnum>0 ORDER BY a.attnum"
        ),
        vec![
            vec![Value::Text("id".into()), Value::Int(0)],
            vec![Value::Text("name".into()), Value::Int(100)],
        ]
    );
    assert_eq!(
        run("SELECT relname,relreplident FROM pg_class WHERE relnamespace=2200 ORDER BY relname"),
        vec![
            vec![Value::Text("r0g_t".into()), Value::Text("d".into())],
            vec![
                Value::Text("r0g_t_name_idx".into()),
                Value::Text("n".into())
            ],
            vec![Value::Text("r0g_t_pkey".into()), Value::Text("n".into())],
        ]
    );
}
