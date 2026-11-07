use Value::{Bool as B, Int as I, Null as N};
use giggles_pg_catalog::*;
fn s(v: &str) -> Value {
    Value::Text(v.into())
}
fn snapshot() -> CatalogSnapshot {
    CatalogSnapshot {
        tables: vec![TableDef {
            id: 10,
            name: "t".into(),
            primary_key: vec![0],
            columns: ["id", "email", "a", "b"]
                .iter()
                .enumerate()
                .map(|(i, n)| ColumnDef {
                    id: 100 + i as u64,
                    name: (*n).into(),
                    data_type: if i == 1 {
                        ScalarType::Text
                    } else {
                        ScalarType::BigInt
                    },
                    not_null: i == 0,
                })
                .collect(),
        }],
        indexes: vec![
            IndexDef {
                id: 12,
                table: 10,
                name: "t_ab_idx".into(),
                columns: vec![103, 102],
                unique: false,
            },
            IndexDef {
                id: 11,
                table: 10,
                name: "t_email_key".into(),
                columns: vec![101],
                unique: true,
            },
            IndexDef {
                id: 20,
                table: 99,
                name: "hidden".into(),
                columns: vec![200],
                unique: false,
            },
        ],
        ..CatalogSnapshot::default()
    }
}
fn run_on(sql: &str, sn: &CatalogSnapshot) -> ResultSet {
    execute(&plan(sql).unwrap().unwrap(), sn, &[]).unwrap()
}
fn run(sql: &str) -> ResultSet {
    run_on(sql, &snapshot())
}
#[test]
fn secondary_relations_keys_flags_and_primary_constraint() {
    assert_eq!(run("SELECT c.relname,c.relam,c.relnatts,i.indkey,i.indnatts,i.indnkeyatts,i.indisunique,i.indisprimary,i.indisvalid,i.indisexclusion,i.indimmediate,i.indisready,i.indislive,i.indisclustered,i.indisreplident FROM pg_class c JOIN pg_index i ON c.oid=i.indexrelid ORDER BY c.oid").rows, vec![
        vec![s("t_pkey"),I(403),I(1),Value::Array(vec![I(1)]),I(1),I(1),B(true),B(true),B(true),B(false),B(true),B(true),B(true),B(false),B(false)],
        vec![s("t_email_key"),I(403),I(1),Value::Array(vec![I(2)]),I(1),I(1),B(true),B(false),B(true),B(false),B(true),B(true),B(true),B(false),B(false)],
        vec![s("t_ab_idx"),I(403),I(2),Value::Array(vec![I(4),I(3)]),I(2),I(2),B(false),B(false),B(true),B(false),B(true),B(true),B(true),B(false),B(false)],
    ]);
    assert_eq!(
        run("SELECT relname,relhasindex FROM pg_class WHERE relnamespace=2200 ORDER BY oid").rows,
        vec![
            vec![s("t"), B(true)],
            vec![s("t_pkey"), B(false)],
            vec![s("t_email_key"), B(false)],
            vec![s("t_ab_idx"), B(false)]
        ]
    );
    assert_eq!(
        run("SELECT conname,contype,conkey,conindid FROM pg_constraint").rows,
        vec![vec![
            s("t_pkey"),
            s("p"),
            Value::Array(vec![I(1)]),
            I(65577)
        ]]
    );
    assert_eq!(run("SELECT c.relname,a.attnum,a.attname FROM pg_class c JOIN pg_attribute a ON a.attrelid=c.oid WHERE c.relkind='i' ORDER BY c.oid,a.attnum").rows,vec![vec![s("t_pkey"),I(1),s("id")],vec![s("t_email_key"),I(1),s("email")],vec![s("t_ab_idx"),I(1),s("b")],vec![s("t_ab_idx"),I(2),s("a")]]);
    assert_eq!(
        run("SELECT oid,amname,amhandler,amtype FROM pg_am").rows,
        vec![
            vec![I(2), s("heap"), I(3), s("t")],
            vec![I(403), s("btree"), I(330), s("i")]
        ]
    );
    assert!(
        run("SELECT relname FROM pg_class WHERE relname='hidden'")
            .rows
            .is_empty()
    );
}
#[test]
fn definitions_match_view_and_both_function_arities() {
    assert_eq!(run("SELECT schemaname,tablename,indexname,tablespace,indexdef FROM pg_indexes ORDER BY indexname").rows,vec![
        vec![s("public"),s("t"),s("t_ab_idx"),N,s("CREATE INDEX t_ab_idx ON public.t USING btree (b, a)")],
        vec![s("public"),s("t"),s("t_email_key"),N,s("CREATE UNIQUE INDEX t_email_key ON public.t USING btree (email)")],
        vec![s("public"),s("t"),s("t_pkey"),N,s("CREATE UNIQUE INDEX t_pkey ON public.t USING btree (id)")],
    ]);
    assert_eq!(run("SELECT c.relname,pg_get_indexdef(i.indexrelid),pg_get_indexdef(i.indexrelid,0,true) FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid ORDER BY c.relname").rows,vec![
        vec![s("t_ab_idx"),s("CREATE INDEX t_ab_idx ON public.t USING btree (b, a)"),s("CREATE INDEX t_ab_idx ON t USING btree (b, a)")],
        vec![s("t_email_key"),s("CREATE UNIQUE INDEX t_email_key ON public.t USING btree (email)"),s("CREATE UNIQUE INDEX t_email_key ON t USING btree (email)")],
        vec![s("t_pkey"),s("CREATE UNIQUE INDEX t_pkey ON public.t USING btree (id)"),s("CREATE UNIQUE INDEX t_pkey ON t USING btree (id)")],
    ]);
    assert_eq!(run("SELECT pg_get_indexdef(i.indexrelid),pg_get_indexdef(i.indexrelid,0,true),pg_get_indexdef(i.indexrelid,1,true),pg_get_indexdef(i.indexrelid,2,false) FROM pg_index i WHERE i.indexrelid=65584").rows,vec![vec![s("CREATE INDEX t_ab_idx ON public.t USING btree (b, a)"),s("CREATE INDEX t_ab_idx ON t USING btree (b, a)"),s("b"),s("a")]]);
    assert_eq!(run("SELECT pg_get_indexdef(123),pg_get_indexdef(123,1,true),pg_get_indexdef(65584,3,true) FROM pg_namespace LIMIT 1").rows,vec![vec![N,N,s("")]]);
    let mut sn = snapshot();
    sn.tables[0].name = "Order Items".into();
    sn.tables[0].columns[2].name = "select".into();
    sn.indexes[0].name = "a\"b".into();
    assert_eq!(
        run_on(
            "SELECT indexdef FROM pg_indexes WHERE indexname='a\"b'",
            &sn
        )
        .rows,
        vec![vec![s(
            "CREATE INDEX \"a\"\"b\" ON public.\"Order Items\" USING btree (b, \"select\")"
        )]]
    );
}
const PSQL: &str = "SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, i.indisvalid, pg_catalog.pg_get_indexdef(i.indexrelid, 0, true), pg_catalog.pg_get_constraintdef(con.oid, true), contype, condeferrable, condeferred, i.indisreplident, c2.reltablespace FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','u','x')) WHERE c.oid = '65576' AND c.oid = i.indrelid AND i.indexrelid = c2.oid ORDER BY i.indisprimary DESC, c2.relname;";
const SUPPORTED_INDEXES: &str = "SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, i.indisvalid, pg_catalog.pg_get_indexdef(i.indexrelid, 0, true), NULL AS pg_get_constraintdef, NULL AS contype, NULL AS condeferrable, NULL AS condeferred, i.indisreplident, c2.reltablespace FROM pg_catalog.pg_class c JOIN pg_catalog.pg_index i ON c.oid=i.indrelid JOIN pg_catalog.pg_class c2 ON c2.oid=i.indexrelid WHERE c.oid=65576 ORDER BY i.indisprimary DESC, c2.relname;";
const SUPPORTED_CONSTRAINTS: &str = "SELECT c2.relname, pg_catalog.pg_get_constraintdef(con.oid, true), con.contype, con.condeferrable, con.condeferred FROM pg_catalog.pg_class c JOIN pg_catalog.pg_index i ON c.oid=i.indrelid JOIN pg_catalog.pg_class c2 ON c2.oid=i.indexrelid JOIN pg_catalog.pg_constraint con ON con.conindid=i.indexrelid WHERE c.oid=65576 ORDER BY c2.relname;";

// Reconstruct the twelve captured columns in test code using supported joins.
fn indexes_via_supported_joins(sn: &CatalogSnapshot) -> ResultSet {
    let mut indexes = run_on(SUPPORTED_INDEXES, sn);
    let constraints = run_on(SUPPORTED_CONSTRAINTS, sn);
    assert_eq!(
        constraints.rows,
        vec![vec![
            s("t_pkey"),
            s("PRIMARY KEY (id)"),
            s("p"),
            B(false),
            B(false)
        ]]
    );
    for index in &mut indexes.rows {
        if let Some(constraint) = constraints.rows.iter().find(|row| row[0] == index[0]) {
            index[6..10].clone_from_slice(&constraint[1..5]);
        }
    }
    indexes
}

#[test]
fn supplied_psql_query_returns_exact_postgres_rows() {
    assert_eq!(
        execute(&plan(PSQL).unwrap().unwrap(), &snapshot(), &[])
            .unwrap()
            .rows,
        vec![
            vec![
                s("t_pkey"),
                B(true),
                B(true),
                B(false),
                B(true),
                s("CREATE UNIQUE INDEX t_pkey ON t USING btree (id)"),
                s("PRIMARY KEY (id)"),
                s("p"),
                B(false),
                B(false),
                B(false),
                I(0)
            ],
            vec![
                s("t_ab_idx"),
                B(false),
                B(false),
                B(false),
                B(true),
                s("CREATE INDEX t_ab_idx ON t USING btree (b, a)"),
                N,
                N,
                N,
                N,
                B(false),
                I(0)
            ],
            vec![
                s("t_email_key"),
                B(false),
                B(true),
                B(false),
                B(true),
                s("CREATE UNIQUE INDEX t_email_key ON t USING btree (email)"),
                N,
                N,
                N,
                N,
                B(false),
                I(0)
            ],
        ]
    );
}

#[test]
fn supported_inner_joins_return_all_indexes_and_constraintdef() {
    assert_eq!(
        indexes_via_supported_joins(&snapshot()).rows,
        vec![
            vec![
                s("t_pkey"),
                B(true),
                B(true),
                B(false),
                B(true),
                s("CREATE UNIQUE INDEX t_pkey ON t USING btree (id)"),
                s("PRIMARY KEY (id)"),
                s("p"),
                B(false),
                B(false),
                B(false),
                I(0)
            ],
            vec![
                s("t_ab_idx"),
                B(false),
                B(false),
                B(false),
                B(true),
                s("CREATE INDEX t_ab_idx ON t USING btree (b, a)"),
                N,
                N,
                N,
                N,
                B(false),
                I(0)
            ],
            vec![
                s("t_email_key"),
                B(false),
                B(true),
                B(false),
                B(true),
                s("CREATE UNIQUE INDEX t_email_key ON t USING btree (email)"),
                N,
                N,
                N,
                N,
                B(false),
                I(0)
            ],
        ]
    );
    assert_eq!(run("SELECT pg_get_constraintdef(65578),pg_get_constraintdef(65578,false),pg_get_constraintdef(123),pg_get_constraintdef(123,true),pg_get_constraintdef(NULL),pg_get_constraintdef(NULL,true) FROM pg_namespace LIMIT 1").rows, vec![vec![s("PRIMARY KEY (id)"), s("PRIMARY KEY (id)"), N, N, N, N]]);
    let mut sn = snapshot();
    sn.tables[0].primary_key = vec![3, 2, 0];
    sn.tables[0].columns[3].name = "a\"b".into();
    sn.tables[0].columns[2].name = "select".into();
    assert_eq!(
        run_on(
            "SELECT pg_get_constraintdef(oid),pg_get_constraintdef(oid,true) FROM pg_constraint",
            &sn
        )
        .rows,
        vec![vec![
            s("PRIMARY KEY (\"a\"\"b\", \"select\", id)"),
            s("PRIMARY KEY (\"a\"\"b\", \"select\", id)")
        ]]
    );
}

#[test]
fn representative_jdbc_get_index_info_each_key_column() {
    // Representative getIndexInfo shape; not an upstream captured query.
    let sql = "SELECT n.nspname,ct.relname,NOT i.indisunique AS non_unique,ci.relname,k,a.attname FROM pg_class ct JOIN pg_namespace n ON n.oid=ct.relnamespace JOIN pg_index i ON i.indrelid=ct.oid JOIN pg_class ci ON ci.oid=i.indexrelid CROSS JOIN unnest(i.indkey) WITH ORDINALITY u(attnum,k) JOIN pg_attribute a ON a.attrelid=ct.oid AND a.attnum=u.attnum WHERE ct.relname='t' ORDER BY ci.relname,k";
    assert_eq!(
        run(sql).rows,
        vec![
            vec![s("public"), s("t"), B(true), s("t_ab_idx"), I(1), s("b")],
            vec![s("public"), s("t"), B(true), s("t_ab_idx"), I(2), s("a")],
            vec![
                s("public"),
                s("t"),
                B(false),
                s("t_email_key"),
                I(1),
                s("email")
            ],
            vec![s("public"), s("t"), B(false), s("t_pkey"), I(1), s("id")]
        ]
    );
}
#[test]
fn shared_allocator_oid_slots_do_not_collide_and_order_is_stable() {
    let mut sn = snapshot();
    let mut t = sn.tables[0].clone();
    t.id = 13;
    t.name = "second".into();
    sn.tables.insert(0, t);
    let relations = run_on(
        "SELECT oid FROM pg_class WHERE relnamespace=2200 ORDER BY oid",
        &sn,
    )
    .rows;
    assert_eq!(
        relations,
        vec![
            vec![I(65576)],
            vec![I(65577)],
            vec![I(65580)],
            vec![I(65584)],
            vec![I(65588)],
            vec![I(65589)]
        ]
    );
    let mut all = relations.iter().map(|r| r[0].clone()).collect::<Vec<_>>();
    all.extend(
        run_on("SELECT oid FROM pg_constraint", &sn)
            .rows
            .into_iter()
            .map(|r| r[0].clone()),
    );
    all.extend(
        run_on("SELECT oid FROM pg_type WHERE typnamespace=2200", &sn)
            .rows
            .into_iter()
            .map(|r| r[0].clone()),
    );
    assert_eq!(
        all.iter().collect::<std::collections::BTreeSet<_>>().len(),
        all.len()
    );
    let before = run_on("SELECT indexrelid,indrelid FROM pg_index", &sn).rows;
    sn.tables.reverse();
    sn.indexes.reverse();
    assert_eq!(
        run_on("SELECT indexrelid,indrelid FROM pg_index", &sn).rows,
        before
    );
    sn.indexes[0].id = 10;
    // The hidden descriptor is ignored by visibility; force the visible descriptor collision.
    sn.indexes.iter_mut().find(|i| i.table == 10).unwrap().id = 10;
    assert!(matches!(
        execute(&plan("SELECT * FROM pg_index").unwrap().unwrap(), &sn, &[]),
        Err(Error::InvalidSnapshot)
    ));
}

#[test]
fn identifier_spelling_uses_postgresql_keyword_categories() {
    let mut sn = snapshot();
    let names = ["id", "abort", "index", "value", "select", "between", "join"];
    sn.tables[0].columns = names
        .iter()
        .enumerate()
        .map(|(i, n)| ColumnDef {
            id: i as u64 + 100,
            name: (*n).into(),
            data_type: ScalarType::Text,
            not_null: false,
        })
        .collect();
    sn.tables[0].primary_key.clear();
    sn.indexes = vec![IndexDef {
        id: 11,
        table: 10,
        name: "index".into(),
        columns: (100..107).collect(),
        unique: false,
    }];
    assert_eq!(
        run_on("SELECT indexdef FROM pg_indexes", &sn).rows,
        vec![vec![s(
            "CREATE INDEX index ON public.t USING btree (id, abort, index, value, \"select\", \"between\", \"join\")"
        )]]
    );
}

#[test]
fn secondary_name_may_match_table_name() {
    let mut sn = snapshot();
    sn.indexes[0].name = "t".into();
    assert_eq!(
        run_on(
            "SELECT c.relname,c.relkind,pg_get_indexdef(c.oid) FROM pg_class c WHERE c.oid=65584",
            &sn
        )
        .rows,
        vec![vec![
            s("t"),
            s("i"),
            s("CREATE INDEX t ON public.t USING btree (b, a)")
        ]]
    );
}

#[test]
fn all_pg18_index_flags_and_secondary_only_table() {
    assert_eq!(run("SELECT indisexclusion,indimmediate,indisready,indislive,indisclustered,indisreplident,indcheckxmin,indnullsnotdistinct FROM pg_index ORDER BY indexrelid").rows, vec![vec![B(false),B(true),B(true),B(true),B(false),B(false),B(false),B(false)];3]);
    let mut sn = snapshot();
    sn.tables[0].primary_key.clear();
    assert_eq!(
        run_on("SELECT relhasindex FROM pg_class WHERE oid=65576", &sn).rows,
        vec![vec![B(true)]]
    );
    assert!(run_on("SELECT oid FROM pg_constraint", &sn).rows.is_empty());
    sn.indexes.clear();
    assert_eq!(
        run_on("SELECT relhasindex FROM pg_class WHERE oid=65576", &sn).rows,
        vec![vec![B(false)]]
    );
}

// Golden rows captured from PostgreSQL 18.6, with database-local OIDs excluded.
#[test]
fn pg18_6_secondary_catalog_capture_matches() {
    let mut sn = snapshot();
    sn.tables[0].columns[2].data_type = ScalarType::Integer;
    sn.tables[0].columns[3].data_type = ScalarType::Integer;
    sn.indexes[0].columns = vec![102, 103];
    fn cell(value: &Value, ty: PgType) -> String {
        match value {
            N => "\\N".into(),
            B(value) => if *value { "t" } else { "f" }.into(),
            I(330) if ty == PgType::Regproc => "bthandler".into(),
            I(value) => value.to_string(),
            Value::Text(value) => value.clone(),
            Value::Array(values) => values
                .iter()
                .map(|v| cell(v, PgType::Int2))
                .collect::<Vec<_>>()
                .join(" "),
            _ => panic!("unmodeled capture value: {value:?}"),
        }
    }
    fn csv_field(value: &str) -> String {
        if value.contains([',', '"', '\n', '\r']) {
            format!("\"{}\"", value.replace('"', "\"\""))
        } else {
            value.into()
        }
    }
    let cases = [
        (
            "attribute",
            "SELECT c.relname,a.attname,a.attnum,a.attnotnull FROM pg_class c JOIN pg_attribute a ON a.attrelid=c.oid WHERE c.relnamespace=2200 AND c.relname IN ('t','t_pkey','t_email_key','t_ab_idx') ORDER BY c.relname,a.attnum;",
            "relname,attname,attnum,attnotnull\nt,id,1,t\nt,email,2,f\nt,a,3,f\nt,b,4,f\nt_ab_idx,a,1,f\nt_ab_idx,b,2,f\nt_email_key,email,1,f\nt_pkey,id,1,f\n",
        ),
        (
            "am",
            "SELECT oid,amname,amhandler,amtype FROM pg_catalog.pg_am WHERE oid=403;",
            "oid,amname,amhandler,amtype\n403,btree,bthandler,i\n",
        ),
        (
            "class",
            "SELECT relname,relkind,relam,relnatts,relhasindex FROM pg_catalog.pg_class WHERE relnamespace=2200 AND relname IN ('t','t_pkey','t_email_key','t_ab_idx') ORDER BY relname;",
            "relname,relkind,relam,relnatts,relhasindex\nt,r,2,4,t\nt_ab_idx,i,403,2,f\nt_email_key,i,403,1,f\nt_pkey,i,403,1,f\n",
        ),
        (
            "index",
            "SELECT c.relname AS indexname,i.indisunique,i.indnullsnotdistinct,i.indisprimary,i.indisexclusion,i.indimmediate,i.indisclustered,i.indisvalid,i.indcheckxmin,i.indisready,i.indislive,i.indisreplident,i.indkey,i.indnatts,i.indnkeyatts FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c ON c.oid=i.indexrelid WHERE i.indrelid=65576 ORDER BY c.relname;",
            "indexname,indisunique,indnullsnotdistinct,indisprimary,indisexclusion,indimmediate,indisclustered,indisvalid,indcheckxmin,indisready,indislive,indisreplident,indkey,indnatts,indnkeyatts\nt_ab_idx,f,f,f,f,t,f,t,f,t,t,f,3 4,2,2\nt_email_key,t,f,f,f,t,f,t,f,t,t,f,2,1,1\nt_pkey,t,f,t,f,t,f,t,f,t,t,f,1,1,1\n",
        ),
        (
            "indexes",
            "SELECT schemaname,tablename,indexname,tablespace,indexdef FROM pg_catalog.pg_indexes WHERE tablename='t' ORDER BY indexname;",
            "schemaname,tablename,indexname,tablespace,indexdef\npublic,t,t_ab_idx,\\N,\"CREATE INDEX t_ab_idx ON public.t USING btree (a, b)\"\npublic,t,t_email_key,\\N,CREATE UNIQUE INDEX t_email_key ON public.t USING btree (email)\npublic,t,t_pkey,\\N,CREATE UNIQUE INDEX t_pkey ON public.t USING btree (id)\n",
        ),
        (
            "indexdef",
            "SELECT c.relname AS indexname,pg_catalog.pg_get_indexdef(c.oid) AS def_one,pg_catalog.pg_get_indexdef(c.oid,0,true) AS def_pretty,pg_catalog.pg_get_indexdef(c.oid,1,true) AS column_one,pg_catalog.pg_get_indexdef(c.oid,2,true) AS column_two,pg_catalog.pg_get_indexdef(c.oid,1,false) AS column_one_plain,pg_catalog.pg_get_indexdef(c.oid,2,false) AS column_two_plain FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c ON c.oid=i.indexrelid WHERE i.indrelid=65576 ORDER BY c.relname;",
            "indexname,def_one,def_pretty,column_one,column_two,column_one_plain,column_two_plain\nt_ab_idx,\"CREATE INDEX t_ab_idx ON public.t USING btree (a, b)\",\"CREATE INDEX t_ab_idx ON t USING btree (a, b)\",a,b,a,b\nt_email_key,CREATE UNIQUE INDEX t_email_key ON public.t USING btree (email),CREATE UNIQUE INDEX t_email_key ON t USING btree (email),email,,email,\nt_pkey,CREATE UNIQUE INDEX t_pkey ON public.t USING btree (id),CREATE UNIQUE INDEX t_pkey ON t USING btree (id),id,,id,\n",
        ),
        (
            "constraintdef",
            "SELECT conname,contype,pg_catalog.pg_get_constraintdef(oid) AS def_one,pg_catalog.pg_get_constraintdef(oid,true) AS def_pretty FROM pg_catalog.pg_constraint WHERE conrelid=65576 AND contype='p' ORDER BY conname;",
            "conname,contype,def_one,def_pretty\nt_pkey,p,PRIMARY KEY (id),PRIMARY KEY (id)\n",
        ),
        (
            "psql-indexes",
            "SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, i.indisvalid, pg_catalog.pg_get_indexdef(i.indexrelid, 0, true), pg_catalog.pg_get_constraintdef(con.oid, true), contype, condeferrable, condeferred, i.indisreplident, c2.reltablespace FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','u','x')) WHERE c.oid = 65576 AND c.oid = i.indrelid AND i.indexrelid = c2.oid ORDER BY i.indisprimary DESC, c2.relname;",
            "relname,indisprimary,indisunique,indisclustered,indisvalid,pg_get_indexdef,pg_get_constraintdef,contype,condeferrable,condeferred,indisreplident,reltablespace\nt_pkey,t,t,f,t,CREATE UNIQUE INDEX t_pkey ON t USING btree (id),PRIMARY KEY (id),p,f,f,f,0\nt_ab_idx,f,f,f,t,\"CREATE INDEX t_ab_idx ON t USING btree (a, b)\",\\N,\\N,\\N,\\N,f,0\nt_email_key,f,t,f,t,CREATE UNIQUE INDEX t_email_key ON t USING btree (email),\\N,\\N,\\N,\\N,f,0\n",
        ),
    ];
    for (name, sql, golden) in cases {
        let result = if name == "psql-indexes" {
            indexes_via_supported_joins(&sn)
        } else {
            run_on(sql, &sn)
        };
        let mut csv = result
            .fields
            .iter()
            .map(|f| csv_field(&f.name))
            .collect::<Vec<_>>()
            .join(",");
        csv.push('\n');
        for row in result.rows {
            csv.push_str(
                &row.iter()
                    .zip(&result.fields)
                    .map(|(v, f)| csv_field(&cell(v, f.data_type)))
                    .collect::<Vec<_>>()
                    .join(","),
            );
            csv.push('\n');
        }
        let capture_label = if name == "psql-indexes" {
            "psql-indexes_via_supported_joins"
        } else {
            name
        };
        assert_eq!(csv, golden, "PostgreSQL 18.6 capture: {capture_label}");
        if let Some(directory) = std::env::var_os("K2C_CAPTURE_DIR") {
            if name == "psql-indexes" {
                std::fs::write(std::path::Path::new(&directory).join("giggles-psql-indexes_via_supported_joins.sql"), format!("-- Test-only merge by index name reconstructs the twelve captured columns. This reconstruction preserves the original K2c capture path; the exact psql LEFT JOIN query is covered separately.\n{SUPPORTED_INDEXES}\n{SUPPORTED_CONSTRAINTS}\n")).unwrap();
            }
            std::fs::write(
                std::path::Path::new(&directory).join(format!("giggles-{name}.csv")),
                csv,
            )
            .unwrap();
        }
    }
}

#[test]
fn index_view_preserves_g1a_type_receive_metadata() {
    assert_eq!(
        run("SELECT typname,typreceive FROM pg_type WHERE typname IN ('pg_indexes','_int4') ORDER BY typname").rows,
        vec![vec![s("_int4"), I(2400)], vec![s("pg_indexes"), I(2403)]]
    );
    assert_eq!(
        run("SELECT attnum FROM pg_attribute WHERE attrelid=1247 AND attname='typreceive'").rows,
        vec![vec![I(22)]]
    );
    assert_eq!(
        run("SELECT relname,oid FROM pg_class WHERE relname IN ('pg_enum','pg_range') ORDER BY relname").rows,
        vec![vec![s("pg_enum"), I(3501)], vec![s("pg_range"), I(3541)]]
    );
}
