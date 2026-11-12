use giggles_pg_catalog::*;

// Exact T47b FULL SchemaObjectAcls wire SQL, numbered by installed pgJDBC42.7.3
// Parser.parseJdbcSql (standardConformingStrings=true, withParameters=true,
// splitStatements=false), not raw question-mark replacement. Conditional v14
// schema2200 FULL recipe; this does not establish an installed GUI refresh mode.
// Recipe source SHA256: 1e1763d11f724063d1f136df258040d0354116a35abac6bfe0e4f2e95700dfc8.
// Processor raw508bytes CRLF SHA256:
// afb29bdaaaa8125176e3adce714619da934efb18bec742739dfc71a27fb2e17c.
// Processor LF-normalized manifest SHA256 (not raw-byte identity):
// f055dedf09323c828a5b161a4e6cbf08443f483fceebb0d3c943442520819f06.
// Wire511bytes SHA256:
// 6128879d7920de00dd0731e30a125bd37c18b3d6e751322ff90c96c3d7689acd.
// pgJDBC JAR SHA256:
// a2644cbfba1baa145ff7e8c8ef582a6eed7a7ec4ca792f7f054122bdec756268.
// Source: GD48 E222 T47b query-ledger.json, key full-SchemaObjectAcls.
const FULL_ACL: &str = include_str!("fixtures/datagrip_full_schema_object_acls.sql");

fn snapshot() -> CatalogSnapshot {
    CatalogSnapshot {
        tables: vec![TableDef {
            id: 7,
            name: "inventory".into(),
            columns: vec![
                ColumnDef {
                    id: 1,
                    name: "id".into(),
                    data_type: ScalarType::BigInt,
                    not_null: true,
                },
                ColumnDef {
                    id: 2,
                    name: "body".into(),
                    data_type: ScalarType::Text,
                    not_null: false,
                },
            ],
            primary_key: vec![0],
        }],
        ..CatalogSnapshot::default()
    }
}
fn run(sql: &str, snapshot: &CatalogSnapshot, parameters: &[Value]) -> ResultSet {
    let p = plan_gateway(sql).expect("catalog plan").expect("catalog");
    let described = describe(&p).expect("describe");
    let result = execute(&p, snapshot, parameters).expect("execute");
    assert_eq!(result.fields, described);
    result
}
fn i(v: i64) -> Value {
    Value::Int(v)
}
fn s(v: &str) -> Value {
    Value::Text(v.into())
}

#[test]
fn exact_full_acl_has_real_ordered_authorized_oids_and_null_acl() {
    // Exact fixture content and significant whitespace. The committed blob is LF, so this
    // must pass on both LF and CRLF (core.autocrlf=true) checkouts: compare CR-stripped text.
    let lf = FULL_ACL.replace('\r', "");
    assert_eq!(lf.len(), 497);
    assert_eq!(lf.matches('\n').count(), 14);
    let p = plan_gateway(FULL_ACL).unwrap().unwrap();
    assert_eq!(p.parameter_types(), &[PgType::Oid; 3]);
    let fields = vec![
        Field {
            name: "object_id".into(),
            data_type: PgType::Oid,
        },
        Field {
            name: "acl".into(),
            data_type: PgType::AclItemArray,
        },
    ];
    assert_eq!(describe(&p).unwrap(), fields);
    assert_eq!(
        fields.iter().map(|f| f.data_type.oid()).collect::<Vec<_>>(),
        vec![26, 1034]
    );
    let result = execute(&p, &snapshot(), &[i(2200), i(2200), i(2200)]).unwrap();
    assert_eq!(result.fields, fields);
    // Actual table65564, PK index65565, composite type65567. No user routines.
    assert_eq!(
        result.rows,
        vec![
            vec![i(65564), Value::Null],
            vec![i(65565), Value::Null],
            vec![i(65567), Value::Null]
        ]
    );
    assert!(
        execute(
            &p,
            &CatalogSnapshot::default(),
            &[i(2200), i(2200), i(2200)]
        )
        .unwrap()
        .rows
        .is_empty()
    );
    assert_eq!(
        execute(&p, &snapshot(), &[i(2200), i(2200)]),
        Err(Error::Parameters)
    );
    assert_eq!(run("select oid, relname, reltype, relacl, xmin from pg_catalog.pg_class where relnamespace=2200 order by oid", &snapshot(), &[]).rows, vec![
        vec![i(65564),s("inventory"),i(65567),Value::Null,i(0)],
        vec![i(65565),s("inventory_pkey"),i(0),Value::Null,i(0)],
    ]);
    assert_eq!(
        run(
            "select indexrelid, indrelid, indkey, indisprimary from pg_catalog.pg_index",
            &snapshot(),
            &[]
        )
        .rows,
        vec![vec![
            i(65565),
            i(65564),
            Value::Array(vec![i(1)]),
            Value::Bool(true)
        ]]
    );
    assert_eq!(run("select atttypid, attnotnull from pg_catalog.pg_attribute where attrelid=65564 AND attnum>0 order by attnum", &snapshot(), &[]).rows, vec![vec![i(20),Value::Bool(true)],vec![i(25),Value::Bool(false)]]);
}

#[test]
fn every_type_constructor_appends_null_acl_with_exact_descriptor() {
    let result = run("select * from pg_catalog.pg_type", &snapshot(), &[]);
    assert_eq!(
        result
            .fields
            .iter()
            .map(|f| f.name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "oid",
            "typname",
            "typnamespace",
            "typlen",
            "typtype",
            "typcategory",
            "typnotnull",
            "typbasetype",
            "typtypmod",
            "typrelid",
            "typelem",
            "typarray",
            "typinput",
            "typdelim",
            "typowner",
            "typndims",
            "typdefault",
            "typisdefined",
            "typispreferred",
            "typacl",
            "typreceive",
            "typcollation",
            "typstorage",
        ]
    );
    assert_eq!(result.fields[19].data_type, PgType::AclItemArray);
    assert_eq!(result.fields[19].data_type.oid(), 1034);
    assert!(
        result
            .rows
            .iter()
            .all(|r| r.len() == 23 && r[19] == Value::Null)
    );
    // Scalar, array, nominal, nominal array, virtual composite and application
    // composite paths must all be represented with NULL, never invented grants.
    for oid in [20, 1016, 24, 1008, 194, 34015, 65567] {
        let rows: Vec<_> = result.rows.iter().filter(|row| row[0] == i(oid)).collect();
        assert_eq!(rows.len(), 1, "type {oid}");
        assert_eq!(rows[0][19], Value::Null);
    }
    assert_eq!(run("select atttypid, attlen, attnum from pg_catalog.pg_attribute where attrelid=1247 and attname='typacl'", &snapshot(), &[]).rows,vec![vec![i(1034),i(-1),i(20)]]);
    assert_eq!(run("select atttypid, attlen, attnum from pg_catalog.pg_attribute where attrelid=1247 and attname='typreceive'", &snapshot(), &[]).rows,vec![vec![i(26),i(4),i(21)]]);
    assert_eq!(run("select oid, typname, typtype, typcategory, typelem, typarray, typacl from pg_catalog.pg_type where oid in (24,194,1008,65567) order by oid", &snapshot(), &[]).rows,vec![
        vec![i(24),s("regproc"),s("b"),s("N"),i(0),i(1008),Value::Null],
        vec![i(194),s("pg_node_tree"),s("b"),s("Z"),i(0),i(0),Value::Null],
        vec![i(1008),s("_regproc"),s("b"),s("A"),i(24),i(0),Value::Null],
        vec![i(65567),s("inventory"),s("c"),s("C"),i(0),i(0),Value::Null],
    ]);
}

#[test]
fn acl_schema_does_not_grant_unknown_catalogs_columns_or_privileges() {
    assert!(matches!(
        plan_gateway("select typacl from pg_catalog.pg_unknown"),
        Err(Error::Unsupported)
    ));
    assert!(matches!(
        plan_gateway("select invented_acl from pg_catalog.pg_type"),
        Err(Error::UnknownColumn)
    ));
    assert!(matches!(
        plan_gateway("update pg_catalog.pg_type set typacl=null"),
        Err(Error::Mutation)
    ));
    assert!(matches!(
        plan_gateway("select T.typacl from pg_catalog.pg_type T join ungranted U on T.oid=U.id"),
        Err(Error::MixedRelations)
    ));
}
