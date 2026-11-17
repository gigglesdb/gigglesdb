use Value::{Int as I, Null as N, Text as S};
use giggles_pg_catalog::*;
fn rows(s: &CatalogSnapshot, sql: &str) -> Vec<Vec<Value>> {
    execute(&plan(sql).unwrap_or_else(|e|panic!("{sql}: {e:?}")).unwrap(), s, &[]).unwrap_or_else(|e|panic!("{sql}: {e:?}")).rows
}
fn snapshot() -> CatalogSnapshot {
    let mut s = CatalogSnapshot {
        tables: vec![TableDef {
            id: 10,
            name: "collated".into(),
            primary_key: vec![0],
            columns: vec![
                ColumnDef {
                    id: 11,
                    name: "id".into(),
                    data_type: ScalarType::Integer,
                    not_null: true,
                },
                ColumnDef {
                    id: 12,
                    name: "a".into(),
                    data_type: ScalarType::Text,
                    not_null: false,
                },
                ColumnDef {
                    id: 13,
                    name: "b".into(),
                    data_type: ScalarType::Varchar,
                    not_null: false,
                },
                ColumnDef {
                    id: 14,
                    name: "d".into(),
                    data_type: ScalarType::Text,
                    not_null: false,
                },
            ],
        }],
        indexes: vec![IndexDef {
            id: 20,
            table: 10,
            name: "c_idx".into(),
            columns: vec![12],
            unique: false,
        }],
        ..Default::default()
    };
    s.collation_oids.insert(12, vec![12362]);
    s.collation_oids.insert(13, vec![950]);
    s.collation_oids.insert(20, vec![950]);
    s.indexes.push(IndexDef{id:21,table:10,name:"mixed_idx".into(),columns:vec![14,12],unique:false});
    s.collation_oids.insert(21,vec![0,950]);
    s
}
#[test]
fn attribute_and_information_schema_collations() {
    let s = snapshot();
    assert_eq!(
        rows(
            &s,
            "SELECT a.attname,a.attcollation::regcollation FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid WHERE c.relname='collated' AND a.attnum>0 ORDER BY a.attnum"
        ),
        vec![
            vec![S("id".into()), S("-".into())],
            vec![S("a".into()), S("\"und-x-icu\"".into())],
            vec![S("b".into()), S("\"C\"".into())],
            vec![S("d".into()), S("\"default\"".into())]
        ]
    );
    assert_eq!(
        rows(
            &s,
            "SELECT column_name,collation_name FROM information_schema.columns WHERE table_name='collated' ORDER BY ordinal_position"
        ),
        vec![
            vec![S("id".into()), N],
            vec![S("a".into()), S("und-x-icu".into())],
            vec![S("b".into()), S("C".into())],
            vec![S("d".into()), N]
        ]
    );
    assert_eq!(
        rows(
            &s,
            "SELECT i.indcollation FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE c.relname='c_idx'"
        ),
        vec![vec![Value::Array(vec![I(950)])]]
    );
    assert_eq!(rows(&s,"SELECT a.attname,a.attcollation FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid WHERE c.relname='c_idx' AND a.attnum>0 ORDER BY a.attnum"),vec![vec![S("a".into()),I(950)]]);
    assert_eq!(rows(&s,"SELECT i.indcollation FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE c.relname='mixed_idx'"),vec![vec![Value::Array(vec![I(100),I(950)])]]);
}
#[test]
fn psql_and_dump_read_column_collation_only_when_not_default() {
    let s = snapshot();
    let sql = "SELECT a.attname,CASE WHEN a.attcollation <> t.typcollation THEN a.attcollation ELSE 0 END AS attcollation FROM pg_attribute a LEFT JOIN pg_type t ON a.atttypid=t.oid JOIN pg_class c ON c.oid=a.attrelid WHERE c.relname='collated' AND a.attnum>0 ORDER BY a.attnum";
    assert_eq!(
        rows(&s, sql),
        vec![
            vec![S("id".into()), I(0)],
            vec![S("a".into()), I(12362)],
            vec![S("b".into()), I(950)],
            vec![S("d".into()), I(0)]
        ]
    );
    let sql = "SELECT a.attname,(SELECT c.collname FROM pg_collation c,pg_type t WHERE c.oid=a.attcollation AND t.oid=a.atttypid AND a.attcollation<>t.typcollation) AS collation FROM pg_attribute a JOIN pg_class r ON r.oid=a.attrelid WHERE r.relname='collated' AND a.attnum>0 ORDER BY a.attnum";
    assert_eq!(rows(&s,sql),vec![vec![S("id".into()),N],vec![S("a".into()),S("und-x-icu".into())],vec![S("b".into()),S("C".into())],vec![S("d".into()),N]]);
}

#[test]
fn moved_user_collation_is_qualified_in_index_definition() {
    let mut s=snapshot();
    s.schemas.insert("moved".into(),(90001,10));
    s.collations.push(vec![I(100000),S("ci2".into()),I(90001),I(10),S("i".into()),Value::Bool(false),I(-1),N,N,S("und-u-ks-level2".into()),N,S("1.0".into()),N]);
    s.collation_oids.insert(20,vec![100000]);
    let result=rows(&s,"SELECT indexdef FROM pg_indexes WHERE indexname='c_idx'");
    let S(definition)=&result[0][0] else {panic!("index definition must be text")};
    assert!(definition.contains("COLLATE moved.ci2"),"{definition}");
    assert_eq!(rows(&s,"SELECT a.attcollation::regcollation::text FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid WHERE c.relname='c_idx' AND a.attnum=1"),vec![vec![S("moved.ci2".into())]]);
}
