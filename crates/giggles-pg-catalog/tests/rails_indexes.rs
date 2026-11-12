use giggles_pg_catalog::*;
use Value::{Array as A, Bool as B, Int as I, Null as N, Text as S};

const RAILS: &str = "SELECT distinct i.relname, d.indisunique, d.indkey, pg_get_indexdef(d.indexrelid),
       pg_catalog.obj_description(i.oid, 'pg_class') AS comment, d.indisvalid,
       ARRAY( SELECT pg_get_indexdef(d.indexrelid, k + 1, true)
              FROM generate_subscripts(d.indkey, 1) AS k ORDER BY k ) AS columns
FROM pg_class t
INNER JOIN pg_index d ON t.oid = d.indrelid
INNER JOIN pg_class i ON d.indexrelid = i.oid
LEFT JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE i.relkind IN ('i', 'I') AND d.indisprimary = 'f'
  AND t.relname = 'order_items' AND n.nspname = ANY (current_schemas(false))
ORDER BY i.relname";

fn run(sql: &str, snapshot: &CatalogSnapshot) -> ResultSet {
    let plan = plan(sql).unwrap().unwrap();
    let result = execute(&plan, snapshot, &[]).unwrap();
    assert_eq!(describe(&plan).unwrap(), result.fields);
    result
}

#[test]
fn exact_rails_index_inventory() {
    let mut snapshot = CatalogSnapshot {
        tables: vec![TableDef {
            id: 1, name: "order_items".into(), primary_key: vec![0, 1],
            columns: ["order_id", "sku", "quantity"].iter().enumerate().map(|(i, n)| ColumnDef {
                id: i as u64 + 1, name: (*n).into(),
                data_type: if i == 1 { ScalarType::Varchar } else { ScalarType::Integer },
                not_null: true,
            }).collect(),
        }],
        indexes: vec![IndexDef {
            id: 10, table: 1, name: "index_order_items_on_order_id".into(),
            columns: vec![1], unique: false,
        }, IndexDef {
            id: 11, table: 1, name: "several".into(),
            columns: vec![3, 1, 2], unique: true,
        }],
        ..CatalogSnapshot::default()
    };
    let result = run(RAILS, &snapshot);
    assert_eq!(result.fields.iter().map(|f| f.data_type).collect::<Vec<_>>(),
        [PgType::Name, PgType::Bool, PgType::Int2Vector, PgType::Text, PgType::Text, PgType::Bool, PgType::TextArray]);
    assert_eq!(result.rows, vec![
        vec![S("index_order_items_on_order_id".into()), B(false), A(vec![I(1)]),
            S("CREATE INDEX index_order_items_on_order_id ON public.order_items USING btree (order_id)".into()),
            N, B(true), A(vec![S("order_id".into())])],
        vec![S("several".into()), B(true), A(vec![I(3), I(1), I(2)]),
            S("CREATE UNIQUE INDEX several ON public.order_items USING btree (quantity, order_id, sku)".into()),
            N, B(true), A(vec![S("quantity".into()), S("order_id".into()), S("sku".into())])],
    ]);
    snapshot.indexes.clear();
    assert!(run(RAILS, &snapshot).rows.is_empty());
}

#[test]
fn subscript_dimensions_null_empty_reverse_alias_and_order() {
    let snapshot = CatalogSnapshot::default();
    let result = run("SELECT ARRAY(SELECT k FROM generate_subscripts(ARRAY[7,8,9],1) AS k ORDER BY k DESC),
        ARRAY(SELECT x FROM pg_catalog.generate_subscripts(ARRAY[4,2],1,true) AS s(x)),
        ARRAY(SELECT k FROM generate_subscripts('{}'::int[],1) AS k),
        ARRAY(SELECT k FROM generate_subscripts(NULL::int[],1) AS k),
        ARRAY(SELECT k FROM generate_subscripts(ARRAY[1],2) AS k),
        ARRAY(SELECT k FROM generate_subscripts(ARRAY[1],0) AS k),
        ARRAY(SELECT k FROM generate_subscripts(ARRAY[1],NULL) AS k),
        ARRAY(SELECT k FROM generate_subscripts(ARRAY[1],1,NULL) AS k)", &snapshot);
    assert_eq!(result.rows, vec![vec![A(vec![I(3), I(2), I(1)]), A(vec![I(2), I(1)]),
        A(vec![]), A(vec![]), A(vec![]), A(vec![]), A(vec![]), A(vec![])]]);
    let plan = plan("SELECT ARRAY(SELECT k FROM generate_subscripts($1::int[],1) AS k) FROM pg_namespace").unwrap().unwrap();
    assert_eq!(plan.parameter_types(), [PgType::Int4Array]);
    assert_eq!(execute(&plan, &snapshot, &[A(vec![I(1); 4097])]), Err(Error::Limit));
}

#[test]
fn empty_single_and_several_vector_subscripts() {
    let fields = vec![Field { name: "keys".into(), data_type: PgType::Int2Vector }];
    let rows = vec![vec![A(vec![])], vec![A(vec![I(3)])], vec![A(vec![I(3), I(1), I(2)])]];
    let mut snapshot = CatalogSnapshot::default();
    snapshot.user_relations.insert("vectors".into(), ResultSet { fields: fields.clone(), rows });
    let plan = plan_gateway_with_relations(
        "SELECT ARRAY(SELECT x FROM pg_catalog.generate_subscripts(v.keys,1,true) AS s(x)) FROM vectors v",
        &[("vectors".into(), fields)].into_iter().collect(),
    ).unwrap().unwrap();
    assert_eq!(execute(&plan, &snapshot, &[]).unwrap().rows,
        vec![vec![A(vec![])], vec![A(vec![I(0)])], vec![A(vec![I(2), I(1), I(0)])]]);
}

#[test]
fn rails_foreign_keys_see_enclosing_rows_through_derived_tables() {
    let table = |id, name: &str| TableDef {
        id, name: name.into(), primary_key: vec![0],
        columns: vec![ColumnDef { id: id * 10, name: "id".into(), data_type: ScalarType::Integer, not_null: true }],
    };
    let snapshot = CatalogSnapshot {
        tables: vec![table(1, "order_items"), table(2, "orders")],
        constraints: vec![ConstraintDef {
            id: 30, table: 1, name: "fk_orders".into(), kind: 'f', columns: vec![10],
            foreign_table: 2, foreign_columns: vec![20], update: 'a', delete: 'a', validated: true,
            ..ConstraintDef::default()
        }],
        ..CatalogSnapshot::default()
    };
    let sql = "SELECT t2.oid::regclass::text AS to_table, c.conname AS name,
      (SELECT array_agg(a.attname ORDER BY idx)
       FROM (SELECT idx, c.conkey[idx] AS conkey_elem
             FROM generate_subscripts(c.conkey, 1) AS idx) indexed_conkeys
       JOIN pg_attribute a ON a.attrelid = t1.oid AND a.attnum = indexed_conkeys.conkey_elem) AS conkey_names,
      (SELECT array_agg(a.attname ORDER BY idx)
       FROM (SELECT idx, c.confkey[idx] AS confkey_elem
             FROM generate_subscripts(c.confkey, 1) AS idx) indexed_confkeys
       JOIN pg_attribute a ON a.attrelid = t2.oid AND a.attnum = indexed_confkeys.confkey_elem) AS confkey_names
    FROM pg_constraint c JOIN pg_class t1 ON c.conrelid = t1.oid
    JOIN pg_class t2 ON c.confrelid = t2.oid JOIN pg_namespace n ON c.connamespace = n.oid
    WHERE c.contype = 'f' AND t1.relname = 'order_items' AND n.nspname = ANY (current_schemas(false))
    ORDER BY c.conname";
    assert_eq!(run(sql, &snapshot).rows,
        vec![vec![S("orders".into()), S("fk_orders".into()), A(vec![S("id".into())]), A(vec![S("id".into())])]]);
    for sql in ["SELECT n.oid FROM pg_namespace n JOIN (SELECT n.oid) d ON true",
        "SELECT n.oid FROM pg_namespace n, (SELECT n.oid) d"] {
        assert_eq!(plan(sql).unwrap_err(), Error::UnknownColumn);
    }
}
