use giggles_pg_catalog::*;

fn snapshot() -> CatalogSnapshot {
    let mut snapshot = CatalogSnapshot::default();
    let modifiers = [655366, 262148, 393223, 133121, 196617, -1];
    snapshot.tables.push(TableDef {
        id: 1,
        name: "numeric_columns".into(),
        columns: modifiers
            .iter()
            .enumerate()
            .map(|(index, _)| ColumnDef {
                id: index as u64 + 1,
                name: format!("n{index}"),
                data_type: ScalarType::Numeric,
                not_null: false,
            })
            .collect(),
        primary_key: vec![],
    });
    snapshot.column_typmods = modifiers
        .into_iter()
        .enumerate()
        .map(|(index, modifier)| (index as u64 + 1, modifier))
        .collect();
    snapshot
}

fn query(sql: &str) -> ResultSet {
    let plan = plan(sql).expect(sql).expect("catalog query");
    execute(&plan, &snapshot(), &[]).expect(sql)
}

#[test]
fn information_schema_numeric_precision_and_scale_match_pg18() {
    use Value::{Int as I, Null as N, Text as T};
    // PG18.6 oracle: negative scales are exposed as unsigned packed fields here.
    let result = query(
        "SELECT column_name,data_type,numeric_precision,numeric_precision_radix,numeric_scale \
         FROM information_schema.columns WHERE table_name='numeric_columns' ORDER BY ordinal_position",
    );
    let expected = [
        (I(10), I(2)),
        (I(4), I(0)),
        (I(6), I(3)),
        (I(2), I(2045)),
        (I(3), I(5)),
        (N, N),
    ]
    .into_iter()
    .enumerate()
    .map(|(index, (precision, scale))| {
        vec![
            T(format!("n{index}")),
            T("numeric".into()),
            precision,
            I(10),
            scale,
        ]
    })
    .collect::<Vec<_>>();
    assert_eq!(result.rows, expected);
}

#[test]
fn numeric_attribute_typmod_and_format_type_match_pg18() {
    use Value::{Int as I, Text as T};
    let result = query(
        "SELECT attname,atttypmod,format_type(atttypid,atttypmod) FROM pg_attribute \
         WHERE attname IN ('n0','n1','n2','n3','n4','n5') ORDER BY attname",
    );
    let expected = [
        (655366, "numeric(10,2)"),
        (262148, "numeric(4,0)"),
        (393223, "numeric(6,3)"),
        (133121, "numeric(2,-3)"),
        (196617, "numeric(3,5)"),
        (-1, "numeric"),
    ]
    .into_iter()
    .enumerate()
    .map(|(index, (modifier, display))| {
        vec![T(format!("n{index}")), I(modifier), T(display.into())]
    })
    .collect::<Vec<_>>();
    assert_eq!(result.rows, expected);
}
