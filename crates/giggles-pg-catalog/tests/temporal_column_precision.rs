use giggles_pg_catalog::*;

#[test]
fn temporal_typmods_format_type_and_datetime_precision_match_pg186() {
    let mut snapshot = CatalogSnapshot::default();
    let types = [
        ScalarType::Timestamp,
        ScalarType::TimestampTz,
        ScalarType::Time,
        ScalarType::TimeTz,
    ];
    let mut columns = vec![];
    let mut formats = vec![];
    let mut precisions = vec![];
    for (index, ty) in types.into_iter().enumerate() {
        for modifier in -1..=6 {
            let id = columns.len() as u64 + 1;
            let name = format!("v{id:02}");
            columns.push(ColumnDef {
                id,
                name: name.clone(),
                data_type: ty,
                not_null: false,
            });
            snapshot.column_typmods.insert(id, modifier);
            let base = if index < 2 { "timestamp" } else { "time" };
            let zone = if index % 2 == 0 { "without" } else { "with" };
            let p = if modifier < 0 {
                String::new()
            } else {
                format!("({modifier})")
            };
            formats.push(vec![
                Value::Text(name.clone()),
                Value::Int(i64::from(modifier)),
                Value::Text(format!("{base}{p} {zone} time zone")),
            ]);
            precisions.push(vec![
                Value::Text(name),
                Value::Int(i64::from(if modifier < 0 { 6 } else { modifier })),
            ]);
        }
    }
    snapshot.tables.push(TableDef {
        id: 1,
        name: "temporal_columns".into(),
        columns,
        primary_key: vec![],
    });
    for (sql, expected) in [
        (
            "SELECT a.attname,a.atttypmod,format_type(a.atttypid,a.atttypmod) FROM pg_attribute a JOIN pg_class t ON a.attrelid=t.oid WHERE t.relname='temporal_columns' AND a.attnum>0 ORDER BY a.attnum",
            formats,
        ),
        (
            "SELECT column_name,datetime_precision FROM information_schema.columns WHERE table_name='temporal_columns' ORDER BY ordinal_position",
            precisions,
        ),
    ] {
        assert_eq!(
            execute(&plan(sql).unwrap().unwrap(), &snapshot, &[])
                .unwrap()
                .rows,
            expected
        );
    }
}
