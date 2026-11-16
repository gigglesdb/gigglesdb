use giggles_pg_catalog::{CatalogSnapshot, PgType, Value, execute, plan};
#[test]
fn timezone_columns_and_values() {
    let snapshot = CatalogSnapshot { timezone_now: Some(1794844800), ..Default::default() };
    let p = plan("SELECT name, abbrev, utc_offset, is_dst FROM pg_timezone_names WHERE name IN ('UTC','Etc/GMT+3') ORDER BY name").unwrap().unwrap();
    let result = execute(&p, &snapshot, &[]).unwrap();
    assert_eq!(result.fields.iter().map(|f| f.data_type).collect::<Vec<_>>(), vec![PgType::Text, PgType::Text, PgType::Interval, PgType::Bool]);
    assert_eq!(result.rows, vec![vec![Value::Text("Etc/GMT+3".into()), Value::Text("-03".into()), Value::Text("-03:00:00".into()), Value::Bool(false)], vec![Value::Text("UTC".into()), Value::Text("UTC".into()), Value::Text("00:00:00".into()), Value::Bool(false)]]);
}
#[test]
fn held_relation_lock_full_shape() {
    let snapshot = CatalogSnapshot { backend_pid: 42, table_locks: vec![(7, 1, 42, "ShareRowExclusiveLock".into(), false), (7, 1, 42, "AccessShareLock".into(), false)], ..Default::default() };
    let p = plan("SELECT * FROM pg_locks WHERE pid = pg_backend_pid() ORDER BY mode").unwrap().unwrap();
    let result = execute(&p, &snapshot, &[]).unwrap();
    assert_eq!(result.fields.iter().map(|f| f.data_type).collect::<Vec<_>>(), vec![PgType::Text, PgType::Oid, PgType::Oid, PgType::Int4, PgType::Int2, PgType::Text, PgType::Xid, PgType::Oid, PgType::Oid, PgType::Int2, PgType::Text, PgType::Int4, PgType::Text, PgType::Bool, PgType::Bool, PgType::TimestampTz]);
    assert_eq!(result.rows.len(), 2);
    let row = &result.rows[0];
    assert_eq!(row[0], Value::Text("relation".into()));
    assert_eq!(row[1], Value::Int(16384));
    assert!(row[3..10].iter().all(|v| *v == Value::Null));
    assert_eq!(row[10], Value::Text("42/7".into()));
    assert_eq!(row[11], Value::Int(42));
    assert_eq!(row[12], Value::Text("AccessShareLock".into()));
    assert_eq!(&row[13..], &[Value::Bool(true), Value::Bool(false), Value::Null]);
    assert!(execute(&p, &CatalogSnapshot::default(), &[]).unwrap().rows.is_empty());
}

fn abbreviation_golden(zone: &str, golden: &str, count: usize) {
    let snapshot = CatalogSnapshot {
        timezone_now: Some(1794844800),
        timezone: giggles_pg_types::temporal::SessionZone::setting(zone)
            .unwrap()
            .0,
        ..Default::default()
    };
    let p = plan("SELECT * FROM pg_catalog.pg_timezone_abbrevs ORDER BY abbrev")
        .unwrap()
        .unwrap();
    let result = execute(&p, &snapshot, &[]).unwrap();
    assert_eq!(
        result
            .fields
            .iter()
            .map(|f| (f.name.as_str(), f.data_type))
            .collect::<Vec<_>>(),
        vec![
            ("abbrev", PgType::Text),
            ("utc_offset", PgType::Interval),
            ("is_dst", PgType::Bool)
        ]
    );
    assert_eq!(result.rows.len(), count);
    let text = result
        .rows
        .iter()
        .map(|row| {
            let [Value::Text(name), Value::Text(offset), Value::Bool(dst)] = row.as_slice() else {
                panic!("unexpected row {row:?}")
            };
            format!("{name}|{offset}|{}\n", if *dst { "t" } else { "f" })
        })
        .collect::<String>();
    assert_eq!(text, golden.replace('\r', ""));
}
#[test]
fn timezone_abbreviations_utc_golden() {
    abbreviation_golden(
        "UTC",
        include_str!("fixtures/pg_timezone_abbrevs/utc.txt"),
        195,
    );
}
#[test]
fn timezone_abbreviations_new_york_golden() {
    abbreviation_golden(
        "America/New_York",
        include_str!("fixtures/pg_timezone_abbrevs/nyc.txt"),
        198,
    );
}
