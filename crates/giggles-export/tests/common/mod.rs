#![allow(dead_code)]
use giggles_core::relational::Value;
use giggles_core::{Admin, Limits};
use giggles_export::*;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
static ID: AtomicU64 = AtomicU64::new(0);
pub fn temp(label: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "giggles-export-{label}-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        ID.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&p).unwrap();
    eprintln!("retained fixture: {}", p.display());
    p
}
pub fn db(label: &str) -> (PathBuf, Admin) {
    let p = temp(label);
    let a = Admin::create(p.join("db"), Limits::default()).unwrap();
    (p, a)
}
pub fn sample() -> Archive {
    Archive {
        tables: vec![Table {
            name: "items".into(),
            columns: vec![
                Column {
                    name: "id".into(),
                    data_type: giggles_core::relational::DataType::BigInt,
                    not_null: true,
                },
                Column {
                    name: "body".into(),
                    data_type: giggles_core::relational::DataType::Text,
                    not_null: false,
                },
            ],
            primary_key: vec![0],
        }],
        rows: vec![vec![
            vec![Cell::Int("-1".into()), Cell::Null],
            vec![Cell::Int("2".into()), Cell::Text("雪\0😀".into())],
        ]],
    }
}
pub fn populated(label: &str) -> (PathBuf, Admin) {
    let (p, a) = db(label);
    let mut s = a.sql_session();
    s.execute(
        "CREATE TABLE items (id BIGINT PRIMARY KEY, body TEXT)",
        &[],
        None,
    )
    .unwrap();
    s.execute(
        "INSERT INTO items VALUES ($1,$2)",
        &[Value::Int(-1), Value::Null],
        None,
    )
    .unwrap();
    s.execute(
        "INSERT INTO items VALUES ($1,$2)",
        &[Value::Int(2), Value::Text("雪\0😀".into())],
        None,
    )
    .unwrap();
    (p, a)
}
