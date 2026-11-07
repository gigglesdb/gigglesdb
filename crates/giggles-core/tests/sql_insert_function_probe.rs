use giggles_core::{relational::*, *};
#[test]
fn insert_scalar_candidate_probe() {
    let path = std::env::temp_dir().join(format!(
        "dd68-insert-functions-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(std::path::Path::new(&path).parent().unwrap()).unwrap();
    let admin = Admin::create(path, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    s.execute(
        "CREATE TABLE ins (id BIGINT PRIMARY KEY, txt TEXT, n INTEGER)",
        &[],
        None,
    )
    .unwrap();
    let p = s
        .prepare("INSERT INTO ins(id,txt,n) VALUES($1,lower($2),abs($3))")
        .unwrap();
    assert_eq!(
        s.describe(&p).unwrap().parameters,
        vec![DataType::BigInt, DataType::Text, DataType::DoublePrecision]
    );
    assert_eq!(
        s.execute_prepared(
            &p,
            &[
                Value::Int(1),
                Value::Text("HELLO".into()),
                Value::Float8(Float8::from_bits((-2.5f64).to_bits()))
            ],
            None
        )
        .unwrap()
        .affected,
        1
    );
    let rows = s
        .execute("SELECT id,txt,n FROM ins LIMIT 1024", &[], None)
        .unwrap()
        .rows;
    assert_eq!(
        rows[0].values,
        vec![Value::Int(1), Value::Text("hello".into()), Value::Int(2)]
    );
}
