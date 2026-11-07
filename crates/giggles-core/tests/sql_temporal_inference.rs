mod common;
use giggles_core::{Admin, Limits, relational::*};
#[test]
fn trunc3_only_typed_middle_text_inference_and_conflicts() {
    let a = Admin::create(common::fresh("trunc3-infer"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    for query in [
        "SELECT date_trunc($1,$2::timestamptz,$3)",
        "SELECT date_trunc($1::text,$2::timestamptz,$3::text)",
    ] {
        let p = s.prepare(query).unwrap();
        assert_eq!(
            s.describe(&p).unwrap().parameters,
            vec![DataType::Text, DataType::TimestampTz, DataType::Text]
        );
        assert_eq!(
            s.describe(&p).unwrap().columns[0].data_type,
            DataType::TimestampTz
        );
        let t = Value::TimestampTz(TimestampTz::from_microseconds(45_296_000_000).unwrap());
        for args in [
            vec![
                Value::Text("day".into()),
                t.clone(),
                Value::Text("UTC".into()),
            ],
            vec![Value::Null, t.clone(), Value::Text("UTC".into())],
            vec![
                Value::Text("day".into()),
                Value::Null,
                Value::Text("UTC".into()),
            ],
            vec![Value::Text("day".into()), t, Value::Null],
        ] {
            let null = args.contains(&Value::Null);
            let r = s.execute_prepared(&p, &args, None).unwrap();
            assert_eq!(
                r.rows[0].values,
                vec![if null {
                    Value::Null
                } else {
                    Value::TimestampTz(TimestampTz::from_microseconds(0).unwrap())
                }]
            );
        }
    }
    for q in [
        "SELECT date_trunc($1,$2::integer,$3)",
        "SELECT date_trunc($1,$2::timestamptz,$3,$4)",
        "SELECT $1::integer,date_trunc($1,$2::timestamptz,$3)",
    ] {
        let p = s.prepare(q).unwrap();
        assert!(s.describe(&p).is_err(), "{q}");
    }
    let p = s
        .prepare("SELECT date_trunc($1,$2::timestamptz,$3),$1::integer")
        .unwrap();
    assert_eq!(
        s.describe(&p).unwrap().parameters,
        vec![DataType::Text, DataType::TimestampTz, DataType::Text]
    );
    let p = s
        .prepare("SELECT date_trunc($1,$2::timestamptz,$1)")
        .unwrap();
    assert_eq!(
        s.describe(&p).unwrap().parameters,
        vec![DataType::Text, DataType::TimestampTz]
    );
}
