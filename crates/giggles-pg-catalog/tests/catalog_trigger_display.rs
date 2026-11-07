use giggles_pg_catalog::*;

fn run(sql: &str, parameters: &[Value]) -> Result<ResultSet, Error> {
    execute(
        &plan(sql)?.expect("catalog"),
        &CatalogSnapshot::default(),
        parameters,
    )
}
const SOURCE: &str = " FROM pg_namespace n WHERE n.oid=11";

#[test]
fn escape_actual_bytes_and_case() {
    // PG14 encode.c: ASCII 1..127 unchanged except backslash; zero/highbit octal.
    let bytes = vec![0, 1, 9, 10, 13, 31, 32, 65, 92, 92, 127, 128, 255];
    let expected = "\\000\x01\t\n\r\x1f A\\\\\\\\\x7f\\200\\377";
    for format in ["escape", "ESCAPE", "eScApE"] {
        let sql = format!("SELECT pg_catalog.encode($1,$2){SOURCE}");
        let p = plan(&sql).unwrap().unwrap();
        assert_eq!(p.parameter_types(), &[PgType::Bytea, PgType::Text]);
        assert_eq!(describe(&p).unwrap()[0].data_type, PgType::Text);
        assert_eq!(
            execute(
                &p,
                &CatalogSnapshot::default(),
                &[Value::Bytes(bytes.clone()), Value::Text(format.into())]
            )
            .unwrap()
            .rows,
            vec![vec![Value::Text(expected.into())]]
        );
    }
    let mut expected = String::from("\\000");
    for b in 1u8..=127 {
        expected.push(char::from(b));
        if b == b'\\' {
            expected.push('\\');
        }
    }
    for b in 128u16..=255 {
        expected.push_str(&format!("\\{b:03o}"));
    }
    assert_eq!(
        run(
            &format!("SELECT encode($1,'escape'){SOURCE}"),
            &[Value::Bytes((0..=255).collect())]
        )
        .unwrap()
        .rows,
        vec![vec![Value::Text(expected)]]
    );
    assert_eq!(
        run(
            &format!("SELECT encode($1,'escape'){SOURCE}"),
            &[Value::Bytes(vec![])]
        )
        .unwrap()
        .rows,
        vec![vec![Value::Text(String::new())]]
    );
}

#[test]
fn strict_null_signatures_and_refusals() {
    for value in [
        Value::Text("bytes".into()),
        Value::Int(1),
        Value::Bool(true),
    ] {
        assert_eq!(
            run(&format!("SELECT encode($1,'escape'){SOURCE}"), &[value]).unwrap_err(),
            Error::Type
        );
    }
    for expression in [
        "encode(NULL,'escape')",
        "encode(NULL,'hex')",
        "encode(NULL,NULL)",
        "pg_get_triggerdef(NULL,true)",
        "pg_get_triggerdef(1,NULL)",
    ] {
        assert_eq!(
            run(&format!("SELECT {expression}{SOURCE}"), &[])
                .unwrap()
                .rows,
            vec![vec![Value::Null]]
        );
    }
    assert_eq!(
        run(
            &format!("SELECT encode($1,NULL){SOURCE}"),
            &[Value::Bytes(vec![255])]
        )
        .unwrap()
        .rows,
        vec![vec![Value::Null]]
    );
    for format in ["hex", "base64", " escape", "escape ", "unknown"] {
        assert_eq!(
            run(
                &format!("SELECT encode($1,$2){SOURCE}"),
                &[Value::Bytes(vec![65]), Value::Text(format.into())]
            )
            .unwrap_err(),
            Error::Unsupported
        );
    }
    for expression in [
        "encode(NULL)",
        "encode(NULL,NULL,NULL)",
        "encode(true,'escape')",
        "encode(NULL,true)",
        "pg_get_triggerdef(1)",
        "pg_get_triggerdef(1,true,false)",
        "pg_get_triggerdef(true,true)",
        "pg_get_triggerdef(1,1)",
        "other.encode(NULL,'escape')",
        "other.pg_get_triggerdef(NULL,true)",
        "unknown_trigger_fn(NULL)",
    ] {
        assert!(
            plan(&format!("SELECT {expression}{SOURCE}")).is_err(),
            "{expression}"
        );
    }
    for expression in ["encode(NULL,'escape')", "pg_get_triggerdef(NULL,true)"] {
        assert!(
            plan_gateway(&format!("SELECT {expression}"))
                .unwrap()
                .is_none()
        );
    }
    let p = plan(&format!("SELECT pg_get_triggerdef($1,$2){SOURCE}"))
        .unwrap()
        .unwrap();
    assert_eq!(p.parameter_types(), &[PgType::Oid, PgType::Bool]);
    assert_eq!(describe(&p).unwrap()[0].data_type, PgType::Text);
    assert_eq!(
        execute(
            &p,
            &CatalogSnapshot::default(),
            &[Value::Null, Value::Bool(false)]
        )
        .unwrap()
        .rows,
        vec![vec![Value::Null]]
    );
    assert_eq!(
        execute(
            &p,
            &CatalogSnapshot::default(),
            &[Value::Int(1), Value::Bool(true)]
        )
        .unwrap_err(),
        Error::Unsupported
    );
    assert!(plan("DELETE FROM pg_trigger").is_err());
    assert!(
        plan("SELECT encode(t.tgargs,'escape') FROM pg_trigger t JOIN private_table p ON true")
            .is_err()
    );
}

#[test]
fn exact_remaining_output_n_and_n_plus_one() {
    let empty_prefix = "p".repeat((MAX_BYTES - 8 - 8) / 2);
    let empty_params = [Value::Text(empty_prefix.clone()), Value::Bytes(vec![])];
    assert_eq!(
        run(
            &format!("SELECT $1::text,encode($2,'escape'){SOURCE}"),
            &empty_params
        )
        .unwrap()
        .rows,
        vec![vec![Value::Text(empty_prefix), Value::Text(String::new())]]
    );
    assert_eq!(
        run(
            &format!("SELECT $1::text,false,encode($2,'escape'){SOURCE}"),
            &empty_params
        )
        .unwrap_err(),
        Error::Limit
    );
    // Conservative Text bytes=2*length+8. 64 zero bytes encode to256 chars.
    // Prefix plus encoded output is exactly4MiB; inserting one Bool consumes one more byte.
    let prefix = "p".repeat((MAX_BYTES - (256 * 2 + 8) - 8) / 2);
    let params = [Value::Text(prefix.clone()), Value::Bytes(vec![0; 64])];
    let out = run(
        &format!("SELECT $1::text,encode($2,'escape'){SOURCE}"),
        &params,
    )
    .unwrap();
    assert_eq!(
        out.rows,
        vec![vec![Value::Text(prefix), Value::Text("\\000".repeat(64))]]
    );
    assert_eq!(
        run(
            &format!("SELECT $1::text,false,encode($2,'escape'){SOURCE}"),
            &params
        )
        .unwrap_err(),
        Error::Limit
    );
    // A later row also receives the aggregate remaining-output budget.
    let across_rows = "SELECT CASE WHEN n.oid=11 THEN $1::text ELSE encode($2,'escape') END FROM pg_namespace n WHERE n.oid IN (11,2200)";
    assert_eq!(
        run(across_rows, &params).unwrap().rows,
        vec![
            vec![params[0].clone()],
            vec![Value::Text("\\000".repeat(64))]
        ]
    );
    assert_eq!(
        run(
            "SELECT CASE WHEN n.oid=11 THEN $1::text ELSE encode($2,'escape') END,false FROM pg_namespace n WHERE n.oid IN (11,2200)",
            &params
        )
        .unwrap_err(),
        Error::Limit
    );
}

#[test]
fn global_output_and_work_admission() {
    let sql = format!("SELECT encode($1,'escape'){SOURCE}");
    assert_eq!(
        run(&sql, &[Value::Bytes(vec![0; 524_288])]).unwrap_err(),
        Error::Limit
    );
    assert_eq!(
        run(&sql, &[Value::Bytes(vec![b'a'; MAX_BYTES / 2])]).unwrap_err(),
        Error::Limit
    );
    // This fixed three-namespace, one-result query reaches exactly2,000,000
    // work units at653053 ASCII bytes; one more byte exceeds the work cap.
    // Both outputs fit4MiB, so output admission cannot hide the work boundary.
    assert_eq!(
        run(&sql, &[Value::Bytes(vec![b'a'; 653_053])])
            .unwrap()
            .rows,
        vec![vec![Value::Text("a".repeat(653_053))]]
    );
    assert_eq!(
        run(&sql, &[Value::Bytes(vec![b'a'; 653_054])]).unwrap_err(),
        Error::Limit
    );
}

#[test]
fn exact_conditional_full_trigger_contract() {
    let sql = r###"select T.tgrelid as table_id,
       T.oid as trigger_id,
       T.xmin as trigger_state_number,
       T.tgname as trigger_name,
       T.tgfoid as function_id,
       pg_catalog.encode(T.tgargs, 'escape') as function_args,
       T.tgtype as bits,
       T.tgdeferrable as is_deferrable,
       T.tginitdeferred as is_init_deferred,
       T.tgenabled as trigger_fire_mode,
       T.tgattr as columns,
       T.tgconstraint != 0 as is_constraint,
       T.tgoldtable /* null */ as old_table_name,
       T.tgnewtable /* null */ as new_table_name,
       pg_catalog.pg_get_triggerdef(T.oid, true) as source_code
from pg_catalog.pg_trigger T
     join pg_catalog.pg_class TAB on TAB.oid = T.tgrelid and TAB.relnamespace = $1::oid
where true
  --  and TAB.relname in ( :[*f_names] )
  --  and pg_catalog.age(T.xmin) <= #TXAGE
  and not T.tgisinternal
"###;
    let p = plan(sql).unwrap().unwrap();
    assert_eq!(p.parameter_types(), &[PgType::Oid]);
    let expected = "table_id:Oid trigger_id:Oid trigger_state_number:Xid trigger_name:Name function_id:Oid function_args:Text bits:Int2 is_deferrable:Bool is_init_deferred:Bool trigger_fire_mode:Char columns:Int2Vector is_constraint:Bool old_table_name:Name new_table_name:Name source_code:Text";
    assert_eq!(
        describe(&p)
            .unwrap()
            .iter()
            .map(|f| format!("{}:{:?}", f.name, f.data_type))
            .collect::<Vec<_>>()
            .join(" "),
        expected
    );
    let snapshot = CatalogSnapshot {
        tables: vec![TableDef {
            id: 1,
            name: "visible".into(),
            columns: vec![ColumnDef {
                id: 1,
                name: "id".into(),
                data_type: ScalarType::Integer,
                not_null: true,
            }],
            primary_key: vec![0],
        }],
        ..CatalogSnapshot::default()
    };
    for snapshot in [CatalogSnapshot::default(), snapshot] {
        assert_eq!(
            execute(&p, &snapshot, &[Value::Int(2200)]).unwrap().rows,
            Vec::<Vec<Value>>::new()
        );
        assert_eq!(
            execute(
                &plan("SELECT * FROM pg_trigger").unwrap().unwrap(),
                &snapshot,
                &[]
            )
            .unwrap()
            .rows,
            Vec::<Vec<Value>>::new()
        );
    }
}
