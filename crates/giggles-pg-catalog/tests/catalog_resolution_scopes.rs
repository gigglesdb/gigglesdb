use giggles_pg_catalog::*;

fn run(sql: &str) -> Result<ResultSet, Error> {
    execute(
        &plan(sql)?.expect("catalog query"),
        &CatalogSnapshot::default(),
        &[],
    )
}
fn scalar(expr: &str, ty: PgType, value: Value) {
    let sql = format!("SELECT {expr} AS result FROM pg_namespace n WHERE n.oid=11");
    let p = plan(&sql).unwrap().unwrap();
    assert_eq!(
        describe(&p).unwrap(),
        vec![Field {
            name: "result".into(),
            data_type: ty
        }]
    );
    assert_eq!(
        execute(&p, &CatalogSnapshot::default(), &[]).unwrap().rows,
        vec![vec![value]],
        "{sql}"
    );
}

#[test]
fn integer_oid_results_and_lazy_case() {
    for integer in ["int2", "int4", "int8"] {
        for (a, b) in [
            (format!("7::{integer}"), "11::oid".into()),
            ("11::oid".into(), format!("7::{integer}")),
        ] {
            for choice in [true, false] {
                let expected = if choice {
                    if a == "11::oid" { 11 } else { 7 }
                } else if b == "11::oid" {
                    11
                } else {
                    7
                };
                scalar(
                    &format!("CASE WHEN {choice} THEN {a} ELSE {b} END"),
                    PgType::Oid,
                    Value::Int(expected),
                );
                scalar(
                    &format!("CASE {choice} WHEN true THEN {a} ELSE {b} END"),
                    PgType::Oid,
                    Value::Int(expected),
                );
            }
            scalar(
                &format!("coalesce(NULL,{a},{b})"),
                PgType::Oid,
                Value::Int(if a == "11::oid" { 11 } else { 7 }),
            );
            scalar(
                &format!("ARRAY[{a},{b}]"),
                PgType::OidArray,
                Value::Array(vec![
                    Value::Int(if a == "11::oid" { 11 } else { 7 }),
                    Value::Int(if b == "11::oid" { 11 } else { 7 }),
                ]),
            );
        }
    }
    for expr in [
        "CASE WHEN true THEN NULL ELSE 11::oid END",
        "CASE WHEN false THEN 11::oid END",
    ] {
        scalar(expr, PgType::Oid, Value::Null);
    }
    for (expr, v) in [
        ("CASE WHEN true THEN 0 ELSE 11::oid END", 0),
        (
            "CASE WHEN true THEN 4294967295::int8 ELSE 11::oid END",
            4294967295,
        ),
        ("CASE WHEN false THEN -1::int8 ELSE 11::oid END", 11),
        ("CASE WHEN true THEN 11::oid ELSE 4294967296::int8 END", 11),
    ] {
        scalar(expr, PgType::Oid, Value::Int(v));
    }
    for expr in [
        "CASE WHEN true THEN -1::int8 ELSE 11::oid END",
        "CASE WHEN false THEN 11::oid ELSE 4294967296::int8 END",
        "coalesce(-1::int8,11::oid)",
    ] {
        assert_eq!(
            run(&format!("SELECT {expr} FROM pg_namespace n WHERE n.oid=11")),
            Err(Error::Type)
        );
    }
    scalar(
        "CASE WHEN true THEN 7::int2 ELSE 8::int8 END",
        PgType::Int8,
        Value::Int(7),
    );
    scalar(
        "CASE WHEN true THEN 'a'::name ELSE 'b'::text END",
        PgType::Text,
        Value::Text("a".into()),
    );
    scalar(
        "CASE WHEN false THEN NULL ELSE true END",
        PgType::Bool,
        Value::Bool(true),
    );
    scalar(
        "11::oid = ANY(ARRAY[7::int8,11::oid])",
        PgType::Bool,
        Value::Bool(true),
    );
    scalar("11::oid::int8", PgType::Int8, Value::Int(11));
}

#[test]
fn nearest_scope_and_outer_fallback_have_nonempty_witnesses() {
    for (sql, expected) in [
        (
            "SELECT (SELECT oid FROM pg_namespace i WHERE i.oid=2200) FROM pg_namespace o WHERE o.oid=11",
            2200,
        ),
        (
            "SELECT (SELECT nspname FROM pg_type i WHERE i.oid=23) FROM pg_namespace o WHERE o.oid=11",
            0,
        ),
        (
            "SELECT (SELECT (SELECT nspname FROM pg_type j WHERE j.oid=23) FROM pg_type i WHERE i.oid=16) FROM pg_namespace o WHERE o.oid=11",
            0,
        ),
        (
            "SELECT (SELECT (SELECT oid FROM pg_namespace j WHERE j.oid=2200) FROM pg_type i WHERE i.oid=23) FROM pg_namespace o WHERE o.oid=11",
            2200,
        ),
    ] {
        let expected = if expected == 0 {
            Value::Text("pg_catalog".into())
        } else {
            Value::Int(expected)
        };
        assert_eq!(run(sql).unwrap().rows, vec![vec![expected]], "{sql}");
    }
    assert_eq!(plan("SELECT (SELECT o.nspname FROM pg_type o WHERE o.oid=23) FROM pg_namespace o WHERE o.oid=11").unwrap_err(),Error::UnknownColumn);
    assert_eq!(
        plan("SELECT (SELECT oid FROM pg_type a JOIN pg_namespace b ON true) FROM pg_namespace o")
            .unwrap_err(),
        Error::AmbiguousColumn
    );
}

#[test]
fn natural_lateral_cte_parameters_and_refusals() {
    assert_eq!(run("SELECT a.oid,b.oid,oid FROM (SELECT oid FROM pg_namespace) a NATURAL JOIN (SELECT oid FROM pg_namespace) b WHERE oid=11").unwrap().rows,vec![vec![Value::Int(11);3]]);
    assert_eq!(run("WITH x AS (SELECT oid FROM pg_namespace WHERE oid=11) SELECT (SELECT oid FROM x) FROM pg_namespace o WHERE o.oid=2200").unwrap().rows,vec![vec![Value::Int(11)]]);
    assert_eq!(run("SELECT n.oid,x.v FROM pg_namespace n CROSS JOIN unnest(ARRAY[n.oid]) x(v) WHERE n.oid=11").unwrap().rows,vec![vec![Value::Int(11);2]]);
    let p = plan(
        "SELECT (SELECT oid FROM pg_type i WHERE i.oid=$1) FROM pg_namespace o WHERE o.oid=$2",
    )
    .unwrap()
    .unwrap();
    assert_eq!(p.parameter_types(), &[PgType::Oid, PgType::Oid]);
    assert_eq!(
        execute(
            &p,
            &CatalogSnapshot::default(),
            &[Value::Int(23), Value::Int(11)]
        )
        .unwrap()
        .rows,
        vec![vec![Value::Int(23)]]
    );
    assert!(plan("UPDATE pg_namespace SET nspname='x'").is_err());
    assert!(plan("SELECT (SELECT secret FROM hidden) FROM pg_namespace").is_err());
}

#[test]
fn exact_full_rules_contract() {
    let p = plan(
        r###"select R.ev_class as table_id,
       R.oid as rule_id,
       R.xmin as rule_state_number,
       R.rulename as rule_name,
       pg_catalog.translate(ev_type,'1234','SUID') as rule_event_code,
       R.ev_enabled as rule_fire_mode,
       R.is_instead as rule_is_instead
from pg_catalog.pg_rewrite R
where R.ev_class in ( select oid
                      from pg_catalog.pg_class
                      where relnamespace = $1::oid
                      --  and relname in ( :[*f_names] )
                    )
  --  and pg_catalog.age(R.xmin) <= #TXAGE
  and R.rulename != '_RETURN'::name
order by R.ev_class::bigint, ev_type
"###,
    )
    .unwrap()
    .unwrap();
    let expected = "table_id:Oid rule_id:Oid rule_state_number:Xid rule_name:Name rule_event_code:Text rule_fire_mode:Char rule_is_instead:Bool";
    assert_eq!(
        describe(&p)
            .unwrap()
            .iter()
            .map(|f| format!("{}:{:?}", f.name, f.data_type))
            .collect::<Vec<_>>()
            .join(" "),
        expected
    );
    assert_eq!(
        execute(&p, &CatalogSnapshot::default(), &[Value::Int(11)])
            .unwrap()
            .rows,
        Vec::<Vec<Value>>::new()
    );
}

#[test]
fn exact_aggregate38_contract() {
    let p=plan(r###"select P.oid as aggregate_id,
       P.xmin as state_number,
       P.proname as aggregate_name,
       P.proargnames as arg_names,
       P.proargmodes as arg_modes,
       P.proargtypes::int[] as in_arg_types,
       P.proallargtypes::int[] as all_arg_types,
       A.aggtransfn::oid as transition_function_id,
       A.aggtransfn::regproc::text as transition_function_name,
       A.aggtranstype as transition_type,
       A.aggfinalfn::oid as final_function_id,
       case when A.aggfinalfn::oid = 0 then null else A.aggfinalfn::regproc::varchar end as final_function_name,
       case when A.aggfinalfn::oid = 0 then 0 else P.prorettype end as final_return_type,
       A.agginitval as initial_value,
       A.aggsortop as sort_operator_id,
       case when A.aggsortop = 0 then null else A.aggsortop::regoper::varchar end as sort_operator_name,
       pg_catalog.pg_get_userbyid(P.proowner) as "owner"
       ,
       A.aggfinalextra as final_extra,
       A.aggtransspace as state_size,
       A.aggmtransfn::oid as moving_transition_id,
       case when A.aggmtransfn::oid = 0 then null else A.aggmtransfn::regproc::varchar end as moving_transition_name,
       A.aggminvtransfn::oid as inverse_transition_id,
       case when A.aggminvtransfn::oid = 0 then null else A.aggminvtransfn::regproc::varchar end as inverse_transition_name,
       A.aggmtranstype::oid as moving_state_type,
       A.aggmtransspace as moving_state_size,
       A.aggmfinalfn::oid as moving_final_id,
       case when A.aggmfinalfn::oid = 0 then null else A.aggmfinalfn::regproc::varchar end as moving_final_name,
       A.aggmfinalextra as moving_final_extra,
       A.aggminitval as moving_initial_value,
       A.aggkind as aggregate_kind,
       A.aggnumdirectargs as direct_args
       
       ,
       A.aggcombinefn::oid as combine_function_id,
       case when A.aggcombinefn::oid = 0 then null else A.aggcombinefn::regproc::varchar end as combine_function_name,
       A.aggserialfn::oid as serialization_function_id,
       case when A.aggserialfn::oid = 0 then null else A.aggserialfn::regproc::varchar end as serialization_function_name,
       A.aggdeserialfn::oid as deserialization_function_id,
       case when A.aggdeserialfn::oid = 0 then null else A.aggdeserialfn::regproc::varchar end as deserialization_function_name,
       P.proparallel as concurrency_kind
       
from pg_catalog.pg_aggregate A
     join pg_catalog.pg_proc P
       on A.aggfnoid = P.oid
where P.pronamespace = $1::oid
  --  and P.proname in ( :[*f_names] )
  --  and (pg_catalog.age(A.xmin) <= #TXAGE or pg_catalog.age(P.xmin) <= #TXAGE)
order by P.oid
"###).unwrap().unwrap();
    let fields = describe(&p).unwrap();
    assert_eq!(fields.len(), 38);
    assert_eq!(
        fields
            .iter()
            .map(|f| format!("{}:{:?}", f.name, f.data_type))
            .collect::<Vec<_>>()
            .join(" "),
        r###"aggregate_id:Oid state_number:Xid aggregate_name:Name arg_names:TextArray arg_modes:CharArray in_arg_types:Int4Array all_arg_types:Int4Array transition_function_id:Oid transition_function_name:Text transition_type:Oid final_function_id:Oid final_function_name:Text final_return_type:Oid initial_value:Text sort_operator_id:Oid sort_operator_name:Text owner:Name final_extra:Bool state_size:Int4 moving_transition_id:Oid moving_transition_name:Text inverse_transition_id:Oid inverse_transition_name:Text moving_state_type:Oid moving_state_size:Int4 moving_final_id:Oid moving_final_name:Text moving_final_extra:Bool moving_initial_value:Text aggregate_kind:Char direct_args:Int2 combine_function_id:Oid combine_function_name:Text serialization_function_id:Oid serialization_function_name:Text deserialization_function_id:Oid deserialization_function_name:Text concurrency_kind:Char"###
    );
    assert_eq!(
        execute(&p, &CatalogSnapshot::default(), &[Value::Int(2200)])
            .unwrap()
            .rows,
        Vec::<Vec<Value>>::new()
    );
}

#[test]
fn signed_width_common_oid_results() {
    for (source, expected) in [
        ("(-1)::int2", 4294967295),
        ("(-32768)::int2", 4294934528),
        ("32767::int2", 32767),
        ("(-1)::int4", 4294967295),
        ("(-2147483648)::int4", 2147483648),
        ("2147483647::int4", 2147483647),
    ] {
        for (a, b, selected) in [(source, "11::oid", true), ("11::oid", source, false)] {
            scalar(
                &format!("CASE WHEN {selected} THEN {a} ELSE {b} END"),
                PgType::Oid,
                Value::Int(expected),
            );
            scalar(
                &format!("CASE {selected} WHEN true THEN {a} ELSE {b} END"),
                PgType::Oid,
                Value::Int(expected),
            );
            scalar(
                &format!("ARRAY[{a},{b},NULL]"),
                PgType::OidArray,
                Value::Array(if selected {
                    vec![Value::Int(expected), Value::Int(11), Value::Null]
                } else {
                    vec![Value::Int(11), Value::Int(expected), Value::Null]
                }),
            );
        }
        scalar(
            &format!("coalesce(NULL,{source},11::oid)"),
            PgType::Oid,
            Value::Int(expected),
        );
    }
    for source in ["NULL::int2", "NULL::int4"] {
        scalar(
            &format!("CASE WHEN true THEN {source} ELSE 11::oid END"),
            PgType::Oid,
            Value::Null,
        );
        scalar(
            &format!("coalesce({source},11::oid)"),
            PgType::Oid,
            Value::Int(11),
        );
        scalar(
            &format!("ARRAY[{source},11::oid]"),
            PgType::OidArray,
            Value::Array(vec![Value::Null, Value::Int(11)]),
        );
    }
    for bad in ["(-1)::int8", "4294967296::int8"] {
        scalar(
            &format!("CASE WHEN true THEN (-1)::int4 ELSE {bad} END"),
            PgType::Int8,
            Value::Int(-1),
        );
        scalar(
            &format!("CASE WHEN true THEN 11::oid ELSE {bad} END"),
            PgType::Oid,
            Value::Int(11),
        );
        scalar(
            &format!("coalesce(11::oid,{bad})"),
            PgType::Oid,
            Value::Int(11),
        );
        assert_eq!(
            run(&format!(
                "SELECT ARRAY[{bad},11::oid] FROM pg_namespace n WHERE n.oid=11"
            )),
            Err(Error::Type)
        );
    }
}

#[test]
fn signed_width_oid_parameters_validate_before_reinterpretation() {
    for (source, minimum, maximum, below, above) in [
        ("int2", -32768, 32767, -32769, 32768),
        ("int4", -2147483648, 2147483647, -2147483649, 2147483648),
    ] {
        for expr in [
            format!("CASE WHEN true THEN $1::{source} ELSE 11::oid END"),
            format!("coalesce($1::{source},11::oid)"),
            format!("ARRAY[$1::{source},11::oid]"),
        ] {
            let p = plan(&format!("SELECT {expr} FROM pg_namespace n WHERE n.oid=11"))
                .unwrap()
                .unwrap();
            assert_eq!(
                p.parameter_types(),
                &[if source == "int2" {
                    PgType::Int2
                } else {
                    PgType::Int4
                }]
            );
            for v in [minimum, -1, 0, maximum] {
                let expected = Value::Int(i64::from(u32::from_ne_bytes((v as i32).to_ne_bytes())));
                let expected = if expr.starts_with("ARRAY") {
                    Value::Array(vec![expected, Value::Int(11)])
                } else {
                    expected
                };
                assert_eq!(
                    execute(&p, &CatalogSnapshot::default(), &[Value::Int(v)])
                        .unwrap()
                        .rows,
                    vec![vec![expected]],
                    "{expr}/{v}"
                );
            }
            for value in [
                Value::Int(below),
                Value::Int(above),
                Value::Int(i64::MAX),
                Value::Text("malformed".into()),
                Value::Bool(true),
            ] {
                assert_eq!(
                    execute(&p, &CatalogSnapshot::default(), &[value]),
                    Err(Error::Type),
                    "{expr}"
                );
            }
        }
    }
    // Source-free parameters still infer Oid rather than signed-source semantics.
    let p = plan("SELECT coalesce($1,11::oid) FROM pg_namespace n WHERE n.oid=11")
        .unwrap()
        .unwrap();
    assert_eq!(p.parameter_types(), &[PgType::Oid]);
    assert_eq!(
        execute(&p, &CatalogSnapshot::default(), &[Value::Int(-1)]),
        Err(Error::Type)
    );
}
