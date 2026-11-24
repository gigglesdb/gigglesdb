use giggles_pg_catalog::*;
fn run(sql: &str) -> Result<ResultSet, Error> {
    execute(
        &plan(sql)?.expect("catalog query"),
        &CatalogSnapshot::default(),
        &[],
    )
}
#[test]
fn strict_null_and_nonnull_refusal() {
    for expression in [
        "pg_get_viewdef(NULL,true)",
        "pg_get_viewdef(1,true)",
        "pg_get_viewdef(1,NULL)",
        "pg_get_ruledef(NULL,false)",
        "pg_get_ruledef(1,NULL)",
        "pg_get_function_arguments(NULL)",
        "pg_get_function_result(NULL)",
        "pg_get_function_sqlbody(NULL)",
        "pg_get_function_arguments(1)",
        "pg_get_function_result(1)",
        "pg_get_functiondef(NULL)",
        "NULL::regtype::text",
        "(NULL::regprocedure)::varchar",
        "CAST(CAST(NULL AS regoperator) AS text)",
    ] {
        let sql = format!("SELECT {expression} FROM pg_namespace n WHERE n.oid=11");
        let out = run(&sql).unwrap();
        assert_eq!(out.fields[0].data_type, PgType::Text, "{sql}");
        assert_eq!(out.rows, vec![vec![Value::Null]], "{sql}");
    }
    for expression in [
        "pg_get_ruledef(1,false)",
        "pg_get_function_sqlbody(1)",
        "1::regoperator::text",
    ] {
        assert_eq!(
            run(&format!(
                "SELECT {expression} FROM pg_namespace n WHERE n.oid=11"
            ))
            .unwrap_err(),
            Error::Unsupported
        );
    }
}
#[test]
fn closed_signatures_and_cast_boundary() {
    for expression in [
        "pg_get_viewdef()",
        "pg_get_ruledef(1,true,false)",
        "pg_get_function_result()",
        "pg_get_function_sqlbody(1,2)",
        "pg_get_function_arguments(true)",
        "pg_get_ruledef(1,1)",
        "pg_get_function_result('bad')",
        "other.pg_get_function_result(NULL)",
        "NULL::regoperator",
        "NULL::other.regtype::text",
        "true::regtype::text",
    ] {
        assert!(
            plan(&format!("SELECT {expression} FROM pg_namespace n")).is_err(),
            "{expression}"
        );
    }
    assert!(
        plan_gateway("SELECT pg_get_function_result(NULL)")
            .unwrap()
            .is_some()
    );
    assert!(
        plan_gateway("SELECT NULL::regtype::text")
            .unwrap()
            .is_none()
    );
}
#[test]
fn display_parameter_traversal() {
    let p = plan("SELECT $1::regtype::text FROM pg_namespace n WHERE n.oid=11")
        .unwrap()
        .unwrap();
    assert_eq!(p.parameter_types(), &[PgType::Text]);
    assert!(!p.allows_numeric_input_override());
    assert_eq!(
        execute(&p, &CatalogSnapshot::default(), &[Value::Null])
            .unwrap()
            .rows,
        vec![vec![Value::Null]]
    );
    assert_eq!(
        execute(&p, &CatalogSnapshot::default(), &[Value::Int(23)])
            .unwrap()
            .rows,
        vec![vec![Value::Text("integer".into())]]
    );
}

#[test]
fn captured_0() {
    let p=plan(r###"select T.relkind as view_kind,
       T.oid as view_id,
       pg_catalog.pg_get_viewdef(T.oid, true) as source_text
from pg_catalog.pg_class T
     join pg_catalog.pg_namespace N on T.relnamespace = N.oid
where N.oid = $1::oid
  and T.relkind in ('m','v')
  --  and T.relname in ( :[*f_names] )
  --  and (pg_catalog.age(T.xmin) <= #SRCTXAGE or exists(
  --  select A.attrelid from pg_catalog.pg_attribute A where A.attrelid = T.oid and pg_catalog.age(A.xmin) <= #SRCTXAGE))
;"###).unwrap().unwrap();
    let out = execute(&p, &CatalogSnapshot::default(), &[Value::Int(2200)]).unwrap();
    assert!(out.rows.is_empty());
    assert!(!out.fields.is_empty());
    for f in &out.fields {
        if matches!(
            f.name.as_str(),
            "source_text" | "arguments_def" | "result_def" | "sqlbody_def" | "in_type" | "key_type"
        ) {
            assert_eq!(f.data_type, PgType::Text);
        }
    }
}

#[test]
fn captured_1() {
    let p = plan(
        r###"with A as ( select oid as table_id,
                   pg_catalog.upper(relkind) as table_kind
            from pg_catalog.pg_class
            where relnamespace = $1::oid
              and relkind in ('r', 'm', 'v', 'f', 'p')
              --  and relname in ( :[*f_names] )
          )
select table_kind,
       table_id,
       R.oid as rule_id,
       pg_catalog.pg_get_ruledef(R.oid, true) as source_text
from A
     join pg_catalog.pg_rewrite R
       on A.table_id = R.ev_class
where R.rulename != '_RETURN'::name
  --  and pg_catalog.age(R.xmin) <= #SRCTXAGE
;"###,
    )
    .unwrap()
    .unwrap();
    let out = execute(&p, &CatalogSnapshot::default(), &[Value::Int(2200)]).unwrap();
    assert!(out.rows.is_empty());
    assert!(!out.fields.is_empty());
    for f in &out.fields {
        if matches!(
            f.name.as_str(),
            "source_text" | "arguments_def" | "result_def" | "sqlbody_def" | "in_type" | "key_type"
        ) {
            assert_eq!(f.data_type, PgType::Text);
        }
    }
}

#[test]
fn captured_2() {
    let p = plan(
        r###"with system_languages as ( select oid as lang
                           from pg_catalog.pg_language
                           where lanname in ('c','internal') )
select oid as id,
       pg_catalog.pg_get_function_arguments(oid) as arguments_def,
       pg_catalog.pg_get_function_result(oid) as result_def,
       pg_catalog.pg_get_function_sqlbody(oid) /* null */ as sqlbody_def,
       prosrc as source_text
from pg_catalog.pg_proc
where pronamespace = $1::oid
  --  and pg_proc.proname in ( :[*f_names] )
  --  and pg_catalog.age(xmin) <= #SRCTXAGE
  and not (prokind = 'a') /* proisagg */
  and prolang not in (select lang from system_languages)
  and prosrc is not null
;"###,
    )
    .unwrap()
    .unwrap();
    let out = execute(&p, &CatalogSnapshot::default(), &[Value::Int(2200)]).unwrap();
    assert!(out.rows.is_empty());
    assert!(!out.fields.is_empty());
    for f in &out.fields {
        if matches!(
            f.name.as_str(),
            "source_text" | "arguments_def" | "result_def" | "sqlbody_def" | "in_type" | "key_type"
        ) {
            assert_eq!(f.data_type, PgType::Text);
        }
    }
}

#[test]
fn captured_3() {
    let p = plan(
        r###"select O.oid as id,
       O.xmin as state_number,
       opcname as name,
       opcintype::regtype::varchar as in_type,
       case when opckeytype = 0 then null else opckeytype::regtype::varchar end as key_type,
       opcdefault as is_default,
       opcfamily as family_id,
       opfname as family,
       opcmethod as access_method_id,
       pg_catalog.pg_get_userbyid(O.opcowner) as "owner"
from pg_catalog.pg_opclass O
     join pg_catalog.pg_opfamily F on F.oid = opcfamily
where opcnamespace = $1::oid
  --  and opcname in ( :[*f_names] )
  --  and pg_catalog.age(O.xmin) <= #TXAGE
;"###,
    )
    .unwrap()
    .unwrap();
    let out = execute(&p, &CatalogSnapshot::default(), &[Value::Int(2200)]).unwrap();
    assert!(out.rows.is_empty());
    assert!(!out.fields.is_empty());
    for f in &out.fields {
        if matches!(
            f.name.as_str(),
            "source_text" | "arguments_def" | "result_def" | "sqlbody_def" | "in_type" | "key_type"
        ) {
            assert_eq!(f.data_type, PgType::Text);
        }
    }
}

#[test]
fn captured_4() {
    let p = plan(
        r###"select pg_amop.oid
from pg_catalog.pg_amop
         join pg_catalog.pg_opfamily on pg_opfamily.oid = amopfamily
where opfnamespace = $1::oid
;"###,
    )
    .unwrap()
    .unwrap();
    let out = execute(&p, &CatalogSnapshot::default(), &[Value::Int(2200)]).unwrap();
    assert!(out.rows.is_empty());
    assert!(!out.fields.is_empty());
    for f in &out.fields {
        if matches!(
            f.name.as_str(),
            "source_text" | "arguments_def" | "result_def" | "sqlbody_def" | "in_type" | "key_type"
        ) {
            assert_eq!(f.data_type, PgType::Text);
        }
    }
}

#[test]
fn captured_5() {
    let p = plan(
        r###"select pg_amproc.oid
from pg_catalog.pg_amproc
         join pg_catalog.pg_opfamily on pg_opfamily.oid = amprocfamily
where opfnamespace = $1::oid
;"###,
    )
    .unwrap()
    .unwrap();
    let out = execute(&p, &CatalogSnapshot::default(), &[Value::Int(2200)]).unwrap();
    assert!(out.rows.is_empty());
    assert!(!out.fields.is_empty());
    for f in &out.fields {
        if matches!(
            f.name.as_str(),
            "source_text" | "arguments_def" | "result_def" | "sqlbody_def" | "in_type" | "key_type"
        ) {
            assert_eq!(f.data_type, PgType::Text);
        }
    }
}

#[test]
fn absent_am_display_projections() {
    // Exact installed projection expressions; isolated from the incremental age filter.
    for sql in [
        "SELECT P.amproc::regprocedure::varchar, P.amproclefttype::regtype::varchar, P.amprocrighttype::regtype::varchar FROM pg_catalog.pg_amproc P",
        "SELECT O.amopopr::regoperator::varchar FROM pg_catalog.pg_amop O",
    ] {
        let out = run(sql).unwrap();
        assert!(out.rows.is_empty());
        assert!(out.fields.iter().all(|f| f.data_type == PgType::Text));
    }
}
#[test]
fn schema_and_binder_legacy_guards() {
    assert!(
        plan("SELECT NULL::regtype::text FROM hidden_table")
            .unwrap()
            .is_none()
    );
    for sql in [
        "SELECT pg_get_function_result(NULL) FROM pg_catalog.unknown",
        "DELETE FROM pg_proc",
    ] {
        assert!(plan(sql).is_err(), "{sql}");
    }
    let out = run(
        "SELECT NULL::regclass, NULL::regproc, NULL::regoper FROM pg_namespace n WHERE n.oid=11",
    )
    .unwrap();
    assert_eq!(out.rows, vec![vec![Value::Null; 3]]);
    assert_eq!(out.fields.iter().map(|f| f.data_type).collect::<Vec<_>>(), [PgType::Oid, PgType::Regproc, PgType::Oid]);
    let p = plan("SELECT pg_get_viewdef($1,$2::boolean) FROM pg_namespace n WHERE n.oid=11")
        .unwrap()
        .unwrap();
    assert_eq!(p.parameter_types(), &[PgType::Text, PgType::Bool]);
    assert_eq!(
        execute(
            &p,
            &CatalogSnapshot::default(),
            &[Value::Null, Value::Bool(true)]
        )
        .unwrap()
        .rows,
        vec![vec![Value::Null]]
    );
}
