use giggles_pg_catalog::*;

fn run(snapshot: &CatalogSnapshot, sql: &str) -> Vec<Vec<Value>> {
    let plan = plan(sql)
        .unwrap()
        .unwrap_or_else(|| panic!("missing catalog plan: {sql}"));
    execute(&plan, snapshot, &[])
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .rows
}
fn s(s: &str) -> Value {
    Value::Text(s.into())
}
fn i(n: i64) -> Value {
    Value::Int(n)
}
fn snapshot() -> CatalogSnapshot {
    let mut s = CatalogSnapshot::default();
    s.tables.push(TableDef {
        id: 1,
        name: "DesignDoc".into(),
        primary_key: vec![0],
        columns: vec![
            ColumnDef {
                id: 2,
                name: "id".into(),
                data_type: ScalarType::Uuid,
                not_null: true,
            },
            ColumnDef {
                id: 3,
                name: "title".into(),
                data_type: ScalarType::Text,
                not_null: false,
            },
            ColumnDef {
                id: 4,
                name: "deletedAt".into(),
                data_type: ScalarType::Timestamp,
                not_null: false,
            },
        ],
    });
    s.routines.insert(
        65560,
        RoutineDef {
            schema: "public".into(),
            name: "cleanup_semantic_vectors".into(),
            owner: 10,
            body: "\nBEGIN\n  RETURN OLD;\nEND;\n".into(),
            volatility: 'v',
            strict: false,
            security_definer: false,
            leakproof: false,
            parallel: 'u',
            cost: giggles_pg_types::Float4::from_bits(100f32.to_bits()),
        },
    );
    s.triggers.push(TriggerDef {
        oid: 65564,
        table: 1,
        routine: 65560,
        name: "cleanup_design_doc_vectors".into(),
        kind: 27,
        enabled: 'O',
        update_columns: vec![4],
        args: vec!["design-doc".into()],
        condition: None,
        condition_columns: vec![],
    });
    s
}

#[test]
fn oracle_catalog_rows_and_dependencies() {
    let snapshot = snapshot();
    assert_eq!(
        run(
            &snapshot,
            "SELECT proname, prolang= (SELECT oid FROM pg_language WHERE lanname='plpgsql'), \
        prorettype::regtype::text, pronargs, proargtypes, prokind, provolatile, proisstrict, prosecdef, proretset, proparallel, \
        length(prosrc) > 0, probin, proconfig, proowner=(SELECT oid FROM pg_roles WHERE rolname=current_user) \
        FROM pg_proc WHERE proname='cleanup_semantic_vectors'"
        ),
        vec![vec![
            s("cleanup_semantic_vectors"),
            Value::Bool(true),
            s("trigger"),
            i(0),
            Value::Array(vec![]),
            s("f"),
            s("v"),
            Value::Bool(false),
            Value::Bool(false),
            Value::Bool(false),
            s("u"),
            Value::Bool(true),
            Value::Null,
            Value::Null,
            Value::Bool(true)
        ]]
    );
    assert_eq!(
        run(
            &snapshot,
            "SELECT tgrelid::regclass::text,tgfoid::regproc::text,tgtype,tgenabled, \
        tgisinternal,tgnargs,tgattr,encode(tgargs,'escape'),tgconstraint,tgdeferrable,tginitdeferred,tgqual IS NULL,tgoldtable,tgnewtable FROM pg_trigger"
        ),
        vec![vec![
            s("\"DesignDoc\""),
            s("cleanup_semantic_vectors"),
            i(27),
            s("O"),
            Value::Bool(false),
            i(1),
            Value::Array(vec![i(3)]),
            s(r"design-doc\000"),
            i(0),
            Value::Bool(false),
            Value::Bool(false),
            Value::Bool(true),
            Value::Null,
            Value::Null
        ]]
    );
    assert_eq!(
        run(
            &snapshot,
            "SELECT refclassid::regclass::text, refobjsubid, deptype FROM pg_depend \
        WHERE classid='pg_trigger'::regclass AND objid=65564 ORDER BY 1,3"
        ),
        vec![
            vec![s("pg_class"), i(0), s("a")],
            vec![s("pg_class"), i(3), s("n")],
            vec![s("pg_proc"), i(0), s("n")]
        ]
    );
    assert_eq!(
        run(
            &snapshot,
            "SELECT relhastriggers FROM pg_class WHERE relname='DesignDoc'"
        ),
        vec![vec![Value::Bool(true)]]
    );
    assert_eq!(
        run(
            &snapshot,
            "SELECT lanname,lanispl,lanpltrusted,lanplcallfoid::regproc::text FROM pg_language WHERE lanname='plpgsql'"
        ),
        vec![vec![
            s("plpgsql"),
            Value::Bool(true),
            Value::Bool(true),
            s("plpgsql_call_handler")
        ]]
    );
}
#[test]
fn oracle_definitions_and_information_schema() {
    let mut snapshot = snapshot();
    assert!(
        plan_gateway("SELECT 'cleanup_semantic_vectors()'::regprocedure")
            .unwrap()
            .is_some()
    );
    const A: &str = "EXECUTE FUNCTION cleanup_semantic_vectors('design-doc')";
    const T: &str =
        "CREATE TRIGGER cleanup_design_doc_vectors BEFORE DELETE OR UPDATE OF \"deletedAt\"";
    let query = "SELECT pg_get_triggerdef(oid), pg_get_triggerdef(oid,true) FROM pg_trigger";
    let expected = vec![vec![
        Value::Text(format!("{T} ON public.\"DesignDoc\" FOR EACH ROW {A}")),
        Value::Text(format!("{T} ON \"DesignDoc\" FOR EACH ROW {A}")),
    ]];
    assert_eq!(run(&snapshot, query), expected);
    assert_eq!(
        run(
            &snapshot,
            "SELECT pg_get_function_result(oid),pg_get_function_arguments(oid),pg_get_function_identity_arguments(oid) FROM pg_proc"
        ),
        vec![vec![s("trigger"), s(""), s("")]]
    );
    assert_eq!(
        run(
            &snapshot,
            "SELECT pg_get_functiondef('cleanup_semantic_vectors'::regproc)"
        ),
        vec![vec![s(
            "CREATE OR REPLACE FUNCTION public.cleanup_semantic_vectors()\n RETURNS trigger\n LANGUAGE plpgsql\nAS $function$\nBEGIN\n  RETURN OLD;\nEND;\n$function$\n"
        )]]
    );
    assert_eq!(
        run(
            &snapshot,
            "SELECT 'public.cleanup_semantic_vectors()'::regprocedure::text, 65560::regproc"
        ),
        vec![vec![
            s("cleanup_semantic_vectors()"),
            Value::Record(vec![i(65560), s("cleanup_semantic_vectors")])
        ]]
    );
    assert_eq!(
        run(
            &snapshot,
            "SELECT event_manipulation,event_object_table,action_order,action_condition,action_statement,action_orientation,action_timing \
        FROM information_schema.triggers ORDER BY 1"
        ),
        vec![
            vec![
                s("DELETE"),
                s("DesignDoc"),
                i(1),
                Value::Null,
                s(A),
                s("ROW"),
                s("BEFORE")
            ],
            vec![
                s("UPDATE"),
                s("DesignDoc"),
                i(1),
                Value::Null,
                s(A),
                s("ROW"),
                s("BEFORE")
            ]
        ]
    );
    assert_eq!(
        run(
            &snapshot,
            "SELECT routine_name,routine_type,data_type,external_language,routine_body FROM information_schema.routines"
        ),
        vec![vec![
            s("cleanup_semantic_vectors"),
            s("FUNCTION"),
            s("trigger"),
            s("PLPGSQL"),
            s("EXTERNAL")
        ]]
    );
    snapshot.tables[0].columns[2].name = "retiredAt".into();
    assert_eq!(
        run(&snapshot, query),
        expected
            .into_iter()
            .map(|row| row
                .into_iter()
                .map(|v| match v {
                    Value::Text(s) => Value::Text(s.replace("deletedAt", "retiredAt")),
                    _ => unreachable!(),
                })
                .collect::<Vec<_>>())
            .collect::<Vec<_>>()
    );
}
