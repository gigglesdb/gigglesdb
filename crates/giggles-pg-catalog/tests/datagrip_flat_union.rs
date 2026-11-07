use giggles_pg_catalog::*;

const COMMENTS: &str = include_str!("fixtures/datagrip_flat_comments.sql");
fn snapshot() -> CatalogSnapshot {
    CatalogSnapshot {
        tables: vec![TableDef {
            id: 7,
            name: "inventory".into(),
            columns: vec![ColumnDef {
                id: 1,
                name: "id".into(),
                data_type: ScalarType::BigInt,
                not_null: true,
            }],
            primary_key: vec![0],
        }],
    }
}
fn arm(expr: &str) -> String {
    format!(
        "SELECT {expr} FROM pg_database WHERE {}",
        ["1=1"; 10].join(" AND ")
    )
}
fn chain(arms: &[String]) -> String {
    arms.join(" UNION ALL ")
}
fn padded(expr: &str, n: usize) -> String {
    chain(&vec![arm(expr); n])
}
fn run(sql: &str, args: &[Value]) -> ResultSet {
    let p = plan(sql).unwrap().expect("catalog plan");
    let fields = describe(&p).unwrap();
    let result = execute(&p, &snapshot(), args).unwrap();
    assert_eq!(result.fields, fields);
    result
}
#[test]
fn exact_comments_contract() {
    // Exact retained client bytes, SHA256
    // 3aa36507c523d1ae91a089f6631485f19d80062e95f40acec38990b33047e438.
    // No modeled object has a comment; grouping empty descriptions yields no rows.
    let p = plan(COMMENTS).unwrap().unwrap();
    assert_eq!(p.parameter_count(), 11);
    assert_eq!(p.parameter_types(), &[PgType::Oid; 11]);
    assert!(p.allows_numeric_input_override());
    assert_eq!(
        describe(&p).unwrap(),
        vec![
            Field {
                name: "id".into(),
                data_type: PgType::Oid
            },
            Field {
                name: "sub_ids".into(),
                data_type: PgType::Int4Array
            },
        ]
    );
    let r = execute(&p, &snapshot(), &vec![Value::Int(2200); 11]).unwrap();
    assert_eq!(r.rows, Vec::<Vec<Value>>::new());
    assert_eq!(r.fields, describe(&p).unwrap());
}
#[test]
fn arm_limits_types_and_first_labels() {
    let r = run(&padded("1 AS first", 16), &[]);
    assert_eq!(r.rows, vec![vec![Value::Int(1)]; 16]);
    assert_eq!(
        r.fields,
        vec![Field {
            name: "first".into(),
            data_type: PgType::Int4
        }]
    );
    assert_eq!(plan(&padded("1", 17)).unwrap_err(), Error::Limit);
    let mut arms = vec![arm("1 AS first"); 10];
    arms.push(arm("2 AS later"));
    let r = run(&chain(&arms), &[]);
    assert_eq!(r.fields[0].name, "first");
    assert_eq!(r.rows.last(), Some(&vec![Value::Int(2)]));
    arms[10] = arm("'text'");
    assert_eq!(plan(&chain(&arms)).unwrap_err(), Error::Type);
    arms[10] = arm("1,2");
    assert_eq!(plan(&chain(&arms)).unwrap_err(), Error::Type);
    // SELECT + FROM + WHERE + IN + '(' = five constructors; all list
    // elements are leaves. 251 '+' tokens bring the first case to exactly256.
    for (n, expected) in [(251, Error::Limit), (252, Error::Limit)] {
        let sql = format!(
            "SELECT 1 FROM pg_database WHERE 1 IN ({})",
            vec!["1"; n + 1].join("+")
        );
        assert_eq!(plan(&sql).unwrap_err(), expected); // depth48 rejects even256
    }
    let sql = format!("SELECT {} FROM pg_database", vec!["1"; 257].join("+"));
    assert_eq!(plan(&sql).unwrap_err(), Error::Limit);
}
#[test]
fn delimiters_quoted_tokens_and_grammar() {
    for (expr, expected) in [
        ("'UNION ALL SELECT' AS \"UNION\"", "UNION ALL SELECT"),
        ("E'UNION ALL'", "UNION ALL"),
        ("'x' /* UNION ALL SELECT */", "x"),
    ] {
        let r = run(&padded(expr, 11), &[]);
        assert_eq!(r.rows.len(), 11);
        assert_eq!(r.rows, vec![vec![Value::Text(expected.into())]; 11]);
    }
    assert_eq!(
        plan(&padded("$$UNION ALL$$", 11)).unwrap_err(),
        Error::Unsupported
    );
    for suffix in [
        " ORDER BY 1",
        " LIMIT 1",
        " OFFSET 0",
        " FETCH FIRST 1 ROW ONLY",
        " FOR UPDATE",
        "; SELECT 1",
        ";",
        ")",
        "]",
    ] {
        assert!(plan(&(padded("1", 11) + suffix)).is_err(), "{suffix}");
    }
    for operator in ["UNION", "UNION DISTINCT", "INTERSECT", "EXCEPT"] {
        let sql = padded("1", 11).replacen("UNION ALL", operator, 1);
        assert_eq!(plan(&sql).unwrap_err(), Error::Unsupported);
    }
    assert!(plan(&("WITH x AS (SELECT 1) ".to_string() + &padded("1", 11))).is_err());
    assert!(plan(&(padded("1", 11) + " UNION ALL DELETE FROM pg_class")).is_err());
    assert!(plan(&padded("(1", 11)).is_err());
    assert!(plan(&padded("ARRAY[1)", 11)).is_err());
    // Existing ordinary grammar still accepts a trailing semicolon/global limit.
    assert_eq!(
        run(
            "SELECT 7 FROM pg_database UNION ALL SELECT 8 FROM pg_database LIMIT 1;",
            &[]
        )
        .rows,
        vec![vec![Value::Int(7)]]
    );
}
#[test]
fn combined_classification_and_parameters() {
    let prefix = padded("1", 10);
    for tail in [
        "SELECT 1 FROM hidden",
        "SELECT (SELECT 1 FROM hidden)",
        "SELECT 1 FROM pg_class JOIN hidden ON true",
    ] {
        assert_eq!(
            plan(&(prefix.clone() + " UNION ALL " + tail)).unwrap_err(),
            Error::MixedRelations
        );
    }
    assert_eq!(
        plan(&(prefix.clone() + " UNION ALL SELECT 1 FROM pg_catalog.unknown")).unwrap_err(),
        Error::Unsupported
    );
    assert!(plan(&padded("(SELECT 1 FROM hidden)", 11)).is_err());
    let sql =
        prefix.clone() + " UNION ALL SELECT 1 FROM pg_database WHERE oid=$11::oid AND oid=$11::oid";
    let p = plan(&sql).unwrap().unwrap();
    assert_eq!(p.parameter_count(), 11);
    assert_eq!(p.parameter_types()[10], PgType::Oid);
    assert_eq!(&p.parameter_types()[..10], &[PgType::Text; 10]);
    assert!(p.allows_numeric_input_override());
    let mut args = vec![Value::Null; 11];
    args[10] = Value::Int(0);
    assert_eq!(
        execute(&p, &snapshot(), &args).unwrap().rows,
        vec![vec![Value::Int(1)]; 10]
    );
    for expr in ["$11::int4", "(SELECT $11::int4)"] {
        let p = plan(&(prefix.clone() + " UNION ALL SELECT " + expr))
            .unwrap()
            .unwrap();
        assert!(!p.allows_numeric_input_override());
        let mut args = vec![Value::Null; 11];
        args[10] = Value::Int(42);
        assert_eq!(
            execute(&p, &snapshot(), &args).unwrap().rows.last(),
            Some(&vec![Value::Int(42)])
        );
    }
    assert_eq!(
        plan(&(prefix.clone() + " UNION ALL SELECT $257::int8")).unwrap_err(),
        Error::Parameters
    );
    assert_eq!(
        plan(&(prefix.clone() + " UNION ALL SELECT $0::int8")).unwrap_err(),
        Error::Parameters
    );
    let sql = padded("1", 10)
        + " UNION ALL SELECT 1 FROM pg_database WHERE $1::bool UNION ALL SELECT 1 FROM pg_database WHERE $1::oid=1";
    assert_eq!(plan(&sql).unwrap_err(), Error::Type);
    let p = plan(&padded("$1::int8", 11)).unwrap().unwrap();
    assert_eq!(p.parameter_count(), 1);
    assert_eq!(
        execute(&p, &snapshot(), &[Value::Int(9)]).unwrap().rows,
        vec![vec![Value::Int(9)]; 11]
    );
    assert_eq!(
        execute(&p, &snapshot(), &[]).unwrap_err(),
        Error::Parameters
    );
}
#[test]
fn whole_input_and_combined_result_bounds() {
    let base = padded("1", 11);
    let mut sql = base.clone();
    sql.push_str(&" ".repeat(MAX_SQL - sql.len()));
    assert_eq!(run(&sql, &[]).rows, vec![vec![Value::Int(1)]; 11]);
    sql.push(' ');
    assert_eq!(plan(&sql).unwrap_err(), Error::Limit);
    let leaves = vec!["1"; 4096].join(",");
    assert_eq!(
        plan(&(base.clone() + " UNION ALL SELECT 1 WHERE 1 IN (" + &leaves + ")")).unwrap_err(),
        Error::Limit
    );
    // Each branch is within1025 series rows; all arms share final8192 admission.
    let series = |end| {
        format!(
            "SELECT generate_series FROM generate_series(1,{end}) CROSS JOIN pg_database WHERE {}",
            ["1=1"; 10].join(" AND ")
        )
    };
    let sql = chain(&vec![series(1024); 8]);
    assert_eq!(run(&sql, &[]).rows.len(), MAX_ROWS);
    let p = plan(&chain(&vec![series(1025); 8])).unwrap().unwrap();
    assert_eq!(execute(&p, &snapshot(), &[]).unwrap_err(), Error::Limit);
    // Text accounting=2*len+8; eight1024-row arms at512units/row=4MiB.
    let sized = |n| {
        series(1024).replacen(
            "SELECT generate_series",
            &format!("SELECT '{}'", "x".repeat(n)),
            1,
        )
    };
    let r = run(&chain(&vec![sized(252); 8]), &[]);
    assert_eq!(r.rows.len(), 8192);
    assert_eq!(r.rows[8191], vec![Value::Text("x".repeat(252))]);
    let p = plan(&chain(&vec![sized(253); 8])).unwrap().unwrap();
    assert_eq!(execute(&p, &snapshot(), &[]).unwrap_err(), Error::Limit);
}
#[test]
fn aggregate_compile_budgets() {
    // 127 additions and aliases plus SELECT/FROM =256 constructors.
    let single = format!(
        "SELECT {} FROM pg_database",
        vec!["1+1 AS x"; 127].join(",")
    );
    let r = run(&single, &[]);
    assert_eq!(r.rows, vec![vec![Value::Int(2); 127]]);
    assert_eq!(
        plan(&(single.clone() + " WHERE 1")).unwrap_err(),
        Error::Limit
    );
    assert_eq!(
        run(&(single.clone() + " UNION ALL " + &single), &[]).rows,
        vec![vec![Value::Int(2); 127]; 2]
    );
    let mut arms = vec![arm("(SELECT 1)"); 15];
    arms.push(arm("1"));
    assert_eq!(run(&chain(&arms), &[]).rows, vec![vec![Value::Int(1)]; 16]);
    arms[15] = arm("(SELECT 1)");
    assert_eq!(plan(&chain(&arms)).unwrap_err(), Error::Limit); // wrapper+32queries
    let joined = format!(
        "SELECT 1 FROM pg_database a CROSS JOIN pg_database b WHERE {}",
        ["1=1"; 10].join(" AND ")
    );
    let mut arms = vec![joined.clone(); 15];
    arms.push(arm("1"));
    assert_eq!(run(&chain(&arms), &[]).rows, vec![vec![Value::Int(1)]; 16]);
    arms[15] = joined;
    assert_eq!(plan(&chain(&arms)).unwrap_err(), Error::Limit); // wrapper+32sources
    let nodes = |n| {
        format!(
            "SELECT ARRAY[{}] FROM pg_database WHERE {}true",
            vec!["1"; n].join(","),
            "NOT ".repeat(14)
        )
    };
    let r = run(&chain(&vec![nodes(240); 16]), &[]); //16*(241+15)=4096 ASTexprs
    assert_eq!(
        r.rows,
        vec![vec![Value::Array(vec![Value::Int(1); 240])]; 16]
    );
    assert_eq!(
        plan(&chain(&vec![nodes(241); 16])).unwrap_err(),
        Error::Limit
    );
}
#[test]
fn bounded_nested_arm_child() {
    if std::env::var_os("GIGGLES_FLAT_NESTED_CHILD").is_none() {
        return;
    }
    let arm = format!(
        "SELECT ARRAY[{}] FROM pg_database WHERE {}true",
        vec!["1"; 214].join(","),
        "NOT ".repeat(40)
    );
    assert_eq!(plan(&chain(&vec![arm; 16])).unwrap_err(), Error::Limit);
}
#[test]
fn shared_work_is_not_reset_between_arms() {
    let mut arms = vec![arm("1").replace("WHERE 1=1", "WHERE 1=0"); 10];
    let heavy = format!(
        "SELECT 1 FROM generate_series(1,550) a JOIN generate_series(1,550) b ON false WHERE {}",
        ["1=1"; 10].join(" AND ")
    );
    arms.extend([heavy.clone(), heavy.clone()]);
    assert_eq!(run(&chain(&arms), &[]).rows, Vec::<Vec<Value>>::new());
    arms.push(heavy);
    let p = plan(&chain(&arms)).unwrap().unwrap();
    assert_eq!(execute(&p, &snapshot(), &[]).unwrap_err(), Error::Limit);
}
#[test]
fn projection_parameter_token_and_prefix_edges() {
    let wide = |n| format!("SELECT {} FROM pg_database", vec!["1 AS a"; n].join(","));
    assert_eq!(
        run(&(wide(128) + " UNION ALL " + &wide(128)), &[]).rows,
        vec![vec![Value::Int(1); 128]; 2]
    );
    assert_eq!(
        plan(&(wide(129) + " UNION ALL " + &wide(129))).unwrap_err(),
        Error::Limit
    );
    let sql = padded("1", 10) + " UNION ALL SELECT $256::int4";
    let p = plan(&sql).unwrap().unwrap();
    assert_eq!(p.parameter_count(), 256);
    let mut args = vec![Value::Null; 256];
    args[255] = Value::Int(42);
    assert_eq!(
        execute(&p, &snapshot(), &args).unwrap().rows.last(),
        Some(&vec![Value::Int(42)])
    );
    let expr = format!("{}true", "NOT ".repeat(16));
    assert_eq!(
        run(&padded(&expr, 11), &[]).rows,
        vec![vec![Value::Bool(true)]; 11]
    );
    assert_eq!(
        plan(&padded(&("NOT ".to_string() + &expr), 11)).unwrap_err(),
        Error::Limit
    );
    use sqlparser::{
        dialect::PostgreSqlDialect,
        tokenizer::{Token, Tokenizer},
    };
    let tokens = |sql: &str| {
        Tokenizer::new(&PostgreSqlDialect {}, sql)
            .tokenize()
            .unwrap()
            .into_iter()
            .filter(|t| !matches!(t, Token::Whitespace(_) | Token::EOF))
            .count()
    };
    let sql = format!("SELECT {} FROM pg_database", vec!["1"; 4095].join(","));
    assert_eq!(tokens(&sql), 8192);
    assert_eq!(plan(&sql).unwrap_err(), Error::Limit); // projection cap on ordinary path
    let sql = sql + ";";
    assert_eq!(tokens(&sql), 8193);
    assert_eq!(plan(&sql).unwrap_err(), Error::Limit); // whole-input token cap
}
#[test]
fn bounded_nested_arm_subprocess() {
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "bounded_nested_arm_child", "--nocapture"])
        .env("GIGGLES_FLAT_NESTED_CHILD", "1")
        .status()
        .unwrap();
    assert!(status.success(), "bounded nested parser child {status}");
}
#[test]
fn flat_parser_child() {
    if std::env::var_os("GIGGLES_FLAT_CHILD").is_none() {
        return;
    }
    let prefix = padded("1", 10);
    for expr in [
        format!("{}1", "(".repeat(4000)),
        vec!["1"; 12000].join("+"),
        format!("1{}", "::int8".repeat(4000)),
    ] {
        assert_eq!(
            plan(&(prefix.clone() + " UNION ALL SELECT " + &expr)).unwrap_err(),
            Error::Limit
        );
    }
    for _ in 0..100 {
        let p = plan(&padded("1", 16)).unwrap().unwrap();
        assert_eq!(describe(&p).unwrap()[0].data_type, PgType::Int4);
        drop(p);
    }
}
#[test]
fn flat_parser_drop_subprocess() {
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "flat_parser_child", "--nocapture"])
        .env("GIGGLES_FLAT_CHILD", "1")
        .status()
        .unwrap();
    assert!(status.success(), "flat parser child {status}");
}
