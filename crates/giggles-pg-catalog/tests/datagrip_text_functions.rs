use giggles_pg_catalog::*;

fn run(sql: &str, args: &[Value]) -> ResultSet {
    let p = plan(sql).unwrap().unwrap();
    execute(&p, &CatalogSnapshot::default(), args).unwrap()
}

#[test]
fn direct_text_semantics() {
    for (sql, expected) in [
        ("SELECT upper('tom')", "TOM"),
        // This profile uses Rust Unicode mapping, independent of PG locales.
        ("SELECT upper('Straße éıσ')", "STRASSE ÉIΣ"),
        ("SELECT upper('')", ""),
        ("SELECT translate('12345', '143', 'ax')", "a2x5"),
        ("SELECT translate('é猫🙂é', 'é猫', '🙂x')", "🙂x🙂🙂"),
        ("SELECT translate('abca', 'aabc', 'XYZ')", "XZX"),
        ("SELECT translate('abc', 'abc', '')", ""),
        ("SELECT translate('abc', '', 'longer')", "abc"),
        ("SELECT translate('abc', 'ab', 'XYZextra')", "XYc"),
        ("SELECT translate('', 'abc', 'xyz')", ""),
        ("SELECT upper('abc'::name)", "ABC"),
        ("SELECT upper('a'::\"char\")", "A"),
        ("SELECT translate('a'::\"char\", 'a'::name, 'b')", "b"),
    ] {
        let r = run(sql, &[]);
        assert_eq!(r.fields[0].data_type, PgType::Text, "{sql}");
        assert_eq!(r.rows, vec![vec![Value::Text(expected.into())]], "{sql}");
    }
}

#[test]
fn nulls_and_rejections() {
    for sql in [
        "SELECT upper(NULL)",
        "SELECT translate(NULL, 'a', 'b')",
        "SELECT translate('a', NULL, 'b')",
        "SELECT translate('a', 'b', NULL)",
    ] {
        assert_eq!(run(sql, &[]).rows, vec![vec![Value::Null]]);
    }
    for sql in [
        "SELECT upper()",
        "SELECT upper('a', 'b')",
        "SELECT translate('a', 'b')",
        "SELECT translate('a', 'b', 'c', 'd')",
    ] {
        assert!(matches!(plan(sql), Err(Error::Unsupported)), "{sql}");
    }
    for sql in [
        "SELECT upper(1)",
        "SELECT upper(true)",
        "SELECT translate('a', 1, 'b')",
        "SELECT translate('a', 'b', false)",
        "SELECT upper(ARRAY['a'])",
    ] {
        assert!(matches!(plan(sql), Err(Error::Type)), "{sql}");
    }
}

#[test]
fn prepared_metadata_and_immutable_inputs() {
    let p = plan("SELECT upper($1), translate($2, $3, $4)")
        .unwrap()
        .unwrap();
    assert_eq!(p.parameter_types(), &[PgType::Text; 4]);
    assert_eq!(
        describe(&p)
            .unwrap()
            .iter()
            .map(|f| f.data_type)
            .collect::<Vec<_>>(),
        vec![PgType::Text; 2]
    );
    let args = ["hé", "abca", "aabc", "XYZ"].map(|v| Value::Text(v.into()));
    let original = args.clone();
    for _ in 0..2 {
        assert_eq!(
            execute(&p, &CatalogSnapshot::default(), &args)
                .unwrap()
                .rows,
            vec![vec![Value::Text("HÉ".into()), Value::Text("XZX".into())]]
        );
        assert_eq!(args, original);
    }
    assert_eq!(
        execute(&p, &CatalogSnapshot::default(), &[]),
        Err(Error::Parameters)
    );
    let p = plan("SELECT upper($1::name), translate($2::\"char\", 'a', 'z')")
        .unwrap()
        .unwrap();
    assert_eq!(p.parameter_types(), &[PgType::Name, PgType::Char]);
    assert_eq!(
        execute(
            &p,
            &CatalogSnapshot::default(),
            &[Value::Text("ab".into()), Value::Text("a".into())]
        )
        .unwrap()
        .rows,
        vec![vec![Value::Text("AB".into()), Value::Text("z".into())]]
    );
}

#[test]
fn exact_byte_bound_remaining_budget_and_work_exhaustion() {
    let p = plan("SELECT translate($1, '', '')").unwrap().unwrap();
    // Text accounting is 2*UTF8+8, including both empty call arguments.
    // These three arguments together are exactly MAX_BYTES.
    let input = "🙂".repeat((MAX_BYTES - 24) / 8);
    let args = [Value::Text(input.clone())];
    assert_eq!(
        execute(&p, &CatalogSnapshot::default(), &args)
            .unwrap()
            .rows,
        vec![vec![Value::Text(input.clone())]]
    );
    let p = plan("SELECT 'abcde', translate($1, '', '')")
        .unwrap()
        .unwrap();
    assert_eq!(
        execute(&p, &CatalogSnapshot::default(), &args),
        Err(Error::Limit)
    );
    let p = plan("SELECT $1, upper('ΐ')").unwrap().unwrap();
    // Input fits remaining=16; its expanded Unicode result requires 20.
    assert_eq!(
        execute(&p, &CatalogSnapshot::default(), &args),
        Err(Error::Limit)
    );
    let p = plan("SELECT translate($1, '', '')").unwrap().unwrap();
    assert_eq!(
        execute(
            &p,
            &CatalogSnapshot::default(),
            &[Value::Text(format!("{input}a"))]
        ),
        Err(Error::Limit)
    );
    assert_eq!(args, [Value::Text(input)]);
    let p = plan("SELECT upper($1)").unwrap().unwrap();
    assert_eq!(
        execute(
            &p,
            &CatalogSnapshot::default(),
            &[Value::Text("a".repeat(MAX_WORK / 4))]
        ),
        Err(Error::Limit)
    );
    let p = plan("SELECT translate($1, $2, '')").unwrap().unwrap();
    assert_eq!(
        execute(
            &p,
            &CatalogSnapshot::default(),
            &[Value::Text("z".repeat(2000)), Value::Text("a".repeat(1000))]
        ),
        Err(Error::Limit)
    );
}
