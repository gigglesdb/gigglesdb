use giggles_core::{
    Admin, CoreError, Limits, SqlSession,
    relational::{Numeric, SqlError, Value},
};

struct Db {
    admin: Option<Admin>,
    path: std::path::PathBuf,
}
impl Db {
    fn new() -> Self {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let path = std::env::temp_dir()
            .join("numeric-column-typmod")
            .join(format!("{:032x}", u128::from_le_bytes(nonce)));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        Self {
            admin: Some(Admin::create(&path, Limits::default()).unwrap()),
            path,
        }
    }
    fn admin(&self) -> &Admin {
        self.admin.as_ref().unwrap()
    }
    fn reopen(&mut self) {
        drop(self.admin.take());
        self.admin = Some(Admin::open(&self.path, Limits::default()).unwrap());
    }
}
impl Drop for Db {
    fn drop(&mut self) {
        drop(self.admin.take());
        std::fs::remove_dir_all(&self.path).unwrap();
    }
}
fn run(s: &mut SqlSession, sql: &str) -> Vec<Vec<Value>> {
    s.execute(sql, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .rows
        .into_iter()
        .map(|r| r.values)
        .collect()
}
fn numeric_text(s: &mut SqlSession, sql: &str) -> Vec<Option<String>> {
    run(s, sql)
        .into_iter()
        .map(|r| match &r[0] {
            Value::Numeric(n) => Some(n.to_string()),
            Value::Null => None,
            v => panic!("expected numeric: {v:?}"),
        })
        .collect()
}
fn fails(s: &mut SqlSession, sql: &str, error: SqlError) {
    assert_eq!(
        s.execute(sql, &[], None),
        Err(CoreError::Sql(error)),
        "{sql}"
    );
}

#[test]
fn exact_numeric_precision_corpus_and_write_overflow() {
    let db = Db::new();
    let mut s = db.admin().sql_session();
    let corpus = include_str!("../../../compat/corpus/known_gaps/numeric_precision.sql");
    let sql = corpus
        .lines()
        .filter(|l| !l.starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");
    for statement in sql.split(';').filter(|s| !s.trim().is_empty()) {
        run(&mut s, statement);
    }
    assert_eq!(
        numeric_text(
            &mut s,
            "SELECT value FROM r0c_numeric_precision_t ORDER BY value"
        ),
        vec![Some("1.01".into()), Some("12345678.90".into())]
    );
    for value in ["123456789.0", "99999999.995"] {
        fails(
            &mut s,
            &format!("INSERT INTO r0c_numeric_precision_t VALUES (3,{value})"),
            SqlError::Overflow,
        );
        fails(
            &mut s,
            &format!("UPDATE r0c_numeric_precision_t SET value={value} WHERE id=1"),
            SqlError::Overflow,
        );
    }
    assert_eq!(
        numeric_text(
            &mut s,
            "SELECT value FROM r0c_numeric_precision_t WHERE id=1"
        ),
        vec![Some("1.01".into())]
    );
    run(
        &mut s,
        "UPDATE r0c_numeric_precision_t SET value=2.005 WHERE id=1",
    );
    assert_eq!(
        numeric_text(
            &mut s,
            "SELECT value FROM r0c_numeric_precision_t WHERE id=1"
        ),
        vec![Some("2.01".into())]
    );
}

#[test]
fn numeric_modifier_variants_and_invalid_declarations() {
    let db = Db::new();
    let mut s = db.admin().sql_session();
    for (ty, value, expected) in [
        ("numeric(3)", "12.5", "13"),
        ("decimal(4,2)", "1.005", "1.01"),
        ("numeric(2,-3)", "12500", "13000"),
        ("numeric(3,5)", "0.001235", "0.00124"),
    ] {
        run(
            &mut s,
            &format!("CREATE TABLE t (id int PRIMARY KEY, v {ty})"),
        );
        run(&mut s, &format!("INSERT INTO t VALUES (1,{value})"));
        assert_eq!(
            numeric_text(&mut s, "SELECT v FROM t"),
            vec![Some(expected.into())]
        );
        run(&mut s, "DROP TABLE t");
    }
    for modifier in ["0", "1001", "2,1001", "2,-1001", "1,2,3", "1,2,3,4"] {
        fails(
            &mut s,
            &format!("CREATE TABLE invalid (v numeric({modifier}))"),
            SqlError::InvalidParameterValue,
        );
    }
    for modifier in ["", "1,", "1,2,", "1,2,3,"] {
        fails(
            &mut s,
            &format!("CREATE TABLE invalid (v numeric({modifier}))"),
            SqlError::Syntax,
        );
    }
    run(&mut s, "CREATE TABLE altered (id int PRIMARY KEY)");
    for modifier in ["1,2,3", "1,2,3,4"] {
        fails(
            &mut s,
            &format!("ALTER TABLE altered ADD COLUMN v numeric({modifier})"),
            SqlError::InvalidParameterValue,
        );
    }
    for modifier in ["", "1,", "1,2,3,"] {
        fails(
            &mut s,
            &format!("ALTER TABLE altered ADD COLUMN v numeric({modifier})"),
            SqlError::Syntax,
        );
    }
    fails(&mut s, "CREATE TABLE invalid (v integer(2))", SqlError::Syntax);
}

#[test]
fn defaults_parameters_alter_catalog_and_reopen() {
    let mut db = Db::new();
    let mut s = db.admin().sql_session();
    run(
        &mut s,
        "CREATE TABLE t (id int PRIMARY KEY, v numeric(4,2) DEFAULT 1.005)",
    );
    run(&mut s, "INSERT INTO t (id) VALUES (1)");
    run(&mut s, "INSERT INTO t VALUES (2,NULL)");
    let insert = s.prepare("INSERT INTO t VALUES ($1,$2)").unwrap();
    s.execute_prepared(
        &insert,
        &[
            Value::Int(3),
            Value::Numeric(Numeric::parse("2.005").unwrap()),
        ],
        None,
    )
    .unwrap();
    assert_eq!(
        s.execute_prepared(
            &insert,
            &[
                Value::Int(4),
                Value::Numeric(Numeric::parse("99.995").unwrap())
            ],
            None
        ),
        Err(CoreError::Sql(SqlError::Overflow))
    );
    let update = s.prepare("UPDATE t SET v=$1 WHERE id=$2").unwrap();
    s.execute_prepared(
        &update,
        &[
            Value::Numeric(Numeric::parse("3.005").unwrap()),
            Value::Int(3),
        ],
        None,
    )
    .unwrap();
    run(
        &mut s,
        "ALTER TABLE t ADD COLUMN added numeric(4,2) DEFAULT 4.005",
    );
    run(&mut s, "INSERT INTO t VALUES (4,DEFAULT,5.005)");
    fails(
        &mut s,
        "UPDATE t SET added=99.995 WHERE id=4",
        SqlError::Overflow,
    );
    assert_eq!(
        numeric_text(&mut s, "SELECT v FROM t ORDER BY id"),
        vec![
            Some("1.01".into()),
            None,
            Some("3.01".into()),
            Some("1.01".into())
        ]
    );
    assert_eq!(
        numeric_text(&mut s, "SELECT added FROM t WHERE id=4"),
        vec![Some("5.01".into())]
    );
    let typmods = db.admin().catalog_typmods().unwrap();
    let columns = &db.admin().catalog().unwrap().1[0].columns;
    let expected = giggles_pg_types::typmod::NumericTypmod::new(4, 2)
        .unwrap()
        .to_typmod();
    assert_eq!(typmods.1.get(&columns[1].id), Some(&expected));
    assert_eq!(typmods.1.get(&columns[2].id), Some(&expected));
    drop(s);
    db.reopen();
    assert_eq!(db.admin().catalog_typmods().unwrap(), typmods);
    let mut s = db.admin().sql_session();
    assert_eq!(
        numeric_text(&mut s, "SELECT v FROM t WHERE id=3"),
        vec![Some("3.01".into())]
    );
    run(&mut s, "UPDATE t SET added=6.005 WHERE id=4");
    assert_eq!(
        numeric_text(&mut s, "SELECT added FROM t WHERE id=4"),
        vec![Some("6.01".into())]
    );
    fails(&mut s, "INSERT INTO t VALUES (5,100,1)", SqlError::Overflow);
}
