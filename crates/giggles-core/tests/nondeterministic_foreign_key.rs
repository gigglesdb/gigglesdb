mod common;
use giggles_core::{
    Admin, CoreError, Engine, Limits, SqlSession,
    relational::{SqlError as E, Value as V},
};
fn engines() -> Vec<Engine> {
    [
        Engine::Legacy,
        #[cfg(feature = "engine-rocksdb")]
        Engine::V2,
    ]
    .to_vec()
}
fn run(s: &mut SqlSession, sql: &str) -> Vec<Vec<V>> {
    s.execute(sql, &[], None)
        .unwrap_or_else(|e| {
            panic!(
                "{sql}: {e:?}, {:?}, {:?}",
                giggles_core::relational::take_sql_error_message(),
                giggles_core::relational::take_sql_error_detail()
            )
        })
        .rows
        .into_iter()
        .map(|r| r.values)
        .collect()
}
fn fail(s: &mut SqlSession, sql: &str, error: E, message: &str) {
    assert_eq!(
        s.execute(sql, &[], None),
        Err(CoreError::Sql(error)),
        "{sql}"
    );
    assert_eq!(
        giggles_core::relational::take_sql_error_message().as_deref(),
        Some(message),
        "{sql}"
    );
}
fn setup(s: &mut SqlSession) {
    run(
        s,
        "CREATE COLLATION ci (provider=icu,locale='und-u-ks-level2',deterministic=false)",
    );
    run(
        s,
        "CREATE COLLATION nd (provider=icu,locale='und',deterministic=false)",
    );
}

#[test]
fn incompatible_collations_and_same_collation_lookup_restart() {
    for engine in engines() {
        let path = common::fresh("nd-fk");
        {
            let a = Admin::create_engine(&path, Limits::default(), engine).unwrap();
            let mut s = a.sql_session();
            setup(&mut s);
            run(
                &mut s,
                "CREATE COLLATION ci2 (provider=icu,locale='und-u-ks-level2',deterministic=false)",
            );
            run(&mut s, "CREATE TABLE p(k text COLLATE ci2 PRIMARY KEY)");
            run(&mut s, "INSERT INTO p VALUES('xyz')");
            fail(
                &mut s,
                "CREATE TABLE f3(id int PRIMARY KEY,k text REFERENCES p(k))",
                E::CollationMismatch,
                "foreign key constraint \"f3_k_fkey\" cannot be implemented",
            );
            assert_eq!(
                giggles_core::relational::take_sql_error_detail().as_deref(),
                Some(
                    "Key columns \"k\" of the referencing table and \"k\" of the referenced table have incompatible collations: \"default\" and \"ci2\".  If either collation is nondeterministic, then both collations have to be the same."
                )
            );
            fail(
                &mut s,
                "CREATE TABLE f4(id int PRIMARY KEY,k text COLLATE ci REFERENCES p(k))",
                E::CollationMismatch,
                "foreign key constraint \"f4_k_fkey\" cannot be implemented",
            );
            run(
                &mut s,
                "CREATE TABLE f1(id int PRIMARY KEY,k text COLLATE ci2 REFERENCES p(k))",
            );
            run(&mut s, "INSERT INTO f1 VALUES(1,'XYZ')");
            assert_eq!(
                s.execute("INSERT INTO f1 VALUES(2,'nope')", &[], None),
                Err(CoreError::Sql(E::ForeignKeyViolation))
            );
            assert_eq!(
                s.execute("DELETE FROM p WHERE k='xyz'", &[], None),
                Err(CoreError::Sql(E::ForeignKeyViolation))
            );
            run(
                &mut s,
                "CREATE TABLE pc(id int PRIMARY KEY,k text COLLATE ci2 UNIQUE)",
            );
            run(&mut s, "INSERT INTO pc VALUES(1,'xyz')");
            run(
                &mut s,
                "CREATE TABLE fc(id int PRIMARY KEY,k text COLLATE ci2 REFERENCES pc(k) ON DELETE CASCADE)",
            );
            run(&mut s, "INSERT INTO fc VALUES(1,'XYZ')");
            run(&mut s, "DELETE FROM pc WHERE id=1");
            assert_eq!(
                run(&mut s, "SELECT count(*) FROM fc"),
                vec![vec![V::Int(0)]]
            );
        }
        let a = Admin::open(&path, Limits::default()).unwrap();
        let mut s = a.sql_session();
        run(&mut s, "INSERT INTO f1 VALUES(3,'XyZ')");
        assert_eq!(
            s.execute("INSERT INTO f1 VALUES(4,'missing')", &[], None),
            Err(CoreError::Sql(E::ForeignKeyViolation))
        );
    }
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn fixed_snapshot_ignores_changes_to_unrelated_collated_parents() {
    let a = Admin::create_engine(
        common::fresh("nd-fk-unrelated"),
        Limits::default(),
        Engine::V2,
    )
    .unwrap();
    let mut current = a.sql_session();
    setup(&mut current);
    run(
        &mut current,
        "CREATE TABLE p(k text COLLATE ci PRIMARY KEY)",
    );
    run(
        &mut current,
        "CREATE TABLE c(id int PRIMARY KEY,k text COLLATE ci REFERENCES p(k))",
    );
    run(&mut current, "INSERT INTO p VALUES('abc'),('old')");
    let mut old = a.sql_session();
    run(&mut old, "BEGIN");
    old.set_transaction_isolation(giggles_core::SqlIsolation::RepeatableRead)
        .unwrap();
    assert_eq!(
        run(&mut old, "SELECT count(*) FROM c"),
        vec![vec![V::Int(0)]]
    );
    run(&mut current, "UPDATE p SET k='new' WHERE k='old'");
    run(&mut old, "INSERT INTO c VALUES(1,'ABC')");
    run(&mut old, "COMMIT");
    assert_eq!(
        run(&mut current, "SELECT k FROM c"),
        vec![vec![V::Text("ABC".into())]]
    );
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn on_conflict_referenced_key_actions_match_collation_equal_children() {
    for (collation, original, equal) in [("ci", "abc", "ABC"), ("nd", "\u{e9}", "e\u{301}")] {
        let safe = format!("{original}safe");
        let safe_equal = format!("{equal}safe");
        for action in [
            "NO ACTION",
            "RESTRICT",
            "CASCADE",
            "SET NULL",
            "SET DEFAULT",
        ] {
            let a = Admin::create_engine(
                common::fresh("nd-conflict-fk-actions"),
                Limits::default(),
                Engine::V2,
            )
            .unwrap();
            let mut s = a.sql_session();
            setup(&mut s);
            run(
                &mut s,
                &format!(
                    "CREATE TABLE p(id int PRIMARY KEY,k text COLLATE {collation} UNIQUE,payload text)"
                ),
            );
            run(
                &mut s,
                "CREATE TABLE src(id int PRIMARY KEY,k text,payload text)",
            );
            let parents = (0..401)
                .map(|i| {
                    format!(
                        "({i},'{}')",
                        if i == 0 {
                            original.into()
                        } else {
                            format!("key{i}")
                        }
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            run(
                &mut s,
                &format!("INSERT INTO p(id,k) VALUES {parents},(999,'{safe}')"),
            );
            run(
                &mut s,
                &format!(
                    "CREATE TABLE c(id int PRIMARY KEY,k text COLLATE {collation} DEFAULT '{safe_equal}' REFERENCES p(k) ON UPDATE {action})"
                ),
            );
            run(&mut s, "CREATE INDEX ck ON c(k)");
            let unrelated = (0..201)
                .map(|i| format!("({i},'{safe}')"))
                .collect::<Vec<_>>()
                .join(",");
            run(
                &mut s,
                &format!("INSERT INTO c VALUES {unrelated},(400,'{equal}')"),
            );
            // Page 1 removes the old key; page 3 restores an ICU-equal spelling.
            let input = (0..401)
                .map(|i| {
                    format!(
                        "({i},'{}','x')",
                        if i == 0 {
                            "moved".into()
                        } else if i == 400 {
                            original.into()
                        } else {
                            format!("key{i}")
                        }
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            run(&mut s, &format!("INSERT INTO src VALUES {input}"));
            run(&mut s, "CREATE VIEW input AS SELECT id,k,payload FROM src");
            let sql = "INSERT INTO p SELECT id,k,repeat(payload,4000) FROM input ORDER BY id ON CONFLICT(id) DO UPDATE SET k=excluded.k";
            if action == "RESTRICT" {
                assert_eq!(
                    s.execute(sql, &[], None),
                    Err(CoreError::Sql(E::RestrictViolation))
                );
                assert_eq!(
                    run(&mut s, "SELECT k FROM p WHERE id=0"),
                    vec![vec![V::Text(original.into())]]
                );
                assert_eq!(
                    run(&mut s, "SELECT k FROM c WHERE id=400"),
                    vec![vec![V::Text(equal.into())]]
                );
                continue;
            }
            assert_eq!(
                s.execute(sql, &[], None)
                    .unwrap_or_else(|e| panic!("{collation}: {action}: {e:?}"))
                    .affected,
                401,
                "{collation}: {action}"
            );
            let expected = match action {
                "CASCADE" => V::Text("moved".into()),
                "SET NULL" => V::Null,
                "SET DEFAULT" => V::Text(safe_equal.clone()),
                _ => V::Text(equal.into()),
            };
            assert_eq!(
                run(&mut s, "SELECT k FROM c WHERE id=400"),
                vec![vec![expected]]
            );
            assert_eq!(
                run(
                    &mut s,
                    &format!("SELECT count(*) FROM c WHERE id<400 AND k='{safe}'")
                ),
                vec![vec![V::Int(201)]]
            );
            if action == "NO ACTION" {
                // No final collation-equal owner: fail at statement end and undo pages.
                assert_eq!(s.execute(
                    "INSERT INTO p SELECT id,'gone' || id,repeat(payload,4000) FROM input ORDER BY id ON CONFLICT(id) DO UPDATE SET k=excluded.k",
                    &[], None,
                ), Err(CoreError::Sql(E::ForeignKeyViolation)));
                assert_eq!(
                    run(&mut s, "SELECT k FROM p WHERE id=400"),
                    vec![vec![V::Text(original.into())]]
                );
                run(&mut s, "DROP TABLE c");
                run(
                    &mut s,
                    &format!(
                        "CREATE TABLE dc(k text COLLATE {collation} REFERENCES p(k) DEFERRABLE INITIALLY DEFERRED)"
                    ),
                );
                run(&mut s, &format!("INSERT INTO dc VALUES('{equal}')"));
                let missing = "INSERT INTO p SELECT id,'gone' || id,repeat(payload,4000) FROM input ORDER BY id ON CONFLICT(id) DO UPDATE SET k=excluded.k";
                run(&mut s, "BEGIN");
                assert_eq!(s.execute(missing, &[], None).unwrap().affected, 401);
                assert_eq!(
                    s.execute("SET CONSTRAINTS ALL IMMEDIATE", &[], None),
                    Err(CoreError::Sql(E::ForeignKeyViolation))
                );
                run(&mut s, "ROLLBACK");
                run(&mut s, "BEGIN");
                assert_eq!(s.execute(missing, &[], None).unwrap().affected, 401);
                run(
                    &mut s,
                    &format!("INSERT INTO p(id,k) VALUES(1001,'{original}')"),
                );
                run(&mut s, "COMMIT");
            }
        }
    }
}
