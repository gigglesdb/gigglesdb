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
fn primary_upsert_update_and_restart() {
    for engine in engines() {
        let path = common::fresh("nd-unique-primary");
        {
            let a = Admin::create_engine(&path, Limits::default(), engine).unwrap();
            let mut s = a.sql_session();
            setup(&mut s);
            run(
                &mut s,
                "CREATE TABLE u1(k text COLLATE ci PRIMARY KEY,n int)",
            );
            run(&mut s, "INSERT INTO u1 VALUES('abc',1)");
            fail(
                &mut s,
                "INSERT INTO u1 VALUES('abc',2)",
                E::Duplicate,
                "duplicate key value violates unique constraint \"u1_pkey\"",
            );
            fail(
                &mut s,
                "INSERT INTO u1 VALUES('ABC',2)",
                E::Duplicate,
                "duplicate key value violates unique constraint \"u1_pkey\"",
            );
            assert_eq!(
                s.execute(
                    "INSERT INTO u1 VALUES('ABC',3) ON CONFLICT DO NOTHING",
                    &[],
                    None
                )
                .unwrap()
                .affected,
                0
            );
            assert_eq!(
                run(
                    &mut s,
                    "INSERT INTO u1 VALUES('Abc',4) ON CONFLICT(k) DO UPDATE SET n=excluded.n RETURNING k,n"
                ),
                vec![vec![V::Text("abc".into()), V::Int(4)]]
            );
            assert_eq!(
                run(&mut s, "SELECT * FROM u1 WHERE k='ABC'"),
                vec![vec![V::Text("abc".into()), V::Int(4)]]
            );
            run(&mut s, "UPDATE u1 SET k='xyz'");
            fail(
                &mut s,
                "INSERT INTO u1 VALUES('XYZ',5)",
                E::Duplicate,
                "duplicate key value violates unique constraint \"u1_pkey\"",
            );
        }
        let a = Admin::open(&path, Limits::default()).unwrap();
        let mut s = a.sql_session();
        fail(
            &mut s,
            "INSERT INTO u1 VALUES('XyZ',6)",
            E::Duplicate,
            "duplicate key value violates unique constraint \"u1_pkey\"",
        );
        run(&mut s, "INSERT INTO u1 VALUES('abc',7)");
        fail(
            &mut s,
            "UPDATE u1 SET k='xyz' WHERE k='abc'",
            E::Duplicate,
            "duplicate key value violates unique constraint \"u1_pkey\"",
        );
    }
}
#[test]
fn canonical_equivalence_unique_build_and_restart() {
    for engine in engines() {
        let path = common::fresh("nd-unique-index");
        {
            let a = Admin::create_engine(&path, Limits::default(), engine).unwrap();
            let mut s = a.sql_session();
            setup(&mut s);
            run(
                &mut s,
                "CREATE TABLE u2(id int PRIMARY KEY,v text COLLATE nd)",
            );
            run(&mut s, "INSERT INTO u2 VALUES(1,'\u{e9}'),(2,'e\u{301}')");
            fail(
                &mut s,
                "CREATE UNIQUE INDEX u2_v ON u2(v)",
                E::Duplicate,
                "could not create unique index \"u2_v\"",
            );
            run(&mut s, "DELETE FROM u2 WHERE id=2");
            run(&mut s, "CREATE UNIQUE INDEX u2_v ON u2(v)");
            fail(
                &mut s,
                "INSERT INTO u2 VALUES(3,'e\u{301}')",
                E::Duplicate,
                "duplicate key value violates unique constraint \"u2_v\"",
            );
            run(&mut s, "INSERT INTO u2 VALUES(4,'E')");
            assert_eq!(
                run(
                    &mut s,
                    "INSERT INTO u2 VALUES(5,'e\u{301}') ON CONFLICT(v) DO UPDATE SET id=excluded.id RETURNING id,v"
                ),
                vec![vec![V::Int(5), V::Text("\u{e9}".into())]]
            );
        }
        let a = Admin::open(&path, Limits::default()).unwrap();
        let mut s = a.sql_session();
        fail(
            &mut s,
            "INSERT INTO u2 VALUES(6,'e\u{301}')",
            E::Duplicate,
            "duplicate key value violates unique constraint \"u2_v\"",
        );
    }
}
#[test]
fn named_composite_nulls_batch_atomicity_and_deterministic_bytes() {
    for engine in engines() {
        let a = Admin::create_engine(
            common::fresh("nd-unique-composite"),
            Limits::default(),
            engine,
        )
        .unwrap();
        let mut s = a.sql_session();
        setup(&mut s);
        run(
            &mut s,
            "CREATE TABLE u3(id int PRIMARY KEY,v text COLLATE ci UNIQUE)",
        );
        fail(
            &mut s,
            "INSERT INTO u3 VALUES(1,'a'),(2,'A')",
            E::Duplicate,
            "duplicate key value violates unique constraint \"u3_v_key\"",
        );
        assert_eq!(
            run(&mut s, "SELECT count(*) FROM u3"),
            vec![vec![V::Int(0)]]
        );
        run(
            &mut s,
            "CREATE TABLE u4(id int PRIMARY KEY,a text COLLATE ci,b int,UNIQUE(a,b))",
        );
        run(
            &mut s,
            "INSERT INTO u4 VALUES(1,'a',1),(2,'A',2),(3,NULL,1),(4,NULL,1),(5,'a',NULL),(6,'A',NULL)",
        );
        fail(
            &mut s,
            "INSERT INTO u4 VALUES(7,'A',1)",
            E::Duplicate,
            "duplicate key value violates unique constraint \"u4_a_b_key\"",
        );
        run(&mut s, "UPDATE u4 SET a='z' WHERE id=1");
        run(&mut s, "INSERT INTO u4 VALUES(7,'A',1)");
        fail(
            &mut s,
            "UPDATE u4 SET a='Z' WHERE id=7",
            E::Duplicate,
            "duplicate key value violates unique constraint \"u4_a_b_key\"",
        );
        run(
            &mut s,
            "CREATE COLLATION det (provider=icu,locale='und-u-ks-level2',deterministic=true)",
        );
        run(
            &mut s,
            "CREATE TABLE detkeys(k text COLLATE det PRIMARY KEY,v text COLLATE det UNIQUE)",
        );
        run(
            &mut s,
            "INSERT INTO detkeys VALUES('abc','\u{e9}'),('ABC','e\u{301}')",
        );
        run(
            &mut s,
            "CREATE TABLE p(a text COLLATE ci,b int,PRIMARY KEY(a,b))",
        );
        run(&mut s, "INSERT INTO p VALUES('a',1),('A',2)");
        fail(
            &mut s,
            "INSERT INTO p VALUES('z',9),('z',9)",
            E::Duplicate,
            "duplicate key value violates unique constraint \"p_pkey\"",
        );
        fail(
            &mut s,
            "INSERT INTO p VALUES('A',1)",
            E::Duplicate,
            "duplicate key value violates unique constraint \"p_pkey\"",
        );
        assert_eq!(
            s.execute(
                "INSERT INTO p VALUES('A',1),('a',1) ON CONFLICT DO NOTHING",
                &[],
                None
            )
            .unwrap()
            .affected,
            0
        );
    }
}

#[test]
fn explicit_index_collation_overrides_column_equality() {
    for engine in engines() {
        let path = common::fresh("nd-explicit-index");
        {
            let a = Admin::create_engine(&path, Limits::default(), engine).unwrap();
            let mut s = a.sql_session();
            setup(&mut s);
            run(&mut s, "CREATE TABLE x(id int PRIMARY KEY,v text)");
            run(&mut s, "INSERT INTO x VALUES(1,'a'),(2,'A')");
            fail(
                &mut s,
                "CREATE UNIQUE INDEX xv ON x(v COLLATE ci)",
                E::Duplicate,
                "could not create unique index \"xv\"",
            );
            run(&mut s, "DELETE FROM x WHERE id=2");
            run(&mut s, "CREATE UNIQUE INDEX xv ON x(v COLLATE ci)");
            fail(
                &mut s,
                "INSERT INTO x VALUES(3,'A')",
                E::Duplicate,
                "duplicate key value violates unique constraint \"xv\"",
            );
            assert_eq!(
                s.execute(
                    "INSERT INTO x VALUES(4,'A') ON CONFLICT DO NOTHING",
                    &[],
                    None
                )
                .unwrap()
                .affected,
                0
            );
            run(
                &mut s,
                "CREATE TABLE y(id int PRIMARY KEY,v text COLLATE ci)",
            );
            run(&mut s, "INSERT INTO y VALUES(1,'a'),(2,'A')");
            run(&mut s, r#"CREATE UNIQUE INDEX yv ON y(v COLLATE "C")"#);
            run(&mut s, "INSERT INTO y VALUES(3,'b')");
        }
        let a = Admin::open(&path, Limits::default()).unwrap();
        let mut s = a.sql_session();
        fail(
            &mut s,
            "INSERT INTO x VALUES(4,'A')",
            E::Duplicate,
            "duplicate key value violates unique constraint \"xv\"",
        );
        run(&mut s, "INSERT INTO y VALUES(4,'B')");
        run(&mut s, "DROP COLLATION ci CASCADE");
        assert_eq!(
            run(&mut s, "SELECT id FROM y ORDER BY id"),
            vec![
                vec![V::Int(1)],
                vec![V::Int(2)],
                vec![V::Int(3)],
                vec![V::Int(4)]
            ]
        );
    }
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn fixed_snapshot_cannot_insert_a_newer_collation_equal_owner() {
    let a = Admin::create_engine(
        common::fresh("nd-unique-fixed"),
        Limits::default(),
        Engine::V2,
    )
    .unwrap();
    let mut setup_session = a.sql_session();
    setup(&mut setup_session);
    run(
        &mut setup_session,
        "CREATE TABLE t(k text COLLATE ci PRIMARY KEY)",
    );
    let mut old = a.sql_session();
    run(&mut old, "BEGIN");
    old.set_transaction_isolation(giggles_core::SqlIsolation::RepeatableRead)
        .unwrap();
    assert_eq!(
        run(&mut old, "SELECT count(*) FROM t"),
        vec![vec![V::Int(0)]]
    );
    run(&mut setup_session, "INSERT INTO t VALUES('abc')");
    fail(
        &mut old,
        "INSERT INTO t VALUES('ABC')",
        E::Duplicate,
        "duplicate key value violates unique constraint \"t_pkey\"",
    );
    run(&mut old, "ROLLBACK");
    assert_eq!(
        run(&mut setup_session, "SELECT count(*) FROM t"),
        vec![vec![V::Int(1)]]
    );
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn fixed_snapshot_unique_probe_checks_unchanged_stale_witnesses() {
    let a = Admin::create_engine(
        common::fresh("nd-stale-unique-witness"),
        Limits::default(),
        Engine::V2,
    )
    .unwrap();
    let mut current = a.sql_session();
    setup(&mut current);
    run(
        &mut current,
        "CREATE TABLE t(id int PRIMARY KEY,v text COLLATE ci UNIQUE)",
    );
    run(&mut current, "INSERT INTO t VALUES(1,'old')");
    let mut old = a.sql_session();
    run(&mut old, "BEGIN");
    old.set_transaction_isolation(giggles_core::SqlIsolation::RepeatableRead)
        .unwrap();
    assert_eq!(
        run(&mut old, "SELECT count(*) FROM t"),
        vec![vec![V::Int(1)]]
    );
    run(&mut current, "UPDATE t SET v='NEW' WHERE id=1");
    fail(
        &mut old,
        "INSERT INTO t VALUES(2,'old'),(3,'new') ON CONFLICT DO NOTHING",
        E::Duplicate,
        "duplicate key value violates unique constraint \"t_v_key\"",
    );
    run(&mut old, "ROLLBACK");
    assert_eq!(
        run(&mut current, "SELECT * FROM t"),
        vec![vec![V::Int(1), V::Text("NEW".into())]]
    );
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn paged_values_and_spilled_view_sources_check_collation_equal_owners() {
    let a = Admin::create_engine(
        common::fresh("nd-paged-insert"),
        Limits::default(),
        Engine::V2,
    )
    .unwrap();
    let mut s = a.sql_session();
    setup(&mut s);
    run(&mut s, "CREATE TABLE src(id int PRIMARY KEY,k text)");
    let values = (0..401)
        .map(|i| format!("({i},'key{i}')"))
        .collect::<Vec<_>>()
        .join(",");
    run(&mut s, &format!("INSERT INTO src VALUES {values}"));
    run(&mut s, "CREATE VIEW input AS SELECT id,k FROM src");
    run(
        &mut s,
        "CREATE TABLE dst(id int PRIMARY KEY,k text COLLATE ci UNIQUE)",
    );
    assert_eq!(
        s.execute(
            "INSERT INTO dst SELECT id,k FROM input ORDER BY id RETURNING id,repeat(k,8000)",
            &[],
            None
        )
        .unwrap()
        .rows
        .len(),
        401
    );
    fail(
        &mut s,
        "INSERT INTO dst SELECT id+1000,upper(k) FROM input ORDER BY id DESC",
        E::Duplicate,
        "duplicate key value violates unique constraint \"dst_k_key\"",
    );
    assert_eq!(
        s.execute(
            "INSERT INTO dst SELECT id+1000,upper(k) FROM input ORDER BY id ON CONFLICT DO NOTHING",
            &[],
            None
        )
        .unwrap()
        .affected,
        0
    );
    assert_eq!(s.execute("INSERT INTO dst SELECT id+1000,upper(k) FROM input ORDER BY id ON CONFLICT(k) DO UPDATE SET id=excluded.id", &[], None).unwrap().affected, 401);
    assert_eq!(
        run(&mut s, "SELECT count(*),min(id) FROM dst"),
        vec![vec![V::Int(401), V::Int(1000)]]
    );

    run(
        &mut s,
        "CREATE TABLE pk(k text COLLATE ci PRIMARY KEY,n int)",
    );
    let values = (0..400)
        .map(|i| format!("('v{i}',{i})"))
        .chain(std::iter::once("('V0',400)".into()))
        .collect::<Vec<_>>()
        .join(",");
    fail(
        &mut s,
        &format!("INSERT INTO pk VALUES {values}"),
        E::Duplicate,
        "duplicate key value violates unique constraint \"pk_pkey\"",
    );
    assert_eq!(
        run(&mut s, "SELECT count(*) FROM pk"),
        vec![vec![V::Int(0)]]
    );
    assert_eq!(
        s.execute(
            &format!("INSERT INTO pk VALUES {values} ON CONFLICT DO NOTHING"),
            &[],
            None
        )
        .unwrap()
        .affected,
        400
    );
    assert_eq!(
        s.execute(
            &format!("INSERT INTO pk VALUES {values} ON CONFLICT(k) DO UPDATE SET n=excluded.n"),
            &[],
            None
        ),
        Err(CoreError::Sql(E::CardinalityViolation))
    );
    assert_eq!(
        run(&mut s, "SELECT n FROM pk WHERE k='v0'"),
        vec![vec![V::Int(0)]]
    );
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn spilled_self_foreign_keys_allow_later_collation_equal_parents() {
    let a = Admin::create_engine(
        common::fresh("nd-paged-self-fk"),
        Limits::default(),
        Engine::V2,
    )
    .unwrap();
    let mut s = a.sql_session();
    setup(&mut s);
    run(&mut s, "CREATE TABLE src(id int PRIMARY KEY,k text,p text)");
    let values = (0..401)
        .map(|i| {
            format!(
                "({i},'key{i}',{})",
                if i == 400 {
                    "NULL".into()
                } else {
                    format!("'KEY{}'", i + 1)
                }
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    run(&mut s, &format!("INSERT INTO src VALUES {values}"));
    run(&mut s, "CREATE VIEW input AS SELECT id,k,p FROM src");
    run(
        &mut s,
        "CREATE TABLE tree(k text COLLATE ci PRIMARY KEY,p text COLLATE ci REFERENCES tree(k))",
    );
    assert_eq!(
        s.execute(
            "INSERT INTO tree SELECT k,p FROM input ORDER BY id",
            &[],
            None
        )
        .unwrap()
        .affected,
        401
    );
    assert_eq!(
        run(&mut s, "SELECT count(*) FROM tree"),
        vec![vec![V::Int(401)]]
    );
    run(
        &mut s,
        "CREATE TABLE bad(k text COLLATE ci PRIMARY KEY,p text COLLATE ci REFERENCES bad(k))",
    );
    assert_eq!(s.execute("INSERT INTO bad SELECT k,CASE WHEN id=400 THEN 'missing' ELSE p END FROM input ORDER BY id", &[], None), Err(CoreError::Sql(E::ForeignKeyViolation)));
    assert_eq!(
        run(&mut s, "SELECT count(*) FROM bad"),
        vec![vec![V::Int(0)]]
    );
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn heap_on_conflict_uses_collation_equal_owners_across_pages_and_spills() {
    for (collation, original, equal) in [("ci", "abc", "ABC"), ("nd", "\u{e9}", "e\u{301}")] {
        let a = Admin::create_engine(
            common::fresh("nd-heap-conflict"),
            Limits::default(),
            Engine::V2,
        )
        .unwrap();
        let mut s = a.sql_session();
        setup(&mut s);
        run(&mut s, "CREATE TABLE src(id int PRIMARY KEY,k text)");
        let values = (0..401)
            .map(|i| format!("({i},'{original}{i}')"))
            .collect::<Vec<_>>()
            .join(",");
        run(&mut s, &format!("INSERT INTO src VALUES {values}"));
        run(&mut s, "CREATE VIEW input AS SELECT id,k FROM src");
        run(
            &mut s,
            &format!("CREATE TABLE heap(k text COLLATE {collation} UNIQUE,n int,payload text)"),
        );
        // The ordered view source exceeds the in-memory spill threshold.
        assert_eq!(
            s.execute(
                "INSERT INTO heap SELECT k,id,repeat('x',4000) FROM input ORDER BY id",
                &[],
                None,
            )
            .unwrap()
            .affected,
            401
        );
        let proposals = (0..401)
            .map(|i| format!("('{equal}{i}',{},'updated')", i + 1000))
            .collect::<Vec<_>>()
            .join(",");
        assert_eq!(
            s.execute(
                &format!(
                    "INSERT INTO heap VALUES {proposals} ON CONFLICT(k) DO UPDATE SET n=excluded.n"
                ),
                &[],
                None
            )
            .unwrap()
            .affected,
            401
        );
        assert_eq!(
            run(&mut s, "SELECT count(*),min(n),max(n) FROM heap"),
            vec![vec![V::Int(401), V::Int(1000), V::Int(1400)]]
        );
        assert_eq!(s.execute(&format!(
            "INSERT INTO heap VALUES {proposals} ON CONFLICT ON CONSTRAINT heap_k_key DO NOTHING"
        ), &[], None).unwrap().affected, 0);
        assert_eq!(s.execute(&format!(
            "INSERT INTO heap SELECT '{equal}' || id,id,repeat('y',4000) FROM input ORDER BY id DESC ON CONFLICT(k) DO UPDATE SET n=excluded.n"
        ), &[], None).unwrap().affected, 401);
        // A second affect on a later page is 21000 and rolls every page back.
        let duplicate = (0..401)
            .map(|i| {
                format!(
                    "('{equal}{}',9999,'duplicate')",
                    if i == 400 { 0 } else { i }
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        assert_eq!(
            s.execute(
                &format!(
                    "INSERT INTO heap VALUES {duplicate} ON CONFLICT(k) DO UPDATE SET n=excluded.n"
                ),
                &[],
                None
            ),
            Err(CoreError::Sql(E::CardinalityViolation))
        );
        assert_eq!(
            run(&mut s, "SELECT count(*),min(n),max(n) FROM heap"),
            vec![vec![V::Int(401), V::Int(0), V::Int(400)]]
        );
        // Unhydrated heap owners and NULL arbiters must not collide with new rowids.
        run(
            &mut s,
            "INSERT INTO heap VALUES(NULL,500,NULL),(NULL,501,NULL),('fresh',502,NULL) ON CONFLICT(k) DO UPDATE SET n=excluded.n",
        );
        assert_eq!(
            run(&mut s, "SELECT count(*),count(k) FROM heap"),
            vec![vec![V::Int(404), V::Int(402)]]
        );
    }
}
