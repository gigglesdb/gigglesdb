mod common;
use common::fresh;
use giggles_core::{
    Admin, CoreError, Limits, SqlSession,
    relational::{SqlError as E, Value as V},
};

fn run(s: &mut SqlSession, sql: &str) -> Vec<Vec<V>> {
    s.execute(sql, &[], None)
        .expect(sql)
        .rows
        .into_iter()
        .map(|r| r.values)
        .collect()
}
fn text(s: &str) -> V {
    V::Text(s.into())
}

#[test]
fn postgres_oracle_character_assignment_and_typmods() {
    // PG18 capture: J:/gigglesdb-work/e2/oracle/e2a-prime.out; raw char output retains padding.
    let a = Admin::create(fresh("e2-character-oracle"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    run(
        &mut s,
        "CREATE TABLE t (id int PRIMARY KEY, v varchar(5), c char(3), bc character, bv varchar, cv character varying(4), b bpchar)",
    );
    run(
        &mut s,
        "INSERT INTO t VALUES (1,'ab','a','x','free text','abcd','zz  ')",
    );
    run(
        &mut s,
        "INSERT INTO t VALUES (2,'abcde   ','ab   ',' ','',NULL,NULL)",
    );
    assert_eq!(
        run(&mut s, "SELECT id,v,c,bc,bv,cv,b FROM t ORDER BY id"),
        vec![
            vec![
                V::Int(1),
                text("ab"),
                text("a  "),
                text("x"),
                text("free text"),
                text("abcd"),
                text("zz  ")
            ],
            vec![
                V::Int(2),
                text("abcde"),
                text("ab "),
                text(" "),
                text(""),
                V::Null,
                V::Null
            ],
        ]
    );
    for sql in [
        "INSERT INTO t (id,v) VALUES (3,'abcdef')",
        "INSERT INTO t (id,c) VALUES (4,'abcd')",
        "INSERT INTO t (id,bc) VALUES (5,'ab')",
        "UPDATE t SET v='toolongvalue' WHERE id=1",
    ] {
        // PG18 22001: non-space overflow rejects the assignment.
        assert_eq!(
            s.execute(sql, &[], None),
            Err(CoreError::Sql(E::StringDataRightTruncation)),
            "{sql}"
        );
    }
    run(&mut s, "UPDATE t SET v='12345     ' WHERE id=1");
    assert_eq!(
        run(&mut s, "SELECT v FROM t WHERE id=1"),
        vec![vec![text("12345")]]
    );
    for sql in [
        "CREATE TABLE e1 (x varchar(0))",
        "CREATE TABLE e2 (x char(10485761))",
    ] {
        // PG18 22023: invalid declared character length.
        assert_eq!(
            s.execute(sql, &[], None),
            Err(CoreError::Sql(E::InvalidParameterValue)),
            "{sql}"
        );
    }
    run(&mut s, "CREATE TABLE e3 (x varchar(10485760))");
}

#[test]
fn character_assignment_applies_across_write_paths_atomically() {
    let a = Admin::create(fresh("e2-character-write-paths"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    run(
        &mut s,
        "CREATE TABLE d (id int PRIMARY KEY, v varchar(3) DEFAULT 'abc  ', c char(3) DEFAULT 'x')",
    );
    run(&mut s, "INSERT INTO d (id) VALUES (1),(2)");
    assert_eq!(
        run(&mut s, "SELECT v,c FROM d ORDER BY id"),
        vec![vec![text("abc"), text("x  ")]; 2]
    );
    for sql in [
        "INSERT INTO d VALUES (3,'ok','y'),(4,'long','z')",
        "UPDATE d SET v='long' WHERE id=1",
        "INSERT INTO d VALUES (1,'ok','x') ON CONFLICT (id) DO UPDATE SET v='long'",
        "ALTER TABLE d ADD COLUMN bad varchar(3) DEFAULT 'long'",
    ] {
        assert_eq!(
            s.execute(sql, &[], None),
            Err(CoreError::Sql(E::StringDataRightTruncation)),
            "{sql}"
        );
    }
    assert_eq!(
        run(&mut s, "SELECT id,v,c FROM d ORDER BY id"),
        vec![
            vec![V::Int(1), text("abc"), text("x  ")],
            vec![V::Int(2), text("abc"), text("x  ")],
        ]
    );
    // INSERT ... SELECT is not supported on main for any type (42601); E3 adds it.
    // Follow-up for E3: INSERT ... SELECT coercion is covered when insert_select lands.
    run(
        &mut s,
        "INSERT INTO d VALUES (3,'def  ','q'),(4,'ghi','rs   ')",
    );
    assert_eq!(
        run(&mut s, "SELECT v,c FROM d WHERE id>=3 ORDER BY id"),
        vec![
            vec![text("def"), text("q  ")],
            vec![text("ghi"), text("rs ")],
        ]
    );
    run(
        &mut s,
        "INSERT INTO d VALUES (1,'uvw  ','p') ON CONFLICT (id) DO UPDATE SET v=EXCLUDED.v,c=EXCLUDED.c",
    );
    assert_eq!(
        run(&mut s, "SELECT v,c FROM d WHERE id=1"),
        vec![vec![text("uvw"), text("p  ")]]
    );
    run(
        &mut s,
        "ALTER TABLE d ADD COLUMN filled char(3) DEFAULT 'z'",
    );
    run(
        &mut s,
        "ALTER TABLE d ADD COLUMN short varchar(3) DEFAULT 'xyz  '",
    );
    assert_eq!(
        run(&mut s, "SELECT filled,short FROM d ORDER BY id"),
        vec![vec![text("z  "), text("xyz")]; 4]
    );
}

#[test]
fn character_lengths_count_unicode_scalars_and_only_ascii_space_is_discarded() {
    let a = Admin::create(fresh("e2-character-unicode"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    run(
        &mut s,
        "CREATE TABLE u (id int PRIMARY KEY, v varchar(2), c char(2))",
    );
    let insert = s.prepare("INSERT INTO u VALUES ($1,$2,$3)").unwrap();
    s.execute_prepared(
        &insert,
        &[V::Int(1), text("\u{e9}\u{732b}  "), text("\u{732b}")],
        None,
    )
    .unwrap();
    assert_eq!(
        run(&mut s, "SELECT v,c FROM u"),
        vec![vec![text("\u{e9}\u{732b}"), text("\u{732b} ")]]
    );
    for value in ["abc", "ab\t", "ab\u{a0}"] {
        assert_eq!(
            s.execute_prepared(&insert, &[V::Int(2), text(value), text("x")], None),
            Err(CoreError::Sql(E::StringDataRightTruncation)),
            "{value:?}"
        );
    }
    // Multi-row INSERT is atomic: one overlong row rejects the whole statement.
    // (INSERT ... SELECT form deferred to E3's insert_select.)
    assert_eq!(
        s.execute("INSERT INTO u (id,v) VALUES (2,'ok'),(3,'long')", &[], None),
        Err(CoreError::Sql(E::StringDataRightTruncation))
    );
    assert_eq!(run(&mut s, "SELECT id FROM u"), vec![vec![V::Int(1)]]);
}

#[test]
fn f18_assignment_casts_feed_character_typmods() {
    // PG18.6 capture: J:/gigglesdb-work/e2/oracle/e2a-f18.out. Typed values convert
    // through F18's assignment casts to text, then varchar(n)/char(n) apply.
    let a = Admin::create(fresh("e2-character-f18-casts"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    run(
        &mut s,
        "CREATE TABLE t (id int PRIMARY KEY, v varchar(5), c char(12), w varchar(12))",
    );
    for sql in [
        "INSERT INTO t (id, v) VALUES (1, '2026-11-08'::date)",
        "INSERT INTO t (id, v) VALUES (4, 123456)",
        "INSERT INTO t (id, w) VALUES (9, '2026-11-08 01:02:03+00'::timestamptz)",
    ] {
        assert_eq!(
            s.execute(sql, &[], None).map(|_| ()),
            Err(CoreError::Sql(E::StringDataRightTruncation)),
            "{sql}"
        );
    }
    for sql in [
        "INSERT INTO t (id, c) VALUES (2, '2026-11-08'::date)",
        "INSERT INTO t (id, v) VALUES (3, 12345)",
        "INSERT INTO t (id, c) VALUES (5, true)",
        "INSERT INTO t (id, w) VALUES (6, '1 day'::interval)",
        "INSERT INTO t (id, c) VALUES (7, 1.50::numeric)",
        "INSERT INTO t (id, w) VALUES (8, 0.1::float8)",
    ] {
        run(&mut s, sql);
    }
    let null = V::Null;
    assert_eq!(
        run(&mut s, "SELECT id, v, c, w FROM t ORDER BY id"),
        vec![
            vec![V::Int(2), null.clone(), text("2026-11-08  "), null.clone()],
            vec![V::Int(3), text("12345"), null.clone(), null.clone()],
            vec![V::Int(5), null.clone(), text("true        "), null.clone()],
            vec![V::Int(6), null.clone(), null.clone(), text("1 day")],
            vec![V::Int(7), null.clone(), text("1.50        "), null.clone()],
            vec![V::Int(8), null.clone(), null.clone(), text("0.1")],
        ]
    );
}
