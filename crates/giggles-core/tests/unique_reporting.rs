#![cfg(feature = "engine-rocksdb")]

mod common;
use giggles_core::{
    Admin, CoreError, Engine, Limits, SqlSession,
    relational::{
        SqlErrorDetail, SqlResult, Value, take_sql_diagnostic_detail, take_sql_error_message,
        take_sql_notices,
    },
    sqlstate,
};

fn run(session: &mut SqlSession, sql: &str) -> Result<SqlResult, CoreError> {
    session.execute(sql, &[], None)
}

fn observe(
    label: &str,
    result: Result<SqlResult, CoreError>,
    code: &str,
    name: &str,
    detail: &str,
) {
    let message = take_sql_error_message();
    let diagnostic = take_sql_diagnostic_detail();
    let notices = take_sql_notices();
    match code {
        "" => {
            result.unwrap_or_else(|err| panic!("{label}: {err:?}, {message:?}"));
        }
        "notice" => {
            result.unwrap();
            assert_eq!(
                notices,
                [(
                    format!("relation \"{name}\" already exists, skipping"),
                    None,
                    Some("42P07")
                )],
                "{label}"
            );
        }
        "23505" => {
            assert_eq!(sqlstate(&result.unwrap_err()), code, "{label}");
            assert_eq!(
                message.unwrap(),
                format!("duplicate key value violates unique constraint \"{name}\""),
                "{label}"
            );
            let Some(SqlErrorDetail::DuplicateKey(key)) = diagnostic else {
                panic!("{label}: expected structured duplicate key detail");
            };
            assert_eq!(key.constraint, name, "{label}");
            assert_eq!(key.schema, "public", "{label}");
            assert_eq!(key.table, name.split('_').next().unwrap(), "{label}");
            assert_eq!(
                key.values,
                format!("Key {detail} already exists."),
                "{label}"
            );
        }
        _ => {
            assert_eq!(sqlstate(&result.unwrap_err()), code, "{label}");
            assert_eq!(message.as_deref(), Some(name), "{label}");
            assert!(diagnostic.is_none(), "{label}: unexpected DETAIL");
        }
    }
}

fn cases(prefix: &str, cases: &[(&str, &str, &str, &str)]) -> (Admin, SqlSession) {
    let admin = Admin::create_engine(common::fresh(prefix), Limits::default(), Engine::V2).unwrap();
    let mut session = admin.sql_session();
    for &(sql, code, name, detail) in cases {
        observe(sql, run(&mut session, sql), code, name, detail);
    }
    (admin, session)
}

#[test]
fn creation_order() {
    cases(
        "unique-order",
        &[
            (
                "create table a (id int primary key, e text, del boolean)",
                "",
                "",
                "",
            ),
            (
                "create unique index a_e_live on a(e) where not del",
                "",
                "",
                "",
            ),
            ("insert into a values (1,'x',false)", "", "", ""),
            (
                "insert into a values (1,'x',false)",
                "23505",
                "a_pkey",
                "(id)=(1)",
            ),
            ("create table b (id int, e text, del boolean)", "", "", ""),
            (
                "create unique index b_e_live on b(e) where not del",
                "",
                "",
                "",
            ),
            ("alter table b add primary key (id)", "", "", ""),
            ("insert into b values (1,'x',false)", "", "", ""),
            (
                "insert into b values (1,'x',false)",
                "23505",
                "b_e_live",
                "(e)=(x)",
            ),
            ("insert into b values (2,'y',false)", "", "", ""),
            (
                "update b set id=1,e='x' where id=2",
                "23505",
                "b_e_live",
                "(e)=(x)",
            ),
            (
                "insert into b values (2,'x',false) on conflict (id) do update set id=1,e=excluded.e",
                "23505",
                "b_e_live",
                "(e)=(x)",
            ),
            (
                "create table c (u int unique, id int primary key)",
                "",
                "",
                "",
            ),
            ("insert into c values (1,1)", "", "", ""),
            ("insert into c values (1,1)", "23505", "c_pkey", "(id)=(1)"),
            (
                "create table d (id int, u int, constraint d_u unique(u), constraint d_pk primary key(id))",
                "",
                "",
                "",
            ),
            ("insert into d values (1,1)", "", "", ""),
            ("insert into d values (1,1)", "23505", "d_pk", "(id)=(1)"),
            ("create table f (id int primary key, u int)", "", "", ""),
            ("create unique index f_u on f(u)", "", "", ""),
            ("alter table f drop constraint f_pkey", "", "", ""),
            ("alter table f add primary key (id)", "", "", ""),
            ("insert into f values (1,1)", "", "", ""),
            ("insert into f values (1,1)", "23505", "f_u", "(u)=(1)"),
            ("create table g (id int primary key, u int)", "", "", ""),
            ("create unique index g_u on g(u)", "", "", ""),
            ("insert into g values (1,1)", "", "", ""),
            ("insert into g values (1,1)", "23505", "g_pkey", "(id)=(1)"),
        ],
    );
}

#[test]
#[ignore = "requires support for deferrable UNIQUE constraints"]
fn deferrable_unique_order() {
    cases(
        "unique-order-deferrable",
        &[
            (
                "create table e (id int, u int, constraint e_u unique(u) deferrable)",
                "",
                "",
                "",
            ),
            ("alter table e add primary key (id)", "", "", ""),
            ("insert into e values (1,1)", "", "", ""),
            ("insert into e values (1,1)", "23505", "e_pkey", "(id)=(1)"),
        ],
    );
}

#[test]
#[ignore = "requires REINDEX support"]
fn reindex_preserves_order() {
    cases(
        "unique-order-reindex",
        &[
            ("create table g (id int primary key, u int)", "", "", ""),
            ("create unique index g_u on g(u)", "", "", ""),
            ("reindex index g_pkey", "", "", ""),
            ("insert into g values (1,1)", "", "", ""),
            ("insert into g values (1,1)", "23505", "g_pkey", "(id)=(1)"),
            ("reindex table g", "", "", ""),
            ("insert into g values (1,1)", "23505", "g_pkey", "(id)=(1)"),
        ],
    );
}

#[test]
#[ignore = "requires expression index support"]
fn expression_index_order() {
    cases(
        "unique-order-expression",
        &[
            ("create table h (id int, s text)", "", "", ""),
            ("create unique index h_lower on h(lower(s))", "", "", ""),
            (
                "alter table h add constraint h_pk primary key(id)",
                "",
                "",
                "",
            ),
            ("insert into h values (1,'A')", "", "", ""),
            (
                "insert into h values (1,'a')",
                "23505",
                "h_lower",
                "(lower(s))=(a)",
            ),
        ],
    );
}

#[test]
fn writes() {
    let (_admin, mut session) = cases(
        "unique-order-writes",
        &[
            (
                "create table a (id int primary key, e text, del boolean)",
                "",
                "",
                "",
            ),
            (
                "create unique index a_e_live on a(e) where not del",
                "",
                "",
                "",
            ),
            ("create unique index a_e2 on a(e, id)", "", "", ""),
            (
                "insert into a values (1,'x',false),(2,'y',false)",
                "",
                "",
                "",
            ),
            (
                "update a set id=1, e='x' where id=2",
                "23505",
                "a_pkey",
                "(id)=(1)",
            ),
            (
                "update a set e='x' where id=2",
                "23505",
                "a_e_live",
                "(e)=(x)",
            ),
            (
                "insert into a values (3,'z',false),(3,'z',false)",
                "23505",
                "a_pkey",
                "(id)=(3)",
            ),
            (
                "insert into a values (9,'x',false),(1,'y',false)",
                "23505",
                "a_e_live",
                "(e)=(x)",
            ),
            (
                "insert into a values (9,'x',false),(3,'z',false),(3,'z',false)",
                "23505",
                "a_e_live",
                "(e)=(x)",
            ),
            (
                "insert into a values (9,'z',false),(8,'z',false),(1,'x',false)",
                "23505",
                "a_e_live",
                "(e)=(z)",
            ),
            (
                "insert into a values (1,'w',false) on conflict (e) where not del do nothing",
                "23505",
                "a_pkey",
                "(id)=(1)",
            ),
            (
                "insert into a values (5,'x',false) on conflict (id) do nothing",
                "23505",
                "a_e_live",
                "(e)=(x)",
            ),
            (
                "insert into a values (6,'x',false) on conflict on constraint a_pkey do update set e=excluded.e",
                "23505",
                "a_e_live",
                "(e)=(x)",
            ),
            (
                "insert into a values (1,'x',false) on conflict do nothing",
                "",
                "",
                "",
            ),
            (
                "insert into a values (1,'y',false) on conflict (id) do update set e=excluded.e",
                "23505",
                "a_e_live",
                "(e)=(y)",
            ),
        ],
    );
    let (_, prepared) = session.prepare_copy("copy a from stdin").unwrap();
    let rows = [("7", "q", "f"), ("1", "x", "f")].map(|(id, e, del)| {
        vec![
            Value::Text(id.into()),
            Value::Text(e.into()),
            Value::Text(del.into()),
        ]
    });
    observe(
        "copy a from stdin",
        session.ingest_copy(&prepared, rows.into()),
        "23505",
        "a_pkey",
        "(id)=(1)",
    );
    assert_eq!(
        run(&mut session, "select count(*) from a").unwrap().rows[0].values,
        [Value::Int(2)]
    );
    let (_admin, _) = cases(
        "unique-order-nulls",
        &[
            (
                "create table m (a int, b text, c int primary key)",
                "",
                "",
                "",
            ),
            ("create unique index m_ab on m(a,b)", "", "", ""),
            ("insert into m values (1,null,1),(1,null,2)", "", "", ""),
            (
                "insert into m values (1,'p',3),(1,'p',4)",
                "23505",
                "m_ab",
                "(a, b)=(1, p)",
            ),
        ],
    );
}

#[test]
#[ignore = "requires UNIQUE NULLS NOT DISTINCT support"]
fn nulls_not_distinct() {
    cases(
        "unique-order-null-equality",
        &[
            (
                "create table n (a int, b int, unique nulls not distinct (a,b))",
                "",
                "",
                "",
            ),
            (
                "insert into n values (1,null),(1,null)",
                "23505",
                "n_a_b_key",
                "(a, b)=(1, null)",
            ),
        ],
    );
}

#[test]
fn ddl_collisions() {
    let (admin, _) = cases(
        "unique-order-ddl",
        &[
            (
                "create table t (id int primary key, u int, v int)",
                "",
                "",
                "",
            ),
            ("create table t2 (id int)", "", "", ""),
            ("create sequence s1", "", "", ""),
            ("create view vw as select id as x from t2", "", "", ""),
            ("create index t_u_idx on t(u)", "", "", ""),
            (
                "create index t_u_idx on t(v)",
                "42P07",
                "relation \"t_u_idx\" already exists",
                "",
            ),
            (
                "create unique index t2 on t(u)",
                "42P07",
                "relation \"t2\" already exists",
                "",
            ),
            (
                "create index s1 on t(u)",
                "42P07",
                "relation \"s1\" already exists",
                "",
            ),
            (
                "create index vw on t(u)",
                "42P07",
                "relation \"vw\" already exists",
                "",
            ),
            (
                "create index t_pkey on t(v)",
                "42P07",
                "relation \"t_pkey\" already exists",
                "",
            ),
            (
                "create index if not exists t_u_idx on t(v)",
                "notice",
                "t_u_idx",
                "",
            ),
            ("create index if not exists t2 on t(v)", "notice", "t2", ""),
            (
                "alter table t add constraint t_u_idx unique (u)",
                "42P07",
                "relation \"t_u_idx\" already exists",
                "",
            ),
            (
                "alter table t add constraint t2 unique (u)",
                "42P07",
                "relation \"t2\" already exists",
                "",
            ),
            (
                "alter table t add constraint s1 unique (v)",
                "42P07",
                "relation \"s1\" already exists",
                "",
            ),
            (
                "alter table t add constraint vw primary key (v)",
                "42P16",
                "multiple primary keys for table \"t\" are not allowed",
                "",
            ),
            (
                "alter table t add constraint t_u_key unique (u)",
                "",
                "",
                "",
            ),
            (
                "alter table t add constraint t_u_key unique (v)",
                "42P07",
                "relation \"t_u_key\" already exists",
                "",
            ),
            (
                "alter table t add constraint t_u_key check (v > 0)",
                "42710",
                "constraint \"t_u_key\" for relation \"t\" already exists",
                "",
            ),
            (
                "alter table t add constraint t_chk check (v > 0)",
                "",
                "",
                "",
            ),
            (
                "alter table t add constraint t_chk unique (v)",
                "42710",
                "constraint \"t_chk\" for relation \"t\" already exists",
                "",
            ),
            (
                "create table t3 (a int, constraint t_pkey unique(a))",
                "42P07",
                "relation \"t_pkey\" already exists",
                "",
            ),
            (
                "create table t4 (a int, constraint s1 primary key(a))",
                "42P07",
                "relation \"s1\" already exists",
                "",
            ),
            (
                "alter index t_u_idx rename to t2",
                "42P07",
                "relation \"t2\" already exists",
                "",
            ),
            (
                "alter table t rename constraint t_u_key to s1",
                "42P07",
                "relation \"s1\" already exists",
                "",
            ),
        ],
    );
    let (_, tables) = admin.catalog().unwrap();
    assert!(
        !tables
            .iter()
            .any(|table| matches!(table.name.as_str(), "t3" | "t4"))
    );
}

#[test]
fn point_updates_and_select_sources() {
    let (_, mut session) = cases(
        "unique-order-select-sources",
        &[
            ("CREATE TABLE bb (id int, e text, del boolean)", "", "", ""),
            (
                "CREATE UNIQUE INDEX bb_e_live ON bb(e) WHERE NOT del",
                "",
                "",
                "",
            ),
            ("ALTER TABLE bb ADD PRIMARY KEY (id)", "", "", ""),
            (
                "INSERT INTO bb VALUES (1,'x',false),(2,'y',false)",
                "",
                "",
                "",
            ),
            (
                "UPDATE bb SET id=1, e='x' WHERE id=2",
                "23505",
                "bb_e_live",
                "(e)=(x)",
            ),
            ("CREATE TABLE dd (id int, u int)", "", "", ""),
            ("CREATE UNIQUE INDEX dd_u ON dd(u)", "", "", ""),
            ("ALTER TABLE dd ADD PRIMARY KEY (id)", "", "", ""),
            ("INSERT INTO dd VALUES (1,1),(2,2)", "", "", ""),
            ("UPDATE dd SET u=1 WHERE id=2", "23505", "dd_u", "(u)=(1)"),
            ("INSERT INTO dd SELECT 1,1", "23505", "dd_u", "(u)=(1)"),
            (
                "INSERT INTO dd (id,u) SELECT id+0,u FROM dd WHERE id=1",
                "23505",
                "dd_u",
                "(u)=(1)",
            ),
            (
                "INSERT INTO dd SELECT 9,9 UNION ALL SELECT 8,9 UNION ALL SELECT 1,7",
                "23505",
                "dd_u",
                "(u)=(9)",
            ),
            (
                "WITH x AS (SELECT 1 id, 1 u) INSERT INTO dd SELECT * FROM x",
                "23505",
                "dd_u",
                "(u)=(1)",
            ),
            (
                "INSERT INTO dd SELECT 1,1 ON CONFLICT (u) DO NOTHING",
                "",
                "",
                "",
            ),
            (
                "INSERT INTO dd SELECT 3,1 ON CONFLICT (id) DO NOTHING",
                "23505",
                "dd_u",
                "(u)=(1)",
            ),
            (
                "INSERT INTO dd SELECT 2,1 ON CONFLICT (id) DO UPDATE SET u=excluded.u",
                "23505",
                "dd_u",
                "(u)=(1)",
            ),
        ],
    );
    let sql = "INSERT INTO dd SELECT $2::int,$1::int ON CONFLICT (id) DO NOTHING";
    let prepared = session.prepare(sql).unwrap();
    observe(
        sql,
        session.execute_prepared(&prepared, &[Value::Int(1), Value::Int(3)], None),
        "23505",
        "dd_u",
        "(u)=(1)",
    );
    assert_eq!(
        run(&mut session, "SELECT count(*) FROM dd").unwrap().rows[0].values,
        [Value::Int(2)]
    );
}

#[test]
fn automatic_index_and_constraint_names() {
    let (_, mut session) = cases(
        "unique-order-auto-names",
        &[
            ("CREATE TABLE ff (u int)", "", "", ""),
            ("CREATE INDEX ON ff(u)", "", "", ""),
            ("CREATE INDEX ON ff(u)", "", "", ""),
            ("CREATE INDEX ON ff(u)", "", "", ""),
            ("CREATE TABLE t_u_key (id int)", "", "", ""),
            ("CREATE TABLE t (u int UNIQUE)", "", "", ""),
            ("ALTER TABLE t ADD UNIQUE(u)", "", "", ""),
            ("INSERT INTO t VALUES (1)", "", "", ""),
            ("INSERT INTO t VALUES (1)", "23505", "t_u_key1", "(u)=(1)"),
            (
                "ALTER INDEX ff_u_idx RENAME TO ff",
                "42P07",
                "relation \"ff\" already exists",
                "",
            ),
            (
                "ALTER INDEX public.ff_u_idx RENAME TO ff_u_idx1",
                "42P07",
                "relation \"ff_u_idx1\" already exists",
                "",
            ),
        ],
    );
    for name in ["ff_u_idx", "ff_u_idx1", "ff_u_idx2", "t_u_key1", "t_u_key2"] {
        // Resolve each name directly so the in-process test does not rely on pgwire's pg_indexes.
        let sql = format!("CREATE INDEX {name} ON ff(u)");
        observe(
            &sql,
            run(&mut session, &sql),
            "42P07",
            &format!("relation \"{name}\" already exists"),
            "",
        );
    }
}
