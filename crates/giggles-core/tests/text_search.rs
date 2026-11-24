mod common;

use giggles_core::{
    Admin, Engine, Limits, SqlSession,
    relational::{DataType, StatementClock, Value, take_sql_error_message},
    sqlstate,
};

fn run(s: &mut SqlSession, sql: &str) -> Vec<Vec<Value>> {
    rows(s.execute(sql, &[], None), sql)
}

fn rows(
    result: Result<giggles_core::relational::SqlResult, giggles_core::CoreError>,
    sql: &str,
) -> Vec<Vec<Value>> {
    result
        .unwrap_or_else(|error| panic!("{sql}: {error:?} ({:?})", take_sql_error_message()))
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect()
}

fn expect_error(session: &mut SqlSession, sql: &str, code: &str) {
    let error = session.execute(sql, &[], None).unwrap_err();
    assert_eq!(
        sqlstate(&error),
        code,
        "{sql} ({:?})",
        take_sql_error_message()
    );
}

fn text(value: &str) -> Value {
    Value::Text(value.into())
}

fn default_engine() -> Engine {
    if cfg!(feature = "engine-rocksdb") {
        Engine::V2
    } else {
        Engine::Legacy
    }
}

fn scope(engine: Engine) {
    let path = common::fresh("text-search");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    let mut session = admin.sql_session();
    for sql in [
        "CREATE TABLE terms(id integer PRIMARY KEY, v tsvector, q tsquery)",
        "INSERT INTO terms VALUES (1, 'cat:1 dog:2', 'cat & dog'), (2, 'dog:2 cat:1', 'cat & dog'), (3, NULL, NULL), (4, 'a:2', 'cat:*A'), (5, 'a:1', 'cat')",
        "ALTER TABLE terms ADD COLUMN source text",
        "ALTER TABLE terms ADD COLUMN added_query tsquery DEFAULT ''",
        "ALTER TABLE terms ADD COLUMN added_vector tsvector DEFAULT 'dog:3'::tsvector",
        "UPDATE terms SET source = 'capital:9' WHERE id = 1",
    ] {
        run(&mut session, sql);
    }
    expect_error(
        &mut session,
        "ALTER TABLE terms ALTER COLUMN source TYPE tsvector",
        "42804",
    );
    run(
        &mut session,
        "ALTER TABLE terms ALTER COLUMN source TYPE tsvector USING source::tsvector",
    );
    assert_eq!(
        run(&mut session, "SELECT source::text FROM terms WHERE id = 1"),
        vec![vec![text("'capital':9")]]
    );
    run(
        &mut session,
        "ALTER TABLE terms ALTER COLUMN added_vector TYPE tsvector USING added_vector || 'cat:9'::tsvector",
    );
    run(
        &mut session,
        "ALTER TABLE terms ALTER COLUMN source TYPE text",
    );
    run(
        &mut session,
        "CREATE TABLE stored_search_values(id integer PRIMARY KEY, v tsvector, q tsquery)",
    );
    run(
        &mut session,
        "INSERT INTO stored_search_values VALUES (1, 'a b:2', '')",
    );
    let unsorted = b"\0\0\0\x02b\0\0\x01\0\x02a\0\0\0";
    let binary =
        giggles_core::relational::text_search_types::binary_input(&DataType::TsVector, unsorted)
            .unwrap();
    session
        .execute(
            "INSERT INTO stored_search_values VALUES (2, $1, '')",
            &[binary],
            None,
        )
        .unwrap();

    let check = |session: &mut SqlSession| {
        assert_eq!(
            run(
                session,
                "SELECT id, v::text, q::text, v = v, v @@ q, q @> 'cat'::tsquery FROM terms WHERE id < 4 ORDER BY v NULLS FIRST, id"
            ),
            vec![
                vec![
                    Value::Int(3),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null
                ],
                vec![
                    Value::Int(1),
                    text("'cat':1 'dog':2"),
                    text("'cat' & 'dog'"),
                    Value::Bool(true),
                    Value::Bool(true),
                    Value::Bool(true)
                ],
                vec![
                    Value::Int(2),
                    text("'cat':1 'dog':2"),
                    text("'cat' & 'dog'"),
                    Value::Bool(true),
                    Value::Bool(true),
                    Value::Bool(true)
                ],
            ],
        );
        assert_eq!(
            run(session, "SELECT id FROM terms WHERE id >= 4 ORDER BY v, id"),
            vec![vec![Value::Int(4)], vec![Value::Int(5)]],
        );
        assert_eq!(run(session, "SELECT DISTINCT q FROM terms").len(), 3);
        assert_eq!(
            run(
                session,
                "SELECT count(*) FROM terms GROUP BY q ORDER BY count(*)"
            ),
            vec![
                vec![Value::Int(1)],
                vec![Value::Int(2)],
                vec![Value::Int(2)]
            ],
        );
        assert_eq!(
            run(session, "SELECT source FROM terms WHERE id = 1"),
            vec![vec![text("'capital':9")]]
        );
        assert_eq!(
            run(session, "SELECT added_query::text, added_vector::text FROM terms WHERE id = 1"),
            vec![vec![text(""), text("'cat':12 'dog':3")]]
        );
        assert_eq!(
            run(
                session,
                "SELECT id, v::text, q::text FROM stored_search_values ORDER BY stored_search_values.v DESC"
            ),
            vec![
                vec![Value::Int(2), text("'a' 'b':2"), text("")],
                vec![Value::Int(1), text("'a' 'b':2"), text("")]
            ]
        );
        expect_error(session, "SELECT max(v) FROM terms", "42883");
        expect_error(session, "SELECT min(q) FROM terms", "42883");
    };
    check(&mut session);
    drop(session);
    drop(admin);

    let admin = Admin::open(&path, Limits::default()).unwrap();
    let (_, tables) = admin.catalog().unwrap();
    let terms = tables.iter().find(|table| table.name == "terms").unwrap();
    assert_eq!(terms.columns[1].data_type, DataType::TsVector);
    assert_eq!(terms.columns[2].data_type, DataType::TsQuery);
    check(&mut admin.sql_session());
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn text_search_legacy_roundtrip() {
    scope(Engine::Legacy);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn text_search_v2_roundtrip() {
    scope(Engine::V2);
}

#[test]
fn text_search_scalar_oracle() {
    let path = common::fresh("text-search-scalars");
    let admin = Admin::create_engine(&path, Limits::default(), default_engine()).unwrap();
    let mut s = admin.sql_session();
    for (sql, expected) in [
        (
            "SELECT to_tsvector('english', 'The Fat Rats')::text, to_tsquery('english', 'fat & rats')::text",
            vec![text("'fat':2 'rat':3"), text("'fat' & 'rat'")],
        ),
        (
            "SELECT strip('a:1A b:2B'::tsvector)::text, length('a b c'::tsvector), ts_delete('a:1 b:2 c:3', 'b')::text, ts_filter('a:1A b:2B c:3', '{a,b}')::text",
            vec![
                text("'a' 'b'"),
                Value::Int(3),
                text("'a':1 'c':3"),
                text("'a':1A 'b':2B"),
            ],
        ),
        (
            "SELECT setweight('a:1 b:2 c:3', 'A', '{a,c}')::text, array_to_tsvector('{c,a,b}')::text, tsvector_to_array('b:1 a:2')",
            vec![
                text("'a':1A 'b':2 'c':3A"),
                text("'a' 'b' 'c'"),
                text("{a,b}"),
            ],
        ),
        (
            "SELECT ('a'::tsquery && 'b')::text, ('a'::tsquery || 'b')::text, (!!'a'::tsquery)::text, ('a'::tsquery <-> 'b')::text, tsquery_phrase('a','b',2)::text",
            vec![
                text("'a' & 'b'"),
                text("'a' | 'b'"),
                text("!'a'"),
                text("'a' <-> 'b'"),
                text("'a' <2> 'b'"),
            ],
        ),
        (
            "SELECT 'fox' @@ 'fox'::tsquery, 'fox'::tsquery @@ 'fox'::tsvector, 'the fox jumped' @@ 'fox', 'fox'::tsvector @@ 'fox'::tsquery",
            vec![Value::Bool(true); 4],
        ),
        (
            "SELECT numnode('a & b'), querytree('a & !b'), ts_rewrite('a & b', 'a', 'c')::text",
            vec![Value::Int(3), text("'a'"), text("'b' & 'c'")],
        ),
        (
            "SELECT pg_typeof(to_tsvector(NULL)), pg_typeof(websearch_to_tsquery('english','x')), pg_typeof(ts_rank('x','x')), pg_typeof('english'::regconfig)",
            vec![
                text("tsvector"),
                text("tsquery"),
                text("real"),
                text("regconfig"),
            ],
        ),
        (
            "SELECT 'english'::regconfig::text, 'pg_catalog.english'::regconfig::oid, 'simple'::regconfig::oid, get_current_ts_config()::text",
            vec![
                text("english"),
                Value::Int(13248),
                Value::Int(3748),
                text("english"),
            ],
        ),
        (
            "SELECT to_tsvector('english', NULL), setweight(NULL::tsvector, 'A'), ts_rank(NULL, 'a'::tsquery), 'a'::tsvector || NULL",
            vec![Value::Null; 4],
        ),
        (
            "SELECT ts_lexize('english_stem','stars'), ts_lexize('simple','The')",
            vec![text("{star}"), text("{the}")],
        ),
    ] {
        assert_eq!(run(&mut s, sql), vec![expected], "{sql}");
    }
    assert_eq!(
        run(&mut s, "SELECT * FROM unnest('a:1A,2 b:3 c'::tsvector)"),
        vec![
            vec![text("a"), text("{1,2}"), text("{A,D}")],
            vec![text("b"), text("{3}"), text("{D}")],
            vec![text("c"), Value::Null, Value::Null]
        ],
    );
    let rank = run(
        &mut s,
        "SELECT ts_rank('{0.1,0.2,0.4,1}'::real[], 'a:1A', 'a', 2), ts_rank_cd(ARRAY[0.1,0.2,0.4,1]::real[], 'a:1A', 'a')",
    );
    assert_eq!(
        rank,
        vec![vec![
            Value::Float4(giggles_core::relational::Float4::parse("0.6079271").unwrap()),
            Value::Float4(giggles_core::relational::Float4::parse("1").unwrap()),
        ]]
    );
    expect_error(&mut s, "SELECT setweight('a:1'::tsvector,'E')", "22023");
    expect_error(&mut s, "SELECT to_tsvector('nosuch','x')", "42704");
    expect_error(&mut s, "SELECT 'cat &'::tsquery", "42601");
    expect_error(&mut s, "SELECT '{}'::tsvector[]", "0A000");
    drop(s);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

fn archdocs_search(engine: Engine) {
    let path = common::fresh("text-search-archdocs");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    let mut s = admin.sql_session();
    run(
        &mut s,
        r#"CREATE TABLE "Organization" ("id" UUID PRIMARY KEY)"#,
    );
    run(
        &mut s,
        r#"INSERT INTO "Organization" VALUES ('00000000-0000-0000-0000-0000000000aa')"#,
    );
    for statement in include_str!("fixtures/text_search_archdocs_setup.sql").split(';') {
        if !statement.trim().is_empty() {
            run(&mut s, statement);
        }
    }
    let clock = StatementClock {
        unavailable: None,
        transaction_us: 0,
        statement_us: 0,
        zone: Some(giggles_pg_types::temporal::SessionZone::Fixed(0)),
        wall: None,
    };
    let insert = r#"INSERT INTO "SearchDocument" ("entityType","entityId","organizationId","title","body","updatedAt") VALUES
        ('doc','00000000-0000-0000-0000-000000000001','00000000-0000-0000-0000-0000000000aa','Payment service architecture','The payment service talks to Stripe through the billing gateway. Retries are idempotent.', '2026-01-01'),
        ('design-doc','00000000-0000-0000-0000-000000000002','00000000-0000-0000-0000-0000000000aa','Search indexing design','We index documents into a tsvector column and rank results with ts_rank.', '2026-01-01'),
        ('object','00000000-0000-0000-0000-000000000003','00000000-0000-0000-0000-0000000000aa','Billing gateway','Gateway for payments; owns invoices.', '2026-01-01')"#;
    rows(s.execute_at(insert, &[], None, &clock), insert);

    let vectors = r#"SELECT tsv::text FROM "SearchDocument" ORDER BY title"#;
    let original = run(&mut s, vectors);
    assert_eq!(
        original,
        vec![
            vec![text(
                "'bill':1A 'gateway':2A,3B 'invoic':7B 'own':6B 'payment':5B"
            )],
            vec![text(
                "'architectur':3A 'bill':12B 'gateway':13B 'idempot':16B 'payment':1A,5B 'retri':14B 'servic':2A,6B 'stripe':9B 'talk':7B"
            )],
            vec![text(
                "'column':10B 'design':3A 'document':6B 'index':2A,5B 'rank':12B,16B 'result':13B 'search':1A 'ts':15B 'tsvector':9B"
            )],
        ]
    );
    let updated = run(
        &mut s,
        r#"UPDATE "SearchDocument" SET body = 'Payments are retried by the gateway' WHERE "entityType" = 'object' RETURNING tsv::text"#,
    );
    assert_eq!(
        updated,
        vec![vec![text(
            "'bill':1A 'gateway':2A,8B 'payment':3B 'retri':5B"
        )]]
    );
    for statement in include_str!("fixtures/text_search_archdocs_match_body.sql").split(';') {
        if !statement.trim().is_empty() {
            run(&mut s, statement);
        }
    }
    assert_eq!(
        run(&mut s, vectors),
        vec![updated[0].clone(), original[1].clone(), original[2].clone()]
    );
    run(
        &mut s,
        r#"UPDATE "SearchDocument" SET "matchOnlyBody" = 'journal entry about refunds' WHERE "entityType" = 'doc'"#,
    );

    let sql = "CREATE TABLE unstable (title text, tsv tsvector GENERATED ALWAYS AS (to_tsvector(title)) STORED)";
    expect_error(&mut s, sql, "42P17");
    assert_eq!(
        take_sql_error_message().as_deref(),
        Some("generation expression is not immutable")
    );
    expect_error(
        &mut s,
        r#"CREATE INDEX ordinary ON "SearchDocument" (tsv)"#,
        "0A000",
    );
    expect_error(
        &mut s,
        r#"CREATE INDEX expression ON "SearchDocument" USING gin (to_tsvector('english', title))"#,
        "0A000",
    );

    drop(s);
    drop(admin);
    let admin = Admin::open(&path, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    let catalog = s.catalog_metadata().unwrap();
    let gin = catalog.presentation.gin_indexes.values().next().unwrap();
    assert!(gin.name.ends_with("SearchDocument_tsv_idx"));
    assert_eq!(gin.opclasses, [10073]);
    let actual = run(&mut s, vectors);
    assert_eq!(
        actual,
        vec![
            updated[0].clone(),
            vec![text(
                "'architectur':3A 'bill':12B 'entri':18C 'gateway':13B 'idempot':16B 'journal':17C 'payment':1A,5B 'refund':20C 'retri':14B 'servic':2A,6B 'stripe':9B 'talk':7B"
            )],
            original[2].clone(),
        ]
    );

    // The generic joined-query planner requires an explicit bigint limit.
    let sql = include_str!("fixtures/text_search_archdocs_query.sql")
        .replace("LIMIT $3", "LIMIT ($3::bigint)");
    let prepared = s
        .prepare(&sql)
        .unwrap()
        .with_parameter_types(
            &[
                DataType::Uuid,
                DataType::Text,
                DataType::Integer,
                DataType::TextArray,
            ]
            .map(Some),
        )
        .unwrap();
    for (terms, expected) in [
        (
            "payment gateway",
            vec![("object", false, "0.763366"), ("doc", false, "0.35984504")],
        ),
        ("refunds", vec![("doc", true, "0.12158542")]),
        (
            r#""billing gateway" -invoices"#,
            vec![("object", false, "0.9953511"), ("doc", false, "0.39641288")],
        ),
        ("the", vec![]),
    ] {
        let result = rows(
            s.execute_prepared(
                &prepared,
                &[
                    text("00000000-0000-0000-0000-0000000000aa"),
                    text(terms),
                    Value::Int(10),
                    text("{doc,object,design-doc}"),
                ],
                None,
            ),
            &sql,
        );
        assert_eq!(result.len(), expected.len(), "{terms}");
        for (row, (entity, journal, rank)) in result.iter().zip(expected) {
            assert_eq!(row[0], text(entity));
            assert_eq!(row[2], Value::Bool(journal));
            assert_eq!(
                row[3],
                Value::Float4(giggles_core::relational::Float4::parse(rank).unwrap())
            );
        }
    }
    run(&mut s, r#"ALTER TABLE "SearchDocument" DROP COLUMN tsv"#);
    assert!(
        s.catalog_metadata()
            .unwrap()
            .presentation
            .gin_indexes
            .is_empty()
    );
    drop(s);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn text_search_archdocs_legacy() {
    archdocs_search(Engine::Legacy);
}

#[test]
#[cfg(feature = "engine-rocksdb")]
fn text_search_archdocs_v2() {
    archdocs_search(Engine::V2);
}
