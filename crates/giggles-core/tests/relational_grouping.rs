//! PostgreSQL 18.6 oracle: e3-query/wo-aggregates/pg-probes.txt.
use giggles_core::{
    relational::{DataType as T, Numeric, SqlError as E, SqlResult, Value as V},
    *,
};
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(0);
fn db() -> (Admin, SqlSession) {
    let path = std::env::temp_dir().join(format!(
        "gd-grouping-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let a = Admin::create(path, Limits::default()).unwrap();
    let s = a.sql_session();
    (a, s)
}
fn run(s: &mut SqlSession, q: &str) -> SqlResult {
    s.execute(q, &[], None)
        .unwrap_or_else(|e| panic!("{q}: {e:?}"))
}
fn rows(s: &mut SqlSession, q: &str) -> Vec<Vec<V>> {
    run(s, q).rows.into_iter().map(|r| r.values).collect()
}
fn fixture(s: &mut SqlSession) {
    run(
        s,
        "CREATE TABLE agg_probe(id int PRIMARY KEY,k int,v int,t text,n numeric,b bigint,f float8)",
    );
    run(
        s,
        "INSERT INTO agg_probe VALUES(1,1,10,'A',1.0,10,10),(2,1,NULL,'A',1.00,20,20),(3,2,30,'B',2,30,30),(4,NULL,40,NULL,NULL,40,40),(5,NULL,NULL,NULL,NULL,50,50)",
    );
}
fn text(v: &str) -> V {
    V::Text(v.into())
}
fn numeric(v: &str) -> V {
    V::Numeric(Numeric::parse(v).unwrap())
}
#[test]
fn grouped_columns_nulls_and_existing_aggregates() {
    let (_a, mut s) = db();
    fixture(&mut s);
    assert_eq!(
        rows(
            &mut s,
            "SELECT k,count(*),count(v),sum(v),min(v),max(v) FROM agg_probe GROUP BY k ORDER BY k"
        ),
        vec![
            vec![
                V::Int(1),
                V::Int(2),
                V::Int(1),
                V::Int(10),
                V::Int(10),
                V::Int(10)
            ],
            vec![
                V::Int(2),
                V::Int(1),
                V::Int(1),
                V::Int(30),
                V::Int(30),
                V::Int(30)
            ],
            vec![
                V::Null,
                V::Int(2),
                V::Int(1),
                V::Int(40),
                V::Int(40),
                V::Int(40)
            ]
        ]
    );
    assert_eq!(
        rows(
            &mut s,
            "SELECT t,count(*) FROM agg_probe GROUP BY t ORDER BY t"
        ),
        vec![
            vec![text("A"), V::Int(2)],
            vec![text("B"), V::Int(1)],
            vec![V::Null, V::Int(2)]
        ]
    );
    assert_eq!(
        rows(
            &mut s,
            "SELECT n,count(*) FROM agg_probe GROUP BY n ORDER BY n"
        ),
        vec![
            vec![numeric("1.0"), V::Int(2)],
            vec![numeric("2"), V::Int(1)],
            vec![V::Null, V::Int(2)]
        ]
    );
}
#[test]
fn expressions_ordinals_aliases_and_bound_column_identity() {
    let (_a, mut s) = db();
    fixture(&mut s);
    for sql in [
        "SELECT k+1 AS bucket,count(*) FROM agg_probe GROUP BY 1 ORDER BY bucket",
        "SELECT k+1 AS bucket,count(*) FROM agg_probe GROUP BY bucket ORDER BY bucket",
    ] {
        assert_eq!(
            rows(&mut s, sql),
            vec![
                vec![V::Int(2), V::Int(2)],
                vec![V::Int(3), V::Int(1)],
                vec![V::Null, V::Int(2)]
            ]
        );
    }
    assert_eq!(
        rows(
            &mut s,
            "SELECT lower(t)||'!' AS x,count(*) FROM agg_probe GROUP BY lower(t) ORDER BY x"
        ),
        vec![
            vec![text("a!"), V::Int(2)],
            vec![text("b!"), V::Int(1)],
            vec![V::Null, V::Int(2)]
        ]
    );
    assert_eq!(
        rows(
            &mut s,
            "SELECT agg_probe.k,count(*) FROM agg_probe GROUP BY k ORDER BY k"
        ),
        vec![
            vec![V::Int(1), V::Int(2)],
            vec![V::Int(2), V::Int(1)],
            vec![V::Null, V::Int(2)]
        ]
    );
}
#[test]
fn having_global_grouped_parameters_and_aggregate_order() {
    let (_a, mut s) = db();
    fixture(&mut s);
    assert_eq!(
        rows(
            &mut s,
            "SELECT k,count(*) FROM agg_probe GROUP BY k HAVING sum(v)>20 ORDER BY sum(v) DESC LIMIT 1 OFFSET 1"
        ),
        vec![vec![V::Int(2), V::Int(1)]]
    );
    assert_eq!(
        rows(&mut s, "SELECT count(*) FROM agg_probe HAVING count(*)>1"),
        vec![vec![V::Int(5)]]
    );
    assert_eq!(
        rows(&mut s, "SELECT 1 FROM agg_probe HAVING true"),
        vec![vec![V::Int(1)]]
    );
    assert_eq!(
        rows(
            &mut s,
            "SELECT k,count(*) FROM agg_probe GROUP BY k HAVING count(*)>0 ORDER BY count(*) DESC,k LIMIT 1 OFFSET 1"
        ),
        vec![vec![V::Null, V::Int(2)]]
    );
    let p = s
        .prepare("SELECT k,count(*) FROM agg_probe GROUP BY k HAVING count(*)>$1 ORDER BY k")
        .unwrap();
    assert_eq!(s.describe(&p).unwrap().parameters, vec![T::BigInt]);
    let r = s.execute_prepared(&p, &[V::Int(1)], None).unwrap();
    assert_eq!(
        r.rows.into_iter().map(|r| r.values).collect::<Vec<_>>(),
        vec![vec![V::Int(1), V::Int(2)], vec![V::Null, V::Int(2)]]
    );
}
#[test]
fn invalid_grouping_is_42803_and_alias_input_priority() {
    let (_a, mut s) = db();
    fixture(&mut s);
    for sql in [
        "SELECT k AS v,count(*) FROM agg_probe GROUP BY v",
        "SELECT t FROM agg_probe GROUP BY lower(t)",
        "SELECT v,count(*) FROM agg_probe GROUP BY k",
        "SELECT k,count(*) FROM agg_probe GROUP BY k HAVING v>1",
        "SELECT k,count(*) FROM agg_probe GROUP BY k ORDER BY v",
    ] {
        assert_eq!(
            s.execute(sql, &[], None),
            Err(CoreError::Sql(E::GroupingError)),
            "{sql}"
        );
    }
    assert_eq!(
        s.execute(
            "SELECT k AS x,v AS x,count(*) FROM agg_probe GROUP BY x",
            &[],
            None
        ),
        Err(CoreError::Sql(E::Duplicate))
    );
}
#[test]
fn describe_existing_aggregate_types_and_empty_sets() {
    let (_a, mut s) = db();
    fixture(&mut s);
    let p = s
        .prepare(
            "SELECT k,count(*),count(v),sum(v),min(v),max(v),sum(b),sum(n),sum(f) FROM agg_probe GROUP BY k",
        )
        .unwrap();
    assert_eq!(
        s.describe(&p)
            .unwrap()
            .columns
            .into_iter()
            .map(|c| c.data_type)
            .collect::<Vec<_>>(),
        vec![
            T::Integer,
            T::BigInt,
            T::BigInt,
            T::BigInt,
            T::Integer,
            T::Integer,
            T::Numeric,
            T::Numeric,
            T::DoublePrecision
        ]
    );
    assert!(
        rows(
            &mut s,
            "SELECT k,count(*),sum(v) FROM agg_probe WHERE id<0 GROUP BY k"
        )
        .is_empty()
    );
    assert_eq!(
        rows(
            &mut s,
            "SELECT count(*),count(v),sum(v),min(v),max(v) FROM agg_probe WHERE id<0"
        ),
        vec![vec![V::Int(0), V::Int(0), V::Null, V::Null, V::Null]]
    );
}
#[test]
fn grouping_composes_with_existing_key_join() {
    let (_a, mut s) = db();
    fixture(&mut s);
    run(
        &mut s,
        "CREATE TABLE agg_join(id int PRIMARY KEY,label text)",
    );
    run(&mut s, "INSERT INTO agg_join VALUES(1,'one'),(2,'two')");
    assert_eq!(
        rows(
            &mut s,
            "SELECT j.label,count(*),sum(p.v) FROM agg_probe p JOIN agg_join j ON p.k=j.id GROUP BY j.label ORDER BY j.label"
        ),
        vec![
            vec![text("one"), V::Int(2), V::Int(10)],
            vec![text("two"), V::Int(1), V::Int(30)]
        ]
    );
}
#[test]
fn cancelled_grouped_read_aborts_transaction() {
    let (_a, mut s) = db();
    fixture(&mut s);
    run(&mut s, "BEGIN");
    let cancel = Cancellation::default();
    cancel.cancel();
    assert_eq!(
        cancel.scope(|| s.execute("SELECT k,count(*) FROM agg_probe GROUP BY k", &[], None)),
        Err(CoreError::Cancelled)
    );
    assert!(s.is_aborted());
    assert_eq!(
        s.execute("SELECT k,count(*) FROM agg_probe GROUP BY k", &[], None),
        Err(CoreError::Sql(E::Aborted))
    );
    s.rollback();
    assert_eq!(
        rows(&mut s, "SELECT count(*) FROM agg_probe HAVING count(*)>1"),
        vec![vec![V::Int(5)]]
    );
}
#[test]
fn grouping_key_and_having_work_budgets_reset_per_row() {
    let (_a, mut s) = db();
    run(&mut s, "CREATE TABLE cap_probe(id int PRIMARY KEY)");
    let values = (1..=24)
        .map(|id| format!("({id})"))
        .collect::<Vec<_>>()
        .join(",");
    run(&mut s, &format!("INSERT INTO cap_probe VALUES {values}"));
    assert_eq!(
        rows(
            &mut s,
            "SELECT count(*) FROM cap_probe WHERE id<=24 GROUP BY repeat('0123456789abcdef',4000)||id::text HAVING length(repeat('0123456789abcdef',4000))=64000 ORDER BY count(*);"
        ),
        vec![vec![V::Int(1)]; 24]
    );
}
#[test]
fn group_key_retention_is_bounded_before_output_paging() {
    let (_a, mut s) = db();
    run(&mut s, "CREATE TABLE cap_probe(id int PRIMARY KEY)");
    for start in (1..=1024).step_by(128) {
        let values = (start..start + 128)
            .map(|id| format!("({id})"))
            .collect::<Vec<_>>()
            .join(",");
        run(&mut s, &format!("INSERT INTO cap_probe VALUES {values}"));
    }
    // PG returns 1024 rows of 1; GigglesDB's documented 64 MiB retention cap
    // includes key/accumulator allocation overhead and returns Limit.
    assert_eq!(s.execute("SELECT count(*) FROM cap_probe GROUP BY repeat(lpad(id::text,4,'0'),16380) ORDER BY count(*)",&[],None),Err(CoreError::Sql(E::Limit)));
}
#[test]
fn where_and_having_parameters_keep_independent_types() {
    let (_a, mut s) = db();
    fixture(&mut s);
    let p = s
        .prepare(
            "SELECT k,count(*) FROM agg_probe WHERE id>$1 GROUP BY k HAVING count(*)>$2 ORDER BY k",
        )
        .unwrap();
    assert_eq!(
        s.describe(&p).unwrap().parameters,
        vec![T::Integer, T::BigInt]
    );
    assert_eq!(
        s.execute_prepared(&p, &[V::Int(1), V::Int(1)], None)
            .unwrap()
            .rows
            .into_iter()
            .map(|r| r.values)
            .collect::<Vec<_>>(),
        vec![vec![V::Null, V::Int(2)]]
    );
}
#[test]
fn identical_output_aliases_and_successful_input_name_priority() {
    let (_a, mut s) = db();
    fixture(&mut s);
    assert_eq!(
        rows(
            &mut s,
            "SELECT k AS x,k AS x,count(*) FROM agg_probe GROUP BY x ORDER BY x"
        ),
        vec![
            vec![V::Int(1), V::Int(1), V::Int(2)],
            vec![V::Int(2), V::Int(2), V::Int(1)],
            vec![V::Null, V::Null, V::Int(2)]
        ]
    );
    // GROUP BY k uses the input k even though the projected v is also named k.
    assert_eq!(
        rows(
            &mut s,
            "SELECT v AS k,count(*) FROM agg_probe GROUP BY v,k ORDER BY v,k"
        ),
        vec![
            vec![V::Int(10), V::Int(1)],
            vec![V::Int(30), V::Int(1)],
            vec![V::Int(40), V::Int(1)],
            vec![V::Null, V::Int(1)],
            vec![V::Null, V::Int(1)]
        ]
    );
}
#[test]
fn multiple_grouping_keys_output_ordinals_and_key_having() {
    let (_a, mut s) = db();
    fixture(&mut s);
    assert_eq!(
        rows(
            &mut s,
            "SELECT k,t,count(*) FROM agg_probe GROUP BY k,t ORDER BY 1,2"
        ),
        vec![
            vec![V::Int(1), text("A"), V::Int(2)],
            vec![V::Int(2), text("B"), V::Int(1)],
            vec![V::Null, V::Null, V::Int(2)]
        ]
    );
    assert_eq!(
        rows(
            &mut s,
            "SELECT k,t,count(*) FROM agg_probe GROUP BY k,t HAVING k>1 ORDER BY 1,2"
        ),
        vec![vec![V::Int(2), text("B"), V::Int(1)]]
    );
}
#[test]
fn grouping_noop_cast_preserves_identity_but_widening_does_not() {
    let (_a, mut s) = db();
    fixture(&mut s);
    assert_eq!(
        rows(&mut s, "SELECT k FROM agg_probe GROUP BY k::int ORDER BY k"),
        vec![vec![V::Int(1)], vec![V::Int(2)], vec![V::Null]]
    );
    assert_eq!(
        s.execute(
            "SELECT k FROM agg_probe GROUP BY k::bigint ORDER BY k",
            &[],
            None
        ),
        Err(CoreError::Sql(E::GroupingError))
    );
}
#[test]
fn requested_compat_corpus_executes_through_sql_session() {
    let cases = [
        (
            "group_by_pk_dependency_django",
            include_str!("../../../compat/corpus/aggregates/group_by_pk_dependency_django.sql"),
        ),
        (
            "group_by_count",
            include_str!("../../../compat/corpus/aggregates/group_by_count.sql"),
        ),
        (
            "group_by_having",
            include_str!("../../../compat/corpus/aggregates/group_by_having.sql"),
        ),
        (
            "group_by_expression",
            include_str!("../../../compat/corpus/aggregates/group_by_expression.sql"),
        ),
        (
            "group_by_ordinal_alias",
            include_str!("../../../compat/corpus/aggregates/group_by_ordinal_alias.sql"),
        ),
        (
            "empty_group_and_global",
            include_str!("../../../compat/corpus/aggregates/empty_group_and_global.sql"),
        ),
    ];
    for (name, source) in cases {
        let (_a, mut s) = db();
        let sql = source
            .lines()
            .filter(|line| !line.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");
        for statement in sql.split(';').map(str::trim).filter(|sql| !sql.is_empty()) {
            s.execute(statement, &[], None)
                .unwrap_or_else(|error| panic!("compat case {name}, SQL {statement}: {error:?}"));
        }
    }
}

// PostgreSQL 18.6 oracle: e3-query/wo-fundep/pg-probes.txt.
fn dependency_fixture(s: &mut SqlSession) {
    run(s, "CREATE TABLE t(id int PRIMARY KEY,name text,age int)");
    run(
        s,
        "CREATE TABLE u(id int PRIMARY KEY,t_id int,name text NOT NULL)",
    );
    run(
        s,
        "INSERT INTO t VALUES(1,'Ada',30),(2,'Bob',40),(3,'Cy',20)",
    );
    run(
        s,
        "INSERT INTO u VALUES(11,1,'one'),(12,1,'two'),(13,2,'three')",
    );
}
#[test]
fn declared_primary_key_dependency_runs_django_left_join_shape() {
    let (_a, mut s) = db();
    dependency_fixture(&mut s);
    assert_eq!(
        rows(
            &mut s,
            r#"SELECT "t"."id", "t"."name", "t"."age", COUNT("u"."id") AS "u__count" FROM "t" LEFT OUTER JOIN "u" ON ("t"."id" = "u"."t_id") GROUP BY "t"."id" ORDER BY "t"."id""#
        ),
        vec![
            vec![V::Int(1), text("Ada"), V::Int(30), V::Int(2)],
            vec![V::Int(2), text("Bob"), V::Int(40), V::Int(1)],
            vec![V::Int(3), text("Cy"), V::Int(20), V::Int(0)],
        ]
    );
    // PostgreSQL 18.6: ordinary non-key LEFT joins preserve unmatched rows.
    assert_eq!(
        rows(
            &mut s,
            "SELECT t.id,u.name FROM t LEFT JOIN u ON t.id=u.t_id ORDER BY t.id,u.name"
        ),
        vec![
            vec![V::Int(1), text("one")],
            vec![V::Int(1), text("two")],
            vec![V::Int(2), text("three")],
            vec![V::Int(3), V::Null]
        ]
    );
}
#[test]
fn primary_key_dependency_resolves_aliases_ordinals_casts_and_extra_groups() {
    let (_a, mut s) = db();
    dependency_fixture(&mut s);
    let expected = vec![
        vec![V::Int(1), text("Ada"), V::Int(1)],
        vec![V::Int(2), text("Bob"), V::Int(1)],
        vec![V::Int(3), text("Cy"), V::Int(1)],
    ];
    for sql in [
        "SELECT id,name,count(*) FROM t GROUP BY t.id ORDER BY id",
        "SELECT t.id,t.name,count(*) FROM t GROUP BY id ORDER BY id",
        "SELECT id AS key,name,count(*) FROM t GROUP BY key ORDER BY key",
        "SELECT id,name,count(*) FROM t GROUP BY 1 ORDER BY id",
        "SELECT id,name,count(*) FROM t GROUP BY CAST(id AS int) ORDER BY id",
        "SELECT id,name,count(*) FROM t GROUP BY age+1,id ORDER BY id",
    ] {
        assert_eq!(rows(&mut s, sql), expected, "{sql}");
    }
    assert_eq!(
        rows(
            &mut s,
            "SELECT id,name,age,count(*) FROM t GROUP BY id HAVING age>=30 ORDER BY name"
        ),
        vec![
            vec![V::Int(1), text("Ada"), V::Int(30), V::Int(1)],
            vec![V::Int(2), text("Bob"), V::Int(40), V::Int(1)],
        ]
    );
    // All dependent slots here exist solely for HAVING/ORDER BY.
    assert_eq!(
        rows(
            &mut s,
            "SELECT count(*) FROM t GROUP BY id HAVING age>=30 ORDER BY name"
        ),
        vec![vec![V::Int(1)]; 2]
    );
    // Aggregate slots and deduplicated dependent slots remain stable in every order.
    assert_eq!(
        rows(
            &mut s,
            "SELECT count(*),name,sum(age),name,age FROM t GROUP BY id HAVING count(*)=1 AND age>=30 ORDER BY name"
        ),
        vec![
            vec![V::Int(1), text("Ada"), V::Int(30), text("Ada"), V::Int(30)],
            vec![V::Int(1), text("Bob"), V::Int(40), text("Bob"), V::Int(40)],
        ]
    );
    assert_eq!(
        rows(
            &mut s,
            "SELECT upper(name),count(*) FROM t GROUP BY id ORDER BY age"
        ),
        vec![
            vec![text("CY"), V::Int(1)],
            vec![text("ADA"), V::Int(1)],
            vec![text("BOB"), V::Int(1)],
        ]
    );
}
#[test]
fn composite_primary_key_dependency_requires_every_plain_key() {
    let (_a, mut s) = db();
    run(
        &mut s,
        "CREATE TABLE c(a int,b int,name text,PRIMARY KEY(a,b))",
    );
    run(&mut s, "INSERT INTO c VALUES(1,1,'x'),(1,2,'y'),(2,1,'z')");
    assert_eq!(
        rows(
            &mut s,
            "SELECT a,b,name,count(*) FROM c GROUP BY b,a ORDER BY a,b"
        ),
        vec![
            vec![V::Int(1), V::Int(1), text("x"), V::Int(1)],
            vec![V::Int(1), V::Int(2), text("y"), V::Int(1)],
            vec![V::Int(2), V::Int(1), text("z"), V::Int(1)],
        ]
    );
    for sql in [
        "SELECT a,name,count(*) FROM c GROUP BY a",
        "SELECT b,name,count(*) FROM c GROUP BY b",
        "SELECT name,count(*) FROM c GROUP BY a,b+0",
    ] {
        assert_eq!(
            s.execute(sql, &[], None),
            Err(CoreError::Sql(E::GroupingError)),
            "{sql}"
        );
    }
}
#[test]
fn dependency_ownership_and_nullable_left_join_columns_match_postgres() {
    let (_a, mut s) = db();
    dependency_fixture(&mut s);
    assert_eq!(
        rows(
            &mut s,
            "SELECT t.id,t.name,count(*) FROM t JOIN u ON t.id=u.t_id GROUP BY t.id ORDER BY t.id"
        ),
        vec![
            vec![V::Int(1), text("Ada"), V::Int(2)],
            vec![V::Int(2), text("Bob"), V::Int(1)],
        ]
    );
    let sql = "SELECT t.id,t.name,u.id,u.name,count(*) FROM t LEFT JOIN u ON t.id=u.t_id GROUP BY t.id,u.id ORDER BY t.id,u.id";
    assert_eq!(
        rows(&mut s, sql),
        vec![
            vec![V::Int(1), text("Ada"), V::Int(11), text("one"), V::Int(1)],
            vec![V::Int(1), text("Ada"), V::Int(12), text("two"), V::Int(1)],
            vec![V::Int(2), text("Bob"), V::Int(13), text("three"), V::Int(1)],
            vec![V::Int(3), text("Cy"), V::Null, V::Null, V::Int(1)],
        ]
    );
    let p = s.prepare(sql).unwrap();
    let description = s.describe(&p).unwrap();
    assert!(description.columns[0].not_null);
    assert!(!description.columns[2].not_null);
    assert!(!description.columns[3].not_null);
    assert_eq!(
        rows(
            &mut s,
            "SELECT u.id,u.name,count(*) FROM t LEFT JOIN u ON t.id=u.t_id GROUP BY u.id ORDER BY u.id"
        ),
        vec![
            vec![V::Int(11), text("one"), V::Int(1)],
            vec![V::Int(12), text("two"), V::Int(1)],
            vec![V::Int(13), text("three"), V::Int(1)],
            vec![V::Null, V::Null, V::Int(1)],
        ]
    );
    for sql in [
        "SELECT t.id,u.name,count(*) FROM t JOIN u ON t.id=u.t_id GROUP BY t.id",
        "SELECT t.id,u.name,count(*) FROM t LEFT JOIN u ON t.id=u.t_id GROUP BY t.id",
        "SELECT t.id,v.name,count(*) FROM t JOIN t v ON t.id=v.id GROUP BY t.id",
        "SELECT t.id,count(*) FROM t JOIN t v ON t.id=v.id GROUP BY t.id HAVING v.age>0",
        "SELECT t.id,count(*) FROM t JOIN t v ON t.id=v.id GROUP BY t.id ORDER BY v.name",
    ] {
        assert_eq!(
            s.execute(sql, &[], None),
            Err(CoreError::Sql(E::GroupingError)),
            "{sql}"
        );
    }
}
#[test]
fn expressions_heaps_and_unique_indexes_do_not_establish_group_dependency() {
    let (_a, mut s) = db();
    dependency_fixture(&mut s);
    run(&mut s, "CREATE TABLE h(id int,name text)");
    run(&mut s, "INSERT INTO h VALUES(1,'heap')");
    run(&mut s, "CREATE TABLE ui(id int NOT NULL,name text)");
    run(&mut s, "CREATE UNIQUE INDEX ui_id ON ui(id)");
    run(&mut s, "INSERT INTO ui VALUES(1,'index')");
    for sql in [
        "SELECT name,count(*) FROM t GROUP BY id+0",
        "SELECT name,count(*) FROM t GROUP BY abs(id)",
        "SELECT name,count(*) FROM t GROUP BY id::bigint",
        "SELECT id,name,count(*) FROM h GROUP BY id",
        "SELECT id,name,count(*) FROM ui GROUP BY id",
    ] {
        assert_eq!(
            s.execute(sql, &[], None),
            Err(CoreError::Sql(E::GroupingError)),
            "{sql}"
        );
    }
    // Existing reserved-identifier policy rejects the physical heap key before
    // name resolution; PostgreSQL instead reports undefined_column (42703).
    for sql in [
        "SELECT giggles_rowid,name,count(*) FROM h GROUP BY giggles_rowid",
        "SELECT h.giggles_rowid,name,count(*) FROM h GROUP BY h.giggles_rowid",
    ] {
        assert_eq!(
            s.execute(sql, &[], None),
            Err(CoreError::Sql(E::Reserved)),
            "{sql}"
        );
    }
}
#[test]
fn grouped_scan_join_above_legacy_cap_preserves_group_counts() {
    let (_a, mut s) = db();
    run(&mut s, "CREATE TABLE join_cap_t(id int PRIMARY KEY)");
    run(
        &mut s,
        "CREATE TABLE join_cap_u(id int PRIMARY KEY,t_id int)",
    );
    for start in [1, 129] {
        let values = (start..start + 128)
            .map(|n| format!("({n})"))
            .collect::<Vec<_>>()
            .join(",");
        run(&mut s, &format!("INSERT INTO join_cap_t VALUES {values}"));
        let values = (start..start + 128)
            .map(|n| format!("({n},1)"))
            .collect::<Vec<_>>()
            .join(",");
        run(&mut s, &format!("INSERT INTO join_cap_u VALUES {values}"));
    }
    // Verify both sides of the former 65,536 candidate boundary.
    assert_eq!(
        rows(
            &mut s,
            "SELECT count(u.id) FROM join_cap_t t LEFT JOIN join_cap_u u ON t.id=u.t_id GROUP BY t.id ORDER BY t.id LIMIT 1"
        ),
        vec![vec![V::Int(256)]]
    );
    run(&mut s, "INSERT INTO join_cap_t VALUES(257)");
    assert_eq!(
        rows(
            &mut s,
            "SELECT t.id,count(u.id) FROM join_cap_t t LEFT JOIN join_cap_u u ON t.id=u.t_id GROUP BY t.id ORDER BY t.id"
        ),
        (1..=257)
            .map(|id| vec![V::Int(id), V::Int(if id == 1 { 256 } else { 0 })])
            .collect::<Vec<_>>()
    );
}

#[test]
fn primary_key_dependency_composes_across_chained_scan_left_joins() {
    let (_a, mut s) = db();
    dependency_fixture(&mut s);
    run(&mut s, "CREATE TABLE v(id int PRIMARY KEY,u_id int)");
    run(&mut s, "INSERT INTO v VALUES(101,11),(102,11),(103,13)");
    assert_eq!(
        rows(
            &mut s,
            "SELECT t.id,t.name,count(v.id) FROM t LEFT JOIN u ON t.id=u.t_id LEFT JOIN v ON u.id=v.u_id GROUP BY t.id ORDER BY t.id"
        ),
        vec![
            vec![V::Int(1), text("Ada"), V::Int(2)],
            vec![V::Int(2), text("Bob"), V::Int(1)],
            vec![V::Int(3), text("Cy"), V::Int(0)],
        ]
    );
}
