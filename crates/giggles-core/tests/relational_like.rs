mod common;
use giggles_core::{
    Admin, CoreError, Limits, SqlSession,
    relational::{DataType as T, SqlError as E, SqlResult, Value as V},
};

fn database(test: impl FnOnce(&mut SqlSession)) {
    let path = common::fresh("relational-like");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    test(&mut session);
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

fn run(s: &mut SqlSession, sql: &str) -> SqlResult {
    s.execute(sql, &[], None).expect(sql)
}

fn ids(result: SqlResult) -> Vec<V> {
    result.rows.into_iter().map(|row| row.values[0].clone()).collect()
}

fn seed(s: &mut SqlSession) {
    run(s, "CREATE TABLE t (a BIGINT PRIMARY KEY, b TEXT)");
    run(s, "INSERT INTO t VALUES (1,'Alpha'),(2,'alpha'),(3,'Beta')");
}

#[test]
fn like_patterns_match_whole_strings_and_unicode_characters() {
    database(|s| {
        seed(s);
        for (predicate, expected) in [
            ("b LIKE 'Al%'", vec![1]),
            ("b LIKE '%ha'", vec![1, 2]),
            ("b LIKE '%ph%'", vec![1, 2]),
            ("b LIKE '____'", vec![3]),
            ("b LIKE '_____'", vec![1, 2]),
            ("b LIKE 'Alpha'", vec![1]),
            ("b LIKE 'lph'", vec![]),
            ("b LIKE ''", vec![]),
            ("'' LIKE ''", vec![1, 2, 3]),
            ("'' LIKE '%'", vec![1, 2, 3]),
            ("'é猫' LIKE '__'", vec![1, 2, 3]),
            ("'é猫' LIKE '_'", vec![]),
            ("'a%b' LIKE 'a\\%b'", vec![1, 2, 3]),
            ("'a_b' LIKE 'a\\_b'", vec![1, 2, 3]),
            ("'a%b' LIKE 'a!%b' ESCAPE '!'", vec![1, 2, 3]),
            ("'a_b' LIKE 'a!_b' ESCAPE '!'", vec![1, 2, 3]),
            ("'a!b' LIKE 'a!!b' ESCAPE '!'", vec![1, 2, 3]),
            ("'a%b' LIKE 'a猫%b' ESCAPE '猫'", vec![1, 2, 3]),
            ("'a\\xyz' LIKE 'a\\%' ESCAPE ''", vec![1, 2, 3]),
            ("'abc' LIKE 'a\\%' ESCAPE ''", vec![]),
        ] {
            let sql = format!("SELECT a FROM t WHERE {predicate} ORDER BY a");
            assert_eq!(ids(run(s, &sql)), expected.into_iter().map(V::Int).collect::<Vec<_>>(), "{predicate}");
        }
    });
}

#[test]
fn ilike_negation_and_nulls_keep_three_valued_predicate_semantics() {
    database(|s| {
        seed(s);
        for (predicate, expected) in [
            ("b ILIKE 'al%'", vec![1, 2]),
            ("b NOT LIKE 'Al%'", vec![2, 3]),
            ("b NOT ILIKE 'al%'", vec![3]),
            // GigglesDB ILIKE uses locale-independent one-to-one folding.
            ("'Ä' ILIKE 'ä'", vec![1, 2, 3]),
            ("'ä' ILIKE 'Ä'", vec![1, 2, 3]),
            ("'Ä' LIKE 'ä'", vec![]),
            ("'A%' ILIKE 'AÄ%' ESCAPE 'Ä'", vec![1, 2, 3]),
        ] {
            let sql = format!("SELECT a FROM t WHERE {predicate} ORDER BY a");
            assert_eq!(ids(run(s, &sql)), expected.into_iter().map(V::Int).collect::<Vec<_>>(), "{predicate}");
        }
        for operator in ["LIKE", "NOT LIKE", "ILIKE", "NOT ILIKE"] {
            for expression in [
                format!("NULL {operator} '%'"),
                format!("b {operator} NULL"),
                format!("b {operator} '%' ESCAPE NULL"),
            ] {
                assert!(run(s, &format!("SELECT a FROM t WHERE {expression}")).rows.is_empty(), "{expression}");
            }
        }
    });
}

#[test]
fn like_escape_errors_and_nontext_arguments_have_specific_errors() {
    database(|s| {
        seed(s);
        for (predicate, error) in [
            ("b LIKE '%' ESCAPE '!!'", E::InvalidEscapeSequence),
            ("b LIKE '%' ESCAPE 'é猫'", E::InvalidEscapeSequence),
            ("'abc' LIKE 'ab\\'", E::InvalidEscapeSequence),
            ("'abc' LIKE 'ab!' ESCAPE '!'", E::InvalidEscapeSequence),
            ("a LIKE '%'", E::UndefinedFunction),
            ("b LIKE 1", E::UndefinedFunction),
            ("b LIKE '%' ESCAPE 1", E::UndefinedFunction),
            ("a ILIKE '%'", E::UndefinedFunction),
        ] {
            let sql = format!("SELECT a FROM t WHERE {predicate}");
            assert_eq!(s.execute(&sql, &[], None), Err(CoreError::Sql(error)), "{predicate}");
        }
    });
}

#[test]
fn prepared_like_infers_all_text_operands() {
    database(|s| {
        seed(s);
        let pattern = s.prepare("SELECT a FROM t WHERE b LIKE $1 ORDER BY a").unwrap();
        assert_eq!(s.describe(&pattern).unwrap().parameters, [T::Text]);
        assert_eq!(ids(s.execute_prepared(&pattern, &[V::Text("Al%".into())], None).unwrap()), [V::Int(1)]);
        let all = s.prepare("SELECT a FROM t WHERE $1 ILIKE $2 ESCAPE $3 ORDER BY a").unwrap();
        assert_eq!(s.describe(&all).unwrap().parameters, [T::Text, T::Text, T::Text]);
        assert_eq!(ids(s.execute_prepared(&all, &[
            V::Text("A%".into()), V::Text("a!%".into()), V::Text("!".into()),
        ], None).unwrap()), [V::Int(1), V::Int(2), V::Int(3)]);
        for null_position in 0..3 {
            let mut arguments = [
                V::Text("Alpha".into()), V::Text("al%".into()), V::Text("!".into()),
            ];
            arguments[null_position] = V::Null;
            assert!(s.execute_prepared(&all, &arguments, None).unwrap().rows.is_empty(),
                "NULL operand {null_position}");
        }
        for sql in [
            "SELECT a FROM t WHERE $1 LIKE '%'",
            "SELECT a FROM t WHERE b LIKE $1",
            "SELECT a FROM t WHERE b LIKE '%' ESCAPE $1",
        ] {
            let declared = s.prepare(sql).unwrap()
                .with_parameter_types(&[Some(T::Integer)]).unwrap();
            assert_eq!(s.describe(&declared).unwrap_err(), CoreError::Sql(E::UndefinedFunction), "{sql}");
            assert_eq!(s.execute_prepared(&declared, &[V::Int(1)], None),
                Err(CoreError::Sql(E::UndefinedFunction)), "{sql}");
        }
    });
}

#[test]
fn like_residuals_apply_with_key_ranges_boolean_groups_and_mutations() {
    database(|s| {
        seed(s);
        for (predicate, expected) in [
            ("a>=1 AND a<=3 AND b LIKE 'Al%'", vec![1]),
            ("a>=2 AND a<=3 AND b ILIKE 'al%'", vec![2]),
            ("(a>=2 AND b LIKE 'Al%') OR a=3", vec![3]),
            ("a>=2 AND (b ILIKE 'al%' OR b LIKE 'Be%')", vec![2, 3]),
        ] {
            assert_eq!(ids(run(s, &format!("SELECT a FROM t WHERE {predicate} ORDER BY a"))),
                expected.into_iter().map(V::Int).collect::<Vec<_>>(), "{predicate}");
        }
        assert_eq!(run(s, "UPDATE t SET b='changed' WHERE a>=2 AND b ILIKE 'al%'").affected, 1);
        assert_eq!(ids(run(s, "SELECT a FROM t WHERE b LIKE 'changed' ORDER BY a")), [V::Int(2)]);
        assert_eq!(run(s, "DELETE FROM t WHERE b NOT ILIKE 'al%' AND a>=2").affected, 2);
        assert_eq!(ids(run(s, "SELECT a FROM t ORDER BY a")), [V::Int(1)]);
    });
}

#[test]
fn null_pattern_and_escape_precede_escape_validation_and_null_value() {
    database(|s| {
        seed(s);
        for operator in ["LIKE", "NOT LIKE", "ILIKE", "NOT ILIKE"] {
            let sql = format!("SELECT a FROM t WHERE NULL::TEXT {operator} 'a' ESCAPE 'xy'");
            assert_eq!(s.execute(&sql, &[], None), Err(CoreError::Sql(E::InvalidEscapeSequence)), "{sql}");
            for predicate in [
                format!("'a' {operator} NULL ESCAPE 'xy'"),
                format!("NULL::TEXT {operator} NULL ESCAPE 'xy'"),
                format!("NULL::TEXT {operator} 'a' ESCAPE NULL"),
            ] {
                assert!(run(s, &format!("SELECT a FROM t WHERE {predicate}")).rows.is_empty(), "{predicate}");
            }
        }
        run(s, "CREATE TABLE n (a BIGINT PRIMARY KEY, v TEXT, p TEXT, e TEXT)");
        run(s, "INSERT INTO n VALUES (1,NULL,'a','xy'),(2,'a',NULL,'xy'),(3,NULL,'a',NULL)");
        for operator in ["LIKE", "NOT LIKE", "ILIKE", "NOT ILIKE"] {
            let sql = format!("SELECT a FROM n WHERE a=1 AND v {operator} p ESCAPE e");
            assert_eq!(s.execute(&sql, &[], None), Err(CoreError::Sql(E::InvalidEscapeSequence)), "{sql}");
            for key in [2, 3] {
                let sql = format!("SELECT a FROM n WHERE a={key} AND v {operator} p ESCAPE e");
                assert!(run(s, &sql).rows.is_empty(), "{sql}");
            }
        }
    });
}
