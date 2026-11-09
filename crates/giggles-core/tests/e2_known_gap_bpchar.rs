mod common;
use common::fresh;
use giggles_core::{Admin, CoreError, Limits, relational::SqlError};

#[test]
fn bpchar_scalar_consumers_fail_closed_pending_dd68_semantics() {
    let a = Admin::create(fresh("e2-bpchar-known-gaps"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    s.execute("CREATE TABLE t (id int PRIMARY KEY, c char(3))", &[], None)
        .unwrap();
    s.execute("INSERT INTO t VALUES (1,'a')", &[], None)
        .unwrap();
    for sql in [
        // DD68 followup must implement trailing-space comparison semantics; task1 fails closed (0A000).
        "SELECT c = 'a' FROM t",
        // DD68 followup must implement trailing-space ordering semantics; task1 fails closed (0A000).
        "SELECT c < 'b' FROM t",
        // DD68 followup must implement bpchar sort semantics; task1 fails closed (0A000).
        "SELECT c FROM t ORDER BY c",
        // DD68 followup must implement bpchar join comparison semantics; task1 fails closed (0A000).
        "SELECT a.id FROM t a JOIN t b ON a.c=b.c",
        // DD68 followup must qualify bpchar pattern semantics; task1 fails closed (0A000).
        "SELECT c LIKE 'a%' FROM t",
        // DD68 followup must trim padding for character length; task1 fails closed (0A000).
        "SELECT length(c) FROM t",
        // DD68 followup must retain padding for byte length; task1 fails closed (0A000).
        "SELECT octet_length(c) FROM t",
        // DD68 followup must qualify bpchar concatenation conversion; task1 fails closed (0A000).
        "SELECT c || 'x' FROM t",
        // DD68 followup must qualify bpchar function conversion; task1 fails closed (0A000).
        "SELECT upper(c) FROM t",
        // DD68 followup must qualify bpchar common-type resolution; task1 fails closed (0A000).
        "SELECT coalesce(c,'x') FROM t",
        // DD68 followup must qualify bpchar CASE type resolution; task1 fails closed (0A000).
        "SELECT CASE WHEN id=1 THEN c ELSE 'x' END FROM t",
        // DD68 followup must qualify bpchar set-operation semantics; task1 fails closed (0A000).
        "SELECT c FROM t UNION SELECT c FROM t",
        // DD68 followup must trim padding for char_length; task1 fails closed (0A000).
        "SELECT char_length(c) FROM t",
        // DD68 followup must qualify bpchar concat() conversion; task1 fails closed (0A000).
        "SELECT concat(c, 'x') FROM t",
        // DD68 followup must qualify bpchar function conversion; task1 fails closed (0A000).
        "SELECT lower(c) FROM t",
        // DD68 followup must qualify bpchar greatest/least resolution; task1 fails closed (0A000).
        "SELECT greatest(c, 'b') FROM t",
        "SELECT least(c, 'b') FROM t",
        // Keys: bpchar is not a primary-key or secondary-index key type until DD68 defines
        // trailing-space-insensitive key encoding; both fail closed (0A000).
        "CREATE TABLE k (c char(3) PRIMARY KEY)",
        "CREATE INDEX t_c ON t (c)",
    ] {
        assert_eq!(
            s.execute(sql, &[], None),
            Err(CoreError::Sql(SqlError::Unsupported)),
            "{sql}"
        );
    }
}

#[test]
fn character_casts_follow_pg18() {
    use giggles_core::{
        SqlSession,
        relational::{DataType as T, Value},
    };

    fn check_query(
        session: &mut SqlSession,
        failures: &mut Vec<String>,
        sql: &str,
        expected_text: &str,
        expected_type: T,
        expected_typmod: i32,
    ) {
        let prepared = match session.prepare(sql) {
            Ok(prepared) => prepared,
            Err(error) => {
                failures.push(format!("{sql}: prepare: {error:?}"));
                return;
            }
        };
        match session.describe(&prepared) {
            Ok(description) => {
                let types: Vec<_> = description
                    .columns
                    .iter()
                    .map(|column| column.data_type.clone())
                    .collect();
                if types != [expected_type.clone()] {
                    failures.push(format!(
                        "{sql}: types {types:?}, expected [{expected_type:?}]"
                    ));
                }
                if description.typmods != [expected_typmod] {
                    failures.push(format!(
                        "{sql}: typmods {:?}, expected [{expected_typmod}]",
                        description.typmods
                    ));
                }
            }
            Err(error) => failures.push(format!("{sql}: describe: {error:?}")),
        }
        match session.execute_prepared(&prepared, &[], None) {
            Ok(result) => {
                let rows: Vec<_> = result.rows.iter().map(|row| row.values.clone()).collect();
                let expected = vec![vec![Value::Text(expected_text.into())]];
                if rows != expected {
                    failures.push(format!("{sql}: rows {rows:?}, expected {expected:?}"));
                }
            }
            Err(error) => failures.push(format!("{sql}: execute: {error:?}")),
        }
    }

    // PG18.6 explicit character casts (DD-0068 F19): silent truncation, char(n) padding, unbounded varchar/bpchar without a typmod.
    let a = Admin::create(fresh("e2-character-casts"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    let mut failures = Vec::new();
    for (sql, text, ty, typmod) in [
        ("SELECT 'abcdef'::varchar(3)", "abc", T::Varchar, 7),
        ("SELECT 'abc'::character varying(2)", "ab", T::Varchar, 6),
        ("SELECT 'abc'::varchar", "abc", T::Varchar, -1),
        ("SELECT 'abc'::char(3)", "abc", T::Bpchar, 7),
        ("SELECT 'ab'::char(3)", "ab ", T::Bpchar, 7),
        ("SELECT 'abc'::bpchar", "abc", T::Bpchar, -1),
        ("SELECT CAST('abcdef' AS varchar(3))", "abc", T::Varchar, 7),
    ] {
        check_query(&mut s, &mut failures, sql, text, ty, typmod);
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
