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
fn character_casts_are_rejected_not_silently_untyped() {
    // PG18.6 truncates explicit character casts silently ('abcdef'::varchar(3) = 'abc').
    // The cast grammar has no varchar/char spelling yet (DD-0068 F19 owns cast typmods),
    // so every form is rejected (0A000) rather than dropping the typmod or describing
    // the result as text (25). DD68 follow-up flips these.
    let a = Admin::create(fresh("e2-character-casts"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    for sql in [
        "SELECT 'abcdef'::varchar(3)",
        "SELECT 'abc'::character varying(2)",
        "SELECT 'abc'::varchar",
        "SELECT 'abc'::char(3)",
        "SELECT 'abc'::bpchar",
        "SELECT CAST('abcdef' AS varchar(3))",
    ] {
        assert_eq!(
            s.execute(sql, &[], None).map(|_| ()),
            Err(CoreError::Sql(SqlError::Unsupported)),
            "{sql}"
        );
    }
}
