mod common;
use bincode::Options;
use giggles_core::{
    Admin, Cancellation, CoreError, Limits, local,
    relational::{Copy, CopyFormat, DataType, MAX_ROW_BYTES, MAX_ROWS, SqlError as E, Value as V},
};

fn db(schema: &str) -> Admin {
    let admin = Admin::create(common::fresh("copy-ingest"), Limits::default()).unwrap();
    admin.sql_session().execute(schema, &[], None).unwrap();
    admin
}
fn text(s: &str) -> V {
    V::Text(s.into())
}
#[test]
fn envelope_grammar_and_rejections() {
    let c = Copy::parse(
        "COPY public.items (b,a) FROM STDIN WITH (FORMAT text, DELIMITER '|', NULL 'nil');",
    )
    .unwrap();
    assert_eq!(c.table, "items");
    assert_eq!(c.columns, ["b", "a"]);
    assert_eq!(c.delimiter, b'|');
    assert_eq!(c.null, "nil");
    assert_eq!(
        Copy::parse("COPY items FROM STDIN BINARY").unwrap().format,
        CopyFormat::Binary
    );
    assert_eq!(
        Copy::parse("COPY items FROM STDIN (FORMAT binary)")
            .unwrap()
            .format,
        CopyFormat::Binary
    );
    assert_eq!(
        Copy::parse("COPY items FROM STDIN (FORMAT csv)")
            .unwrap()
            .format,
        CopyFormat::Csv
    );
    for suffix in [
        "WITH (HEADER true)",
        "WITH (QUOTE 'x')",
        "WITH (ESCAPE 'x')",
        "WITH (FORCE_NULL (a))",
        "WITH (FORCE_NOT_NULL (a))",
        "WITH (FORCE_QUOTE (a))",
        "WITH (ENCODING 'UTF8')",
        "WITH (FREEZE true)",
        "WHERE a=1",
    ] {
        assert_eq!(
            Copy::parse(&format!("COPY items FROM STDIN {suffix}")),
            Err(E::Unsupported),
            "{suffix}"
        );
    }
    for sql in [
        "COPY items TO STDOUT",
        "COPY items FROM 'file'",
        "COPY items FROM PROGRAM 'cmd'",
    ] {
        assert_eq!(Copy::parse(sql), Err(E::Unsupported));
    }
    assert!(Copy::is_copy(
        " /*nested /*x*/ y*/ --comment\n CoPy items FROM STDIN '"
    ));
    assert_eq!(
        Copy::target("COPY public.items FROM STDIN (NULL '")
            .unwrap()
            .as_deref(),
        Some("items")
    );
    assert!(!Copy::is_copy("copycat"));
    assert_eq!(
        Copy::parse("COPY items FROM STDIN (DELIMITER 'xx')"),
        Err(E::InvalidParameterValue)
    );
}
#[test]
fn defaults_reordered_columns_heap_and_typed_insert_parity() {
    let admin = db(
        "CREATE TABLE items (id BIGINT PRIMARY KEY, n INTEGER, b BOOLEAN, bytes BYTEA, label TEXT DEFAULT 'default')",
    );
    let mut s = admin.sql_session();
    let (copy, p) = s
        .prepare_copy("COPY items (bytes,b,n,id) FROM STDIN")
        .unwrap();
    assert_eq!(p.command(), "INSERT");
    assert_eq!(
        s.copy_columns(&copy)
            .unwrap()
            .iter()
            .map(|c| c.data_type.clone())
            .collect::<Vec<_>>(),
        [
            DataType::Bytes,
            DataType::Boolean,
            DataType::Integer,
            DataType::BigInt
        ]
    );
    assert_eq!(
        s.ingest_copy(
            &p,
            vec![vec![text("\\x00ff"), text("yes"), text("42"), text("1")]]
        )
        .unwrap()
        .affected,
        1
    );
    s.execute(
        "INSERT INTO items (id,n,b,bytes) VALUES (2,42,true,CAST('\\x00ff' AS BYTEA))",
        &[],
        None,
    )
    .unwrap();
    let rows = s
        .execute(
            "SELECT n,b,bytes,label FROM items ORDER BY id LIMIT 10",
            &[],
            None,
        )
        .unwrap()
        .rows;
    assert_eq!(rows[0].values, rows[1].values);
    assert_eq!(
        rows[0].values,
        [
            V::Int(42),
            V::Bool(true),
            V::Bytes(vec![0, 255]),
            text("default")
        ]
    );
    s.execute("CREATE TABLE heap (n BIGINT)", &[], None)
        .unwrap();
    let (copy, p) = s.prepare_copy("COPY heap FROM STDIN").unwrap();
    assert_eq!(s.copy_columns(&copy).unwrap().len(), 1);
    assert_eq!(
        s.ingest_copy(&p, vec![vec![V::Int(8)], vec![V::Int(8)]])
            .unwrap()
            .affected,
        2
    );
}
#[test]
fn constraints_and_input_failures_are_atomic() {
    let admin = db("CREATE TABLE items (id BIGINT PRIMARY KEY, n INTEGER NOT NULL CHECK (n > 0))");
    let mut s = admin.sql_session();
    for rows in [
        vec![vec![V::Int(1), V::Int(1)], vec![V::Int(1), V::Int(2)]],
        vec![vec![V::Int(1), V::Int(1)], vec![V::Int(2), V::Null]],
        vec![vec![V::Int(1), V::Int(1)], vec![V::Int(2), V::Int(-1)]],
        vec![vec![V::Int(1), V::Int(1)], vec![V::Int(2), text("bad")]],
    ] {
        let (_, p) = s.prepare_copy("COPY items FROM STDIN").unwrap();
        assert!(s.ingest_copy(&p, rows).is_err());
        assert!(
            s.execute("SELECT * FROM items LIMIT 10", &[], None)
                .unwrap()
                .rows
                .is_empty()
        );
    }
}
#[test]
fn limits_cancellation_and_transaction_abort() {
    let admin = db("CREATE TABLE items (id BIGINT PRIMARY KEY, label TEXT)");
    let mut s = admin.sql_session();
    let (_, p) = s.prepare_copy("COPY items FROM STDIN").unwrap();
    assert!(matches!(
        s.ingest_copy(&p, vec![vec![V::Int(1), text("x")]; MAX_ROWS + 1]),
        Err(CoreError::Sql(E::Limit))
    ));
    let (_, p) = s.prepare_copy("COPY items FROM STDIN").unwrap();
    assert!(matches!(
        s.ingest_copy(
            &p,
            vec![vec![V::Int(1), V::Text("x".repeat(MAX_ROW_BYTES + 1))]]
        ),
        Err(CoreError::Sql(E::Limit))
    ));
    let (_, p) = s.prepare_copy("COPY items FROM STDIN").unwrap();
    let statement_rows = (0..17)
        .map(|id| vec![V::Int(id), V::Text("x".repeat(64_000))])
        .collect();
    assert!(matches!(
        s.ingest_copy(&p, statement_rows),
        Err(CoreError::Sql(E::Limit))
    ));
    assert!(
        s.execute("SELECT * FROM items LIMIT 10", &[], None)
            .unwrap()
            .rows
            .is_empty()
    );
    let (_, p) = s.prepare_copy("COPY items FROM STDIN").unwrap();
    let cancel = Cancellation::default();
    cancel.cancel();
    assert!(matches!(
        cancel.scope(|| s.ingest_copy(&p, vec![vec![V::Int(1), text("x")]])),
        Err(CoreError::Cancelled)
    ));
    s.execute("BEGIN", &[], None).unwrap();
    let (_, p) = s.prepare_copy("COPY items FROM STDIN").unwrap();
    assert!(s.ingest_copy(&p, vec![vec![V::Null, text("x")]]).is_err());
    assert!(s.is_aborted());
    s.execute("ROLLBACK", &[], None).unwrap();
    assert!(
        s.execute("SELECT * FROM items LIMIT 10", &[], None)
            .unwrap()
            .rows
            .is_empty()
    );
}
#[test]
fn copy_error_append_contract() {
    let error = E::BadCopyFileFormat;
    let le = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_little_endian()
        .reject_trailing_bytes();
    let be = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_big_endian()
        .reject_trailing_bytes();
    assert_eq!(le.serialize(&error).unwrap(), [50, 0, 0, 0]);
    assert_eq!(be.serialize(&error).unwrap(), [0, 0, 0, 50]);
    assert_eq!(le.deserialize::<E>(&[50, 0, 0, 0]).unwrap(), error);
    assert_eq!(be.deserialize::<E>(&[0, 0, 0, 50]).unwrap(), error);
    assert!(le.deserialize::<E>(&[58, 0, 0, 0]).is_err());
    assert!(be.deserialize::<E>(&[0, 0, 0, 58]).is_err());
    assert_eq!(
        serde_json::to_string(&error).unwrap(),
        "\"BadCopyFileFormat\""
    );
    assert_eq!(
        serde_json::from_str::<E>("\"BadCopyFileFormat\"").unwrap(),
        error
    );
    assert_eq!(local::failure(CoreError::Sql(error)).code, 250);
}

fn clock() -> giggles_core::relational::StatementClock {
    giggles_core::relational::StatementClock {
        unavailable: None,
        transaction_us: 0,
        statement_us: 0,
        zone: None,
        wall: None,
    }
}
#[test]
fn row_normalization_checks_early_and_reserves_defaults_once() {
    let admin = db(
        "CREATE TABLE items (id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, n INTEGER NOT NULL CHECK (n > 0))",
    );
    let mut s = admin.sql_session();
    let (_, p) = s.prepare_copy("COPY items (n) FROM STDIN").unwrap();
    assert!(!s.implicit_sequence_dependencies(&p).unwrap().is_empty());
    let (normalized, rows) = s
        .normalize_copy_rows_at(&p, vec![vec![text("1")]], &clock())
        .unwrap();
    assert_eq!(rows[0], [V::Int(1), V::Int(1)]);
    assert!(
        s.implicit_sequence_dependencies(&normalized)
            .unwrap()
            .is_empty()
    );
    s.ingest_copy_at(&normalized, rows, &clock()).unwrap();
    s.execute_at("INSERT INTO items (n) VALUES (2)", &[], None, &clock())
        .unwrap();
    assert_eq!(
        s.execute("SELECT id FROM items ORDER BY id LIMIT 10", &[], None)
            .unwrap()
            .rows
            .iter()
            .map(|r| r.values[0].clone())
            .collect::<Vec<_>>(),
        [V::Int(1), V::Int(2)]
    );
    let (_, p) = s.prepare_copy("COPY items (n) FROM STDIN").unwrap();
    assert!(matches!(
        s.normalize_copy_rows_at(&p, vec![vec![V::Int(-1)]], &clock()),
        Err(CoreError::Sql(E::CheckViolation))
    ));
    let (_, p) = s.prepare_copy("COPY items (n) FROM STDIN").unwrap();
    assert!(matches!(
        s.normalize_copy_rows_at(&p, vec![vec![V::Null]], &clock()),
        Err(CoreError::Sql(E::Null))
    ));
}
#[test]
fn foreign_key_forward_reference_and_successful_transaction_rollback() {
    let admin =
        db("CREATE TABLE nodes (id BIGINT PRIMARY KEY, parent BIGINT REFERENCES nodes(id))");
    let mut s = admin.sql_session();
    let (_, p) = s.prepare_copy("COPY nodes FROM STDIN").unwrap();
    let (normalized, rows) = s
        .normalize_copy_rows_at(
            &p,
            vec![vec![V::Int(1), V::Int(2)], vec![V::Int(2), V::Null]],
            &clock(),
        )
        .unwrap();
    assert_eq!(
        s.ingest_copy_at(&normalized, rows, &clock())
            .unwrap()
            .affected,
        2
    );
    let (_, p) = s.prepare_copy("COPY nodes FROM STDIN").unwrap();
    assert!(matches!(
        s.ingest_copy(&p, vec![vec![V::Int(3), V::Int(999)]]),
        Err(CoreError::Sql(E::ForeignKeyViolation))
    ));
    s.execute("BEGIN", &[], None).unwrap();
    let (_, p) = s.prepare_copy("COPY nodes FROM STDIN").unwrap();
    s.ingest_copy(&p, vec![vec![V::Int(3), V::Int(1)]]).unwrap();
    s.execute("ROLLBACK", &[], None).unwrap();
    assert_eq!(
        s.execute("SELECT * FROM nodes LIMIT 10", &[], None)
            .unwrap()
            .rows
            .len(),
        2
    );
}

#[test]
fn malformed_boolean_text_is_copy_local_invalid_input() {
    let admin = db("CREATE TABLE flags (id BIGINT PRIMARY KEY, flag BOOLEAN)");
    let mut s = admin.sql_session();
    let (_, p) = s.prepare_copy("COPY flags FROM STDIN").unwrap();
    assert!(matches!(
        s.ingest_copy(&p, vec![vec![V::Int(1), text("maybe")]]),
        Err(CoreError::Sql(E::InvalidTextRepresentation))
    ));
    for sql in [
        "INSERT INTO flags VALUES (1,'maybe')",
        "INSERT INTO flags VALUES (1,'maybe'),(2,'true')",
    ] {
        // PG18.6: unknown literals take the column input function (22P02).
        assert!(matches!(
            s.execute(sql, &[], None),
            Err(CoreError::Sql(E::InvalidTextRepresentation))
        ));
    }
    let (_, p) = s.prepare_copy("COPY flags FROM STDIN").unwrap();
    assert!(matches!(
        s.normalize_copy_rows_at(&p, vec![vec![V::Int(1), text("maybe")]], &clock()),
        Err(CoreError::Sql(E::InvalidTextRepresentation))
    ));
}

#[test]
fn target_extraction_is_utf8_safe_before_malformed_body() {
    for sql in [
        "COPY items??",
        "COPY items ?? FROM STDIN",
        "COPY items FROM STDIN (NULL '??",
    ] {
        assert_eq!(Copy::target(sql).unwrap().as_deref(), Some("items"));
    }
    assert_eq!(
        Copy::target("COPY \"??\" FROM STDIN").unwrap().as_deref(),
        Some("??")
    );
    assert_eq!(
        Copy::parse(&format!(
            "COPY items FROM STDIN {}",
            " ".repeat(giggles_core::relational::MAX_STATEMENT_SQL)
        )),
        Err(E::Limit)
    );
}

#[test]
fn cancelled_normalization_returns_cancelled_and_aborts_transaction() {
    let admin = db("CREATE TABLE items (id BIGINT PRIMARY KEY)");
    let mut s = admin.sql_session();
    s.execute("BEGIN", &[], None).unwrap();
    let (_, p) = s.prepare_copy("COPY items FROM STDIN").unwrap();
    let cancellation = Cancellation::default();
    cancellation.cancel();
    assert!(matches!(
        cancellation.scope(|| s.normalize_copy_rows_at(&p, vec![vec![V::Int(1)]], &clock())),
        Err(CoreError::Cancelled)
    ));
    assert!(s.is_aborted());
    s.execute("ROLLBACK", &[], None).unwrap();
    assert!(
        s.execute("SELECT * FROM items LIMIT 10", &[], None)
            .unwrap()
            .rows
            .is_empty()
    );
}

#[test]
fn all_copy_text_types_equal_typed_insert_without_assignment_changes() {
    // Exercise every DataType through its existing scalar input conversion.
    for (ty, input) in [
        ("BOOLEAN", "yes"),
        ("SMALLINT", "-123"),
        ("INTEGER", "123"),
        ("BIGINT", "9223372036854775807"),
        ("TEXT", "hello"),
        ("BYTEA", "\\x00ff"),
        ("NUMERIC", "123.50"),
        ("REAL", "1.25"),
        ("DOUBLE PRECISION", "1.25"),
        ("DATE", "2024-01-02"),
        ("TIME", "03:04:05"),
        ("TIME WITH TIME ZONE", "03:04:05+02"),
        ("TIMESTAMP", "2024-01-02 03:04:05"),
        ("TIMESTAMP WITH TIME ZONE", "2024-01-02 03:04:05"),
        ("INTERVAL", "1 day 02:03:04"),
    ] {
        let admin = db(&format!(
            "CREATE TABLE typed (id BIGINT PRIMARY KEY, v {ty})"
        ));
        let mut s = admin.sql_session();
        let (_, p) = s.prepare_copy("COPY typed FROM STDIN").unwrap();
        let (normalized, rows) = s
            .normalize_copy_rows_at(&p, vec![vec![text("1"), text(input)]], &clock())
            .unwrap_or_else(|e| panic!("COPY {ty}: {e:?}"));
        s.ingest_copy_at(&normalized, rows, &clock()).unwrap();
        s.execute_at(
            &format!("INSERT INTO typed VALUES (2,CAST('{input}' AS {ty}))"),
            &[],
            None,
            &clock(),
        )
        .unwrap_or_else(|e| panic!("typed INSERT {ty}: {e:?}"));
        let rows = s
            .execute("SELECT v FROM typed ORDER BY id LIMIT 10", &[], None)
            .unwrap()
            .rows;
        assert_eq!(rows[0].values, rows[1].values, "{ty}");
    }
}
