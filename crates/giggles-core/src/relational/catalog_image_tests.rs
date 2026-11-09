//! Rows-free catalog reconstruction and v2 dispatch boundaries.
use super::*;

fn run(state: &mut Relational, sql: &str) {
    let prepared = Prepared::parse(sql, 1, state.epoch)
        .unwrap_or_else(|error| panic!("parse {sql}: {error:?}"));
    state
        .execute(&prepared, &[], None)
        .unwrap_or_else(|error| panic!("execute {sql}: {error:?}"));
}

fn fixture() -> Relational {
    let mut state = Relational::default();
    run(
        &mut state,
        "CREATE TABLE parent (id serial PRIMARY KEY, v text DEFAULT 'x')",
    );
    run(
        &mut state,
        "CREATE TABLE child (id BIGINT PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
    );
    run(&mut state, "INSERT INTO parent (id, v) VALUES (1, 'y')");
    state
}

fn plain_fixture() -> Relational {
    // No CHECK/FK: v2 images refuse those until enforcement exists (E1 release).
    let mut state = Relational::default();
    run(
        &mut state,
        "CREATE TABLE parent (id serial PRIMARY KEY, v text DEFAULT 'x')",
    );
    run(&mut state, "INSERT INTO parent (id, v) VALUES (1, 'y')");
    state
}

#[test]
fn catalog_image_roundtrip_is_rows_free() {
    let mut original = plain_fixture();
    original.generation = 42;
    original.epoch = 19;
    let (tables, mut ext, next_id, next_version) = original.catalog_parts();
    // Normalization uses the existing canonical metadata rules.
    ext.columns.insert(u64::MAX, ColumnExt::default());
    ext.constraints.insert(u64::MAX, vec![]);
    ext.ext_version = 99;
    let image = Relational::catalog_image(tables, ext, next_id, next_version).unwrap();
    assert_eq!(image.catalog_parts(), original.catalog_parts());
    assert_eq!((image.format, image.generation, image.epoch), (1, 0, 0));
    assert_eq!(image.rows.len(), image.tables.len());
    assert!(image.rows.values().all(BTreeMap::is_empty));
    assert_eq!(image.secondary, secondary_index::Sidecar::default());
    assert!(original.rows.values().any(|rows| !rows.is_empty()));
    image.validate().unwrap();
}

fn assert_same_validation_error(state: Relational) {
    let expected = state.validate().unwrap_err();
    assert_eq!(expected, SqlError::Type);
    let (tables, ext, next_id, next_version) = state.catalog_parts();
    assert_eq!(
        Relational::catalog_image(tables, ext, next_id, next_version).unwrap_err(),
        expected
    );
}

#[test]
fn catalog_image_preserves_full_validation_errors() {
    let mut state = fixture();
    let column = state.tables["parent"].columns[0].id;
    state.ext.columns.entry(column).or_default().identity = Some(Identity {
        always: false,
        sequence_id: u64::MAX,
    });
    assert_same_validation_error(state);

    let mut state = fixture();
    let fk = state
        .ext
        .constraints
        .values_mut()
        .flatten()
        .find(|constraint| matches!(constraint.kind, ConstraintKind::ForeignKey { .. }))
        .unwrap();
    if let ConstraintKind::ForeignKey { columns, .. } = &mut fk.kind {
        columns[0] = u64::MAX;
    }
    assert_same_validation_error(state);

    let mut state = fixture();
    let table = state.tables.get_mut("parent").unwrap();
    table.columns[1].id = table.columns[0].id;
    assert_same_validation_error(state);
}

#[test]
fn v2_statement_classes_and_reachable_refusals() {
    use V2Class::*;
    let cases = [
        ("BEGIN", Control("begin")),
        ("COMMIT", Control("commit")),
        ("ROLLBACK", Control("rollback")),
        ("SELECT 1", Read),
        ("SELECT id FROM t", Read),
        ("INSERT INTO t VALUES (1)", Dml),
        ("INSERT INTO t VALUES (1), (2)", Dml),
        ("INSERT INTO t VALUES (1 + 2)", Dml),
        ("INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING", Dml),
        ("UPDATE t SET id = 2", Dml),
        ("UPDATE t SET id = id + 1", Dml),
        ("DELETE FROM t", Dml),
        ("CREATE TABLE t (id BIGINT PRIMARY KEY)", Ddl),
        (
            "CREATE TABLE t (id BIGINT PRIMARY KEY, n BIGINT CHECK (n > 0))",
            Refused("SQL statement"),
        ),
        (
            "CREATE TABLE t (id BIGINT PRIMARY KEY, n BIGINT, CHECK (n > 0))",
            Refused("SQL statement"),
        ),
        (
            "CREATE TABLE t (id BIGINT PRIMARY KEY, p BIGINT REFERENCES q(id))",
            Refused("SQL statement"),
        ),
        (
            "CREATE TABLE t (id BIGINT PRIMARY KEY, p BIGINT, FOREIGN KEY (p) REFERENCES q(id))",
            Refused("SQL statement"),
        ),
        ("DROP TABLE t", Ddl),
        ("CREATE SEQUENCE s", Ddl),
        ("DROP SEQUENCE s", Ddl),
        ("ALTER TABLE t RENAME COLUMN n TO m", Ddl),
        ("ALTER TABLE t RENAME TO u", Ddl),
        ("ALTER TABLE t ALTER COLUMN n SET DEFAULT 1", Ddl),
        ("ALTER TABLE t ALTER COLUMN n DROP NOT NULL", Ddl),
        ("ALTER TABLE t ALTER COLUMN n DROP DEFAULT", Ddl),
        (
            "CREATE TABLE t (id BIGINT PRIMARY KEY, n varchar(10))",
            Refused("varchar/char types"),
        ),
        (
            "CREATE TABLE t (id BIGINT PRIMARY KEY, n char(3))",
            Refused("varchar/char types"),
        ),
        (
            "CREATE TABLE t (id BIGINT PRIMARY KEY, n character varying(5))",
            Refused("varchar/char types"),
        ),
        (
            "ALTER TABLE t ADD COLUMN n varchar(10)",
            Refused("varchar/char types"),
        ),
        ("CREATE INDEX i ON t (n)", Refused("secondary indexes")),
        ("INSERT INTO t SELECT id, n FROM t", Refused("INSERT ... SELECT")),
        (
            "CREATE TABLE t (id BIGINT PRIMARY KEY, n BIGINT UNIQUE)",
            Refused("secondary indexes"),
        ),
        ("ALTER TABLE t ADD UNIQUE (n)", Refused("secondary indexes")),
        (
            "ALTER TABLE t ADD COLUMN n BIGINT",
            Refused("ALTER TABLE row rewrite"),
        ),
        (
            "ALTER TABLE t DROP COLUMN n",
            Refused("ALTER TABLE row rewrite"),
        ),
        (
            "ALTER TABLE t ALTER COLUMN n SET NOT NULL",
            Refused("ALTER TABLE row rewrite"),
        ),
        (
            "ALTER TABLE t ADD PRIMARY KEY (n)",
            Refused("SQL statement"),
        ),
        ("ALTER TABLE t ADD CHECK (n > 0)", Refused("SQL statement")),
        (
            "ALTER TABLE t ADD FOREIGN KEY (n) REFERENCES u(id)",
            Refused("SQL statement"),
        ),
        (
            "ALTER TABLE t VALIDATE CONSTRAINT c",
            Refused("SQL statement"),
        ),
        ("ALTER TABLE t DROP CONSTRAINT c", Refused("SQL statement")),
        ("SET CONSTRAINTS ALL DEFERRED", Refused("SQL statement")),
        ("SAVEPOINT s", Refused("SQL statement")),
        ("RELEASE SAVEPOINT s", Refused("SQL statement")),
        ("ROLLBACK TO SAVEPOINT s", Refused("SQL statement")),
        (
            "ALTER TABLE t ALTER COLUMN n SET DEFAULT 1, ADD COLUMN m BIGINT",
            Refused("ALTER TABLE row rewrite"),
        ),
        (
            "ALTER TABLE t ADD COLUMN m BIGINT, ADD UNIQUE (n)",
            Refused("secondary indexes"),
        ),
        (
            "ALTER TABLE t ADD UNIQUE (n), ADD COLUMN m BIGINT",
            Refused("secondary indexes"),
        ),
    ];
    for (sql, expected) in cases {
        let prepared =
            Prepared::parse(sql, 1, 0).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
        assert_eq!(prepared.v2_class(), expected, "{sql}");
    }
    // This shape currently has a dedicated AlterTable::Unique parser route;
    // cover Definition::Unique too so either representation remains refused.
    let prepared = Prepared::engine(
        Statement::AlterTable(alter_table::AlterTable {
            table: "t".into(),
            actions: vec![alter_table::Action::Constraint(
                constraints::Definition::Unique {
                    name: None,
                    columns: vec!["n".into()],
                },
            )],
            resolution_error: None,
        }),
        1,
        0,
    );
    assert_eq!(prepared.v2_class(), Refused("secondary indexes"));
}

#[test]
fn catalog_image_refuses_unenforced_check_and_foreign_key() {
    // Valid catalogs (validate() passes) that carry CHECK or FK fail closed on v2.
    for sql in [
        "CREATE TABLE c (id BIGINT PRIMARY KEY, n BIGINT CHECK (n > 0))",
        "CREATE TABLE f (id BIGINT PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
        // E1 r5: a legacy-created varchar/char column fails closed too.
        "CREATE TABLE t (id BIGINT PRIMARY KEY, v varchar(10))",
        "CREATE TABLE b (id BIGINT PRIMARY KEY, v char(3))",
    ] {
        let mut state = plain_fixture();
        run(&mut state, sql);
        state.validate().unwrap();
        let (tables, ext, next_id, next_version) = state.catalog_parts();
        assert_eq!(
            Relational::catalog_image(tables, ext, next_id, next_version).unwrap_err(),
            SqlError::Unsupported,
            "{sql}"
        );
    }
}
