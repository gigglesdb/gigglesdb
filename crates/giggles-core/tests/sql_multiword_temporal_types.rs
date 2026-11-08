//! PG 18.6 spelling oracle: e1-sql/evidence/oracle-pg186-multiword-temporal-types.txt.
//! CREATE precision and array support deliberately follow the existing aliases.
mod common;

use giggles_core::{
    Admin, CoreError, Limits,
    relational::{DataType, SqlError, SqlResult},
};
use std::path::PathBuf;

struct Database {
    root: PathBuf,
    admin: Option<Admin>,
}
impl Database {
    fn new() -> Self {
        // common::fresh honors GIGGLES_TEST_ARTIFACTS, set under e1-sql/tmp/tz.
        let root = common::fresh("multiword-temporal-types");
        std::fs::create_dir_all(&root).unwrap();
        let admin = Admin::create(root.join("db"), Limits::default()).unwrap();
        Self {
            root,
            admin: Some(admin),
        }
    }
    fn admin(&self) -> &Admin {
        self.admin.as_ref().unwrap()
    }
    fn run(&self, sql: &str) -> Result<SqlResult, CoreError> {
        self.admin().sql_session().execute(sql, &[], None)
    }
    fn restore(&self) -> Admin {
        let export = self.root.join("witness.export");
        self.admin().export(&export).unwrap();
        Admin::restore(export, self.root.join("restored"), Limits::default()).unwrap()
    }
}
impl Drop for Database {
    fn drop(&mut self) {
        self.admin.take();
        std::fs::remove_dir_all(&self.root).unwrap();
    }
}

fn cases() -> [(&'static str, &'static str, DataType, bool); 7] {
    [
        (
            "timestamp with time zone",
            "timestamptz",
            DataType::TimestampTz,
            false,
        ),
        (
            "timestamp without time zone",
            "timestamp",
            DataType::Timestamp,
            false,
        ),
        ("time with time zone", "timetz", DataType::TimeTz, false),
        ("time without time zone", "time", DataType::Time, false),
        (
            "timestamp(3) with time zone",
            "timestamptz(3)",
            DataType::TimestampTz,
            true,
        ),
        ("time(2) without time zone", "time(2)", DataType::Time, true),
        (
            "TIMESTAMP WITH TIME ZONE",
            "timestamptz",
            DataType::TimestampTz,
            false,
        ),
    ]
}

#[test]
fn all_seven_oracle_forms_follow_create_alias_policy_independently() {
    for (spelling, alias, data_type, precision) in cases() {
        let long = Database::new();
        let short = Database::new();
        let result = long.run(&format!("CREATE TABLE t (v {spelling})"));
        let alias_result = short.run(&format!("CREATE TABLE t (v {alias})"));
        if precision {
            assert_eq!(result, Err(CoreError::Sql(SqlError::Syntax)), "{spelling}");
            assert_eq!(alias_result, result, "{alias}");
            assert!(long.admin().catalog().unwrap().1.is_empty());
            continue;
        }
        result.unwrap();
        alias_result.unwrap();
        let catalog = long.admin().catalog().unwrap();
        assert_eq!(
            catalog.1[0].logical_columns().next().unwrap().data_type,
            data_type
        );
        assert_eq!(catalog, short.admin().catalog().unwrap(), "{spelling}");
        assert_eq!(
            long.admin().catalog_typmods().unwrap(),
            short.admin().catalog_typmods().unwrap()
        );
        // Same-name, same-id catalog definitions survive the persisted boundary.
        let restored_long = long.restore();
        let restored_short = short.restore();
        assert_eq!(
            restored_long.catalog().unwrap(),
            restored_short.catalog().unwrap()
        );
        assert_eq!(restored_long.catalog().unwrap(), catalog);
    }
}

#[test]
fn all_seven_forms_add_columns_with_identical_alias_typmods() {
    for (spelling, alias, data_type, _) in cases() {
        let long = Database::new();
        let short = Database::new();
        for db in [&long, &short] {
            db.run("CREATE TABLE t (id int PRIMARY KEY)").unwrap();
        }
        long.run(&format!("ALTER TABLE t ADD COLUMN v {spelling}"))
            .unwrap();
        short
            .run(&format!("ALTER TABLE t ADD COLUMN v {alias}"))
            .unwrap();
        let catalog = long.admin().catalog().unwrap();
        assert_eq!(catalog.1[0].columns[1].data_type, data_type);
        assert_eq!(catalog, short.admin().catalog().unwrap(), "{spelling}");
        assert_eq!(
            long.admin().catalog_typmods().unwrap(),
            short.admin().catalog_typmods().unwrap(),
            "{spelling}"
        );
    }
    for (base, alias) in [
        ("timestamp with time zone", "timestamptz"),
        ("timestamp without time zone", "timestamp"),
        ("time with time zone", "timetz"),
        ("time without time zone", "time"),
    ] {
        let (first, suffix) = base.split_once(' ').unwrap();
        for precision in [0, 2, 3, 6] {
            let long = Database::new();
            let short = Database::new();
            for db in [&long, &short] {
                db.run("CREATE TABLE t (id int PRIMARY KEY)").unwrap();
            }
            long.run(&format!(
                "ALTER TABLE t ADD v {first}({precision}) {suffix}"
            ))
            .unwrap();
            short
                .run(&format!("ALTER TABLE t ADD v {alias}({precision})"))
                .unwrap();
            assert_eq!(
                long.admin().catalog().unwrap(),
                short.admin().catalog().unwrap()
            );
            let long_mods = long.admin().catalog_typmods().unwrap();
            assert_eq!(long_mods, short.admin().catalog_typmods().unwrap());
            assert_eq!(
                long_mods.1.values().copied().collect::<Vec<_>>(),
                vec![precision]
            );
        }
    }
}

#[test]
fn both_alter_type_forms_parse_valid_types_but_keep_conversion_unsupported() {
    let db = Database::new();
    db.run("CREATE TABLE t (v timestamp)").unwrap();
    let before = db.admin().catalog().unwrap();
    for (spelling, alias, _, _) in cases() {
        for verb in ["TYPE", "SET DATA TYPE"] {
            for ty in [spelling, alias] {
                let sql = format!("ALTER TABLE t ALTER COLUMN v {verb} {ty}");
                assert_eq!(
                    db.run(&sql),
                    Err(CoreError::Sql(SqlError::Unsupported)),
                    "{sql}"
                );
            }
        }
    }
    assert_eq!(db.admin().catalog().unwrap(), before);
}

#[test]
fn malformed_suffix_matrix_is_syntax_at_every_ddl_site() {
    let db = Database::new();
    db.run("CREATE TABLE t (v timestamp)").unwrap();
    let before = db.admin().catalog().unwrap();
    for base in ["timestamp", "time"] {
        for suffix in [
            "with",
            "without",
            "with zone",
            "without zone",
            "with time",
            "without time",
            "with local time zone",
            "without local time zone",
            "with time zone(3)",
            "without time zone(3)",
            "with time zone with time zone",
            "without time zone without time zone",
            "with time zone without time zone",
            "without time zone with time zone",
        ] {
            for spelling in [format!("{base} {suffix}"), format!("{base}(3) {suffix}")] {
                for sql in [
                    format!("CREATE TABLE malformed (v {spelling})"),
                    format!("ALTER TABLE t ADD COLUMN bad {spelling}"),
                    format!("ALTER TABLE t ALTER COLUMN v TYPE {spelling}"),
                    format!("ALTER TABLE t ALTER COLUMN v SET DATA TYPE {spelling}"),
                ] {
                    assert_eq!(db.run(&sql), Err(CoreError::Sql(SqlError::Syntax)), "{sql}");
                }
            }
        }
    }
    // A valid but unsupported TYPE must not conceal a later malformed action.
    assert_eq!(
        db.run("ALTER TABLE t ALTER v TYPE timestamp with time zone, ADD bad time with zone"),
        Err(CoreError::Sql(SqlError::Syntax))
    );
    assert_eq!(db.admin().catalog().unwrap(), before);
}

#[test]
fn quoted_names_defaults_and_timestamp_values_match_aliases() {
    let db = Database::new();
    db.run("CREATE TABLE named (\"timestamp\" timestamp with time zone, \"with\" int)")
        .unwrap();
    db.run("INSERT INTO named VALUES ('2024-01-02 03:04:05+02', 7)")
        .unwrap();
    db.run("ALTER TABLE named ADD COLUMN \"time\" time with time zone NOT NULL DEFAULT '12:00+00'")
        .unwrap();
    db.run("CREATE TABLE defaulted (v time with time zone NOT NULL DEFAULT '12:00+00')")
        .unwrap();
    db.run("INSERT INTO defaulted DEFAULT VALUES").unwrap();
    assert_eq!(
        db.run("SELECT \"time\" FROM named").unwrap().rows[0].values,
        db.run("SELECT v FROM defaulted").unwrap().rows[0].values
    );
    let values = db.run("SELECT \"timestamp\", \"with\" FROM named").unwrap();
    assert_eq!(values.rows.len(), 1);
    for verb in ["TYPE", "SET DATA TYPE"] {
        assert_eq!(
            db.run(&format!(
                "ALTER TABLE named ALTER COLUMN \"timestamp\" {verb} timestamp with time zone"
            )),
            Err(CoreError::Sql(SqlError::Unsupported))
        );
    }
    let long = Database::new();
    let short = Database::new();
    long.run("CREATE TABLE t (v timestamp with time zone)")
        .unwrap();
    short.run("CREATE TABLE t (v timestamptz)").unwrap();
    for db in [&long, &short] {
        db.run("INSERT INTO t VALUES ('2024-01-02 03:04:05+02')")
            .unwrap();
    }
    assert_eq!(
        long.run("SELECT v FROM t").unwrap(),
        short.run("SELECT v FROM t").unwrap()
    );
    assert_eq!(
        db.run("SELECT \"timestamp\" FROM named").unwrap().rows,
        long.run("SELECT v FROM t").unwrap().rows
    );
}

#[test]
fn temporal_arrays_retain_alias_rejection_at_each_ddl_site() {
    let db = Database::new();
    db.run("CREATE TABLE t (v timestamp)").unwrap();
    for (spelling, alias, _, _) in cases() {
        for ty in [spelling, alias] {
            for sql in [
                format!("CREATE TABLE arrays (v {ty}[])"),
                format!("ALTER TABLE t ADD COLUMN a {ty}[]"),
                format!("ALTER TABLE t ALTER COLUMN v TYPE {ty}[]"),
                format!("ALTER TABLE t ALTER COLUMN v SET DATA TYPE {ty}[]"),
            ] {
                // '[' is outside the admitted lexer, before DDL type parsing.
                assert_eq!(
                    db.run(&sql),
                    Err(CoreError::Sql(SqlError::Unsupported)),
                    "{sql}"
                );
            }
        }
    }
}
