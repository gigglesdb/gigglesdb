mod common;

use giggles_core::{
    Admin, Engine, Limits, SqlSession,
    relational::{Value, take_sql_error_message, take_sql_notices},
    sqlstate,
};

fn run(sql: &mut SqlSession, statement: &str, tag: &str) {
    let prepared = sql
        .prepare(statement)
        .unwrap_or_else(|e| panic!("{statement}: {e:?}"));
    assert_eq!(prepared.command(), tag, "{statement}");
    sql.execute(statement, &[], None)
        .unwrap_or_else(|e| panic!("{statement}: {e:?} {:?}", take_sql_error_message()));
}

fn fail(sql: &mut SqlSession, statement: &str, state: &str, message: &str) {
    let error = sql.execute(statement, &[], None).expect_err(statement);
    assert_eq!(sqlstate(&error), state, "{statement}: {error:?}");
    assert_eq!(
        take_sql_error_message().as_deref(),
        Some(message),
        "{statement}"
    );
}

fn check_alter_table_if_exists_missing(engine: Engine) {
    let path = common::fresh("alter-table-if-exists-missing");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    let mut sql = admin.sql_session();
    run(&mut sql, "CREATE TABLE keep (id int)", "CREATE TABLE");
    let catalog = admin.catalog().unwrap();
    for statement in [
        "ALTER TABLE IF EXISTS missing ADD COLUMN v int",
        "ALTER TABLE IF EXISTS missing SET (fillfactor=70)",
        "ALTER TABLE IF EXISTS missing ALTER COLUMN v SET STATISTICS 10",
        "ALTER TABLE IF EXISTS missing CLUSTER ON missing_idx",
    ] {
        run(&mut sql, statement, "ALTER TABLE");
        assert_eq!(
            take_sql_notices(),
            [("relation \"missing\" does not exist, skipping".into(), None, None)],
            "{statement}"
        );
        assert_eq!(admin.catalog().unwrap(), catalog, "{statement}");
        let result = sql.execute("SELECT 1", &[], None).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values, [Value::Int(1)]);
    }
    drop(sql);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn alter_table_if_exists_missing_legacy() {
    check_alter_table_if_exists_missing(Engine::Legacy);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn alter_table_if_exists_missing_v2() {
    check_alter_table_if_exists_missing(Engine::V2);
}

#[test]
fn legacy_refuses_storage_metadata_and_accepts_defaults() {
    let path = common::fresh("legacy-storage-options");
    let admin = Admin::create_engine(&path, Limits::default(), Engine::Legacy).unwrap();
    let mut sql = admin.sql_session();
    run(
        &mut sql,
        "CREATE TABLE t (id int PRIMARY KEY, v text) WITH (oids=false)",
        "CREATE TABLE",
    );
    run(&mut sql, "CREATE INDEX t_v ON t(v)", "CREATE INDEX");
    for statement in [
        "CREATE TABLE u (id int) WITH (fillfactor=70)",
        "CREATE UNLOGGED TABLE u (id int)",
        "ALTER TABLE t SET (fillfactor=70)",
        "ALTER TABLE t SET UNLOGGED",
        "ALTER TABLE t CLUSTER ON t_v",
        "CREATE INDEX t_opts ON t(v) WITH (fillfactor=80)",
        "ALTER INDEX t_v SET (fillfactor=80)",
        "ALTER TABLE t ALTER COLUMN v SET STATISTICS 40",
        "ALTER TABLE t ALTER COLUMN v SET STORAGE EXTERNAL",
        "ALTER TABLE t ALTER COLUMN v SET COMPRESSION lz4",
        "ALTER TABLE t ALTER COLUMN v SET (n_distinct=-0.5)",
    ] {
        fail(
            &mut sql,
            statement,
            "0A000",
            "storage metadata requires the v2 engine",
        );
    }
    for statement in [
        "ALTER TABLE t RESET (bogus, fillfactor)",
        "ALTER TABLE t SET LOGGED",
        "ALTER TABLE t SET WITHOUT CLUSTER",
        "ALTER TABLE t ALTER COLUMN id SET STORAGE PLAIN",
        "ALTER TABLE t ALTER COLUMN v SET STATISTICS -1",
        "ALTER TABLE t ALTER COLUMN v SET COMPRESSION default",
        "ALTER TABLE t ALTER COLUMN v RESET (n_distinct)",
    ] {
        run(&mut sql, statement, "ALTER TABLE");
    }
    run(
        &mut sql,
        "ALTER INDEX t_v RESET (fillfactor)",
        "ALTER INDEX",
    );
    run(&mut sql, "CLUSTER t USING t_v", "CLUSTER");
    fail(
        &mut sql,
        "CLUSTER t",
        "42704",
        "there is no previously clustered index for table \"t\"",
    );
    assert!(
        sql.catalog_metadata()
            .unwrap()
            .presentation
            .table_storage
            .is_empty()
    );
    drop(sql);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[cfg(feature = "engine-rocksdb")]
mod v2 {
    use super::*;
    use giggles_core::{
        CatalogMetadata,
        relational::{ColumnStorage, Persistence, TableDef, TableStorage},
    };
    use std::collections::BTreeMap;

    fn feature_bits(path: &std::path::Path) -> u64 {
        let opts = rocksdb::Options::default();
        let families = rocksdb::DB::list_cf(&opts, path).unwrap();
        let db = rocksdb::DB::open_cf_for_read_only(&opts, path, families, false).unwrap();
        u64::from_be_bytes(
            db.get_cf(db.cf_handle("meta").unwrap(), b"features")
                .unwrap()
                .unwrap()
                .try_into()
                .unwrap(),
        )
    }

    fn live_table_value(path: &std::path::Path, id: u64) -> Box<[u8]> {
        use rocksdb::{DB, Direction, IteratorMode, Options};

        let opts = Options::default();
        let families = DB::list_cf(&opts, path).unwrap();
        let db = DB::open_cf_for_read_only(&opts, path, families, false).unwrap();
        let cf = db.cf_handle("catalog").unwrap();
        let key = [b"\x01".as_slice(), &id.to_be_bytes()].concat();
        let (stored_key, value) = db
            .iterator_cf(cf, IteratorMode::From(&key, Direction::Forward))
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(&stored_key[..stored_key.len() - 8], key);
        assert_eq!(value[0], 0, "live MVCC value");
        value
    }

    fn table<'a>(catalog: &'a CatalogMetadata, name: &str) -> &'a TableDef {
        catalog
            .tables
            .iter()
            .find(|table| table.name == name)
            .unwrap()
    }

    fn table_storage<'a>(catalog: &'a CatalogMetadata, name: &str) -> &'a TableStorage {
        &catalog.presentation.table_storage[&table(catalog, name).id]
    }

    fn options(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|&(name, value)| (name.to_owned(), value.to_owned()))
            .collect()
    }

    #[test]
    fn storage_options_rollback_drop_and_reopen() {
        let path = common::fresh("storage-options");
        let limits = Limits::default();
        let admin = Admin::create_engine(&path, limits, Engine::V2).unwrap();
        let mut sql = admin.sql_session();
        run(
            &mut sql,
            "CREATE TABLE baseline (id int PRIMARY KEY)",
            "CREATE TABLE",
        );
        run(
            &mut sql,
            "ALTER TABLE baseline RESET (fillfactor)",
            "ALTER TABLE",
        );
        drop(sql);
        drop(admin);
        assert_eq!(feature_bits(&path) & (1 << 40), 0);

        let admin = Admin::open(&path, limits).unwrap();
        let mut sql = admin.sql_session();
        for (statement, tag) in [
            (
                "CREATE UNLOGGED TABLE t (id int PRIMARY KEY, v text) WITH (fillfactor=70, toast.autovacuum_enabled=false)",
                "CREATE TABLE",
            ),
            (
                "ALTER TABLE t SET (autovacuum_enabled=false)",
                "ALTER TABLE",
            ),
            (
                "CREATE INDEX t_v ON t(v) WITH (fillfactor=60)",
                "CREATE INDEX",
            ),
            (
                "ALTER INDEX t_v SET (deduplicate_items=off, fillfactor=90)",
                "ALTER INDEX",
            ),
            ("CLUSTER t USING t_v", "CLUSTER"),
            ("CLUSTER t", "CLUSTER"),
            (
                "ALTER TABLE t ALTER COLUMN v SET STATISTICS 200, ALTER COLUMN v SET STORAGE EXTERNAL",
                "ALTER TABLE",
            ),
            (
                "ALTER TABLE t ALTER COLUMN v SET COMPRESSION pglz, ALTER COLUMN v SET (n_distinct=0.5)",
                "ALTER TABLE",
            ),
        ] {
            run(&mut sql, statement, tag);
        }
        let catalog = sql.catalog_metadata().unwrap();
        let original_table = table(&catalog, "t");
        let column_id = original_table
            .logical_columns()
            .find(|col| col.name == "v")
            .unwrap()
            .id;
        let index = catalog
            .indexes
            .iter()
            .find(|index| index.table == original_table.id && index.name == "t_v")
            .unwrap();
        let expected = TableStorage {
            reloptions: options(&[
                ("fillfactor", "70"),
                ("toast.autovacuum_enabled", "false"),
                ("autovacuum_enabled", "false"),
            ]),
            persistence: Persistence::Unlogged,
            clustered_index: Some(index.id),
            index_reloptions: BTreeMap::from([(
                index.id,
                options(&[("deduplicate_items", "off"), ("fillfactor", "90")]),
            )]),
            columns: BTreeMap::from([(
                column_id,
                ColumnStorage {
                    stattarget: Some(200),
                    storage: Some(b'e'),
                    compression: Some(b'p'),
                    options: options(&[("n_distinct", "0.5")]),
                },
            )]),
        };
        assert_eq!(table_storage(&catalog, "t"), &expected);
        run(
            &mut sql,
            "ALTER TABLE t RENAME COLUMN v TO payload",
            "ALTER TABLE",
        );
        run(&mut sql, "ALTER TABLE t RENAME TO saved", "ALTER TABLE");
        let catalog = sql.catalog_metadata().unwrap();
        let saved = table(&catalog, "saved");
        assert_eq!(saved.id, original_table.id);
        assert_eq!(
            saved
                .logical_columns()
                .find(|col| col.name == "payload")
                .unwrap()
                .id,
            column_id
        );
        assert_eq!(table_storage(&catalog, "saved"), &expected);

        run(&mut sql, "BEGIN", "BEGIN");
        run(
            &mut sql,
            "ALTER TABLE saved SET LOGGED, SET (fillfactor=50), SET WITHOUT CLUSTER",
            "ALTER TABLE",
        );
        assert_eq!(
            table_storage(&sql.catalog_metadata().unwrap(), "saved"),
            &TableStorage {
                reloptions: options(&[
                    ("toast.autovacuum_enabled", "false"),
                    ("autovacuum_enabled", "false"),
                    ("fillfactor", "50"),
                ]),
                persistence: Persistence::Permanent,
                clustered_index: None,
                ..expected.clone()
            }
        );
        run(&mut sql, "ROLLBACK", "ROLLBACK");
        assert_eq!(
            table_storage(&sql.catalog_metadata().unwrap(), "saved"),
            &expected
        );
        drop(sql);
        drop(admin);
        assert_ne!(feature_bits(&path) & (1 << 40), 0);

        let admin = Admin::open(&path, limits).unwrap();
        let mut sql = admin.sql_session();
        assert_eq!(
            table_storage(&sql.catalog_metadata().unwrap(), "saved"),
            &expected
        );
        run(&mut sql, "DROP INDEX t_v", "DROP INDEX");
        fail(
            &mut sql,
            "CLUSTER saved",
            "42704",
            "there is no previously clustered index for table \"saved\"",
        );
        let catalog = sql.catalog_metadata().unwrap();
        let storage = table_storage(&catalog, "saved");
        assert!(storage.clustered_index.is_none());
        assert!(storage.index_reloptions.is_empty());
        run(
            &mut sql,
            "ALTER TABLE saved DROP COLUMN payload",
            "ALTER TABLE",
        );
        let catalog = sql.catalog_metadata().unwrap();
        assert!(table_storage(&catalog, "saved").columns.is_empty());
        let saved = table(&catalog, "saved");
        assert!(saved.logical_columns().all(|col| col.name != "payload"));
        drop(sql);
        drop(admin);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn cascade_drops_prune_storage_and_restore_old_table_bytes() {
        let path = common::fresh("cascade-storage-options");
        let limits = Limits::default();
        let admin = Admin::create_engine(&path, limits, Engine::V2).unwrap();
        let mut sql = admin.sql_session();
        for (statement, tag) in [
            ("CREATE TYPE mood AS ENUM ('a')", "CREATE TYPE"),
            (
                "CREATE TABLE typed (id int PRIMARY KEY, m mood UNIQUE)",
                "CREATE TABLE",
            ),
            (
                "CREATE TABLE keyed (id int, m mood PRIMARY KEY)",
                "CREATE TABLE",
            ),
            (
                "CREATE COLLATION custom_c (provider=icu,locale='und')",
                "CREATE COLLATION",
            ),
            (
                "CREATE TABLE collated (id int PRIMARY KEY, label text COLLATE custom_c)",
                "CREATE TABLE",
            ),
            (
                "CREATE TABLE index_only (id int PRIMARY KEY, label text)",
                "CREATE TABLE",
            ),
        ] {
            run(&mut sql, statement, tag);
        }
        let index_only = table(&sql.catalog_metadata().unwrap(), "index_only").id;
        drop((sql, admin));
        let old = live_table_value(&path, index_only);

        let admin = Admin::open(&path, limits).unwrap();
        let mut sql = admin.sql_session();
        for (statement, tag) in [
            (
                "ALTER TABLE typed ALTER COLUMN m SET STATISTICS 200",
                "ALTER TABLE",
            ),
            ("ALTER INDEX typed_m_key SET (fillfactor=80)", "ALTER INDEX"),
            ("CLUSTER typed USING typed_m_key", "CLUSTER"),
            ("ALTER INDEX keyed_pkey SET (fillfactor=80)", "ALTER INDEX"),
            ("CLUSTER keyed USING keyed_pkey", "CLUSTER"),
            (
                "ALTER TABLE collated ALTER COLUMN label SET STATISTICS 200",
                "ALTER TABLE",
            ),
            (
                "CREATE INDEX label_custom ON index_only(label COLLATE custom_c) WITH (fillfactor=80)",
                "CREATE INDEX",
            ),
            ("CLUSTER index_only USING label_custom", "CLUSTER"),
            ("DROP TYPE mood CASCADE", "DROP TYPE"),
            ("DROP COLLATION custom_c CASCADE", "DROP COLLATION"),
        ] {
            run(&mut sql, statement, tag);
        }
        let check = |sql: &mut SqlSession| {
            let catalog = sql.catalog_metadata().unwrap();
            assert!(catalog.presentation.table_storage.is_empty());
            for name in ["typed", "keyed", "collated"] {
                let def = table(&catalog, name);
                assert_eq!(
                    def.logical_columns()
                        .map(|col| col.name.as_str())
                        .collect::<Vec<_>>(),
                    ["id"]
                );
            }
            assert!(table(&catalog, "keyed").hidden_rowid().is_some());
            assert!(
                catalog
                    .indexes
                    .iter()
                    .all(|index| index.name != "typed_m_key" && index.name != "label_custom")
            );
        };
        check(&mut sql);
        drop((sql, admin));
        assert_eq!(live_table_value(&path, index_only), old);
        let admin = Admin::open(&path, limits).unwrap();
        check(&mut admin.sql_session());
        drop(admin);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn gin_options_use_the_old_table_trailer() {
        let path = common::fresh("gin-option-trailer");
        let limits = Limits::default();
        let admin = Admin::create_engine(&path, limits, Engine::V2).unwrap();
        let mut sql = admin.sql_session();
        run(
            &mut sql,
            "CREATE TABLE docs (id int PRIMARY KEY, entries int[])",
            "CREATE TABLE",
        );
        let id = table(&sql.catalog_metadata().unwrap(), "docs").id;
        drop((sql, admin));
        let old = live_table_value(&path, id);

        let admin = Admin::open(&path, limits).unwrap();
        let mut sql = admin.sql_session();
        run(
            &mut sql,
            "CREATE INDEX docs_entries ON docs USING gin (entries) WITH (fastupdate=off, gin_pending_list_limit=128)",
            "CREATE INDEX",
        );
        drop((sql, admin));
        let with_gin = live_table_value(&path, id);
        assert_eq!(&with_gin[..old.len()], old.as_ref());
        assert_eq!(&with_gin[old.len()..old.len() + 8], &1u64.to_be_bytes());
        assert_eq!(feature_bits(&path) & (1 << 40), 0);
        let admin = Admin::open(&path, limits).unwrap();
        let mut sql = admin.sql_session();
        let catalog = sql.catalog_metadata().unwrap();
        let gin = catalog.presentation.gin_indexes.values().next().unwrap();
        assert_eq!(
            gin.options,
            options(&[("fastupdate", "off"), ("gin_pending_list_limit", "128")])
        );
        assert!(catalog.presentation.table_storage.is_empty());
        run(
            &mut sql,
            "ALTER INDEX docs_entries RESET (fastupdate)",
            "ALTER INDEX",
        );
        drop((sql, admin));
        let altered = live_table_value(&path, id);
        assert_eq!(&altered[..old.len()], old.as_ref());
        assert_eq!(&altered[old.len()..old.len() + 8], &1u64.to_be_bytes());
        assert_eq!(feature_bits(&path) & (1 << 40), 0);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn storage_diagnostics_match_postgres() {
        let path = common::fresh("storage-diagnostics");
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        let mut sql = admin.sql_session();
        run(
            &mut sql,
            "CREATE TABLE t (id int PRIMARY KEY, v text) WITH (FILLFACTOR=70)",
            "CREATE TABLE",
        );
        run(&mut sql, "CREATE INDEX t_v ON t(v)", "CREATE INDEX");
        for (statement, state, message) in [
            (
                "ALTER TABLE t SET (fillfactor=5)",
                "22023",
                "value 5 out of bounds for option \"fillfactor\"",
            ),
            (
                "ALTER TABLE t SET (fillfactor=101)",
                "22023",
                "value 101 out of bounds for option \"fillfactor\"",
            ),
            (
                "ALTER TABLE t SET (fillfactor='abc')",
                "22023",
                "invalid value for integer option \"fillfactor\": abc",
            ),
            (
                "ALTER TABLE t SET (bogus=1)",
                "22023",
                "unrecognized parameter \"bogus\"",
            ),
            (
                "ALTER TABLE t SET (autovacuum_enabled=maybe)",
                "22023",
                "invalid value for boolean option \"autovacuum_enabled\": maybe",
            ),
            (
                "ALTER TABLE t SET (fillfactor)",
                "22023",
                "invalid value for integer option \"fillfactor\": true",
            ),
            (
                "ALTER TABLE t SET (fillfactor=10, Fillfactor=20)",
                "22023",
                "parameter \"fillfactor\" specified more than once",
            ),
            (
                "ALTER TABLE t SET (toast.fillfactor=50)",
                "22023",
                "unrecognized parameter \"fillfactor\"",
            ),
            (
                "ALTER TABLE t SET (foo.fillfactor=50)",
                "22023",
                "unrecognized parameter namespace \"foo\"",
            ),
            (
                "CREATE TABLE oids (id int) WITH (oids=true)",
                "0A000",
                "tables declared WITH OIDS are not supported",
            ),
            (
                "ALTER INDEX t_v SET (autovacuum_enabled=false)",
                "22023",
                "unrecognized parameter \"autovacuum_enabled\"",
            ),
            (
                "ALTER TABLE t RESET (fillfactor=10)",
                "42601",
                "RESET must not include values for parameters",
            ),
            (
                "ALTER TABLE missing SET (fillfactor=10)",
                "42P01",
                "relation \"missing\" does not exist",
            ),
            (
                "CLUSTER t",
                "42704",
                "there is no previously clustered index for table \"t\"",
            ),
            (
                "ALTER TABLE t ALTER COLUMN v SET STATISTICS -5",
                "22023",
                "statistics target -5 is too low",
            ),
            (
                "ALTER TABLE t ALTER COLUMN v SET STORAGE bogus",
                "22023",
                "invalid storage type \"bogus\"",
            ),
            (
                "ALTER TABLE t ALTER COLUMN id SET STORAGE EXTERNAL",
                "0A000",
                "column data type integer can only have storage PLAIN",
            ),
            (
                "ALTER TABLE t ALTER COLUMN v SET COMPRESSION bogus",
                "22023",
                "invalid compression method \"bogus\"",
            ),
            (
                "ALTER TABLE t ALTER COLUMN v SET (bogus=1)",
                "22023",
                "unrecognized parameter \"bogus\"",
            ),
            (
                "ALTER TABLE t ALTER COLUMN v SET (n_distinct=-2)",
                "22023",
                "value -2 out of bounds for option \"n_distinct\"",
            ),
            (
                "ALTER TABLE t ALTER COLUMN nope SET STATISTICS 10",
                "42703",
                "column \"nope\" of relation \"t\" does not exist",
            ),
        ] {
            fail(&mut sql, statement, state, message);
        }
        for statement in [
            "ALTER TABLE t SET (autovacuum_enabled)",
            "ALTER TABLE t RESET (fillfactor, bogus)",
            "ALTER TABLE t SET (autovacuum_vacuum_scale_factor=0.05, autovacuum_analyze_threshold=100, parallel_workers=4, toast_tuple_target=256, user_catalog_table=false, vacuum_truncate=off, log_autovacuum_min_duration=10)",
            "ALTER TABLE t ALTER COLUMN v SET STORAGE EXTERNAL",
            "ALTER TABLE t ALTER COLUMN v SET COMPRESSION lz4",
            "ALTER TABLE t ALTER COLUMN v SET COMPRESSION default",
            "ALTER TABLE t ALTER COLUMN v SET STATISTICS -1",
            "ALTER TABLE t ALTER COLUMN v SET STATISTICS 20000",
        ] {
            run(&mut sql, statement, "ALTER TABLE");
        }
        assert_eq!(
            take_sql_notices().last().unwrap().0,
            "lowering statistics target to 10000"
        );
        run(
            &mut sql,
            "ALTER TABLE IF EXISTS missing SET (fillfactor=10)",
            "ALTER TABLE",
        );
        assert_eq!(
            take_sql_notices().last().unwrap().0,
            "relation \"missing\" does not exist, skipping"
        );
        run(
            &mut sql,
            "CREATE TABLE without_oid (id int) WITHOUT OIDS",
            "CREATE TABLE",
        );
        run(
            &mut sql,
            "ALTER TABLE t SET (fillfactor=50), SET ACCESS METHOD heap, CLUSTER ON t_pkey",
            "ALTER TABLE",
        );
        run(&mut sql, "CLUSTER t", "CLUSTER");
        for statement in [
            "ALTER TABLE t SET (autovacuum_enabled=OFF, fillfactor='90')",
            "ALTER TABLE t SET (autovacuum_vacuum_scale_factor=0.0000001)",
            "ALTER TABLE t SET (autovacuum_vacuum_scale_factor=1e2)",
        ] {
            run(&mut sql, statement, "ALTER TABLE");
        }
        assert_eq!(
            table_storage(&sql.catalog_metadata().unwrap(), "t").reloptions,
            options(&[
                ("autovacuum_analyze_threshold", "100"),
                ("parallel_workers", "4"),
                ("toast_tuple_target", "256"),
                ("user_catalog_table", "false"),
                ("vacuum_truncate", "off"),
                ("log_autovacuum_min_duration", "10"),
                ("autovacuum_enabled", "off"),
                ("fillfactor", "90"),
                ("autovacuum_vacuum_scale_factor", "1e2"),
            ]),
        );
        run(
            &mut sql,
            "ALTER INDEX t_v SET (fillfactor=70)",
            "ALTER INDEX",
        );
        run(
            &mut sql,
            "ALTER INDEX t_v RESET (fillfactor, missing)",
            "ALTER INDEX",
        );
        assert!(
            table_storage(&sql.catalog_metadata().unwrap(), "t")
                .index_reloptions
                .is_empty()
        );
        run(
            &mut sql,
            "ALTER TABLE t ALTER COLUMN v SET (n_distinct=0.5)",
            "ALTER TABLE",
        );
        run(
            &mut sql,
            "ALTER TABLE t ALTER COLUMN v RESET (n_distinct)",
            "ALTER TABLE",
        );
        let catalog = sql.catalog_metadata().unwrap();
        let column = table(&catalog, "t")
            .logical_columns()
            .find(|col| col.name == "v")
            .unwrap();
        assert!(
            table_storage(&catalog, "t").columns[&column.id]
                .options
                .is_empty()
        );
        drop(sql);
        drop(admin);
        std::fs::remove_dir_all(path).unwrap();
    }
}
