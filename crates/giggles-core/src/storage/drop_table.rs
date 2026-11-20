use super::{Admin, CoreError, Ordering, relational};

impl Admin {
    /// Autocommit DROP TABLE. Returns true when dropped, false when absent and
    /// `if_exists`, or `SqlError::UnknownTable` when absent and !if_exists.
    /// Active SQL transactions must finish or roll back first.
    pub fn drop_table(&self, name: &str, if_exists: bool) -> Result<bool, CoreError> {
        if self.inner.sql_transactions.load(Ordering::Acquire) != 0 {
            return Err(CoreError::Sql(relational::SqlError::Conflict));
        }
        if matches!(self.inner.backend, super::Backend::V2(_)) {
            let mut session = self.sql_session();
            // Match legacy's absence-at-preflight behavior. SQL DDL results have
            // no affected-row count, so a successful nonoptional DROP means true.
            if !session.relation_exists(name)? {
                return if if_exists {
                    Ok(false)
                } else {
                    Err(CoreError::Sql(relational::SqlError::UnknownTable))
                };
            }
            let (schema, name) = relational::split_key(name);
            let sql = format!(
                "DROP TABLE \"{}\".\"{}\"",
                schema.replace('"', "\"\""),
                name.replace('"', "\"\""),
            );
            return match session.execute(&sql, &[], None) {
                Ok(_) => Ok(true),
                Err(CoreError::Sql(relational::SqlError::UnknownTable)) if if_exists => Ok(false),
                Err(error) => Err(error),
            };
        }
        let (_, tables) = self.catalog()?;
        let schemas = self.catalog_schema_metadata()?;
        if self.inner.sql_transactions.load(Ordering::Acquire) != 0 {
            return Err(CoreError::Sql(relational::SqlError::Conflict));
        }
        // Absence is decided at this catalog snapshot; a concurrent CREATE may
        // follow it. No whole-state write is needed for this result.
        if !tables.iter().any(|table| {
            relational::relation_key(
                schemas
                    .tables
                    .get(&table.id)
                    .map_or("public", String::as_str),
                &table.name,
            ) == name
        }) {
            return if if_exists {
                Ok(false)
            } else {
                Err(CoreError::Sql(relational::SqlError::UnknownTable))
            };
        }
        self.inner.change(|state| {
            // BEGIN reserves this counter before taking its snapshot under the
            // same gate. Keep this check for a BEGIN racing with the preflight.
            // Only that rare refusal race enters change and advances generation.
            if self.inner.sql_transactions.load(Ordering::Acquire) != 0 {
                return Err(CoreError::Sql(relational::SqlError::Conflict));
            }
            let dropped = state.sql.drop_table(name).map_err(CoreError::Sql)?;
            if !dropped && !if_exists {
                return Err(CoreError::Sql(relational::SqlError::UnknownTable));
            }
            Ok(dropped)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Limits, SqlSession, relational::SqlError};
    use std::path::PathBuf;

    struct Database {
        admin: Option<Admin>,
        path: PathBuf,
    }

    impl Database {
        fn new() -> Self {
            Self::with_engine(crate::Engine::Legacy)
        }

        fn with_engine(engine: crate::Engine) -> Self {
            let mut nonce = [0; 16];
            getrandom::fill(&mut nonce).unwrap();
            let path = std::env::temp_dir().join(format!(
                "gd55-drop-table-{:032x}",
                u128::from_be_bytes(nonce)
            ));
            let admin = Some(Admin::create_engine(&path, Limits::default(), engine).unwrap());
            Self { admin, path }
        }

        fn admin(&self) -> &Admin {
            self.admin.as_ref().unwrap()
        }

        fn reopen(&mut self) {
            drop(self.admin.take());
            self.admin = Some(Admin::open(&self.path, Limits::default()).unwrap());
        }
    }

    impl Drop for Database {
        fn drop(&mut self) {
            drop(self.admin.take());
            std::fs::remove_dir_all(&self.path).unwrap();
        }
    }

    fn sql(session: &mut SqlSession, text: &str) -> relational::SqlResult {
        session.execute(text, &[], None).unwrap()
    }

    fn create_index(admin: &Admin, table: &str, name: &str) {
        // SQL CREATE INDEX is unsupported; use the production typed catalog path.
        let mut session = admin.sql_session();
        session.begin_engine().unwrap();
        let (_, tables) = session.catalog_engine().unwrap();
        let table = tables.iter().find(|t| t.name == table).unwrap();
        session
            .create_index_engine(table, name, &[table.columns[1].id])
            .unwrap();
        session.commit_engine().unwrap();
    }

    fn counters(admin: &Admin) -> (u64, u64) {
        admin
            .inner
            .inspect(|s| Ok((s.sql.epoch, s.sql.generation)))
            .unwrap()
    }

    #[test]
    fn drop_table_removes_rows_and_indexes_preserves_other_tables_and_reopens() {
        let mut db = Database::new();
        {
            let admin = db.admin();
            let mut session = admin.sql_session();
            sql(
                &mut session,
                "CREATE TABLE doomed (id BIGINT PRIMARY KEY, tag TEXT)",
            );
            sql(
                &mut session,
                "CREATE TABLE kept (id BIGINT PRIMARY KEY, tag TEXT)",
            );
            create_index(admin, "doomed", "by_doomed_tag");
            create_index(admin, "doomed", "by_doomed_tag_again");
            create_index(admin, "kept", "by_kept_tag");
            sql(&mut session, "INSERT INTO doomed VALUES (1,'gone')");
            sql(&mut session, "INSERT INTO doomed VALUES (2,'also_gone')");
            sql(&mut session, "INSERT INTO kept VALUES (3,'safe')");
            let kept = sql(&mut session, "SELECT * FROM kept LIMIT 10");
            let before = counters(admin);
            assert_eq!(admin.drop_table("doomed", false), Ok(true));
            assert_eq!(counters(admin), (before.0 + 1, before.1 + 1));
            let (_, tables) = admin.catalog().unwrap();
            assert_eq!(tables.len(), 1);
            assert_eq!(tables[0].name, "kept");
            assert_eq!(
                session.execute("SELECT * FROM doomed LIMIT 10", &[], None),
                Err(CoreError::Sql(SqlError::UnknownTable))
            );
            assert_eq!(sql(&mut session, "SELECT * FROM kept LIMIT 10"), kept);
            admin
                .inner
                .inspect(|s| {
                    let indexes = s.sql.secondary_descriptors_engine();
                    assert_eq!(indexes.len(), 1);
                    assert_eq!(indexes[0].name, "by_kept_tag");
                    s.sql.secondary_validate_engine(|| false).unwrap();
                    Ok(())
                })
                .unwrap();
        }
        db.reopen();
        let admin = db.admin();
        let mut session = admin.sql_session();
        assert_eq!(
            session.execute("SELECT * FROM doomed LIMIT 10", &[], None),
            Err(CoreError::Sql(SqlError::UnknownTable))
        );
        assert_eq!(
            sql(&mut session, "SELECT * FROM kept LIMIT 10").rows.len(),
            1
        );
        sql(
            &mut session,
            "CREATE TABLE doomed (id BIGINT PRIMARY KEY, tag TEXT)",
        );
        assert!(
            sql(&mut session, "SELECT * FROM doomed LIMIT 10")
                .rows
                .is_empty()
        );
    }

    #[test]
    fn drop_table_last_index_removes_durable_sidecar_and_reuses_index_name() {
        let mut db = Database::new();
        {
            let admin = db.admin();
            let mut session = admin.sql_session();
            sql(
                &mut session,
                "CREATE TABLE doomed (id BIGINT PRIMARY KEY, tag TEXT)",
            );
            create_index(admin, "doomed", "by_tag");
            sql(&mut session, "INSERT INTO doomed VALUES (1,'gone')");
            assert_eq!(admin.drop_table("doomed", true), Ok(true));
            assert!(admin.catalog().unwrap().1.is_empty());
            assert_eq!(admin.logical_export_catalog().unwrap().1.len(), 0);
        }
        db.reopen();
        {
            let admin = db.admin();
            assert!(admin.catalog().unwrap().1.is_empty());
            let mut session = admin.sql_session();
            assert_eq!(
                session.execute("SELECT * FROM doomed LIMIT 10", &[], None),
                Err(CoreError::Sql(SqlError::UnknownTable))
            );
            sql(
                &mut session,
                "CREATE TABLE doomed (id BIGINT PRIMARY KEY, tag TEXT)",
            );
            create_index(admin, "doomed", "by_tag");
            assert!(
                sql(&mut session, "SELECT * FROM doomed LIMIT 10")
                    .rows
                    .is_empty()
            );
        }
        db.reopen();
        assert_eq!(db.admin().catalog().unwrap().1.len(), 1);
    }

    #[test]
    fn drop_table_if_exists_and_plain_table_persist() {
        let mut db = Database::new();
        let epoch = db.admin().catalog().unwrap().0;
        assert_eq!(db.admin().drop_table("missing", true), Ok(false));
        assert_eq!(
            db.admin().drop_table("missing", false),
            Err(CoreError::Sql(SqlError::UnknownTable))
        );
        assert_eq!(db.admin().catalog().unwrap().0, epoch);
        {
            let mut session = db.admin().sql_session();
            sql(&mut session, "CREATE TABLE plain (id BIGINT PRIMARY KEY)");
            sql(&mut session, "INSERT INTO plain VALUES (1)");
        }
        assert_eq!(db.admin().drop_table("plain", false), Ok(true));
        assert_eq!(db.admin().drop_table("plain", true), Ok(false));
        assert_eq!(
            db.admin().drop_table("plain", false),
            Err(CoreError::Sql(SqlError::UnknownTable))
        );
        db.reopen();
        assert!(db.admin().catalog().unwrap().1.is_empty());
    }

    #[test]
    fn drop_table_invalidates_prepared_statements_even_after_recreate() {
        let db = Database::new();
        let admin = db.admin();
        let mut session = admin.sql_session();
        sql(&mut session, "CREATE TABLE doomed (id BIGINT PRIMARY KEY)");
        sql(&mut session, "INSERT INTO doomed VALUES (1)");
        let prepared = session.prepare("SELECT * FROM doomed LIMIT 10").unwrap();
        assert_eq!(admin.drop_table("doomed", false), Ok(true));
        assert_eq!(
            session.execute_prepared(&prepared, &[], None),
            Err(CoreError::Sql(SqlError::StaleSchema))
        );
        sql(&mut session, "CREATE TABLE doomed (id BIGINT PRIMARY KEY)");
        assert!(
            sql(&mut session, "SELECT * FROM doomed LIMIT 10")
                .rows
                .is_empty()
        );
        assert_eq!(
            session.execute_prepared(&prepared, &[], None),
            Err(CoreError::Sql(SqlError::StaleSchema))
        );
    }

    #[test]
    fn drop_table_refuses_active_snapshots_including_staged_schema_and_rows() {
        let db = Database::new();
        let admin = db.admin();
        let mut session = admin.sql_session();
        sql(&mut session, "CREATE TABLE doomed (id BIGINT PRIMARY KEY)");
        sql(&mut session, "INSERT INTO doomed VALUES (1)");
        for staged in [false, true] {
            sql(&mut session, "BEGIN");
            if staged {
                sql(&mut session, "CREATE TABLE pending (id BIGINT PRIMARY KEY)");
                sql(&mut session, "INSERT INTO doomed VALUES (2)");
            }
            let before = admin.catalog().unwrap();
            let before_counters = counters(admin);
            assert_eq!(
                admin.drop_table("doomed", false),
                Err(CoreError::Sql(SqlError::Conflict))
            );
            assert_eq!(
                admin.drop_table("missing", true),
                Err(CoreError::Sql(SqlError::Conflict))
            );
            assert_eq!(admin.catalog().unwrap(), before);
            assert_eq!(counters(admin), before_counters);
            assert_eq!(
                sql(&mut session, "SELECT * FROM doomed LIMIT 10")
                    .rows
                    .len(),
                if staged { 2 } else { 1 }
            );
            sql(&mut session, "COMMIT");
            assert!(
                admin
                    .catalog()
                    .unwrap()
                    .1
                    .iter()
                    .any(|t| t.name == "doomed")
            );
        }
        let mut other = admin.sql_session();
        assert_eq!(
            sql(&mut other, "SELECT * FROM doomed LIMIT 10").rows.len(),
            2
        );
        assert!(
            sql(&mut other, "SELECT * FROM pending LIMIT 10")
                .rows
                .is_empty()
        );
        let before_counters = counters(admin);
        assert_eq!(admin.drop_table("missing", true), Ok(false));
        assert_eq!(
            admin.drop_table("missing", false),
            Err(CoreError::Sql(SqlError::UnknownTable))
        );
        assert_eq!(counters(admin), before_counters);
        sql(&mut session, "BEGIN");
        drop(session);
        assert_eq!(admin.drop_table("doomed", false), Ok(true));
    }

    #[test]
    fn drop_table_epoch_overflow_leaves_relational_state_unchanged() {
        let db = Database::new();
        let admin = db.admin();
        let mut session = admin.sql_session();
        sql(
            &mut session,
            "CREATE TABLE doomed (id BIGINT PRIMARY KEY, tag TEXT)",
        );
        create_index(admin, "doomed", "by_tag");
        sql(&mut session, "INSERT INTO doomed VALUES (1,'safe')");
        admin
            .inner
            .inspect(|s| {
                let mut state = s.sql.clone();
                state.epoch = u64::MAX;
                let before = state.clone();
                assert_eq!(state.drop_table("doomed"), Err(SqlError::Overflow));
                assert_eq!(state, before);
                Ok(())
            })
            .unwrap();
    }
    fn cascade_notice(engine: crate::Engine) {
        let db = Database::with_engine(engine);
        let mut session = db.admin().sql_session();
        for text in [
            "CREATE SCHEMA da",
            "CREATE TABLE da.a (id INT PRIMARY KEY)",
            "CREATE TABLE da.b (id INT PRIMARY KEY)",
            "CREATE TABLE da.c (id INT, b INT REFERENCES da.b(id))",
        ] { sql(&mut session, text); }
        relational::take_sql_notices();
        sql(&mut session, "DROP TABLE da.a, da.b CASCADE");
        let notices = relational::take_sql_notices();
        assert_eq!(notices, vec![(
            "drop cascades to constraint c_b_fkey on table da.c".into(),
            None, Some("00000"),
        )]);
    }

    #[test]
    fn multi_drop_cascade_notice_legacy() { cascade_notice(crate::Engine::Legacy); }

    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn multi_drop_cascade_notice_v2() { cascade_notice(crate::Engine::V2); }

}
