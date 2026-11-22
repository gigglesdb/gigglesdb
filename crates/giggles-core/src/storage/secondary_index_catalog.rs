//! Native catalog bridge; publication remains the session commit's responsibility.
use super::*;

impl SqlSession {
    pub(crate) fn secondary_read_native_engine(
        &mut self,
        table: &relational::TableDef,
        request: &relational::secondary_index_access::Request<'_>,
    ) -> Result<relational::SqlResult, CoreError> {
        if self.is_v2_engine() {
            let db = self.v2_lease(Holder::Sql)?;
            let before = self.snapshot_identity_engine()?;
            let inner = self.admin.inner.clone();
            let _ticket = inner.enter_sql()?;
            let txn = self.v2_transaction.as_mut()
                .ok_or(CoreError::Sql(relational::SqlError::Transaction))?;
            let result = v2_sql::secondary_read_native(&db, txn, table, request);
            if self.snapshot_identity_engine()? != before {
                return Err(CoreError::Sql(relational::SqlError::StaleSchema));
            }
            return result;
        }
        let (_, epoch) = self.snapshot_identity_engine()?;
        let state = self
            .staged
            .as_ref()
            .ok_or(CoreError::Sql(relational::SqlError::Transaction))?;
        if state.sql.tables.get(&state.sql.table_key(table)) != Some(table) {
            return Err(CoreError::Sql(relational::SqlError::StaleSchema));
        }
        if table.hidden_rowid().is_some() {
            return Err(CoreError::Sql(relational::SqlError::Unsupported));
        }
        if !state.sql.secondary_descriptors_engine().iter().any(|d| {
            d.table == table.id
                && request.equal.len() <= d.columns.len()
                && request
                    .equal
                    .iter()
                    .zip(&d.columns)
                    .all(|((id, _), c)| id == c)
                && request
                    .range
                    .is_none_or(|r| d.columns.get(request.equal.len()) == Some(&r.column))
        }) {
            return Err(CoreError::Sql(relational::SqlError::Unsupported));
        }
        state
            .sql
            .secondary_read_engine(table, epoch, request, || {
                crate::cancellation::check().is_err()
            })
            .map_err(CoreError::Sql)
    }
    pub(crate) fn create_index_engine(
        &mut self,
        table: &relational::TableDef,
        name: &str,
        columns: &[u64],
    ) -> Result<u64, CoreError> {
        self.create_index_internal_engine(table, name, columns, false)
    }
    pub(crate) fn create_index_internal_engine(
        &mut self,
        table: &relational::TableDef,
        name: &str,
        columns: &[u64],
        unique: bool,
    ) -> Result<u64, CoreError> {
        let (_, epoch) = self.snapshot_identity_engine()?;
        if !self.catalog_empty_engine()? {
            return Err(CoreError::Unsupported);
        }
        let result = (|| {
            if self.is_v2_engine() {
                let db = self.v2_lease(Holder::Sql)?;
                if unique {
                    return Err(CoreError::Unsupported);
                }
                let key = self.inspect_metadata(|s| Ok(s.sql.table_key(table)))?;
                let (domain, current) = db.inspect_binding(
                    self.v2_transaction.as_ref(),
                    |domain, _, tables| (domain, tables.get(&key) == Some(table)),
                )?;
                if !current {
                    return Err(CoreError::Sql(relational::SqlError::StaleSchema));
                }
                if table.hidden_rowid().is_some() {
                    return Err(CoreError::Sql(relational::SqlError::Unsupported));
                }
                let names = columns.iter().map(|id| {
                    table.columns.iter().find(|column| column.id == *id)
                        .map(|column| column.name.clone())
                        .ok_or(CoreError::Sql(relational::SqlError::UnknownColumn))
                }).collect::<Result<Vec<_>, _>>()?;
                let mut prepared = relational::Prepared::create_index_engine(
                    key, name.into(), names, domain, 0,
                );
                prepared.transaction = self.transaction_id;
                self.execute_bound_engine(&prepared, &[], None)?;
                return self.catalog_metadata()?.indexes.into_iter()
                    .find(|index| index.table == table.id && index.name == name)
                    .map(|index| index.id)
                    .ok_or(CoreError::Sql(relational::SqlError::UnknownTable));
            }
            let revision = self
                .staged_revision
                .checked_add(1)
                .ok_or(CoreError::Sql(relational::SqlError::Overflow))?;
            let state = self
                .staged
                .as_mut()
                .ok_or(CoreError::Sql(relational::SqlError::Transaction))?;
            if table.hidden_rowid().is_some() {
                if state.sql.tables.get(&state.sql.table_key(table)) != Some(table) {
                    return Err(CoreError::Sql(relational::SqlError::StaleSchema));
                }
                return Err(CoreError::Sql(relational::SqlError::Unsupported));
            }
            let id = if unique {
                state.sql.create_secondary_index_internal_engine(
                    table,
                    epoch,
                    name,
                    columns,
                    true,
                    || crate::cancellation::check().is_err(),
                )
            } else {
                state
                    .sql
                    .create_secondary_index_engine(table, epoch, name, columns, || {
                        crate::cancellation::check().is_err()
                    })
            }
            .map_err(CoreError::Sql)?;
            self.staged_revision = revision;
            Ok(id)
        })();
        if result.is_err() {
            self.abort();
        }
        result
    }
}

impl Admin {
    pub(crate) fn inspect_secondary_catalog_engine<T>(
        &self,
        inspect: impl FnOnce(
            u128,
            u64,
            &BTreeMap<String, relational::TableDef>,
            &[relational::secondary_index::Descriptor],
        ) -> T,
    ) -> Result<T, CoreError> {
        self.inner.inspect_metadata(|state| {
            crate::cancellation::check()?;
            let result = inspect(
                state.view.domain,
                state.sql.epoch,
                &state.sql.tables,
                state.sql.secondary_descriptors_engine(),
            );
            crate::cancellation::check()?;
            Ok(result)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relational::{MAX_RESULT_BYTES, MAX_ROWS, Value, secondary_index_access::Request};

    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn v2_native_secondary_reads_keep_statement_snapshot_and_project_private_rows() {
        let mut token = [0; 16];
        getrandom::fill(&mut token).unwrap();
        let path = crate::test_artifact_root().join(format!("v2-native-index-snapshot-{:032x}",
            u128::from_be_bytes(token)));
        let admin = Admin::create_engine(&path, crate::Limits::default(), Engine::V2).unwrap();
        let mut session = admin.sql_session();
        for sql in ["CREATE TABLE items(id BIGINT PRIMARY KEY, tag TEXT)",
            "CREATE INDEX by_tag ON items(tag)", "INSERT INTO items VALUES(1,'z')"] {
            session.execute(sql, &[], None).unwrap();
        }
        session.begin_native().unwrap();
        let table = session.catalog_engine().unwrap().1.remove(0);
        let equal = [(table.columns[1].id, Value::Text("z".into()))];
        let projection = [table.columns[0].id];
        let mut request = Request { equal: &equal, range: None, projection: &projection,
            descending: false, residuals: &[], max_rows: MAX_ROWS, max_bytes: MAX_RESULT_BYTES };
        let first = relational::with_sql_parser_forbidden(|| session.secondary_read_native_engine(&table, &request)).unwrap();
        assert_eq!(first.columns, vec!["id"]);
        assert_eq!(first.rows[0].values, vec![Value::Int(1)]);
        admin.sql_session().execute("INSERT INTO items VALUES(2,'z')", &[], None).unwrap();
        assert_eq!(session.secondary_read_native_engine(&table, &request).unwrap(), first);
        session.execute("INSERT INTO items VALUES(3,'z')", &[], None).unwrap();
        let own = session.secondary_read_native_engine(&table, &request).unwrap();
        assert_eq!(own.rows.into_iter().map(|r| r.values).collect::<Vec<_>>(),
            vec![vec![Value::Int(1)], vec![Value::Int(2)], vec![Value::Int(3)]]);
        request.max_rows = 1;
        assert_eq!(session.secondary_read_native_engine(&table, &request), Err(CoreError::Sql(relational::SqlError::Limit)));
        request.max_rows = MAX_ROWS;
        request.descending = true;
        assert_eq!(session.secondary_read_native_engine(&table, &request), Err(CoreError::Sql(relational::SqlError::Unsupported)));
        session.rollback_native().unwrap();
        drop(session);
        drop(admin);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn native_secondary_read_and_index_creates_reject_heaps_but_accept_keyed_tables() {
        let mut token = [0; 16];
        getrandom::fill(&mut token).unwrap();
        let path = crate::test_artifact_root().join(format!(
            "heap-native-index-{:032x}",
            u128::from_be_bytes(token)
        ));
        let admin = Admin::create(&path, crate::Limits::default()).unwrap();
        let mut session = admin.sql_session();
        for sql in [
            "CREATE TABLE heap_items (value BIGINT)",
            "CREATE TABLE keyed_items (id BIGINT PRIMARY KEY, value BIGINT)",
            "CREATE INDEX heap_value ON heap_items (value)",
            "CREATE INDEX keyed_value ON keyed_items (value)",
        ] {
            session.execute(sql, &[], None).unwrap();
        }
        let tables = admin.catalog().unwrap().1;
        let heap = tables
            .iter()
            .find(|table| table.name == "heap_items")
            .unwrap();
        let keyed = tables
            .iter()
            .find(|table| table.name == "keyed_items")
            .unwrap();
        for table in [heap, keyed] {
            session.begin_engine().unwrap();
            let value = table
                .columns
                .iter()
                .find(|column| column.name == "value")
                .unwrap()
                .id;
            let equal = [(value, Value::Int(1))];
            let request = Request {
                equal: &equal,
                range: None,
                projection: &[value],
                descending: false,
                residuals: &[],
                max_rows: MAX_ROWS,
                max_bytes: MAX_RESULT_BYTES,
            };
            let result = session.secondary_read_native_engine(table, &request);
            if table.hidden_rowid().is_some() {
                assert_eq!(
                    result,
                    Err(CoreError::Sql(relational::SqlError::Unsupported))
                );
            } else {
                assert!(result.unwrap().rows.is_empty());
            }
            session.rollback();
            for unique in [false, true] {
                session.begin_engine().unwrap();
                let result = if unique {
                    session.create_index_internal_engine(table, "native_value", &[value], true)
                } else {
                    session.create_index_engine(table, "native_value", &[value])
                };
                if table.hidden_rowid().is_some() {
                    assert_eq!(
                        result,
                        Err(CoreError::Sql(relational::SqlError::Unsupported))
                    );
                } else {
                    assert!(result.is_ok());
                }
                session.rollback();
            }
        }
        drop(session);
        drop(admin);
        std::fs::remove_dir_all(path).unwrap();
    }
}
