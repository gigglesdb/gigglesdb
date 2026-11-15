//! Native catalog bridge; publication remains the session commit's responsibility.
use super::*;

impl SqlSession {
    pub(crate) fn secondary_read_native_engine(
        &mut self,
        table: &relational::TableDef,
        request: &relational::secondary_index_access::Request<'_>,
    ) -> Result<relational::SqlResult, CoreError> {
        if self.is_v2_engine() {
            return Err(CoreError::Sql(relational::SqlError::Unsupported));
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
            let revision = self
                .staged_revision
                .checked_add(1)
                .ok_or(CoreError::Sql(relational::SqlError::Overflow))?;
            let state = self
                .staged
                .as_mut()
                .ok_or(CoreError::Sql(relational::SqlError::Transaction))?;
            if state.applications.is_some() {
                return Err(CoreError::Unsupported);
            }
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
        self.inner.inspect(|state| {
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
