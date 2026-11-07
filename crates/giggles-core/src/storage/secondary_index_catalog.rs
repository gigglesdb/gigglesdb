//! Native catalog bridge; publication remains the session commit's responsibility.
use super::*;

impl SqlSession {
    pub(crate) fn secondary_read_native_engine(
        &mut self,
        table: &relational::TableDef,
        request: &relational::secondary_index_access::Request<'_>,
    ) -> Result<relational::SqlResult, CoreError> {
        let (_, epoch) = self.snapshot_identity_engine()?;
        let state = self
            .staged
            .as_ref()
            .ok_or(CoreError::Sql(relational::SqlError::Transaction))?;
        if state.sql.tables.get(&table.name) != Some(table) {
            return Err(CoreError::Sql(relational::SqlError::StaleSchema));
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
