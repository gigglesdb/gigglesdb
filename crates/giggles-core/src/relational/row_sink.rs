//! Bounded single-table statement staging over an external row store.
//!
//! The legacy publisher checks complete pre/postimages (including immediate PK
//! occupancy, witness ownership, work and byte limits). Reuse those checks in a
//! transient statement image, then publish the validated difference through the
//! caller's transaction. This adds no fields to the serialized Relational image.
use super::*;
use crate::CoreError;

pub(crate) trait RowSink {
    fn scan(
        &mut self,
        table: &TableDef,
        visit: &mut dyn FnMut(SqlRow) -> R<()>,
    ) -> Result<(), CoreError>;

    /// Publish the difference atomically; the caller owns a statement savepoint.
    fn apply(
        &mut self,
        table: &TableDef,
        before: &BTreeMap<Vec<u8>, SqlRow>,
        after: &BTreeMap<Vec<u8>, SqlRow>,
    ) -> Result<(), CoreError>;
}

impl Relational {
    pub(crate) fn execute_with_sink(
        &mut self,
        prepared: &Prepared,
        params: &[Value],
        expected: Option<u64>,
        clock: Option<&clock::ExecutionClock>,
        sink: &mut impl RowSink,
        version_end: u64,
    ) -> Result<SqlResult, CoreError> {
        let table = prepared
            .table()
            .and_then(|name| self.tables.get(name))
            .cloned();
        let mut retention = scan::Retention::new(scan::MAX_RETAINED_BYTES);
        if let Some(table) = &table {
            let rows = self
                .rows
                .get_mut(&table.id)
                .ok_or(CoreError::Sql(SqlError::UnknownTable))?;
            sink.scan(table, &mut |row| {
                if crate::cancellation::check().is_err() {
                    return Err(SqlError::Aborted);
                }
                self.next_version = self
                    .next_version
                    .max(row.version.checked_add(1).ok_or(SqlError::Overflow)?);
                let key = pk(table, &row.values)?;
                retention.charge(searched::retained_row_bytes(&key, &row)?)?;
                if rows.insert(key, row).is_some() {
                    return Err(SqlError::Duplicate);
                }
                Ok(())
            })?;
        }
        let before = table.as_ref().map(|table| self.rows[&table.id].clone());
        let result = self
            .execute_clocked(
                prepared,
                params,
                expected,
                || crate::cancellation::check().is_err(),
                clock,
            )
            .map_err(CoreError::Sql)?;
        if self.next_version > version_end {
            return Err(CoreError::Corrupt(
                "v2 SQL exceeded row-version reservation".into(),
            ));
        }
        if let (Some(table), Some(before)) = (&table, &before) {
            let empty = BTreeMap::new();
            let after = self.rows.get(&table.id).unwrap_or(&empty);
            sink.apply(table, before, after)?;
        }
        Ok(result)
    }
}
