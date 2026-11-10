//! Bounded multi-table statement staging over an external row store.
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

    fn indexes(&self) -> Vec<secondary_index::Descriptor> {
        Vec::new()
    }
    fn apply_indexes(
        &mut self,
        _table: &TableDef,
        _after_table: Option<&TableDef>,
        _before: &BTreeMap<Vec<u8>, SqlRow>,
        _after: &BTreeMap<Vec<u8>, SqlRow>,
        _descriptors: &[secondary_index::Descriptor],
    ) -> Result<(), CoreError> {
        Ok(())
    }
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
        let bound = prepared.bind_schemas(self).map_err(CoreError::Sql)?;
        let prepared = &bound;
        // Keep the mutation target separate from the read set: a join's first
        // source must never be published, and INSERT ... SELECT still needs its
        // destination's complete occupancy image for immediate PK checks.
        let table = (!matches!(prepared.v2_class(), V2Class::Read))
            .then(|| {
                prepared
                    .table()
                    .and_then(|name| self.tables.get(name))
                    .cloned()
            })
            .flatten();
        let mut names = prepared.read_tables();
        if let Some(table) = &table
            && !names.contains(&self.table_key(table))
        {
            names.push(self.table_key(table));
        }
        // One budget across ALL scanned relations, including nested subqueries.
        // The target preimage and legacy execution retain A1's separate bounds.
        let mut retention = scan::Retention::new(scan::retention_limit());
        for name in names {
            let Some(table) = self.tables.get(&name) else {
                // Binding owns SQL error precedence (including unknown tables).
                continue;
            };
            let is_target = prepared.table() == Some(self.table_key(table).as_str());
            let rows = self
                .rows
                .get_mut(&table.id)
                .ok_or(CoreError::Sql(SqlError::UnknownTable))?;
            sink.scan(table, &mut |row| {
                if crate::cancellation::check().is_err() {
                    return Err(SqlError::Aborted);
                }
                // Only destination rows constrain its version allocator. A
                // concurrent source writer may hold a newer reserved range.
                if is_target {
                    self.next_version = self
                        .next_version
                        .max(row.version.checked_add(1).ok_or(SqlError::Overflow)?);
                }
                let key = pk(table, &row.values)?;
                retention.charge(searched::retained_row_bytes(&key, &row)?)?;
                if rows.insert(key, row).is_some() {
                    return Err(SqlError::Duplicate);
                }
                Ok(())
            })?;
        }
        self.secondary = self
            .secondary_build_complete_engine(&sink.indexes(), || {
                crate::cancellation::check().is_err()
            })
            .map_err(CoreError::Sql)?;
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
            let after_table = self.tables.values().find(|t| t.id == table.id);
            sink.apply_indexes(
                table,
                after_table,
                before,
                after,
                self.secondary_descriptors_engine(),
            )?;
            // Index hook owns re-keyed rows, whose column layout differs.
            if after_table.is_none_or(|new| {
                new.primary_key == table.primary_key && new.columns.len() == table.columns.len()
            }) {
                sink.apply(table, before, after)?;
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct WideSources {
        scans: Vec<(String, usize)>,
    }
    impl RowSink for WideSources {
        fn scan(
            &mut self,
            table: &TableDef,
            visit: &mut dyn FnMut(SqlRow) -> R<()>,
        ) -> Result<(), CoreError> {
            self.scans.push((table.name.clone(), 0));
            // Each table is below both MAX_ROWS and 64 MiB. Together they exceed
            // the bridge's shared budget; reject before running the cross join.
            for id in 0..600 {
                self.scans.last_mut().unwrap().1 += 1;
                visit(SqlRow {
                    values: vec![Value::Int(id), Value::Text("x".repeat(60 * 1024))],
                    version: 1,
                })
                .map_err(CoreError::Sql)?;
            }
            Ok(())
        }
        fn apply(
            &mut self,
            _: &TableDef,
            _: &BTreeMap<Vec<u8>, SqlRow>,
            _: &BTreeMap<Vec<u8>, SqlRow>,
        ) -> Result<(), CoreError> {
            panic!("read-only statements must never publish");
        }
    }

    #[test]
    fn staging_budget_is_shared_across_tables() {
        let mut image = Relational::default();
        for sql in [
            "CREATE TABLE a (id bigint PRIMARY KEY,v text)",
            "CREATE TABLE b (id bigint PRIMARY KEY,v text)",
        ] {
            let p = Prepared::parse(sql, 1, image.epoch).unwrap();
            image.execute(&p, &[], None).unwrap();
        }
        let p = Prepared::parse("SELECT a.id,b.id FROM a JOIN b ON true", 1, image.epoch).unwrap();
        let mut sink = WideSources { scans: Vec::new() };
        assert_eq!(
            image.execute_with_sink(&p, &[], None, None, &mut sink, u64::MAX),
            Err(CoreError::Sql(SqlError::Limit))
        );
        assert_eq!(sink.scans[0], ("a".into(), 600));
        assert_eq!(sink.scans[1].0, "b");
        assert!((1..600).contains(&sink.scans[1].1));
    }
}
