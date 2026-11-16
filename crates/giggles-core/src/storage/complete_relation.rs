//! Complete, bounded materializations tied to one active transaction revision.
use super::{CoreError, SqlSession};
use crate::relational::{MAX_ROWS, Scan, SqlError, SqlRow, TableDef};

#[derive(Debug, PartialEq, Eq)]
struct Snapshot {
    domain: u128,
    epoch: u64,
    transaction: u128,
    revision: u64,
    read_timestamp: Option<u64>,
    table: TableDef,
}

/// Only a successful full-table proof can construct this value. In particular,
/// ordinary paged scans and row vectors cannot be promoted to complete results.
/// It is consumed once, after checking the current transaction and revision.
#[derive(Debug)]
pub(crate) struct CompleteRelation {
    snapshot: Snapshot,
    rows: Vec<SqlRow>,
}

impl SqlSession {
    /// V2 read input is complete without imposing the legacy table capacity.
    /// The physical visitor decodes one row at a time; native order/filter
    /// evaluation retains the resulting relation, independently of output pages.
    pub(crate) fn complete_read_table_engine(
        &mut self,
        table: &TableDef,
    ) -> Result<CompleteRelation, CoreError> {
        if !self.is_v2_engine() {
            return self.complete_table_engine(table);
        }
        let result = (|| {
            let snapshot = self.complete_snapshot(table)?;
            let mut rows = Vec::new();
            let mut retained = crate::relational::scan::Retention::new(
                crate::relational::scan::MAX_RETAINED_BYTES,
            );
            self.visit_native_page(table, &mut |row| {
                retained.charge(std::mem::size_of::<SqlRow>()).map_err(CoreError::Sql)?;
                for value in &row.values {
                    retained.charge(std::mem::size_of_val(value) + crate::relational::value_bytes(value))
                        .map_err(CoreError::Sql)?;
                }
                rows.push(row.clone());
                Ok(())
            }, |error| error)?;
            Ok(CompleteRelation { snapshot, rows })
        })();
        if result.is_err() {
            self.abort();
        }
        result
    }

    /// Stream one statement snapshot without asserting full-table boundedness.
    pub(crate) fn visit_native_page<E>(
        &mut self, table: &TableDef,
        visit: &mut impl FnMut(&SqlRow) -> Result<(), E>,
        map: fn(CoreError) -> E,
    ) -> Result<(), E> {
        let snapshot = self.complete_snapshot(table).map_err(map)?;
        #[cfg(feature = "engine-rocksdb")]
        if let super::Backend::V2(db) = &self.admin.inner.backend {
            let db = db.clone();
            self.visit_native_page_v2(&db, table, visit, map)?;
        } else {
            self.visit_native_page_legacy(table, visit, map)?;
        }
        #[cfg(not(feature = "engine-rocksdb"))]
        self.visit_native_page_legacy(table, visit, map)?;
        if self.complete_snapshot(table).map_err(map)? != snapshot {
            return Err(map(CoreError::Sql(SqlError::StaleSchema)));
        }
        Ok(())
    }

    fn visit_native_page_legacy<E>(
        &self, table: &TableDef, visit: &mut impl FnMut(&SqlRow) -> Result<(), E>,
        map: fn(CoreError) -> E,
    ) -> Result<(), E> {
        use crate::relational::scan::{Direction, KeyRange, LegacyRows, RowSource};
        let state = self.staged.as_ref().ok_or_else(|| map(CoreError::Sql(SqlError::Transaction)))?;
        let source = LegacyRows(&state.sql);
        let range = KeyRange { lo: std::ops::Bound::Unbounded, hi: std::ops::Bound::Unbounded };
        for row in source.scan(table, &range, Direction::Forward).map_err(|e| map(CoreError::Sql(e)))? {
            crate::cancellation::check().map_err(map)?;
            visit(&row.map_err(|e| map(CoreError::Sql(e)))?.row)?;
        }
        Ok(())
    }

    fn complete_snapshot(&mut self, table: &TableDef) -> Result<Snapshot, CoreError> {
        if self.aborted {
            return Err(CoreError::Sql(SqlError::Aborted));
        }
        if self.is_v2_engine() {
            #[cfg(feature = "engine-rocksdb")]
            self.v2_transaction.as_mut()
                .ok_or(CoreError::Sql(SqlError::Transaction))?.snapshot_ts();
            let (domain, epoch) = self.snapshot_identity_engine()?;
            let transaction = self.transaction_id.ok_or(CoreError::Sql(SqlError::Transaction))?;
            if !self.catalog_engine()?.1.contains(table) {
                return Err(CoreError::Sql(SqlError::StaleSchema));
            }
            if table.hidden_rowid().is_some() {
                return Err(CoreError::Sql(SqlError::Unsupported));
            }
            return Ok(Snapshot { domain, epoch, transaction,
                revision: self.staged_revision,
                read_timestamp: self.v2_transaction.as_ref()
                    .and_then(|txn| txn.current_snapshot_ts()).map(|ts| ts.0),
                table: table.clone() });
        }
        let staged = self
            .staged
            .as_ref()
            .ok_or(CoreError::Sql(SqlError::Transaction))?;
        let transaction = self
            .transaction_id
            .ok_or(CoreError::Sql(SqlError::Transaction))?;
        crate::cancellation::check()?;
        if staged.sql.tables.get(&staged.sql.table_key(table)) != Some(table) {
            return Err(CoreError::Sql(SqlError::StaleSchema));
        }
        if table.hidden_rowid().is_some() {
            return Err(CoreError::Sql(SqlError::Unsupported));
        }
        Ok(Snapshot {
            domain: staged.view.domain,
            epoch: staged.sql.epoch,
            transaction,
            revision: self.staged_revision,
            read_timestamp: None,
            table: table.clone(),
        })
    }

    pub(crate) fn complete_table_engine(
        &mut self,
        table: &TableDef,
    ) -> Result<CompleteRelation, CoreError> {
        let result = (|| {
            let snapshot = self.complete_snapshot(table)?;
            let key = self.inspect_metadata(|s| Ok(s.sql.table_key(table)))?;
            let rows = self
                .scan(Scan {
                    table: key.clone(),
                    projection: vec![],
                    prefix: vec![],
                    lower: None,
                    upper: None,
                    reverse: false,
                    limit: MAX_ROWS,
                })?
                .rows;
            if rows.len() == MAX_ROWS {
                // The page bound is not a table capacity. Prove that the final
                // full primary key equals this same snapshot's actual maximum.
                let maximum = self.scan(Scan {
                    table: key,
                    projection: table
                        .primary_key
                        .iter()
                        .map(|index| table.columns[*index].name.clone())
                        .collect(),
                    prefix: vec![],
                    lower: None,
                    upper: None,
                    reverse: true,
                    limit: 1,
                })?;
                let last = rows.last().ok_or(CoreError::Limit)?;
                let last_key: Vec<_> = table
                    .primary_key
                    .iter()
                    .map(|index| last.values[*index].clone())
                    .collect();
                if maximum.rows.len() != 1 || maximum.rows[0].values != last_key {
                    return Err(CoreError::Limit);
                }
            }
            if self.complete_snapshot(table)? != snapshot {
                return Err(CoreError::Sql(SqlError::StaleSchema));
            }
            Ok(CompleteRelation { snapshot, rows })
        })();
        if result.is_err() {
            self.abort();
        }
        result
    }

    pub(crate) fn consume_complete_engine(
        &mut self,
        relation: CompleteRelation,
        table: &TableDef,
    ) -> Result<Vec<SqlRow>, CoreError> {
        let result = (|| {
            if self.complete_snapshot(table)? != relation.snapshot {
                return Err(CoreError::Sql(SqlError::StaleSchema));
            }
            Ok(relation.rows)
        })();
        if result.is_err() {
            self.abort();
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Admin, Cancellation, Limits,
        relational::{
            MAX_RESULT_BYTES, MAX_ROW_BYTES, Prepared, Value as V, with_sql_parser_forbidden,
        },
    };

    fn fixture(count: usize, note_bytes: usize) -> (Admin, SqlSession, TableDef) {
        let root = crate::test_artifact_root();
        std::fs::create_dir_all(&root).unwrap();
        let mut token = [0u8; 16];
        getrandom::fill(&mut token).unwrap();
        let path = root.join(format!(
            "complete-relation-{:032x}",
            u128::from_be_bytes(token)
        ));
        let admin = Admin::create(path, Limits::default()).unwrap();
        let mut session = admin.sql_session();
        session
            .execute(
                "CREATE TABLE complete_items (tenant BIGINT, id BIGINT, note TEXT NOT NULL, PRIMARY KEY (tenant,id))",
                &[],
                None,
            )
            .unwrap();
        let table = admin.catalog().unwrap().1.remove(0);
        // Seed boundary fixtures in one trusted storage transaction. Tests below
        // exercise session writes separately, including revision invalidation.
        if count != 0 {
            admin
                .inner
                .change(|state| {
                    for id in 0..count {
                        let prepared = Prepared::insert_engine(
                            &table,
                            vec![
                                V::Int(7),
                                V::Int(id as i64),
                                V::Text("x".repeat(note_bytes)),
                            ],
                            state.view.domain,
                            &state.sql,
                        )
                        .map_err(CoreError::Sql)?;
                        state
                            .sql
                            .execute(&prepared, &[], None)
                            .map_err(CoreError::Sql)?;
                    }
                    Ok(())
                })
                .unwrap();
        }
        (admin, session, table)
    }

    fn page(session: &mut SqlSession, table: &TableDef) -> Vec<SqlRow> {
        session
            .scan(Scan {
                table: table.name.clone(),
                projection: vec![],
                prefix: vec![],
                lower: None,
                upper: None,
                reverse: false,
                limit: MAX_ROWS,
            })
            .unwrap()
            .rows
    }

    #[test]
    fn heap_complete_relations_and_typed_session_wrappers_are_unsupported() {
        let (_, mut session, keyed) = fixture(0, 1);
        session
            .execute("CREATE TABLE heap_items (note TEXT)", &[], None)
            .unwrap();
        let heap = session
            .catalog_engine()
            .unwrap()
            .1
            .into_iter()
            .find(|table| table.name == "heap_items")
            .unwrap();
        with_sql_parser_forbidden(|| {
            session.begin_engine().unwrap();
            let complete = session.complete_table_engine(&keyed).unwrap();
            assert!(
                session
                    .consume_complete_engine(complete, &keyed)
                    .unwrap()
                    .is_empty()
            );
            assert!(matches!(
                session.complete_table_engine(&heap),
                Err(CoreError::Sql(SqlError::Unsupported))
            ));
            assert!(session.is_aborted());
            session.rollback();
            for operation in 0..4 {
                session.begin_engine().unwrap();
                let result = match operation {
                    0 => session.insert_engine(&heap, vec![]).map(|_| ()),
                    1 => session.update_engine(&heap, vec![], vec![], 1).map(|_| ()),
                    2 => session.delete_engine(&heap, vec![], 1).map(|_| ()),
                    _ => session.row_version_engine(&heap, &[]).map(|_| ()),
                };
                assert_eq!(result, Err(CoreError::Sql(SqlError::Unsupported)));
                session.rollback();
            }
            session.begin_engine().unwrap();
            session
                .insert_engine(&keyed, vec![V::Int(7), V::Int(1), V::Text("one".into())])
                .unwrap();
            let key = vec![V::Int(7), V::Int(1)];
            let version = session.row_version_engine(&keyed, &key).unwrap();
            session
                .update_engine(
                    &keyed,
                    key.clone(),
                    vec![(2, V::Text("two".into()))],
                    version,
                )
                .unwrap();
            let version = session.row_version_engine(&keyed, &key).unwrap();
            session.delete_engine(&keyed, key, version).unwrap();
            session.rollback();
        });
    }

    #[test]
    fn empty_and_exactly_full_relations_are_complete_but_a_page_is_not_a_proof() {
        for count in [0, MAX_ROWS, MAX_ROWS + 1] {
            let (admin, mut session, table) = fixture(count, 1);
            let before = admin.inner.inspect(|state| Ok(state.sql.clone())).unwrap();
            with_sql_parser_forbidden(|| {
                session.begin_engine().unwrap();
                // Scan remains a paged API even when the table has 1025 rows.
                assert_eq!(page(&mut session, &table).len(), count.min(MAX_ROWS));
                let complete = session.complete_table_engine(&table);
                if count > MAX_ROWS {
                    assert!(matches!(complete, Err(CoreError::Limit)));
                    assert!(session.is_aborted());
                } else {
                    let rows = session
                        .consume_complete_engine(complete.unwrap(), &table)
                        .unwrap();
                    assert_eq!(rows.len(), count);
                    if count != 0 {
                        // All rows share their first key component. Comparing
                        // only that component would incorrectly accept 1025.
                        assert_eq!(rows.last().unwrap().values[1], V::Int((count - 1) as i64));
                    }
                }
                session.rollback();
            });
            assert_eq!(
                admin.inner.inspect(|state| Ok(state.sql.clone())).unwrap(),
                before
            );
        }
    }

    #[test]
    fn result_byte_overflow_aborts_without_changing_rows_or_versions() {
        let count = MAX_RESULT_BYTES / MAX_ROW_BYTES + 1;
        let (admin, mut session, table) = fixture(count, MAX_ROW_BYTES - 18);
        let before = admin.inner.inspect(|state| Ok(state.sql.clone())).unwrap();
        with_sql_parser_forbidden(|| {
            session.begin_engine().unwrap();
            assert!(matches!(
                session.complete_table_engine(&table),
                Err(CoreError::Sql(SqlError::Limit))
            ));
            assert!(session.is_aborted());
            session.rollback();
        });
        assert_eq!(
            admin.inner.inspect(|state| Ok(state.sql.clone())).unwrap(),
            before
        );
    }

    #[test]
    fn completeness_requires_an_active_nonaborted_transaction_and_exact_descriptor() {
        let (_, mut session, table) = fixture(1, 1);
        assert!(matches!(
            session.complete_table_engine(&table),
            Err(CoreError::Sql(SqlError::Transaction))
        ));
        session.begin_engine().unwrap();
        let relation = session.complete_table_engine(&table).unwrap();
        session.abort();
        assert!(matches!(
            session.complete_table_engine(&table),
            Err(CoreError::Sql(SqlError::Aborted))
        ));
        assert_eq!(
            session.consume_complete_engine(relation, &table),
            Err(CoreError::Sql(SqlError::Aborted))
        );
        session.rollback();

        session.begin_engine().unwrap();
        let mut malformed = table.clone();
        malformed.primary_key = vec![usize::MAX];
        assert!(matches!(
            session.complete_table_engine(&malformed),
            Err(CoreError::Sql(SqlError::StaleSchema))
        ));
        assert!(session.is_aborted());
        session.rollback();

        session
            .execute(
                "CREATE TABLE other_items (id BIGINT PRIMARY KEY)",
                &[],
                None,
            )
            .unwrap();
        session.begin_engine().unwrap();
        let other = session
            .catalog_engine()
            .unwrap()
            .1
            .into_iter()
            .find(|table| table.name == "other_items")
            .unwrap();
        let relation = session.complete_table_engine(&table).unwrap();
        assert_eq!(
            session.consume_complete_engine(relation, &other),
            Err(CoreError::Sql(SqlError::StaleSchema))
        );
        assert!(session.is_aborted());
    }

    #[test]
    fn complete_relations_expire_on_transaction_exit_and_cannot_cross_sessions() {
        let (admin, mut session, table) = fixture(1, 1);
        for commit in [false, true] {
            session.begin_engine().unwrap();
            let relation = session.complete_table_engine(&table).unwrap();
            if commit {
                session.commit_engine().unwrap();
            } else {
                session.rollback();
            }
            session.begin_engine().unwrap();
            assert_eq!(
                session.consume_complete_engine(relation, &table),
                Err(CoreError::Sql(SqlError::StaleSchema))
            );
            session.rollback();
        }

        session.begin_engine().unwrap();
        let relation = session.complete_table_engine(&table).unwrap();
        let mut other = admin.sql_session();
        other.begin_engine().unwrap();
        assert_eq!(
            other.consume_complete_engine(relation, &table),
            Err(CoreError::Sql(SqlError::StaleSchema))
        );
        assert!(other.is_aborted());
        assert!(!session.is_aborted());

        let relation = session.complete_table_engine(&table).unwrap();
        session.rollback();
        assert_eq!(
            session.consume_complete_engine(relation, &table),
            Err(CoreError::Sql(SqlError::Transaction))
        );
    }

    #[test]
    fn successful_sql_writes_and_ddl_invalidate_prior_materializations() {
        let (admin, mut session, table) = fixture(1, 1);
        let before = admin.inner.inspect(|state| Ok(state.sql.clone())).unwrap();
        for statement in [
            "INSERT INTO complete_items VALUES (7,1,'new')",
            "UPDATE complete_items SET note='updated' WHERE tenant=7 AND id=0",
            "UPDATE complete_items SET note='x' WHERE tenant=7 AND id=0",
            "DELETE FROM complete_items WHERE tenant=7 AND id=0",
            "CREATE TABLE other_items (id BIGINT PRIMARY KEY)",
        ] {
            session.begin_engine().unwrap();
            let relation = session.complete_table_engine(&table).unwrap();
            session.execute(statement, &[], None).unwrap();
            assert_eq!(
                session.consume_complete_engine(relation, &table),
                Err(CoreError::Sql(SqlError::StaleSchema)),
                "{statement}"
            );
            assert!(session.is_aborted());
            session.rollback();
        }
        assert_eq!(
            admin.inner.inspect(|state| Ok(state.sql.clone())).unwrap(),
            before
        );
    }

    #[test]
    fn reads_preserve_materializations_but_typed_writes_invalidate_them() {
        let (_, mut session, table) = fixture(1, 1);
        with_sql_parser_forbidden(|| {
            session.begin_engine().unwrap();
            let relation = session.complete_table_engine(&table).unwrap();
            assert_eq!(page(&mut session, &table).len(), 1);
            assert_eq!(session.catalog_engine().unwrap().1, vec![table.clone()]);
            assert_eq!(
                session
                    .consume_complete_engine(relation, &table)
                    .unwrap()
                    .len(),
                1
            );
            let relation = session.complete_table_engine(&table).unwrap();
            session
                .insert_engine(&table, vec![V::Int(7), V::Int(1), V::Text("new".into())])
                .unwrap();
            assert_eq!(
                session.consume_complete_engine(relation, &table),
                Err(CoreError::Sql(SqlError::StaleSchema))
            );
        });
    }

    #[test]
    fn cancellation_rejects_production_and_consumption_of_complete_relations() {
        let (admin, mut session, table) = fixture(1, 1);
        let before = admin.inner.inspect(|state| Ok(state.sql.clone())).unwrap();
        let cancelled = Cancellation::default();
        assert!(cancelled.cancel());
        session.begin_engine().unwrap();
        assert!(matches!(
            cancelled.scope(|| session.complete_table_engine(&table)),
            Err(CoreError::Cancelled)
        ));
        assert!(session.is_aborted());
        session.rollback();

        session.begin_engine().unwrap();
        let relation = session.complete_table_engine(&table).unwrap();
        assert_eq!(
            cancelled.scope(|| session.consume_complete_engine(relation, &table)),
            Err(CoreError::Cancelled)
        );
        assert!(session.is_aborted());
        session.rollback();
        assert_eq!(
            admin.inner.inspect(|state| Ok(state.sql.clone())).unwrap(),
            before
        );
    }

    #[test]
    fn completeness_uses_the_transaction_snapshot_despite_concurrent_commits() {
        let (admin, mut session, table) = fixture(MAX_ROWS, 1);
        session.begin_engine().unwrap();
        let concurrent = admin.clone();
        std::thread::spawn(move || {
            concurrent
                .sql_session()
                .execute(
                    "INSERT INTO complete_items VALUES (7,1024,'concurrent')",
                    &[],
                    None,
                )
                .unwrap();
        })
        .join()
        .unwrap();
        with_sql_parser_forbidden(|| {
            let relation = session.complete_table_engine(&table).unwrap();
            assert_eq!(
                session
                    .consume_complete_engine(relation, &table)
                    .unwrap()
                    .len(),
                MAX_ROWS
            );
            assert_eq!(
                session.commit_engine(),
                Err(CoreError::Sql(SqlError::Conflict))
            );
            session.rollback();
            session.begin_engine().unwrap();
            assert!(matches!(
                session.complete_table_engine(&table),
                Err(CoreError::Limit)
            ));
            assert!(session.is_aborted());
        });
    }

    #[test]
    fn revision_overflow_aborts_before_accepting_a_stage_and_resets_on_rollback() {
        let (admin, mut session, table) = fixture(1, 1);
        session.begin_engine().unwrap();
        session.staged_revision = u64::MAX;
        let before = session.staged.as_ref().unwrap().sql.clone();
        assert_eq!(
            session.insert_engine(&table, vec![V::Int(7), V::Int(1), V::Text("new".into())]),
            Err(CoreError::Sql(SqlError::Overflow))
        );
        assert!(session.is_aborted());
        assert_eq!(session.staged.as_ref().unwrap().sql, before);
        session.rollback();
        assert_eq!(session.staged_revision, 0);
        session.begin_engine().unwrap();
        assert_eq!(session.staged_revision, 0);
        assert_eq!(page(&mut session, &table).len(), 1);
        assert_eq!(
            admin.inner.inspect(|state| Ok(state.sql.clone())).unwrap(),
            before
        );
    }
}
