//! SQL DDL bridge: relational plans mutate a rows-free image, then the v2 catalog.
use super::*;
use engine::{catalog::CatalogParts, txn::Isolation};

impl SqlSession {
    pub(super) fn prepare_v2(
        &mut self,
        sql: &str,
        domain: u128,
    ) -> Result<relational::Prepared, CoreError> {
        relational::assert_sql_frontend_allowed();
        let result = relational::Prepared::parse(sql, domain, 0)
            .map_err(CoreError::Sql)
            .and_then(|mut p| {
                if self.aborted && !p.allows_aborted_transaction() {
                    return Err(CoreError::Sql(relational::SqlError::Aborted));
                }
                p.transaction = self.transaction_id;
                Ok(p)
            });
        if result.is_err() {
            self.abort();
        }
        result
    }

    pub(super) fn execute_v2(
        &mut self,
        db: &v2::V2Db,
        p: &relational::Prepared,
        params: &[relational::Value],
        expected_version: Option<u64>,
        clock: Option<&relational::clock::ExecutionClock>,
    ) -> Result<relational::SqlResult, CoreError> {
        use relational::{SqlError as E, V2Class};
        if self.aborted && !p.allows_aborted_transaction() {
            return Err(CoreError::Sql(E::Aborted));
        }
        let result = (|| {
            crate::cancellation::check()?;
            if p.domain != u128::from_be_bytes(db.meta.domain)
                || p.epoch != 0
                || (p.transaction.is_some() && p.transaction != self.transaction_id)
            {
                return Err(CoreError::Sql(E::StaleSchema));
            }
            let unsupported = |feature| CoreError::EngineUnsupported {
                engine: "v2",
                feature,
            };
            match p.v2_class() {
                V2Class::Control(control) => {
                    if !params.is_empty() || expected_version.is_some() {
                        return Err(CoreError::Sql(E::Parameters));
                    }
                    match control {
                        "begin" => {
                            if self.v2_transaction.is_some() {
                                return Err(CoreError::Sql(E::Transaction));
                            }
                            let mut token = [0u8; 16];
                            getrandom::fill(&mut token).map_err(io)?;
                            self.v2_transaction = Some(db.begin(Isolation::ReadCommitted));
                            self.transaction_id = Some(u128::from_be_bytes(token));
                        }
                        "commit" => {
                            let txn = self
                                .v2_transaction
                                .take()
                                .ok_or(CoreError::Sql(E::Transaction))?;
                            let committed = db.catalog().commit_ddl(txn).map_err(CoreError::from);
                            self.rollback();
                            committed?;
                        }
                        "rollback" => {
                            if self.v2_transaction.is_none() {
                                return Err(CoreError::Sql(E::Transaction));
                            }
                            self.rollback();
                        }
                        _ => unreachable!("v2 classification limits transaction controls"),
                    }
                    Ok(Default::default())
                }
                V2Class::Ddl => {
                    if expected_version.is_some() {
                        return Err(CoreError::Sql(E::Parameters));
                    }
                    let mut implicit = None;
                    let txn = match &mut self.v2_transaction {
                        Some(txn) => txn,
                        None => implicit.insert(db.begin(Isolation::ReadCommitted)),
                    };
                    txn.begin_statement();
                    let mark = txn.savepoint();
                    let result = (|| {
                        let old = db.catalog().parts(txn)?;
                        // This bounds every ID E1 DDL allocates per statement.
                        let ids = db
                            .catalog()
                            .reserve_ids(3 * relational::MAX_COLUMNS as u64 + 64)?;
                        let mut image = relational::Relational::catalog_image(
                            old.tables.clone(),
                            old.ext.clone(),
                            ids.start,
                            1,
                        )
                        .map_err(|error| match error {
                            E::Unsupported => CoreError::Sql(E::Unsupported),
                            error => {
                                CoreError::Corrupt(format!("invalid stored v2 catalog: {error:?}"))
                            }
                        })?;
                        let result = image
                            .execute_clocked(
                                p,
                                params,
                                None,
                                || crate::cancellation::check().is_err(),
                                clock,
                            )
                            .map_err(CoreError::Sql)?;
                        let (tables, ext, next_id, _) = image.catalog_parts();
                        if next_id > ids.end {
                            return Err(CoreError::Corrupt(
                                "v2 DDL exceeded ID reservation".into(),
                            ));
                        }
                        check_post_image(&tables, &ext, next_id)?;
                        let heap = tables
                            .values()
                            .filter(|def| {
                                if old.tables.values().any(|table| table.id == def.id) {
                                    old.heap.contains(&def.id)
                                } else {
                                    def.hidden_rowid().is_some()
                                }
                            })
                            .map(|def| def.id)
                            .collect();
                        let new = CatalogParts { tables, heap, ext };
                        db.catalog().apply_diff(txn, &old, &new, &ids, None, &|| {
                            crate::cancellation::check().is_err()
                        })?;
                        Ok(result)
                    })();
                    if result.is_err() {
                        txn.rollback_to(&mark).map_err(CoreError::from)?;
                    }
                    txn.release(mark).map_err(CoreError::from)?;
                    if self.v2_transaction.is_none() && result.is_ok() {
                        db.catalog()
                            .commit_ddl(implicit.take().expect("implicit transaction"))
                            .map_err(CoreError::from)?;
                    }
                    result
                }
                V2Class::Dml => Err(unsupported(SQL_DML)),
                V2Class::Read => Err(unsupported(SQL_QUERIES)),
                V2Class::Refused(feature) => Err(unsupported(feature)),
            }
        })();
        if result.is_err() {
            self.abort();
        }
        if self.v2_transaction.is_none() {
            self.transaction_clock_us = None;
        }
        result
    }
}

/// DD-0068 X3: validate the post-statement image before `apply_diff` can write
/// catalog changes. Unsupported features and invalid images must fail while the
/// statement can still roll back without publishing an unenforced catalog.
fn check_post_image(
    tables: &BTreeMap<String, relational::TableDef>,
    ext: &relational::catalog_ext::CatalogExt,
    next_id: u64,
) -> Result<(), CoreError> {
    relational::Relational::catalog_image(tables.clone(), ext.clone(), next_id, 1)
        .map(|_| ())
        .map_err(|error| match error {
            relational::SqlError::Unsupported => CoreError::EngineUnsupported {
                engine: "v2",
                feature: "SQL statement",
            },
            error => CoreError::Corrupt(format!("v2 DDL produced an invalid catalog: {error:?}")),
        })
}

#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "v2_sql_tests.rs"]
mod tests;
