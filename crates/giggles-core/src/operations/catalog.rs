//! Finite catalog intents over the same typed storage engine as SQL.
use super::*;

/// An unallocated column definition. Stable IDs are assigned only by execution.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnSpec {
    pub name: String,
    pub data_type: DataType,
    pub not_null: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CatalogRequest {
    CreateTable {
        table: String,
        columns: Vec<ColumnSpec>,
        primary_key: Vec<String>,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogOutcome {
    pub catalog_epoch: u64,
    pub table: TableDef,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundCatalogDescription {
    pub contract_version: u32,
    pub database_domain: u128,
    pub catalog_epoch: u64,
    /// Proposed definition without allocated object/column identifiers.
    pub request: CatalogRequest,
}
/// Process-local immutable catalog intent, not authority or a migration receipt.
#[derive(Clone)]
pub struct BoundCatalogOperation {
    description: BoundCatalogDescription,
}
impl BoundCatalogOperation {
    pub fn description(&self) -> &BoundCatalogDescription {
        &self.description
    }
}
impl std::fmt::Debug for BoundCatalogOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoundCatalogOperation")
            .field("description", &self.description)
            .finish_non_exhaustive()
    }
}
fn columns_for_create(columns: &[ColumnSpec]) -> Vec<ColumnDef> {
    columns
        .iter()
        .map(|column| ColumnDef {
            id: 0,
            name: column.name.clone(),
            data_type: column.data_type.clone(),
            not_null: column.not_null,
        })
        .collect()
}
fn validate_catalog_request(request: &CatalogRequest) -> Result<()> {
    let CatalogRequest::CreateTable {
        table,
        columns,
        primary_key,
    } = request;
    // Reject caller-owned collections before serialization or definition copies.
    if columns.len() > crate::relational::MAX_COLUMNS
        || primary_key.len() > crate::relational::MAX_COLUMNS
    {
        return Err(Error::Limit);
    }
    if columns.is_empty() {
        return Err(Error::Bind("columns".into()));
    }
    if primary_key.is_empty() {
        return Err(Error::Bind("primary key".into()));
    }
    if !identifier(table)
        || columns.iter().any(|column| !identifier(&column.name))
        || primary_key.iter().any(|name| !identifier(name))
    {
        return Err(Error::Bind("identifier".into()));
    }
    validate_size(request)?;
    let definition = columns_for_create(columns);
    crate::relational::validate_create_definition(table, &definition, primary_key)
        .map_err(|error| core(CoreError::Sql(error)))?;
    for key in primary_key {
        let column = columns
            .iter()
            .find(|column| column.name == *key)
            .ok_or_else(|| core(CoreError::Sql(SqlError::UnknownColumn)))?;
        // This explicit catalog intent rejects nullable PK annotations. SQL's
        // parsed CREATE continues to strengthen primary-key columns itself.
        if !column.not_null {
            return Err(Error::Type);
        }
    }
    Ok(())
}
fn bind_catalog(
    session: &mut SqlSession,
    request: &CatalogRequest,
) -> Result<BoundCatalogOperation> {
    let (database_domain, catalog_epoch) = session.snapshot_identity_engine().map_err(core)?;
    let (_, tables) = session.catalog_engine().map_err(core)?;
    let CatalogRequest::CreateTable { table, .. } = request;
    if tables.iter().any(|existing| existing.name == *table) {
        return Err(core(CoreError::Sql(SqlError::Duplicate)));
    }
    if tables.len() >= crate::relational::MAX_TABLES {
        return Err(core(CoreError::Sql(SqlError::Limit)));
    }
    Ok(BoundCatalogOperation {
        description: BoundCatalogDescription {
            contract_version: ENGINE_CONTRACT_VERSION,
            database_domain,
            catalog_epoch,
            request: request.clone(),
        },
    })
}
impl Engine {
    /// Execute a safe create in one fresh snapshot and one commit decision.
    pub fn execute_catalog(&self, request: &CatalogRequest) -> Result<CatalogOutcome> {
        validate_catalog_request(request)?;
        self.execute_catalog_plan(|session| bind_catalog(session, request))
    }
    /// Validate/freeze a proposed definition without allocating IDs, scanning
    /// business rows or deciding whether the database is currently populated.
    pub fn prepare_catalog_bound(&self, request: &CatalogRequest) -> Result<BoundCatalogOperation> {
        validate_catalog_request(request)?;
        let mut session = self.admin.sql_session();
        session.begin_engine().map_err(core)?;
        let result = bind_catalog(&mut session, request);
        session.rollback();
        result
    }
    /// Revalidate catalog/domain and current empty-database eligibility. This
    /// process-local handle is not a retry key and does not rebind after drift.
    pub fn execute_catalog_bound(
        &self,
        prepared: &BoundCatalogOperation,
    ) -> Result<CatalogOutcome> {
        self.execute_catalog_plan(|session| {
            let description = &prepared.description;
            let (domain, epoch) = session.snapshot_identity_engine().map_err(core)?;
            if description.contract_version != ENGINE_CONTRACT_VERSION
                || description.database_domain != domain
                || description.catalog_epoch != epoch
            {
                return Err(core(CoreError::Sql(SqlError::StaleSchema)));
            }
            bind_catalog(session, &description.request)
        })
    }
    fn execute_catalog_plan(
        &self,
        bind: impl FnOnce(&mut SqlSession) -> Result<BoundCatalogOperation>,
    ) -> Result<CatalogOutcome> {
        let mut session = self.admin.sql_session();
        session.begin_engine().map_err(core)?;
        let staged = (|| {
            let prepared = bind(&mut session)?;
            if !session.catalog_empty_engine().map_err(core)? {
                return Err(Error::PlanRequired);
            }
            let CatalogRequest::CreateTable {
                table,
                columns,
                primary_key,
            } = &prepared.description.request;
            session
                .create_table_engine(
                    table.clone(),
                    columns_for_create(columns),
                    primary_key.clone(),
                )
                .map_err(core)?;
            let (catalog_epoch, tables) = session.catalog_engine().map_err(core)?;
            let table = tables
                .into_iter()
                .find(|created| created.name == *table)
                .ok_or_else(|| core(CoreError::Sql(SqlError::UnknownTable)))?;
            Ok(CatalogOutcome {
                catalog_epoch,
                table,
            })
        })();
        let outcome = match staged {
            Ok(outcome) => outcome,
            Err(error) => {
                session.rollback();
                return Err(error);
            }
        };
        #[cfg(test)]
        tests::before_publish();
        match session.commit_engine() {
            Ok(()) => Ok(outcome),
            Err(error) => {
                session.rollback();
                Err(match error {
                    CoreError::Storage(_) => Error::Unknown,
                    other => core(other),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests;
