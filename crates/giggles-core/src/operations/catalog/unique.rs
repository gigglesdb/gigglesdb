//! Explicit UNIQUE intents for the trusted embedded Rust surface.
use super::*;

/// Exact flat names and ordered plain columns. NULL components remain distinct.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UniqueIndexRequest {
    pub table: String,
    pub name: String,
    pub columns: Vec<String>,
}
impl UniqueIndexRequest {
    fn catalog_request(&self) -> Result<CatalogRequest> {
        // Bound caller-owned collections before cloning into the shared validator.
        if self.columns.is_empty() || self.columns.len() > crate::relational::MAX_COLUMNS {
            return Err(Error::Limit);
        }
        if !identifier(&self.table)
            || !identifier(&self.name)
            || self.columns.iter().any(|column| !identifier(column))
        {
            return Err(Error::Bind("identifier".into()));
        }
        validate_size(self)?;
        let request = CatalogRequest::CreateIndex {
            table: self.table.clone(),
            name: self.name.clone(),
            columns: self.columns.clone(),
        };
        validate_catalog_request(&request)?;
        Ok(request)
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundUniqueIndexDescription {
    pub contract_version: u32,
    pub database_domain: u128,
    pub catalog_epoch: u64,
    pub request: UniqueIndexRequest,
}
/// Immutable process-local intent; neither authority nor a migration receipt.
#[derive(Clone, Debug)]
pub struct BoundUniqueIndexOperation {
    description: BoundUniqueIndexDescription,
    catalog: BoundCatalogOperation,
}
impl BoundUniqueIndexOperation {
    pub fn description(&self) -> &BoundUniqueIndexDescription {
        &self.description
    }
}
impl Engine {
    /// Create a local UNIQUE index on an eligible empty database.
    /// This API does not provide distributed or global uniqueness.
    pub fn execute_unique_index(&self, request: &UniqueIndexRequest) -> Result<CatalogOutcome> {
        let catalog = request.catalog_request()?;
        self.execute_catalog_plan_with_index_creator(
            |session| bind_catalog(session, &catalog),
            |session, table, name, columns| {
                session.create_index_internal_engine(table, name, columns, true)
            },
        )
    }
    /// Validate and freeze intent without allocating IDs or testing population.
    pub fn prepare_unique_index_bound(
        &self,
        request: &UniqueIndexRequest,
    ) -> Result<BoundUniqueIndexOperation> {
        let catalog = self.prepare_catalog_bound(&request.catalog_request()?)?;
        let description = BoundUniqueIndexDescription {
            contract_version: catalog.description.contract_version,
            database_domain: catalog.description.database_domain,
            catalog_epoch: catalog.description.catalog_epoch,
            request: request.clone(),
        };
        Ok(BoundUniqueIndexOperation {
            description,
            catalog,
        })
    }
    /// Recheck domain/epoch and the exact execution snapshot's empty-state gate.
    pub fn execute_unique_index_bound(
        &self,
        prepared: &BoundUniqueIndexOperation,
    ) -> Result<CatalogOutcome> {
        self.execute_catalog_plan_with_index_creator(
            |session| {
                let description = &prepared.catalog.description;
                let (domain, epoch) = session.snapshot_identity_engine().map_err(core)?;
                if description.contract_version != ENGINE_CONTRACT_VERSION
                    || description.database_domain != domain
                    || description.catalog_epoch != epoch
                {
                    return Err(core(CoreError::Sql(SqlError::StaleSchema)));
                }
                bind_catalog(session, &description.request)
            },
            |session, table, name, columns| {
                session.create_index_internal_engine(table, name, columns, true)
            },
        )
    }
}

#[cfg(test)]
mod tests;
