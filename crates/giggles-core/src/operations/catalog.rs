//! Finite catalog intents over the same typed storage engine as SQL.
use super::*;
mod unique;
pub use unique::*;
pub use crate::relational::ViewRead;

/// Named constraints share SQL's immediate validation and referential actions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ConstraintSpec {
    Unique { columns: Vec<String> },
    /// Currently bounded to a column compared with an int, text, bool or NULL literal.
    Check { expression: Expr },
    ForeignKey { columns: Vec<String>, ref_table: String, ref_columns: Option<Vec<String>>, on_delete: ForeignKeyAction, on_update: ForeignKeyAction },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ForeignKeyAction { NoAction, Restrict, Cascade, SetNull, SetDefault }

/// An unallocated column definition. Stable IDs are assigned only by execution.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnSpec {
    pub name: String,
    pub data_type: DataType,
    pub not_null: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum AlterTableAction {
    AddColumn { #[serde(deserialize_with = "strict_alter_column")] column: ColumnSpec },
    DropColumn { name: String, cascade: bool },
    RenameColumn { from: String, to: String },
    RenameTable { to: String },
}
fn strict_alter_column<'de, D: serde::Deserializer<'de>>(d: D) -> std::result::Result<ColumnSpec, D::Error> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Column { name: String, data_type: DataType, not_null: bool }
    let Column { name, data_type, not_null } = Column::deserialize(d)?;
    Ok(ColumnSpec { name, data_type, not_null })
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum CatalogRequest {
    CreateTable {
        table: String,
        columns: Vec<ColumnSpec>,
        primary_key: Vec<String>,
    },
    /// Exact flat names, nonunique ascending plain-column keys only.
    CreateIndex {
        table: String,
        name: String,
        columns: Vec<String>,
    },
    DropTable { table: String, cascade: bool },
    DropIndex { name: String, cascade: bool },
    AlterTable { table: String, action: AlterTableAction },
    AddConstraint { table: String, name: Option<String>, constraint: ConstraintSpec },
    DropConstraint { table: String, name: String, cascade: bool },
    CreateView { name: String, read: ViewRead },
    DropView { name: String, cascade: bool },
}
// Preserve the legacy CreateTable decoder while requiring exact new catalog
// requests. Unknown options must never disappear during raw request decoding.
impl<'de> Deserialize<'de> for CatalogRequest {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Index {
            table: String,
            name: String,
            columns: Vec<String>,
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct DropTable { table: String, cascade: bool }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct DropIndex { name: String, cascade: bool }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct AlterTable { table: String, action: AlterTableAction }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct AddConstraint { table: String, name: Option<String>, constraint: ConstraintSpec }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct DropConstraint { table: String, name: String, cascade: bool }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct CreateView { name: String, read: ViewRead }
        #[derive(Deserialize)]
        enum Wire {
            CreateTable {
                table: String,
                columns: Vec<ColumnSpec>,
                primary_key: Vec<String>,
            },
            CreateIndex(Index),
            DropTable(DropTable),
            DropIndex(DropIndex),
            AlterTable(AlterTable),
            AddConstraint(AddConstraint),
            DropConstraint(DropConstraint),
            CreateView(CreateView),
            DropView(DropIndex),
        }
        Ok(match Wire::deserialize(d)? {
            Wire::AlterTable(AlterTable { table, action }) => Self::AlterTable { table, action },
            Wire::AddConstraint(AddConstraint { table, name, constraint }) => Self::AddConstraint { table, name, constraint },
            Wire::DropConstraint(DropConstraint { table, name, cascade }) => Self::DropConstraint { table, name, cascade },
            Wire::CreateView(CreateView { name, read }) => Self::CreateView { name, read },
            Wire::DropView(DropIndex { name, cascade }) => Self::DropView { name, cascade },
            Wire::DropTable(DropTable { table, cascade }) => Self::DropTable { table, cascade },
            Wire::DropIndex(DropIndex { name, cascade }) => Self::DropIndex { name, cascade },
            Wire::CreateTable {
                table,
                columns,
                primary_key,
            } => Self::CreateTable {
                table,
                columns,
                primary_key,
            },
            Wire::CreateIndex(Index {
                table,
                name,
                columns,
            }) => Self::CreateIndex {
                table,
                name,
                columns,
            },
        })
    }
}
/// DROP TABLE returns the table's preimage and no index ID. DROP INDEX returns
/// its owning table's postimage and the dropped index ID; creates and ALTER return postimages.
/// Views return their column descriptor (no primary key), postimage on create, preimage on drop.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogOutcome {
    pub catalog_epoch: u64,
    pub table: TableDef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_id: Option<u64>,
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
fn constraint_relation_keys(request: &CatalogRequest) -> CatalogRequest {
    let mut request = request.clone();
    match &mut request {
        CatalogRequest::AddConstraint { table, constraint, .. } => {
            *table = native_relation_key(table);
            if let ConstraintSpec::ForeignKey { ref_table, .. } = constraint {
                *ref_table = native_relation_key(ref_table);
            }
        }
        CatalogRequest::DropConstraint { table, .. } => *table = native_relation_key(table),
        _ => (),
    }
    request
}

fn validate_catalog_request(request: &CatalogRequest) -> Result<()> {
    if let CatalogRequest::AlterTable { table, action } = request {
        let names = match action {
            AlterTableAction::AddColumn { column } => vec![&column.name],
            AlterTableAction::DropColumn { name, .. } => vec![name],
            AlterTableAction::RenameColumn { from, to } => vec![from, to],
            AlterTableAction::RenameTable { to } => vec![to],
        };
        if !relation_identifier(table) || names.into_iter().any(|n| !identifier(n)) {
            return Err(Error::Bind("identifier".into()));
        }
        return validate_size(request);
    }
    if let CatalogRequest::AddConstraint { table, .. } | CatalogRequest::DropConstraint { table, .. } = request {
        let name = match request { CatalogRequest::AddConstraint { name, .. } => name.as_deref(), CatalogRequest::DropConstraint { name, .. } => Some(name.as_str()), _ => unreachable!() };
        if !relation_identifier(table) || name.is_some_and(|n| !identifier(n)) { return Err(Error::Bind("identifier".into())); }
        if let CatalogRequest::AddConstraint { constraint, .. } = request {
            let check_columns = |columns: &[String]| -> Result<()> {
                if columns.is_empty() || columns.len() > crate::relational::MAX_COLUMNS { return Err(Error::Limit); }
                if columns.iter().any(|n| !identifier(n)) { return Err(Error::Bind("identifier".into())); }
                Ok(())
            };
            match constraint {
                ConstraintSpec::Unique { columns } => check_columns(columns)?,
                ConstraintSpec::ForeignKey { columns, ref_table, ref_columns, .. } => {
                    check_columns(columns)?;
                    if !relation_identifier(ref_table) { return Err(Error::Bind("identifier".into())); }
                    if let Some(columns) = ref_columns { check_columns(columns)?; }
                }
                ConstraintSpec::Check { expression } => {
                    let Expr::Compare(left, _, right) = expression else { return Err(Error::Unsupported); };
                    let (Expr::Field(column), Expr::Value(ValueExpr::Literal(Value::Int(_) | Value::Bool(_) | Value::Text(_) | Value::Null))) = (&**left, &**right) else { return Err(Error::Unsupported); };
                    if !identifier(column) { return Err(Error::Bind("identifier".into())); }
                }
            }
        }
        validate_size(request)?;
        crate::relational::Prepared::constraint_engine(&constraint_relation_keys(request), 0, 0).map_err(|e| core(CoreError::Sql(e)))?;
        return Ok(());
    }
    if let CatalogRequest::CreateView { name, read } = request {
        if !relation_identifier(name) { return Err(Error::Bind("identifier".into())); }
        validate_size(request)?;
        crate::relational::Prepared::from_view_read(0, 0, native_relation_key(name), Some(read.clone()), false)
            .map_err(|e| core(CoreError::Sql(e)))?;
        return Ok(());
    }
    if let CatalogRequest::DropView { name, .. } = request {
        if !relation_identifier(name) { return Err(Error::Bind("identifier".into())); }
        return validate_size(request);
    }
    if let CatalogRequest::DropTable { table: name, .. } | CatalogRequest::DropIndex { name, .. } = request {
        if !relation_identifier(name) { return Err(Error::Bind("identifier".into())); }
        return validate_size(request);
    }
    if let CatalogRequest::CreateIndex {
        table,
        name,
        columns,
    } = request
    {
        if columns.is_empty() || columns.len() > crate::relational::MAX_COLUMNS {
            return Err(Error::Limit);
        }
        if !relation_identifier(table) || !identifier(name) || columns.iter().any(|c| !identifier(c)) {
            return Err(Error::Bind("identifier".into()));
        }
        if columns.iter().collect::<BTreeSet<_>>().len() != columns.len() {
            return Err(Error::Bind("duplicate column".into()));
        }
        return validate_size(request);
    }
    let CatalogRequest::CreateTable {
        table,
        columns,
        primary_key,
    } = request
    else {
        unreachable!()
    };
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
    if !relation_identifier(table)
        || columns.iter().any(|column| !identifier(&column.name))
        || primary_key.iter().any(|name| !identifier(name))
    {
        return Err(Error::Bind("identifier".into()));
    }
    validate_size(request)?;
    let definition = columns_for_create(columns);
    crate::relational::validate_create_definition(table.rsplit('.').next().unwrap(), &definition, primary_key)
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
    if matches!(request, CatalogRequest::DropTable { .. } | CatalogRequest::DropIndex { .. } | CatalogRequest::AlterTable { .. } | CatalogRequest::AddConstraint { .. } | CatalogRequest::DropConstraint { .. } | CatalogRequest::CreateView { .. } | CatalogRequest::DropView { .. }) {
        return Ok(BoundCatalogOperation {
            description: BoundCatalogDescription {
                contract_version: ENGINE_CONTRACT_VERSION,
                database_domain,
                catalog_epoch,
                request: request.clone(),
            },
        });
    }
    if let CatalogRequest::CreateIndex { table, columns, .. } = request {
        session.require_ddl_schema_engine(&native_relation_key(table)).map_err(core)?;
        let table = session.native_table_engine(&native_relation_key(table)).map_err(core)?;
        if table.hidden_rowid().is_some() {
            return Err(Error::Unsupported);
        }
        for name in columns.iter().map(String::as_str).chain(
            table
                .primary_key
                .iter()
                .map(|p| table.columns[*p].name.as_str()),
        ) {
            let column = table
                .columns
                .iter()
                .find(|c| c.name == name)
                .ok_or_else(|| core(CoreError::Sql(SqlError::UnknownColumn)))?;
            if !matches!(
                column.data_type,
                DataType::Boolean
                    | DataType::SmallInt
                    | DataType::Integer
                    | DataType::BigInt
                    | DataType::Text
                    | DataType::Varchar
                    | DataType::Bytes
            ) {
                return Err(Error::Unsupported);
            }
        }
        return Ok(BoundCatalogOperation {
            description: BoundCatalogDescription {
                contract_version: ENGINE_CONTRACT_VERSION,
                database_domain,
                catalog_epoch,
                request: request.clone(),
            },
        });
    }
    let CatalogRequest::CreateTable { table, .. } = request else {
        unreachable!()
    };
    let key = session.native_creation_key_engine(&native_relation_key(table)).map_err(core)?;
    match session.native_table_engine(&key) {
        Ok(_) => return Err(Error::Sql { sqlstate: "42P07".into() }),
        Err(CoreError::Sql(SqlError::UnknownTable)) => {},
        Err(error) => return Err(core(error)),
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
    /// Execute a catalog intent in one fresh snapshot and one commit decision.
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
        self.execute_catalog_plan_with_index_creator(bind, SqlSession::create_index_engine)
    }
    fn execute_catalog_plan_with_index_creator(
        &self,
        bind: impl FnOnce(&mut SqlSession) -> Result<BoundCatalogOperation>,
        create_index: impl FnOnce(
            &mut SqlSession,
            &TableDef,
            &str,
            &[u64],
        ) -> std::result::Result<u64, CoreError>,
    ) -> Result<CatalogOutcome> {
        let mut session = self.admin.sql_session();
        session.begin_engine().map_err(core)?;
        let staged = (|| {
            let prepared = bind(&mut session)?;
            if matches!(prepared.description.request, CatalogRequest::CreateView { .. } | CatalogRequest::DropView { .. }) && session.is_v2_engine() {
                session.catalog_empty_engine().map_err(core)?;
            }
            if let CatalogRequest::CreateView { name, read } = &prepared.description.request {
                let table = session.execute_typed_view(native_relation_key(name), Some(read.clone()), false).map_err(core)?;
                let (catalog_epoch, _) = session.catalog_engine().map_err(core)?;
                return Ok(CatalogOutcome { catalog_epoch, table, index_id: None });
            }
            if let CatalogRequest::DropView { name, cascade } = &prepared.description.request {
                let table = session.execute_typed_view(native_relation_key(name), None, *cascade).map_err(core)?;
                let (catalog_epoch, _) = session.catalog_engine().map_err(core)?;
                return Ok(CatalogOutcome { catalog_epoch, table, index_id: None });
            }
            if !session.catalog_empty_engine().map_err(core)? {
                return Err(Error::PlanRequired);
            }
            if matches!(&prepared.description.request, CatalogRequest::AddConstraint { .. } | CatalogRequest::DropConstraint { .. }) {
                let table = session.constraint_catalog_engine(&constraint_relation_keys(&prepared.description.request)).map_err(core)?;
                let (catalog_epoch, _) = session.catalog_engine().map_err(core)?;
                return Ok(CatalogOutcome { catalog_epoch, table, index_id: None });
            }
            if let CatalogRequest::DropTable { table: name, cascade } | CatalogRequest::DropIndex { name, cascade } = &prepared.description.request {
                let is_index = matches!(prepared.description.request, CatalogRequest::DropIndex { .. });
                let (table, index_id) = session.drop_catalog_engine(native_relation_key(name), *cascade, is_index).map_err(core)?;
                let (catalog_epoch, _) = session.catalog_engine().map_err(core)?;
                return Ok(CatalogOutcome { catalog_epoch, table, index_id });
            }
            if let CatalogRequest::AlterTable { table, action } = &prepared.description.request {
                let table = session.alter_table_engine(native_relation_key(table), action.clone()).map_err(core)?;
                let (catalog_epoch, _) = session.catalog_engine().map_err(core)?;
                return Ok(CatalogOutcome { catalog_epoch, table, index_id: None });
            }
            let index_id;
            let table_name;
            if let CatalogRequest::CreateIndex {
                table,
                name,
                columns,
            } = &prepared.description.request
            {
                let definition = session.native_table_engine(&native_relation_key(table)).map_err(core)?;
                let ids = columns
                    .iter()
                    .map(|name| {
                        definition
                            .columns
                            .iter()
                            .find(|c| c.name == *name)
                            .map(|c| c.id)
                            .ok_or_else(|| core(CoreError::Sql(SqlError::UnknownColumn)))
                    })
                    .collect::<Result<Vec<_>>>()?;
                index_id = Some(create_index(&mut session, &definition, name, &ids).map_err(core)?);
                table_name = native_relation_key(table);
            } else {
                let CatalogRequest::CreateTable {
                    table,
                    columns,
                    primary_key,
                } = &prepared.description.request
                else {
                    unreachable!()
                };
                session
                    .create_table_engine(
                        session.native_creation_key_engine(&native_relation_key(table)).map_err(core)?,
                        columns_for_create(columns),
                        primary_key.clone(),
                    )
                    .map_err(core)?;
                table_name = session.native_creation_key_engine(&native_relation_key(table)).map_err(core)?;
                index_id = None;
            }
            let (catalog_epoch, _) = session.catalog_engine().map_err(core)?;
            let table = session.native_table_engine(&table_name).map_err(core)?;
            Ok(CatalogOutcome {
                catalog_epoch,
                table,
                index_id,
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
