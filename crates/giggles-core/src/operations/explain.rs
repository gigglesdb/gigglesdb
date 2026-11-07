//! Finite trusted read explanation; never executes a logical business-row read.
use super::*;

pub const MAX_EXPLAIN_METADATA_BYTES: usize = 65_536;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadExplainStage {
    WholeStateSnapshot,
    BoundedCompleteTableMaterialization,
    /// Executed only when the materialized table page reaches MAX_ROWS.
    ReversePrimaryKeyCompletenessCertification,
    InMemoryFilter,
    InMemoryOrder,
    FieldProjection,
    OutputPage,
    /// Additional access after the complete input admission above; no avoided
    /// full materialization or estimated cost is claimed.
    SecondaryIndexCandidates {
        index_id: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadExplainOrder {
    pub column_id: u64,
    pub descending: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadExplainLimits {
    pub input_rows: usize,
    pub output_rows: usize,
    pub row_bytes: usize,
    pub result_bytes: usize,
    pub request_bytes: usize,
    pub binding_count: usize,
    pub binding_bytes: usize,
    pub bound_value_bytes: usize,
    pub expression_depth: usize,
    pub expression_nodes: usize,
    pub admitted: usize,
    pub selected_keys: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadExplainOutcome {
    pub contract_version: u32,
    #[serde(with = "domain_text")]
    pub database_domain: u128,
    pub catalog_epoch: u64,
    pub table_id: u64,
    pub table_name: String,
    pub projection_column_ids: Vec<u64>,
    pub order: Vec<ReadExplainOrder>,
    pub has_filter: bool,
    pub stages: Vec<ReadExplainStage>,
    pub limits: ReadExplainLimits,
    pub estimated_rows: Option<u64>,
    pub estimated_cost: Option<u64>,
}

mod domain_text {
    use serde::{Deserialize, Deserializer, Serializer, de::Error};
    pub fn serialize<S: Serializer>(value: &u128, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.collect_str(value)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> std::result::Result<u128, D::Error> {
        let text = String::deserialize(d)?;
        if text.is_empty()
            || text.len() > 39
            || (text.len() > 1 && text.starts_with('0'))
            || !text.bytes().all(|b| b.is_ascii_digit())
        {
            return Err(D::Error::custom("noncanonical explain domain"));
        }
        text.parse().map_err(D::Error::custom)
    }
}

// Logical output charge: fixed header/limits/stages 256, name length prefix 8,
// projection length prefix 8 and 8 per ID, order prefix 8 and 9 per item.
// Only borrowed metadata is inspected before this admission completes.
fn admit(table: &TableDef, projection_len: usize, order_len: usize, budget: usize) -> Result<()> {
    let bytes = 280usize
        .checked_add(table.name.len())
        .and_then(|n| n.checked_add(projection_len.checked_mul(8)?))
        .and_then(|n| n.checked_add(order_len.checked_mul(9)?))
        .ok_or(Error::Limit)?;
    if bytes > budget {
        return Err(Error::Limit);
    }
    Ok(())
}

impl Engine {
    /// Explain only an existing simple field-projection read using actual values
    /// and one current committed catalog snapshot. Admin authority is trusted,
    /// not task authorization; the outcome is descriptive, never a credential.
    /// Storage snapshot acquisition still decodes/validates whole bounded state.
    pub fn explain_read(
        &self,
        request: &Request,
        bindings: &Bindings,
    ) -> Result<ReadExplainOutcome> {
        let Request::Read {
            table,
            filter,
            order,
            projection,
            ..
        } = request
        else {
            return Err(Error::Unsupported);
        };
        validate_request(request)?;
        validate_bindings(bindings)?;
        let configured = self.admin.explain_limits_engine();
        self.admin
            .inspect_secondary_catalog_engine(|domain, epoch, tables, descriptors| {
                let table = tables
                    .get(table)
                    .ok_or_else(|| Error::Bind("table".into()))?;
                if table.hidden_rowid().is_some() {
                    return Err(Error::Unsupported);
                }
                admit(
                    table,
                    if projection.is_empty() {
                        table.columns.len()
                    } else {
                        projection.len()
                    },
                    order.len(),
                    MAX_EXPLAIN_METADATA_BYTES,
                )?;
                let mut binder = Binder {
                    table: Some(table),
                    allow_fields: true,
                    allow_conflict_fields: false,
                    locals: None,
                    bindings,
                    used: BTreeSet::new(),
                    remaining_value_bytes: MAX_BOUND_VALUE_BYTES,
                };
                let Operation::Read {
                    filter: bound_filter,
                    order,
                    limit,
                    projection,
                    ..
                } = binder.bind(request)?
                else {
                    return Err(Error::Unsupported);
                };
                if binder.used.len() != bindings.len() {
                    return Err(Error::Bind("parameters".into()));
                }
                crate::cancellation::check().map_err(core)?;
                let mut stages = vec![
                    ReadExplainStage::WholeStateSnapshot,
                    ReadExplainStage::BoundedCompleteTableMaterialization,
                    ReadExplainStage::ReversePrimaryKeyCompletenessCertification,
                ];
                if let Some(id) = secondary_selector::choose(table, &bound_filter)
                    .and_then(|choice| choice.descriptor(table, descriptors))
                {
                    stages.push(ReadExplainStage::SecondaryIndexCandidates { index_id: id });
                }
                stages.extend([
                    ReadExplainStage::InMemoryFilter,
                    ReadExplainStage::InMemoryOrder,
                    ReadExplainStage::FieldProjection,
                    ReadExplainStage::OutputPage,
                ]);
                Ok(ReadExplainOutcome {
                    contract_version: ENGINE_CONTRACT_VERSION,
                    database_domain: domain,
                    catalog_epoch: epoch,
                    table_id: table.id,
                    table_name: table.name.clone(),
                    projection_column_ids: projection
                        .iter()
                        .map(|i| table.columns[*i].id)
                        .collect(),
                    order: order
                        .iter()
                        .map(|(i, descending)| ReadExplainOrder {
                            column_id: table.columns[*i].id,
                            descending: *descending,
                        })
                        .collect(),
                    has_filter: filter.is_some(),
                    stages,
                    limits: ReadExplainLimits {
                        input_rows: MAX_ROWS,
                        output_rows: limit,
                        row_bytes: crate::relational::MAX_ROW_BYTES,
                        result_bytes: crate::relational::MAX_RESULT_BYTES,
                        request_bytes: MAX_INPUT,
                        binding_count: MAX_BINDINGS,
                        binding_bytes: MAX_BINDING_BYTES,
                        bound_value_bytes: MAX_BOUND_VALUE_BYTES,
                        expression_depth: MAX_DEPTH,
                        expression_nodes: MAX_EXPRESSION_NODES,
                        admitted: configured.admitted,
                        selected_keys: configured.selected_keys,
                    },
                    estimated_rows: None,
                    estimated_cost: None,
                })
            })
            .map_err(core)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn metadata_exact_bound_and_one_over() {
        let table = TableDef {
            id: 1,
            name: "items".into(),
            columns: vec![],
            primary_key: vec![],
        };
        assert_eq!(admit(&table, 2, 1, 310), Ok(()));
        assert_eq!(admit(&table, 2, 1, 309), Err(Error::Limit));
        assert_eq!(admit(&table, usize::MAX, 1, usize::MAX), Err(Error::Limit));
    }

    #[test]
    fn no_sql_parser_and_no_business_row_evaluation() {
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("artifacts");
        std::fs::create_dir_all(&root).unwrap();
        let mut token = [0u8; 16];
        getrandom::fill(&mut token).unwrap();
        let admin = Admin::create(
            root.join(format!("explain-{:032x}", u128::from_le_bytes(token))),
            crate::Limits::default(),
        )
        .unwrap();
        admin
            .sql_session()
            .execute("create table items (id bigint primary key)", &[], None)
            .unwrap();
        let engine = Engine::new(admin);
        let request = Request::Read {
            table: "items".into(),
            order: vec![],
            limit: None,
            projection: vec![],
            // Binding succeeds, but evaluating this constant would divide by zero.
            filter: Some(Expr::Compare(
                Box::new(Expr::Arithmetic(
                    Box::new(Expr::Value(ValueExpr::Literal(Value::Int(1)))),
                    Arithmetic::Divide,
                    Box::new(Expr::Value(ValueExpr::Literal(Value::Int(0)))),
                )),
                Compare::Eq,
                Box::new(Expr::Value(ValueExpr::Literal(Value::Int(1)))),
            )),
        };
        crate::relational::with_sql_parser_forbidden(|| {
            assert!(
                engine
                    .explain_read(&request, &Bindings::new())
                    .unwrap()
                    .has_filter
            );
        });
    }
}
