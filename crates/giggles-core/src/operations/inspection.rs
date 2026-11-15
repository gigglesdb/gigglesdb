//! Bounded trusted catalog inspection, separate from data and catalog mutations.
use super::{ENGINE_CONTRACT_VERSION, Engine, Error, Result, core, identifier, validate_size};
use crate::{CoreError, relational::{MAX_COLUMNS, MAX_TABLES, SqlError, TableDef}};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Logical metadata admission limit, not a wire codec or a total heap bound.
/// Charge the outcome header as 30 bytes; strings as 8 plus UTF-8 length;
/// collection lengths, IDs, counts and PK indexes as 8; and each data-type
/// discriminator and nullability flag as 1. See OPERATIONS.md for the formula.
pub const MAX_INSPECTION_METADATA_BYTES: usize = 65_536;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum InspectionRequest {
    Tables,
    Table { table: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSummary {
    pub id: u64,
    pub name: String,
    pub column_count: usize,
    pub primary_key_columns: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum InspectionResult {
    Tables(Vec<TableSummary>),
    Table(TableDef),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectionOutcome {
    pub contract_version: u32,
    /// Canonical decimal text in serialized inspection results; u128 in Rust.
    #[serde(with = "domain_text")]
    pub database_domain: u128,
    pub catalog_epoch: u64,
    pub complete: bool,
    pub result: InspectionResult,
}

mod domain_text {
    use serde::{
        Deserializer, Serializer,
        de::{Error, Visitor},
    };

    pub fn serialize<S: Serializer>(
        domain: &u128,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_str(domain)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<u128, D::Error> {
        struct Domain;
        impl Visitor<'_> for Domain {
            type Value = u128;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a canonical decimal u128 string")
            }
            fn visit_str<E: Error>(self, text: &str) -> std::result::Result<u128, E> {
                if text.is_empty()
                    || text.len() > 39
                    || (text.len() > 1 && text.starts_with('0'))
                    || !text.bytes().all(|byte| byte.is_ascii_digit())
                {
                    return Err(E::custom("noncanonical inspection domain"));
                }
                text.parse()
                    .map_err(|_| E::custom("inspection domain overflow"))
            }
        }
        deserializer.deserialize_str(Domain)
    }
}

struct MetadataBudget(usize);
impl MetadataBudget {
    fn charge(&mut self, bytes: usize) -> Result<()> {
        self.0 = self.0.checked_sub(bytes).ok_or(Error::Limit)?;
        Ok(())
    }
    fn name(&mut self, name: &str) -> Result<()> {
        if name.len() > 63 {
            return Err(Error::Limit);
        }
        self.charge(8)?;
        self.charge(name.len())
    }
}

fn check_table_shape(table: &TableDef) -> Result<()> {
    if table.logical_columns().count() > MAX_COLUMNS || table.primary_key.len() > MAX_COLUMNS {
        return Err(Error::Limit);
    }
    Ok(())
}

/// Only borrowed descriptors are visited during admission. All output copying
/// happens after the entire selected result has passed this function.
fn admit(
    request: &InspectionRequest,
    tables: &BTreeMap<String, TableDef>,
    budget: &mut MetadataBudget,
) -> Result<()> {
    if tables.len() > MAX_TABLES {
        return Err(Error::Limit);
    }
    // contract version + domain + epoch + complete + result discriminator.
    budget.charge(4 + 16 + 8 + 1 + 1)?;
    match request {
        InspectionRequest::Tables => {
            budget.charge(8)?;
            for table in tables.values() {
                crate::cancellation::check().map_err(core)?;
                check_table_shape(table)?;
                budget.charge(8 + 8 + 8)?;
                budget.name(&table.name)?;
            }
        }
        InspectionRequest::Table { table } => {
            let table = tables
                .get(table)
                .ok_or_else(|| core(CoreError::Sql(SqlError::UnknownTable)))?;
            check_table_shape(table)?;
            // Table ID and the lengths of its columns and primary-key vectors.
            budget.charge(8 + 8 + 8)?;
            budget.name(&table.name)?;
            for column in table.logical_columns() {
                crate::cancellation::check().map_err(core)?;
                budget.charge(8 + 1 + 1)?;
                budget.name(&column.name)?;
            }
            if table.hidden_rowid().is_none() {
                for _ in &table.primary_key {
                    budget.charge(8)?;
                }
            }
        }
    }
    Ok(())
}

impl Engine {
    /// Inspect fresh committed catalog metadata under trusted Admin authority.
    /// This never opens or commits a SQL transaction, exposes business rows or
    /// row counts, or allocates durable catalog identities. The returned domain
    /// and epoch identify the same snapshot as every returned descriptor.
    pub fn inspect(&self, request: &InspectionRequest) -> Result<InspectionOutcome> {
        if let InspectionRequest::Table { table } = request {
            if table.len() > 63 {
                return Err(Error::Limit);
            }
            if !identifier(table) {
                return Err(Error::Bind("identifier".into()));
            }
        }
        validate_size(request)?;
        self.admin
            .inspect_catalog_engine(|database_domain, catalog_epoch, tables| {
                admit(
                    request,
                    tables,
                    &mut MetadataBudget(MAX_INSPECTION_METADATA_BYTES),
                )?;
                crate::cancellation::check().map_err(core)?;
                let result = match request {
                    InspectionRequest::Tables => InspectionResult::Tables(
                        tables
                            .values()
                            .map(|table| TableSummary {
                                id: table.id,
                                name: table.name.clone(),
                                column_count: table.logical_columns().count(),
                                primary_key_columns: if table.hidden_rowid().is_some() {
                                    0
                                } else {
                                    table.primary_key.len()
                                },
                            })
                            .collect(),
                    ),
                    InspectionRequest::Table { table } => {
                        let table = tables
                            .get(table)
                            .ok_or_else(|| core(CoreError::Sql(SqlError::UnknownTable)))?;
                        InspectionResult::Table(TableDef {
                            id: table.id,
                            name: table.name.clone(),
                            columns: table.logical_columns().cloned().collect(),
                            primary_key: if table.hidden_rowid().is_some() {
                                vec![]
                            } else {
                                table.primary_key.clone()
                            },
                        })
                    }
                };
                Ok(InspectionOutcome {
                    contract_version: ENGINE_CONTRACT_VERSION,
                    database_domain,
                    catalog_epoch,
                    complete: true,
                    result,
                })
            })
            .map_err(core)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relational::{ColumnDef, DataType};

    fn table() -> TableDef {
        TableDef {
            id: 1,
            name: "items".into(),
            columns: vec![ColumnDef {
                id: 2,
                name: "id".into(),
                data_type: DataType::BigInt,
                not_null: true,
            }],
            primary_key: vec![0],
        }
    }

    #[test]
    fn metadata_metric_charges_headers_names_and_descriptors_exactly() {
        let tables = BTreeMap::from([("items".into(), table())]);
        // Header30 + vector8 + summary(id/count/count24 + name(8+5)).
        let mut exact = MetadataBudget(75);
        admit(&InspectionRequest::Tables, &tables, &mut exact).unwrap();
        assert_eq!(exact.0, 0);
        assert_eq!(
            admit(&InspectionRequest::Tables, &tables, &mut MetadataBudget(74)),
            Err(Error::Limit)
        );
        // Header30 + table(id8/name13/column-count8/PK-count8) +
        // column(id8/name10/type1/not-null1) + PK-index8.
        let request = InspectionRequest::Table {
            table: "items".into(),
        };
        let mut exact = MetadataBudget(95);
        admit(&request, &tables, &mut exact).unwrap();
        assert_eq!(exact.0, 0);
        assert_eq!(
            admit(&request, &tables, &mut MetadataBudget(94)),
            Err(Error::Limit)
        );
        let mut exact = MetadataBudget(MAX_INSPECTION_METADATA_BYTES);
        exact.charge(MAX_INSPECTION_METADATA_BYTES).unwrap();
        assert_eq!(exact.charge(1), Err(Error::Limit));
        assert_eq!(
            MetadataBudget(MAX_INSPECTION_METADATA_BYTES).charge(usize::MAX),
            Err(Error::Limit)
        );
    }

    #[test]
    fn metadata_shape_admission_rejects_before_any_descriptor_copy() {
        let request = InspectionRequest::Table {
            table: "items".into(),
        };
        let mut table = table();
        table
            .columns
            .resize(MAX_COLUMNS + 1, table.columns[0].clone());
        let mut tables = BTreeMap::from([("items".into(), table)]);
        assert_eq!(
            admit(
                &request,
                &tables,
                &mut MetadataBudget(MAX_INSPECTION_METADATA_BYTES)
            ),
            Err(Error::Limit)
        );
        tables.get_mut("items").unwrap().columns.truncate(1);
        tables
            .get_mut("items")
            .unwrap()
            .primary_key
            .resize(MAX_COLUMNS + 1, 0);
        assert_eq!(
            admit(
                &request,
                &tables,
                &mut MetadataBudget(MAX_INSPECTION_METADATA_BYTES)
            ),
            Err(Error::Limit)
        );
        tables.get_mut("items").unwrap().primary_key.truncate(1);
        tables.get_mut("items").unwrap().columns[0].name = "x".repeat(64);
        assert_eq!(
            admit(
                &request,
                &tables,
                &mut MetadataBudget(MAX_INSPECTION_METADATA_BYTES)
            ),
            Err(Error::Limit)
        );
        let tables: BTreeMap<_, _> = (0..=MAX_TABLES)
            .map(|i| (format!("t{i}"), self::table()))
            .collect();
        assert_eq!(
            admit(
                &InspectionRequest::Tables,
                &tables,
                &mut MetadataBudget(MAX_INSPECTION_METADATA_BYTES)
            ),
            Err(Error::Limit)
        );
    }

    #[test]
    fn inspection_domain_is_lossless_canonical_decimal_text() {
        let outcome = InspectionOutcome {
            contract_version: ENGINE_CONTRACT_VERSION,
            database_domain: u128::MAX,
            catalog_epoch: 1,
            complete: true,
            result: InspectionResult::Tables(vec![]),
        };
        let value = serde_json::to_value(&outcome).unwrap();
        assert_eq!(value["database_domain"], u128::MAX.to_string());
        assert_eq!(
            serde_json::from_value::<InspectionOutcome>(value.clone()).unwrap(),
            outcome
        );
        for invalid in [
            "",
            "00",
            "01",
            "+1",
            "-1",
            "1.0",
            "1e2",
            " 1",
            "340282366920938463463374607431768211456",
        ] {
            let mut invalid_value = value.clone();
            invalid_value["database_domain"] = invalid.into();
            assert!(
                serde_json::from_value::<InspectionOutcome>(invalid_value).is_err(),
                "{invalid}"
            );
        }
        let mut numeric = value.clone();
        numeric["database_domain"] = serde_json::json!(1);
        assert!(serde_json::from_value::<InspectionOutcome>(numeric).is_err());
        let mut zero = value;
        zero["database_domain"] = "0".into();
        assert_eq!(
            serde_json::from_value::<InspectionOutcome>(zero)
                .unwrap()
                .database_domain,
            0
        );
    }
}
