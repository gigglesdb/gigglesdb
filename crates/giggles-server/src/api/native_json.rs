//! Strict native JSON profile; all keys are observed before typed conversion.

use super::json::{Decimal, JsonRow, JsonRows, JsonValue};
use serde::{
    Serialize, Serializer,
    ser::{Error as _, SerializeMap},
};
impl Serialize for p::NativeOutcome {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(None)?;
        m.serialize_entry("columns", &self.columns)?;
        m.serialize_entry("rows", &JsonRows(&self.rows))?;
        m.serialize_entry("matched", &Decimal(self.matched))?;
        m.serialize_entry("affected", &Decimal(self.affected))?;
        m.serialize_entry("changed", &Decimal(self.changed))?;
        m.serialize_entry("complete", &self.complete)?;
        if let Some(v) = &self.conflict_results {
            m.serialize_entry("conflictResults", v)?;
        }
        m.end()
    }
}
impl Serialize for p::NativeConflictResults {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(None)?;
        m.serialize_entry("items", &self.items)?;
        m.end()
    }
}
impl Serialize for p::NativeConflictTupleOutcome {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(None)?;
        m.serialize_entry("inputIndex", &Decimal(self.input_index))?;
        let v = p::NativeConflictStatus::try_from(self.status)
            .map_err(|_| S::Error::custom("invalid native output enum"))?;
        if v as i32 == 0 {
            return Err(S::Error::custom("missing native output enum"));
        }
        m.serialize_entry("status", v.as_str_name())?;
        m.serialize_entry("changed", &self.changed)?;
        if let Some(v) = self.returned_row {
            m.serialize_entry("returnedRow", &Decimal(v))?;
        }
        m.end()
    }
}
impl Serialize for p::NativeStepSummary {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(None)?;
        m.serialize_entry("itemIndex", &Decimal(self.item_index))?;
        m.serialize_entry("matched", &Decimal(self.matched))?;
        m.serialize_entry("affected", &Decimal(self.affected))?;
        m.serialize_entry("changed", &Decimal(self.changed))?;
        m.serialize_entry("complete", &self.complete)?;
        if let Some(v) = &self.conflict_results {
            m.serialize_entry("conflictResults", v)?;
        }
        m.end()
    }
}
impl Serialize for p::NativeTransactionOutcome {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(None)?;
        m.serialize_entry("summaries", &self.summaries)?;
        m.serialize_entry("emitted", &self.emitted)?;
        m.end()
    }
}
impl Serialize for p::NativeEmittedRow {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(None)?;
        m.serialize_entry("columns", &self.columns)?;
        m.serialize_entry(
            "row",
            &JsonRow(
                self.row
                    .as_ref()
                    .ok_or_else(|| S::Error::custom("missing row"))?,
            ),
        )?;
        m.end()
    }
}
impl Serialize for p::NativeEmittedRows {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(None)?;
        m.serialize_entry("columns", &self.columns)?;
        m.serialize_entry("rows", &JsonRows(&self.rows))?;
        m.serialize_entry("complete", &self.complete)?;
        m.end()
    }
}
impl Serialize for p::NativeColumn {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(None)?;
        m.serialize_entry("id", &Decimal(self.id))?;
        m.serialize_entry("name", &self.name)?;
        let v = p::NativeDataType::try_from(self.data_type)
            .map_err(|_| S::Error::custom("invalid native output enum"))?;
        if v as i32 == 0 {
            return Err(S::Error::custom("missing native output enum"));
        }
        m.serialize_entry("dataType", v.as_str_name())?;
        m.serialize_entry("notNull", &self.not_null)?;
        m.end()
    }
}
impl Serialize for p::NativeError {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(None)?;
        let v = p::NativeErrorClass::try_from(self.kind)
            .map_err(|_| S::Error::custom("invalid native output enum"))?;
        if v as i32 == 0 {
            return Err(S::Error::custom("missing native output enum"));
        }
        m.serialize_entry("kind", v.as_str_name())?;
        if let Some(v) = self.syntax_offset {
            m.serialize_entry("syntaxOffset", &Decimal(v))?;
        }
        if let Some(v) = &self.sqlstate {
            m.serialize_entry("sqlstate", v)?;
        }
        m.end()
    }
}
impl Serialize for p::NativeEmittedValue {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(Some(1))?;
        match &self.value {
            Some(p::native_emitted_value::Value::Scalar(v)) => {
                m.serialize_entry("scalar", &JsonValue(v))?
            }
            Some(p::native_emitted_value::Value::Row(v)) => m.serialize_entry("row", v)?,
            Some(p::native_emitted_value::Value::Rows(v)) => m.serialize_entry("rows", v)?,
            None => return Err(S::Error::custom("missing emitted value")),
        }
        m.end()
    }
}

use super::json::{Json, Object, array, decode_value, empty, text};
use super::{error::ApiError, proto as p};
type R<T> = Result<T, ApiError>;
fn boolean(v: Json) -> R<bool> {
    if let Json::Bool(v) = v {
        Ok(v)
    } else {
        Err(ApiError::InvalidArgument)
    }
}
fn items(v: Option<Json>, max: usize) -> R<Vec<Json>> {
    let v = v.map(array).transpose()?.unwrap_or_default();
    if v.len() > max {
        Err(ApiError::InvalidArgument)
    } else {
        Ok(v)
    }
}
#[derive(Default)]
pub(super) struct Decoder {
    depth: usize,
    nodes: usize,
    wrappers: usize,
}
impl Decoder {
    pub(super) fn catalog(&mut self, v: Json) -> R<p::NativeCatalogRequest> {
        use p::native_catalog_request::Request as Q;
        let (kind, v) = Object::new(v)?.one()?;
        let mut f = Object::new(v)?;
        let request = Some(match kind.as_str() {
            "createTable" => Q::CreateTable(p::NativeCreateTable {
                table: Some(text(f.required("table")?)?),
                columns: items(f.optional("columns"), 1600)?.into_iter().map(Self::catalog_column).collect::<R<_>>()?,
                primary_key: items(f.optional("primaryKey"), 1600)?.into_iter().map(text).collect::<R<_>>()?,
            }),
            "createIndex" => Q::CreateIndex(p::NativeCreateIndex {
                table: Some(text(f.required("table")?)?), name: Some(text(f.required("name")?)?),
                columns: items(f.optional("columns"), 1600)?.into_iter().map(text).collect::<R<_>>()?,
            }),
            "dropTable" => Q::DropTable(p::NativeDropTable {
                table: Some(text(f.required("table")?)?), cascade: f.optional("cascade").map(boolean).transpose()?,
            }),
            "dropIndex" => Q::DropIndex(p::NativeDropIndex {
                name: Some(text(f.required("name")?)?), cascade: f.optional("cascade").map(boolean).transpose()?,
            }),
            "addConstraint" => {
                use p::native_add_constraint::Constraint as K;
                let table = Some(text(f.required("table")?)?);
                let name = f.optional("name").map(text).transpose()?;
                let (kind, value) = f.one()?;
                let mut a = Object::new(value)?;
                let constraint = Some(match kind.as_str() {
                    "unique" => K::Unique(p::NativeUniqueConstraint { columns: items(a.optional("columns"),1600)?.into_iter().map(text).collect::<R<_>>()? }),
                    "check" => K::Check(p::NativeCheckConstraint { expression: Some(self.native_expr(a.required("expression")?)?) }),
                    "foreignKey" => K::ForeignKey(p::NativeForeignKey {
                        columns: items(a.optional("columns"),1600)?.into_iter().map(text).collect::<R<_>>()?,
                        ref_table: Some(text(a.required("refTable")?)?),
                        ref_columns: items(a.optional("refColumns"),1600)?.into_iter().map(text).collect::<R<_>>()?,
                        ref_columns_set: a.optional("refColumnsSet").map(boolean).transpose()?,
                        on_delete: a.optional("onDelete").map(text).transpose()?, on_update: a.optional("onUpdate").map(text).transpose()?,
                    }),
                    _ => return Err(ApiError::InvalidArgument),
                });
                a.finish()?;
                return Ok(p::NativeCatalogRequest { request: Some(Q::AddConstraint(p::NativeAddConstraint { table, name, constraint })) });
            }
            "dropConstraint" => Q::DropConstraint(p::NativeDropConstraint { table: Some(text(f.required("table")?)?),
                name: Some(text(f.required("name")?)?), cascade: f.optional("cascade").map(boolean).transpose()? }),
            "alterTable" => {
                use p::native_alter_table::Action as A;
                let table = Some(text(f.required("table")?)?);
                let (kind, value) = f.one()?;
                let action = Some(match kind.as_str() {
                    "addColumn" => A::AddColumn(Self::catalog_column(value)?),
                    "dropColumn" => { let mut a = Object::new(value)?;
                        let v = p::NativeDropColumn { name: Some(text(a.required("name")?)?), cascade: a.optional("cascade").map(boolean).transpose()? };
                        a.finish()?; A::DropColumn(v) }
                    "renameColumn" => { let mut a = Object::new(value)?;
                        let v = p::NativeRenameColumn { from: Some(text(a.required("from")?)?), to: Some(text(a.required("to")?)?) };
                        a.finish()?; A::RenameColumn(v) }
                    "renameTable" => { let mut a = Object::new(value)?;
                        let v = p::NativeRenameTable { to: Some(text(a.required("to")?)?) };
                        a.finish()?; A::RenameTable(v) }
                    _ => return Err(ApiError::InvalidArgument),
                });
                return Ok(p::NativeCatalogRequest { request: Some(Q::AlterTable(p::NativeAlterTable { table, action })) });
            }
            _ => return Err(ApiError::InvalidArgument),
        });
        f.finish()?;
        Ok(p::NativeCatalogRequest { request })
    }
    fn catalog_column(v: Json) -> R<p::NativeCatalogColumn> {
        let mut f = Object::new(v)?;
        let column = p::NativeCatalogColumn { name: Some(text(f.required("name")?)?),
            data_type: Some(text(f.required("dataType")?)?), not_null: f.optional("notNull").map(boolean).transpose()?,
            default: f.optional("default").map(Self::column_default).transpose()?, generated: f.optional("generated").map(text).transpose()? };
        f.finish()?;
        Ok(column)
    }
    fn column_default(v: Json) -> R<p::NativeColumnDefault> {
        use p::native_column_default::Kind as K;
        let (kind, v) = Object::new(v)?.one()?;
        let kind = Some(match kind.as_str() {
            "Value" => K::Value(decode_value(v)?), "Nextval" => K::Nextval(text(v)?),
            "Clock" => {
                let mut f = Object::new(v)?;
                let kind = Some(text(f.required("kind")?)?);
                let precision = f.optional("precision").map(|v| match v {
                    Json::Unsigned(n) => u32::try_from(n).map_err(|_| ApiError::InvalidArgument),
                    _ => Err(ApiError::InvalidArgument),
                }).transpose()?;
                f.finish()?;
                K::Clock(p::NativeClockDefault { kind, precision })
            }
            _ => return Err(ApiError::InvalidArgument),
        });
        Ok(p::NativeColumnDefault { kind })
    }
    pub(super) fn search_path(&mut self, v: Json) -> R<p::NativeSearchPathRequest> {
        let mut f = Object::new(v)?;
        let set = f.optional("set").map(boolean).transpose()?;
        let schemas = items(f.optional("schemas"), 64)?.into_iter().map(text).collect::<R<_>>()?;
        f.finish()?;
        Ok(p::NativeSearchPathRequest { set, schemas })
    }
    pub(super) fn typed_read(&mut self, v: Json) -> R<p::NativeTypedRead> {
        let mut f = Object::new(v)?;
        let table = Some(text(f.required("table")?)?);
        let projection = items(f.optional("projection"), 64)?.into_iter().map(|v| {
            let mut f = Object::new(v)?;
            let expression = Some(self.read_expr(f.required("expression")?)?);
            let alias = f.optional("alias").map(text).transpose()?;
            f.finish()?;
            Ok(p::NativeReadProjection { expression, alias })
        }).collect::<R<_>>()?;
        let filter = f.optional("filter").map(|v| self.read_expr(v)).transpose()?;
        let group_by = items(f.optional("groupBy"), 64)?.into_iter()
            .map(|v| self.read_expr(v)).collect::<R<_>>()?;
        let having = f.optional("having").map(|v| self.read_expr(v)).transpose()?;
        let order_by = items(f.optional("orderBy"), 64)?.into_iter().map(|v| {
            let mut f = Object::new(v)?;
            let expression = Some(self.read_expr(f.required("expression")?)?);
            let descending = Some(boolean(f.required("descending")?)?);
            let nulls_first = f.optional("nullsFirst").map(boolean).transpose()?;
            f.finish()?;
            Ok(p::NativeReadOrder { expression, descending, nulls_first })
        }).collect::<R<_>>()?;
        let limit = f.optional("limit").map(super::json::int).transpose()?;
        let join = f.optional("join").map(|v| -> R<_> {
            let mut f = Object::new(v)?;
            let table = Some(text(f.required("table")?)?);
            let left_alias = Some(text(f.required("leftAlias")?)?);
            let right_alias = Some(text(f.required("rightAlias")?)?);
            let left = Some(boolean(f.required("left")?)?);
            let on = Some(self.read_expr(f.required("on")?)?);
            f.finish()?;
            Ok(p::NativeReadJoin { table, left_alias, right_alias, left, on })
        }).transpose()?;
        f.finish()?;
        Ok(p::NativeTypedRead { table, projection, filter, group_by, having, order_by, limit, join })
    }
    fn read_column(&mut self, v: Json) -> R<p::NativeReadColumn> {
        let mut f = Object::new(v)?;
        let name = Some(text(f.required("name")?)?);
        let source = f.optional("source").map(text).transpose()?;
        f.finish()?;
        Ok(p::NativeReadColumn { name, source })
    }
    fn read_expr(&mut self, v: Json) -> R<p::NativeReadExpr> {
        if self.depth > super::native_codec::MAX_EXPR_DEPTH || self.nodes >= 4096 {
            return Err(ApiError::InvalidArgument);
        }
        self.depth += 1;
        self.nodes += 1;
        let (kind, v) = Object::new(v)?.one()?;
        use p::native_read_expr::Expression as E;
        let expression = Some(match kind.as_str() {
            "column" => E::Column(self.read_column(v)?),
            "literal" => E::Literal(decode_value(v)?),
            "aggregate" => {
                let mut f = Object::new(v)?;
                let function = Some(text(f.required("function")?)?);
                let column = f.optional("column").map(|v| self.read_column(v)).transpose()?;
                f.finish()?;
                E::Aggregate(p::NativeReadAggregate { function, column })
            }
            "notExpr" => E::NotExpr(Box::new(self.read_expr(v)?)),
            "isNull" => {
                let mut f = Object::new(v)?;
                let expression = Some(Box::new(self.read_expr(f.required("expression")?)?));
                let negated = Some(boolean(f.required("negated")?)?);
                f.finish()?;
                E::IsNull(Box::new(p::NativeReadNull { expression, negated }))
            }
            "compare" | "andExpr" | "orExpr" => {
                let mut f = Object::new(v)?;
                let left = Some(Box::new(self.read_expr(f.required("left")?)?));
                let right = Some(Box::new(self.read_expr(f.required("right")?)?));
                let result = if kind == "compare" {
                    let comparison = p::NativeComparison::from_str_name(&text(f.required("comparison")?)?)
                        .ok_or(ApiError::InvalidArgument)? as i32;
                    E::Compare(Box::new(p::NativeReadCompare { left, comparison, right }))
                } else if kind == "andExpr" {
                    E::AndExpr(Box::new(p::NativeReadLogic { left, right }))
                } else {
                    E::OrExpr(Box::new(p::NativeReadLogic { left, right }))
                };
                f.finish()?;
                result
            }
            _ => return Err(ApiError::InvalidArgument),
        });
        self.depth -= 1;
        Ok(p::NativeReadExpr { expression })
    }
    pub(super) fn savepoint(&mut self, v: Json) -> R<p::NativeSavepointRequest> {
        let mut f = Object::new(v)?;
        let name = text(f.required("name")?)?;
        if name.is_empty() || name.contains('\0') { return Err(ApiError::InvalidArgument); }
        f.finish()?;
        Ok(p::NativeSavepointRequest { name: Some(name) })
    }
    pub(super) fn begin_request(&mut self, v: Json) -> R<p::BeginRequest> {
        empty(v)?;
        Ok(p::BeginRequest {})
    }
    pub(super) fn commit_request(&mut self, v: Json) -> R<p::CommitRequest> {
        empty(v)?;
        Ok(p::CommitRequest {})
    }
    pub(super) fn rollback_request(&mut self, v: Json) -> R<p::RollbackRequest> {
        empty(v)?;
        Ok(p::RollbackRequest {})
    }
    pub(super) fn native_empty(&mut self, v: Json) -> R<p::NativeEmpty> {
        empty(v)?;
        Ok(p::NativeEmpty {})
    }
    pub(super) fn native_text_request(&mut self, v: Json) -> R<p::NativeTextRequest> {
        let mut f = Object::new(v)?;
        let language_version = f.optional("languageVersion").map(text).transpose()?;
        let text = text(f.required("text")?)?;
        let bindings = items(f.optional("bindings"), 256)?
            .into_iter()
            .map(|v| self.native_binding(v))
            .collect::<R<Vec<_>>>()?;
        f.finish()?;
        Ok(p::NativeTextRequest {
            text,
            bindings,
            language_version,
        })
    }
    pub(super) fn native_operation_request(&mut self, v: Json) -> R<p::NativeOperationRequest> {
        let mut f = Object::new(v)?;
        let request = Some(self.native_request(f.required("request")?)?);
        let bindings = items(f.optional("bindings"), 256)?
            .into_iter()
            .map(|v| self.native_binding(v))
            .collect::<R<Vec<_>>>()?;
        f.finish()?;
        Ok(p::NativeOperationRequest { request, bindings })
    }
    pub(super) fn native_transaction_request(&mut self, v: Json) -> R<p::NativeTransactionRequest> {
        let mut f = Object::new(v)?;
        let transaction = Some(self.native_transaction(f.required("transaction")?)?);
        let bindings = items(f.optional("bindings"), 256)?
            .into_iter()
            .map(|v| self.native_binding(v))
            .collect::<R<Vec<_>>>()?;
        f.finish()?;
        Ok(p::NativeTransactionRequest {
            transaction,
            bindings,
        })
    }
    pub(super) fn native_binding(&mut self, v: Json) -> R<p::NativeBinding> {
        let mut f = Object::new(v)?;
        let name = Some(text(f.required("name")?)?);
        let value = Some(decode_value(f.required("value")?)?);
        f.finish()?;
        Ok(p::NativeBinding { name, value })
    }
    pub(super) fn native_request(&mut self, v: Json) -> R<p::NativeRequest> {
        if self.wrappers > 2 {
            return Err(ApiError::InvalidArgument);
        }
        self.wrappers += 1;
        let (kind, v) = Object::new(v)?.one()?;
        let request = Some(match kind.as_str() {
            "read" => p::native_request::Request::Read(self.native_read(v)?),
            "insert" => p::native_request::Request::Insert(self.native_insert(v)?),
            "update" => p::native_request::Request::Update(self.native_update(v)?),
            "delete" => p::native_request::Request::Delete(self.native_delete(v)?),
            "updateExpressions" => {
                p::native_request::Request::UpdateExpressions(self.native_update_expressions(v)?)
            }
            "readExpressions" => {
                p::native_request::Request::ReadExpressions(self.native_read_expressions(v)?)
            }
            "returning" => {
                p::native_request::Request::Returning(Box::new(self.native_returning(v)?))
            }
            "insertExpressions" => {
                p::native_request::Request::InsertExpressions(self.native_insert_expressions(v)?)
            }
            "insertMany" => p::native_request::Request::InsertMany(self.native_insert_many(v)?),
            "onConflict" => {
                p::native_request::Request::OnConflict(Box::new(self.native_on_conflict(v)?))
            }
            _ => return Err(ApiError::InvalidArgument),
        });
        self.wrappers -= 1;
        Ok(p::NativeRequest { request })
    }
    pub(super) fn native_read(&mut self, v: Json) -> R<p::NativeRead> {
        let mut f = Object::new(v)?;
        let table = Some(text(f.required("table")?)?);
        let filter = f
            .optional("filter")
            .map(|v| self.native_expr(v))
            .transpose()?;
        let order = items(f.optional("order"), 64)?
            .into_iter()
            .map(|v| self.native_sort(v))
            .collect::<R<Vec<_>>>()?;
        let limit = f
            .optional("limit")
            .map(|v| self.native_value_expr(v))
            .transpose()?;
        let projection = items(f.optional("projection"), 64)?
            .into_iter()
            .map(text)
            .collect::<R<Vec<_>>>()?;
        f.finish()?;
        Ok(p::NativeRead {
            table,
            filter,
            order,
            limit,
            projection,
        })
    }
    pub(super) fn native_insert(&mut self, v: Json) -> R<p::NativeInsert> {
        let mut f = Object::new(v)?;
        let table = Some(text(f.required("table")?)?);
        let values = items(f.optional("values"), 64)?
            .into_iter()
            .map(|v| self.native_value_assignment(v))
            .collect::<R<Vec<_>>>()?;
        f.finish()?;
        Ok(p::NativeInsert { table, values })
    }
    pub(super) fn native_update(&mut self, v: Json) -> R<p::NativeUpdate> {
        let mut f = Object::new(v)?;
        let table = Some(text(f.required("table")?)?);
        let filter = Some(self.native_expr(f.required("filter")?)?);
        let guard = f
            .optional("guard")
            .map(|v| self.native_expr(v))
            .transpose()?;
        let values = items(f.optional("values"), 64)?
            .into_iter()
            .map(|v| self.native_value_assignment(v))
            .collect::<R<Vec<_>>>()?;
        let intent = Some(self.native_intent(f.required("intent")?)?);
        f.finish()?;
        Ok(p::NativeUpdate {
            table,
            filter,
            guard,
            values,
            intent,
        })
    }
    pub(super) fn native_delete(&mut self, v: Json) -> R<p::NativeDelete> {
        let mut f = Object::new(v)?;
        let table = Some(text(f.required("table")?)?);
        let filter = Some(self.native_expr(f.required("filter")?)?);
        let guard = f
            .optional("guard")
            .map(|v| self.native_expr(v))
            .transpose()?;
        let cap = f
            .optional("cap")
            .map(|v| self.native_value_expr(v))
            .transpose()?;
        f.finish()?;
        Ok(p::NativeDelete {
            table,
            filter,
            guard,
            cap,
        })
    }
    pub(super) fn native_update_expressions(&mut self, v: Json) -> R<p::NativeUpdateExpressions> {
        let mut f = Object::new(v)?;
        let table = Some(text(f.required("table")?)?);
        let filter = Some(self.native_expr(f.required("filter")?)?);
        let guard = f
            .optional("guard")
            .map(|v| self.native_expr(v))
            .transpose()?;
        let values = items(f.optional("values"), 64)?
            .into_iter()
            .map(|v| self.native_assignment(v))
            .collect::<R<Vec<_>>>()?;
        let intent = Some(self.native_intent(f.required("intent")?)?);
        f.finish()?;
        Ok(p::NativeUpdateExpressions {
            table,
            filter,
            guard,
            values,
            intent,
        })
    }
    pub(super) fn native_read_expressions(&mut self, v: Json) -> R<p::NativeReadExpressions> {
        let mut f = Object::new(v)?;
        let table = Some(text(f.required("table")?)?);
        let filter = f
            .optional("filter")
            .map(|v| self.native_expr(v))
            .transpose()?;
        let order = items(f.optional("order"), 64)?
            .into_iter()
            .map(|v| self.native_sort(v))
            .collect::<R<Vec<_>>>()?;
        let limit = f
            .optional("limit")
            .map(|v| self.native_value_expr(v))
            .transpose()?;
        let projection = items(f.optional("projection"), 64)?
            .into_iter()
            .map(|v| self.native_projection(v))
            .collect::<R<Vec<_>>>()?;
        f.finish()?;
        Ok(p::NativeReadExpressions {
            table,
            filter,
            order,
            limit,
            projection,
        })
    }
    pub(super) fn native_returning(&mut self, v: Json) -> R<p::NativeReturning> {
        let mut f = Object::new(v)?;
        let request = Some(Box::new(self.native_request(f.required("request")?)?));
        let projection = items(f.optional("projection"), 64)?
            .into_iter()
            .map(|v| self.native_projection(v))
            .collect::<R<Vec<_>>>()?;
        f.finish()?;
        Ok(p::NativeReturning {
            request,
            projection,
        })
    }
    pub(super) fn native_insert_expressions(&mut self, v: Json) -> R<p::NativeInsertExpressions> {
        let mut f = Object::new(v)?;
        let table = Some(text(f.required("table")?)?);
        let values = items(f.optional("values"), 64)?
            .into_iter()
            .map(|v| self.native_assignment(v))
            .collect::<R<Vec<_>>>()?;
        f.finish()?;
        Ok(p::NativeInsertExpressions { table, values })
    }
    pub(super) fn native_insert_many(&mut self, v: Json) -> R<p::NativeInsertMany> {
        let mut f = Object::new(v)?;
        let table = Some(text(f.required("table")?)?);
        let columns = items(f.optional("columns"), 64)?
            .into_iter()
            .map(text)
            .collect::<R<Vec<_>>>()?;
        let rows = items(f.optional("rows"), 1024)?
            .into_iter()
            .map(|v| self.native_expression_row(v))
            .collect::<R<Vec<_>>>()?;
        let cap = Some(self.native_value_expr(f.required("cap")?)?);
        f.finish()?;
        Ok(p::NativeInsertMany {
            table,
            columns,
            rows,
            cap,
        })
    }
    pub(super) fn native_on_conflict(&mut self, v: Json) -> R<p::NativeOnConflict> {
        let mut f = Object::new(v)?;
        let request = Some(Box::new(self.native_request(f.required("request")?)?));
        let target = items(f.optional("target"), 64)?
            .into_iter()
            .map(text)
            .collect::<R<Vec<_>>>()?;
        let action = Some(self.native_conflict_action(f.required("action")?)?);
        f.finish()?;
        Ok(p::NativeOnConflict {
            request,
            target,
            action,
        })
    }
    pub(super) fn native_value_assignment(&mut self, v: Json) -> R<p::NativeValueAssignment> {
        let mut f = Object::new(v)?;
        let column = Some(text(f.required("column")?)?);
        let value = Some(self.native_value_expr(f.required("value")?)?);
        f.finish()?;
        Ok(p::NativeValueAssignment { column, value })
    }
    pub(super) fn native_assignment(&mut self, v: Json) -> R<p::NativeAssignment> {
        let mut f = Object::new(v)?;
        let column = Some(text(f.required("column")?)?);
        let expression = Some(self.native_expr(f.required("expression")?)?);
        f.finish()?;
        Ok(p::NativeAssignment { column, expression })
    }
    pub(super) fn native_projection(&mut self, v: Json) -> R<p::NativeProjection> {
        let mut f = Object::new(v)?;
        let expression = Some(self.native_expr(f.required("expression")?)?);
        let alias = f.optional("alias").map(text).transpose()?;
        f.finish()?;
        Ok(p::NativeProjection { expression, alias })
    }
    pub(super) fn native_expression_row(&mut self, v: Json) -> R<p::NativeExpressionRow> {
        let mut f = Object::new(v)?;
        let values = items(f.optional("values"), 64)?
            .into_iter()
            .map(|v| self.native_expr(v))
            .collect::<R<Vec<_>>>()?;
        f.finish()?;
        Ok(p::NativeExpressionRow { values })
    }
    pub(super) fn native_sort(&mut self, v: Json) -> R<p::NativeSort> {
        let mut f = Object::new(v)?;
        let field = Some(text(f.required("field")?)?);
        let descending = Some(boolean(f.required("descending")?)?);
        f.finish()?;
        Ok(p::NativeSort { field, descending })
    }
    pub(super) fn native_value_expr(&mut self, v: Json) -> R<p::NativeValueExpr> {
        let (kind, v) = Object::new(v)?.one()?;
        let value = Some(match kind.as_str() {
            "literal" => p::native_value_expr::Value::Literal(decode_value(v)?),
            "parameter" => p::native_value_expr::Value::Parameter(text(v)?),
            _ => return Err(ApiError::InvalidArgument),
        });
        Ok(p::NativeValueExpr { value })
    }
    pub(super) fn native_intent(&mut self, v: Json) -> R<p::NativeIntent> {
        let (kind, v) = Object::new(v)?.one()?;
        let intent = Some(match kind.as_str() {
            "one" => p::native_intent::Intent::One(self.native_empty(v)?),
            "many" => p::native_intent::Intent::Many(self.native_many(v)?),
            "claim" => p::native_intent::Intent::Claim(self.native_claim(v)?),
            _ => return Err(ApiError::InvalidArgument),
        });
        Ok(p::NativeIntent { intent })
    }
    pub(super) fn native_many(&mut self, v: Json) -> R<p::NativeMany> {
        let mut f = Object::new(v)?;
        let cap = Some(self.native_value_expr(f.required("cap")?)?);
        f.finish()?;
        Ok(p::NativeMany { cap })
    }
    pub(super) fn native_claim(&mut self, v: Json) -> R<p::NativeClaim> {
        let mut f = Object::new(v)?;
        let order = items(f.optional("order"), 64)?
            .into_iter()
            .map(|v| self.native_sort(v))
            .collect::<R<Vec<_>>>()?;
        f.finish()?;
        Ok(p::NativeClaim { order })
    }
    pub(super) fn native_conflict_action(&mut self, v: Json) -> R<p::NativeConflictAction> {
        let (kind, v) = Object::new(v)?.one()?;
        let action = Some(match kind.as_str() {
            "nothing" => p::native_conflict_action::Action::Nothing(self.native_empty(v)?),
            "update" => p::native_conflict_action::Action::Update(self.native_conflict_update(v)?),
            _ => return Err(ApiError::InvalidArgument),
        });
        Ok(p::NativeConflictAction { action })
    }
    pub(super) fn native_conflict_update(&mut self, v: Json) -> R<p::NativeConflictUpdate> {
        let mut f = Object::new(v)?;
        let guard = f
            .optional("guard")
            .map(|v| self.native_expr(v))
            .transpose()?;
        let values = items(f.optional("values"), 64)?
            .into_iter()
            .map(|v| self.native_assignment(v))
            .collect::<R<Vec<_>>>()?;
        f.finish()?;
        Ok(p::NativeConflictUpdate { guard, values })
    }
    pub(super) fn native_expr(&mut self, v: Json) -> R<p::NativeExpr> {
        if self.depth > super::native_codec::MAX_EXPR_DEPTH || self.nodes >= 4096 {
            return Err(ApiError::InvalidArgument);
        }
        self.nodes += 1;
        self.depth += 1;
        let (kind, v) = Object::new(v)?.one()?;
        let expression = Some(match kind.as_str() {
            "value" => p::native_expr::Expression::Value(self.native_value_expr(v)?),
            "field" => p::native_expr::Expression::Field(text(v)?),
            "compare" => p::native_expr::Expression::Compare(Box::new(self.native_compare(v)?)),
            "isNull" => p::native_expr::Expression::IsNull(Box::new(self.native_is_null(v)?)),
            "notExpr" => p::native_expr::Expression::NotExpr(Box::new(self.native_expr(v)?)),
            "andExpr" => p::native_expr::Expression::AndExpr(Box::new(self.native_binary(v)?)),
            "orExpr" => p::native_expr::Expression::OrExpr(Box::new(self.native_binary(v)?)),
            "negate" => p::native_expr::Expression::Negate(Box::new(self.native_expr(v)?)),
            "arithmetic" => {
                p::native_expr::Expression::Arithmetic(Box::new(self.native_arithmetic(v)?))
            }
            "local" => p::native_expr::Expression::Local(self.native_local_ref(v)?),
            "call" => p::native_expr::Expression::Call(self.native_call(v)?),
            "caseExpr" => p::native_expr::Expression::CaseExpr(Box::new(self.native_case(v)?)),
            "conflictField" => {
                p::native_expr::Expression::ConflictField(self.native_conflict_field(v)?)
            }
            _ => return Err(ApiError::InvalidArgument),
        });
        self.depth -= 1;
        Ok(p::NativeExpr { expression })
    }
    pub(super) fn native_compare(&mut self, v: Json) -> R<p::NativeCompare> {
        let mut f = Object::new(v)?;
        let left = Some(Box::new(self.native_expr(f.required("left")?)?));
        let comparison = p::NativeComparison::from_str_name(&text(f.required("comparison")?)?)
            .filter(|v| *v as i32 != 0)
            .ok_or(ApiError::InvalidArgument)? as i32;
        let right = Some(Box::new(self.native_expr(f.required("right")?)?));
        f.finish()?;
        Ok(p::NativeCompare {
            left,
            comparison,
            right,
        })
    }
    pub(super) fn native_is_null(&mut self, v: Json) -> R<p::NativeIsNull> {
        let mut f = Object::new(v)?;
        let expression = Some(Box::new(self.native_expr(f.required("expression")?)?));
        let negated = Some(boolean(f.required("negated")?)?);
        f.finish()?;
        Ok(p::NativeIsNull {
            expression,
            negated,
        })
    }
    pub(super) fn native_binary(&mut self, v: Json) -> R<p::NativeBinary> {
        let mut f = Object::new(v)?;
        let left = Some(Box::new(self.native_expr(f.required("left")?)?));
        let right = Some(Box::new(self.native_expr(f.required("right")?)?));
        f.finish()?;
        Ok(p::NativeBinary { left, right })
    }
    pub(super) fn native_arithmetic(&mut self, v: Json) -> R<p::NativeArithmetic> {
        let mut f = Object::new(v)?;
        let left = Some(Box::new(self.native_expr(f.required("left")?)?));
        let operator = p::NativeArithmeticOperator::from_str_name(&text(f.required("operator")?)?)
            .filter(|v| *v as i32 != 0)
            .ok_or(ApiError::InvalidArgument)? as i32;
        let right = Some(Box::new(self.native_expr(f.required("right")?)?));
        f.finish()?;
        Ok(p::NativeArithmetic {
            left,
            operator,
            right,
        })
    }
    pub(super) fn native_call(&mut self, v: Json) -> R<p::NativeCall> {
        let mut f = Object::new(v)?;
        let function = p::NativeFunction::from_str_name(&text(f.required("function")?)?)
            .filter(|v| *v as i32 != 0)
            .ok_or(ApiError::InvalidArgument)? as i32;
        let arguments = items(f.optional("arguments"), 64)?
            .into_iter()
            .map(|v| self.native_expr(v))
            .collect::<R<Vec<_>>>()?;
        f.finish()?;
        Ok(p::NativeCall {
            function,
            arguments,
        })
    }
    pub(super) fn native_case(&mut self, v: Json) -> R<p::NativeCase> {
        let mut f = Object::new(v)?;
        let operand = f
            .optional("operand")
            .map(|v| Ok(Box::new(self.native_expr(v)?)))
            .transpose()?;
        let arms = items(f.optional("arms"), 64)?
            .into_iter()
            .map(|v| self.native_case_arm(v))
            .collect::<R<Vec<_>>>()?;
        let else_expr = f
            .optional("elseExpr")
            .map(|v| Ok(Box::new(self.native_expr(v)?)))
            .transpose()?;
        f.finish()?;
        Ok(p::NativeCase {
            operand,
            arms,
            else_expr,
        })
    }
    pub(super) fn native_case_arm(&mut self, v: Json) -> R<p::NativeCaseArm> {
        let mut f = Object::new(v)?;
        let when = Some(self.native_expr(f.required("when")?)?);
        let then = Some(self.native_expr(f.required("then")?)?);
        f.finish()?;
        Ok(p::NativeCaseArm { when, then })
    }
    pub(super) fn native_conflict_field(&mut self, v: Json) -> R<p::NativeConflictField> {
        let mut f = Object::new(v)?;
        let source = p::NativeConflictSource::from_str_name(&text(f.required("source")?)?)
            .filter(|v| *v as i32 != 0)
            .ok_or(ApiError::InvalidArgument)? as i32;
        let field = Some(text(f.required("field")?)?);
        f.finish()?;
        Ok(p::NativeConflictField { source, field })
    }
    pub(super) fn native_local_ref(&mut self, v: Json) -> R<p::NativeLocalRef> {
        let (kind, v) = Object::new(v)?.one()?;
        let reference = Some(match kind.as_str() {
            "field" => p::native_local_ref::Reference::Field(self.native_local_field(v)?),
            "status" => p::native_local_ref::Reference::Status(text(v)?),
            "changed" => p::native_local_ref::Reference::Changed(text(v)?),
            "count" => p::native_local_ref::Reference::Count(text(v)?),
            _ => return Err(ApiError::InvalidArgument),
        });
        Ok(p::NativeLocalRef { reference })
    }
    pub(super) fn native_local_field(&mut self, v: Json) -> R<p::NativeLocalField> {
        let mut f = Object::new(v)?;
        let local = Some(text(f.required("local")?)?);
        let field = Some(text(f.required("field")?)?);
        f.finish()?;
        Ok(p::NativeLocalField { local, field })
    }
    pub(super) fn native_transaction(&mut self, v: Json) -> R<p::NativeTransaction> {
        let mut f = Object::new(v)?;
        let read_only = Some(boolean(f.required("readOnly")?)?);
        let steps = items(f.optional("steps"), 64)?
            .into_iter()
            .map(|v| self.native_transaction_step(v))
            .collect::<R<Vec<_>>>()?;
        let emit = f
            .optional("emit")
            .map(|v| self.native_emit(v))
            .transpose()?;
        f.finish()?;
        Ok(p::NativeTransaction {
            read_only,
            steps,
            emit,
        })
    }
    pub(super) fn native_transaction_step(&mut self, v: Json) -> R<p::NativeTransactionStep> {
        let (kind, v) = Object::new(v)?.one()?;
        let step = Some(match kind.as_str() {
            "execute" => p::native_transaction_step::Step::Execute(self.native_request(v)?),
            "letBinding" => p::native_transaction_step::Step::LetBinding(self.native_let(v)?),
            "require" => p::native_transaction_step::Step::Require(self.native_expr(v)?),
            _ => return Err(ApiError::InvalidArgument),
        });
        Ok(p::NativeTransactionStep { step })
    }
    pub(super) fn native_let(&mut self, v: Json) -> R<p::NativeLet> {
        let mut f = Object::new(v)?;
        let name = Some(text(f.required("name")?)?);
        let request = Some(self.native_request(f.required("request")?)?);
        f.finish()?;
        Ok(p::NativeLet { name, request })
    }
    pub(super) fn native_emit(&mut self, v: Json) -> R<p::NativeEmit> {
        let mut f = Object::new(v)?;
        let values = items(f.optional("values"), 64)?
            .into_iter()
            .map(|v| self.native_emit_expr(v))
            .collect::<R<Vec<_>>>()?;
        f.finish()?;
        Ok(p::NativeEmit { values })
    }
    pub(super) fn native_emit_expr(&mut self, v: Json) -> R<p::NativeEmitExpr> {
        let (kind, v) = Object::new(v)?.one()?;
        let value = Some(match kind.as_str() {
            "scalar" => p::native_emit_expr::Value::Scalar(self.native_expr(v)?),
            "row" => p::native_emit_expr::Value::Row(text(v)?),
            "rows" => p::native_emit_expr::Value::Rows(text(v)?),
            _ => return Err(ApiError::InvalidArgument),
        });
        Ok(p::NativeEmitExpr { value })
    }
}

impl Serialize for p::NativeCatalogColumn {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(None)?;
        if let Some(v) = &self.name { m.serialize_entry("name", v)?; }
        if let Some(v) = &self.data_type { m.serialize_entry("dataType", v)?; }
        if let Some(v) = &self.not_null { m.serialize_entry("notNull", v)?; }
        if let Some(v) = &self.default { m.serialize_entry("default", v)?; }
        if let Some(v) = &self.generated { m.serialize_entry("generated", v)?; }
        m.end()
    }
}
impl Serialize for p::NativeCatalogOutcome {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(None)?;
        m.serialize_entry("catalogEpoch", &Decimal(self.catalog_epoch))?;
        m.serialize_entry("table", &self.table)?;
        m.serialize_entry("columns", &self.columns)?;
        m.serialize_entry("primaryKey", &self.primary_key)?;
        if let Some(v) = self.index_id { m.serialize_entry("indexId", &Decimal(v))?; }
        m.end()
    }
}

impl Serialize for p::NativeColumnDefault {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(Some(1))?;
        match &self.kind {
            Some(p::native_column_default::Kind::Value(v)) => m.serialize_entry("Value", &JsonValue(v))?,
            Some(p::native_column_default::Kind::Nextval(v)) => m.serialize_entry("Nextval", v)?,
            Some(p::native_column_default::Kind::Clock(v)) => m.serialize_entry("Clock", v)?,
            None => return Err(S::Error::custom("missing column default")),
        }
        m.end()
    }
}
impl Serialize for p::NativeClockDefault {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(None)?;
        if let Some(v) = &self.kind { m.serialize_entry("kind", v)?; }
        if let Some(v) = &self.precision { m.serialize_entry("precision", v)?; }
        m.end()
    }
}
#[cfg(test)]
mod catalog_defaults_tests {
    use super::*;
    fn decode(s: &str, add: bool) -> R<giggles_core::operations::CatalogRequest> {
        let action = if add { "alterTable" } else { "createTable" };
        let columns = if add { format!("\"addColumn\":{s}") } else { format!("\"columns\":[{s}]") };
        let json = format!("{{\"{action}\":{{\"table\":\"t\",{columns}}}}}");
        let request = super::super::json::decode_execute(format!("{{\"catalog\":{json}}}").as_bytes())?;
        let Some(p::execute_request::Request::Catalog(catalog)) = request.request else { unreachable!() };
        if let Some(p::native_catalog_request::Request::CreateTable(v)) = &catalog.request {
            assert_eq!(serde_json::to_value(&v.columns[0]).unwrap(), serde_json::from_str::<serde_json::Value>(s).unwrap());
        }
        super::super::native_codec::catalog_request(catalog)
    }
    #[test]
    fn strict_column_defaults() {
        for field in [r#""default":{"Value":{"intValue":"7"}}"#, r#""default":{"Nextval":"s"}"#,
            r#""generated":"IdentityAlways""#, r#""generated":"IdentityByDefault""#, r#""generated":"Serial""#] {
            let column = format!(r#"{{"name":"c","dataType":"Integer",{field}}}"#);
            assert!(decode(&column, false).is_ok(), "{column}");
            assert!(decode(&column, true).is_err());
        }
        for kind in ["Now", "TransactionTimestamp", "StatementTimestamp", "ClockTimestamp", "CurrentDate", "CurrentTime", "CurrentTimestamp", "LocalTime", "LocalTimestamp"] {
            for precision in ["", ",\"precision\":3"] {
                let column = format!(r#"{{"name":"c","dataType":"Time","default":{{"Clock":{{"kind":"{kind}"{precision}}}}}}}"#);
                assert_eq!(decode(&column, false).is_ok(), precision.is_empty() || ["CurrentTime", "CurrentTimestamp", "LocalTime", "LocalTimestamp"].contains(&kind), "{column}");
            }
        }
        for field in [r#""default":{}"#, r#""default":{"Value":{}}"#, r#""default":{"Unknown":"x"}"#, r#""default":{"Value":{"intValue":"1"},"Nextval":"s"}"#,
            r#""default":{"Clock":{}}"#, r#""default":{"Clock":{"kind":"bad"}}"#, r#""default":{"Clock":{"kind":"Now","extra":1}}"#,
            r#""default":{"Clock":{"kind":"CurrentTime","precision":-1}}"#, r#""default":{"Clock":{"kind":"CurrentTime","precision":"3"}}"#,
            r#""default":{"Clock":{"kind":"CurrentTime","precision":4294967296}}"#, r#""default":{"Clock":{"kind":"CurrentTime","precision":null}}"#,
            r#""generated":"unknown""#, r#""extra":1"#] {
            assert!(decode(&format!(r#"{{"name":"c","dataType":"Integer",{field}}}"#), false).is_err(), "{field}");
        }
    }
}
