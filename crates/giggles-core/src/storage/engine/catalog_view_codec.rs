//! SQL-view DTO v1/v2 and catalog-wide reference validation.
use super::{corrupt, object_key};
use crate::{
    CoreError,
    relational::{DataType, MAX_COLUMNS},
};
use std::collections::BTreeSet;

const MAX_SELECT_BYTES: usize = 1 << 20;
const MAX_DEPENDENCIES: usize = 100_000;
// One-byte tags independent of DataType's serde representation. Tags 0..=21 are frozen.
const STORAGE_TYPES: [DataType; 23] = [
    DataType::Boolean,
    DataType::SmallInt,
    DataType::Integer,
    DataType::BigInt,
    DataType::Text,
    DataType::Bytes,
    DataType::Numeric,
    DataType::Real,
    DataType::DoublePrecision,
    DataType::Date,
    DataType::Time,
    DataType::TimeTz,
    DataType::Timestamp,
    DataType::TimestampTz,
    DataType::Interval,
    DataType::Varchar,
    DataType::Bpchar,
    DataType::Uuid,
    DataType::Jsonb,
    DataType::IntArray,
    DataType::BigIntArray,
    DataType::TextArray,
    DataType::UuidArray,
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SqlViewColumn {
    pub(crate) id: u64,
    pub(crate) name: String,
    pub(crate) storage_type: DataType,
    pub(crate) pg_type_oid: u32,
    pub(crate) typmod: i32,
    pub(crate) source: Option<(u64, u64)>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SqlViewDependency {
    pub(crate) relation: u64,
    pub(crate) columns: Vec<u64>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SqlViewDef {
    pub(crate) schema: u64,
    pub(crate) name: String,
    pub(crate) columns: Vec<SqlViewColumn>,
    pub(crate) select: String,
    pub(crate) dependencies: Vec<SqlViewDependency>,
}
pub(crate) fn view_key(id: u64) -> Vec<u8> {
    object_key(17, id)
}
fn identifier(name: &str) -> bool {
    !name.is_empty() && name.len() <= 63 && !name.contains('\0')
}
fn validate(view: &SqlViewDef) -> Result<(), CoreError> {
    let mut ids = BTreeSet::new();
    let mut names = BTreeSet::new();
    if !identifier(&view.name)
        || view.columns.is_empty()
        || view.columns.len() > MAX_COLUMNS
        || view.columns.iter().any(|c| c.source.is_some() != view.columns[0].source.is_some())
        || view.select.is_empty()
        || view.select.len() > MAX_SELECT_BYTES
        || view.select.contains('\0')
        || view.dependencies.len() > MAX_DEPENDENCIES
        || view.columns.iter().any(|c| {
            c.id == 0
                || !identifier(&c.name)
                || !ids.insert(c.id)
                || !names.insert(&c.name)
                || c.pg_type_oid == 0
                || c.source.is_some_and(|(relation, column)| !view.dependencies.iter()
                    .any(|d| d.relation == relation && d.columns.contains(&column)))
        })
        || view
            .dependencies
            .iter()
            .any(|d| d.relation == 0 || d.columns.len() > MAX_COLUMNS || d.columns.contains(&0)
                || d.columns.iter().collect::<BTreeSet<_>>().len() != d.columns.len())
        || view.dependencies.windows(2).any(|d| d[0].relation >= d[1].relation)
    {
        return Err(corrupt("invalid SQL view definition"));
    }
    Ok(())
}
fn string(out: &mut Vec<u8>, text: &str) {
    out.extend((text.len() as u32).to_be_bytes());
    out.extend(text.as_bytes());
}
/// Version byte; schema u64 (0 = public); name; output count u32; outputs (id u64, name,
/// storage tag u8, PG OID u32, typmod i32); SELECT; dependency count u32;
/// dependencies (relation u64, column count u32, ordered column IDs u64).
/// Dependencies ascend by relation ID; unique columns follow source column order.
/// V2 appends one (source relation u64, source column u64) per output, in output order.
/// V1 has no bindings. All integers are BE; strings are u32 byte length plus UTF-8.
pub(crate) fn encode_view(view: &SqlViewDef) -> Result<Vec<u8>, CoreError> {
    validate(view)?;
    let version = if view.columns[0].source.is_some() { 2 } else { 1 };
    let mut out = vec![version];
    out.extend(view.schema.to_be_bytes());
    string(&mut out, &view.name);
    out.extend((view.columns.len() as u32).to_be_bytes());
    for column in &view.columns {
        out.extend(column.id.to_be_bytes());
        string(&mut out, &column.name);
        let tag = STORAGE_TYPES
            .iter()
            .position(|ty| ty == &column.storage_type)
            .ok_or_else(|| corrupt("unsupported SQL view storage type"))?;
        out.push(tag as u8);
        out.extend(column.pg_type_oid.to_be_bytes());
        out.extend(column.typmod.to_be_bytes());
    }
    string(&mut out, &view.select);
    out.extend((view.dependencies.len() as u32).to_be_bytes());
    for dependency in &view.dependencies {
        out.extend(dependency.relation.to_be_bytes());
        out.extend((dependency.columns.len() as u32).to_be_bytes());
        for column in &dependency.columns {
            out.extend(column.to_be_bytes());
        }
    }
    for (relation, source) in view.columns.iter().filter_map(|c| c.source) {
        out.extend(relation.to_be_bytes());
        out.extend(source.to_be_bytes());
    }
    Ok(out)
}
struct Reader<'a>(&'a [u8]);
impl Reader<'_> {
    fn bytes<const N: usize>(&mut self) -> Result<[u8; N], CoreError> {
        let (bytes, rest) = self
            .0
            .split_at_checked(N)
            .ok_or_else(|| corrupt("truncated SQL view"))?;
        self.0 = rest;
        Ok(bytes.try_into().unwrap())
    }
    fn count(&mut self, max: usize) -> Result<usize, CoreError> {
        let count = u32::from_be_bytes(self.bytes()?) as usize;
        if count > max {
            return Err(corrupt("SQL view length limit"));
        }
        Ok(count)
    }
    fn string(&mut self, max: usize) -> Result<String, CoreError> {
        let len = self.count(max)?;
        let (text, rest) = self
            .0
            .split_at_checked(len)
            .ok_or_else(|| corrupt("truncated SQL view string"))?;
        self.0 = rest;
        std::str::from_utf8(text)
            .map(str::to_owned)
            .map_err(|_| corrupt("invalid SQL view UTF-8"))
    }
}
pub(crate) fn decode_view(bytes: &[u8]) -> Result<SqlViewDef, CoreError> {
    let mut input = Reader(bytes);
    let version = input.bytes::<1>()?[0];
    if !matches!(version, 1 | 2) {
        return Err(corrupt("unsupported SQL view version"));
    }
    let schema = u64::from_be_bytes(input.bytes()?);
    let name = input.string(63)?;
    let count = input.count(MAX_COLUMNS)?;
    let mut columns = Vec::new();
    for _ in 0..count {
        columns.push(SqlViewColumn {
            id: u64::from_be_bytes(input.bytes()?),
            name: input.string(63)?,
            storage_type: STORAGE_TYPES
                .get(input.bytes::<1>()?[0] as usize)
                .ok_or_else(|| corrupt("invalid SQL view storage tag"))?
                .clone(),
            pg_type_oid: u32::from_be_bytes(input.bytes()?),
            typmod: i32::from_be_bytes(input.bytes()?),
            source: None,
        });
    }
    let select = input.string(MAX_SELECT_BYTES)?;
    let count = input.count(MAX_DEPENDENCIES)?;
    let mut dependencies = Vec::new();
    for _ in 0..count {
        let relation = u64::from_be_bytes(input.bytes()?);
        let count = input.count(MAX_COLUMNS)?;
        let mut columns = Vec::new();
        for _ in 0..count {
            columns.push(u64::from_be_bytes(input.bytes()?));
        }
        dependencies.push(SqlViewDependency { relation, columns });
    }
    if version == 2 {
        for column in &mut columns {
            column.source = Some((u64::from_be_bytes(input.bytes()?), u64::from_be_bytes(input.bytes()?)));
        }
    }
    if !input.0.is_empty() {
        return Err(corrupt("trailing SQL view bytes"));
    }
    let view = SqlViewDef {
        schema,
        name,
        columns,
        select,
        dependencies,
    };
    validate(&view)?;
    Ok(view)
}

pub(crate) fn relation_name(view: &SqlViewDef, schemas: &std::collections::BTreeMap<String, crate::relational::schemas::Schema>) -> Result<String, CoreError> {
    if view.schema == 0 { return Ok(crate::relational::relation_key("public", &view.name)); }
    let (schema, _) = schemas.iter().find(|(_, s)| s.id == view.schema)
        .ok_or(CoreError::Sql(crate::relational::SqlError::InvalidSchema))?;
    Ok(crate::relational::relation_key(schema, &view.name))
}
pub(crate) fn validate_catalog(parts: &crate::storage::engine::catalog::CatalogParts) -> Result<(), CoreError> {
    use crate::relational::SqlError;
    use std::collections::BTreeMap;
    if parts.sql_views.is_empty() { return Ok(()); }
    let fail = || CoreError::Sql(SqlError::DependentObjects);
    let mut columns: BTreeMap<u64, Vec<u64>> = parts.tables.values()
        .map(|t| (t.id, t.columns.iter().map(|c| c.id).collect())).collect();
    let mut pending = BTreeMap::new();
    let mut users: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
    for (id, view) in &parts.sql_views {
        validate(view)?;
        relation_name(view, &parts.ext.schemas)?;
        if *id == 0 || columns.insert(*id, view.columns.iter().map(|c| c.id).collect()).is_some() {
            return Err(CoreError::Sql(SqlError::Duplicate));
        }
    }
    for (id, view) in &parts.sql_views {
        let mut deps = BTreeSet::new();
        for dep in &view.dependencies {
            let source = columns.get(&dep.relation).ok_or_else(fail)?;
            if dep.columns.iter().any(|c| !source.contains(c)) { return Err(fail()); }
            if dep.columns.windows(2).any(|c| source.iter().position(|id| *id == c[0]) >= source.iter().position(|id| *id == c[1])) {
                return Err(corrupt("noncanonical SQL view dependency columns"));
            }
            if parts.sql_views.contains_key(&dep.relation) { deps.insert(dep.relation); }
        }
        pending.insert(*id, deps.len());
        for dep in deps { users.entry(dep).or_default().push(*id); }
    }
    let mut ready: Vec<_> = pending.iter().filter(|(_, n)| **n == 0).map(|(id, _)| *id).collect();
    let mut seen = 0;
    while let Some(id) = ready.pop() {
        seen += 1;
        for user in users.get(&id).into_iter().flatten() {
            let n = pending.get_mut(user).unwrap();
            *n -= 1;
            if *n == 0 { ready.push(*user); }
        }
    }
    if seen != parts.sql_views.len() { return Err(fail()); }
    Ok(())
}

#[cfg(test)]
#[path = "catalog_view_codec_tests.rs"]
mod tests;
