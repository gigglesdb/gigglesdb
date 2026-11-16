//! Deterministic MySQL -> PG18 DDL and catalog qualification.
//! Verification uses temporary expected tables to let PostgreSQL canonicalize
//! expressions itself; these tables and all verification writes are rolled back.
use super::schema::{ColumnDescriptor, TableDescriptor};
use crate::mapping::{
    PgType,
    mysql::{self, MysqlPgColumn},
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use tokio_postgres::{Client, GenericClient};

const VERSION: &str = "skedaddle-mysql-pg-schema-v1";
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockerKind {
    Descriptor,
    Mapping,
    GeneratedExpression,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Blocker {
    pub object: String,
    pub kind: BlockerKind,
    pub reason: String,
}
impl std::fmt::Display for Blocker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {:?}: {}", self.object, self.kind, self.reason)
    }
}
impl std::error::Error for Blocker {}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct OmittedDefault {
    pub object: String,
    pub value: String,
    pub reason: String,
}
#[derive(Clone, Debug, Serialize)]
struct Table {
    schema: String,
    name: String,
    body: String,
}
#[derive(Clone, Debug, Serialize)]
struct Collation {
    name: String,
    locale: String,
    deterministic: bool,
}
#[derive(Clone, Debug)]
pub struct MappedSchema {
    statements: Vec<String>,
    tables: Vec<Table>,
    collations: Vec<Collation>,
    omitted_defaults: Vec<OmittedDefault>,
    digest: [u8; 32],
}
impl MappedSchema {
    pub fn statements(&self) -> &[String] {
        &self.statements
    }
    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }
    pub fn omitted_defaults(&self) -> &[OmittedDefault] {
        &self.omitted_defaults
    }
}
fn ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}
// Utility DDL cannot bind parameters. Only quoted literals, never raw values,
// enter its grammar. E strings keep backslashes independent of session settings.
fn literal(s: &str) -> String {
    format!("E'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
}
fn qualified(schema: &str, name: &str) -> String {
    format!("{}.{}", ident(schema), ident(name))
}
fn valid_ident(s: &str) -> bool {
    !s.is_empty() && s.len() <= 63 && !s.contains('\0')
}

/// Plans columns internally so DDL cannot disagree with caller-supplied plans.
/// Input table order is irrelevant; column ordinal and primary-key order matter.
pub fn build(descriptors: &[TableDescriptor]) -> Result<MappedSchema, Blocker> {
    let mut descriptors = descriptors.to_vec();
    descriptors.sort_by(|a, b| (&a.schema, &a.table).cmp(&(&b.schema, &b.table)));
    let mut tables = Vec::new();
    let mut collations = BTreeMap::new();
    let mut omitted_defaults = Vec::new();
    let mut seen = BTreeSet::new();
    for t in &mut descriptors {
        let object = format!("{}.{}", t.schema, t.table);
        let block = |kind, reason: &str| Blocker {
            object: object.clone(),
            kind,
            reason: reason.into(),
        };
        t.columns.sort_by_key(|c| c.ordinal);
        if !valid_ident(&t.schema)
            || !valid_ident(&t.table)
            || !seen.insert((t.schema.clone(), t.table.clone()))
            || t.columns.is_empty()
            || t.primary_key.is_empty()
        {
            return Err(block(
                BlockerKind::Descriptor,
                "invalid/duplicate table or missing key/columns",
            ));
        }
        let mut names = BTreeSet::new();
        let mut ordinals = BTreeSet::new();
        for c in &t.columns {
            if !valid_ident(&c.name)
                || !names.insert(c.name.clone())
                || c.ordinal == 0
                || !ordinals.insert(c.ordinal)
            {
                return Err(block(
                    BlockerKind::Descriptor,
                    "invalid/duplicate column name or ordinal",
                ));
            }
        }
        if t.primary_key.iter().collect::<BTreeSet<_>>().len() != t.primary_key.len()
            || t.primary_key.iter().any(|k| {
                !t.columns
                    .iter()
                    .any(|c| &c.name == k && !c.nullable && !c.generated)
            })
        {
            return Err(block(
                BlockerKind::Descriptor,
                "primary key must name distinct non-null writable columns",
            ));
        }
        let plans = t
            .columns
            .iter()
            .map(mysql::plan_column)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| block(BlockerKind::Mapping, &e.to_string()))?;
        let mut definitions = Vec::new();
        for (c, p) in t.columns.iter().zip(&plans) {
            if let Some(co) = &p.collation
                && co.name != "C"
            {
                collations.insert(
                    co.name.clone(),
                    Collation {
                        name: co.name.clone(),
                        locale: co.locale.clone(),
                        deterministic: co.deterministic,
                    },
                );
            }
            // Qualify every type and collation; search_path never chooses them.
            let base = p.ddl_type.split(" COLLATE ").next().unwrap().replacen(
                "char(",
                "bpchar(",
                usize::from(matches!(p.pg_type, PgType::Char { .. })),
            );
            let mut ddl = format!("{} pg_catalog.{}", ident(&c.name), base);
            if let Some(co) = &p.collation {
                ddl.push_str(&format!(
                    " COLLATE {}",
                    qualified(
                        if co.name == "C" {
                            "pg_catalog"
                        } else {
                            "public"
                        },
                        &co.name
                    )
                ));
            }
            if c.generated {
                let expr = generated(c, &plans).ok_or_else(|| {
                    block(
                        BlockerKind::GeneratedExpression,
                        &format!("{}: unsupported generated expression", c.name),
                    )
                })?;
                ddl.push_str(&format!(" GENERATED ALWAYS AS ({expr}) STORED"));
            } else if c.generation_expression.is_some() {
                return Err(block(
                    BlockerKind::Descriptor,
                    "expression on non-generated column",
                ));
            } else if let Some(value) = &c.column_default {
                match default_literal(c, p, value) {
                    Some(value) => ddl.push_str(&format!(" DEFAULT {value}")),
                    None => omitted_defaults.push(OmittedDefault {
                        object: format!("{object}.{}", c.name),
                        value: value.clone(),
                        reason: "expression or literal without a qualified lossless mapping".into(),
                    }),
                }
            }
            if !c.nullable {
                ddl.push_str(" NOT NULL");
            }
            // Re-escape ENUM values as E strings; plan.check uses SQL strings.
            if let Some(check) = &p.check {
                ddl.push(' ');
                ddl.push_str(&escape_check(check));
            }
            definitions.push(ddl);
        }
        definitions.push(format!(
            "PRIMARY KEY ({})",
            t.primary_key
                .iter()
                .map(|k| ident(k))
                .collect::<Vec<_>>()
                .join(", ")
        ));
        tables.push(Table {
            schema: t.schema.clone(),
            name: t.table.clone(),
            body: definitions.join(", "),
        });
    }
    let collations: Vec<_> = collations.into_values().collect();
    let mut statements: Vec<_> = collations.iter().map(|co|format!("CREATE COLLATION IF NOT EXISTS {} (provider = icu, locale = {}, deterministic = {})",qualified("public",&co.name),literal(&co.locale),co.deterministic)).collect();
    for schema in tables.iter().map(|t| &t.schema).collect::<BTreeSet<_>>() {
        statements.push(format!("CREATE SCHEMA IF NOT EXISTS {}", ident(schema)));
    }
    for t in &tables {
        statements.push(format!(
            "CREATE TABLE IF NOT EXISTS {} ({})",
            qualified(&t.schema, &t.name),
            t.body
        ));
    }
    let canonical = serde_json::to_vec(&(VERSION, &descriptors, &statements, &omitted_defaults))
        .expect("strings serialize");
    Ok(MappedSchema {
        digest: Sha256::digest(canonical).into(),
        statements,
        tables,
        collations,
        omitted_defaults,
    })
}
fn escape_check(check: &str) -> String {
    // Only plan_column's closed CHECK grammar reaches here. Preserve doubled
    // quotes while converting its string tokens to explicit escape strings.
    let mut out = String::new();
    let mut inside = false;
    let mut identifier = false;
    let mut chars = check.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '"' && !inside {
            identifier = !identifier;
        }
        if c == '\'' && !identifier {
            if inside && chars.peek() == Some(&'\'') {
                out.push_str("''");
                chars.next();
                continue;
            }
            if !inside {
                out.push('E');
            }
            inside = !inside;
        }
        if c == '\\' && inside {
            out.push('\\');
        }
        out.push(c);
    }
    out
}
fn generated(c: &ColumnDescriptor, plans: &[MysqlPgColumn]) -> Option<String> {
    // A direct reference between exactly equal MySQL domains has no changed
    // arithmetic, rounding, charset, time-zone or function semantics.
    let expression = c.generation_expression.as_deref()?.trim();
    let name = if let Some(inner) = expression
        .strip_prefix('`')
        .and_then(|s| s.strip_suffix('`'))
    {
        let name = inner.replace("``", "`");
        if format!("`{}`", name.replace('`', "``")) != expression {
            return None;
        }
        name
    } else {
        if !expression.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_' || c == '$')
            || !expression
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
        {
            return None;
        }
        expression.into()
    };
    let source = plans
        .iter()
        .find(|p| p.source.name == name && !p.generated)?;
    let target = plans.iter().find(|p| p.source.name == c.name)?;
    (source.source.column_type == c.column_type
        && source.source.character_set == c.character_set
        && source.source.collation == c.collation
        && source.type_id == target.type_id
        && !matches!(target.pg_type, PgType::Jsonb | PgType::Bytea))
    .then(|| ident(&name))
}
fn default_literal(c: &ColumnDescriptor, p: &MysqlPgColumn, s: &str) -> Option<String> {
    use super::binlog::MysqlValue as V;
    if c.default_generated || s.contains('\0') {
        return None;
    }
    let value = match p.pg_type {
        PgType::Int2 | PgType::Int4 | PgType::Int8 | PgType::Numeric
            if matches!(
                c.data_type.as_str(),
                "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint"
            ) =>
        {
            if c.column_type.contains("unsigned") {
                V::Unsigned(s.parse().ok()?)
            } else {
                V::Signed(s.parse().ok()?)
            }
        }
        PgType::Numeric => V::Decimal(s.into()),
        PgType::Text { .. } if matches!(c.data_type.as_str(), "enum" | "set") => {
            let mapped =
                crate::mapping::Value::parse_pg_text(&p.pg_type, Some(s.as_bytes())).ok()?;
            mysql::inverse(p, &mapped).ok()?
        }
        PgType::Text { .. } | PgType::Char { .. } | PgType::Varchar { .. }
            if !matches!(c.data_type.as_str(), "enum" | "set") =>
        {
            V::Text {
                bytes: s.as_bytes().into(),
                collation: c.collation.as_ref()?.id,
            }
        }
        _ => return None,
    };
    let mapped = mysql::forward(p, &value).ok()?;
    let back = mysql::inverse(p, &mapped).ok()?;
    if value != back {
        return None;
    }
    let text = mapped.format_pg_text(&p.pg_type).ok()??;
    Some(literal(std::str::from_utf8(&text).ok()?))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MismatchKind {
    MissingTable,
    TableKind,
    Column,
    ExtraRequiredColumn,
    PrimaryKey,
    Check,
    Collation,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Mismatch {
    pub object: String,
    pub kind: MismatchKind,
}
#[derive(Debug)]
pub enum Error {
    Database(tokio_postgres::Error),
    Mismatches(Vec<Mismatch>),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "destination schema: {self:?}")
    }
}
impl std::error::Error for Error {}
impl From<tokio_postgres::Error> for Error {
    fn from(e: tokio_postgres::Error) -> Self {
        Self::Database(e)
    }
}

/// Catalog verification takes TEMP permission to canonicalize expected expressions
/// in real PostgreSQL. It rolls back the temporary relations even on success.
pub async fn verify_destination(
    client: &mut Client,
    schema: &MappedSchema,
) -> Result<Vec<Mismatch>, Error> {
    let tx = client.transaction().await?;
    let result = verify_in(&tx, schema).await;
    tx.rollback().await?;
    result
}
/// Verify within a caller-owned transaction; temporary expected relations are
/// dropped before returning. The caller owns rollback on a database error.
pub async fn verify_destination_in(
    tx: &tokio_postgres::Transaction<'_>,
    schema: &MappedSchema,
) -> Result<Vec<Mismatch>, Error> {
    verify_in(tx, schema).await
}
/// PG18 allows ICU collation DDL in a transaction. Failed qualification rolls
/// back this installation, including all newly created tables and collations.
pub async fn install(client: &mut Client, schema: &MappedSchema) -> Result<(), Error> {
    let tx = client.transaction().await?;
    tx.batch_execute(
        "SET LOCAL search_path = pg_catalog; SET LOCAL standard_conforming_strings = on",
    )
    .await?;
    for statement in &schema.statements {
        tx.batch_execute(statement).await?;
    }
    let mismatches = verify_in(&tx, schema).await?;
    if !mismatches.is_empty() {
        return Err(Error::Mismatches(mismatches));
    }
    tx.commit().await?;
    Ok(())
}
async fn columns<C: GenericClient + Sync>(
    client: &C,
    oid: u32,
) -> Result<BTreeMap<String, serde_json::Value>, Error> {
    let rows=client.query("SELECT a.attname, json_build_array(tn.nspname,t.typname,a.atttypmod,a.attnotnull,a.attgenerated,a.attidentity,cn.nspname,co.collname,pg_get_expr(d.adbin,d.adrelid))::text FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_type t ON t.oid=a.atttypid JOIN pg_catalog.pg_namespace tn ON tn.oid=t.typnamespace LEFT JOIN pg_catalog.pg_collation co ON co.oid=a.attcollation LEFT JOIN pg_catalog.pg_namespace cn ON cn.oid=co.collnamespace LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE a.attrelid=$1 AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum",&[&oid]).await?;
    Ok(rows
        .into_iter()
        .map(|r| {
            (
                r.get(0),
                serde_json::from_str(&r.get::<_, String>(1)).expect("PG JSON"),
            )
        })
        .collect())
}
async fn constraints<C: GenericClient + Sync>(
    client: &C,
    oid: u32,
    kind: &str,
) -> Result<Vec<String>, Error> {
    Ok(client.query("SELECT pg_get_constraintdef(oid) FROM pg_catalog.pg_constraint WHERE conrelid=$1 AND contype::text=$2 AND convalidated AND NOT condeferrable ORDER BY 1",&[&oid,&kind]).await?.iter().map(|r|r.get(0)).collect())
}
async fn verify_in<C: GenericClient + Sync>(
    client: &C,
    schema: &MappedSchema,
) -> Result<Vec<Mismatch>, Error> {
    client
        .batch_execute(
            "SET LOCAL search_path = pg_catalog; SET LOCAL standard_conforming_strings = on",
        )
        .await?;
    let mut mismatches = Vec::new();
    for co in &schema.collations {
        let rows=client.query("SELECT c.collprovider::text,c.colllocale,c.collisdeterministic,c.collencoding FROM pg_catalog.pg_collation c JOIN pg_catalog.pg_namespace n ON n.oid=c.collnamespace WHERE n.nspname=$1 AND c.collname=$2",&[&"public",&co.name]).await?;
        if rows.len() != 1
            || rows[0].get::<_, String>(0) != "i"
            || rows[0].get::<_, Option<String>>(1).as_deref() != Some(&co.locale)
            || rows[0].get::<_, bool>(2) != co.deterministic
            || rows[0].get::<_, i32>(3) != -1
        {
            mismatches.push(Mismatch {
                object: qualified("public", &co.name),
                kind: MismatchKind::Collation,
            });
        }
    }
    // Missing or incompatible collations cannot canonicalize expected tables.
    if !mismatches.is_empty() {
        return Ok(mismatches);
    }
    for t in &schema.tables {
        let object = qualified(&t.schema, &t.name);
        let rows=client.query("SELECT c.oid,c.relkind::text FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=$2",&[&t.schema,&t.name]).await?;
        let mut add = |kind| {
            mismatches.push(Mismatch {
                object: object.clone(),
                kind,
            })
        };
        if rows.is_empty() {
            add(MismatchKind::MissingTable);
            continue;
        }
        if rows[0].get::<_, String>(1) != "r" {
            add(MismatchKind::TableKind);
            continue;
        }
        let actual: u32 = rows[0].get(0);
        let temp = format!("skedaddle_expected_{}", uuid::Uuid::new_v4().simple());
        client
            .batch_execute(&format!(
                "CREATE TEMP TABLE {} ({}) ON COMMIT DROP",
                ident(&temp),
                t.body
            ))
            .await?;
        let expected:u32=client.query_one("SELECT c.oid FROM pg_catalog.pg_class c WHERE c.relnamespace=pg_my_temp_schema() AND c.relname=$1",&[&temp]).await?.get(0);
        let want = columns(client, expected).await?;
        let have = columns(client, actual).await?;
        for (name, description) in &want {
            if have.get(name) != Some(description) {
                mismatches.push(Mismatch {
                    object: format!("{object}.{}", ident(name)),
                    kind: MismatchKind::Column,
                });
            }
        }
        for (name, description) in &have {
            if !want.contains_key(name)
                && description[3] == true
                && description[8].is_null()
                && description[4] == ""
                && description[5] == ""
            {
                mismatches.push(Mismatch {
                    object: format!("{object}.{}", ident(name)),
                    kind: MismatchKind::ExtraRequiredColumn,
                });
            }
        }
        for (kind, mismatch) in [("p", MismatchKind::PrimaryKey), ("c", MismatchKind::Check)] {
            if constraints(client, actual, kind).await?
                != constraints(client, expected, kind).await?
            {
                mismatches.push(Mismatch {
                    object: object.clone(),
                    kind: mismatch,
                });
            }
        }
        client
            .batch_execute(&format!("DROP TABLE pg_temp.{}", ident(&temp)))
            .await?;
    }
    Ok(mismatches)
}

#[cfg(test)]
#[path = "dest_schema_tests.rs"]
mod tests;
