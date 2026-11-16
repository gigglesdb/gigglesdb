//! Selected-table metadata. MissingPrimaryKey is an M6 preflight blocker.
use super::client::{MysqlError, Result};
use mysql_async::{Conn, prelude::Queryable};
use serde::{Deserialize, Serialize};

/// Keep the numeric wire identity as well as the information_schema name so
/// opaque text can be inverted without assuming a server's default collation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Collation {
    pub name: String,
    pub id: u16,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDescriptor {
    pub name: String,
    /// MySQL's one-based ORDINAL_POSITION.
    pub ordinal: u64,
    pub data_type: String,
    pub column_type: String,
    pub character_set: Option<String>,
    pub collation: Option<Collation>,
    pub nullable: bool,
    pub generated: bool,
    pub column_default: Option<String>,
    /// EXTRA contains DEFAULT_GENERATED: the default is an expression, not a literal.
    pub default_generated: bool,
    pub generation_expression: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableDescriptor {
    pub schema: String,
    pub table: String,
    pub columns: Vec<ColumnDescriptor>,
    pub primary_key: Vec<String>,
}
pub async fn load_descriptors(
    conn: &mut Conn,
    tables: &[(String, String)],
) -> Result<Vec<TableDescriptor>> {
    let mut descriptors = Vec::new();
    for (schema, table) in tables {
        let rows: Vec<mysql_async::Row> = conn
            .exec(
                "SELECT c.COLUMN_NAME,c.ORDINAL_POSITION,c.DATA_TYPE,c.COLUMN_TYPE,\
             c.CHARACTER_SET_NAME,c.COLLATION_NAME,c.IS_NULLABLE,c.EXTRA,k.ID,c.COLUMN_DEFAULT,c.GENERATION_EXPRESSION \
             FROM information_schema.COLUMNS c LEFT JOIN information_schema.COLLATIONS k \
             ON c.COLLATION_NAME=k.COLLATION_NAME \
             WHERE c.TABLE_SCHEMA=? AND c.TABLE_NAME=? ORDER BY c.ORDINAL_POSITION",
                (schema, table),
            )
            .await?;
        let mut columns = Vec::new();
        for row in rows {
            type Fields = (
                String,
                u64,
                String,
                String,
                Option<String>,
                Option<String>,
                String,
                String,
                Option<u16>,
                Option<String>,
                Option<String>,
            );
            let (
                name,
                ordinal,
                data_type,
                column_type,
                character_set,
                collation,
                nullable,
                extra,
                id,
                column_default,
                generation_expression,
            ): Fields = mysql_async::from_row_opt(row)
                .map_err(|_| MysqlError::Invalid("column descriptor"))?;
            let collation = match (collation, id) {
                (Some(name), Some(id)) => Some(Collation { name, id }),
                (None, None) => None,
                _ => return Err(MysqlError::Invalid("collation descriptor")),
            };
            columns.push(ColumnDescriptor {
                name,
                ordinal,
                data_type,
                column_type,
                character_set,
                collation,
                nullable: nullable == "YES",
                column_default,
                default_generated: extra.contains("DEFAULT_GENERATED"),
                generation_expression: generation_expression.filter(|s| !s.is_empty()),
                generated: extra.contains("VIRTUAL GENERATED")
                    || extra.contains("STORED GENERATED"),
            });
        }
        if columns.is_empty() {
            return Err(MysqlError::Invalid("selected table not found"));
        }
        let primary_key: Vec<String> = conn
            .exec(
                "SELECT COLUMN_NAME FROM information_schema.STATISTICS \
             WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND INDEX_NAME='PRIMARY' ORDER BY SEQ_IN_INDEX",
                (schema, table),
            )
            .await?;
        if primary_key.is_empty() {
            return Err(MysqlError::MissingPrimaryKey {
                schema: schema.clone(),
                table: table.clone(),
            });
        }
        descriptors.push(TableDescriptor {
            schema: schema.clone(),
            table: table.clone(),
            columns,
            primary_key,
        });
    }
    Ok(descriptors)
}
/// Difference from the start descriptor; ordinals are MySQL's one-based ordinals.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TableMapChange {
    ColumnCount {
        expected: usize,
        actual: usize,
    },
    Column {
        ordinal: u64,
        name: String,
        field: &'static str,
    },
}

/// Compare the wire schema to the start descriptor, never to a previous map.
/// FULL optional metadata (names, signedness, collations and labels) participates.
pub fn compare_table_map(
    descriptor: &TableDescriptor,
    map: &super::binlog::MysqlTable,
) -> std::result::Result<(), TableMapChange> {
    if descriptor.columns.len() != map.columns.len() {
        return Err(TableMapChange::ColumnCount {
            expected: descriptor.columns.len(),
            actual: map.columns.len(),
        });
    }
    for (column, wire) in descriptor.columns.iter().zip(&map.columns) {
        let error = |field| TableMapChange::Column {
            ordinal: column.ordinal,
            name: column.name.clone(),
            field,
        };
        let (ty, metadata) =
            wire_shape(column).ok_or_else(|| error("unsupported descriptor metadata"))?;
        for (matches, field) in [
            (wire.column_type == ty, "column type"),
            (wire.metadata == metadata, "type metadata"),
            (wire.name == column.name.as_bytes(), "column name"),
            (wire.nullable == column.nullable, "nullability"),
            (
                wire.collation
                    == column.collation.as_ref().map(|c| c.id).or_else(|| {
                        matches!(
                            column.data_type.as_str(),
                            "binary"
                                | "varbinary"
                                | "tinyblob"
                                | "blob"
                                | "mediumblob"
                                | "longblob"
                        )
                        .then_some(63)
                    }),
                "collation",
            ),
        ] {
            if !matches {
                return Err(error(field));
            }
        }
        let numeric =
            mysql_common::constants::ColumnType::try_from(ty).is_ok_and(|ty| ty.is_numeric_type());
        // YEAR is unsigned on the wire despite COLUMN_TYPE being just "year".
        let unsigned = numeric.then(|| {
            column.data_type == "year"
                || column
                    .column_type
                    .split_whitespace()
                    .any(|s| s.eq_ignore_ascii_case("unsigned"))
        });
        if wire.unsigned != unsigned {
            return Err(error("signedness"));
        }
        if matches!(column.data_type.as_str(), "enum" | "set")
            && wire.labels.as_ref() != descriptor_labels(&column.column_type).as_ref()
        {
            return Err(error("enum/set labels"));
        }
    }
    Ok(())
}

fn wire_shape(c: &ColumnDescriptor) -> Option<(u8, Vec<u8>)> {
    use mysql_common::constants::ColumnType::*;
    let args = || -> Option<Vec<u16>> {
        c.column_type
            .split_once('(')?
            .1
            .split_once(')')?
            .0
            .split(',')
            .map(|s| s.trim().parse().ok())
            .collect()
    };
    let length = || -> Option<u16> {
        let n = *args()?.first()?;
        let width = match c.character_set.as_deref() {
            Some("utf8mb4") => 4,
            Some("utf8" | "utf8mb3") => 3,
            None | Some("ascii" | "binary" | "latin1") => 1,
            _ => return None,
        };
        n.checked_mul(width)
    };
    let mut meta = Vec::new();
    let ty = match c.data_type.as_str() {
        "tinyint" => MYSQL_TYPE_TINY,
        "smallint" => MYSQL_TYPE_SHORT,
        "mediumint" => MYSQL_TYPE_INT24,
        "int" | "integer" => MYSQL_TYPE_LONG,
        "bigint" => MYSQL_TYPE_LONGLONG,
        "float" => {
            meta.push(4);
            MYSQL_TYPE_FLOAT
        }
        "double" => {
            meta.push(8);
            MYSQL_TYPE_DOUBLE
        }
        "decimal" | "numeric" => {
            let a = args()?;
            meta = vec![
                u8::try_from(*a.first()?).ok()?,
                u8::try_from(*a.get(1).unwrap_or(&0)).ok()?,
            ];
            MYSQL_TYPE_NEWDECIMAL
        }
        "bit" => {
            let n = *args()?.first()?;
            meta = vec![(n % 8) as u8, (n / 8) as u8];
            MYSQL_TYPE_BIT
        }
        "date" => MYSQL_TYPE_NEWDATE,
        "year" => MYSQL_TYPE_YEAR,
        "datetime" | "timestamp" | "time" => {
            meta.push(u8::try_from(*args().unwrap_or_default().first().unwrap_or(&0)).ok()?);
            match c.data_type.as_str() {
                "datetime" => MYSQL_TYPE_DATETIME2,
                "timestamp" => MYSQL_TYPE_TIMESTAMP2,
                _ => MYSQL_TYPE_TIME2,
            }
        }
        "varchar" | "varbinary" => {
            meta = length()?.to_le_bytes().to_vec();
            MYSQL_TYPE_VARCHAR
        }
        "char" | "binary" => {
            let n = length()?;
            if n > 1023 {
                return None;
            }
            meta = vec![(MYSQL_TYPE_STRING as u8) ^ (((n >> 8) as u8) << 4), n as u8];
            MYSQL_TYPE_STRING
        }
        "tinytext" | "tinyblob" => {
            meta.push(1);
            MYSQL_TYPE_BLOB
        }
        "text" | "blob" => {
            meta.push(2);
            MYSQL_TYPE_BLOB
        }
        "mediumtext" | "mediumblob" => {
            meta.push(3);
            MYSQL_TYPE_BLOB
        }
        "longtext" | "longblob" => {
            meta.push(4);
            MYSQL_TYPE_BLOB
        }
        "json" => {
            meta.push(4);
            MYSQL_TYPE_JSON
        }
        "enum" | "set" => {
            let n = descriptor_labels(&c.column_type)?.len();
            let (ty, size) = if c.data_type == "enum" {
                (MYSQL_TYPE_ENUM, if n > 255 { 2 } else { 1 })
            } else {
                (MYSQL_TYPE_SET, n.div_ceil(8) as u8)
            };
            meta = vec![ty as u8, size];
            ty
        }
        _ => return None,
    };
    Some((ty as u8, meta))
}

fn descriptor_labels(definition: &str) -> Option<Vec<Vec<u8>>> {
    let mut chars = definition
        .split_once('(')?
        .1
        .strip_suffix(')')?
        .chars()
        .peekable();
    let mut labels = Vec::new();
    loop {
        if chars.next()? != '\'' {
            return None;
        }
        let mut label = String::new();
        loop {
            match chars.next()? {
                '\'' if chars.peek() == Some(&'\'') => {
                    chars.next();
                    label.push('\'');
                }
                '\'' => break,
                '\\' => label.push(match chars.next()? {
                    '0' => '\0',
                    'n' => '\n',
                    'r' => '\r',
                    't' => '\t',
                    'b' => '\u{8}',
                    'Z' => '\u{1a}',
                    c => c,
                }),
                c => label.push(c),
            }
        }
        labels.push(label.into_bytes());
        match chars.next() {
            None => return Some(labels),
            Some(',') => {}
            _ => return None,
        }
    }
}
