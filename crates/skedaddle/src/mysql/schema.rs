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
             c.CHARACTER_SET_NAME,c.COLLATION_NAME,c.IS_NULLABLE,c.EXTRA,k.ID \
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
