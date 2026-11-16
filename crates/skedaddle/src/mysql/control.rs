//! Transactional, insert-only chunk markers. M5b-2 supplies the writer side.
use super::{
    assembly::ControlRow,
    client::{MysqlError, Result},
    schema::{ColumnDescriptor, TableDescriptor},
    value::from_opaque,
};
use crate::{event::EventKind, mysql::binlog::MysqlValue, pg::chunk::Watermark};

pub const WATERMARK_DDL: &str = "CREATE TABLE IF NOT EXISTS _skedaddle.watermark (\
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, marker JSON NOT NULL\
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

/// Explicit installation; existing incompatible tables are never repaired silently.
pub async fn install_watermark_table(conn: &mut mysql_async::Conn) -> Result<()> {
    use mysql_async::prelude::Queryable;
    conn.query_drop("CREATE DATABASE IF NOT EXISTS _skedaddle")
        .await?;
    conn.query_drop(WATERMARK_DDL).await?;
    let engine: Option<String> = conn
        .exec_first(
            "SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?",
            ("_skedaddle", "watermark"),
        )
        .await?;
    let tables =
        super::schema::load_descriptors(conn, &[("_skedaddle".into(), "watermark".into())]).await?;
    let actual = &tables[0];
    let expected = descriptor();
    if engine.as_deref() != Some("InnoDB")
        || actual.primary_key != expected.primary_key
        || actual.columns.len() != expected.columns.len()
        || actual.columns.iter().zip(&expected.columns).any(|(a, e)| {
            a.name != e.name
                || a.ordinal != e.ordinal
                || a.data_type != e.data_type
                || a.column_type != e.column_type
                || a.nullable
                || a.generated
        })
    {
        return Err(MysqlError::Control("watermark table schema mismatch"));
    }
    let extra: Option<String> = conn.exec_first(
        "SELECT EXTRA FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME='id'",
        ("_skedaddle", "watermark"),
    ).await?;
    if extra.as_deref() != Some("auto_increment") {
        return Err(MysqlError::Control("watermark id must auto increment"));
    }
    Ok(())
}

/// The caller supplies a dedicated idle autocommit connection, outside snapshot reads.
pub async fn emit_watermark(conn: &mut mysql_async::Conn, marker: &Watermark) -> Result<()> {
    use mysql_async::prelude::Queryable;
    let payload = marker
        .encode()
        .map_err(|_| MysqlError::Control("invalid watermark identity"))?;
    let state: Option<u8> = conn.query_first("SELECT @@autocommit").await?;
    if state != Some(1) {
        return Err(MysqlError::Control(
            "watermark needs idle autocommit connection",
        ));
    }
    conn.exec_drop(
        "INSERT INTO _skedaddle.watermark(marker) VALUES(?)",
        (payload,),
    )
    .await?;
    Ok(())
}

pub fn descriptor() -> TableDescriptor {
    TableDescriptor {
        schema: "_skedaddle".into(),
        table: "watermark".into(),
        columns: [
            ("id", "bigint", "bigint unsigned"),
            ("marker", "json", "json"),
        ]
        .into_iter()
        .enumerate()
        .map(|(i, (name, data_type, column_type))| ColumnDescriptor {
            name: name.into(),
            ordinal: i as u64 + 1,
            data_type: data_type.into(),
            column_type: column_type.into(),
            character_set: None,
            collation: None,
            nullable: false,
            generated: false,
        })
        .collect(),
        primary_key: vec!["id".into()],
    }
}

/// Non-control data is not a marker. Every unknown control route fails closed.
pub fn decode_watermark(row: &ControlRow) -> Result<Option<Watermark>> {
    if !row.table.starts_with("_skedaddle.") {
        return Ok(None);
    }
    if row.table != "_skedaddle.watermark" {
        return Err(MysqlError::Control("unknown control table"));
    }
    if row.kind != EventKind::Insert || row.before.is_some() {
        return Err(MysqlError::Control("watermarks require inserts"));
    }
    let after = row
        .after
        .as_ref()
        .ok_or(MysqlError::Control("missing watermark row"))?;
    let d = descriptor();
    if after.len() != 2 || !after.contains_key("id") {
        return Err(MysqlError::Control("invalid watermark columns"));
    }
    if !matches!(
        from_opaque(&d.columns[0], &after["id"])?,
        MysqlValue::Unsigned(1..)
    ) {
        return Err(MysqlError::Control("invalid watermark id"));
    }
    let value = after
        .get("marker")
        .ok_or(MysqlError::Control("missing watermark payload"))?;
    let MysqlValue::Json(payload) = from_opaque(&d.columns[1], value)? else {
        return Err(MysqlError::Control("watermark payload must be JSON"));
    };
    let bytes =
        serde_json::to_vec(&payload).map_err(|_| MysqlError::Control("invalid watermark JSON"))?;
    if bytes.len() > 64 * 1024 {
        return Err(MysqlError::Control("watermark too large"));
    }
    let marker: Watermark = serde_json::from_slice(&bytes)
        .map_err(|_| MysqlError::Control("malformed watermark payload"))?;
    marker
        .encode()
        .map_err(|_| MysqlError::Control("invalid watermark identity"))?;
    Ok(Some(marker))
}
