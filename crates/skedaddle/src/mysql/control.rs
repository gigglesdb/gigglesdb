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
            column_default: None,
            default_generated: false,
            generation_expression: None,
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

/// Fixed M10 replay-table contracts. Capture owns these independently of selection.
pub fn apply_descriptors() -> Vec<TableDescriptor> {
    let specs = [
        (
            "apply_receipts",
            vec![
                ("stream", "varbinary", "varbinary(32)"),
                ("position_hash", "binary", "binary(32)"),
                ("position_domain", "varbinary", "varbinary(1024)"),
                ("position", "longblob", "longblob"),
                ("txn_id", "longblob", "longblob"),
                ("digest", "binary", "binary(32)"),
                ("committed_at", "timestamp", "timestamp(6)"),
            ],
            vec!["stream", "position_hash"],
        ),
        (
            "apply_progress",
            vec![
                ("stream", "varbinary", "varbinary(32)"),
                ("position_domain", "varbinary", "varbinary(1024)"),
                ("last_position", "longblob", "longblob"),
            ],
            vec!["stream"],
        ),
    ];
    specs
        .into_iter()
        .map(|(table, columns, key)| TableDescriptor {
            schema: "_skedaddle".into(),
            table: table.into(),
            columns: columns
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
                    column_default: None,
                    default_generated: false,
                    generation_expression: None,
                })
                .collect(),
            primary_key: key.into_iter().map(String::from).collect(),
        })
        .collect()
}

/// Validate every control image before deciding whether to suppress a transaction.
/// Receipt inserts mark apply; receipt deletes and progress writes are GC/progress.
pub fn decode_apply(row: &ControlRow) -> Result<Option<bool>> {
    let Some(d) = apply_descriptors()
        .into_iter()
        .find(|d| row.table == format!("{}.{}", d.schema, d.table))
    else {
        return Ok(None);
    };
    let receipt = d.table == "apply_receipts";
    let images = (row.before.is_some(), row.after.is_some());
    if images
        != match row.kind {
            EventKind::Insert => (false, true),
            EventKind::Update if !receipt => (true, true),
            EventKind::Delete if receipt => (true, false),
            _ => return Err(MysqlError::Control("invalid apply control operation")),
        }
    {
        return Err(MysqlError::Control("invalid apply control images"));
    }
    for image in row.before.iter().chain(row.after.iter()) {
        if image.len() != d.columns.len() {
            return Err(MysqlError::Control("invalid apply control columns"));
        }
        for c in &d.columns {
            let value = image
                .get(&c.name)
                .ok_or(MysqlError::Control("missing apply control column"))?;
            let decoded = from_opaque(c, value)
                .map_err(|_| MysqlError::Control("invalid apply control value"))?;
            let valid = match decoded {
                MysqlValue::Binary(bytes) => match c.name.as_str() {
                    "stream" | "position_hash" | "digest" => bytes.len() == 32,
                    "position_domain" => !bytes.is_empty() && bytes.len() <= 1024,
                    _ => !bytes.is_empty(),
                },
                MysqlValue::Timestamp(_) => c.name == "committed_at",
                _ => false,
            };
            if !valid {
                return Err(MysqlError::Control("invalid apply control value"));
            }
        }
    }
    Ok(Some(receipt && row.kind == EventKind::Insert))
}
