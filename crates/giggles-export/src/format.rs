use super::*;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

#[derive(Serialize, Deserialize)]
#[serde(tag = "record", rename_all = "snake_case", deny_unknown_fields)]
enum Record {
    Header {
        format: u32,
        profile: String,
        tables: Vec<Table>,
    },
    Row {
        table: usize,
        values: Vec<Cell>,
    },
    Manifest {
        sha256: String,
        table_rows: Vec<usize>,
        rows: usize,
    },
}
pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
pub(crate) fn hash(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}
pub(crate) fn unhex(s: &str) -> Result<Vec<u8>> {
    if !s.len().is_multiple_of(2)
        || !s
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
    {
        return Err(invalid("hex"));
    }
    s.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|c| {
            let x = std::str::from_utf8(c).map_err(|_| invalid("hex"))?;
            u8::from_str_radix(x, 16).map_err(|_| invalid("hex"))
        })
        .collect()
}
fn identifier(s: &str) -> Result<()> {
    if s.is_empty()
        || s.len() > 63
        || !s.as_bytes()[0].is_ascii_lowercase()
        || !s
            .bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_')
    {
        return Err(invalid("identifier"));
    }
    if [
        "deliveries",
        "giggles",
        "system",
        "information_schema",
        "pg_catalog",
        "repair",
        "internal",
        "tasks",
        "operations",
        "approvals",
        "receipts",
    ]
    .contains(&s)
        || ["giggles_", "system_", "pg_", "repair_", "internal_"]
            .iter()
            .any(|p| s.starts_with(p))
    {
        return Err(invalid("reserved core identifier"));
    }
    Ok(())
}
fn user_table(s: &str) -> Result<()> {
    identifier(s)?;
    for prefix in [
        "deliveries",
        "giggles",
        "system",
        "information_schema",
        "pg",
        "repair",
        "internal",
        "task",
        "operation",
        "approval",
        "receipt",
        "ledger",
        "outbox",
        "protected",
        "consensus",
        "membership",
    ] {
        if s == prefix || s.starts_with(&format!("{prefix}_")) || s == format!("{prefix}s") {
            return Err(invalid("reserved application catalog"));
        }
    }
    Ok(())
}
pub(crate) fn schema(tables: &[Table], bounds: Bounds) -> Result<()> {
    bounds.check()?;
    if tables.len() > bounds.tables {
        return Err(Error::Limit);
    }
    let mut prev = None;
    for t in tables {
        user_table(&t.name)?;
        if prev.is_some_and(|p: &str| p >= t.name.as_str()) {
            return Err(invalid("table order/duplicate"));
        }
        prev = Some(t.name.as_str());
        if t.columns.is_empty()
            || t.columns.len() > relational::MAX_COLUMNS
            || t.primary_key.is_empty()
            || t.primary_key.len() > t.columns.len()
        {
            return Err(invalid("schema bounds"));
        }
        let mut names = BTreeSet::new();
        for c in &t.columns {
            identifier(&c.name)?;
            // CREATE TABLE interprets this leading token as a table constraint.
            // Reject it during archive preflight, before creating a destination.
            if c.name == "primary" {
                return Err(invalid("non-importable column name"));
            }
            if !names.insert(&c.name) {
                return Err(invalid("duplicate column"));
            }
        }
        let mut keys = BTreeSet::new();
        for &k in &t.primary_key {
            if k >= t.columns.len() || !keys.insert(k) || !t.columns[k].not_null {
                return Err(invalid("primary key"));
            }
        }
    }
    Ok(())
}
pub(crate) fn validate(a: &Archive, bounds: Bounds) -> Result<()> {
    schema(&a.tables, bounds)?;
    if a.rows.len() != a.tables.len() {
        return Err(invalid("table rows"));
    }
    let mut total = 0usize;
    for (t, rows) in a.tables.iter().zip(&a.rows) {
        total = total.checked_add(rows.len()).ok_or(Error::Limit)?;
        if rows.len() > bounds.rows_per_table || total > bounds.total_rows {
            return Err(Error::Limit);
        }
        let mut previous: Option<Vec<u8>> = None;
        for row in rows {
            if row.len() != t.columns.len() {
                return Err(invalid("row arity"));
            }
            let mut payload = 0usize;
            let mut values = Vec::new();
            for (cell, c) in row.iter().zip(&t.columns) {
                let v = cell.to_value()?;
                let valid = match (&v, &c.data_type) {
                    (Value::Null, _) => !c.not_null,
                    (Value::Int(n), DataType::SmallInt) => i16::try_from(*n).is_ok(),
                    (Value::Int(n), DataType::Integer) => i32::try_from(*n).is_ok(),
                    (Value::Int(_), DataType::BigInt)
                    | (Value::Bool(_), DataType::Boolean)
                    | (Value::Text(_), DataType::Text)
                    | (Value::Bytes(_), DataType::Bytes) => true,
                    _ => false,
                };
                if !valid {
                    return Err(invalid("type/null/range"));
                }
                payload = payload
                    .checked_add(match &v {
                        Value::Text(s) => s.len(),
                        Value::Bytes(b) => b.len(),
                        _ => 9,
                    })
                    .ok_or(Error::Limit)?;
                if payload > relational::MAX_ROW_BYTES {
                    return Err(Error::Limit);
                }
                values.push(v);
            }
            let key = relational::encode_key(
                &t.primary_key
                    .iter()
                    .map(|i| values[*i].clone())
                    .collect::<Vec<_>>(),
            )
            .map_err(CoreError::Sql)?;
            if previous.as_ref().is_some_and(|p| p >= &key) {
                return Err(invalid("duplicate/out-of-order key"));
            }
            previous = Some(key);
        }
    }
    Ok(())
}
fn line(record: &Record) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec(record).map_err(|_| invalid("JSON encode"))?;
    bytes.push(b'\n');
    Ok(bytes)
}
pub(crate) fn header_charge(tables: &[Table], b: Bounds) -> Result<usize> {
    let size = line(&Record::Header {
        format: 1,
        profile: "giggles-logical-v1".into(),
        tables: tables.to_vec(),
    })?
    .len();
    if size > b.line_bytes || size > b.file_bytes {
        return Err(Error::Limit);
    }
    Ok(size)
}
pub(crate) fn row_charge(table: usize, values: &[Cell], used: &mut usize, b: Bounds) -> Result<()> {
    let size = line(&Record::Row {
        table,
        values: values.to_vec(),
    })?
    .len();
    *used = used.checked_add(size).ok_or(Error::Limit)?;
    if size > b.line_bytes || *used > b.file_bytes {
        return Err(Error::Limit);
    }
    Ok(())
}
fn append(out: &mut Vec<u8>, r: &Record, b: Bounds) -> Result<()> {
    let bytes = line(r)?;
    if bytes.len() > b.line_bytes
        || out.len().checked_add(bytes.len()).ok_or(Error::Limit)? > b.file_bytes
    {
        return Err(Error::Limit);
    }
    out.extend(bytes);
    Ok(())
}
pub fn encode(a: &Archive, b: Bounds) -> Result<Vec<u8>> {
    validate(a, b)?;
    let mut out = Vec::new();
    append(
        &mut out,
        &Record::Header {
            format: 1,
            profile: "giggles-logical-v1".into(),
            tables: a.tables.clone(),
        },
        b,
    )?;
    for (table, rows) in a.rows.iter().enumerate() {
        for values in rows {
            append(
                &mut out,
                &Record::Row {
                    table,
                    values: values.clone(),
                },
                b,
            )?;
        }
    }
    let footer = Record::Manifest {
        sha256: hash(&out),
        table_rows: a.rows.iter().map(Vec::len).collect(),
        rows: a.rows.iter().map(Vec::len).sum(),
    };
    append(&mut out, &footer, b)?;
    Ok(out)
}
pub fn decode(bytes: &[u8], b: Bounds) -> Result<Archive> {
    b.check()?;
    if bytes.len() > b.file_bytes {
        return Err(Error::Limit);
    }
    if bytes.last() != Some(&b'\n') {
        return Err(invalid("missing final newline"));
    }
    let mut archive = None;
    let mut offset = 0usize;
    let mut footer = false;
    let mut previous_table = 0usize;
    let mut total = 0usize;
    for raw in bytes.split_inclusive(|c| *c == b'\n') {
        if raw.len() > b.line_bytes {
            return Err(Error::Limit);
        }
        if footer {
            return Err(invalid("trailing record"));
        }
        let r: Record = serde_json::from_slice(raw).map_err(|_| invalid("JSON record"))?;
        if line(&r)? != raw {
            return Err(invalid("noncanonical record"));
        }
        match r {
            Record::Header {
                format,
                profile,
                tables,
            } => {
                if archive.is_some()
                    || offset != 0
                    || format != 1
                    || profile != "giggles-logical-v1"
                {
                    return Err(invalid("header/version/profile"));
                }
                schema(&tables, b)?;
                let rows = vec![Vec::new(); tables.len()];
                archive = Some(Archive { tables, rows });
            }
            Record::Row { table, values } => {
                let a = archive.as_mut().ok_or_else(|| invalid("missing header"))?;
                if table < previous_table || table >= a.tables.len() {
                    return Err(invalid("row table order"));
                }
                previous_table = table;
                total = total.checked_add(1).ok_or(Error::Limit)?;
                if total > b.total_rows || a.rows[table].len() >= b.rows_per_table {
                    return Err(Error::Limit);
                }
                a.rows[table].push(values);
            }
            Record::Manifest {
                sha256,
                table_rows,
                rows,
            } => {
                let a = archive.as_ref().ok_or_else(|| invalid("missing header"))?;
                if sha256 != hash(&bytes[..offset])
                    || table_rows != a.rows.iter().map(Vec::len).collect::<Vec<_>>()
                    || rows != total
                {
                    return Err(Error::Integrity);
                }
                footer = true;
            }
        }
        offset += raw.len();
    }
    if !footer {
        return Err(invalid("missing manifest"));
    }
    let a = archive.ok_or_else(|| invalid("missing header"))?;
    validate(&a, b)?;
    Ok(a)
}
pub(crate) fn create_sql(t: &Table) -> String {
    let mut parts = t
        .columns
        .iter()
        .map(|c| {
            format!(
                "{} {}{}",
                c.name,
                match c.data_type {
                    DataType::Boolean => "BOOLEAN",
                    DataType::SmallInt => "SMALLINT",
                    DataType::Integer => "INTEGER",
                    DataType::BigInt => "BIGINT",
                    DataType::Text => "TEXT",
                    DataType::Bytes => "BYTEA",
                },
                if c.not_null { " NOT NULL" } else { "" }
            )
        })
        .collect::<Vec<_>>();
    parts.push(format!(
        "PRIMARY KEY ({})",
        t.primary_key
            .iter()
            .map(|i| t.columns[*i].name.as_str())
            .collect::<Vec<_>>()
            .join(",")
    ));
    format!("CREATE TABLE {} ({})", t.name, parts.join(","))
}
