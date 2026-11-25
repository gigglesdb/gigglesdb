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
pub(crate) fn profile(tables: &[Table]) -> (u32, &'static str) {
    if tables.iter().flat_map(|t| &t.columns).any(|c| {
        matches!(
            c.data_type,
            DataType::Date
                | DataType::Time
                | DataType::TimeTz
                | DataType::Timestamp
                | DataType::TimestampTz
                | DataType::Interval
        )
    }) {
        return (3, "giggles-logical-v3-temporal");
    }
    if tables.iter().flat_map(|t| &t.columns).any(|c| {
        matches!(
            c.data_type,
            DataType::Numeric | DataType::Real | DataType::DoublePrecision
        )
    }) {
        (2, "giggles-logical-v2-numeric")
    } else {
        (1, "giggles-logical-v1")
    }
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
            || t.columns.len() > relational::MAX_TABLE_COLUMNS
            || t.primary_key.is_empty()
            || t.primary_key.len() > relational::MAX_COLUMNS
            || t.primary_key.len() > t.columns.len()
        {
            return Err(invalid("schema bounds"));
        }
        let mut names = BTreeSet::new();
        for c in &t.columns {
            identifier(&c.name)?;
            // The archive must preserve logical type identity, including for
            // empty and all-NULL tables.
            if matches!(
                c.data_type,
                DataType::Uuid
                    | DataType::Jsonb
                    | DataType::IntArray
                    | DataType::BigIntArray
                    | DataType::TextArray
                    | DataType::UuidArray
                    | DataType::RegType
                    | DataType::ExpressionArray(_)
            ) || c.data_type.text_search_oid().is_some() {
                return Err(invalid(
                    "logical type is unsupported by the archive profile",
                ));
            }
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
                    | (Value::Text(_), DataType::Text | DataType::Varchar | DataType::Bpchar)
                    | (Value::Bytes(_), DataType::Bytes)
                    | (Value::Numeric(_), DataType::Numeric)
                    | (Value::Float4(_), DataType::Real)
                    | (Value::Float8(_), DataType::DoublePrecision)
                    | (Value::Date(_), DataType::Date)
                    | (Value::Time(_), DataType::Time)
                    | (Value::TimeTz(_), DataType::TimeTz)
                    | (Value::Timestamp(_), DataType::Timestamp)
                    | (Value::TimestampTz(_), DataType::TimestampTz)
                    | (Value::Interval(_), DataType::Interval) => true,
                    _ => false,
                };
                if !valid {
                    return Err(invalid("type/null/range"));
                }
                payload = payload
                    .checked_add(match &v {
                        Value::Text(s) => s.len(),
                        Value::Bytes(b) => b.len(),
                        Value::Numeric(n) => n.to_string().len(),
                        Value::Date(_) => 4,
                        Value::Time(_) => 8,
                        Value::TimeTz(_) => 12,
                        Value::Timestamp(_) => 8,
                        Value::TimestampTz(_) => 8,
                        Value::Interval(_) => 16,
                        Value::Float4(_) => 4,
                        Value::Float8(_) => 8,
                        Value::Null | Value::Bool(_) | Value::Int(_) => 9,
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
        format: profile(tables).0,
        profile: profile(tables).1.into(),
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
            format: profile(&a.tables).0,
            profile: profile(&a.tables).1.into(),
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
    let mut typed_profile = false;
    let mut temporal_profile = false;
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
        // Gate the header before deserializing schemas with extended enum tags.
        if offset == 0 {
            let header: serde_json::Value =
                serde_json::from_slice(raw).map_err(|_| invalid("JSON header"))?;
            let pair = (header["format"].as_u64(), header["profile"].as_str());
            if !matches!(
                pair,
                (Some(1), Some("giggles-logical-v1"))
                    | (Some(2), Some("giggles-logical-v2-numeric"))
                    | (Some(3), Some("giggles-logical-v3-temporal"))
            ) {
                return Err(invalid("header/version/profile"));
            }
            typed_profile = matches!(pair.0, Some(2 | 3));
            temporal_profile = pair.0 == Some(3);
            if !temporal_profile
                && header["tables"].as_array().is_some_and(|ts| {
                    ts.iter().any(|t| {
                        t["columns"].as_array().is_some_and(|cs| {
                            cs.iter().any(|c| {
                                matches!(
                                    c["data_type"].as_str(),
                                    Some(
                                        "Date"
                                            | "Time"
                                            | "TimeTz"
                                            | "Timestamp"
                                            | "TimestampTz"
                                            | "Interval"
                                    )
                                )
                            })
                        })
                    })
                })
            {
                return Err(invalid("temporal schema requires v3"));
            }
            if pair.0 == Some(1)
                && header["tables"].as_array().is_some_and(|ts| {
                    ts.iter().any(|t| {
                        t["columns"].as_array().is_some_and(|cs| {
                            cs.iter().any(|c| {
                                matches!(
                                    c["data_type"].as_str(),
                                    Some("Numeric" | "Real" | "DoublePrecision")
                                )
                            })
                        })
                    })
                })
            {
                return Err(invalid("typed schema requires v2"));
            }
        }
        if offset != 0 && !temporal_profile {
            let record: serde_json::Value =
                serde_json::from_slice(raw).map_err(|_| invalid("JSON record"))?;
            if record["values"].as_array().is_some_and(|vs| {
                vs.iter().any(|v| {
                    matches!(
                        v["type"].as_str(),
                        Some(
                            "date" | "time" | "time_tz" | "timestamp" | "timestamp_tz" | "interval"
                        )
                    )
                })
            }) {
                return Err(invalid("temporal cells require v3"));
            }
        }
        if offset != 0 && !typed_profile {
            let record: serde_json::Value =
                serde_json::from_slice(raw).map_err(|_| invalid("JSON record"))?;
            if record["values"].as_array().is_some_and(|vs| {
                vs.iter()
                    .any(|v| matches!(v["type"].as_str(), Some("numeric" | "float4" | "float8")))
            }) {
                return Err(invalid("typed cells require v2"));
            }
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
                    || (format, profile.as_str()) != self::profile(&tables)
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
                    DataType::Date => "DATE",
                    DataType::Time => "TIME",
                    DataType::TimeTz => "TIMETZ",
                    DataType::Timestamp => "TIMESTAMP",
                    DataType::TimestampTz => "TIMESTAMPTZ",
                    DataType::Interval => "INTERVAL",
                    DataType::Boolean => "BOOLEAN",
                    DataType::SmallInt => "SMALLINT",
                    DataType::Integer => "INTEGER",
                    DataType::BigInt => "BIGINT",
                    DataType::Text | DataType::Varchar | DataType::Bpchar => "TEXT",
                    DataType::Bytes => "BYTEA",
                    DataType::Numeric => "NUMERIC",
                    DataType::Real => "REAL",
                    DataType::DoublePrecision => "DOUBLE PRECISION",
                    DataType::Uuid => "UUID",
                    DataType::Jsonb => "JSONB",
                    DataType::IntArray => "INTEGER[]",
                    DataType::BigIntArray => "BIGINT[]",
                    DataType::TextArray => "TEXT[]",
                    DataType::UuidArray => "UUID[]",
                    DataType::RegType => "REGTYPE",
                    DataType::ExpressionArray(array) => array.metadata().2,
                    DataType::TsVector => "TSVECTOR",
                    DataType::TsQuery => "TSQUERY",
                    DataType::RegConfig => "REGCONFIG",
                    DataType::RegDictionary => "REGDICTIONARY",
                    DataType::Oid => "OID",
                    DataType::PgChar => "\"char\"",
                    DataType::RealArray => "REAL[]",
                    DataType::PgCharArray => "\"char\"[]",
                    DataType::SmallIntArray => "SMALLINT[]",
                    DataType::RegDictionaryArray => "REGDICTIONARY[]",
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logical_types_create_sql_roundtrip_and_archive_refusal() {
        let path = std::env::temp_dir().join(format!(
            "giggles-export-logical-types-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let admin = Admin::create(&path, Limits::default()).unwrap();
        for (kind, sql_type, text) in [
            (
                DataType::Uuid,
                "UUID",
                "550e8400-e29b-41d4-a716-446655440000",
            ),
            (DataType::Jsonb, "JSONB", r#"{"nested":[1,null,"snow"]}"#),
            (DataType::IntArray, "INTEGER[]", "{1,NULL,-2}"),
            (
                DataType::BigIntArray,
                "BIGINT[]",
                "{-9223372036854775808,9223372036854775807}",
            ),
            (DataType::TextArray, "TEXT[]", r#"{"a,b","NULL",NULL,""}"#),
            (DataType::UuidArray, "UUID[]", "{NULL,12345678-1234-5678-1234-567812345678}"),
            (DataType::TsVector, "TSVECTOR", "cat:1 dog:2"),
            (DataType::TsQuery, "TSQUERY", "cat & dog"),
        ] {
            let table = Table {
                name: "restored".into(),
                columns: vec![
                    Column {
                        name: "id".into(),
                        data_type: DataType::Integer,
                        not_null: true,
                    },
                    Column {
                        name: "v".into(),
                        data_type: kind.clone(),
                        not_null: false,
                    },
                ],
                primary_key: vec![0],
            };
            let sql = create_sql(&table);
            assert_eq!(
                sql,
                format!(
                    "CREATE TABLE restored (id INTEGER NOT NULL,v {sql_type},PRIMARY KEY (id))"
                )
            );
            let mut session = admin.sql_session();
            session
                .execute(
                    &format!("CREATE TABLE original (id INTEGER PRIMARY KEY,v {sql_type})"),
                    &[],
                    None,
                )
                .unwrap();
            session.execute(&sql, &[], None).unwrap();
            for (id, value) in [Value::Text(text.into()), Value::Null]
                .into_iter()
                .enumerate()
            {
                session
                    .execute(
                        "INSERT INTO original VALUES ($1,$2)",
                        &[Value::Int(id as i64), value],
                        None,
                    )
                    .unwrap();
            }
            let original = session
                .execute("SELECT id,v FROM original ORDER BY id", &[], None)
                .unwrap();
            for row in &original.rows {
                session
                    .execute("INSERT INTO restored VALUES ($1,$2)", &row.values, None)
                    .unwrap();
            }
            let restored = session
                .execute("SELECT id,v FROM restored ORDER BY id", &[], None)
                .unwrap();
            assert_eq!(
                original.rows.iter().map(|r| &r.values).collect::<Vec<_>>(),
                restored.rows.iter().map(|r| &r.values).collect::<Vec<_>>()
            );
            assert!(
                admin
                    .catalog()
                    .unwrap()
                    .1
                    .iter()
                    .all(|t| t.columns[1].data_type == kind)
            );
            assert!(matches!(
                capture(&admin, Bounds::default()),
                Err(Error::Core(CoreError::Unsupported))
            ));
            let output = path.join("copy.jsonl");
            assert!(matches!(
                export(&admin, &output, Bounds::default()),
                Err(Error::Core(CoreError::Unsupported))
            ));
            assert!(!output.exists());
            // Preflight must also reject caller-built archives before serde can
            // erase the type, including empty and all-NULL data.
            for rows in [
                vec![],
                vec![vec![Cell::Int("1".into()), Cell::Null]],
                vec![vec![Cell::Int("1".into()), Cell::Text(text.into())]],
            ] {
                let archive = Archive {
                    tables: vec![table.clone()],
                    rows: vec![rows],
                };
                assert!(
                    matches!(encode(&archive, Bounds::default()), Err(Error::Invalid(message)) if message.contains("logical type is unsupported"))
                );
            }
            session.execute("DROP TABLE original", &[], None).unwrap();
            session.execute("DROP TABLE restored", &[], None).unwrap();
        }
        drop(admin);
        std::fs::remove_dir_all(path).unwrap();
    }
}
