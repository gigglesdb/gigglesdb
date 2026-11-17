//! Checked MySQL -> PostgreSQL mapping. Typmods participate in the PG source's
//! skva.v1 identity. Generated columns must be recomputed by the destination.
//! BINARY padding is retained verbatim. JSONB normalizes keys and numeric spelling;
//! serde_json's existing number representation bounds the MySQL-side precision.
use super::{Classification, Jsonb, Mapping, MappingError, Numeric, PgType, Value};
use crate::{
    event::OpaqueValue,
    mysql::{binlog::MysqlValue as V, schema::ColumnDescriptor, value as wire},
};
use sha2::{Digest, Sha256};
type Result<T> = std::result::Result<T, MappingError>;
fn bad() -> MappingError {
    MappingError::OutOfDomain("MySQL column domain".into())
}
fn wire_error(e: impl std::fmt::Display) -> MappingError {
    MappingError::OutOfDomain(e.to_string())
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CollationRequirement {
    pub name: String,
    pub locale: String,
    pub deterministic: bool,
    /// Nondeterministic collations restrict LIKE and pattern operator indexes.
    pub pattern_operations_restricted: bool,
}
#[derive(Clone, Debug)]
pub struct MysqlPgColumn {
    pub source: ColumnDescriptor,
    pub pg_type: PgType,
    pub ddl_type: String,
    pub classification: Classification,
    pub supported_with_transformation: bool,
    pub collation: Option<CollationRequirement>,
    /// Column-local CHECK expression, including the quoted source column name.
    pub check: Option<String>,
    pub type_modifier: i32,
    pub type_id: String,
    pub generated: bool,
    pub(crate) args: Vec<u32>,
    pub(crate) labels: Vec<Vec<u8>>,
}
fn quote(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}
fn mysql_id(c: &ColumnDescriptor) -> String {
    let mut id = format!("mysql.v1:{}", c.column_type.to_lowercase());
    if let (Some(cs), Some(co)) = (&c.character_set, &c.collation) {
        id.push_str(&format!(
            ";cs={};co={}",
            cs.to_lowercase(),
            co.name.to_lowercase()
        ));
    }
    id
}
fn definition(c: &ColumnDescriptor) -> Result<Vec<Vec<u8>>> {
    let s = c
        .column_type
        .split_once('(')
        .and_then(|(_, s)| s.strip_suffix(')'))
        .ok_or_else(bad)?;
    let mut chars = s.chars().peekable();
    let mut out = Vec::new();
    loop {
        if chars.next() != Some('\'') {
            return Err(bad());
        }
        let mut label = String::new();
        loop {
            match chars.next().ok_or_else(bad)? {
                '\'' if chars.peek() == Some(&'\'') => {
                    chars.next();
                    label.push('\'');
                }
                '\'' => break,
                '\\' => label.push(match chars.next().ok_or_else(bad)? {
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
        if label.contains('\0') || out.contains(&label.as_bytes().to_vec()) {
            return Err(bad());
        }
        out.push(label.into_bytes());
        match chars.next() {
            None => return Ok(out),
            Some(',') => {}
            _ => return Err(bad()),
        }
    }
}
pub fn plan_column(c: &ColumnDescriptor) -> Result<MysqlPgColumn> {
    let t = c.data_type.to_ascii_lowercase();
    let unsigned = c
        .column_type
        .to_ascii_lowercase()
        .split_whitespace()
        .any(|s| s == "unsigned");
    let mut args = Vec::new();
    if !matches!(t.as_str(), "enum" | "set")
        && let Some((_, a)) = c.column_type.split_once('(')
    {
        let (a, _) = a.split_once(')').ok_or_else(bad)?;
        args = a
            .split(',')
            .map(|s| s.trim().parse().map_err(|_| bad()))
            .collect::<Result<_>>()?;
    }
    let text = || PgType::Text {
        encoding: "UTF8".into(),
    };
    let mut typmod = -1;
    let (pg_type, mut ddl, transform) = match t.as_str() {
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" => {
            match (t.as_str(), unsigned) {
                ("tinyint", _) | ("smallint", false) => (PgType::Int2, "int2".into(), false),
                ("smallint", true) | ("mediumint", _) | ("int" | "integer", false) => {
                    (PgType::Int4, "int4".into(), false)
                }
                ("bigint", true) => {
                    typmod = (20 << 16) + 4;
                    (PgType::Numeric, "numeric(20,0)".into(), false)
                }
                _ => (PgType::Int8, "int8".into(), false),
            }
        }
        "decimal" | "numeric" => {
            if args.is_empty() {
                args = vec![10, 0];
            }
            if args.len() != 2 || args[0] == 0 || args[0] > 65 || args[1] > 30 || args[1] > args[0]
            {
                return Err(bad());
            }
            typmod = ((args[0] << 16) | args[1]) as i32 + 4;
            (
                PgType::Numeric,
                format!("numeric({},{})", args[0], args[1]),
                false,
            )
        }
        "float" => (PgType::Float4, "float4".into(), false),
        "double" | "real" => (PgType::Float8, "float8".into(), false),
        "bit" => {
            if args.len() != 1 || !(1..=64).contains(&args[0]) {
                return Err(bad());
            }
            (PgType::Bytea, "bytea".into(), true)
        }
        "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" => {
            (PgType::Bytea, "bytea".into(), false)
        }
        "date" => (PgType::Date, "date".into(), false),
        "datetime" | "timestamp" | "time" => {
            let f = args.first().copied().unwrap_or(0);
            if args.len() > 1 || f > 6 {
                return Err(bad());
            }
            args = vec![f];
            if t != "time" {
                typmod = f as i32;
            }
            match t.as_str() {
                "datetime" => (PgType::Timestamp, format!("timestamp({f})"), false),
                "timestamp" => (PgType::Timestamptz, format!("timestamptz({f})"), false),
                _ => (PgType::Interval, "interval".into(), true),
            }
        }
        "year" => (PgType::Int2, "int2".into(), false),
        "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" => {
            if c.character_set.as_deref() == Some("binary") {
                (PgType::Bytea, "bytea".into(), false)
            } else if matches!(t.as_str(), "char" | "varchar") {
                if args.len() != 1 || args[0] == 0 || args[0] > 10_485_760 {
                    return Err(bad());
                }
                typmod = args[0] as i32 + 4;
                (
                    if t == "char" {
                        PgType::Char {
                            encoding: "UTF8".into(),
                        }
                    } else {
                        PgType::Varchar {
                            encoding: "UTF8".into(),
                        }
                    },
                    format!("{t}({})", args[0]),
                    false,
                )
            } else {
                (text(), "text".into(), false)
            }
        }
        "enum" | "set" => (text(), "text".into(), true),
        "json" => (PgType::Jsonb, "jsonb".into(), true),
        _ => {
            return Err(MappingError::Unsupported(format!(
                "MySQL {}",
                c.column_type
            )));
        }
    };
    let mut collation = None;
    if matches!(
        pg_type,
        PgType::Text { .. } | PgType::Char { .. } | PgType::Varchar { .. }
    ) {
        if !matches!(
            c.character_set.as_deref(),
            Some("utf8mb4" | "utf8mb3" | "utf8" | "ascii")
        ) {
            return Err(MappingError::Unsupported(format!(
                "MySQL charset {:?} (latin1 requires Windows-1252 qualification)",
                c.character_set
            )));
        }
        let co = &c.collation.as_ref().ok_or_else(bad)?.name;
        let (name, locale, deterministic) = if co.ends_with("_bin") {
            ("C", "C", true)
        } else if co.ends_with("_ai_ci") {
            ("skedaddle_und_ai_ci", "und-u-ks-level1", false)
        } else if co.ends_with("_ci") {
            ("skedaddle_und_ci", "und-u-ks-level2", false)
        } else if co.ends_with("_cs") {
            ("skedaddle_und_as_cs", "und-u-ks-level3", true)
        } else {
            return Err(MappingError::Unsupported(format!("MySQL collation {co}")));
        };
        ddl.push_str(&format!(" COLLATE {}", quote(name)));
        collation = Some(CollationRequirement {
            name: name.into(),
            locale: locale.into(),
            deterministic,
            pattern_operations_restricted: !deterministic,
        });
    }
    let labels = if matches!(t.as_str(), "enum" | "set") {
        definition(c)?
    } else {
        vec![]
    };
    if t == "set" && (labels.len() > 64 || labels.iter().any(|s| s.is_empty() || s.contains(&b',')))
    {
        return Err(MappingError::Unsupported(
            "ambiguous MySQL SET labels".into(),
        ));
    }
    if t == "enum" && (labels.len() > 65535 || labels.iter().any(Vec::is_empty)) {
        return Err(MappingError::Unsupported(
            "ambiguous MySQL ENUM labels".into(),
        ));
    }
    let check = (t == "enum").then(|| {
        format!(
            "CHECK ({} IN ({}))",
            quote(&c.name),
            labels
                .iter()
                .map(|s| format!("'{}'", String::from_utf8_lossy(s).replace('\'', "''")))
                .collect::<Vec<_>>()
                .join(", ")
        )
    });
    let hash = Sha256::digest(serde_json::to_vec(&(&pg_type, typmod)).map_err(wire_error)?);
    Ok(MysqlPgColumn {
        source: c.clone(),
        pg_type,
        ddl_type: ddl,
        classification: Classification::Supported,
        supported_with_transformation: transform,
        collation,
        check,
        type_modifier: typmod,
        type_id: format!("skva.v1:{hash:x}"),
        generated: c.generated,
        args,
        labels,
    })
}
fn integer_bounds(c: &ColumnDescriptor) -> (i128, i128) {
    let bits = match c.data_type.as_str() {
        "tinyint" => 8,
        "smallint" => 16,
        "mediumint" => 24,
        "int" | "integer" => 32,
        _ => 64,
    };
    if c.column_type.contains("unsigned") {
        (0, (1i128 << bits) - 1)
    } else {
        (-(1i128 << (bits - 1)), (1i128 << (bits - 1)) - 1)
    }
}
fn text_check(p: &MysqlPgColumn, b: &[u8]) -> Result<()> {
    let s = std::str::from_utf8(b).map_err(wire_error)?;
    if b.len()
        > match p.source.data_type.as_str() {
            "tinytext" => 255,
            "text" => 65_535,
            "mediumtext" => 16_777_215,
            _ => usize::MAX,
        }
    {
        return Err(bad());
    }
    if b.contains(&0)
        || (p.source.character_set.as_deref() == Some("ascii") && !s.is_ascii())
        || (matches!(p.source.character_set.as_deref(), Some("utf8mb3" | "utf8"))
            && s.chars().any(|c| c as u32 > 0xffff))
    {
        return Err(bad());
    }
    if matches!(p.source.data_type.as_str(), "char" | "varchar")
        && s.chars().count() > p.args[0] as usize
    {
        return Err(bad());
    }
    Ok(())
}
pub fn forward(p: &MysqlPgColumn, v: &V) -> Result<Value> {
    let t = p.source.data_type.as_str();
    let value = match v {
        V::Null if p.source.nullable => Value::Null,
        V::Float(f) if t == "float" => Value::Float4(f.to_bits()),
        V::Double(f) if matches!(t, "double" | "real") => Value::Float8(f.to_bits()),
        _ => {
            let opaque = wire::to_opaque(&p.source, v).map_err(wire_error)?;
            let b = opaque.bytes.ok_or_else(bad)?;
            match v {
                V::Signed(_) | V::Unsigned(_) => {
                    let n = std::str::from_utf8(&b)
                        .map_err(wire_error)?
                        .parse::<i128>()
                        .map_err(wire_error)?;
                    let (lo, hi) = integer_bounds(&p.source);
                    if n < lo || n > hi {
                        return Err(bad());
                    }
                    Value::parse_pg_text(&p.pg_type, Some(&b))?
                }
                V::Decimal(s) => {
                    let n = Numeric::parse(s)?;
                    let (negative, digits, scale) = n.finite_parts().ok_or_else(bad)?;
                    if scale != p.args[1]
                        || digits.len().saturating_sub(scale as usize)
                            > (p.args[0] - p.args[1]) as usize
                        || n.to_string() != *s
                        || (negative && p.source.column_type.contains("unsigned"))
                    {
                        return Err(bad());
                    }
                    Value::Numeric(n)
                }
                V::Year(s) => {
                    let y = s.parse::<i16>().map_err(wire_error)?;
                    if y != 0 && !(1901..=2155).contains(&y) {
                        return Err(bad());
                    }
                    Value::Int2(y)
                }
                V::Bit(b) => {
                    let n = p.args[0];
                    if b.len() != n.div_ceil(8) as usize
                        || (!n.is_multiple_of(8) && b[0] >> (n % 8) != 0)
                    {
                        return Err(bad());
                    }
                    Value::Bytea(b.clone())
                }
                V::Binary(b) | V::Text { bytes: b, .. } if p.pg_type == PgType::Bytea => {
                    if let Some(n) = p.args.first()
                        && ((t == "binary" && b.len() != *n as usize)
                            || (matches!(t, "varbinary" | "char" | "varchar")
                                && b.len() > *n as usize))
                    {
                        return Err(bad());
                    }
                    if b.len()
                        > match t {
                            "tinyblob" => 255,
                            "blob" => 65_535,
                            "mediumblob" => 16_777_215,
                            _ => usize::MAX,
                        }
                    {
                        return Err(bad());
                    }
                    Value::Bytea(b.clone())
                }
                V::Text { .. } | V::Enum { .. } | V::Set { .. } => {
                    text_check(p, &b)?;
                    if t == "enum" && !p.labels.contains(&b) {
                        return Err(bad());
                    }
                    Value::parse_pg_text(&p.pg_type, Some(&b))?
                }
                V::DateTime(y, m, d, ..) => {
                    if *y == 0 || *m == 0 || *d == 0 {
                        return Err(MappingError::Unsupported("MySQL zero date".into()));
                    }
                    if *y < 1000 {
                        return Err(bad());
                    }
                    let bytes = if t == "date" { &b[..10] } else { &b };
                    Value::parse_pg_text(&p.pg_type, Some(bytes))?
                }
                V::Timestamp(s) => {
                    if s.split('.').next() == Some("0") {
                        return Err(MappingError::Unsupported("MySQL zero timestamp".into()));
                    }
                    if s.split('.')
                        .next()
                        .ok_or_else(bad)?
                        .parse::<u64>()
                        .map_err(wire_error)?
                        > i32::MAX as u64
                    {
                        return Err(bad());
                    }
                    let mut b = b;
                    b.extend(b"+00:00");
                    Value::parse_pg_text(&p.pg_type, Some(&b))?
                }
                V::Time {
                    negative,
                    days,
                    hours,
                    minutes,
                    seconds,
                    micros,
                } => {
                    let n = ((i64::from(*days) * 24 + i64::from(*hours)) * 3600
                        + i64::from(*minutes) * 60
                        + i64::from(*seconds))
                        * 1_000_000
                        + i64::from(*micros);
                    if n > 3_020_399_000_000 || (*negative && n == 0) {
                        return Err(bad());
                    }
                    Value::Interval {
                        months: 0,
                        days: 0,
                        micros: if *negative { -n } else { n },
                    }
                }
                V::Json(j) => {
                    let normalized = Jsonb::parse(&serde_json::to_string(j).map_err(wire_error)?)?;
                    if serde_json::from_str::<serde_json::Value>(normalized.as_str())
                        .map_err(wire_error)?
                        != *j
                    {
                        return Err(MappingError::Unsupported(
                            "JSON number representation changes through JSONB".into(),
                        ));
                    }
                    Value::Jsonb(normalized)
                }
                _ => return Err(bad()),
            }
        }
    };
    p.pg_type.check(&value, None, 0)?;
    Ok(value)
}
pub fn inverse(p: &MysqlPgColumn, value: &Value) -> Result<V> {
    p.pg_type.check(value, None, 0)?;
    let t = p.source.data_type.as_str();
    let v = match value {
        Value::Null if p.source.nullable => V::Null,
        Value::Float4(b) if t == "float" => V::Float(f32::from_bits(*b)),
        Value::Float8(b) if matches!(t, "double" | "real") => V::Double(f64::from_bits(*b)),
        Value::Interval {
            months: 0,
            days: 0,
            micros,
        } if t == "time" => {
            let n = micros.unsigned_abs();
            if n > 3_020_399_000_000 {
                return Err(bad());
            }
            V::Time {
                negative: *micros < 0,
                days: (n / 86_400_000_000) as u32,
                hours: (n / 3_600_000_000 % 24) as u8,
                minutes: (n / 60_000_000 % 60) as u8,
                seconds: (n / 1_000_000 % 60) as u8,
                micros: (n % 1_000_000) as u32,
            }
        }
        _ => {
            let mut b = match value {
                Value::Bytea(b) => b.clone(),
                Value::Jsonb(j) => serde_json::to_vec(
                    &serde_json::from_str::<serde_json::Value>(j.as_str()).map_err(wire_error)?,
                )
                .map_err(wire_error)?,
                _ => value.format_pg_text(&p.pg_type)?.ok_or_else(bad)?,
            };
            if t == "year" {
                b = format!(
                    "{:04}",
                    std::str::from_utf8(&b)
                        .map_err(wire_error)?
                        .parse::<u16>()
                        .map_err(wire_error)?
                )
                .into_bytes();
            }
            if t == "date" {
                b.extend(b" 00:00:00");
            }
            if matches!(t, "datetime" | "timestamp") {
                let s = std::str::from_utf8(&b)
                    .map_err(wire_error)?
                    .trim_end_matches("+00:00");
                if s.len() < 19 || !s.is_ascii() {
                    return Err(bad());
                }
                let f = p.args[0] as usize;
                let fraction = s.split_once('.').map_or("", |(_, f)| f);
                if fraction.len() > f && fraction[f..].bytes().any(|b| b != b'0') {
                    return Err(bad());
                }
                b = format!(
                    "{}{}",
                    &s[..19],
                    if f == 0 {
                        String::new()
                    } else {
                        format!(
                            ".{:0<width$}",
                            &fraction[..fraction.len().min(f)],
                            width = f
                        )
                    }
                )
                .into_bytes();
            }
            wire::from_opaque(
                &p.source,
                &OpaqueValue {
                    type_id: mysql_id(&p.source),
                    bytes: Some(b),
                },
            )
            .map_err(wire_error)?
        }
    };
    if forward(p, &v)? != *value {
        return Err(bad());
    }
    Ok(v)
}
pub fn to_pg_opaque(p: &MysqlPgColumn, value: &OpaqueValue) -> Result<OpaqueValue> {
    let v = forward(p, &wire::from_opaque(&p.source, value).map_err(wire_error)?)?;
    Ok(OpaqueValue {
        type_id: p.type_id.clone(),
        bytes: if v == Value::Null {
            None
        } else {
            Some(v.canonical_bytes()?)
        },
    })
}
/// Mapping adapter: the MySQL side is Bytea(mysql.v1 payload), or Null. PG side
/// is the planned Value. Nonfinite floats require the direct typed API because
/// mysql.v1's existing text codec excludes them; the PG applier uses binary floats.
impl Mapping for MysqlPgColumn {
    fn forward(&self, value: &Value) -> Result<Value> {
        let bytes = match value {
            Value::Null => None,
            Value::Bytea(b) => Some(b.clone()),
            _ => return Err(bad()),
        };
        forward(
            self,
            &wire::from_opaque(
                &self.source,
                &OpaqueValue {
                    type_id: mysql_id(&self.source),
                    bytes,
                },
            )
            .map_err(wire_error)?,
        )
    }
    fn inverse(&self, value: &Value) -> Result<Value> {
        let v = inverse(self, value)?;
        Ok(wire::to_opaque(&self.source, &v)
            .map_err(wire_error)?
            .bytes
            .map_or(Value::Null, Value::Bytea))
    }
    fn domain_check(&self, value: &Value) -> Result<()> {
        self.forward(value).map(|_| ())
    }
    fn is_reversible(&self) -> bool {
        true
    }
    fn classification(&self) -> Classification {
        self.classification
    }
}
#[cfg(test)]
mod tests;
