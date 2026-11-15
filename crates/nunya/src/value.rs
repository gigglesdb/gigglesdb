//! Typed SQL values with redacted debugging.
/// A SQL value; dates use days and timestamps microseconds since PostgreSQL's 2000-01-01 epoch.
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum SqlValue {
    /// Null SQL value.
    Null,
    /// Text SQL value.
    Text(String),
    /// Int2 SQL value.
    Int2(i16),
    /// Int4 SQL value.
    Int4(i32),
    /// Int8 SQL value.
    Int8(i64),
    /// Uuid SQL value.
    Uuid([u8; 16]),
    /// Date SQL value.
    Date(i32),
    /// Timestamp SQL value.
    Timestamp(i64),
    /// Timestamptz SQL value.
    Timestamptz(i64),
    /// Bool SQL value.
    Bool(bool),
}
impl std::fmt::Debug for SqlValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Null => "SqlValue::Null",
            Self::Text(..) => "SqlValue::Text(..)",
            Self::Int2(..) => "SqlValue::Int2(..)",
            Self::Int4(..) => "SqlValue::Int4(..)",
            Self::Int8(..) => "SqlValue::Int8(..)",
            Self::Uuid(..) => "SqlValue::Uuid(..)",
            Self::Date(..) => "SqlValue::Date(..)",
            Self::Timestamp(..) => "SqlValue::Timestamp(..)",
            Self::Timestamptz(..) => "SqlValue::Timestamptz(..)",
            Self::Bool(..) => "SqlValue::Bool(..)",
        })
    }
}
/// Database column type including unsupported and user-defined types.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum ColumnType {
    /// Text column type.
    Text,
    /// Varchar column type.
    Varchar(Option<i32>),
    /// Bpchar column type.
    Bpchar(Option<i32>),
    /// Int2 column type.
    Int2,
    /// Int4 column type.
    Int4,
    /// Int8 column type.
    Int8,
    /// Uuid column type.
    Uuid,
    /// Date column type.
    Date,
    /// Timestamp column type.
    Timestamp,
    /// Timestamptz column type.
    Timestamptz,
    /// Bool column type.
    Bool,
    /// Numeric column type.
    Numeric,
    /// Bytea column type.
    Bytea,
    /// Json column type.
    Json,
    /// Jsonb column type.
    Jsonb,
    /// Array column type.
    Array(Box<ColumnType>),
    /// Enum column type.
    Enum(String),
    /// Other column type.
    Other(String),
}
impl ColumnType {
    /// Return the PostgreSQL OID of a built-in scalar type.
    pub fn pg_oid(&self) -> Option<u32> {
        match self {
            Self::Text => Some(25),
            Self::Varchar(..) => Some(1043),
            Self::Bpchar(..) => Some(1042),
            Self::Int2 => Some(21),
            Self::Int4 => Some(23),
            Self::Int8 => Some(20),
            Self::Uuid => Some(2950),
            Self::Date => Some(1082),
            Self::Timestamp => Some(1114),
            Self::Timestamptz => Some(1184),
            Self::Bool => Some(16),
            Self::Numeric => Some(1700),
            Self::Bytea => Some(17),
            Self::Json => Some(114),
            Self::Jsonb => Some(3802),
            Self::Array(..) => None,
            Self::Enum(..) => None,
            Self::Other(..) => None,
        }
    }
    /// Whether this type is a supported text type.
    pub fn is_textual(&self) -> bool {
        matches!(self, Self::Text | Self::Varchar(_) | Self::Bpchar(_))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn builtin_oids() {
        let cases = [
            (ColumnType::Text, 25),
            (ColumnType::Varchar(None), 1043),
            (ColumnType::Bpchar(None), 1042),
            (ColumnType::Int2, 21),
            (ColumnType::Int4, 23),
            (ColumnType::Int8, 20),
            (ColumnType::Uuid, 2950),
            (ColumnType::Date, 1082),
            (ColumnType::Timestamp, 1114),
            (ColumnType::Timestamptz, 1184),
            (ColumnType::Bool, 16),
            (ColumnType::Numeric, 1700),
            (ColumnType::Bytea, 17),
            (ColumnType::Json, 114),
            (ColumnType::Jsonb, 3802),
        ];
        for (ty, oid) in cases {
            assert_eq!(ty.pg_oid(), Some(oid));
        }
        assert_eq!(ColumnType::Array(Box::new(ColumnType::Text)).pg_oid(), None);
    }
    #[test]
    fn values_are_redacted() {
        assert_eq!(
            format!("{:?}", SqlValue::Text("secret".into())),
            "SqlValue::Text(..)"
        );
        assert_eq!(format!("{:?}", SqlValue::Int8(123)), "SqlValue::Int8(..)");
    }
}
