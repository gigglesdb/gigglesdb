//! Built-in objects exposed by the PostgreSQL utility commands and catalog views.

pub const TABLESPACES: [(i64, &str); 2] = [(1663, "pg_default"), (1664, "pg_global")];

pub const PLPGSQL_OID: i64 = 13647;
pub const LANGUAGES: [(i64, &str, bool, i64, i64, i64); 4] = [
    (12, "internal", false, 0, 0, 2246),
    (13, "c", false, 0, 0, 2247),
    (14, "sql", true, 0, 0, 2248),
    (PLPGSQL_OID, "plpgsql", true, 13644, 13645, 13646),
];
