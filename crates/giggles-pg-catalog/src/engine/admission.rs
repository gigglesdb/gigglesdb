//! Overflow admission only: never replace the AST's normal ownership decision.
use super::*;

pub(super) fn decline_or_limit(sql: &str) -> R<Option<Plan>> {
    if may_own(sql) {
        Err(Error::Limit)
    } else {
        Ok(None)
    }
}

// Scan without constructing sqlparser tokens or an AST. Recognize every explicit
// ownership marker used by Scan, including its closed introspection route.
// Ambiguous/invalid lexical forms retain the catalog guard. False positives
// (e.g. a catalog name used as an alias) also retain it; never route a possible
// catalog query to core merely because its catalog reference occurs late.
pub(super) fn may_own(sql: &str) -> bool {
    fn owned(name: &str) -> bool {
        unqualified_catalog(name)
            || matches!(
                name,
                "pg_catalog"
                    | "information_schema"
                    | "pg_options_to_table"
                    | "pg_get_sequence_data"
                    | "user"
                    | "current_role"
                    | "current_catalog"
                    | "version"
                    | "pg_postmaster_start_time"
                    | "pg_backend_pid"
                    | "pg_is_in_recovery"
                    | "txid_current"
                    | "current_database"
                    | "current_schema"
                    | "current_user"
                    | "session_user"
                    | "current_schemas"
                    | "pg_get_indexdef"
                    | "pg_get_constraintdef"
                    | "pg_get_expr"
                    | "pg_get_userbyid"
                    | "format_type"
                    | "obj_description"
                    | "pg_collation_actual_version"
                    | "col_description"
                    | "shobj_description"
                    | "pg_get_viewdef"
                    | "has_table_privilege"
                    | "has_column_privilege"
            )
    }
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                i += 2;
                while i < bytes.len() && !matches!(bytes[i], b'\r' | b'\n') {
                    i += 1;
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i += 2;
                let mut depth = 1usize;
                while i < bytes.len() && depth > 0 {
                    if bytes.get(i..i + 2) == Some(b"/*") {
                        depth += 1;
                        i += 2;
                    } else if bytes.get(i..i + 2) == Some(b"*/") {
                        depth -= 1;
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                if depth != 0 {
                    return true;
                }
            }
            quote @ (b'\'' | b'"') => {
                i += 1;
                let start = i;
                let mut escaped = false;
                loop {
                    let Some(&b) = bytes.get(i) else { return true };
                    // Backslash interpretation depends on literal prefix and GUCs.
                    if b == b'\\' {
                        return true;
                    }
                    if b == quote {
                        if bytes.get(i + 1) == Some(&quote) {
                            escaped = true;
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                if quote == b'"' {
                    if escaped {
                        return true;
                    }
                    if owned(&sql[start..i]) {
                        return true;
                    }
                }
                i += 1;
            }
            b'$' => {
                let start = i;
                i += 1;
                while bytes
                    .get(i)
                    .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_')
                {
                    i += 1;
                }
                if bytes.get(i) == Some(&b'$') {
                    i += 1;
                    let delimiter = &sql[start..i];
                    let Some(end) = sql[i..].find(delimiter) else {
                        return true;
                    };
                    i += end + delimiter.len();
                }
            }
            b if b.is_ascii_alphabetic() || b == b'_' => {
                let start = i;
                i += 1;
                while bytes
                    .get(i)
                    .is_some_and(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'$'))
                {
                    i += 1;
                }
                // Longer words cannot match an ownership marker. Avoid allocating
                // a lowercased copy of an arbitrarily long application identifier.
                if i - start <= 128 && owned(&sql[start..i].to_ascii_lowercase()) {
                    return true;
                }
            }
            b if !b.is_ascii() => return true,
            _ => i += 1,
        }
    }
    false
}
