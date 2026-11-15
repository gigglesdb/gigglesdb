//! Closed allowlist of analysed PostgreSQL functions.
/// Return aggregate status only for the closed function allowlist.
pub(super) fn aggregate(name: &str, snapshot: &super::Snapshot) -> Option<bool> {
    if snapshot.user_routines.iter().any(|routine| routine == name) {
        return None;
    }
    match name {
        "count" | "sum" | "avg" | "min" | "max" | "bool_and" | "bool_or" | "every"
        | "string_agg" | "array_agg" => Some(true),
        "lower" | "upper" | "length" | "char_length" | "character_length" | "octet_length"
        | "substr" | "substring" | "left" | "right" | "trim" | "btrim" | "ltrim" | "rtrim"
        | "lpad" | "rpad" | "replace" | "concat" | "concat_ws" | "position" | "strpos"
        | "split_part" | "starts_with" | "abs" | "ceil" | "ceiling" | "floor" | "round"
        | "trunc" | "mod" | "power" | "sqrt" | "greatest" | "least" | "coalesce" | "nullif"
        | "now" | "current_date" | "current_timestamp" | "date_trunc" | "date_part" | "extract"
        | "age" | "make_date" | "to_char" | "to_date" | "to_timestamp" | "md5" | "encode"
        | "decode" | "gen_random_uuid" => Some(false),
        _ => None,
    }
}
