//! Callable built-in argument names and defaults from PostgreSQL 18.6 pg_proc.
use super::{DataType::*, Signature};

macro_rules! signature {
    ($inputs:expr, $defaults:expr, $variadic:expr, $aggregate:expr) => {
        Signature {
            inputs: &$inputs,
            defaults: &$defaults,
            variadic: $variadic,
            aggregate: $aggregate,
        }
    };
}

pub(super) fn lookup(name: &str) -> &'static [Signature] {
    match name {
        "json_object_agg" => &[signature!(
            [("key", None), ("value", None)],
            [],
            false,
            true
        )],
        "jsonb_array_elements" => &[signature!([("from_json", Some(Jsonb))], [], false, false)],
        "jsonb_array_elements_text" => {
            &[signature!([("from_json", Some(Jsonb))], [], false, false)]
        }
        "jsonb_each" => &[signature!([("from_json", Some(Jsonb))], [], false, false)],
        "jsonb_each_text" => &[signature!([("from_json", Some(Jsonb))], [], false, false)],
        "jsonb_object_agg" => &[signature!(
            [("key", None), ("value", None)],
            [],
            false,
            true
        )],
        "jsonb_set" => &[signature!(
            [
                ("jsonb_in", Some(Jsonb)),
                ("path", Some(TextArray)),
                ("replacement", Some(Jsonb)),
                ("create_if_missing", Some(Boolean))
            ],
            ["true"],
            false,
            false
        )],
        "make_date" => &[signature!(
            [
                ("year", Some(Integer)),
                ("month", Some(Integer)),
                ("day", Some(Integer))
            ],
            [],
            false,
            false
        )],
        "make_interval" => &[signature!(
            [
                ("years", Some(Integer)),
                ("months", Some(Integer)),
                ("weeks", Some(Integer)),
                ("days", Some(Integer)),
                ("hours", Some(Integer)),
                ("mins", Some(Integer)),
                ("secs", Some(DoublePrecision))
            ],
            ["0", "0", "0", "0", "0", "0", "0.0"],
            false,
            false
        )],
        "make_time" => &[signature!(
            [
                ("hour", Some(Integer)),
                ("min", Some(Integer)),
                ("sec", Some(DoublePrecision))
            ],
            [],
            false,
            false
        )],
        "make_timestamp" => &[signature!(
            [
                ("year", Some(Integer)),
                ("month", Some(Integer)),
                ("mday", Some(Integer)),
                ("hour", Some(Integer)),
                ("min", Some(Integer)),
                ("sec", Some(DoublePrecision))
            ],
            [],
            false,
            false
        )],
        "make_timestamptz" => &[
            signature!(
                [
                    ("year", Some(Integer)),
                    ("month", Some(Integer)),
                    ("mday", Some(Integer)),
                    ("hour", Some(Integer)),
                    ("min", Some(Integer)),
                    ("sec", Some(DoublePrecision))
                ],
                [],
                false,
                false
            ),
            signature!(
                [
                    ("year", Some(Integer)),
                    ("month", Some(Integer)),
                    ("mday", Some(Integer)),
                    ("hour", Some(Integer)),
                    ("min", Some(Integer)),
                    ("sec", Some(DoublePrecision)),
                    ("timezone", Some(Text))
                ],
                [],
                false,
                false
            ),
        ],
        "random" => &[signature!([("min", None), ("max", None)], [], false, false)],
        "regexp_count" => &[
            signature!(
                [("string", Some(Text)), ("pattern", Some(Text))],
                [],
                false,
                false
            ),
            signature!(
                [
                    ("string", Some(Text)),
                    ("pattern", Some(Text)),
                    ("start", Some(Integer))
                ],
                [],
                false,
                false
            ),
            signature!(
                [
                    ("string", Some(Text)),
                    ("pattern", Some(Text)),
                    ("start", Some(Integer)),
                    ("flags", Some(Text))
                ],
                [],
                false,
                false
            ),
        ],
        "regexp_instr" => &[
            signature!(
                [("string", Some(Text)), ("pattern", Some(Text))],
                [],
                false,
                false
            ),
            signature!(
                [
                    ("string", Some(Text)),
                    ("pattern", Some(Text)),
                    ("start", Some(Integer))
                ],
                [],
                false,
                false
            ),
            signature!(
                [
                    ("string", Some(Text)),
                    ("pattern", Some(Text)),
                    ("start", Some(Integer)),
                    ("N", Some(Integer))
                ],
                [],
                false,
                false
            ),
            signature!(
                [
                    ("string", Some(Text)),
                    ("pattern", Some(Text)),
                    ("start", Some(Integer)),
                    ("N", Some(Integer)),
                    ("endoption", Some(Integer))
                ],
                [],
                false,
                false
            ),
            signature!(
                [
                    ("string", Some(Text)),
                    ("pattern", Some(Text)),
                    ("start", Some(Integer)),
                    ("N", Some(Integer)),
                    ("endoption", Some(Integer)),
                    ("flags", Some(Text))
                ],
                [],
                false,
                false
            ),
            signature!(
                [
                    ("string", Some(Text)),
                    ("pattern", Some(Text)),
                    ("start", Some(Integer)),
                    ("N", Some(Integer)),
                    ("endoption", Some(Integer)),
                    ("flags", Some(Text)),
                    ("subexpr", Some(Integer))
                ],
                [],
                false,
                false
            ),
        ],
        "regexp_like" => &[
            signature!(
                [("string", Some(Text)), ("pattern", Some(Text))],
                [],
                false,
                false
            ),
            signature!(
                [
                    ("string", Some(Text)),
                    ("pattern", Some(Text)),
                    ("flags", Some(Text))
                ],
                [],
                false,
                false
            ),
        ],
        "regexp_match" => &[
            signature!(
                [("string", Some(Text)), ("pattern", Some(Text))],
                [],
                false,
                false
            ),
            signature!(
                [
                    ("string", Some(Text)),
                    ("pattern", Some(Text)),
                    ("flags", Some(Text))
                ],
                [],
                false,
                false
            ),
        ],
        "regexp_matches" => &[
            signature!(
                [("string", Some(Text)), ("pattern", Some(Text))],
                [],
                false,
                false
            ),
            signature!(
                [
                    ("string", Some(Text)),
                    ("pattern", Some(Text)),
                    ("flags", Some(Text))
                ],
                [],
                false,
                false
            ),
        ],
        "regexp_replace" => &[
            signature!(
                [
                    ("string", Some(Text)),
                    ("pattern", Some(Text)),
                    ("replacement", Some(Text))
                ],
                [],
                false,
                false
            ),
            signature!(
                [
                    ("string", Some(Text)),
                    ("pattern", Some(Text)),
                    ("replacement", Some(Text)),
                    ("flags", Some(Text))
                ],
                [],
                false,
                false
            ),
            signature!(
                [
                    ("string", Some(Text)),
                    ("pattern", Some(Text)),
                    ("replacement", Some(Text)),
                    ("start", Some(Integer))
                ],
                [],
                false,
                false
            ),
            signature!(
                [
                    ("string", Some(Text)),
                    ("pattern", Some(Text)),
                    ("replacement", Some(Text)),
                    ("start", Some(Integer)),
                    ("N", Some(Integer))
                ],
                [],
                false,
                false
            ),
            signature!(
                [
                    ("string", Some(Text)),
                    ("pattern", Some(Text)),
                    ("replacement", Some(Text)),
                    ("start", Some(Integer)),
                    ("N", Some(Integer)),
                    ("flags", Some(Text))
                ],
                [],
                false,
                false
            ),
        ],
        "regexp_split_to_array" => &[
            signature!(
                [("string", Some(Text)), ("pattern", Some(Text))],
                [],
                false,
                false
            ),
            signature!(
                [
                    ("string", Some(Text)),
                    ("pattern", Some(Text)),
                    ("flags", Some(Text))
                ],
                [],
                false,
                false
            ),
        ],
        "regexp_split_to_table" => &[
            signature!(
                [("string", Some(Text)), ("pattern", Some(Text))],
                [],
                false,
                false
            ),
            signature!(
                [
                    ("string", Some(Text)),
                    ("pattern", Some(Text)),
                    ("flags", Some(Text))
                ],
                [],
                false,
                false
            ),
        ],
        "regexp_substr" => &[
            signature!(
                [("string", Some(Text)), ("pattern", Some(Text))],
                [],
                false,
                false
            ),
            signature!(
                [
                    ("string", Some(Text)),
                    ("pattern", Some(Text)),
                    ("start", Some(Integer))
                ],
                [],
                false,
                false
            ),
            signature!(
                [
                    ("string", Some(Text)),
                    ("pattern", Some(Text)),
                    ("start", Some(Integer)),
                    ("N", Some(Integer))
                ],
                [],
                false,
                false
            ),
            signature!(
                [
                    ("string", Some(Text)),
                    ("pattern", Some(Text)),
                    ("start", Some(Integer)),
                    ("N", Some(Integer)),
                    ("flags", Some(Text))
                ],
                [],
                false,
                false
            ),
            signature!(
                [
                    ("string", Some(Text)),
                    ("pattern", Some(Text)),
                    ("start", Some(Integer)),
                    ("N", Some(Integer)),
                    ("flags", Some(Text)),
                    ("subexpr", Some(Integer))
                ],
                [],
                false,
                false
            ),
        ],
        "string_agg" => &[
            signature!(
                [("value", Some(Text)), ("delimiter", Some(Text))],
                [],
                false,
                true
            ),
            signature!(
                [("value", Some(Bytes)), ("delimiter", Some(Bytes))],
                [],
                false,
                true
            ),
        ],
        "ts_debug" => &[
            signature!(
                [("config", Some(RegConfig)), ("document", Some(Text))],
                [],
                false,
                false
            ),
            signature!([("document", Some(Text))], [], false, false),
        ],
        "ts_parse" => &[
            signature!([("parser_name", None), ("txt", None)], [], false, false),
            signature!([("parser_oid", None), ("txt", None)], [], false, false),
        ],
        "ts_token_type" => &[
            signature!([("parser_name", None)], [], false, false),
            signature!([("parser_oid", None)], [], false, false),
        ],
        "unnest" => &[signature!([("tsvector", Some(TsVector))], [], false, false)],
        "uuidv7" => &[signature!([("shift", Some(Interval))], [], false, false)],
        _ => &[],
    }
}
