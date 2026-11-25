//! Demand-driven routine/trigger rows and PostgreSQL definition presentation.
use super::*;
use crate::{RoutineDef, TriggerDef};

pub(crate) use giggles_pg_types::admin_catalog::PLPGSQL_OID as LANGUAGE_OID;
const EVENTS: [(u8, &str); 3] = [(4, "INSERT"), (8, "DELETE"), (16, "UPDATE")];
const HANDLERS: [(u32, &str); 3] = [
    (13644, "plpgsql_call_handler"),
    (13645, "plpgsql_inline_handler"),
    (13646, "plpgsql_validator"),
];

/// PostgreSQL 18.6 names of the built-in regproc values exposed by the catalog.
pub fn builtin_regproc_name(oid: u32) -> Option<&'static str> {
    Some(match oid {
        0 => "-",
        3 => "heap_tableam_handler",
        330 => "bthandler",
        331 => "hashhandler",
        332 => "gisthandler",
        333 => "ginhandler",
        334 => "spghandler",
        335 => "brinhandler",
        101 => "eqsel",
        102 => "neqsel",
        103 => "scalarltsel",
        104 => "scalargtsel",
        105 => "eqjoinsel",
        106 => "neqjoinsel",
        107 => "scalarltjoinsel",
        108 => "scalargtjoinsel",
        336 => "scalarlesel",
        337 => "scalargesel",
        386 => "scalarlejoinsel",
        398 => "scalargejoinsel",
        3616 => "tsvector_lt",
        3617 => "tsvector_le",
        3618 => "tsvector_eq",
        3619 => "tsvector_ne",
        3620 => "tsvector_ge",
        3621 => "tsvector_gt",
        3625 => "tsvector_concat",
        3634 => "ts_match_vq",
        3635 => "ts_match_qv",
        3662 => "tsquery_lt",
        3663 => "tsquery_le",
        3664 => "tsquery_eq",
        3665 => "tsquery_ne",
        3666 => "tsquery_ge",
        3667 => "tsquery_gt",
        3669 => "tsquery_and",
        3670 => "tsquery_or",
        3671 => "tsquery_not",
        3686 => "tsmatchsel",
        3687 => "tsmatchjoinsel",
        3691 => "tsq_mcontains",
        3692 => "tsq_mcontained",
        3760 => "ts_match_tt",
        3761 => "ts_match_tq",
        5003 => "pg_catalog.tsquery_phrase",
        5040 => "matchingsel",
        5041 => "matchingjoinsel",
        3717 => "prsd_start",
        3718 => "prsd_nexttoken",
        3719 => "prsd_end",
        3720 => "prsd_headline",
        3721 => "prsd_lextype",
        3725 => "dsimple_init",
        3726 => "dsimple_lexize",
        13232 => "dsnowball_init",
        13233 => "dsnowball_lexize",
        _ => return None,
    })
}

fn qualified(schema: &str, name: &str) -> String {
    format!("{}.{}", quote_identifier(schema), quote_identifier(name))
}
fn visible_routine(snapshot: &CatalogSnapshot, name: &str) -> Option<u32> {
    snapshot.search_path.iter().find_map(|schema| {
        snapshot
            .routines
            .iter()
            .find_map(|(&oid, r)| (r.schema == *schema && r.name == name).then_some(oid))
    })
}
fn routine_name(snapshot: &CatalogSnapshot, oid: u32, def: &RoutineDef) -> String {
    if visible_routine(snapshot, &def.name) == Some(oid) {
        quote_identifier(&def.name)
    } else {
        qualified(&def.schema, &def.name)
    }
}
fn quoted_name(value: &str) -> Option<Vec<String>> {
    let mut chars = value.trim().chars().peekable();
    let mut parts = Vec::new();
    loop {
        let mut part = String::new();
        if chars.peek() == Some(&'"') {
            chars.next();
            loop {
                match chars.next()? {
                    '"' if chars.peek() == Some(&'"') => {
                        chars.next();
                        part.push('"');
                    }
                    '"' => break,
                    c => part.push(c),
                }
            }
        } else {
            while chars
                .peek()
                .is_some_and(|c| c.is_alphanumeric() || *c == '_' || *c == '$')
            {
                part.push(chars.next()?.to_ascii_lowercase());
            }
        }
        if part.is_empty() {
            return None;
        }
        parts.push(part);
        match chars.next() {
            None => return Some(parts),
            Some('.') if parts.len() == 1 => (),
            _ => return None,
        }
    }
}
pub(crate) fn registered_oid(snapshot: &CatalogSnapshot, value: &str, procedure: bool) -> R<u32> {
    if value == "-" {
        return Ok(0);
    }
    if let Ok(oid) = value.parse() {
        return Ok(oid);
    }
    let name = if procedure {
        let (name, arguments) = value.rsplit_once('(').ok_or(Error::Inquiry("22P02"))?;
        if arguments
            .strip_suffix(')')
            .is_none_or(|args| !args.trim().is_empty())
        {
            return Err(Error::Inquiry("42883"));
        }
        name
    } else {
        value
    };
    let parts = quoted_name(name).ok_or(Error::Inquiry("22P02"))?;
    let name = parts.last().unwrap();
    let schema = if parts.len() == 2 {
        Some(parts[0].as_str())
    } else {
        None
    };
    if schema.is_none_or(|s| s == "pg_catalog") {
        if let Some((oid, _)) = HANDLERS.iter().find(|(_, n)| *n == name) {
            return Ok(*oid);
        }
        if name == "array_in" && !procedure {
            return Ok(750);
        }
    }
    if let Some(schema) = schema {
        snapshot
            .routines
            .iter()
            .find_map(|(&oid, r)| (r.schema == schema && r.name == *name).then_some(oid))
    } else {
        visible_routine(snapshot, name)
    }
    .ok_or(Error::Inquiry("42883"))
}
pub(crate) fn registered_display(snapshot: &CatalogSnapshot, oid: u32, procedure: bool) -> String {
    let name = if oid == 0 {
        return "-".into();
    } else if let Some(def) = snapshot.routines.get(&oid) {
        routine_name(snapshot, oid, def)
    } else if let Some((_, name)) = HANDLERS.iter().find(|(id, _)| *id == oid) {
        (*name).into()
    } else if !procedure && let Some(name) = builtin_regproc_name(oid) {
        name.into()
    } else if oid == 750 && !procedure {
        "array_in".into()
    } else {
        return oid.to_string();
    };
    if procedure { format!("{name}()") } else { name }
}

pub(crate) fn function_definition(r: &RoutineDef) -> String {
    let name = qualified(&r.schema, &r.name);
    let mut options: Vec<String> = Vec::new();
    match r.volatility {
        'i' => options.push("IMMUTABLE".into()),
        's' => options.push("STABLE".into()),
        _ => (),
    }
    match r.parallel {
        's' => options.push("PARALLEL SAFE".into()),
        'r' => options.push("PARALLEL RESTRICTED".into()),
        _ => (),
    }
    if r.strict {
        options.push("STRICT".into());
    }
    if r.security_definer {
        options.push("SECURITY DEFINER".into());
    }
    if r.leakproof {
        options.push("LEAKPROOF".into());
    }
    if r.cost != giggles_pg_types::Float4::from_bits(100f32.to_bits()) {
        options.push(format!("COST {}", r.cost.value()));
    }
    let options = if options.is_empty() {
        String::new()
    } else {
        format!(" {}\n", options.join(" "))
    };
    let mut delimiter = "$function$".to_owned();
    while r.body.contains(&delimiter) {
        delimiter.insert(delimiter.len() - 1, 'x');
    }
    format!(
        "CREATE OR REPLACE FUNCTION {name}()\n RETURNS trigger\n LANGUAGE plpgsql\n{options}AS {delimiter}{}{delimiter}\n",
        r.body
    )
}
fn literal(arg: &str) -> String {
    format!("'{}'", arg.replace('\'', "''"))
}
fn action(snapshot: &CatalogSnapshot, t: &TriggerDef) -> R<String> {
    let routine = snapshot
        .routines
        .get(&t.routine)
        .ok_or(Error::InvalidSnapshot)?;
    let name = routine_name(snapshot, t.routine, routine);
    let args = t
        .args
        .iter()
        .map(|arg| literal(arg))
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!("EXECUTE FUNCTION {name}({args})"))
}
fn timing(t: &TriggerDef) -> &'static str {
    if t.kind & 2 != 0 { "BEFORE" } else { "AFTER" }
}
fn orientation(t: &TriggerDef) -> &'static str {
    if t.kind & 1 != 0 { "ROW" } else { "STATEMENT" }
}

struct Condition {
    pretty: String,
    full: String,
}
fn condition(t: &TriggerDef) -> Option<Condition> {
    use sqlparser::{
        ast::Expr,
        dialect::PostgreSqlDialect,
        parser::Parser,
        tokenizer::{Token, Tokenizer, Whitespace},
    };
    fn full(expr: &Expr) -> String {
        match expr {
            Expr::BinaryOp { left, op, right } => format!("({} {op} {})", full(left), full(right)),
            Expr::IsNull(a) => format!("({} IS NULL)", full(a)),
            Expr::IsNotNull(a) => format!("({} IS NOT NULL)", full(a)),
            Expr::IsTrue(a) => format!("({} IS TRUE)", full(a)),
            Expr::IsFalse(a) => format!("({} IS FALSE)", full(a)),
            Expr::IsNotTrue(a) => format!("({} IS NOT TRUE)", full(a)),
            Expr::IsNotFalse(a) => format!("({} IS NOT FALSE)", full(a)),
            Expr::Nested(a) => full(a),
            _ => expr.to_string(),
        }
    }
    let text = t.condition.as_deref()?.trim();
    let dialect = PostgreSqlDialect {};
    let normalized = Tokenizer::new(&dialect, text)
        .tokenize()
        .ok()?
        .into_iter()
        .map(|token| match token {
            Token::Word(w) if w.quote_style.is_none() => w.value.to_ascii_lowercase(),
            Token::Whitespace(
                Whitespace::SingleLineComment { .. } | Whitespace::MultiLineComment(_),
            ) => " ".into(),
            token => token.to_string(),
        })
        .collect::<String>();
    let expr = Parser::new(&dialect)
        .try_with_sql(&normalized)
        .and_then(|mut parser| parser.parse_expr());
    Some(match expr {
        Ok(expr) => Condition {
            pretty: expr.to_string(),
            full: full(&expr),
        },
        Err(_) => Condition {
            pretty: normalized.clone(),
            full: normalized,
        },
    })
}
fn attnum(table: &TableDef, id: u64) -> R<i64> {
    table
        .columns
        .iter()
        .position(|column| column.id == id)
        .map(|i| i as i64 + 1)
        .ok_or(Error::InvalidSnapshot)
}
pub(crate) fn trigger_definition(
    snapshot: &CatalogSnapshot,
    oid: u32,
    pretty: bool,
) -> R<Option<String>> {
    let Some(t) = snapshot.triggers.iter().find(|t| t.oid == oid) else {
        return Ok(None);
    };
    let table = snapshot
        .tables
        .iter()
        .find(|table| table.id == t.table)
        .ok_or(Error::InvalidSnapshot)?;
    let (schema, _) = snapshot.table_schema(t.table);
    let visible = snapshot
        .search_path
        .iter()
        .find_map(|schema| {
            snapshot.tables.iter().find(|candidate| {
                candidate.name == table.name && snapshot.table_schema(candidate.id).0 == schema
            })
        })
        .is_some_and(|found| found.id == table.id);
    let relation = if pretty && visible {
        quote_identifier(&table.name)
    } else {
        qualified(schema, &table.name)
    };
    let mut events = Vec::new();
    for (bit, event) in EVENTS {
        if t.kind & bit == 0 {
            continue;
        }
        if bit == 16 && !t.update_columns.is_empty() {
            let columns = t
                .update_columns
                .iter()
                .map(|id| {
                    attnum(table, *id)
                        .map(|index| quote_identifier(&table.columns[index as usize - 1].name))
                })
                .collect::<R<Vec<_>>>()?
                .join(", ");
            events.push(format!("UPDATE OF {columns}"));
        } else {
            events.push(event.into());
        }
    }
    let when = condition(t).map_or_else(String::new, |when| {
        format!(" WHEN ({})", if pretty { when.pretty } else { when.full })
    });
    Ok(Some(format!(
        "CREATE TRIGGER {} {} {} ON {relation} FOR EACH {}{when} {}",
        quote_identifier(&t.name),
        timing(t),
        events.join(" OR "),
        orientation(t),
        action(snapshot, t)?
    )))
}

pub(super) fn populate(
    snapshot: &CatalogSnapshot,
    lookup: &Lookup<'_>,
    budget: &mut Budget,
    all: &mut Relations,
) -> R<()> {
    macro_rules! add {
        ($name:literal, $row:expr) => {
            if let Some(relation) = all.get_mut($name) {
                budget.add(&mut relation.rows, $row)?;
            }
        };
    }
    if all.contains_key("pg_language") {
        for (oid, name, trusted, handler, inline, validator) in giggles_pg_types::admin_catalog::LANGUAGES {
            add!(
                "pg_language",
                vec![
                    I(oid),
                    s(name),
                    B(trusted),
                    I(handler),
                    I(inline),
                    I(validator),
                    N,
                    N,
                    I(10),
                    B(handler != 0)
                ]
            );
        }
    }
    let user = snapshot
        .roles
        .iter()
        .find(|role| role.name == snapshot.user);
    if all.contains_key("pg_proc") || all.contains_key("routines") {
        for (&oid, r) in &snapshot.routines {
            budget.visit(1)?;
            let schema = if r.schema == "public" {
                2200
            } else {
                snapshot
                    .schemas
                    .get(&r.schema)
                    .ok_or(Error::InvalidSnapshot)?
                    .0
            };
            add!(
                "pg_proc",
                vec![
                    I(i64::from(oid)),
                    s(&r.name),
                    I(i64::from(schema)),
                    I(i64::from(r.owner)),
                    I(LANGUAGE_OID),
                    Value::Float4(r.cost),
                    Value::Float4(giggles_pg_types::Float4::from_bits(0)),
                    I(0),
                    s("f"),
                    B(r.security_definer),
                    B(r.leakproof),
                    B(r.strict),
                    B(false),
                    Value::Text(r.volatility.to_string()),
                    Value::Text(r.parallel.to_string()),
                    I(0),
                    I(0),
                    I(2279),
                    Value::Array(vec![]),
                    N,
                    N,
                    N,
                    N,
                    s(&r.body),
                    N,
                    N,
                    N,
                    N,
                    I(0),
                    N,
                    N
                ]
            );
            if let Some(relation) = all.get_mut("routines") {
                let can_define = user.is_some_and(|role| role.superuser || role.oid == r.owner);
                let row = relation
                    .fields
                    .iter()
                    .map(|field| match field.name.as_str() {
                        "specific_catalog" | "routine_catalog" | "type_udt_catalog" => {
                            s(&snapshot.database)
                        }
                        "specific_schema" | "routine_schema" => s(&r.schema),
                        "specific_name" => {
                            let name = format!("{}_{oid}", r.name);
                            s(&name[..name.floor_char_boundary(63)])
                        }
                        "routine_name" => s(&r.name),
                        "routine_type" => s("FUNCTION"),
                        "data_type" | "type_udt_name" => s("trigger"),
                        "type_udt_schema" => s("pg_catalog"),
                        "dtd_identifier" => s("0"),
                        "routine_body" => s("EXTERNAL"),
                        "routine_definition" if can_define => s(&r.body),
                        "external_language" => s("PLPGSQL"),
                        "parameter_style" => s("GENERAL"),
                        "is_deterministic" => s(if r.volatility == 'i' { "YES" } else { "NO" }),
                        "sql_data_access" => s("MODIFIES"),
                        "is_null_call" => s(if r.strict { "YES" } else { "NO" }),
                        "schema_level_routine" => s("YES"),
                        "max_dynamic_result_sets" => I(0),
                        "security_type" => s(if r.security_definer {
                            "DEFINER"
                        } else {
                            "INVOKER"
                        }),
                        _ => N,
                    })
                    .collect();
                budget.add(&mut relation.rows, row)?;
            }
        }
    }
    if !["pg_trigger", "pg_depend", "triggers"]
        .into_iter()
        .any(|name| all.contains_key(name))
    {
        return Ok(());
    }
    let info = all.contains_key("triggers");
    let mut triggers: Vec<_> = snapshot.triggers.iter().collect();
    if info {
        triggers.sort_unstable_by_key(|t| (t.table, &t.name));
    }
    let mut action_order = BTreeMap::new();
    for t in triggers {
        budget.visit(1)?;
        let table = lookup.tables.get(&t.table).ok_or(Error::InvalidSnapshot)?;
        let relation = i64::from(table_oid(t.table)?);
        let oid = i64::from(t.oid);
        let routine = i64::from(t.routine);
        let (schema, _) = snapshot.table_schema(t.table);
        let columns = t
            .update_columns
            .iter()
            .map(|id| attnum(table, *id))
            .collect::<R<Vec<_>>>()?;
        let when = condition(t);
        add!(
            "pg_trigger",
            vec![
                I(oid),
                I(relation),
                I(0),
                s(&t.name),
                I(routine),
                I(i64::from(t.kind)),
                Value::Text(t.enabled.to_string()),
                B(false),
                I(0),
                I(0),
                I(0),
                B(false),
                B(false),
                I(t.args.len() as i64),
                Value::Array(columns.iter().copied().map(I).collect()),
                Value::Bytes(
                    t.args
                        .iter()
                        .flat_map(|arg| arg.bytes().chain([0]))
                        .collect()
                ),
                N,
                N,
                N,
                when.as_ref().map_or(N, |w| s(&w.full))
            ]
        );
        add!(
            "pg_depend",
            vec![I(2620), I(oid), I(0), I(1255), I(routine), I(0), s("n")]
        );
        add!(
            "pg_depend",
            vec![I(2620), I(oid), I(0), I(1259), I(relation), I(0), s("a")]
        );
        if all.contains_key("pg_depend") {
            for column in columns.into_iter().chain(
                t.condition_columns
                    .iter()
                    .map(|id| attnum(table, *id))
                    .collect::<R<Vec<_>>>()?,
            ) {
                add!(
                    "pg_depend",
                    vec![
                        I(2620),
                        I(oid),
                        I(0),
                        I(1259),
                        I(relation),
                        I(column),
                        s("n")
                    ]
                );
            }
        }
        if info {
            let owner = snapshot.table_owners.get(&t.table).copied().unwrap_or(10);
            let owns = user.is_some_and(|u| u.superuser || u.oid == owner);
            let can_see = owns
                || user.is_some_and(|u| {
                    snapshot.table_acls.get(&t.table).is_some_and(|entries| {
                        entries.iter().any(|a| {
                            (a.grantee == u.oid || a.grantee == 0)
                                && a.privileges.iter().any(|p| *p != AclPrivilege::Select)
                        })
                    })
                });
            if !can_see {
                continue;
            }
            let statement = action(snapshot, t)?;
            for (bit, event) in EVENTS {
                if t.kind & bit == 0 {
                    continue;
                }
                let order = action_order.entry((t.table, bit, t.kind & 3)).or_insert(0);
                *order += 1;
                add!(
                    "triggers",
                    vec![
                        s(&snapshot.database),
                        s(schema),
                        s(&t.name),
                        s(event),
                        s(&snapshot.database),
                        s(schema),
                        s(&table.name),
                        I(*order),
                        if owns {
                            when.as_ref().map_or(N, |w| s(&w.full))
                        } else {
                            N
                        },
                        s(&statement),
                        s(orientation(t)),
                        s(timing(t)),
                        N,
                        N,
                        N,
                        N,
                        N
                    ]
                );
            }
        }
    }
    Ok(())
}

pub(super) fn information_schema(name: &str) -> Option<Vec<Field>> {
    Some(fields(match name {
        "triggers" => &[
            ("trigger_catalog", T::Name),
            ("trigger_schema", T::Name),
            ("trigger_name", T::Name),
            ("event_manipulation", T::Varchar),
            ("event_object_catalog", T::Name),
            ("event_object_schema", T::Name),
            ("event_object_table", T::Name),
            ("action_order", T::Int4),
            ("action_condition", T::Varchar),
            ("action_statement", T::Varchar),
            ("action_orientation", T::Varchar),
            ("action_timing", T::Varchar),
            ("action_reference_old_table", T::Name),
            ("action_reference_new_table", T::Name),
            ("action_reference_old_row", T::Name),
            ("action_reference_new_row", T::Name),
            ("created", T::TimestampTz),
        ],
        "routines" => &[
            ("specific_catalog", T::Name),
            ("specific_schema", T::Name),
            ("specific_name", T::Name),
            ("routine_catalog", T::Name),
            ("routine_schema", T::Name),
            ("routine_name", T::Name),
            ("routine_type", T::Varchar),
            ("module_catalog", T::Name),
            ("module_schema", T::Name),
            ("module_name", T::Name),
            ("udt_catalog", T::Name),
            ("udt_schema", T::Name),
            ("udt_name", T::Name),
            ("data_type", T::Varchar),
            ("character_maximum_length", T::Int4),
            ("character_octet_length", T::Int4),
            ("character_set_catalog", T::Name),
            ("character_set_schema", T::Name),
            ("character_set_name", T::Name),
            ("collation_catalog", T::Name),
            ("collation_schema", T::Name),
            ("collation_name", T::Name),
            ("numeric_precision", T::Int4),
            ("numeric_precision_radix", T::Int4),
            ("numeric_scale", T::Int4),
            ("datetime_precision", T::Int4),
            ("interval_type", T::Varchar),
            ("interval_precision", T::Int4),
            ("type_udt_catalog", T::Name),
            ("type_udt_schema", T::Name),
            ("type_udt_name", T::Name),
            ("scope_catalog", T::Name),
            ("scope_schema", T::Name),
            ("scope_name", T::Name),
            ("maximum_cardinality", T::Int4),
            ("dtd_identifier", T::Name),
            ("routine_body", T::Varchar),
            ("routine_definition", T::Varchar),
            ("external_name", T::Varchar),
            ("external_language", T::Varchar),
            ("parameter_style", T::Varchar),
            ("is_deterministic", T::Varchar),
            ("sql_data_access", T::Varchar),
            ("is_null_call", T::Varchar),
            ("sql_path", T::Varchar),
            ("schema_level_routine", T::Varchar),
            ("max_dynamic_result_sets", T::Int4),
            ("is_user_defined_cast", T::Varchar),
            ("is_implicitly_invocable", T::Varchar),
            ("security_type", T::Varchar),
            ("to_sql_specific_catalog", T::Name),
            ("to_sql_specific_schema", T::Name),
            ("to_sql_specific_name", T::Name),
            ("as_locator", T::Varchar),
            ("created", T::TimestampTz),
            ("last_altered", T::TimestampTz),
            ("new_savepoint_level", T::Varchar),
            ("is_udt_dependent", T::Varchar),
            ("result_cast_from_data_type", T::Varchar),
            ("result_cast_as_locator", T::Varchar),
            ("result_cast_char_max_length", T::Int4),
            ("result_cast_char_octet_length", T::Int4),
            ("result_cast_char_set_catalog", T::Name),
            ("result_cast_char_set_schema", T::Name),
            ("result_cast_char_set_name", T::Name),
            ("result_cast_collation_catalog", T::Name),
            ("result_cast_collation_schema", T::Name),
            ("result_cast_collation_name", T::Name),
            ("result_cast_numeric_precision", T::Int4),
            ("result_cast_numeric_precision_radix", T::Int4),
            ("result_cast_numeric_scale", T::Int4),
            ("result_cast_datetime_precision", T::Int4),
            ("result_cast_interval_type", T::Varchar),
            ("result_cast_interval_precision", T::Int4),
            ("result_cast_type_udt_catalog", T::Name),
            ("result_cast_type_udt_schema", T::Name),
            ("result_cast_type_udt_name", T::Name),
            ("result_cast_scope_catalog", T::Name),
            ("result_cast_scope_schema", T::Name),
            ("result_cast_scope_name", T::Name),
            ("result_cast_maximum_cardinality", T::Int4),
            ("result_cast_dtd_identifier", T::Name),
        ],
        _ => return None,
    }))
}
