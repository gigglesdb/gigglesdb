use crate::*;
use std::collections::{BTreeMap, BTreeSet};
pub(crate) struct Relation {
    pub fields: Vec<Field>,
    pub rows: Vec<Vec<Value>>,
}
fn fields(s: &[(&str, PgType)]) -> Vec<Field> {
    s.iter()
        .map(|(n, t)| Field {
            name: (*n).into(),
            data_type: *t,
        })
        .collect()
}
use PgType as T;
use Value::{Bool as B, Int as I, Null as N};
fn s(v: &str) -> Value {
    Value::Text(v.into())
}
/// Injective OIDs in disjoint slots; oversized IDs fail instead of hashing/colliding.
pub fn table_oid(id: u64) -> R<u32> {
    u32::try_from(
        id.checked_mul(4)
            .and_then(|x| x.checked_add(65536))
            .ok_or(Error::InvalidSnapshot)?,
    )
    .map_err(|_| Error::InvalidSnapshot)
    .and_then(|x| {
        if x <= u32::MAX - 3 {
            Ok(x)
        } else {
            Err(Error::InvalidSnapshot)
        }
    })
}
pub(crate) fn schema(name: &str) -> Option<Vec<Field>> {
    Some(fields(match name {
        "pg_namespace" => &[
            ("oid", T::Oid),
            ("nspname", T::Name),
            ("nspowner", T::Oid),
            ("nspacl", T::AclItemArray),
            ("xmin", T::Xid),
        ],
        "pg_class" => &[
            ("oid", T::Oid),
            ("relname", T::Name),
            ("relnamespace", T::Oid),
            ("relkind", T::Char),
            ("relowner", T::Oid),
            ("relnatts", T::Int2),
            ("relhasindex", T::Bool),
            ("relpersistence", T::Char),
            ("relispartition", T::Bool),
            ("reltype", T::Oid),
            ("relam", T::Oid),
            ("reltablespace", T::Oid),
            ("reloptions", T::TextArray),
            ("relacl", T::AclItemArray),
            ("xmin", T::Xid),
            ("relpartbound", T::Text),
        ],
        "pg_attribute" => &[
            ("attrelid", T::Oid),
            ("attname", T::Name),
            ("atttypid", T::Oid),
            ("attnum", T::Int2),
            ("attnotnull", T::Bool),
            ("atttypmod", T::Int4),
            ("attlen", T::Int2),
            ("attisdropped", T::Bool),
            ("attidentity", T::Char),
            ("attgenerated", T::Char),
            ("atthasdef", T::Bool),
            ("attcollation", T::Oid),
            ("xmin", T::Xid),
            ("attndims", T::Int4),
            ("attislocal", T::Bool),
            ("attfdwoptions", T::TextArray),
        ],
        "pg_type" => &[
            ("oid", T::Oid),
            ("typname", T::Name),
            ("typnamespace", T::Oid),
            ("typlen", T::Int2),
            ("typtype", T::Char),
            ("typcategory", T::Char),
            ("typnotnull", T::Bool),
            ("typbasetype", T::Oid),
            ("typtypmod", T::Int4),
            ("typrelid", T::Oid),
            ("typelem", T::Oid),
            ("typarray", T::Oid),
            ("typinput", T::Oid),
            ("typdelim", T::Char),
            ("typowner", T::Oid),
            ("xmin", T::Xid),
            ("typndims", T::Int4),
            ("typdefault", T::Text),
            ("typisdefined", T::Bool),
        ],
        "pg_index" => &[
            ("indexrelid", T::Oid),
            ("indrelid", T::Oid),
            ("indnatts", T::Int2),
            ("indnkeyatts", T::Int2),
            ("indisunique", T::Bool),
            ("indisprimary", T::Bool),
            ("indisvalid", T::Bool),
            ("indkey", T::Int2Vector),
            ("indpred", T::Text),
            ("indexprs", T::Text),
            ("indoption", T::Int2Vector),
            ("indcollation", T::OidVector),
            ("indclass", T::OidVector),
        ],
        "pg_constraint" => &[
            ("oid", T::Oid),
            ("conname", T::Name),
            ("connamespace", T::Oid),
            ("contype", T::Char),
            ("conrelid", T::Oid),
            ("conindid", T::Oid),
            ("conkey", T::Int2Array),
            ("confrelid", T::Oid),
            ("confkey", T::Int2Array),
            ("condeferrable", T::Bool),
            ("condeferred", T::Bool),
            ("convalidated", T::Bool),
            ("xmin", T::Xid),
            ("contypid", T::Oid),
            ("confupdtype", T::Char),
            ("confdeltype", T::Char),
            ("confmatchtype", T::Char),
            ("connoinherit", T::Bool),
            ("conbin", T::Text),
            ("conexclop", T::OidArray),
        ],
        "pg_description" | "pg_shdescription" => &[
            ("objoid", T::Oid),
            ("classoid", T::Oid),
            ("objsubid", T::Int4),
            ("description", T::Text),
        ],
        "pg_attrdef" => &[
            ("oid", T::Oid),
            ("adrelid", T::Oid),
            ("adnum", T::Int2),
            ("adbin", T::Text),
        ],
        "pg_database" => &[
            ("oid", T::Oid),
            ("datname", T::Name),
            ("datdba", T::Oid),
            ("datistemplate", T::Bool),
            ("datallowconn", T::Bool),
            ("datacl", T::AclItemArray),
        ],
        "pg_user" => &[
            ("usename", T::Name),
            ("usesysid", T::Oid),
            ("usesuper", T::Bool),
        ],
        // These engine features are absent. Their schemas are explicit, never catch-all empty tables.
        "pg_language" => &[
            ("oid", T::Oid),
            ("lanname", T::Name),
            ("lanpltrusted", T::Bool),
            ("lanplcallfoid", T::Oid),
            ("laninline", T::Oid),
            ("lanvalidator", T::Oid),
            ("xmin", T::Xid),
            ("lanacl", T::AclItemArray),
        ],
        "pg_roles" => &[
            ("oid", T::Oid),
            ("rolname", T::Name),
            ("rolsuper", T::Bool),
            ("rolinherit", T::Bool),
            ("rolcreaterole", T::Bool),
            ("rolcreatedb", T::Bool),
            ("rolcanlogin", T::Bool),
            ("rolreplication", T::Bool),
            ("rolconnlimit", T::Int4),
            ("rolvaliduntil", T::Text),
            ("rolbypassrls", T::Bool),
            ("rolconfig", T::TextArray),
        ],
        "pg_auth_members" => &[
            ("roleid", T::Oid),
            ("member", T::Oid),
            ("grantor", T::Oid),
            ("admin_option", T::Bool),
        ],
        "pg_timezone_names" => &[("name", T::Text), ("is_dst", T::Bool)],
        "pg_timezone_abbrevs" => &[("abbrev", T::Text), ("is_dst", T::Bool)],
        "pg_tablespace" => &[
            ("oid", T::Oid),
            ("spcname", T::Name),
            ("spcowner", T::Oid),
            ("spcoptions", T::TextArray),
            ("spcacl", T::AclItemArray),
            ("xmin", T::Xid),
        ],
        "pg_foreign_data_wrapper" => &[
            ("oid", T::Oid),
            ("fdwname", T::Name),
            ("fdwowner", T::Oid),
            ("fdwhandler", T::Oid),
            ("fdwvalidator", T::Oid),
            ("fdwoptions", T::TextArray),
            ("fdwacl", T::AclItemArray),
            ("xmin", T::Xid),
        ],
        "pg_foreign_server" => &[
            ("oid", T::Oid),
            ("srvname", T::Name),
            ("srvowner", T::Oid),
            ("srvfdw", T::Oid),
            ("srvtype", T::Text),
            ("srvversion", T::Text),
            ("srvoptions", T::TextArray),
            ("srvacl", T::AclItemArray),
            ("xmin", T::Xid),
        ],
        "pg_user_mappings" => &[
            ("umid", T::Oid),
            ("srvid", T::Oid),
            ("usename", T::Name),
            ("umoptions", T::TextArray),
        ],
        "pg_am" => &[
            ("oid", T::Oid),
            ("amname", T::Name),
            ("amhandler", T::Oid),
            ("amtype", T::Char),
            ("xmin", T::Xid),
        ],
        "pg_extension" => &[
            ("oid", T::Oid),
            ("extname", T::Name),
            ("extowner", T::Oid),
            ("extnamespace", T::Oid),
            ("extversion", T::Text),
            ("xmin", T::Xid),
        ],
        "pg_depend" => &[
            ("classid", T::Oid),
            ("objid", T::Oid),
            ("objsubid", T::Int4),
            ("refclassid", T::Oid),
            ("refobjid", T::Oid),
            ("refobjsubid", T::Int4),
            ("deptype", T::Char),
        ],
        "pg_cast" => &[
            ("oid", T::Oid),
            ("castsource", T::Oid),
            ("casttarget", T::Oid),
            ("castfunc", T::Oid),
            ("castcontext", T::Char),
            ("castmethod", T::Char),
            ("xmin", T::Xid),
        ],
        "pg_inherits" => &[
            ("inhrelid", T::Oid),
            ("inhparent", T::Oid),
            ("inhseqno", T::Int4),
        ],
        "pg_foreign_table" => &[
            ("ftrelid", T::Oid),
            ("ftserver", T::Oid),
            ("ftoptions", T::TextArray),
            ("xmin", T::Xid),
        ],
        "pg_opclass" => &[
            ("oid", T::Oid),
            ("opcname", T::Name),
            ("opcnamespace", T::Oid),
            ("opcmethod", T::Oid),
            ("opcdefault", T::Bool),
        ],
        "pg_collation" => &[
            ("oid", T::Oid),
            ("collname", T::Name),
            ("collnamespace", T::Oid),
        ],
        "pg_proc" => &[
            ("oid", T::Oid),
            ("proname", T::Name),
            ("pronamespace", T::Oid),
        ],
        "pg_event_trigger" => &[
            ("oid", T::Oid),
            ("evtname", T::Name),
            ("evtevent", T::Text),
            ("evtfoid", T::Oid),
            ("evtowner", T::Oid),
            ("evttags", T::TextArray),
            ("evtenabled", T::Char),
            ("xmin", T::Xid),
        ],
        _ => return None,
    }))
}
pub(crate) fn known_names() -> &'static [&'static str] {
    &[
        "pg_namespace",
        "pg_class",
        "pg_attribute",
        "pg_type",
        "pg_index",
        "pg_constraint",
        "pg_description",
        "pg_shdescription",
        "pg_attrdef",
        "pg_database",
        "pg_user",
        "pg_language",
        "pg_proc",
        "pg_event_trigger",
        "pg_roles",
        "pg_auth_members",
        "pg_timezone_names",
        "pg_timezone_abbrevs",
        "pg_tablespace",
        "pg_foreign_data_wrapper",
        "pg_foreign_server",
        "pg_user_mappings",
        "pg_am",
        "pg_extension",
        "pg_depend",
        "pg_cast",
        "pg_inherits",
        "pg_foreign_table",
        "pg_opclass",
        "pg_collation",
    ]
}
pub(crate) fn relation_oid(name: &str) -> Option<u32> {
    Some(match name {
        "pg_class" => 1259,
        "pg_type" => 1247,
        "pg_namespace" => 2615,
        "pg_attribute" => 1249,
        "pg_index" => 2610,
        "pg_constraint" => 2606,
        "pg_description" => 2609,
        "pg_attrdef" => 2604,
        "pg_database" => 1262,
        "pg_shdescription" => 2396,
        "pg_language" => 2612,
        "pg_proc" => 1255,
        "pg_event_trigger" => 3466,
        "pg_user" => 12001,
        "pg_roles" => 12002,
        "pg_auth_members" => 1261,
        "pg_timezone_names" => 12003,
        "pg_timezone_abbrevs" => 12004,
        "pg_tablespace" => 1213,
        "pg_foreign_data_wrapper" => 2328,
        "pg_foreign_server" => 1417,
        "pg_user_mappings" => 12005,
        "pg_am" => 2601,
        "pg_extension" => 3079,
        "pg_depend" => 2608,
        "pg_cast" => 2605,
        "pg_inherits" => 2611,
        "pg_foreign_table" => 3118,
        "pg_opclass" => 2616,
        "pg_collation" => 3456,
        _ => return None,
    })
}
pub(crate) fn scalar(t: ScalarType) -> (u32, &'static str, i64, u32) {
    match t {
        ScalarType::Boolean => (16, "bool", 1, 1000),
        ScalarType::SmallInt => (21, "int2", 2, 1005),
        ScalarType::Integer => (23, "int4", 4, 1007),
        ScalarType::BigInt => (20, "int8", 8, 1016),
        ScalarType::Text => (25, "text", -1, 1009),
        ScalarType::Bytes => (17, "bytea", -1, 1001),
    }
}
pub(crate) fn build(snapshot: &CatalogSnapshot) -> R<BTreeMap<String, Relation>> {
    if snapshot.tables.len() > 128 {
        return Err(Error::Limit);
    }
    let mut names = BTreeSet::new();
    let mut ids = BTreeSet::new();
    let mut input_bytes = 0;
    for t in &snapshot.tables {
        if !ids.insert(t.id)
            || !names.insert(&t.name)
            || t.name.is_empty()
            || t.columns.is_empty()
            || t.columns.len() > 64
        {
            return Err(Error::InvalidSnapshot);
        }
        table_oid(t.id)?;
        let mut cn = BTreeSet::new();
        let mut ci = BTreeSet::new();
        let mut pk = BTreeSet::new();
        for c in &t.columns {
            if c.name.is_empty() || !cn.insert(&c.name) || !ci.insert(c.id) {
                return Err(Error::InvalidSnapshot);
            }
            input_bytes += c.name.len();
        }
        for &k in &t.primary_key {
            if k >= t.columns.len() || !pk.insert(k) {
                return Err(Error::InvalidSnapshot);
            }
        }
        input_bytes += t.name.len();
        if input_bytes > 65536 {
            return Err(Error::Limit);
        }
    }
    let mut occupied = snapshot
        .tables
        .iter()
        .map(|t| t.name.clone())
        .collect::<BTreeSet<_>>();
    let mut key_names = BTreeMap::new();
    let mut sorted = snapshot.tables.iter().collect::<Vec<_>>();
    sorted.sort_by_key(|t| t.id);
    for t in sorted {
        if t.primary_key.is_empty() {
            continue;
        }
        let mut name = format!("{}_pkey", t.name);
        let mut suffix = 0usize;
        while occupied.contains(&name) {
            name = format!("{}_pkey_{}_{}", t.name, t.id, suffix);
            suffix += 1;
        }
        occupied.insert(name.clone());
        key_names.insert(t.id, name);
    }
    let mut all: BTreeMap<String, Relation> = known_names()
        .iter()
        .map(|n| {
            (
                (*n).into(),
                Relation {
                    fields: schema(n).unwrap(),
                    rows: vec![],
                },
            )
        })
        .collect();
    let mut add = |name: &str, row: Vec<Value>| {
        all.get_mut(name).unwrap().rows.push(row);
    };
    for (id, name) in [(11, "pg_catalog"), (2200, "public")] {
        // No PostgreSQL MVCC tuple exists: xmin is NULL, never a made-up transaction ID.
        add("pg_namespace", vec![I(id), s(name), I(10), N, N]);
    }
    add(
        "pg_database",
        vec![I(16384), s("giggles"), I(10), B(false), B(true), N],
    );
    add("pg_user", vec![s("giggles"), I(10), B(false)]);
    // One certificate-bound presentation identity; PostgreSQL role management,
    // inheritance, expiration and role connection-limit policies do not exist.
    add(
        "pg_roles",
        vec![
            I(10),
            s("giggles"),
            B(false),
            B(false),
            B(false),
            B(false),
            B(true),
            B(false),
            N,
            N,
            B(false),
            N,
        ],
    );
    // No timezone conversion database exists. Startup timezone presentation
    // labels do not establish a PostgreSQL timezone catalog.
    // Catalog relations are genuine virtual tables, with their actual columns below.
    for &name in known_names() {
        let oid = relation_oid(name).unwrap() as i64;
        let fs = schema(name).unwrap();
        add(
            "pg_class",
            vec![
                I(oid),
                s(name),
                I(11),
                s("r"),
                I(10),
                I(fs.len() as i64),
                B(false),
                s("p"),
                B(false),
                I(32768 + oid),
                I(0),
                I(0),
                N,
                N,
                N,
                N,
            ],
        );
        add(
            "pg_type",
            composite_row((32768 + oid) as u32, name, 11, oid),
        );
        for (i, f) in fs.iter().enumerate() {
            add(
                "pg_attribute",
                vec![
                    I(oid),
                    s(&f.name),
                    I(f.data_type.oid() as i64),
                    I(i as i64 + 1),
                    B(false),
                    I(-1),
                    I(type_len(f.data_type)),
                    B(false),
                    s(""),
                    s(""),
                    B(false),
                    I(0),
                    N,
                    I(0),
                    B(true),
                    N,
                ],
            );
        }
    }
    for ty in [
        ScalarType::Boolean,
        ScalarType::SmallInt,
        ScalarType::Integer,
        ScalarType::BigInt,
        ScalarType::Text,
        ScalarType::Bytes,
    ] {
        let (oid, name, len, array) = scalar(ty);
        add("pg_type", type_row(oid, name, len, 0, array, false));
        add(
            "pg_type",
            type_row(array, &format!("_{name}"), -1, oid, 0, true),
        );
    }
    // Catalog transport types are metadata-only; they are not application column types.
    for (oid, name, len, arr) in [
        (18, "char", 1, 1002),
        (19, "name", 64, 1003),
        (26, "oid", 4, 1028),
        (28, "xid", 4, 1011),
        (1033, "aclitem", 12, 1034),
    ] {
        add("pg_type", type_row(oid, name, len, 0, arr, false));
        add(
            "pg_type",
            type_row(arr, &format!("_{name}"), -1, oid, 0, true),
        );
    }
    add("pg_type", type_row(22, "int2vector", -1, 21, 1006, false));
    add("pg_type", type_row(1006, "_int2vector", -1, 22, 0, true));
    add("pg_type", type_row(30, "oidvector", -1, 26, 1013, false));
    add("pg_type", type_row(1013, "_oidvector", -1, 30, 0, true));
    for t in &snapshot.tables {
        let oid = table_oid(t.id)? as i64;
        let has_pk = !t.primary_key.is_empty();
        add(
            "pg_class",
            vec![
                I(oid),
                s(&t.name),
                I(2200),
                s("r"),
                I(10),
                I(t.columns.len() as i64),
                B(has_pk),
                s("p"),
                B(false),
                I(oid + 3),
                I(0),
                I(0),
                N,
                N,
                N,
                N,
            ],
        );
        add(
            "pg_type",
            composite_row((oid + 3) as u32, &t.name, 2200, oid),
        );
        for (i, c) in t.columns.iter().enumerate() {
            let (ty, _, len, _) = scalar(c.data_type);
            add(
                "pg_attribute",
                vec![
                    I(oid),
                    s(&c.name),
                    I(ty as i64),
                    I(i as i64 + 1),
                    B(c.not_null || t.primary_key.contains(&i)),
                    I(-1),
                    I(len),
                    B(false),
                    s(""),
                    s(""),
                    B(false),
                    I(0),
                    N,
                    I(0),
                    B(true),
                    N,
                ],
            );
        }
        if has_pk {
            let key = Value::Array(t.primary_key.iter().map(|k| I(*k as i64 + 1)).collect());
            let n = &key_names[&t.id];
            add(
                "pg_class",
                vec![
                    I(oid + 1),
                    s(n),
                    I(2200),
                    s("i"),
                    I(10),
                    I(t.primary_key.len() as i64),
                    B(false),
                    s("p"),
                    B(false),
                    I(0),
                    I(0),
                    I(0),
                    N,
                    N,
                    N,
                    N,
                ],
            );
            add(
                "pg_index",
                vec![
                    I(oid + 1),
                    I(oid),
                    I(t.primary_key.len() as i64),
                    I(t.primary_key.len() as i64),
                    B(true),
                    B(true),
                    B(true),
                    key.clone(),
                    N,
                    N,
                    Value::Array(vec![I(0); t.primary_key.len()]),
                    Value::Array(vec![I(0); t.primary_key.len()]),
                    Value::Array(vec![I(0); t.primary_key.len()]),
                ],
            );
            for (i, k) in t.primary_key.iter().enumerate() {
                let c = &t.columns[*k];
                let (ty, _, len, _) = scalar(c.data_type);
                add(
                    "pg_attribute",
                    vec![
                        I(oid + 1),
                        s(&c.name),
                        I(ty as i64),
                        I(i as i64 + 1),
                        B(true),
                        I(-1),
                        I(len),
                        B(false),
                        s(""),
                        s(""),
                        B(false),
                        I(0),
                        N,
                        I(0),
                        B(true),
                        N,
                    ],
                );
            }
            add(
                "pg_constraint",
                vec![
                    I(oid + 2),
                    s(n),
                    I(2200),
                    s("p"),
                    I(oid),
                    I(oid + 1),
                    key,
                    I(0),
                    N,
                    B(false),
                    B(false),
                    B(true),
                    N,
                    I(0),
                    s(" "),
                    s(" "),
                    s(" "),
                    B(true),
                    N,
                    N,
                ],
            );
        }
    }
    let total: usize = all
        .values()
        .flat_map(|r| &r.rows)
        .flat_map(|r| r.iter())
        .map(Value::bytes)
        .sum();
    if total > MAX_BYTES {
        return Err(Error::Limit);
    }
    for relation in all.values() {
        if relation
            .rows
            .iter()
            .any(|r| r.len() != relation.fields.len())
        {
            return Err(Error::InvalidSnapshot);
        }
    }
    Ok(all)
}
fn type_len(t: PgType) -> i64 {
    t.type_size() as i64
}
fn composite_row(oid: u32, name: &str, namespace: i64, relation: i64) -> Vec<Value> {
    vec![
        I(oid as i64),
        s(name),
        I(namespace),
        I(-1),
        s("c"),
        s("C"),
        B(false),
        I(0),
        I(-1),
        I(relation),
        I(0),
        I(0),
        I(0),
        s(","),
        I(10),
        N,
        I(0),
        N,
        B(true),
    ]
}
fn type_row(oid: u32, name: &str, len: i64, elem: u32, array: u32, is_array: bool) -> Vec<Value> {
    vec![
        I(oid as i64),
        s(name),
        I(11),
        I(len),
        s("b"),
        s(if is_array {
            "A"
        } else if oid == 16 {
            "B"
        } else if matches!(oid, 20 | 21 | 23 | 26) {
            "N"
        } else if matches!(oid, 17 | 28 | 1033) {
            "U"
        } else if oid == 18 {
            "Z"
        } else if matches!(oid, 22 | 30) {
            "A"
        } else {
            "S"
        }),
        B(false),
        I(0),
        I(-1),
        I(0),
        I(if oid == 19 { 18 } else { elem as i64 }),
        I(array as i64),
        I(if is_array { 750 } else { 0 }),
        s(","),
        I(10),
        N,
        I(0),
        N,
        B(true),
    ]
}
