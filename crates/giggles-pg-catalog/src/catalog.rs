mod information_schema;
mod pg_tables;
mod constraints;
mod materialize;
use materialize::{Budget, Lookup, oid as row_oid};
pub(crate) use constraints::Definitions;
pub(crate) use materialize::{MAX_CATALOG_BYTES, row_bytes};
use crate::*;
use std::collections::{BTreeMap, BTreeSet, HashMap};
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
const PREDEFINED_ROLES: &[(u32, &str)] = &[
    (3373, "pg_monitor"),
    (3374, "pg_read_all_settings"),
    (3375, "pg_read_all_stats"),
    (3377, "pg_stat_scan_tables"),
    (4200, "pg_signal_backend"),
    (4544, "pg_checkpoint"),
    (4550, "pg_use_reserved_connections"),
    (4569, "pg_read_server_files"),
    (4570, "pg_write_server_files"),
    (4571, "pg_execute_server_program"),
    (6171, "pg_database_owner"),
    (6181, "pg_read_all_data"),
    (6182, "pg_write_all_data"),
    (6304, "pg_create_subscription"),
    (6337, "pg_maintain"),
    (6392, "pg_signal_autovacuum_worker"),
];
pub(crate) fn role_name(snapshot: &CatalogSnapshot, oid: i64) -> Option<&str> {
    snapshot
        .roles
        .iter()
        .find(|r| i64::from(r.oid) == oid)
        .map(|r| r.name.as_str())
        .or_else(|| {
            PREDEFINED_ROLES
                .iter()
                .find(|(id, name)| i64::from(*id) == oid && !snapshot.roles.iter().any(|role| role.name == *name))
                .map(|(_, name)| *name)
        })
}
pub(crate) fn acl_role_name(name: &str) -> String {
    if name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
        name.to_owned()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}
fn acl_value(lookup: &Lookup<'_>, entries: Option<&Vec<AclEntry>>) -> Value {
    entries.map_or(N, |entries| {
        Value::Array(
            entries
                .iter()
                .map(|entry| {
                    let grantee = if entry.grantee == 0 {
                        String::new()
                    } else {
                        acl_role_name(lookup.roles.get(&i64::from(entry.grantee)).copied().unwrap())
                    };
                    let grantor =
                        acl_role_name(lookup.roles.get(&i64::from(entry.grantor)).copied().unwrap());
                    let mut privileges = String::new();
                    for privilege in &entry.privileges {
                        privileges.push(privilege.code());
                        if entry.grant_option {
                            privileges.push('*');
                        }
                    }
                    s(&format!("{grantee}={privileges}/{grantor}"))
                })
                .collect(),
        )
    })
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
    let mut fields = storage_schema(name)?;
    let order = physical_order(name);
    if !order.is_empty() {
        for (name, data_type) in [
            ("relfilenode", PgType::Oid), ("relrewrite", PgType::Oid),
            ("relisshared", PgType::Bool), ("relhassubclass", PgType::Bool),
        ] {
            if order.contains(&name) { fields.push(Field { name: name.into(), data_type }); }
        }
        fields.sort_by_key(|f| order.iter().position(|n| *n == f.name).unwrap_or(order.len()));
        for f in &mut fields {
            if matches!(f.name.as_str(), "relpartbound" | "indexprs" | "indpred" | "conbin") {
                f.data_type = PgType::PgNodeTree;
            }
        }
    }
    Some(fields)
}
// The builders retain their private slot layout; public rows and reflected
// attributes use PostgreSQL 18's physical order, including SELECT *.
fn physical_order(name: &str) -> &'static [&'static str] {
    match name {
        "pg_class" => &[
            "oid", "relname", "relnamespace", "reltype", "reloftype", "relowner", "relam",
            "relfilenode", "reltablespace", "relpages", "reltuples", "relallvisible",
            "relallfrozen", "reltoastrelid", "relhasindex", "relisshared", "relpersistence",
            "relkind", "relnatts", "relchecks", "relhasrules", "relhastriggers", "relhassubclass",
            "relrowsecurity", "relforcerowsecurity", "relispopulated", "relreplident",
            "relispartition", "relrewrite", "relfrozenxid", "relminmxid", "relacl", "reloptions",
            "relpartbound",
        ],
        "pg_constraint" => &[
            "oid", "conname", "connamespace", "contype", "condeferrable", "condeferred",
            "conenforced", "convalidated", "conrelid", "contypid", "conindid", "conparentid",
            "confrelid", "confupdtype", "confdeltype", "confmatchtype", "conislocal", "coninhcount",
            "connoinherit", "conperiod", "conkey", "confkey", "conpfeqop", "conppeqop", "conffeqop",
            "confdelsetcols", "conexclop", "conbin",
        ],
        "pg_index" => &[
            "indexrelid", "indrelid", "indnatts", "indnkeyatts", "indisunique",
            "indnullsnotdistinct", "indisprimary", "indisexclusion", "indimmediate",
            "indisclustered", "indisvalid", "indcheckxmin", "indisready", "indislive",
            "indisreplident", "indkey", "indcollation", "indclass", "indoption", "indexprs",
            "indpred",
        ],
        _ => &[],
    }
}
fn storage_schema(name: &str) -> Option<Vec<Field>> {
    if let Some(fields) = information_schema::schema(name) {
        return Some(fields);
    }
    if name == "pg_tables" {
        return Some(pg_tables::schema());
    }
    let mut result = fields(pg_schema_columns(name)?);
    if tableoid(name).is_some() && !result.iter().any(|f| f.name == "xmin") {
        result.push(Field {
            name: "xmin".into(),
            data_type: T::Xid,
        });
    }
    Some(result)
}

pub(crate) fn pg_schema_columns(name: &str) -> Option<&'static [(&'static str, PgType)]> {
    Some(match name {
        "pg_prepared_statements" => &[
            ("name", T::Text), ("statement", T::Text), ("prepare_time", T::TimestampTz),
            ("parameter_types", T::RegtypeArray), ("result_types", T::RegtypeArray),
            ("from_sql", T::Bool), ("generic_plans", T::Int8), ("custom_plans", T::Int8),
        ],
        "pg_views" => &[("schemaname", T::Name), ("viewname", T::Name), ("viewowner", T::Name), ("definition", T::Text)],
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
            ("relchecks", T::Int2),
            ("relhasrules", T::Bool),
            ("relhastriggers", T::Bool),
            ("relrowsecurity", T::Bool),
            ("relforcerowsecurity", T::Bool),
            ("reloftype", T::Oid),
            ("relreplident", T::Char),
            ("reltoastrelid", T::Oid),
            ("relpages", T::Int4),
            ("reltuples", T::Float4),
            ("relallvisible", T::Int4),
            ("relfrozenxid", T::Xid),
            ("relispopulated", T::Bool),
            ("relminmxid", T::Xid),
            ("relallfrozen", T::Int4),
        ],
        // PostgreSQL 18 physical attribute order; synthetic xmin follows it.
        "pg_attribute" => &[
            ("attrelid", T::Oid),
            ("attname", T::Name),
            ("atttypid", T::Oid),
            ("attlen", T::Int2),
            ("attnum", T::Int2),
            ("atttypmod", T::Int4),
            ("attndims", T::Int2),
            ("attbyval", T::Bool),
            ("attalign", T::Char),
            ("attstorage", T::Char),
            ("attcompression", T::Char),
            ("attnotnull", T::Bool),
            ("atthasdef", T::Bool),
            ("atthasmissing", T::Bool),
            ("attidentity", T::Char),
            ("attgenerated", T::Char),
            ("attisdropped", T::Bool),
            ("attislocal", T::Bool),
            ("attinhcount", T::Int2),
            ("attcollation", T::Oid),
            ("attstattarget", T::Int2),
            ("attacl", T::AclItemArray),
            ("attoptions", T::TextArray),
            ("attfdwoptions", T::TextArray),
            ("attmissingval", T::AnyArray),
            ("xmin", T::Xid),
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
            ("typispreferred", T::Bool),
            ("typacl", T::AclItemArray),
            ("typreceive", T::Oid),
            ("typcollation", T::Oid),
            ("typstorage", T::Char),
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
            ("indisexclusion", T::Bool),
            ("indimmediate", T::Bool),
            ("indisready", T::Bool),
            ("indislive", T::Bool),
            ("indisclustered", T::Bool),
            ("indisreplident", T::Bool),
            ("indcheckxmin", T::Bool),
            ("indnullsnotdistinct", T::Bool),
        ],
        "pg_indexes" => &[
            ("schemaname", T::Name),
            ("tablename", T::Name),
            ("indexname", T::Name),
            ("tablespace", T::Name),
            ("indexdef", T::Text),
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
            ("conparentid", T::Oid),
            ("conperiod", T::Bool),
            ("conenforced", T::Bool),
            ("conislocal", T::Bool),
            ("coninhcount", T::Int2),
            ("conpfeqop", T::OidArray),
            ("conppeqop", T::OidArray),
            ("conffeqop", T::OidArray),
            ("confdelsetcols", T::Int2Array),
        ],
        "pg_description" => &[
            ("objoid", T::Oid),
            ("classoid", T::Oid),
            ("objsubid", T::Int4),
            ("description", T::Text),
            ("xmin", T::Xid),
        ],
        "pg_shdescription" => &[
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
            ("encoding", T::Int4),
            ("datlocprovider", T::Char),
            ("datcollate", T::Text),
            ("datctype", T::Text),
            ("datlocale", T::Text),
            ("daticurules", T::Text),
            ("dattablespace", T::Oid),
        ],
        "pg_user" | "pg_shadow" => &[
            ("usename", T::Name),
            ("usesysid", T::Oid),
            ("usecreatedb", T::Bool),
            ("usesuper", T::Bool),
            ("userepl", T::Bool),
            ("usebypassrls", T::Bool),
            ("passwd", T::Text),
            // No timestamptz descriptor exists; expiration is always NULL.
            ("valuntil", T::Text),
            ("useconfig", T::TextArray),
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
            ("lanowner", T::Oid),
            ("lanispl", T::Bool),
        ],
        "pg_roles" => &[
            ("rolname", T::Name),
            ("rolsuper", T::Bool),
            ("rolinherit", T::Bool),
            ("rolcreaterole", T::Bool),
            ("rolcreatedb", T::Bool),
            ("rolcanlogin", T::Bool),
            ("rolreplication", T::Bool),
            ("rolconnlimit", T::Int4),
            ("rolpassword", T::Text),
            ("rolvaliduntil", T::Text),
            ("rolbypassrls", T::Bool),
            ("rolconfig", T::TextArray),
            ("oid", T::Oid),
        ],
        "pg_authid" => &[
            ("oid", T::Oid),
            ("rolname", T::Name),
            ("rolsuper", T::Bool),
            ("rolinherit", T::Bool),
            ("rolcreaterole", T::Bool),
            ("rolcreatedb", T::Bool),
            ("rolcanlogin", T::Bool),
            ("rolreplication", T::Bool),
            ("rolbypassrls", T::Bool),
            ("rolconnlimit", T::Int4),
            ("rolpassword", T::Text),
            ("rolvaliduntil", T::Text),
        ],
        "pg_auth_members" => &[
            ("oid", T::Oid),
            ("roleid", T::Oid),
            ("member", T::Oid),
            ("grantor", T::Oid),
            ("admin_option", T::Bool),
            ("inherit_option", T::Bool),
            ("set_option", T::Bool),
        ],
        "pg_timezone_names" => &[("name", T::Text), ("abbrev", T::Text), ("utc_offset", T::Interval), ("is_dst", T::Bool)],
        "pg_timezone_abbrevs" => &[("abbrev", T::Text), ("utc_offset", T::Interval), ("is_dst", T::Bool)],
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
        "pg_user_mapping" => &[
            ("oid", T::Oid),
            ("umuser", T::Oid),
            ("umserver", T::Oid),
            ("umoptions", T::TextArray),
        ],
        // Relation locks come from the engine; transaction xid locks are not exposed.
        "pg_locks" => &[
            ("locktype", T::Text),
            ("database", T::Oid),
            ("relation", T::Oid),
            ("page", T::Int4),
            ("tuple", T::Int2),
            ("virtualxid", T::Text),
            ("transactionid", T::Xid),
            ("classid", T::Oid),
            ("objid", T::Oid),
            ("objsubid", T::Int2),
            ("virtualtransaction", T::Text),
            ("pid", T::Int4),
            ("mode", T::Text),
            ("granted", T::Bool),
            ("fastpath", T::Bool),
            ("waitstart", T::TimestampTz),
        ],
        "pg_stat_ssl" => &[
            ("pid", T::Int4),
            ("ssl", T::Bool),
            ("version", T::Text),
            ("cipher", T::Text),
            ("bits", T::Int4),
            ("client_dn", T::Text),
            ("client_serial", T::Numeric),
            ("issuer_dn", T::Text),
        ],
        "pg_stat_gssapi" => &[
            ("pid", T::Int4),
            ("gss_authenticated", T::Bool),
            ("principal", T::Text),
            ("encrypted", T::Bool),
            ("credentials_delegated", T::Bool),
        ],
        "pg_replication_slots" => &[("slot_name", T::Name)],
        "pg_user_mappings" => &[
            ("umid", T::Oid),
            ("srvid", T::Oid),
            ("usename", T::Name),
            ("umoptions", T::TextArray),
        ],
        "pg_am" => &[
            ("oid", T::Oid),
            ("amname", T::Name),
            ("amhandler", T::Regproc),
            ("amtype", T::Char),
            ("xmin", T::Xid),
        ],
        "pg_available_extensions" => &[("name", T::Name), ("default_version", T::Text), ("installed_version", T::Text), ("comment", T::Text)],
        "pg_available_extension_versions" => &[("name", T::Name), ("version", T::Text), ("installed", T::Bool),
            ("superuser", T::Bool), ("trusted", T::Bool), ("relocatable", T::Bool), ("schema", T::Name), ("requires", T::NameArray), ("comment", T::Text)],
        "pg_extension" => &[
            ("oid", T::Oid),
            ("extname", T::Name),
            ("extowner", T::Oid),
            ("extnamespace", T::Oid),
            ("extrelocatable", T::Bool),
            ("extversion", T::Text),
            ("extconfig", T::OidArray),
            ("extcondition", T::TextArray),
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
        // No extension initial privileges or SQL transforms are installed.
        "pg_partitioned_table" => &[
            ("partrelid", T::Oid),
            ("partstrat", T::Char),
            ("partnatts", T::Int2),
            ("partdefid", T::Oid),
            ("partattrs", T::Int2Vector),
            ("partclass", T::OidVector),
            ("partcollation", T::OidVector),
            ("partexprs", T::PgNodeTree),
        ],
        "pg_statistic_ext" => &[
            ("oid", T::Oid),
            ("stxrelid", T::Oid),
            ("stxname", T::Name),
            ("stxnamespace", T::Oid),
            ("stxowner", T::Oid),
            ("stxkeys", T::Int2Vector),
            ("stxstattarget", T::Int2),
            ("stxkind", T::CharArray),
            ("stxexprs", T::PgNodeTree),
        ],
        "pg_publication" => &[
            ("oid", T::Oid),
            ("pubname", T::Name),
            ("pubowner", T::Oid),
            ("puballtables", T::Bool),
            ("pubinsert", T::Bool),
            ("pubupdate", T::Bool),
            ("pubdelete", T::Bool),
            ("pubtruncate", T::Bool),
            ("pubviaroot", T::Bool),
            ("pubgencols", T::Char),
        ],
        "pg_publication_namespace" => &[("oid", T::Oid), ("pnpubid", T::Oid), ("pnnspid", T::Oid)],
        "pg_publication_rel" => &[
            ("oid", T::Oid),
            ("prpubid", T::Oid),
            ("prrelid", T::Oid),
            ("prqual", T::PgNodeTree),
            ("prattrs", T::Int2Vector),
        ],
        "pg_subscription" => &[
            ("oid", T::Oid),
            ("subdbid", T::Oid),
            ("subskiplsn", T::PgLsn),
            ("subname", T::Name),
            ("subowner", T::Oid),
            ("subenabled", T::Bool),
            ("subbinary", T::Bool),
            ("substream", T::Char),
            ("subtwophasestate", T::Char),
            ("subdisableonerr", T::Bool),
            ("subpasswordrequired", T::Bool),
            ("subrunasowner", T::Bool),
            ("subfailover", T::Bool),
            ("subconninfo", T::Text),
            ("subslotname", T::Name),
            ("subsynccommit", T::Text),
            ("subpublications", T::TextArray),
            ("suborigin", T::Text),
        ],
        "pg_seclabels" => &[
            ("objoid", T::Oid),
            ("classoid", T::Oid),
            ("objsubid", T::Int4),
            ("objtype", T::Text),
            ("objnamespace", T::Oid),
            ("objname", T::Text),
            ("provider", T::Text),
            ("label", T::Text),
        ],
        "pg_conversion" => &[
            ("oid", T::Oid),
            ("conname", T::Name),
            ("connamespace", T::Oid),
            ("conowner", T::Oid),
            ("conforencoding", T::Int4),
            ("contoencoding", T::Int4),
            ("conproc", T::Regproc),
            ("condefault", T::Bool),
        ],
        "pg_default_acl" => &[
            ("oid", T::Oid),
            ("defaclrole", T::Oid),
            ("defaclnamespace", T::Oid),
            ("defaclobjtype", T::Char),
            ("defaclacl", T::AclItemArray),
        ],
        "pg_init_privs" => &[
            ("objoid", T::Oid),
            ("classoid", T::Oid),
            ("objsubid", T::Int4),
            ("privtype", T::Char),
            ("initprivs", T::AclItemArray),
        ],
        "pg_transform" => &[
            ("oid", T::Oid),
            ("trftype", T::Oid),
            ("trflang", T::Oid),
            ("trffromsql", T::Regproc),
            ("trftosql", T::Regproc),
        ],
        // No text-search configurations, dictionaries or parser runtimes are installed.
        "pg_ts_config" => &[
            ("oid", T::Oid),
            ("cfgname", T::Name),
            ("cfgnamespace", T::Oid),
            ("cfgowner", T::Oid),
            ("cfgparser", T::Oid),
        ],
        "pg_ts_config_map" => &[
            ("mapcfg", T::Oid),
            ("maptokentype", T::Int4),
            ("mapseqno", T::Int4),
            ("mapdict", T::Oid),
        ],
        "pg_ts_dict" => &[
            ("oid", T::Oid),
            ("dictname", T::Name),
            ("dictnamespace", T::Oid),
            ("dictowner", T::Oid),
            ("dicttemplate", T::Oid),
            ("dictinitoption", T::Text),
        ],
        "pg_ts_parser" => &[
            ("oid", T::Oid),
            ("prsname", T::Name),
            ("prsnamespace", T::Oid),
            ("prsstart", T::Regproc),
            ("prstoken", T::Regproc),
            ("prsend", T::Regproc),
            ("prsheadline", T::Regproc),
            ("prslextype", T::Regproc),
        ],
        "pg_ts_template" => &[
            ("oid", T::Oid),
            ("tmplname", T::Name),
            ("tmplnamespace", T::Oid),
            ("tmplinit", T::Regproc),
            ("tmpllexize", T::Regproc),
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
            ("inhdetachpending", T::Bool),
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
            ("opcowner", T::Oid),
            ("opcfamily", T::Oid),
            ("opcintype", T::Oid),
            ("opckeytype", T::Oid),
            ("xmin", T::Xid),
        ],
        "pg_collation" => &[
            ("oid", T::Oid),
            ("collname", T::Name),
            ("collnamespace", T::Oid),
            ("collowner", T::Oid),
            ("collprovider", T::Char),
            ("collisdeterministic", T::Bool),
            ("collencoding", T::Int4),
            ("collcollate", T::Text),
            ("collctype", T::Text),
            ("colllocale", T::Text),
            ("collicurules", T::Text),
            ("collversion", T::Text),
            ("xmin", T::Xid),
        ],
        // No user routines are modeled. The callable SQL registry is not a snapshot
        // of PostgreSQL builtins; this empty relation is not a complete pg_proc.
        "pg_proc" => &[
            ("oid", T::Oid),
            ("proname", T::Name),
            ("pronamespace", T::Oid),
            ("proowner", T::Oid),
            ("prolang", T::Oid),
            ("procost", T::Float4),
            ("prorows", T::Float4),
            ("provariadic", T::Oid),
            ("prokind", T::Char),
            ("prosecdef", T::Bool),
            ("proleakproof", T::Bool),
            ("proisstrict", T::Bool),
            ("proretset", T::Bool),
            ("provolatile", T::Char),
            ("proparallel", T::Char),
            ("pronargs", T::Int2),
            ("pronargdefaults", T::Int2),
            ("prorettype", T::Oid),
            ("proargtypes", T::OidVector),
            ("proallargtypes", T::OidArray),
            ("proargmodes", T::CharArray),
            ("proargnames", T::TextArray),
            ("protrftypes", T::OidArray),
            ("prosrc", T::Text),
            ("probin", T::Text),
            ("proconfig", T::TextArray),
            ("proacl", T::AclItemArray),
            ("xmin", T::Xid),
            ("prosupport", T::Regproc),
            ("proargdefaults", T::PgNodeTree),
            ("prosqlbody", T::PgNodeTree),
        ],
        // Nominal PostgreSQL 18 descriptors preserve regproc/pg_node_tree identity;
        // these absent features still contribute no invented rows.
        "pg_aggregate" => &[
            ("aggkind", T::Char),
            ("aggnumdirectargs", T::Int2),
            ("aggfinalextra", T::Bool),
            ("aggmfinalextra", T::Bool),
            ("aggfinalmodify", T::Char),
            ("aggmfinalmodify", T::Char),
            ("aggsortop", T::Oid),
            ("aggtranstype", T::Oid),
            ("aggtransspace", T::Int4),
            ("aggmtranstype", T::Oid),
            ("aggmtransspace", T::Int4),
            ("agginitval", T::Text),
            ("aggminitval", T::Text),
            ("xmin", T::Xid),
            ("aggfnoid", T::Regproc),
            ("aggtransfn", T::Regproc),
            ("aggfinalfn", T::Regproc),
            ("aggcombinefn", T::Regproc),
            ("aggserialfn", T::Regproc),
            ("aggdeserialfn", T::Regproc),
            ("aggmtransfn", T::Regproc),
            ("aggminvtransfn", T::Regproc),
            ("aggmfinalfn", T::Regproc),
        ],
        "pg_amop" => &[
            ("oid", T::Oid),
            ("amopfamily", T::Oid),
            ("amoplefttype", T::Oid),
            ("amoprighttype", T::Oid),
            ("amopstrategy", T::Int2),
            ("amoppurpose", T::Char),
            ("amopopr", T::Oid),
            ("amopmethod", T::Oid),
            ("amopsortfamily", T::Oid),
            ("xmin", T::Xid),
        ],
        "pg_amproc" => &[
            ("oid", T::Oid),
            ("amprocfamily", T::Oid),
            ("amproclefttype", T::Oid),
            ("amprocrighttype", T::Oid),
            ("amprocnum", T::Int2),
            ("xmin", T::Xid),
            ("amproc", T::Regproc),
        ],
        "pg_operator" => &[
            ("oid", T::Oid),
            ("oprname", T::Name),
            ("oprnamespace", T::Oid),
            ("oprowner", T::Oid),
            ("oprkind", T::Char),
            ("oprcanmerge", T::Bool),
            ("oprcanhash", T::Bool),
            ("oprleft", T::Oid),
            ("oprright", T::Oid),
            ("oprresult", T::Oid),
            ("oprcom", T::Oid),
            ("oprnegate", T::Oid),
            ("xmin", T::Xid),
            ("oprcode", T::Regproc),
            ("oprrest", T::Regproc),
            ("oprjoin", T::Regproc),
        ],
        "pg_opfamily" => &[
            ("oid", T::Oid),
            ("opfmethod", T::Oid),
            ("opfname", T::Name),
            ("opfnamespace", T::Oid),
            ("opfowner", T::Oid),
            ("xmin", T::Xid),
        ],
        "pg_policy" => &[
            ("oid", T::Oid),
            ("polname", T::Name),
            ("polrelid", T::Oid),
            ("polcmd", T::Char),
            ("polpermissive", T::Bool),
            ("polroles", T::OidArray),
            ("xmin", T::Xid),
            ("polqual", T::PgNodeTree),
            ("polwithcheck", T::PgNodeTree),
        ],
        "pg_rewrite" => &[
            ("oid", T::Oid),
            ("rulename", T::Name),
            ("ev_class", T::Oid),
            ("ev_type", T::Char),
            ("ev_enabled", T::Char),
            ("is_instead", T::Bool),
            ("xmin", T::Xid),
            ("ev_qual", T::PgNodeTree),
            ("ev_action", T::PgNodeTree),
        ],
        "pg_range" => &[
            ("rngtypid", T::Oid),
            ("rngsubtype", T::Oid),
            ("rngmultitypid", T::Oid),
            ("rngcollation", T::Oid),
            ("rngsubopc", T::Oid),
            ("rngcanonical", T::Regproc),
            ("rngsubdiff", T::Regproc),
        ],
        "pg_enum" => &[
            ("oid", T::Oid),
            ("enumtypid", T::Oid),
            ("enumsortorder", T::Float4),
            ("enumlabel", T::Name),
        ],
        "pg_sequence" => &[
            ("seqrelid", T::Oid),
            ("seqtypid", T::Oid),
            ("seqstart", T::Int8),
            ("seqincrement", T::Int8),
            ("seqmax", T::Int8),
            ("seqmin", T::Int8),
            ("seqcache", T::Int8),
            ("seqcycle", T::Bool),
            ("xmin", T::Xid),
        ],
        "pg_trigger" => &[
            ("oid", T::Oid),
            ("tgrelid", T::Oid),
            ("tgparentid", T::Oid),
            ("tgname", T::Name),
            ("tgfoid", T::Oid),
            ("tgtype", T::Int2),
            ("tgenabled", T::Char),
            ("tgisinternal", T::Bool),
            ("tgconstrrelid", T::Oid),
            ("tgconstrindid", T::Oid),
            ("tgconstraint", T::Oid),
            ("tgdeferrable", T::Bool),
            ("tginitdeferred", T::Bool),
            ("tgnargs", T::Int2),
            ("tgattr", T::Int2Vector),
            ("tgargs", T::Bytea),
            ("tgoldtable", T::Name),
            ("tgnewtable", T::Name),
            ("xmin", T::Xid),
            ("tgqual", T::PgNodeTree),
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
    })
}
pub(crate) fn known_names() -> &'static [&'static str] {
    &[
        "views",
        "pg_views",
        "schemata",
        "tables",
        "columns",
        "table_constraints",
        "key_column_usage",
        "pg_namespace",
        "pg_class",
        "pg_attribute",
        "pg_type",
        "pg_index",
        "pg_indexes",
        "pg_tables",
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
        "pg_authid",
        "pg_shadow",
        "pg_auth_members",
        "pg_timezone_names",
        "pg_timezone_abbrevs",
        "pg_tablespace",
        "pg_foreign_data_wrapper",
        "pg_foreign_server",
        "pg_user_mappings",
        "pg_user_mapping",
        "pg_locks",
        "pg_stat_ssl",
        "pg_stat_gssapi",
        "pg_replication_slots",
        "pg_am",
        "pg_extension",
        "pg_available_extensions",
        "pg_available_extension_versions",
        "pg_depend",
        "pg_cast",
        "pg_ts_config",
        "pg_ts_config_map",
        "pg_ts_dict",
        "pg_ts_parser",
        "pg_ts_template",
        "pg_init_privs",
        "pg_default_acl",
        "pg_conversion",
        "pg_seclabels",
        "pg_subscription",
        "pg_partitioned_table",
        "pg_transform",
        "pg_inherits",
        "pg_foreign_table",
        "pg_opclass",
        "pg_collation",
        "pg_aggregate",
        "pg_amop",
        "pg_amproc",
        "pg_operator",
        "pg_opfamily",
        "pg_policy",
        "pg_rewrite",
        "pg_sequence",
        "pg_range",
        "pg_enum",
        "pg_trigger",
    ]
}
pub(crate) fn relation_namespace(name: &str) -> &'static str {
    if information_schema::identity(name).is_some() {
        "information_schema"
    } else {
        "pg_catalog"
    }
}
/// Only physical catalog tables have system columns; catalog views do not.
pub(crate) fn tableoid(name: &str) -> Option<u32> {
    if relation_namespace(name) != "pg_catalog"
        || matches!(
            name,
            "pg_available_extensions" | "pg_available_extension_versions" | "pg_indexes" | "pg_tables" | "pg_views"
                | "pg_seclabels"
                | "pg_user"
                | "pg_roles"
                | "pg_shadow"
                | "pg_timezone_names"
                | "pg_timezone_abbrevs"
                | "pg_user_mappings"
                | "pg_locks"
                | "pg_stat_ssl"
                | "pg_stat_gssapi"
                | "pg_replication_slots"
        )
    {
        None
    } else {
        relation_oid(name)
    }
}
pub(crate) fn relation_oid(name: &str) -> Option<u32> {
    if let Some((oid, _)) = information_schema::identity(name) {
        return Some(oid);
    }
    Some(match name {
        "pg_class" => 1259,
        "pg_type" => 1247,
        "pg_namespace" => 2615,
        "pg_attribute" => 1249,
        "pg_index" => 2610,
        "pg_indexes" => 12006,
        "pg_tables" => 12033,
        "pg_views" => 12028,
        "pg_constraint" => 2606,
        "pg_description" => 2609,
        "pg_attrdef" => 2604,
        "pg_database" => 1262,
        "pg_shdescription" => 2396,
        "pg_language" => 2612,
        "pg_proc" => 1255,
        "pg_event_trigger" => 3466,
        "pg_user" => 12014,
        "pg_roles" => 12000,
        "pg_authid" => 1260,
        "pg_shadow" => 12005,
        "pg_auth_members" => 1261,
        "pg_timezone_names" => 12003,
        "pg_timezone_abbrevs" => 12004,
        "pg_tablespace" => 1213,
        "pg_foreign_data_wrapper" => 2328,
        "pg_foreign_server" => 1417,
        "pg_user_mappings" => 12338,
        "pg_user_mapping" => 1418,
        // Stable virtual view identities, not persisted PostgreSQL catalog OIDs.
        "pg_locks" => 12050,
        "pg_available_extensions" => 12052,
        "pg_available_extension_versions" => 12053,
        "pg_stat_ssl" => 12051,
        "pg_stat_gssapi" => 12257,
        "pg_replication_slots" => 12261,
        "pg_am" => 2601,
        "pg_extension" => 3079,
        "pg_depend" => 2608,
        "pg_cast" => 2605,
        "pg_ts_config" => 3602,
        "pg_ts_config_map" => 3603,
        "pg_ts_dict" => 3600,
        "pg_ts_parser" => 3601,
        "pg_ts_template" => 3764,

        "pg_init_privs" => 3394,
        "pg_default_acl" => 826,
        "pg_conversion" => 2607,
        "pg_seclabels" => 12099,
        "pg_subscription" => 6100,
        "pg_publication" => 6104,
        "pg_publication_namespace" => 6237,
        "pg_publication_rel" => 6106,

        "pg_partitioned_table" => 3350,
        "pg_statistic_ext" => 3381,

        "pg_transform" => 3576,
        "pg_inherits" => 2611,
        "pg_foreign_table" => 3118,
        "pg_opclass" => 2616,
        "pg_collation" => 3456,
        "pg_aggregate" => 2600,
        "pg_amop" => 2602,
        "pg_amproc" => 2603,
        "pg_operator" => 2617,
        "pg_opfamily" => 2753,
        "pg_policy" => 3256,
        "pg_rewrite" => 2618,
        "pg_sequence" => 2224,
        "pg_range" => 3541,
        "pg_enum" => 3501,
        "pg_trigger" => 2620,
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
        ScalarType::Varchar => (1043, "varchar", -1, 1015),
        ScalarType::Bpchar => (1042, "bpchar", -1, 1014),
        ScalarType::Uuid => (2950, "uuid", 16, 2951),
        ScalarType::Jsonb => (3802, "jsonb", -1, 3807),
        ScalarType::IntArray => (1007, "_int4", -1, 0),
        ScalarType::BigIntArray => (1016, "_int8", -1, 0),
        ScalarType::TextArray => (1009, "_text", -1, 0),
        ScalarType::UuidArray => (2951, "_uuid", -1, 0),
        ScalarType::TsVector => (3614, "tsvector", -1, 3643),
        ScalarType::TsQuery => (3615, "tsquery", -1, 3645),
        ScalarType::Bytes => (17, "bytea", -1, 1001),
        ScalarType::Numeric => (1700, "numeric", -1, 1231),
        ScalarType::Real => (700, "float4", 4, 1021),
        ScalarType::DoublePrecision => (701, "float8", 8, 1022),
        ScalarType::Date => (1082, "date", 4, 1182),
        ScalarType::Time => (1083, "time", 8, 1183),
        ScalarType::TimeTz => (1266, "timetz", 12, 1270),
        ScalarType::Timestamp => (1114, "timestamp", 8, 1115),
        ScalarType::TimestampTz => (1184, "timestamptz", 8, 1185),
        ScalarType::Interval => (1186, "interval", 16, 1187),
    }
}
type Relations = BTreeMap<String, Relation>;

fn rows<'a>(all: &'a Relations, name: &str) -> &'a [Vec<Value>] {
    all.get(name).map_or(&[], |r| &r.rows)
}

fn rows_mut<'a>(all: &'a mut Relations, name: &str) -> &'a mut [Vec<Value>] {
    all.get_mut(name).map_or(&mut [], |r| &mut r.rows)
}

pub(crate) fn build(
    snapshot: &CatalogSnapshot,
    requested: &BTreeSet<String>,
) -> R<Relations> {
    build_with_budget(snapshot, requested, &mut Budget::default())
}

fn build_with_budget(
    snapshot: &CatalogSnapshot,
    requested: &BTreeSet<String>,
    budget: &mut Budget,
) -> R<Relations> {
    let mut demand = requested.clone();
    for (name, dependencies) in [
        ("pg_operator", &["pg_constraint"][..]),
        ("pg_depend", &["pg_constraint"]),
        ("columns", &["pg_attribute", "pg_attrdef", "pg_type", "pg_collation", "pg_class"]),
        ("schemata", &["pg_namespace"]),
        ("tables", &["pg_class"]),
        ("pg_tables", &["pg_class"]),
    ] {
        if demand.contains(name) {
            demand.extend(dependencies.iter().map(|name| (*name).to_owned()));
        }
    }
    if demand.contains("pg_prepared_statements") && !snapshot.prepared_statements.is_empty() {
        demand.insert("pg_type".into());
    }
    let needs = |name: &str| demand.contains(name);
    if snapshot.schema_privileges.len() > MAX_ROWS
        || snapshot.schema_privileges.keys().any(|(_,name)| name.len() > 63)
        || snapshot.roles.len() > MAX_ROWS - PREDEFINED_ROLES.len()
    {
        return Err(Error::Limit);
    }
    let lookup = Lookup::new(snapshot, budget)?;
    let supplied_roles: BTreeSet<_> = snapshot.roles.iter().map(|role| role.oid).collect();
    let mut sequence_oids = BTreeSet::new();
    for sequence in &snapshot.sequences {
        budget.visit(1)?;
        if sequence.name.len() > 128 || sequence.name.is_empty() || sequence.oid < 65536
            || !sequence_oids.insert(sequence.oid)
            || lookup.relations.contains_key(&i64::from(sequence.oid))
            || !supplied_roles.contains(&sequence.owner) {
            return Err(Error::InvalidSnapshot);
        }
        if sequence.acl.as_ref().is_some_and(|entries| entries.iter().map(|e| 1+e.privileges.len()).sum::<usize>() > MAX_ROWS) { return Err(Error::Limit); }
    }
    if snapshot.primary_names.iter().any(|(id, name)| name.is_empty() || name.len() > 63 || !lookup.tables.contains_key(id)) {
        return Err(Error::InvalidSnapshot);
    }
    let mut schema_oids = BTreeSet::new();
    for (name, (oid, owner)) in &snapshot.schemas {
        if name.is_empty()
            || name.len() > 63
            || name == "public"
            || *oid < 65536
            || !schema_oids.insert(*oid)
            || !supplied_roles.contains(owner)
        {
            return Err(Error::InvalidSnapshot);
        }
    }
    if snapshot.table_schemas.iter().any(|(id, name)| {
        !lookup.tables.contains_key(id) || !snapshot.schemas.contains_key(name)
    }) {
        return Err(Error::InvalidSnapshot);
    }
    if snapshot.views.iter().any(|(id, sql)| sql.len() > 65536 || sql.is_empty()
        || !lookup.tables.get(id).is_some_and(|t| t.primary_key.is_empty()
            && t.columns.iter().all(|c| !c.not_null))
        || lookup.indexes_by_table.contains_key(id)
        || lookup.constrained_tables.contains(id)) { return Err(Error::InvalidSnapshot); }
    let mut names = BTreeSet::new();
    let mut ids = BTreeSet::new();
    let mut input_bytes = 0usize;
    let mut role_ids = BTreeSet::new();
    let mut role_names = BTreeSet::new();
    for role in &snapshot.roles {
        if role.name.is_empty()
            || PREDEFINED_ROLES.iter().any(|(oid, name)| *oid == role.oid && *name != role.name)
            || !role_ids.insert(role.oid)
            || !role_names.insert(&role.name)
        {
            return Err(Error::InvalidSnapshot);
        }
        input_bytes = input_bytes
            .checked_add(role.name.len())
            .ok_or(Error::Limit)?;
        if input_bytes > 65536 {
            return Err(Error::Limit);
        }
    }
    if !role_ids.contains(&10) || !role_ids.contains(&snapshot.database_owner) {
        return Err(Error::InvalidSnapshot);
    }
    let mut type_oids = BTreeSet::new();
    let mut type_names = BTreeSet::new();
    for ty in &snapshot.user_types {
        if ty.oid < 65536
            || (ty.oid - 65536) % 4 != 0
            || !type_oids.insert(ty.oid)
            || !type_names.insert(&ty.name)
            || ty.name.is_empty()
            || ty.name.len() > 63
            || !matches!(ty.kind, 'e' | 'd' | 'c')
            || ty.schema != "public" && !snapshot.schemas.contains_key(&ty.schema)
            || ty.labels.len() > 64
        {
            return Err(Error::InvalidSnapshot);
        }
        let mut labels = BTreeSet::new();
        for label in &ty.labels {
            if label.len() > 63 || !labels.insert(label) {
                return Err(Error::InvalidSnapshot);
            }
        }
        if ty.label_oids.len() != ty.labels.len()
            || ty.label_oids.iter().any(|&oid| oid < 65536 || (oid - 65536) % 4 != 0)
        {
            return Err(Error::InvalidSnapshot);
        }
    }
    for t in &snapshot.tables {
        if !ids.insert(t.id)
            || !names.insert((snapshot.table_schema(t.id).0, &t.name))
            || t.name.is_empty()
            || t.columns.is_empty() && !snapshot.collation_oids.get(&t.id).is_some_and(Vec::is_empty)
            || t.columns.len() > 64
        {
            return Err(Error::InvalidSnapshot);
        }
        if schema_oids.contains(&table_oid(t.id)?) {
            return Err(Error::InvalidSnapshot);
        }
        let mut cn = BTreeSet::new();
        let mut ci = BTreeSet::new();
        let mut pk = BTreeSet::new();
        for c in &t.columns {
            if c.name.is_empty() || !cn.insert(&c.name) || !ci.insert(c.id) {
                return Err(Error::InvalidSnapshot);
            }
        }
        for &k in &t.primary_key {
            if k >= t.columns.len() || !pk.insert(k) {
                return Err(Error::InvalidSnapshot);
            }
        }
    }
    // Table and descriptor IDs share the core allocator. The same four-slot
    // encoding is injective; reject malformed snapshots that reuse an ID.
    let visible_indexes = snapshot
        .indexes
        .iter()
        .filter(|i| ids.contains(&i.table))
        .collect::<Vec<_>>();
    let mut descriptor_ids = ids.clone();
    let mut descriptor_names = BTreeSet::new();
    for i in &visible_indexes {
        let t = lookup.tables[&i.table];
        if !descriptor_ids.insert(i.id)
            || !descriptor_names.insert((snapshot.table_schema(i.table).0, &i.name))
            || i.name.is_empty()
            || i.columns.is_empty()
            || i.columns.len() > 64
            || i.columns.iter().collect::<BTreeSet<_>>().len() != i.columns.len()
            || i.columns
                .iter()
                .any(|id| lookup.column(t, *id).is_none())
        {
            return Err(Error::InvalidSnapshot);
        }
        if schema_oids.contains(&table_oid(i.id)?) {
            return Err(Error::InvalidSnapshot);
        }
    }
    if snapshot
        .table_owners
        .iter()
        .any(|(table, owner)| !ids.contains(table) || !role_ids.contains(owner))
    {
        return Err(Error::InvalidSnapshot);
    }
    let mut acl_size = 0usize;
    let mut acl_bytes = 0usize;
    for (table, column, entries) in snapshot
        .table_acls
        .iter()
        .map(|(t, a)| (*t, None, a))
        .chain(
            snapshot
                .column_acls
                .iter()
                .map(|((t, c), a)| (*t, Some(*c), a)),
        )
    {
        let Some(t) = lookup.tables.get(&table).copied() else {
            return Err(Error::InvalidSnapshot);
        };
        if column.is_some_and(|id| lookup.column(t, id).is_none()) {
            return Err(Error::InvalidSnapshot);
        }
        for entry in entries {
            acl_size = acl_size.saturating_add(1 + entry.privileges.len());
            if acl_size > MAX_ROWS {
                return Err(Error::Limit);
            }
            if (entry.grantee != 0 && lookup.roles.get(&i64::from(entry.grantee)).copied().is_none())
                || lookup.roles.get(&i64::from(entry.grantor)).copied().is_none()
                || entry.privileges.iter().collect::<BTreeSet<_>>().len() != entry.privileges.len()
                || (column.is_some()
                    && entry.privileges.iter().any(|p| {
                        !matches!(
                            p,
                            AclPrivilege::Select
                                | AclPrivilege::Insert
                                | AclPrivilege::Update
                                | AclPrivilege::References
                        )
                    }))
            {
                return Err(Error::InvalidSnapshot);
            }
            // Bound encoded ACL allocation before constructing the catalog rows;
            // repeated long role names must not multiply the input-name budget.
            let grantee = lookup.roles.get(&i64::from(entry.grantee)).copied().map_or(0, str::len);
            let grantor = lookup.roles.get(&i64::from(entry.grantor)).copied().unwrap().len();
            acl_bytes =
                acl_bytes.saturating_add(2 * (grantee + grantor) + 2 * entry.privileges.len() + 64);
            if acl_bytes > MAX_BYTES {
                return Err(Error::Limit);
            }
        }
    }
    // Every visible name is materialized at least once; bound the aggregate
    // before allocating derived names and index definitions.
    let names = std::iter::once(snapshot.database.as_str())
        .chain(snapshot.tables.iter().flat_map(|table| {
            std::iter::once(table.name.as_str()).chain(table.columns.iter().map(|c| c.name.as_str()))
        }))
        .chain(visible_indexes.iter().map(|i| i.name.as_str()));
    names.map(str::len).try_fold(0usize, |sum, len| {
        sum.checked_add(len)
            .filter(|&total| len <= MAX_SQL && total <= MAX_BYTES / 2)
            .ok_or(Error::Limit)
    })?;
    let mut key_names = BTreeMap::new();
    if ["pg_class", "pg_indexes", "pg_constraint", "table_constraints", "key_column_usage"]
        .into_iter().any(needs)
    {
        let mut occupied = snapshot
            .tables
            .iter()
            .map(|t| (snapshot.table_schema(t.id).0, t.name.clone()))
            .collect::<BTreeSet<_>>();
        occupied.extend(
            visible_indexes
                .iter()
                .map(|i| (snapshot.table_schema(i.table).0, i.name.clone())),
        );
        let mut sorted = snapshot.tables.iter().collect::<Vec<_>>();
        sorted.sort_by_key(|t| t.id);
        for t in sorted {
            if t.primary_key.is_empty() {
                continue;
            }
            let mut name = format!("{}_pkey", t.name);
            let mut suffix = 0usize;
            while occupied.contains(&(snapshot.table_schema(t.id).0, name.clone())) {
                name = format!("{}_pkey_{}_{}", t.name, t.id, suffix);
                suffix += 1;
                budget.visit(1)?;
            }
            occupied.insert((snapshot.table_schema(t.id).0, name.clone()));
            key_names.insert(t.id, snapshot.primary_names.get(&t.id).cloned().unwrap_or(name));
        }
    }
    let mut all: Relations = demand.iter().map(|name| {
        Ok((name.clone(), Relation {
            fields: storage_schema(name).ok_or(Error::Unsupported)?,
            rows: Vec::new(),
        }))
    }).collect::<R<_>>()?;
    macro_rules! add {
        ($name:expr, $row:expr $(,)?) => {
            if let Some(relation) = all.get_mut($name) {
                budget.add(&mut relation.rows, $row)?;
            }
        };
    }
    for ext in &snapshot.extensions {
        budget.visit(1 + ext.versions.len())?;
        if let Some((oid, owner, namespace, version)) = &ext.installed {
            add!("pg_extension", vec![I(i64::from(*oid)), s(&ext.name), I(i64::from(*owner)), I(i64::from(*namespace)), B(ext.relocatable), s(version), N, N, N]);
        }
        add!("pg_available_extensions", vec![s(&ext.name), s(&ext.default_version), ext.installed.as_ref().map_or(N, |i| s(&i.3)), s(&ext.comment)]);
        for version in &ext.versions {
            add!("pg_available_extension_versions", vec![s(&ext.name), s(version), B(ext.installed.as_ref().is_some_and(|i| &i.3 == version)),
                B(ext.superuser), B(ext.trusted), B(ext.relocatable), ext.schema.as_ref().map_or(N, |v| s(v)), N, s(&ext.comment)]);
        }
    }
    add!("pg_description", vec![I(2200), I(2615), I(0), s("standard public schema"), N]);
    if needs("pg_description") {
        for table in &snapshot.tables {
            for (&(id, column), description) in snapshot.comments.range((table.id, 0)..=(table.id, u64::MAX)) {
                budget.visit(1)?;
                let subid = if column == 0 { 0 } else {
                    let Some(offset) = lookup.columns.get(&(id, column)) else { continue; };
                    offset + 1
                };
                if description.len() > MAX_BYTES / 2 {
                    return Err(Error::Limit);
                }
                add!("pg_description", vec![I(i64::from(table_oid(id)?)), I(1259),
                    I(subid as i64), s(description), N]);
            }
        }
    }
    // PG18.6 casts from the integer/text GUI edit-type inventories. Both ends
    // are reflected built-in scalar/catalog types; this is not a routine registry.
    for (oid, source, target, function, context, method) in [
        (10010,23,20,481,"i","f"), (10011,23,21,314,"a","f"),
        (10012,23,700,318,"i","f"), (10013,23,701,316,"i","f"),
        (10014,23,1700,1740,"i","f"), (10032,23,790,3811,"a","f"), (10034,23,16,2557,"e","f"),
        (10039,23,26,0,"i","b"), (10046,23,24,0,"i","b"),
        (10055,23,2202,0,"i","b"), (10062,23,2203,0,"i","b"), (10071,23,2204,0,"i","b"),
        (10078,23,2205,0,"i","b"), (10085,23,4191,0,"i","b"), (10092,23,2206,0,"i","b"),
        (10099,23,3734,0,"i","b"), (10106,23,3769,0,"i","b"), (10109,25,2205,1079,"i","f"),
        (10115,23,4096,0,"i","b"), (10122,23,4089,0,"i","b"),
        (10125,25,1042,0,"i","b"), (10126,25,1043,0,"i","b"),
        (10137,25,18,944,"a","f"), (10140,25,19,407,"i","f"),
        (10144,23,17,6368,"e","f"), (10150,23,18,78,"e","f"),
    ] {
        add!("pg_cast", vec![I(oid),I(source),I(target),I(function),s(context),s(method),N]);
    }
    if needs("pg_collation") {
        for line in include_str!("../../giggles-icu/src/pg_collation.txt").lines() {
            let fields: Vec<_> = line.split('|').collect();
            let mut row = Vec::new();
            for (i, field) in fields.iter().enumerate() {
                row.push(match i {
                    0 | 6 => I(field.parse().expect("embedded collation integer")),
                    2 => I(11), 3 => I(10), 5 => B(*field == "t"),
                    _ if field.is_empty() => N,
                    _ => s(field),
                });
            }
            row.push(N);
            add!("pg_collation", row);
        }
    }
    for row in &snapshot.collations {
        if row.len() != 13 { return Err(Error::InvalidSnapshot); }
        add!("pg_collation", row.clone());
        if row[12] != N {
            add!("pg_description", vec![row[0].clone(), I(3456), I(0), row[12].clone(), N]);
        }
    }
    // Only this freshly authorized connection is visible. TLS details beyond
    // the handshake status are not retained by the hosting session.
    add!(
        "pg_stat_ssl",
        vec![
            I(i64::from(snapshot.backend_pid)),
            B(snapshot.backend_ssl),
            N,
            N,
            N,
            N,
            N,
            N,
        ],
    );
    add!(
        "pg_stat_gssapi",
        vec![
            I(i64::from(snapshot.backend_pid)),
            B(false),
            N,
            B(false),
            B(false),
        ],
    );
    for (id, name, owner) in [
        (11, "pg_catalog", 10),
        (2200, "public", 6171),
        (13293, "information_schema", 10),
    ] {
        add!("pg_namespace", vec![I(id), s(name), I(owner), N, N]);
    }
    add!(
        "pg_database",
        vec![
            I(16384),
            s(&snapshot.database),
            I(i64::from(snapshot.database_owner)),
            B(false),
            B(true),
            N,
        ],
    );
    for (oid, name) in [(1663, "pg_default"), (1664, "pg_global")] {
        add!("pg_tablespace", vec![I(oid), s(name), I(10), N, N, N]);
    }
    if snapshot.database == "postgres" {
        add!(
            "pg_shdescription",
            vec![
                I(16384),
                I(1262),
                I(0),
                s("default administrative connection database"),
            ],
        );
    }
    for (oid, name, handler, kind) in [
        (2, "heap", 3, "t"),
        (403, "btree", 330, "i"),
        (405, "hash", 331, "i"),
        (783, "gist", 332, "i"),
        (2742, "gin", 333, "i"),
        (3580, "brin", 335, "i"),
        (4000, "spgist", 334, "i"),
    ] {
        add!("pg_am", vec![I(oid), s(name), I(handler), s(kind), N]);
    }
    for (oid, name, method, family, input, default, key) in [
        (1978, "int4_ops", 403, 1976, 23, true, 0),
        (10064, "array_ops", 2742, 2745, 2277, true, 2283),
        (10071, "tsvector_ops", 403, 3626, 3614, true, 0),
        (10073, "tsvector_ops", 2742, 3659, 3614, true, 25),
        (10090, "jsonb_ops", 2742, 4036, 3802, true, 25),
        (10091, "jsonb_path_ops", 2742, 4037, 3802, false, 23),
    ] {
        add!("pg_opclass", vec![I(oid), s(name), I(11), I(method), B(default), I(10), I(family), I(input), I(key), N]);
    }
    // Snapshot flags and predefined identities are presentation metadata, not
    // authentication, privileges, memberships, or enforced role policies.
    let predefined = PREDEFINED_ROLES
        .iter()
        .filter(|(_, name)| !snapshot.roles.iter().any(|role| role.name == *name))
        .map(|(oid, name)| RoleDef {
            oid: *oid,
            name: (*name).into(),
            superuser: false,
            login: false,
            createrole: false,
            createdb: false,
            inherit: true,
            replication: false,
            bypassrls: false,
            connlimit: -1,
        })
        .collect::<Vec<_>>();
    for row in &snapshot.role_memberships {
        budget.visit(1)?;
        if row.len() != 7 { return Err(Error::InvalidSnapshot); }
        add!("pg_auth_members", row.clone());
    }
    for role in snapshot.roles.iter().chain(&predefined) {
        add!(
            "pg_roles",
            vec![
                s(&role.name),
                B(role.superuser),
                B(role.inherit),
                B(role.createrole),
                B(role.createdb),
                B(role.login),
                B(role.replication),
                I(i64::from(role.connlimit)),
                s("********"),
                N,
                B(role.bypassrls),
                N,
                I(i64::from(role.oid)),
            ],
        );
        // This finite catalog has no privilege model. Expose structural authid
        // metadata to callers, but never accept, store, or expose password hashes.
        add!(
            "pg_authid",
            vec![
                I(i64::from(role.oid)),
                s(&role.name),
                B(role.superuser),
                B(role.inherit),
                B(role.createrole),
                B(role.createdb),
                B(role.login),
                B(role.replication),
                B(role.bypassrls),
                I(i64::from(role.connlimit)),
                N,
                N,
            ],
        );
        if role.login {
            let mut row = vec![
                s(&role.name),
                I(i64::from(role.oid)),
                B(role.createdb),
                B(role.superuser),
                B(role.replication),
                B(role.bypassrls),
                s("********"),
                N,
                N,
            ];
            add!("pg_user", row.clone());
            row[6] = N;
            add!("pg_shadow", row);
        }
    }
    if needs("pg_timezone_names") || needs("pg_timezone_abbrevs") {
        let now = snapshot.timezone_now.unwrap_or_else(|| {
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |v| v.as_secs() as i64)
        });
        if needs("pg_timezone_names") {
            for name in giggles_pg_types::temporal::TIMEZONE_NAMES.lines() {
                let (abbrev, seconds, dst) = giggles_pg_types::temporal::timezone_info(name, now)
                    .map_err(|_| Error::InvalidSnapshot)?;
                let sign = if seconds < 0 { "-" } else { "" };
                let seconds = seconds.unsigned_abs();
                let interval = format!("{sign}{:02}:{:02}:{:02}", seconds / 3600, seconds / 60 % 60, seconds % 60);
                add!("pg_timezone_names", vec![s(name), s(&abbrev), s(&interval), B(dst)]);
            }
        }
        if needs("pg_timezone_abbrevs") {
            for (abbrev, seconds, dst) in crate::timezone_abbrevs::rows(snapshot.timezone, now)? {
                let sign = if seconds < 0 { "-" } else { "" };
                let seconds = seconds.unsigned_abs();
                let interval = format!("{sign}{:02}:{:02}:{:02}", seconds / 3600, seconds / 60 % 60, seconds % 60);
                add!("pg_timezone_abbrevs", vec![s(&abbrev), s(&interval), B(dst)]);
            }
        }
    }
    for (txn, table, pid, mode, fastpath) in &snapshot.table_locks {
        budget.visit(1)?;
        let oid = table_oid(*table)?;
        add!("pg_locks", vec![s("relation"), I(16384), I(i64::from(oid)),
            N, N, N, N, N, N, N, s(&format!("{pid}/{txn}")), I(i64::from(*pid)),
            s(mode), B(true), B(*fastpath), N]);
    }
    // Catalog relations are genuine virtual tables, with their actual columns below.
    for &name in known_names().iter().filter(|_| {
        ["pg_class", "pg_type", "pg_attribute"].into_iter().any(needs)
    }) {
        let oid = relation_oid(name).unwrap() as i64;
        let fs = schema(name).unwrap();
        let info = information_schema::identity(name);
        let namespace = if info.is_some() { 13293 } else { 11 };
        let rowtype = info.map_or(if name == "pg_views" { 12030 } else { 32768 + oid }, |(_, ty)| i64::from(ty));
        add!(
            "pg_class",
            vec![
                I(oid),
                s(name),
                I(namespace),
                s(if tableoid(name).is_none() { "v" } else { "r" }),
                I(10),
                I(fs.iter().filter(|f| f.name != "xmin").count() as i64),
                B(false),
                s("p"),
                B(false),
                I(rowtype),
                I(0),
                I(0),
                N,
                N,
                N,
                N,
            ],
        );
        add!(
            "pg_type",
            composite_row(rowtype as u32, name, namespace, oid, 10),
        );
        for (i, f) in fs
            .iter()
            .filter(|f| f.name != "xmin")
            .chain(fs.iter().filter(|f| f.name == "xmin"))
            .enumerate()
        {
            add!(
                "pg_attribute",
                vec![
                    I(oid),
                    s(&f.name),
                    I(f.data_type.oid() as i64),
                    I(if f.name == "xmin" { -2 } else { i as i64 + 1 }),
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
        ScalarType::Varchar,
        ScalarType::Bpchar,
        ScalarType::Uuid,
        ScalarType::TsVector,
        ScalarType::TsQuery,
        ScalarType::Jsonb,
        ScalarType::Bytes,
        ScalarType::Numeric,
        ScalarType::Real,
        ScalarType::DoublePrecision,
        ScalarType::Date,
        ScalarType::Time,
        ScalarType::TimeTz,
        ScalarType::Timestamp,
        ScalarType::TimestampTz,
        ScalarType::Interval,
    ] {
        // Built-ins must be visible before application DDL: drivers cache
        // type mappings when opening a connection to an empty database.
        let (oid, name, len, array) = scalar(ty);
        add!("pg_type", type_row(oid, name, len, 0, array, false));
        add!(
            "pg_type",
            type_row(array, &format!("_{name}"), -1, oid, 0, true),
        );
    }
    // Catalog transport types are metadata-only; they are not application column types.
    for (oid, name, len, arr) in [
        (18, "char", 1, 1002),
        (19, "name", 64, 1003),
        (26, "oid", 4, 1028),
        (27, "tid", 6, 1010),
        (29, "cid", 4, 1012),
        (28, "xid", 4, 1011),
        (1033, "aclitem", 16, 1034),
        (3642, "gtsvector", -1, 3644),
    ] {
        add!("pg_type", type_row(oid, name, len, 0, arr, false));
        add!(
            "pg_type",
            type_row(arr, &format!("_{name}"), -1, oid, 0, true),
        );
    }
    add!("pg_ts_config", vec![I(3748), s("simple"), I(11), I(10), I(3722)]);
    add!("pg_ts_config", vec![I(13248), s("english"), I(11), I(10), I(3722)]);
    add!("pg_ts_dict", vec![I(3765), s("simple"), I(11), I(10), I(3727), N]);
    add!("pg_ts_dict", vec![I(13247), s("english_stem"), I(11), I(10), I(13234), s("language = 'english', stopwords = 'english'")]);
    add!("pg_ts_parser", vec![I(3722), s("default"), I(11), I(3717), I(3718), I(3719), I(3720), I(3721)]);
    add!("pg_ts_template", vec![I(3727), s("simple"), I(11), I(3725), I(3726)]);
    add!("pg_ts_template", vec![I(13234), s("snowball"), I(11), I(13232), I(13233)]);
    add!("pg_type", type_row(22, "int2vector", -1, 21, 1006, false));
    add!("pg_type", type_row(1006, "_int2vector", -1, 22, 0, true));
    add!("pg_type", type_row(30, "oidvector", -1, 26, 1013, false));
    add!("pg_type", type_row(1013, "_oidvector", -1, 30, 0, true));
    // PostgreSQL 18 nominal metadata only, not a callable routine registry.
    // pg_type.dat + pg_type.h: both are base types; node trees are category Z,
    // not pseudo-types. Only regproc has an associated array type.
    for (oid, name, len, category, elem, array, input) in [
        (24, "regproc", 4, "N", 0, 1008, 44),
        (1008, "_regproc", -1, "A", 24, 0, 750),
        (194, "pg_node_tree", -1, "Z", 0, 0, 195),
    ] {
        add!(
            "pg_type",
            vec![
                I(oid),
                s(name),
                I(11),
                I(len),
                s("b"),
                s(category),
                B(false),
                I(0),
                I(-1),
                I(0),
                I(elem),
                I(array),
                I(input),
                s(","),
                I(10),
                N,
                I(0),
                N,
                B(true),
                B(false),
                N,
                I(type_receive(oid as u32, oid == 1008)),
            ],
        );
    }
    // PG18.6 metadata-only targets used by pgAdmin's integer/text edit list.
    for (oid, name, len, array, input, receive) in [
        (790,"money",8,791,886,2492), (2202,"regprocedure",4,2207,2212,2446),
        (2203,"regoper",4,2208,2214,2448), (2204,"regoperator",4,2209,2216,2450),
        (2205,"regclass",4,2210,2218,2452), (2206,"regtype",4,2211,2220,2454),
        (3734,"regconfig",4,3735,3736,3738), (3769,"regdictionary",4,3770,3771,3773),
        (4089,"regnamespace",4,4090,4084,4087), (4096,"regrole",4,4097,4098,4094),
        (4191,"regcollation",4,4192,4193,4196),
    ] {
        let mut row = type_row(oid, name, len, 0, array, false);
        row[5] = s("N"); row[12] = I(input); row[21] = I(receive);
        add!("pg_type", row);
        add!("pg_type", type_row(array, &format!("_{name}"), -1, oid, 0, true));
    }
    if needs("pg_type") {
        let mut cardinal = type_row(13307, "cardinal_number", 4, 0, 13306, false);
        cardinal[2] = I(13293); cardinal[4] = s("d"); cardinal[5] = s("N"); cardinal[7] = I(23);
        cardinal[12] = I(2597); cardinal[21] = I(2598); add!("pg_type", cardinal);
        let mut array = type_row(13306, "_cardinal_number", -1, 13307, 0, true);
        array[2] = I(13293); add!("pg_type", array);
        // Nominal descriptors needed by NULL-only catalog fields, not new SQL types.
        for (oid, name, len, kind, category, elem, array, input, receive) in [
            (2277, "anyarray", -1, "p", "P", 0, 0, 2296, 2502),
            (3220, "pg_lsn", 8, "b", "U", 0, 3221, 3229, 3238),
            (3221, "_pg_lsn", -1, "b", "A", 3220, 0, 750, 2400),
        ] {
            let mut row = type_row(oid, name, len, elem, array, elem != 0);
            row[4] = s(kind);
            row[5] = s(category);
            row[12] = I(input);
            row[21] = I(receive);
            add!("pg_type", row);
        }
    }
    for (name, (oid, owner)) in &snapshot.schemas {
        add!(
            "pg_namespace",
            vec![I(i64::from(*oid)), s(name), I(i64::from(*owner)), N, N],
        );
    }
    let mut ordered_tables = if [
        "pg_class", "pg_type", "pg_views", "pg_attribute", "pg_indexes",
        "pg_index", "pg_constraint", "pg_depend",
    ].into_iter().any(needs) {
        snapshot.tables.iter().collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    ordered_tables.sort_by_key(|t| t.id);
    for t in ordered_tables {
        let (schema, namespace) = snapshot.table_schema(t.id);
        let namespace = i64::from(namespace);
        let oid = table_oid(t.id)? as i64;
        let view = snapshot.views.get(&t.id);
        let has_pk = !t.primary_key.is_empty();
        let owner = i64::from(*snapshot.table_owners.get(&t.id).unwrap_or(&10));
        add!(
            "pg_class",
            vec![
                I(oid),
                s(&t.name),
                I(namespace),
                s(if view.is_some() { "v" } else { "r" }),
                I(owner),
                I(t.columns.len() as i64),
                B(has_pk || lookup.indexes_by_table.contains_key(&t.id)),
                s("p"),
                B(false),
                I(oid + 3),
                I(if view.is_some() { 0 } else { 2 }),
                I(0),
                N,
                acl_value(&lookup, snapshot.table_acls.get(&t.id)),
                N,
                N,
            ],
        );
        add!(
            "pg_type",
            composite_row((oid + 3) as u32, &t.name, namespace, oid, owner),
        );
        if let Some(definition) = view {
            add!("pg_views", vec![s(schema), s(&t.name), s(lookup.roles[&owner]), s(definition)]);
        }
        // Heap system attributes exist in pg_attribute but are excluded by
        // clients that explicitly request attnum > 0.
        for (attnum, name, ty, len) in [
            (-6, "tableoid", 26, 4),
            (-5, "cmax", 29, 4),
            (-4, "xmax", 28, 4),
            (-3, "cmin", 29, 4),
            (-2, "xmin", 28, 4),
            (-1, "ctid", 27, 6),
        ].into_iter().filter(|_| view.is_none()) {
            add!(
                "pg_attribute",
                vec![
                    I(oid),
                    s(name),
                    I(ty),
                    I(attnum),
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
        for (i, c) in t.columns.iter().enumerate() {
            let (ty, _, len, _) = scalar(c.data_type);
            let generated = snapshot.generated_columns.get(&c.id);
            add!(
                "pg_attribute",
                vec![
                    I(oid),
                    s(&c.name),
                    I(snapshot.column_type_oids.get(&c.id).copied().unwrap_or(ty) as i64),
                    I(i as i64 + 1),
                    B(c.not_null || t.primary_key.contains(&i)),
                    I(i64::from(
                        *snapshot.column_typmods.get(&c.id).unwrap_or(&-1),
                    )),
                    I(len),
                    B(false),
                    s(&snapshot.column_identities.get(&c.id).map_or(String::new(), char::to_string)),
                    s(&generated.map_or(String::new(), |column| column.kind.to_string())),
                    B(generated.is_some() || snapshot.column_defaults.contains_key(&c.id) && !snapshot.column_identities.contains_key(&c.id)),
                    I(0),
                    N,
                    I(i64::from(view.is_none() && c.data_type == ScalarType::UuidArray)),
                    B(true),
                    N,
                ],
            );
        }
        if has_pk {
            let key = Value::Array(t.primary_key.iter().map(|k| I(*k as i64 + 1)).collect());
            let name = || key_names[&t.id].as_str();
            add!(
                "pg_indexes",
                vec![
                    s(schema),
                    s(&t.name),
                    s(name()),
                    N,
                    s(&index_definition(snapshot, &lookup, None,schema, t, name(), &t.primary_key)),
                ],
            );
            add!(
                "pg_class",
                vec![
                    I(oid + 1),
                    s(name()),
                    I(namespace),
                    s("i"),
                    I(owner),
                    I(t.primary_key.len() as i64),
                    B(false),
                    s("p"),
                    B(false),
                    I(0),
                    I(403),
                    I(0),
                    N,
                    N,
                    N,
                    N,
                ],
            );
            add!(
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
                    B(false),
                    B(true),
                    B(true),
                    B(true),
                    B(false),
                    B(false),
                    B(false),
                    B(false),
                ],
            );
            for (i, k) in t.primary_key.iter().enumerate() {
                let c = &t.columns[*k];
                let (ty, _, len, _) = scalar(c.data_type);
                add!(
                    "pg_attribute",
                    vec![
                        I(oid + 1),
                        s(&c.name),
                        I(ty as i64),
                        I(i as i64 + 1),
                        B(false),
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
            add!(
                "pg_constraint",
                vec![
                    I(oid + 2),
                    s(name()),
                    I(namespace),
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
            // A primary-key index is internally owned by its constraint (PG18).
            add!(
                "pg_depend",
                vec![I(1259), I(oid + 1), I(0), I(2606), I(oid + 2), I(0), s("i")],
            );
        }
        for &index in lookup.indexes_by_table.get(&t.id).into_iter().flatten() {
            let index_oid = table_oid(index.id)? as i64;
            let n = &index.name;
            let keys = index
                .columns
                .iter()
                .map(|id| lookup.columns[&(t.id, *id)])
                .collect::<Vec<_>>();
            let key = Value::Array(keys.iter().map(|k| I(*k as i64 + 1)).collect());
            add!(
                "pg_class",
                vec![
                    I(index_oid),
                    s(n),
                    I(namespace),
                    s("i"),
                    I(owner),
                    I(keys.len() as i64),
                    B(false),
                    s("p"),
                    B(false),
                    I(0),
                    I(403),
                    I(0),
                    N,
                    N,
                    N,
                    N,
                ],
            );
            add!(
                "pg_index",
                vec![
                    I(index_oid),
                    I(oid),
                    I(keys.len() as i64),
                    I(keys.len() as i64),
                    B(index.unique),
                    B(false),
                    B(true),
                    key.clone(),
                    snapshot.index_predicates.get(&index.id).map_or(N, |p| s(p)),
                    N,
                    Value::Array(vec![I(0); keys.len()]),
                    Value::Array(vec![I(0); keys.len()]),
                    Value::Array(vec![I(0); keys.len()]),
                    B(false),
                    B(true),
                    B(true),
                    B(true),
                    B(false),
                    B(false),
                    B(false),
                    B(false),
                ],
            );
            for (i, k) in keys.iter().enumerate() {
                let c = &t.columns[*k];
                let (ty, _, len, _) = scalar(c.data_type);
                add!(
                    "pg_attribute",
                    vec![
                        I(index_oid),
                        s(&c.name),
                        I(ty as i64),
                        I(i as i64 + 1),
                        B(false),
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
            add!(
                "pg_indexes",
                vec![
                    s(schema),
                    s(&t.name),
                    s(n),
                    N,
                    s(&index_definition(snapshot, &lookup, Some(index),schema, t, n, &keys)),
                ],
            );
        }
    }
    for row in rows_mut(&mut all, "pg_index") {
        let Some(&table) = lookup.relations.get(&row_oid(&row[1])) else { continue };
        let index = lookup.index_relations.get(&row_oid(&row[0]));
        let gin = index.and_then(|index| snapshot.gin_indexes.get(&index.id));
        let keys = index.map(|i| i.columns.clone())
            .unwrap_or_else(|| table.primary_key.iter().map(|i| table.columns[*i].id).collect());
        let overrides = index.and_then(|i| snapshot.collation_oids.get(&i.id));
        budget.rewrite(row, |row| {
            if let Some(gin) = gin {
                row[12] = Value::Array(gin.opclasses.iter().map(|oid| I(i64::from(*oid))).collect());
                return;
            }
            row[11] = Value::Array(keys.iter().enumerate().map(|(offset, id)| {
                let column = lookup.column(table, *id).unwrap();
                let collation = overrides.and_then(|v| v.get(offset)).copied()
                    .filter(|oid| *oid != 0)
                    .unwrap_or_else(|| column_collation(snapshot, &lookup, column));
                I(i64::from(collation))
            }).collect());
            row[12] = Value::Array(keys.iter().map(|id| {
                let column = lookup.column(table, *id).unwrap();
                I(if column.data_type == ScalarType::Integer { 1978 } else { 0 })
            }).collect());
        })?;
    }
    rows_mut(&mut all, "pg_index").sort_by_key(|r| match (&r[1], &r[0]) {
            (I(t), I(i)) => (*t, *i),
            _ => unreachable!(),
        });
    // Append descriptor extensions here so established field positions remain stable.
    for row in rows_mut(&mut all, "pg_class") {
        budget.rewrite(row, |row| {
            if row[2] == I(2200) && row[3] != s("v") {
                row[10] = I(if row[3] == s("i") { 403 } else { 2 });
            }
            if let Some(gin) = lookup.index_relations.get(&row_oid(&row[0]))
                .and_then(|index| snapshot.gin_indexes.get(&index.id))
            {
                row[10] = I(2742);
                if !gin.options.is_empty() {
                    row[12] = Value::Array(gin.options.iter().map(|(name, value)| s(&format!("{name}={value}"))).collect());
                }
            }
            row.extend([
                I(0),
                B(false),
                B(false),
                B(false),
                B(false),
                I(0),
                s(if row[3] == s("i") || row[3] == s("v") { "n" } else { "d" }),
                I(0),
                I(0),
                Value::Float4(giggles_pg_types::Float4::parse("-1").unwrap()),
                I(0),
                // FrozenTransactionId (2) and FirstMultiXactId (1): virtual
                // relations have no heap transaction horizon to preserve in dumps.
                I(if row[3] == s("r") { 2 } else { 0 }),
                B(true),
                I(if row[3] == s("r") { 1 } else { 0 }),
                I(0),
            ]);
        })?;
    }
    let enum_array_oids: BTreeSet<_> = if needs("pg_type") || needs("pg_attribute") {
        snapshot.user_types.iter().filter(|ty| ty.kind == 'e')
            .map(|ty| i64::from(ty.oid) + 1).collect()
    } else {
        BTreeSet::new()
    };
    let namespace_oid = |name: &str| {
        if name == "public" { Ok(2200) } else {
            snapshot.schemas.get(name).map(|(oid, _)| i64::from(*oid)).ok_or(Error::InvalidSnapshot)
        }
    };
    let mut user_type_names: BTreeSet<_> = all.get("pg_type").into_iter().flat_map(|relation| &relation.rows)
        .filter_map(|r| match (&r[2], &r[1]) {
            (I(namespace), Value::Text(name)) => Some((*namespace, name.clone())),
            _ => None,
        }).collect();
    if needs("pg_type") {
        for ty in &snapshot.user_types {
            user_type_names.insert((namespace_oid(&ty.schema)?, ty.name.clone()));
        }
    }
    for ty in &snapshot.user_types {
        if needs("pg_type") {
            let len = match (ty.kind, ty.base) {
                ('e', _) | ('d', 23 | 700 | 1082) => 4,
                ('d', 20 | 701 | 1083 | 1114 | 1184) => 8,
                ('d', 21) => 2,
                ('d', 16) => 1,
                _ => -1,
            };
            let mut row = type_row(ty.oid, &ty.name, len, 0, 0, false);
            let namespace = namespace_oid(&ty.schema)?;
            row[2] = I(namespace);
            row[4] = s(&ty.kind.to_string());
            row[5] = s(match ty.kind {
                'e' => "E",
                'c' => "C",
                _ => "N",
            });
            row[6] = B(ty.not_null);
            row[7] = I(ty.base as i64);
            row[8] = I(ty.typmod as i64);
            // Enum IDs occupy the aligned slot, so the following OID is free
            // for the array type. Label IDs are allocated in separate slots.
            if ty.kind == 'e' {
                let array_oid = ty.oid + 1;
                let mut serial = 0u32;
                let name = loop {
                    let suffix = if serial == 0 { String::new() } else { format!("_{serial}") };
                    let mut end = ty.name.len().min(62 - suffix.len());
                    while !ty.name.is_char_boundary(end) {
                        end -= 1;
                    }
                    let candidate = format!("_{}{suffix}", &ty.name[..end]);
                    if user_type_names.insert((namespace, candidate.clone())) {
                        break candidate;
                    }
                    serial = serial.checked_add(1).ok_or(Error::Limit)?;
                };
                let mut array = type_row(array_oid, &name, -1, ty.oid, 0, true);
                array[2] = row[2].clone();
                row[11] = I(i64::from(array_oid));
                add!("pg_type", array);
            }
            add!("pg_type", row);
        }
        if !needs("pg_enum") { continue; }
        for (i, (label, &oid)) in ty.labels.iter().zip(&ty.label_oids).enumerate() {
            add!("pg_enum", vec![
                I(i64::from(oid)),
                I(ty.oid as i64),
                Value::Float4(giggles_pg_types::Float4::from_bits(
                    ((i + 1) as f32).to_bits(),
                )),
                s(label),
            ]);
        }
    }
    if needs("pg_prepared_statements") && !snapshot.prepared_statements.is_empty() {
        let types = &all["pg_type"].rows;
        let mut names = HashMap::new();
        let mut array = |oids: &[u32], budget: &mut Budget| -> R<Value> {
            let values = oids.iter().map(|&oid| {
                let name = match names.entry(oid) {
                    std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        budget.visit(types.len())?;
                        entry.insert(crate::engine::format_type(i64::from(oid), N, types)
                            .unwrap_or_else(|_| oid.to_string()))
                    }
                };
                Ok(Value::Regtype(oid, name.clone()))
            }).collect::<R<_>>()?;
            Ok(Value::Array(values))
        };
        let mut rows = Vec::new();
        for row in &snapshot.prepared_statements {
            let parameters = array(&row.parameter_types, budget)?;
            let result = row.result_types.as_deref()
                .map_or(Ok(N), |oids| array(oids, budget))?;
            budget.add(&mut rows, vec![
                s(&row.name), s(&row.statement), I(row.prepare_time), parameters,
                result, B(row.from_sql), I(row.generic_plans), I(row.custom_plans),
            ])?;
        }
        all.get_mut("pg_prepared_statements").unwrap().rows = rows;
    }
    for row in rows_mut(&mut all, "pg_type") {
        let oid = match row[0] {
            I(v) => v,
            _ => 0,
        };
        budget.rewrite(row, |row| {
            row.push(I(type_collation(&lookup, oid)));
            row.push(s(if enum_array_oids.contains(&oid) { "x" } else { type_layout(oid).0 }));
        })?;
    }
    for row in rows_mut(&mut all, "pg_attribute") {
        budget.rewrite(row, |row| {
            let rel = row_oid(&row[0]);
            let oid = row_oid(&row[2]);
            let mut collation = type_collation(&lookup, oid);
            let mut column_acl = None;
            if let I(n) = row[3] && n > 0 {
                let offset = (n - 1) as usize;
                if let Some(table) = lookup.relations.get(&rel)
                    && let Some(column) = table.columns.get(offset)
                {
                    if let Some(oid) = snapshot.collation_oids.get(&column.id).and_then(|v| v.first()) {
                        collation = i64::from(*oid);
                    }
                    column_acl = snapshot.column_acls.get(&(table.id, column.id));
                }
                if let Some(index) = lookup.index_relations.get(&rel) {
                    let column = index.columns.get(offset).and_then(|id| lookup.column(lookup.tables.get(&index.table)?, *id));
                    if let Some(column) = column {
                        let oid = snapshot.collation_oids.get(&index.id).and_then(|v| v.get(offset)).copied()
                            .filter(|oid| *oid != 0).unwrap_or_else(|| column_collation(snapshot, &lookup, column));
                        collation = i64::from(oid);
                    }
                } else if let Some(table) = lookup.primary_indexes.get(&rel)
                    && let Some(column) = table.primary_key.get(offset).and_then(|i| table.columns.get(*i))
                {
                    collation = i64::from(column_collation(snapshot, &lookup, column));
                }
            }
            let enum_array = enum_array_oids.contains(&oid);
            let (storage, align) = if enum_array { ("x", "i") } else { type_layout(oid) };
            // Project the structural row into PG18's physical order.
            let column_acl = acl_value(&lookup, column_acl);
            let old = std::mem::take(row);
            *row = vec![
                old[0].clone(),
                old[1].clone(),
                old[2].clone(),
                old[6].clone(),
                old[3].clone(),
                old[5].clone(),
                old[13].clone(),
                B(matches!(
                    oid,
                    16 | 18
                        | 20
                        | 21
                        | 23
                        | 24
                        | 26
                        | 28
                        | 29
                        | 700
                        | 701
                        | 1082
                        | 1083
                        | 1114
                        | 1184
                        | 3220
                )),
                s(align),
                s(storage),
                s(""),
                old[4].clone(),
                if enum_array { I(1) } else { old[10].clone() },
                B(false),
                old[8].clone(),
                old[9].clone(),
                old[7].clone(),
                old[14].clone(),
                I(0),
                I(collation),
                N,
                column_acl,
                N,
                old[15].clone(),
                N,
                old[12].clone(),
            ];
        })?;
    }
    // PG18 discovers NOT NULL via pg_constraint rather than attnotnull.
    // These are snapshot-only identities; no persisted format is changed.
    let mut used_oids = if needs("pg_constraint") || needs("pg_attrdef")
        || !snapshot.constraints.is_empty()
        || snapshot.user_types.iter().any(|ty| !ty.label_oids.is_empty())
    {
        materialize::occupied_oids(snapshot, &lookup, budget)?
    } else {
        BTreeSet::new()
    };
    let mut next_oid = i64::from(u32::MAX);
    for c in &snapshot.constraints {
        if !used_oids.insert(i64::from(table_oid(c.id)?)) { return Err(Error::InvalidSnapshot); }
    }
    if needs("pg_constraint") || needs("pg_attrdef") {
        let mut tables = snapshot.tables.iter().collect::<Vec<_>>();
        tables.sort_by_key(|table| table.id);
        for table in tables {
            for (i, column) in table.columns.iter().enumerate() {
                if !column.not_null && !table.primary_key.contains(&i) {
                    continue;
                }
                while !used_oids.insert(next_oid) {
                    budget.visit(1)?;
                    next_oid -= 1;
                }
                budget.visit(1)?;
                add!("pg_constraint", vec![
                    I(next_oid),
                    s(&format!("{}_{}_not_null", table.name, column.name)),
                    I(i64::from(snapshot.table_schema(table.id).1)),
                    s("n"),
                    I(i64::from(table_oid(table.id)?)),
                    I(0),
                    Value::Array(vec![I(i as i64 + 1)]),
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
                    B(false),
                    N,
                    N,
                ]);
                next_oid -= 1;
            }
        }
    }
    constraints::populate(snapshot, &lookup, budget, &mut all, &mut used_oids, &mut next_oid)?;
    let foreign = if needs("pg_constraint") {
        snapshot.constraints.iter().filter(|c| c.kind == 'f')
            .map(|c| Ok((I(i64::from(table_oid(c.id)?)), c))).collect::<R<BTreeMap<_, _>>>()?
    } else {
        BTreeMap::new()
    };
    for row in rows_mut(&mut all, "pg_constraint") {
        budget.rewrite(row, |row| {
            row.extend([I(0), B(false), B(true), B(true), I(0), N, N, N, N]);
            if let Some(c) = foreign.get(&row[0]) {
                let operators = c.foreign_columns.iter().map(|id| {
                    let ty = lookup.column(lookup.tables[&c.foreign_table], *id).unwrap().data_type;
                    I(match ty {
                        ScalarType::Boolean => 91, ScalarType::SmallInt => 94, ScalarType::Integer => 96,
                        ScalarType::BigInt => 410, ScalarType::Text | ScalarType::Varchar => 98,
                        ScalarType::Bpchar => 1054, ScalarType::Uuid => 2972, ScalarType::Jsonb => 3240,
                        ScalarType::Bytes => 1955, ScalarType::Numeric => 1752, ScalarType::Real => 620,
                        ScalarType::DoublePrecision => 670, ScalarType::Date => 1093, ScalarType::Time => 1108,
                        ScalarType::TimeTz => 1550, ScalarType::Timestamp => 2060, ScalarType::TimestampTz => 1320,
                        ScalarType::TsVector => 3629, ScalarType::TsQuery => 3676,
                        ScalarType::Interval => 1330, ScalarType::IntArray | ScalarType::BigIntArray | ScalarType::TextArray | ScalarType::UuidArray => 1070,
                    })
                }).collect::<Vec<_>>();
                for slot in &mut row[25..28] { *slot = Value::Array(operators.clone()); }
            }
        })?;
    }
    if needs("pg_operator") {
        let referenced_operators = rows(&all, "pg_constraint").iter().flat_map(|row| {
            match &row[25] {
                Value::Array(operators) => operators.as_slice(),
                _ => &[],
            }
        }).filter_map(|value| match value {
            I(oid) => Some(*oid),
            _ => None,
        }).collect::<BTreeSet<_>>();
        // PG18 equality operators already referenced by pg_constraint.conpfeqop.
        // Emit only referenced operators, once each in OID order.
        // Source: PostgreSQL REL_18_STABLE pg_operator.dat and pg_proc.dat.
        let mut equality_operators = [
            (91, 16, 85, 60), (94, 21, 519, 63), (96, 23, 518, 65),
            (410, 20, 411, 467), (98, 25, 531, 67), (1054, 1042, 1057, 1048),
            (2972, 2950, 2973, 2956), (3240, 3802, 3241, 4043), (1955, 17, 1956, 1948),
            (1752, 1700, 1753, 1718), (620, 700, 621, 287), (670, 701, 671, 293),
            (1093, 1082, 1094, 1086), (1108, 1083, 1109, 1145), (1550, 1266, 1551, 1352),
            (2060, 1114, 2061, 2052), (1320, 1184, 1321, 1152), (1330, 1186, 1331, 1162),
            (1070, 2277, 1071, 744),
        ];
        equality_operators.sort_unstable_by_key(|operator| operator.0);
        for (oid, ty, negate, code) in equality_operators {
            if !referenced_operators.contains(&oid) { continue; }
            add!("pg_operator", vec![I(oid), s("="), I(11), I(10), s("b"), B(true), B(true),
                I(ty), I(ty), I(16), I(oid), I(negate), N, I(code), I(101), I(105)]);
        }
        // PostgreSQL 18.6 pg_operator and pg_proc OIDs for supported full-text operators.
        for (oid, name, left, right, result, code, commutator, negator, restrict, join) in [
            (3627, "<", 3614, 3614, 16, 3616, 3632, 3631, 103, 107),
            (3628, "<=", 3614, 3614, 16, 3617, 3631, 3632, 336, 386),
            (3629, "=", 3614, 3614, 16, 3618, 3629, 3630, 101, 105),
            (3630, "<>", 3614, 3614, 16, 3619, 3630, 3629, 102, 106),
            (3631, ">=", 3614, 3614, 16, 3620, 3628, 3627, 337, 398),
            (3632, ">", 3614, 3614, 16, 3621, 3627, 3628, 104, 108),
            (3633, "||", 3614, 3614, 3614, 3625, 0, 0, 0, 0),
            (3636, "@@", 3614, 3615, 16, 3634, 3637, 0, 3686, 3687),
            (3637, "@@", 3615, 3614, 16, 3635, 3636, 0, 3686, 3687),
            (3674, "<", 3615, 3615, 16, 3662, 3679, 3678, 103, 107),
            (3675, "<=", 3615, 3615, 16, 3663, 3678, 3679, 336, 386),
            (3676, "=", 3615, 3615, 16, 3664, 3676, 3677, 101, 105),
            (3677, "<>", 3615, 3615, 16, 3665, 3677, 3676, 102, 106),
            (3678, ">=", 3615, 3615, 16, 3666, 3675, 3674, 337, 398),
            (3679, ">", 3615, 3615, 16, 3667, 3674, 3675, 104, 108),
            (3680, "&&", 3615, 3615, 3615, 3669, 0, 0, 0, 0),
            (3681, "||", 3615, 3615, 3615, 3670, 0, 0, 0, 0),
            (3682, "!!", 0, 3615, 3615, 3671, 0, 0, 0, 0),
            (3693, "@>", 3615, 3615, 16, 3691, 3694, 0, 5040, 5041),
            (3694, "<@", 3615, 3615, 16, 3692, 3693, 0, 5040, 5041),
            (3762, "@@", 25, 25, 16, 3760, 0, 0, 5040, 5041),
            (3763, "@@", 25, 3615, 16, 3761, 0, 0, 5040, 5041),
            (5005, "<->", 3615, 3615, 3615, 5003, 0, 0, 0, 0),
        ] {
            add!("pg_operator", vec![I(oid), s(name), I(11), I(10),
                s(if left == 0 { "l" } else { "b" }), B(name == "="), B(false),
                I(left), I(right), I(result), I(commutator), I(negator), N, I(code), I(restrict), I(join)]);
        }
    }
    for row in rows_mut(&mut all, "pg_database") {
        budget.rewrite(row, |row| {
            row.extend([I(6), s("c"), s("C.UTF-8"), s("C.UTF-8"), N, N, I(1663)]);
        })?;
    }
    // No per-object MVCC tuple exists. Reporting the current durable generation
    // for every xmin makes DataGrip conservatively refresh every catalog object.
    for relation in all.values_mut() {
        if let Some(xmin) = relation
            .fields
            .iter()
            .position(|field| field.name == "xmin")
        {
            for row in &mut relation.rows {
                // Newly synthesized system slots follow all physical fields.
                budget.rewrite(row, |row| {
                    if xmin == row.len() {
                        row.push(N);
                    }
                    row[xmin] = I(i64::from(snapshot.generation as u32));
                })?;
            }
        }
    }
    information_schema::populate(snapshot, &lookup, &key_names, budget, &mut all)?;
    // pg_tables reads pg_class in its private builder layout, so it runs
    // before pg_class/pg_index are presented in PG18 column order.
    if needs("pg_tables") {
        pg_tables::populate(&lookup, budget, &mut all)?;
    }
    for name in ["pg_class", "pg_index", "pg_constraint"] {
        let Some(relation) = all.get_mut(name) else { continue };
        let fields = schema(name).unwrap();
        let positions: Vec<_> = fields.iter().map(|f| {
            relation.fields.iter().position(|old| old.name == f.name)
        }).collect();
        for row in &mut relation.rows {
            budget.rewrite(row, |row| {
                *row = fields.iter().zip(&positions).map(|(field, position)| {
                    position.map_or_else(|| if field.data_type == PgType::Bool { B(false) } else { I(0) }, |i| row[i].clone())
                }).collect();
            })?;
        }
        relation.fields = fields;
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
    all.retain(|name, _| requested.contains(name));
    Ok(all)
}
// PostgreSQL physical type layout is catalog presentation only; it does
// not describe GigglesDB's persisted scalar encodings.
fn type_layout(oid: i64) -> (&'static str, &'static str) {
    let storage = match oid {
        1700 => "m",
        17
        | 25
        | 194
        | 1042
        | 1043
        | 2249
        | 2277
        | 1000..=1034
        | 1115
        | 1182
        | 1183
        | 1185
        | 1187
        | 1231
        | 1270
        | 2951
        | 3614 | 3643..=3645 | 3735 | 3770
        | 3221 => "x",
        n if n >= 32768 => "x",
        _ => "p",
    };
    let align = match oid {
        16 | 18 | 19 => "c",
        21 | 27 => "s",
        20 | 701 | 1016 | 1034 | 1083 | 1183 | 1114 | 1115 | 1184 | 1185 | 1186 | 1187 | 1266
        | 1270 | 2249 | 2277 | 3220 | 3221 => "d",
        n if n >= 32768 => "d",
        _ => "i",
    };
    (storage, align)
}
fn type_len(t: PgType) -> i64 {
    t.type_size() as i64
}
fn composite_row(oid: u32, name: &str, namespace: i64, relation: i64, owner: i64) -> Vec<Value> {
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
        I(owner),
        N,
        I(0),
        N,
        B(true),
        B(false),
        N,
        I(2403),
    ]
}
fn type_is_preferred(oid: u32) -> bool {
    matches!(oid, 16 | 25 | 26 | 701 | 869 | 1184 | 1186 | 1562)
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
        } else if matches!(oid, 20 | 21 | 23 | 26 | 700 | 701 | 1700) {
            "N"
        } else if matches!(oid, 1082 | 1083 | 1266 | 1114 | 1184) {
            "D"
        } else if oid == 1186 {
            "T"
        } else if matches!(oid, 17 | 27 | 28 | 29 | 1033 | 2950 | 3614 | 3615 | 3642 | 3802) {
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
        I(if is_array {
            750
        } else {
            match oid {
                27 => 48,
                29 => 52,
                3614 => 3610,
                3615 => 3612,
                3642 => 3646,
                _ => 0,
            }
        }),
        s(","),
        I(10),
        N,
        I(0),
        N,
        B(true),
        B(type_is_preferred(oid)),
        N,
        I(type_receive(oid, is_array)),
    ]
}

fn type_receive(oid: u32, is_array: bool) -> i64 {
    if oid == 3614 { return 3639; }
    if oid == 3615 { return 3641; }
    if is_array {
        return 2400;
    }
    match oid {
        16 => 2436,
        17 => 2412,
        18 => 2434,
        19 => 2422,
        20 => 2408,
        21 => 2404,
        22 => 2410,
        23 => 2406,
        24 => 2444,
        25 => 2414,
        26 => 2418,
        27 => 2438,
        28 => 2440,
        29 => 2442,
        30 => 2420,
        194 => 197,
        700 => 2424,
        701 => 2426,
        1033 => 0,
        1082 => 2468,
        1083 => 2470,
        1114 => 2474,
        1184 => 2476,
        1186 => 2478,
        1266 => 2472,
        1700 => 2460,
        2950 => 2961,
        3802 => 3805,
        _ => 0,
    }
}

/// PostgreSQL identifier spelling for emitted index DDL.
pub(crate) fn quote_identifier(name: &str) -> String {
    let simple = !name.is_empty()
        && name
            .chars()
            .enumerate()
            .all(|(i, c)| c == '_' || c.is_ascii_lowercase() || i > 0 && c.is_ascii_digit());
    // Match the committed core quote_ident policy: only non-unreserved
    // PostgreSQL keywords need quoting, not every sqlparser dialect keyword.
    let keyword = REQUIRES_QUOTES.binary_search(&name).is_ok();
    if simple && !keyword {
        name.into()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}
fn type_collation(lookup: &Lookup<'_>, oid:i64)->i64 {
    match oid {19|1003=>950,25|1009|1042|1043|1014|1015=>100,_=>{
        if lookup.user_types.get(&oid).is_some_and(|t|t.kind=='d' && matches!(t.base,25|1042|1043)) {100} else {0}
    }}
}
fn column_collation(snapshot:&CatalogSnapshot, lookup: &Lookup<'_>, column:&ColumnDef)->u32 {
    snapshot.collation_oids.get(&column.id).and_then(|v|v.first()).copied().unwrap_or_else(|| {
        let oid=snapshot.column_type_oids.get(&column.id).copied().unwrap_or(scalar(column.data_type).0);
        type_collation(lookup,i64::from(oid)) as u32
    })
}
fn index_definition(
    snapshot:&CatalogSnapshot,
    lookup: &Lookup<'_>,
    index: Option<&IndexDef>,
    schema: &str,
    t: &TableDef,
    name: &str,
    keys: &[usize],
) -> String {
    let unique = index.is_none_or(|index| index.unique);
    if let Some(gin) = index.and_then(|index| snapshot.gin_indexes.get(&index.id)) {
        let keys = keys.iter().zip(&gin.opclasses).map(|(offset, oid)| {
            let name = quote_identifier(&t.columns[*offset].name);
            if *oid == 10091 { format!("{name} jsonb_path_ops") } else { name }
        }).collect::<Vec<_>>().join(", ");
        let options = if gin.options.is_empty() { String::new() } else {
            let options = gin.options.iter().map(|(name, value)| {
                let value = if quote_identifier(value) == *value {
                    value.clone()
                } else {
                    format!("'{}'", value.replace('\'', "''"))
                };
                format!("{name}={value}")
            }).collect::<Vec<_>>().join(", ");
            format!(" WITH ({options})")
        };
        return format!("CREATE INDEX {} ON {}.{} USING gin ({keys}){options}",
            quote_identifier(name), quote_identifier(schema), quote_identifier(&t.name));
    }
    let mut definition = format!(
        "CREATE {}INDEX {} ON {}.{} USING btree ({})",
        if unique { "UNIQUE " } else { "" },
        quote_identifier(name),
        quote_identifier(schema),
        quote_identifier(&t.name),
        keys.iter()
            .enumerate().map(|(offset,k)| {
                let c=&t.columns[*k];let mut name=quote_identifier(&c.name);
                let col=column_collation(snapshot,lookup,c);
                if let Some(oid)=index.and_then(|index|snapshot.collation_oids.get(&index.id)).and_then(|v|v.get(offset)).copied().filter(|oid|*oid!=col && *oid!=0) {
                    let collation = lookup.collations.get(&oid)
                        .map(|name| format!("pg_catalog.{}", quote_identifier(name)))
                        .or_else(|| {
                            let row = lookup.user_collations.get(&i64::from(oid))?;
                            let (Value::Text(name), I(namespace)) = (&row[1], &row[2]) else { return None };
                            let schema = if *namespace == 2200 { "public" } else {
                                // User collation definitions refer to supplied schemas.
                                let name = lookup.namespaces.get(namespace)?;
                                snapshot.schemas.get(*name)?;
                                name
                            };
                            Some(format!("{}.{}", quote_identifier(schema), quote_identifier(name)))
                        });
                    if let Some(collation) = collation { name.push_str(&format!(" COLLATE {collation}")); }
                }
                name
            })
            .collect::<Vec<_>>()
            .join(", ")
    );
    if let Some(predicate) = index.and_then(|index| snapshot.index_predicates.get(&index.id)) {
        definition.push_str(" WHERE ");
        definition.push_str(predicate);
    }
    definition
}

// Copied from the committed core quoting_keywords.rs; keep category policy aligned.
// PostgreSQL REL_18_0-derived algorithm/data.
// Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
// Portions Copyright (c) 1994, Regents of the University of California
// PostgreSQL Database Management System
// (also known as Postgres, formerly known as Postgres95)
//
// Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//
// Portions Copyright (c) 1994, The Regents of the University of California
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written agreement
// is hereby granted, provided that the above copyright notice and this
// paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
// DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
// LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
// DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.
//
// THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
// AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
// ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
// PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
// Source: src/include/parser/kwlist.h, SHA256 fdcdf3694513cba63b4016f63032472b686e381bb35f17c5d645bc2f6f1dac16.
// All and only non-UNRESERVED_KEYWORD names, in source ASCII order.
const REQUIRES_QUOTES: &[&str] = &[
    "all",
    "analyse",
    "analyze",
    "and",
    "any",
    "array",
    "as",
    "asc",
    "asymmetric",
    "authorization",
    "between",
    "bigint",
    "binary",
    "bit",
    "boolean",
    "both",
    "case",
    "cast",
    "char",
    "character",
    "check",
    "coalesce",
    "collate",
    "collation",
    "column",
    "concurrently",
    "constraint",
    "create",
    "cross",
    "current_catalog",
    "current_date",
    "current_role",
    "current_schema",
    "current_time",
    "current_timestamp",
    "current_user",
    "dec",
    "decimal",
    "default",
    "deferrable",
    "desc",
    "distinct",
    "do",
    "else",
    "end",
    "except",
    "exists",
    "extract",
    "false",
    "fetch",
    "float",
    "for",
    "foreign",
    "freeze",
    "from",
    "full",
    "grant",
    "greatest",
    "group",
    "grouping",
    "having",
    "ilike",
    "in",
    "initially",
    "inner",
    "inout",
    "int",
    "integer",
    "intersect",
    "interval",
    "into",
    "is",
    "isnull",
    "join",
    "json",
    "json_array",
    "json_arrayagg",
    "json_exists",
    "json_object",
    "json_objectagg",
    "json_query",
    "json_scalar",
    "json_serialize",
    "json_table",
    "json_value",
    "lateral",
    "leading",
    "least",
    "left",
    "like",
    "limit",
    "localtime",
    "localtimestamp",
    "merge_action",
    "national",
    "natural",
    "nchar",
    "none",
    "normalize",
    "not",
    "notnull",
    "null",
    "nullif",
    "numeric",
    "offset",
    "on",
    "only",
    "or",
    "order",
    "out",
    "outer",
    "overlaps",
    "overlay",
    "placing",
    "position",
    "precision",
    "primary",
    "real",
    "references",
    "returning",
    "right",
    "row",
    "select",
    "session_user",
    "setof",
    "similar",
    "smallint",
    "some",
    "substring",
    "symmetric",
    "system_user",
    "table",
    "tablesample",
    "then",
    "time",
    "timestamp",
    "to",
    "trailing",
    "treat",
    "trim",
    "true",
    "union",
    "unique",
    "user",
    "using",
    "values",
    "varchar",
    "variadic",
    "verbose",
    "when",
    "where",
    "window",
    "with",
    "xmlattributes",
    "xmlconcat",
    "xmlelement",
    "xmlexists",
    "xmlforest",
    "xmlnamespaces",
    "xmlparse",
    "xmlpi",
    "xmlroot",
    "xmlserialize",
    "xmltable",
];

#[cfg(test)]
mod schema_tests {
    use super::*;
    #[test]
    fn user_schema_namespaces_include_primary_index_and_information_schema() {
        let snapshot = CatalogSnapshot {
            schemas: BTreeMap::from([("app".into(), (70000, 10))]),
            table_schemas: BTreeMap::from([(1, "app".into())]),
            tables: vec![TableDef {
                id: 1,
                name: "t".into(),
                columns: vec![ColumnDef {
                    id: 2,
                    name: "id".into(),
                    data_type: ScalarType::Integer,
                    not_null: true,
                }],
                primary_key: vec![0],
            }],
            ..Default::default()
        };
        let requested = ["pg_namespace", "pg_class", "pg_constraint", "tables", "columns"]
            .into_iter().map(str::to_owned).collect();
        let catalogs = build(&snapshot, &requested).unwrap();
        assert!(
            catalogs["pg_namespace"]
                .rows
                .iter()
                .any(|r| r[0] == I(70000) && r[1] == s("app"))
        );
        for name in ["t", "t_pkey"] {
            assert!(
                catalogs["pg_class"]
                    .rows
                    .iter()
                    .any(|r| r[1] == s(name) && r[2] == I(70000))
            );
        }
        assert!(
            catalogs["pg_constraint"]
                .rows
                .iter()
                .any(|r| r[1] == s("t_pkey") && r[2] == I(70000))
        );
        assert!(
            catalogs["tables"]
                .rows
                .iter()
                .any(|r| r[1] == s("app") && r[2] == s("t"))
        );
        assert!(
            catalogs["columns"]
                .rows
                .iter()
                .any(|r| r[1] == s("app") && r[2] == s("t"))
        );
    }
    #[test]
    fn exists_constant_projection_is_correlated_and_collects_parameters() {
        let snapshot = CatalogSnapshot::default();
        for (name, expected) in [("pg_class", true), ("absent", false)] {
            let plan = plan("SELECT EXISTS(SELECT 1 FROM pg_class WHERE relname=$1)")
                .unwrap()
                .unwrap();
            assert_eq!(
                execute(&plan, &snapshot, &[s(name)]).unwrap().rows,
                vec![vec![B(expected)]]
            );
        }
        let plan = plan("SELECT EXISTS(SELECT 1 FROM pg_class i WHERE i.oid=c.oid), NOT EXISTS(SELECT 1 FROM pg_class i WHERE i.oid=c.oid) FROM pg_class c WHERE c.relname='pg_class'").unwrap().unwrap();
        assert_eq!(
            execute(&plan, &snapshot, &[]).unwrap().rows,
            vec![vec![B(true), B(false)]]
        );
    }
}

#[cfg(test)]
mod system_xmin_tests {
    use super::*;

    #[test]
    fn every_catalog_base_table_plans_xmin_and_stamps_catalog_xid() {
        let snapshot = CatalogSnapshot {
            generation: 12345,
            ..Default::default()
        };
        let requested = known_names().iter().map(|&name| name.to_owned()).collect();
        let relations = build(&snapshot, &requested).unwrap();
        for name in known_names() {
            let sql = format!("select xmin from pg_catalog.{name}");
            if tableoid(name).is_some() {
                let plan = crate::plan(&sql).unwrap().unwrap();
                assert_eq!(
                    crate::describe(&plan).unwrap()[0].data_type,
                    PgType::Xid,
                    "{name}"
                );
                let result = crate::execute(&plan, &snapshot, &[]).unwrap();
                for row in result.rows {
                    assert_eq!(row, vec![I(12345)], "{name}");
                }
                let star = crate::plan(&format!("select * from pg_catalog.{name}"))
                    .unwrap()
                    .unwrap();
                assert!(
                    !crate::describe(&star)
                        .unwrap()
                        .iter()
                        .any(|f| f.name == "xmin"),
                    "{name}"
                );
                let oid = I(i64::from(tableoid(name).unwrap()));
                let attrs = &relations["pg_attribute"].rows;
                assert!(
                    attrs
                        .iter()
                        .any(|row| row[0] == oid && row[1] == s("xmin") && row[4] == I(-2)),
                    "{name}"
                );
                let ordinary = attrs
                    .iter()
                    .filter(|row| row[0] == oid && matches!(row[4], I(n) if n > 0))
                    .count();
                let class = relations["pg_class"]
                    .rows
                    .iter()
                    .find(|row| row[0] == oid)
                    .unwrap();
                assert_eq!(class[18], I(ordinary as i64), "{name}");
                let fields = &relations[*name].fields;
                assert_eq!(
                    fields.iter().filter(|f| f.name == "xmin").count(),
                    1,
                    "{name}"
                );
            } else if relation_namespace(name) == "pg_catalog" {
                assert_eq!(
                    crate::plan(&sql).unwrap_err(),
                    Error::UnknownColumn,
                    "{name}"
                );
            }
        }
    }
}

#[cfg(test)]
mod materialization_tests;
