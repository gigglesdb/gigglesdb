mod information_schema;
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
                .find(|(id, _)| i64::from(*id) == oid)
                .map(|(_, name)| *name)
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
    if let Some(fields) = information_schema::schema(name) {
        return Some(fields);
    }
    let mut result = fields(match name {
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
        "pg_user_mapping" => &[
            ("oid", T::Oid),
            ("umuser", T::Oid),
            ("umserver", T::Oid),
            ("umoptions", T::TextArray),
        ],
        // No PostgreSQL lock manager or assigned transaction xid is exposed.
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
            ("collcollate", T::Name),
            ("collctype", T::Name),
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
    });
    if tableoid(name).is_some() && !result.iter().any(|f| f.name == "xmin") {
        result.push(Field {
            name: "xmin".into(),
            data_type: T::Xid,
        });
    }
    Some(result)
}
pub(crate) fn known_names() -> &'static [&'static str] {
    &[
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
            "pg_indexes"
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
pub(crate) fn build(snapshot: &CatalogSnapshot) -> R<BTreeMap<String, Relation>> {
    if snapshot.tables.len() > 128 || snapshot.roles.len() > MAX_ROWS - PREDEFINED_ROLES.len() {
        return Err(Error::Limit);
    }
    if snapshot.schemas.len() > 128 || snapshot.table_schemas.len() > 128 {
        return Err(Error::Limit);
    }
    let mut schema_oids = BTreeSet::new();
    for (name, (oid, owner)) in &snapshot.schemas {
        if name.is_empty()
            || name.len() > 63
            || name == "public"
            || *oid < 65536
            || !schema_oids.insert(*oid)
            || !snapshot.roles.iter().any(|r| r.oid == *owner)
        {
            return Err(Error::InvalidSnapshot);
        }
    }
    if snapshot.table_schemas.iter().any(|(id, name)| {
        !snapshot.tables.iter().any(|t| t.id == *id) || !snapshot.schemas.contains_key(name)
    }) {
        return Err(Error::InvalidSnapshot);
    }
    let mut names = BTreeSet::new();
    let mut ids = BTreeSet::new();
    let mut input_bytes = 0usize;
    let mut role_ids = BTreeSet::new();
    let mut role_names = BTreeSet::new();
    for role in &snapshot.roles {
        if role.name.is_empty()
            || role.name.starts_with("pg_")
            || PREDEFINED_ROLES.iter().any(|(oid, _)| *oid == role.oid)
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
    if snapshot.user_types.len() > 16384 {
        return Err(Error::Limit);
    }
    for ty in &snapshot.user_types {
        if ty.oid < 65536
            || (ty.oid - 65536) % 4 != 0
            || !type_oids.insert(ty.oid)
            || !type_names.insert(&ty.name)
            || ty.name.is_empty()
            || ty.name.len() > 63
            || !matches!(ty.kind, 'e' | 'd' | 'c')
            || ty.labels.len() > 64
        {
            return Err(Error::InvalidSnapshot);
        }
        let mut labels = BTreeSet::new();
        for label in &ty.labels {
            if label.len() > 63 || !labels.insert(label) {
                return Err(Error::InvalidSnapshot);
            }
            input_bytes += label.len();
        }
        let last_oid = u64::from(ty.oid) + ty.labels.len() as u64 * 4;
        if last_oid > u32::MAX as u64 {
            return Err(Error::InvalidSnapshot);
        }
        input_bytes += ty.name.len();
        if input_bytes > 65536 {
            return Err(Error::Limit);
        }
    }
    for t in &snapshot.tables {
        if !ids.insert(t.id)
            || !names.insert((snapshot.table_schema(t.id).0, &t.name))
            || t.name.is_empty()
            || t.columns.is_empty()
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
        let t = snapshot.tables.iter().find(|t| t.id == i.table).unwrap();
        if !descriptor_ids.insert(i.id)
            || !descriptor_names.insert((snapshot.table_schema(i.table).0, &i.name))
            || i.name.is_empty()
            || i.columns.is_empty()
            || i.columns.len() > 64
            || i.columns.iter().collect::<BTreeSet<_>>().len() != i.columns.len()
            || i.columns
                .iter()
                .any(|id| !t.columns.iter().any(|c| c.id == *id))
        {
            return Err(Error::InvalidSnapshot);
        }
        if schema_oids.contains(&table_oid(i.id)?) {
            return Err(Error::InvalidSnapshot);
        }
        input_bytes += i.name.len();
        if input_bytes > 65536 {
            return Err(Error::Limit);
        }
    }
    if snapshot
        .table_owners
        .iter()
        .any(|(table, owner)| !ids.contains(table) || !role_ids.contains(owner))
    {
        return Err(Error::InvalidSnapshot);
    }
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
    let mut key_names = BTreeMap::new();
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
        }
        occupied.insert((snapshot.table_schema(t.id).0, name.clone()));
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
    // These absent features are queryable, but do not expand the established
    // finite reflected pg_class/pg_type inventory.
    for name in [
        "pg_statistic_ext",
        "pg_publication",
        "pg_publication_namespace",
        "pg_publication_rel",
    ] {
        all.insert(
            name.into(),
            Relation {
                fields: schema(name).unwrap(),
                rows: vec![],
            },
        );
    }
    let mut add = |name: &str, row: Vec<Value>| {
        all.get_mut(name).unwrap().rows.push(row);
    };
    // Only this freshly authorized connection is visible. TLS details beyond
    // the handshake status are not retained by the hosting session.
    add(
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
    add(
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
        add("pg_namespace", vec![I(id), s(name), I(owner), N, N]);
    }
    add(
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
        add("pg_tablespace", vec![I(oid), s(name), I(10), N, N, N]);
    }
    if snapshot.database == "postgres" {
        add(
            "pg_shdescription",
            vec![
                I(16384),
                I(1262),
                I(0),
                s("default administrative connection database"),
            ],
        );
    }
    add("pg_am", vec![I(2), s("heap"), I(3), s("t"), N]);
    add("pg_am", vec![I(403), s("btree"), I(330), s("i"), N]);
    // Snapshot flags and predefined identities are presentation metadata, not
    // authentication, privileges, memberships, or enforced role policies.
    let predefined = PREDEFINED_ROLES
        .iter()
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
    for role in snapshot.roles.iter().chain(&predefined) {
        add(
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
        add(
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
            add("pg_user", row.clone());
            row[6] = N;
            add("pg_shadow", row);
        }
    }
    // No timezone conversion database exists. Startup timezone presentation
    // labels do not establish a PostgreSQL timezone catalog.
    // Catalog relations are genuine virtual tables, with their actual columns below.
    for &name in known_names() {
        let oid = relation_oid(name).unwrap() as i64;
        let fs = schema(name).unwrap();
        let info = information_schema::identity(name);
        let namespace = if info.is_some() { 13293 } else { 11 };
        let rowtype = info.map_or(32768 + oid, |(_, ty)| i64::from(ty));
        add(
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
        add(
            "pg_type",
            composite_row(rowtype as u32, name, namespace, oid, 10),
        );
        for (i, f) in fs
            .iter()
            .filter(|f| f.name != "xmin")
            .chain(fs.iter().filter(|f| f.name == "xmin"))
            .enumerate()
        {
            add(
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
        (27, "tid", 6, 1010),
        (29, "cid", 4, 1012),
        (28, "xid", 4, 1011),
        (1033, "aclitem", 16, 1034),
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
    // PostgreSQL 18 nominal metadata only, not a callable routine registry.
    // pg_type.dat + pg_type.h: both are base types; node trees are category Z,
    // not pseudo-types. Only regproc has an associated array type.
    for (oid, name, len, category, elem, array, input) in [
        (24, "regproc", 4, "N", 0, 1008, 44),
        (1008, "_regproc", -1, "A", 24, 0, 750),
        (194, "pg_node_tree", -1, "Z", 0, 0, 195),
    ] {
        add(
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
        add("pg_type", row);
    }
    for (name, (oid, owner)) in &snapshot.schemas {
        add(
            "pg_namespace",
            vec![I(i64::from(*oid)), s(name), I(i64::from(*owner)), N, N],
        );
    }
    let mut ordered_tables = snapshot.tables.iter().collect::<Vec<_>>();
    ordered_tables.sort_by_key(|t| t.id);
    for t in ordered_tables {
        let (schema, namespace) = snapshot.table_schema(t.id);
        let namespace = i64::from(namespace);
        let oid = table_oid(t.id)? as i64;
        let has_pk = !t.primary_key.is_empty();
        let owner = i64::from(*snapshot.table_owners.get(&t.id).unwrap_or(&10));
        add(
            "pg_class",
            vec![
                I(oid),
                s(&t.name),
                I(namespace),
                s("r"),
                I(owner),
                I(t.columns.len() as i64),
                B(has_pk || visible_indexes.iter().any(|i| i.table == t.id)),
                s("p"),
                B(false),
                I(oid + 3),
                I(2),
                I(0),
                N,
                N,
                N,
                N,
            ],
        );
        add(
            "pg_type",
            composite_row((oid + 3) as u32, &t.name, namespace, oid, owner),
        );
        // Heap system attributes exist in pg_attribute but are excluded by
        // clients that explicitly request attnum > 0.
        for (attnum, name, ty, len) in [
            (-6, "tableoid", 26, 4),
            (-5, "cmax", 29, 4),
            (-4, "xmax", 28, 4),
            (-3, "cmin", 29, 4),
            (-2, "xmin", 28, 4),
            (-1, "ctid", 27, 6),
        ] {
            add(
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
            add(
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
                "pg_indexes",
                vec![
                    s(schema),
                    s(&t.name),
                    s(n),
                    N,
                    s(&index_definition(schema, t, n, &t.primary_key, true)),
                ],
            );
            add(
                "pg_class",
                vec![
                    I(oid + 1),
                    s(n),
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
                add(
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
            add(
                "pg_constraint",
                vec![
                    I(oid + 2),
                    s(n),
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
            add(
                "pg_depend",
                vec![I(1259), I(oid + 1), I(0), I(2606), I(oid + 2), I(0), s("i")],
            );
        }
        let mut indexes = visible_indexes
            .iter()
            .filter(|i| i.table == t.id)
            .collect::<Vec<_>>();
        indexes.sort_by_key(|i| i.id);
        for index in indexes {
            let index_oid = table_oid(index.id)? as i64;
            let n = &index.name;
            let keys = index
                .columns
                .iter()
                .map(|id| t.columns.iter().position(|c| c.id == *id).unwrap())
                .collect::<Vec<_>>();
            let key = Value::Array(keys.iter().map(|k| I(*k as i64 + 1)).collect());
            add(
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
            add(
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
                    N,
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
                add(
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
            add(
                "pg_indexes",
                vec![
                    s(schema),
                    s(&t.name),
                    s(n),
                    N,
                    s(&index_definition(schema, t, n, &keys, index.unique)),
                ],
            );
        }
    }
    all.get_mut("pg_index")
        .unwrap()
        .rows
        .sort_by_key(|r| match (&r[1], &r[0]) {
            (I(t), I(i)) => (*t, *i),
            _ => unreachable!(),
        });
    // Append descriptor extensions here so established field positions remain stable.
    for row in &mut all.get_mut("pg_class").unwrap().rows {
        if row[2] == I(2200) {
            row[10] = I(if row[3] == s("i") { 403 } else { 2 });
        }
        row.extend([
            I(0),
            B(false),
            B(false),
            B(false),
            B(false),
            I(0),
            s(if row[3] == s("i") { "n" } else { "d" }),
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
    }
    for ty in &snapshot.user_types {
        let len = match (ty.kind, ty.base) {
            ('e', _) | ('d', 23 | 700 | 1082) => 4,
            ('d', 20 | 701 | 1083 | 1114 | 1184) => 8,
            ('d', 21) => 2,
            ('d', 16) => 1,
            _ => -1,
        };
        let mut row = type_row(ty.oid, &ty.name, len, 0, 0, false);
        row[2] = I(if ty.schema == "public" {
            2200
        } else {
            snapshot
                .schemas
                .get(&ty.schema)
                .ok_or(Error::InvalidSnapshot)?
                .0 as i64
        });
        row[4] = s(&ty.kind.to_string());
        row[5] = s(match ty.kind {
            'e' => "E",
            'c' => "C",
            _ => "N",
        });
        row[6] = B(ty.not_null);
        row[7] = I(ty.base as i64);
        row[8] = I(ty.typmod as i64);
        all.get_mut("pg_type").unwrap().rows.push(row);
        for (i, label) in ty.labels.iter().enumerate() {
            // Label OIDs are stable within this immutable enumeration.
            let oid = i64::from(ty.oid) + 4 * (i as i64 + 1);
            all.get_mut("pg_enum").unwrap().rows.push(vec![
                I(oid),
                I(ty.oid as i64),
                Value::Float4(giggles_pg_types::Float4::from_bits(
                    ((i + 1) as f32).to_bits(),
                )),
                s(label),
            ]);
        }
    }
    for row in &mut all.get_mut("pg_type").unwrap().rows {
        let oid = match row[0] {
            I(v) => v,
            _ => 0,
        };
        row.push(I(match oid {
            19 | 1003 => 950,
            25 | 1009 => 100,
            _ => 0,
        }));
        row.push(s(type_layout(oid).0));
    }
    for row in &mut all.get_mut("pg_attribute").unwrap().rows {
        row[11] = I(match row[2] {
            I(19) => 950,
            I(25) => 100,
            _ => 0,
        });
    }
    for row in &mut all.get_mut("pg_attribute").unwrap().rows {
        let oid = match row[2] {
            I(oid) => oid,
            _ => 0,
        };
        let (storage, align) = type_layout(oid);
        // Builders above supply the structural fields in their original order.
        // Project once into PG18's physical order, including NULL column ACLs.
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
            old[10].clone(),
            B(false),
            old[8].clone(),
            old[9].clone(),
            old[7].clone(),
            old[14].clone(),
            I(0),
            old[11].clone(),
            N,
            N,
            N,
            old[15].clone(),
            N,
            old[12].clone(),
        ];
    }
    // PG18 discovers NOT NULL via pg_constraint rather than attnotnull.
    // These are snapshot-only identities; no persisted format is changed.
    let mut used_oids = all
        .values()
        .flat_map(|relation| {
            let oid = relation.fields.iter().position(|field| field.name == "oid");
            relation.rows.iter().filter_map(move |row| {
                oid.and_then(|i| match row[i] {
                    I(oid) => Some(oid),
                    _ => None,
                })
            })
        })
        .collect::<BTreeSet<_>>();
    let mut next_oid = i64::from(u32::MAX);
    let mut tables = snapshot.tables.iter().collect::<Vec<_>>();
    tables.sort_by_key(|table| table.id);
    for table in tables {
        for (i, column) in table.columns.iter().enumerate() {
            if !column.not_null && !table.primary_key.contains(&i) {
                continue;
            }
            while !used_oids.insert(next_oid) {
                next_oid -= 1;
            }
            all.get_mut("pg_constraint").unwrap().rows.push(vec![
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
    for row in &mut all.get_mut("pg_constraint").unwrap().rows {
        row.extend([I(0), B(false), B(true), B(true), I(0)]);
    }
    for row in &mut all.get_mut("pg_database").unwrap().rows {
        row.extend([I(6), s("c"), s("C.UTF-8"), s("C.UTF-8"), N, N, I(1663)]);
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
                if xmin == row.len() {
                    row.push(N);
                }
                row[xmin] = I(i64::from(snapshot.generation as u32));
            }
        }
    }
    information_schema::populate(snapshot, &key_names, &mut all)?;
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
        } else if matches!(oid, 17 | 27 | 28 | 29 | 1033 | 2950 | 3802) {
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
fn index_definition(
    schema: &str,
    t: &TableDef,
    name: &str,
    keys: &[usize],
    unique: bool,
) -> String {
    format!(
        "CREATE {}INDEX {} ON {}.{} USING btree ({})",
        if unique { "UNIQUE " } else { "" },
        quote_identifier(name),
        quote_identifier(schema),
        quote_identifier(&t.name),
        keys.iter()
            .map(|k| quote_identifier(&t.columns[*k].name))
            .collect::<Vec<_>>()
            .join(", ")
    )
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
        let catalogs = build(&snapshot).unwrap();
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
        let relations = build(&snapshot).unwrap();
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
                assert_eq!(class[5], I(ordinary as i64), "{name}");
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
