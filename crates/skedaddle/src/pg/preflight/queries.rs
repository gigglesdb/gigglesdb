//! SQL uses only catalogs present on every supported major. PG18 generation
//! differences are exposed through attgenerated ('s'/'v'); no PG18-only column
//! is referenced when connected to PG16/17.
use serde_json::Value;
use tokio_postgres::Transaction;

use super::{Check, Observation, Snapshot};
use crate::definition::{ObjectKind, ObjectSelector};

fn literal(text: &str) -> String {
    // E strings are independent of standard_conforming_strings.
    format!("E'{}'", text.replace('\\', "\\\\").replace('\'', "''"))
}

fn identifier(text: &str) -> String {
    format!("\"{}\"", text.replace('"', "\"\""))
}

fn relation(object: &ObjectSelector) -> String {
    format!(
        "{}.{}",
        identifier(&object.schema),
        identifier(&object.name)
    )
}

fn table_oid(object: &ObjectSelector) -> String {
    format!(
        "SELECT c.oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname={} AND c.relname={}",
        literal(&object.schema),
        literal(&object.name)
    )
}

async fn observe(
    tx: &Transaction<'_>,
    check: Check,
    object: Option<&ObjectSelector>,
    sql: &str,
) -> Result<Observation, tokio_postgres::Error> {
    let sql =
        format!("SELECT pg_catalog.row_to_json(preflight_row)::text FROM ({sql}) preflight_row");
    tx.batch_execute("SAVEPOINT skedaddle_preflight_query")
        .await?;
    let result = tx.query(&sql, &[]).await;
    let (rows, error) = match result {
        Ok(rows) => {
            tx.batch_execute("RELEASE SAVEPOINT skedaddle_preflight_query")
                .await?;
            let parsed: Result<Vec<Value>, _> = rows
                .iter()
                .map(|row| serde_json::from_str(row.get::<_, &str>(0)))
                .collect();
            match parsed {
                Ok(rows) => (rows, None),
                Err(_) => (vec![], Some("invalid JSON result".into())),
            }
        }
        Err(error) => {
            // Never copy server DETAIL, CONTEXT or messages into evidence:
            // they can echo application values or credential-bearing SQL.
            let state = error
                .code()
                .map_or("transport-error", |code| code.code())
                .to_owned();
            tx.batch_execute("ROLLBACK TO SAVEPOINT skedaddle_preflight_query; RELEASE SAVEPOINT skedaddle_preflight_query").await?;
            (vec![], Some(format!("SQLSTATE {state}")))
        }
    };
    Ok(Observation {
        check,
        object: object.cloned(),
        sql,
        rows,
        error,
    })
}

async fn add(
    snapshot: &mut Snapshot,
    tx: &Transaction<'_>,
    check: Check,
    object: Option<&ObjectSelector>,
    sql: &str,
) -> Result<(), tokio_postgres::Error> {
    snapshot
        .observations
        .push(observe(tx, check, object, sql).await?);
    Ok(())
}

pub(super) async fn collect(
    tx: &Transaction<'_>,
    objects: &[ObjectSelector],
) -> Result<Snapshot, tokio_postgres::Error> {
    // Do not resolve built-in functions against a caller-controlled schema.
    tx.batch_execute("SET LOCAL search_path = pg_catalog")
        .await?;
    let mut snapshot = Snapshot::default();
    add(&mut snapshot, tx, Check::Version, None, "SELECT current_setting('server_version_num')::bigint AS version_num, current_setting('server_version') AS version").await?;
    let major = snapshot.observations[0]
        .rows
        .first()
        .and_then(|r| r["version_num"].as_u64())
        .unwrap_or(0)
        / 10000;
    if !(16..=18).contains(&major) {
        // Do not run catalogs we have not qualified on an unknown major.
        return Ok(snapshot);
    }
    for (check, sql) in [
        (
            Check::WalLevel,
            "SELECT current_setting('wal_level') AS wal_level, pg_is_in_recovery() AS in_recovery",
        ),
        (
            Check::SlotHeadroom,
            "SELECT current_setting('max_replication_slots')::bigint AS maximum, (SELECT count(*) FROM pg_catalog.pg_replication_slots) AS used",
        ),
        (
            Check::WalSenderHeadroom,
            "SELECT current_setting('max_wal_senders')::bigint AS maximum, (SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE backend_type='walsender') AS used",
        ),
        (
            Check::WalRetention,
            "SELECT setting::bigint AS setting, unit, CASE WHEN setting='-1' THEN -1::bigint ELSE pg_size_bytes(setting || COALESCE(unit,'')) END AS bytes FROM pg_catalog.pg_settings WHERE name='max_slot_wal_keep_size'",
        ),
        // pg_ls_waldir reports allocated bytes, never free bytes. External
        // source-filesystem evidence is separately required by classification.
        (
            Check::WalDisk,
            "SELECT COALESCE(sum(size),0)::bigint AS allocated_wal_bytes, count(*) AS wal_files FROM pg_catalog.pg_ls_waldir()",
        ),
        (
            Check::ReplicationPrivilege,
            "SELECT rolcanlogin AS login, rolreplication AS replication, rolsuper AS superuser FROM pg_catalog.pg_roles WHERE rolname=current_user",
        ),
        (
            Check::DatabaseCreate,
            "SELECT has_database_privilege(current_user,current_database(),'CREATE') AS create",
        ),
        // Still superuser-only on PG16, PG17 and PG18. Do not confuse the
        // pg_create_subscription role with event-trigger creation authority.
        (
            Check::EventTriggerPrivilege,
            "SELECT current_setting('server_version_num')::bigint AS version_num, rolsuper AS superuser FROM pg_catalog.pg_roles WHERE rolname=current_user",
        ),
        (
            Check::OriginGrants,
            "SELECT p.oid::regprocedure::text AS function, has_function_privilege(current_user,p.oid,'EXECUTE') AS execute FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='pg_catalog' AND p.proname ~ '^pg_replication_origin_' ORDER BY p.oid",
        ),
        (
            Check::LargeObjects,
            "SELECT count(*) AS count FROM pg_catalog.pg_largeobject_metadata",
        ),
        (
            Check::PreparedTransactions,
            "SELECT gid, owner, database, prepared::text AS prepared FROM pg_catalog.pg_prepared_xacts ORDER BY prepared, gid",
        ),
        // pg_stat_activity masks other users' transactions for ordinary roles.
        // The visibility flag prevents masked rows from becoming a false pass.
        (
            Check::LongTransactions,
            "SELECT (SELECT rolsuper OR pg_has_role(current_user,'pg_read_all_stats','USAGE') FROM pg_catalog.pg_roles WHERE rolname=current_user) AS visible, COALESCE(max(EXTRACT(epoch FROM (clock_timestamp()-xact_start))) FILTER (WHERE pid<>pg_backend_pid()),0)::double precision AS oldest_seconds, count(*) FILTER (WHERE pid<>pg_backend_pid() AND xact_start IS NOT NULL) AS transactions FROM pg_catalog.pg_stat_activity",
        ),
        (
            Check::RestartLocks,
            "SELECT name, setting, unit, context, pending_restart FROM pg_catalog.pg_settings WHERE name IN ('wal_level','max_replication_slots','max_wal_senders','max_slot_wal_keep_size') ORDER BY name",
        ),
    ] {
        add(&mut snapshot, tx, check, None, sql).await?;
    }

    // Inventory only schemas named by the selection. With an empty selection
    // no user objects are in scope; server-wide risk checks still run above.
    let schemas = objects
        .iter()
        .map(|o| literal(&o.schema))
        .collect::<Vec<_>>()
        .join(",");
    let schema_filter = if schemas.is_empty() {
        "false".to_owned()
    } else {
        format!("n.nspname IN ({schemas})")
    };
    for (check, sql) in [
        (Check::Views, format!("SELECT n.nspname AS schema,c.relname AS name,c.relkind::text AS kind,pg_get_viewdef(c.oid,true) AS definition,c.relacl::text AS acl FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind IN ('v','m') AND {schema_filter} ORDER BY n.nspname,c.relname")),
        // No prosrc/proconfig: routine bodies/settings may contain secrets.
        (Check::Routines, format!("SELECT n.nspname AS schema,p.proname AS name,pg_get_function_identity_arguments(p.oid) AS arguments,p.prokind::text AS kind,l.lanname AS language,p.prosecdef AS security_definer,p.provolatile::text AS volatility,p.proacl::text AS acl FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace JOIN pg_catalog.pg_language l ON l.oid=p.prolang WHERE {schema_filter} ORDER BY n.nspname,p.proname,p.oid")),
        // Every installed extension is disclosed: types/functions can depend on
        // extension objects outside the selected schemas.
        (Check::Extensions, "SELECT e.extname AS name,e.extversion AS version,n.nspname AS schema,e.extrelocatable AS relocatable FROM pg_catalog.pg_extension e JOIN pg_catalog.pg_namespace n ON n.oid=e.extnamespace ORDER BY e.extname".into()),
    ] {
        add(&mut snapshot, tx, check, None, &sql).await?;
    }

    for object in objects {
        if object.kind != ObjectKind::Table {
            // Keep an explicit blocker, even for a missing non-table object.
            add(
                &mut snapshot,
                tx,
                Check::SelectedObject,
                Some(object),
                &format!(
                    "SELECT {}::text AS schema,{}::text AS name,{}::text AS selected_kind",
                    literal(&object.schema),
                    literal(&object.name),
                    literal(&format!("{:?}", object.kind))
                ),
            )
            .await?;
            continue;
        }
        let oid = table_oid(object);
        add(&mut snapshot, tx, Check::Table, Some(object), &format!("SELECT c.relkind::text AS kind,c.relpersistence::text AS persistence,c.relreplident::text AS replica_identity,c.relrowsecurity AS rls,c.relforcerowsecurity AS force_rls,row_security_active(c.oid) AS rls_active FROM pg_catalog.pg_class c WHERE c.oid IN ({oid})")).await?;
        let keys = format!(
            "SELECT c.relreplident::text AS replica_identity,ic.relname AS name,i.indisprimary AS primary,i.indisunique AS unique,i.indisvalid AS valid,i.indisready AS ready,i.indimmediate AS immediate,i.indisreplident AS identity_index,(i.indpred IS NOT NULL) AS partial,(i.indexprs IS NOT NULL) AS expression,COALESCE((SELECT bool_and(a.attnotnull) FROM unnest(i.indkey::smallint[]) WITH ORDINALITY k(attnum,ord) LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid=c.oid AND a.attnum=k.attnum WHERE k.ord<=i.indnkeyatts),false) AS not_null,ARRAY(SELECT a.attname FROM unnest(i.indkey::smallint[]) WITH ORDINALITY k(attnum,ord) JOIN pg_catalog.pg_attribute a ON a.attrelid=c.oid AND a.attnum=k.attnum WHERE k.ord<=i.indnkeyatts ORDER BY k.ord)AS columns,am.amname AS access_method,ARRAY(SELECT json_build_object('column',a.attname,'desc',(i.indoption[k.ord::int-1] & 1)<>0,'nulls_first',(i.indoption[k.ord::int-1] & 2)<>0,'collation_schema',cn.nspname,'collation_name',coll.collname,'opclass_schema',opn.nspname,'opclass_name',op.opcname,'opclass_default',op.opcdefault,'opclass_input_oid',op.opcintype::bigint,'opfamily_schema',ofn.nspname,'opfamily_name',opf.opfname) FROM unnest(i.indkey::smallint[]) WITH ORDINALITY k(attnum,ord) JOIN pg_catalog.pg_attribute a ON a.attrelid=c.oid AND a.attnum=k.attnum JOIN pg_catalog.pg_opclass op ON op.oid=i.indclass[k.ord::int-1] JOIN pg_catalog.pg_namespace opn ON opn.oid=op.opcnamespace JOIN pg_catalog.pg_opfamily opf ON opf.oid=op.opcfamily JOIN pg_catalog.pg_namespace ofn ON ofn.oid=opf.opfnamespace LEFT JOIN pg_catalog.pg_collation coll ON coll.oid=i.indcollation[k.ord::int-1] LEFT JOIN pg_catalog.pg_namespace cn ON cn.oid=coll.collnamespace WHERE k.ord<=i.indnkeyatts ORDER BY k.ord) AS key_columns FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_index i ON i.indrelid=c.oid LEFT JOIN pg_catalog.pg_class ic ON ic.oid=i.indexrelid LEFT JOIN pg_catalog.pg_am am ON am.oid=ic.relam WHERE c.oid IN ({oid}) ORDER BY ic.relname"
        );
        for check in [Check::Key, Check::ReplicaIdentity, Check::MutableKeys] {
            add(&mut snapshot, tx, check, Some(object), &keys).await?;
        }
        // Scan actual data for each nullable unique index, grouping composite
        // keys with NULLs (SQL UNIQUE normally allows those duplicate groups).
        let key_rows = snapshot.observations.last().unwrap().rows.clone();
        let mut duplicate_queries = Vec::new();
        for row in key_rows {
            if row["unique"] != true
                || row["not_null"] != false
                || row["partial"] != false
                || row["expression"] != false
            {
                continue;
            }
            let columns: Vec<_> = row["columns"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(identifier)
                .collect();
            if columns.is_empty() {
                continue;
            }
            duplicate_queries.push(format!("SELECT {}::text AS index_name,count(*) AS duplicate_groups FROM (SELECT 1 FROM {} GROUP BY {} HAVING count(*)>1) duplicate_keys", literal(row["name"].as_str().unwrap_or("")), relation(object), columns.join(",")));
        }
        let duplicates = if duplicate_queries.is_empty() {
            "SELECT 0::bigint AS duplicate_groups WHERE false".into()
        } else {
            duplicate_queries.join(" UNION ALL ")
        };
        add(
            &mut snapshot,
            tx,
            Check::NullableKeyDuplicates,
            Some(object),
            &duplicates,
        )
        .await?;
        // Failed key inspection must not imply there were no duplicate keys.
        let key_error = snapshot
            .observations
            .iter()
            .find(|o| o.check == Check::Key && o.object.as_ref() == Some(object))
            .and_then(|o| o.error.clone());
        if let Some(error) = key_error {
            snapshot.observations.last_mut().unwrap().error =
                Some(format!("key enumeration failed: {error}"));
        }

        for (check, sql) in [
            (
                Check::SchemaDescriptor,
                format!(
                    "SELECT c.oid::bigint AS oid, (SELECT jsonb_agg(jsonb_build_array(a.attnum,a.attname,a.atttypid,a.atttypmod,a.attcollation,a.attnotnull,a.attidentity,a.attgenerated,pg_get_expr(d.adbin,d.adrelid)) ORDER BY a.attnum) FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped) AS columns, (SELECT jsonb_agg(jsonb_build_array(conname,contype,convalidated,pg_get_constraintdef(oid)) ORDER BY conname) FROM pg_constraint WHERE conrelid=c.oid) AS constraints, (SELECT jsonb_agg(jsonb_build_array(indexrelid,indisvalid,indisready,pg_get_indexdef(indexrelid)) ORDER BY indexrelid) FROM pg_index WHERE indrelid=c.oid) AS indexes FROM pg_class c WHERE c.oid IN ({oid})"
                ),
            ),
            (
                Check::ColumnTypes,
                format!(
                    "SELECT a.attname AS column,t.oid::bigint AS oid,tn.nspname AS schema,t.typname AS name,t.typtype::text AS kind,t.typbasetype::bigint AS base_oid,t.typelem::bigint AS element_oid,a.atttypmod AS typmod,format_type(a.atttypid,a.atttypmod) AS formatted,a.attnotnull AS not_null,a.atthasdef AS has_default,(a.attgenerated<>'') AS generated,(a.attidentity<>'') AS identity FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_type t ON t.oid=a.atttypid JOIN pg_catalog.pg_namespace tn ON tn.oid=t.typnamespace WHERE a.attrelid IN ({oid}) AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum"
                ),
            ),
            (
                Check::GeneratedColumns,
                format!(
                    "SELECT a.attname AS column,a.attgenerated::text AS generated,pg_get_expr(d.adbin,d.adrelid) AS expression,{major}::bigint AS server_major FROM pg_catalog.pg_attribute a LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE a.attrelid IN ({oid}) AND a.attnum>0 AND NOT a.attisdropped AND a.attgenerated<>'' ORDER BY a.attnum"
                ),
            ),
            (
                Check::IdentityColumns,
                format!(
                    "SELECT a.attname AS column,a.attidentity::text AS identity FROM pg_catalog.pg_attribute a WHERE a.attrelid IN ({oid}) AND a.attnum>0 AND NOT a.attisdropped AND a.attidentity<>'' ORDER BY a.attnum"
                ),
            ),
            (
                Check::Sequences,
                format!(
                    "SELECT DISTINCT n.nspname AS schema,c.relname AS name,s.seqstart,s.seqincrement,s.seqmax,s.seqmin,s.seqcache,s.seqcycle,c.relacl::text AS acl,has_sequence_privilege(current_user,c.oid,'SELECT') AS select FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace JOIN pg_catalog.pg_sequence s ON s.seqrelid=c.oid WHERE c.oid IN (SELECT d.objid FROM pg_catalog.pg_depend d WHERE d.classid='pg_catalog.pg_class'::regclass AND d.refclassid='pg_catalog.pg_class'::regclass AND d.refobjid IN ({oid}) AND d.deptype IN ('a','i') UNION SELECT d.refobjid FROM pg_catalog.pg_depend d JOIN pg_catalog.pg_attrdef a ON a.oid=d.objid WHERE d.classid='pg_catalog.pg_attrdef'::regclass AND d.refclassid='pg_catalog.pg_class'::regclass AND a.adrelid IN ({oid})) ORDER BY n.nspname,c.relname"
                ),
            ),
            (
                Check::Triggers,
                format!(
                    "SELECT format('%I.%I.%I',n.nspname,c.relname,t.tgname) AS qualified_name,t.tgenabled::text AS enabled,t.tgisinternal AS internal,t.tgtype AS type,fn.nspname AS function_schema,p.proname AS function_name,l.lanname AS language,p.prosecdef AS security_definer FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_class c ON c.oid=t.tgrelid JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace JOIN pg_catalog.pg_proc p ON p.oid=t.tgfoid JOIN pg_catalog.pg_namespace fn ON fn.oid=p.pronamespace JOIN pg_catalog.pg_language l ON l.oid=p.prolang WHERE t.tgrelid IN ({oid}) ORDER BY t.tgname"
                ),
            ),
            (
                Check::Partitions,
                format!(
                    "WITH RECURSIVE family(oid) AS (SELECT c.oid FROM pg_catalog.pg_class c WHERE c.oid IN ({oid}) UNION SELECT i.inhrelid FROM pg_catalog.pg_inherits i JOIN family f ON f.oid=i.inhparent) SELECT n.nspname AS schema,c.relname AS name,c.relkind::text AS kind,c.relispartition AS is_partition,pg_get_expr(c.relpartbound,c.oid) AS bound,pg_partition_root(c.oid)::text AS root,ARRAY(SELECT p.pubname||':root='||p.pubviaroot::text FROM pg_catalog.pg_publication_rel pr JOIN pg_catalog.pg_publication p ON p.oid=pr.prpubid WHERE pr.prrelid=c.oid) AS publications FROM family f JOIN pg_catalog.pg_class c ON c.oid=f.oid JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind='p' OR c.relispartition ORDER BY n.nspname,c.relname"
                ),
            ),
            (
                Check::Grants,
                format!(
                    "SELECT c.relowner::regrole::text AS owner_name,c.relacl::text AS acl,n.nspacl::text AS schema_acl,has_table_privilege(current_user,c.oid,'SELECT') AS select,has_schema_privilege(current_user,n.oid,'USAGE') AS usage,pg_has_role(current_user,c.relowner,'USAGE') AS owner,row_security_active(c.oid) AS rls_active,(SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname=current_user) AS superuser FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE c.oid IN ({oid})"
                ),
            ),
        ] {
            add(&mut snapshot, tx, check, Some(object), &sql).await?;
        }
    }
    Ok(snapshot)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quotes_identifiers_and_literals_without_search_path_or_guc_assumptions() {
        assert_eq!(identifier("a\"b"), "\"a\"\"b\"");
        assert_eq!(literal("a'\\b"), "E'a''\\\\b'");
        let object = ObjectSelector {
            schema: "odd'.schema".into(),
            name: "x\";drop table t;--".into(),
            kind: ObjectKind::Table,
            exclude_columns: Vec::new(),
        };
        assert_eq!(
            relation(&object),
            "\"odd'.schema\".\"x\"\";drop table t;--\""
        );
        assert!(table_oid(&object).contains("E'odd''.schema'"));
    }
}
