//! Verified-ready checks capability to perform P11a inside the fenced window.
//! Installed objects are optional here; incompatible name collisions still block.
use super::destination_readiness::{flags, rows};
use tokio_postgres::Client;

fn literal(value: &str) -> String {
    format!("E'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

fn destination_sql(name: &str, scope: &str) -> String {
    format!(
        "SELECT 'destination wal_level logical',(current_setting('wal_level')='logical')::text
UNION ALL SELECT 'destination reverse replication role',(rolsuper OR rolreplication)::text FROM pg_roles WHERE rolname=current_user
UNION ALL SELECT 'destination publication/DDL setup role',(rolsuper AND has_database_privilege(current_database(),'CREATE'))::text FROM pg_roles WHERE rolname=current_user
UNION ALL SELECT 'destination reverse publication compatible or installable',(NOT EXISTS(SELECT 1 FROM pg_publication WHERE pubname={name}) OR EXISTS(SELECT 1 FROM pg_publication WHERE pubname={name} AND NOT puballtables AND pubinsert AND pubupdate AND pubdelete AND pubtruncate AND NOT pubviaroot
AND (SELECT array_agg(prrelid ORDER BY prrelid) FROM pg_publication_rel WHERE prpubid=pg_publication.oid) IS NOT DISTINCT FROM (SELECT array_agg(x ORDER BY x) FROM unnest(ARRAY[{scope}]::oid[]) x))
AND NOT EXISTS(SELECT 1 FROM pg_publication_rel WHERE prpubid=(SELECT oid FROM pg_publication WHERE pubname={name}) AND (prqual IS NOT NULL OR prattrs IS NOT NULL))
AND NOT EXISTS(SELECT 1 FROM pg_publication_namespace WHERE pnpubid=(SELECT oid FROM pg_publication WHERE pubname={name})))::text
UNION ALL SELECT 'destination reverse slot compatible or creatable',(NOT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name={name}) OR EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name={name} AND plugin='pgoutput' AND slot_type='logical' AND database=current_database() AND NOT temporary AND NOT active AND restart_lsn IS NOT NULL AND confirmed_flush_lsn IS NOT NULL AND wal_status IN ('reserved','extended')))::text
UNION ALL SELECT 'destination slot capacity',((SELECT count(*) FROM pg_replication_slots)<current_setting('max_replication_slots')::int OR EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name={name}))::text
UNION ALL SELECT 'destination reverse table SELECT',bool_and(has_table_privilege(x,'SELECT'))::text FROM unnest(ARRAY[{scope}]::oid[]) x
UNION ALL SELECT 'destination reverse schema USAGE',bool_and(has_schema_privilege(relnamespace,'USAGE'))::text FROM pg_class WHERE oid IN ({scope})"
    )
}

fn source_sql(scope: &str) -> String {
    let mut probes = vec![
        "SELECT 'old source reverse apply/origin installation role',rolsuper::text FROM pg_roles WHERE rolname=current_user".into(),
        "SELECT 'old source receiving schema installable',(has_database_privilege(current_database(),'CREATE') AND COALESCE((SELECT has_schema_privilege(oid,'USAGE') AND has_schema_privilege(oid,'CREATE') FROM pg_namespace WHERE nspname='_skedaddle'),true))::text".into(),
        format!("SELECT 'old source reverse schema USAGE',bool_and(has_schema_privilege(relnamespace,'USAGE'))::text FROM pg_class WHERE oid IN ({scope})"),
    ];
    for privilege in ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"] {
        probes.push(format!("SELECT 'old source reverse target {privilege}',bool_and(has_table_privilege(x,'{privilege}'))::text FROM unnest(ARRAY[{scope}]::oid[]) x"));
    }
    for table in ["apply_progress", "apply_receipts"] {
        for privilege in ["SELECT", "INSERT", "UPDATE", "DELETE"] {
            // A missing object is installed by the qualified superuser role.
            // Existing objects must grant every privilege used by reverse apply.
            probes.push(format!("SELECT 'old source {table} {privilege} or installable',CASE WHEN to_regclass('_skedaddle.{table}') IS NULL THEN true ELSE has_table_privilege(to_regclass('_skedaddle.{table}'),'{privilege}') END::text"));
        }
    }
    probes.join(" UNION ALL ")
}

pub(super) async fn probe(
    source: Option<&Client>,
    destination: Option<&Client>,
    id: &str,
    scope: &str,
) -> Vec<String> {
    let mut gaps = vec![];
    for (endpoint, client, sql) in [
        (
            "destination",
            destination,
            destination_sql(&literal(&format!("skedaddle_{id}_rev")), scope),
        ),
        ("old source", source, source_sql(scope)),
    ] {
        match rows(client, &sql).await.and_then(|r| flags(&r)) {
            Ok(missing) => gaps.extend(missing),
            Err(error) => gaps.push(format!("{endpoint}: {error}")),
        }
    }
    // Superuser plus a supported version establishes event-trigger installation
    // capability. If already present, keep P11a's exact collision verification.
    let result = async {
        let version = rows(destination, "SELECT current_setting('server_version_num')").await?;
        let trigger = version.first().and_then(|r| r.first()).and_then(|v| v.parse().ok())
            .and_then(|v| crate::pg::ddl::TriggerSql::new(id, v).ok())
            .ok_or("destination PostgreSQL version cannot qualify P11a DDL setup; use PG16-18")?;
        let installed = rows(destination, &format!("SELECT EXISTS(SELECT 1 FROM pg_event_trigger WHERE evtname={})::text", literal(&format!("skedaddle_ddl_{id}")))).await?;
        match installed.first().and_then(|r| r.first()).map(String::as_str) {
            Some("false") => Ok(()),
            Some("true") if rows(destination, &trigger.verify).await?.first().and_then(|r| r.first()).map(String::as_str) == Some("t") => Ok(()),
            _ => Err("destination exact P11a DDL trigger collision verification unavailable or false; repair setup".into()),
        }
    }.await;
    if let Err(error) = result {
        gaps.push(error);
    }
    gaps
}
