use super::{ApplyError, Client, OriginPolicy, Result};
use std::future::Future;
use tokio_postgres::GenericClient;

pub const INSTALL_OPAQUE_INTENT: &str = "install-skedaddle-opaque-apply-progress-and-receipts-v3";
pub const OPAQUE_DDL: &str = "CREATE SCHEMA IF NOT EXISTS _skedaddle;
CREATE TABLE IF NOT EXISTS _skedaddle.apply_progress_v3 (
 stream text PRIMARY KEY, last_mseq numeric NOT NULL, last_txn bytea NOT NULL,
 position_domain text NOT NULL, last_position bytea NOT NULL, last_digest bytea NOT NULL,
 record_kind smallint, manifest_digest bytea, previous_manifest_digest bytea);
CREATE TABLE IF NOT EXISTS _skedaddle.apply_receipts_v3 (
 stream text NOT NULL, mseq numeric NOT NULL, txn bytea NOT NULL,
 position_hash bytea NOT NULL CHECK (octet_length(position_hash) = 32),
 position_domain text NOT NULL, position bytea NOT NULL,
 digest bytea NOT NULL CHECK (octet_length(digest) = 32),
 record_kind smallint, manifest_digest bytea, previous_manifest_digest bytea,
 PRIMARY KEY (stream, mseq, position_hash));";

pub async fn install_opaque_progress<F, Fut>(client: &mut Client, journal_intent: F) -> Result<()>
where
    F: FnOnce(&'static str) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    journal_intent(INSTALL_OPAQUE_INTENT).await?;
    let tx = client.transaction().await?;
    tx.query_one(
        "SELECT pg_catalog.pg_advisory_xact_lock(1936418148, 49)",
        &[],
    )
    .await?;
    let rows = tx
        .query(crate::pg::ddl::ADMIN_INSTALLATION_SQL, &[])
        .await?;
    let installations = rows
        .iter()
        .map(|row| vec![row.get::<_, String>(0)])
        .collect();
    let origin = crate::pg::ddl::administrative_origin_sql(&installations)
        .map_err(|_| ApplyError::SchemaMismatch)?;
    // Reject malformed existing tables before CREATE can fail with an untyped SQL error.
    for table in ["apply_progress_v3", "apply_receipts_v3"] {
        if exists(&tx, table).await? {
            validate_table(&tx, table).await?;
        }
    }
    tx.batch_execute(&format!("{origin}{OPAQUE_DDL}")).await?;
    validate(&tx).await?;
    tx.commit().await?;
    Ok(())
}
async fn exists(client: &impl GenericClient, table: &str) -> Result<bool> {
    Ok(client
        .query_one(
            "SELECT pg_catalog.to_regclass($1) IS NOT NULL",
            &[&format!("_skedaddle.{table}")],
        )
        .await?
        .get(0))
}
pub(super) async fn validate(client: &impl GenericClient) -> Result<()> {
    for table in ["apply_progress_v3", "apply_receipts_v3"] {
        validate_table(client, table).await?;
    }
    Ok(())
}
async fn validate_table(client: &impl GenericClient, table: &str) -> Result<()> {
    let progress = table == "apply_progress_v3";
    let mut expected = vec![
        ("stream", "text", true),
        ("position_domain", "text", true),
        ("record_kind", "smallint", false),
        ("manifest_digest", "bytea", false),
        ("previous_manifest_digest", "bytea", false),
    ];
    if progress {
        expected.extend([
            ("last_mseq", "numeric", true),
            ("last_txn", "bytea", true),
            ("last_position", "bytea", true),
            ("last_digest", "bytea", true),
        ]);
    } else {
        expected.extend([
            ("mseq", "numeric", true),
            ("txn", "bytea", true),
            ("position", "bytea", true),
            ("digest", "bytea", true),
            ("position_hash", "bytea", true),
        ]);
    }
    expected.sort_unstable();
    let name = format!("_skedaddle.{table}");
    let rows = client.query("SELECT a.attname::text, pg_catalog.format_type(a.atttypid,a.atttypmod), a.attnotnull FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid=a.attrelid WHERE a.attrelid=pg_catalog.to_regclass($1) AND c.relkind='r' AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attname", &[&name]).await?;
    let actual: Vec<(String, String, bool)> = rows
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    if actual
        .iter()
        .map(|(a, b, c)| (a.as_str(), b.as_str(), *c))
        .collect::<Vec<_>>()
        != expected
    {
        return Err(ApplyError::SchemaMismatch.into());
    }
    // Pin PK and checks too: a width-limited position or wrong key is not v3.
    let rows = client.query("SELECT pg_catalog.pg_get_constraintdef(oid) FROM pg_catalog.pg_constraint WHERE conrelid=pg_catalog.to_regclass($1) AND contype IN ('p','c','u','f','x') ORDER BY 1", &[&name]).await?;
    let actual: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    let mut expected = if progress {
        vec!["PRIMARY KEY (stream)"]
    } else {
        vec![
            "PRIMARY KEY (stream, mseq, position_hash)",
            "CHECK ((octet_length(digest) = 32))",
            "CHECK ((octet_length(position_hash) = 32))",
        ]
    };
    expected.sort_unstable();
    if actual.iter().map(String::as_str).collect::<Vec<_>>() != expected {
        return Err(ApplyError::SchemaMismatch.into());
    }
    Ok(())
}
pub(super) async fn check_mode(client: &Client, stream: &str, policy: OriginPolicy) -> Result<()> {
    let tables = match policy {
        OriginPolicy::Opaque => ["apply_progress", "apply_receipts"],
        OriginPolicy::NativePgLsn => ["apply_progress_v3", "apply_receipts_v3"],
    };
    for table in tables {
        if exists(client, table).await?
            && client
                .query_opt(
                    &format!("SELECT 1 FROM _skedaddle.{table} WHERE stream=$1 LIMIT 1"),
                    &[&stream],
                )
                .await?
                .is_some()
        {
            return Err(ApplyError::OriginPolicyMismatch.into());
        }
    }
    Ok(())
}
