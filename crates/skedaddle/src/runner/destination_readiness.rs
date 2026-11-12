//! Read-only activation prerequisites, independent of row verification.
use super::{fence_readiness::Request, readiness::Fact};
use crate::{coordinator::Materialized, status::Blocker};
use std::time::{Duration, Instant};
use tokio_postgres::{Client, SimpleQueryMessage};

#[derive(Clone, Debug)]
pub(super) struct Checks {
    pub expires_at_ms: Option<u64>,
    pub validity: Fact,
    pub sequences: Fact,
    pub reverse: Fact,
}
fn fact(code: &str, source: &str, gaps: Vec<String>, next: &str) -> Fact {
    Fact {
        ready: gaps.is_empty(),
        source: source.into(),
        staleness: "Read-only probes expire 1 second from start; owner/epoch/definition/scope changes and owner exit revoke them.".into(),
        blocker: (!gaps.is_empty()).then(|| Blocker {
            code: code.into(), reason: gaps.join("; "), remediation: next.into(),
        }),
    }
}
impl Checks {
    pub fn missing() -> Self {
        Self {
            expires_at_ms: None,
            validity: fact("destination-invalid", "Fresh destination pg_index.indisvalid/indisready and pg_constraint.convalidated; authenticated current V1 schema and destination_foreign_keys sections.", vec!["Fresh destination catalog probe unavailable; current V1 schema/FK integrity authority is also required".into()], "Restore destination access and refresh catalog probes; run complete verification for current schema/FK integrity."),
            sequences: fact("sequences-not-syncable", "Fresh source/destination pg_sequence, owned/default dependency, identity/default binding, schema USAGE and sequence privileges; actual P6c discovery contract.", vec!["Fresh two-endpoint sequence capability probe unavailable".into()], "Restore both endpoint connections and sequence SELECT on source/destination plus UPDATE on destination; resolve mappings and refresh."),
            reverse: fact("reverse-prerequisites-missing", "Fresh destination logical WAL, replication/publication/DDL setup capability, slot capacity and old-source reverse receiving/installability privileges. The reverse slot is created inside the fenced window; activation requires journaled consistent-point proof under the retained destination fence. The activation proof enforcement contract remains unimplemented.", vec!["Fresh reverse capability probes unavailable".into()], "Restore reverse setup capability and refresh. Create the reverse slot inside the fenced window; activation requires journaled P11a proof before releasing the fence (enforcement contract remains unimplemented)."),
        }
    }
}
pub(super) struct Observation {
    owner: String,
    started: Instant,
    epoch: u64,
    generation: u64,
    digest: String,
    plans: Vec<(String, String)>,
    checks: Checks,
}
impl Observation {
    fn new(request: Request<'_>, mut checks: Checks) -> Option<Self> {
        if request.scope.is_empty() {
            return None;
        }
        let now_ms: u64 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()?
            .as_millis()
            .try_into()
            .ok()?;
        let elapsed_ms: u64 = request.started.elapsed().as_millis().try_into().ok()?;
        checks.expires_at_ms = Some(now_ms.checked_sub(elapsed_ms)?.checked_add(999)?);
        Some(Self {
            owner: request.owner,
            started: request.started,
            epoch: request.state.epoch,
            generation: request.state.verification.generation,
            digest: request.state.definition.as_ref()?.canonical_digest().ok()?,
            plans: request
                .state
                .chunk_plans
                .iter()
                .map(|(k, v)| (k.clone(), v.digest.clone()))
                .collect(),
            checks,
        })
    }
    pub fn checks(
        &self,
        state: &Materialized,
        owner: Option<&str>,
        running: bool,
    ) -> Option<Checks> {
        (running
            && owner == Some(self.owner.as_str())
            && self.started.elapsed() <= Duration::from_secs(1)
            && state.epoch == self.epoch
            && state.verification.generation == self.generation
            && state.definition.as_ref()?.canonical_digest().ok()? == self.digest
            && state
                .chunk_plans
                .iter()
                .map(|(k, v)| (k.clone(), v.digest.clone()))
                .collect::<Vec<_>>()
                == self.plans)
            .then(|| self.checks.clone())
    }
}
pub(super) async fn rows(client: Option<&Client>, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let client = client.ok_or(
        "endpoint connection unavailable; restore verified TLS endpoint access and refresh",
    )?;
    client.simple_query(sql).await.map_err(|_| "read-only catalog query failed (endpoint privilege, missing object or unavailable connection); restore catalog access and refresh".into()).map(|messages| messages.into_iter().filter_map(|m| match m {
        SimpleQueryMessage::Row(r) => Some((0..r.len()).map(|i| r.get(i).unwrap_or("").into()).collect()), _ => None,
    }).collect())
}
fn validity(rows: &[Vec<String>]) -> Result<Vec<String>, String> {
    rows.iter().map(|row| {
        if row.len()!=4 || !matches!(row[2].as_str(), "true"|"false") || !matches!(row[3].as_str(), "true"|"false") { return Err("malformed validity probe".into()); }
        Ok((row[2]!="true" || row[3]!="true").then(|| format!("{} {} invalid/not ready or NOT VALID; next: rebuild the index or validate the constraint",row[0],row[1])))
    }).collect::<Result<Vec<_>,_>>().map(|v| v.into_iter().flatten().collect())
}
fn sequence_sql(scope: &str) -> String {
    format!(
        "WITH scoped AS (SELECT d.objid oid FROM pg_depend d WHERE d.classid='pg_class'::regclass AND d.refclassid='pg_class'::regclass AND d.deptype IN ('a','i') AND d.refobjid IN ({scope}) UNION SELECT d.refobjid FROM pg_attrdef a JOIN pg_depend d ON d.classid='pg_attrdef'::regclass AND d.objid=a.oid AND d.refclassid='pg_class'::regclass WHERE a.adrelid IN ({scope})) SELECT format('%I.%I',n.nspname,c.relname),s.seqtypid::text,s.seqincrement::text,s.seqmin::text,s.seqmax::text,s.seqcycle::text,COALESCE((SELECT format('%I.%I.%I:%s',tn.nspname,t.relname,a.attname,a.attidentity) FROM pg_depend d JOIN pg_class t ON t.oid=d.refobjid JOIN pg_namespace tn ON tn.oid=t.relnamespace JOIN pg_attribute a ON a.attrelid=t.oid AND a.attnum=d.refobjsubid WHERE d.classid='pg_class'::regclass AND d.objid=c.oid AND d.refclassid='pg_class'::regclass AND d.deptype IN ('a','i') AND d.refobjid IN ({scope})),''),(has_schema_privilege(n.oid,'USAGE') AND has_sequence_privilege(c.oid,'SELECT'))::text,(has_schema_privilege(n.oid,'USAGE') AND has_sequence_privilege(c.oid,'UPDATE'))::text,COALESCE((SELECT string_agg(format('%I.%I.%I:%s',dn.nspname,dt.relname,da.attname,pg_get_expr(ad.adbin,ad.adrelid)),',' ORDER BY dn.nspname,dt.relname,da.attname) FROM pg_attrdef ad JOIN pg_depend dep ON dep.classid='pg_attrdef'::regclass AND dep.objid=ad.oid AND dep.refclassid='pg_class'::regclass AND dep.refobjid=c.oid JOIN pg_class dt ON dt.oid=ad.adrelid JOIN pg_namespace dn ON dn.oid=dt.relnamespace JOIN pg_attribute da ON da.attrelid=ad.adrelid AND da.attnum=ad.adnum WHERE ad.adrelid IN ({scope})),'') FROM scoped JOIN pg_class c ON c.oid=scoped.oid AND c.relkind='S' JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_sequence s ON s.seqrelid=c.oid ORDER BY 1"
    )
}
fn sequences(source: &[Vec<String>], destination: &[Vec<String>]) -> Result<Vec<String>, String> {
    let mut gaps = vec![];
    for row in source.iter().chain(destination) {
        if row.len() != 10
            || row[1..5].iter().any(|v| v.parse::<i64>().is_err())
            || !matches!(row[5].as_str(), "true" | "false")
            || row[7..9]
                .iter()
                .any(|v| !matches!(v.as_str(), "true" | "false"))
        {
            return Err("malformed sequence capability probe".into());
        }
    }
    for src in source {
        let Some(dst) = destination.iter().find(|d| d[0] == src[0]) else {
            gaps.push(format!(
                "{} destination sequence missing; next: create compatible mapping",
                src[0]
            ));
            continue;
        };
        if src[..7] != dst[..7] || src[9] != dst[9] {
            gaps.push(format!("{} type/range/increment/cycle/identity/default dependency mapping differs; next: align P6c sequence definitions",src[0]));
        }
        if src[7] != "true" || dst[7] != "true" || dst[8] != "true" {
            gaps.push(format!("{} sequence privileges insufficient: source schema-USAGE/SELECT={}, destination schema-USAGE/SELECT={}, schema-USAGE/UPDATE={}; next: grant required sequence access",src[0],src[7],dst[7],dst[8]));
        }
    }
    for dst in destination {
        if !source.iter().any(|s| s[0] == dst[0]) {
            gaps.push(format!(
                "{} extra destination sequence; next: align selected sequence mappings",
                dst[0]
            ));
        }
    }
    Ok(gaps)
}
pub(super) fn flags(rows: &[Vec<String>]) -> Result<Vec<String>, String> {
    rows.iter().map(|r| {
        if r.len()!=2 || !matches!(r[1].as_str(),"true"|"false") { return Err("malformed reverse prerequisite probe".into()); }
        Ok((r[1]!="true").then(|| format!("{} missing; next: complete journaled P11a setup with qualified reverse endpoint roles",r[0])))
    }).collect::<Result<Vec<_>,_>>().map(|r| r.into_iter().flatten().collect())
}
fn literal(value: &str) -> String {
    format!("E'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}
pub(super) async fn refresh(
    retained: &std::sync::Mutex<Option<Observation>>,
    source: Option<&Client>,
    destination: Option<&Client>,
    id: &str,
    request: Request<'_>,
) {
    *retained.lock().expect("destination readiness poisoned") = None;
    let mut checks = Checks::missing();
    let scope = request
        .scope
        .iter()
        .map(|s| format!("{}::regclass", literal(s)))
        .collect::<Vec<_>>()
        .join(",");
    if scope.is_empty() {
        return;
    }
    let sql = format!(
        "SELECT 'index',indexrelid::regclass::text,indisvalid::text,indisready::text FROM pg_index WHERE indrelid IN ({scope}) UNION ALL SELECT 'constraint',conname,convalidated::text,'true' FROM pg_constraint WHERE conrelid IN ({scope})"
    );
    let gaps = match rows(destination, &sql).await.and_then(|r| validity(&r)) {
        Ok(g) => g,
        Err(e) => vec![e],
    };
    checks.validity = fact(
        "destination-invalid",
        &checks.validity.source,
        gaps,
        "Rebuild invalid/unready indexes, validate all constraints, and refresh; authenticated current V1 schema and destination FK integrity sections must also be clean.",
    );
    let src = rows(source, &sequence_sql(&scope)).await;
    let dst = rows(destination, &sequence_sql(&scope)).await;
    let gaps = match (src, dst) {
        (Ok(s), Ok(d)) => sequences(&s, &d).unwrap_or_else(|e| vec![e]),
        (s, d) => s
            .err()
            .into_iter()
            .map(|e| format!("source: {e}"))
            .chain(d.err().map(|e| format!("destination: {e}")))
            .collect(),
    };
    checks.sequences = fact(
        "sequences-not-syncable",
        &checks.sequences.source,
        gaps,
        "Align source/destination owned/default-dependent sequence type/range/configuration and identity/default dependencies; grant schema USAGE, source/destination SELECT and destination UPDATE. This probe never calls setval/nextval.",
    );
    let reverse = super::reverse_capabilities::probe(source, destination, id, &scope).await;
    checks.reverse = fact(
        "reverse-prerequisites-missing",
        &checks.reverse.source,
        reverse,
        "Restore the listed reverse setup capabilities and refresh. The reverse slot is created inside the fenced window; activation requires journaled P11a consistent-point proof under the retained destination fence before application writes. The activation proof enforcement contract remains unimplemented.",
    );
    *retained.lock().expect("destination readiness poisoned") = Observation::new(request, checks);
}

#[cfg(test)]
mod tests {
    use super::*;
    fn row(values: &[&str]) -> Vec<String> {
        values.iter().map(|v| (*v).into()).collect()
    }
    fn state() -> Materialized {
        Materialized {
            definition: Some(
                crate::MigrationDefinition::from_json(
                    include_str!("../../README.md")
                        .split("```json")
                        .nth(1)
                        .unwrap()
                        .split("```")
                        .next()
                        .unwrap(),
                )
                .unwrap(),
            ),
            ..Default::default()
        }
    }
    #[test]
    fn catalog_validity_checks_both_index_flags_and_every_constraint() {
        assert!(
            validity(&[
                row(&["index", "s.i", "true", "true"]),
                row(&["constraint", "fk", "true", "true"])
            ])
            .unwrap()
            .is_empty()
        );
        let gaps = validity(&[
            row(&["index", "invalid", "false", "true"]),
            row(&["index", "unready", "true", "false"]),
            row(&["constraint", "not_valid", "false", "true"]),
        ])
        .unwrap();
        for name in ["invalid", "unready", "not_valid"] {
            assert!(gaps.iter().any(|g| g.contains(name) && g.contains("next:")));
        }
        assert!(validity(&[row(&["index", "missing column"])]).is_err());
    }
    #[test]
    fn sequence_rows_cover_type_range_identity_default_only_and_privileges() {
        let good = row(&[
            "s.seq",
            "23",
            "1",
            "1",
            "2147483647",
            "false",
            "s.t.id:a",
            "true",
            "true",
            "",
        ]);
        assert!(
            sequences(std::slice::from_ref(&good), std::slice::from_ref(&good))
                .unwrap()
                .is_empty()
        );
        for column in [1, 2, 3, 4, 5, 6, 7, 8, 9] {
            let mut bad = good.clone();
            bad[column] = match column {
                1 => "20",
                2 => "2",
                3 => "0",
                4 => "999",
                5 => "true",
                6 => "s.t.id:d",
                9 => "s.other.id:nextval(other)",
                _ => "false",
            }
            .into();
            assert!(
                !sequences(std::slice::from_ref(&good), &[bad])
                    .unwrap()
                    .is_empty(),
                "column {column}"
            );
        }
        let mut default = good.clone();
        default[6].clear();
        default[9] = "s.t.id:nextval(s.seq)".into();
        assert!(
            sequences(
                std::slice::from_ref(&default),
                std::slice::from_ref(&default)
            )
            .unwrap()
            .is_empty()
        );
        assert!(
            !sequences(std::slice::from_ref(&good), &[])
                .unwrap()
                .is_empty()
        );
        assert!(!sequences(&[], &[good]).unwrap().is_empty());
    }
    #[test]
    fn reverse_rows_accept_capabilities_without_installation_or_consistent_point() {
        let names = [
            "wal_level logical",
            "replication role",
            "publication setup capability",
            "slot capacity",
            "DDL setup capability",
            "source receiving objects installable",
        ];
        let rows = names
            .iter()
            .map(|name| row(&[name, "true"]))
            .collect::<Vec<_>>();
        assert!(flags(&rows).unwrap().is_empty());
        let capability = fact(
            "reverse-prerequisites-missing",
            &Checks::missing().reverse.source,
            flags(&rows).unwrap(),
            "Restore reverse setup capability",
        );
        assert!(capability.ready);
        assert!(
            capability
                .source
                .contains("slot is created inside the fenced window")
        );
        assert!(
            capability
                .source
                .contains("enforcement contract remains unimplemented")
        );
        let rows = names
            .iter()
            .map(|name| row(&[name, "false"]))
            .collect::<Vec<_>>();
        assert_eq!(flags(&rows).unwrap().len(), names.len());
        assert!(flags(&[row(&["unknown", "null"])]).is_err());
        assert!(!Checks::missing().reverse.ready);
    }
    #[test]
    fn observation_binding_expiry_and_stopped_owner_revoke_all_facts() {
        let mut state = state();
        let mut observed = Observation::new(
            Request {
                state: &state,
                owner: "owner".into(),
                scope: vec!["s.t".into()],
                started: Instant::now(),
            },
            Checks::missing(),
        )
        .unwrap();
        assert!(observed.checks(&state, Some("owner"), true).is_some());
        assert!(observed.checks(&state, Some("other"), true).is_none());
        assert!(observed.checks(&state, Some("owner"), false).is_none());
        state.epoch += 1;
        assert!(observed.checks(&state, Some("owner"), true).is_none());
        state.epoch -= 1;
        state.verification.generation += 1;
        assert!(observed.checks(&state, Some("owner"), true).is_none());
        state.verification.generation -= 1;
        observed.plans.push(("other".into(), "plan".into()));
        assert!(observed.checks(&state, Some("owner"), true).is_none());
        observed.plans.clear();
        observed.started = Instant::now() - Duration::from_millis(1001);
        assert!(observed.checks(&state, Some("owner"), true).is_none());
    }
    #[tokio::test]
    async fn live_invalid_index_not_valid_sequence_type_and_reverse_privilege() {
        let Some(path) = std::env::var_os("SKEDADDLE_TEST_CONFIG") else {
            use std::io::Write;
            writeln!(std::io::stderr(),"LOUD SKIP R5b readiness conditions6/7/8: SKEDADDLE_TEST_CONFIG absent; NOT VALID, failed concurrent index, two-endpoint sequence mismatch and reverse privilege PostgreSQL qualification NOT RUN; no Docker started").unwrap();
            return;
        };
        let config: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let connect = |key: &str| {
            crate::pg::tls::Config::new(
                config[key]
                    .as_str()
                    .expect("harness endpoint admin DSN")
                    .parse()
                    .unwrap(),
                config["ca_pem"].as_str().expect("harness CA"),
            )
        };
        let (source, source_driver, _) =
            crate::pg::tls::connect(&connect("pg_fence"), &crate::security::TlsPolicy::Required)
                .await
                .unwrap();
        let source_driver = tokio::spawn(source_driver);
        let (destination, destination_driver, _) = crate::pg::tls::connect(
            &connect("pg_activate"),
            &crate::security::TlsPolicy::Required,
        )
        .await
        .unwrap();
        let destination_driver = tokio::spawn(destination_driver);
        let schema = format!("r5b_{}", uuid::Uuid::new_v4().simple());
        let role = format!("{schema}_role");
        let state = state();
        let retained = std::sync::Mutex::new(None);
        let result=async {
            let setup=format!("CREATE SCHEMA {schema}; CREATE TABLE {schema}.t(id integer GENERATED ALWAYS AS IDENTITY, v integer); INSERT INTO {schema}.t(v) VALUES(1),(1);");
            source.batch_execute(&setup).await.map_err(|e|e.to_string())?;
            destination.batch_execute(&format!("{setup} ALTER TABLE {schema}.t ADD CONSTRAINT unvalidated CHECK(v>0) NOT VALID; ALTER SEQUENCE {schema}.t_id_seq AS bigint; CREATE ROLE {role} NOLOGIN NOREPLICATION;")).await.map_err(|e|e.to_string())?;
            if destination.batch_execute(&format!("CREATE UNIQUE INDEX CONCURRENTLY bad_index ON {schema}.t(v)")).await.is_ok() { return Err("duplicate rows must leave an invalid concurrent index".into()); }
            async fn probe(retained: &std::sync::Mutex<Option<Observation>>, source: &Client, destination: &Client, state: &Materialized, schema: &str) -> Result<Checks,String> {
                refresh(retained,Some(source),Some(destination),"r5b",Request {state,owner:"owner".into(),scope:vec![format!("{schema}.t")],started:Instant::now()}).await;
                retained.lock().unwrap().as_ref().and_then(|v|v.checks(state,Some("owner"),true)).ok_or_else(||"fresh production observation unavailable/expired".into())
            }
            let checks=probe(&retained,&source,&destination,&state,&schema).await?;
            if !checks.reverse.ready { return Err(format!("capable endpoints without installed reverse objects must pass condition 8: {:?}",checks.reverse.blocker)); }
            let gaps=checks.validity.blocker.as_ref().ok_or("invalid catalog objects passed readiness")?;
            if !gaps.reason.contains("bad_index") || !gaps.reason.contains("unvalidated") {return Err("invalid index/NOT VALID constraint not diagnosed through refresh".into());}
            if checks.sequences.ready || !checks.sequences.blocker.as_ref().is_some_and(|b|b.reason.contains("type/range")) {return Err("two-endpoint sequence type mismatch not diagnosed through refresh".into());}
            destination.batch_execute(&format!("SET ROLE {role}")).await.map_err(|e|e.to_string())?;
            let checks=probe(&retained,&source,&destination,&state,&schema).await?;
            if checks.reverse.ready || !checks.reverse.blocker.as_ref().is_some_and(|b|b.reason.contains("destination reverse replication role missing")) {return Err("missing reverse privilege not diagnosed through refresh".into());}
            Ok::<(),String>(())
        }.await;
        // Teardown happens for partial setup and every returned test failure.
        let destination_cleanup = destination
            .batch_execute(&format!(
                "RESET ROLE; DROP SCHEMA IF EXISTS {schema} CASCADE; DROP ROLE IF EXISTS {role}"
            ))
            .await;
        let source_cleanup = source
            .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .await;
        drop(source);
        drop(destination);
        source_driver.await.unwrap().unwrap();
        destination_driver.await.unwrap().unwrap();
        destination_cleanup.unwrap();
        source_cleanup.unwrap();
        result.unwrap();
    }
}
