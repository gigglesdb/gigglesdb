//! Opt-in identity binding checks against real database catalogs.
use nunya::{
    policy::{
        Policy, PolicyDocument, PolicyVersion, ReadLogSink, RouteDefault, Treatment,
        bindings::{self, BindingStatus, RouteBindings},
        effective::{Effective, Origin, effective},
    },
    schema::{
        ColumnRef, Snapshot,
        pg::{PgSchemaOptions, load_pg_snapshot},
        spanner_gsql::load_gsql_snapshot,
    },
    spanner::{catalog::CatalogRunner, proxy::SpannerUpstream},
    state::{model::RouteId, store::RedbStore},
    token::keys::RootKey,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
struct Temp(PathBuf);
impl Temp {
    fn new() -> Result<Self> {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce)?;
        let path = std::env::temp_dir().join(format!(
            "bindings-live-{}",
            data_encoding::HEXLOWER.encode(&nonce)
        ));
        std::fs::create_dir_all(&path)?;
        Ok(Self(path))
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn document() -> PolicyDocument {
    PolicyDocument {
        file_readlog: None,
        file_snapshot_max_age_ms: None,
        policy: Policy {
            retired_domains: Default::default(),
            json_filters: Default::default(),
            columns: BTreeMap::new(),
            domains: BTreeMap::new(),
            snapshot_max_age_ms: 60000,
            readlog: ReadLogSink::Stdout,
        },
        version: PolicyVersion::V2,
        route_default: RouteDefault::Tokenize,
        json_paths: vec![],
        links: vec![],
    }
}
fn column(schema: &str, table: &str, name: &str) -> ColumnRef {
    ColumnRef {
        schema: schema.into(),
        table: table.into(),
        column: name.into(),
    }
}
fn repair(
    doc: &PolicyDocument,
    snapshot: &Snapshot,
    records: &RouteBindings,
    c: &ColumnRef,
) -> Result<()> {
    assert_eq!(
        effective(doc, snapshot, records, c)?,
        (Effective::Masked { placeholder: None }, Origin::NeedsRepair)
    );
    Ok(())
}
fn unique(records: &RouteBindings) {
    let names: Vec<_> = records
        .iter()
        .filter_map(|(_, r)| r.domain.as_ref())
        .collect();
    assert_eq!(names.len(), names.iter().collect::<BTreeSet<_>>().len());
}
#[tokio::test]
#[ignore = "requires NUNYA_TEST_BINDINGS_PG"]
async fn pg_bindings_follow_identity() -> Result<()> {
    let (client, connection) = tokio_postgres::connect(
        &std::env::var("NUNYA_TEST_BINDINGS_PG")?,
        tokio_postgres::NoTls,
    )
    .await?;
    let driver = tokio::spawn(async move {
        let _ = connection.await;
    });
    let temp = Temp::new()?;
    let schema = temp
        .0
        .file_name()
        .ok_or("missing nonce")?
        .to_string_lossy()
        .replace('-', "_");
    client.batch_execute(&format!("CREATE SCHEMA {schema}; CREATE TABLE {schema}.customers(id int8, email text, note text)")).await?;
    let result: Result<()> = async {
        let root = RootKey::from_hex(&"01".repeat(32))?;
        let store = RedbStore::open(&temp.0, &root)?;
        let route = RouteId::new("bindings-live").ok_or("invalid route")?;
        let mut doc = document();
        let opts = PgSchemaOptions { schemas: vec![schema.clone()], search_path: vec![schema.clone()] };
        let c = |name: &str| column(&schema, "customers", name);
        let initial = load_pg_snapshot(&client, &opts, 1).await?;
        let first = bindings::refresh(&store, &route, &doc, &initial)?;
        assert_eq!(first.iter().count(), 3);
        assert!(first.iter().all(|(_, r)| r.status == BindingStatus::Active && r.domain.is_some()));
        let email_id = initial.column_id(&c("email")).ok_or("email identity")?.clone();
        let note_id = initial.column_id(&c("note")).ok_or("note identity")?.clone();
        let id = initial.column_id(&c("id")).ok_or("id identity")?.clone();
        let email_domain = first.get(&email_id).ok_or("email binding")?.domain.clone().ok_or("email domain")?;
        let id_domain = first.get(&id).ok_or("id binding")?.domain.clone();
        // Separate route proves the same rename with an anchored explicit Show.
        let show_route = RouteId::new("bindings-show").ok_or("invalid route")?;
        doc.policy.columns.insert(c("email"), Treatment::Show);
        bindings::refresh(&store, &show_route, &doc, &initial)?;
        client.batch_execute(&format!("ALTER TABLE {schema}.customers RENAME COLUMN email TO contact_email")).await?;
        let renamed = load_pg_snapshot(&client, &opts, 2).await?;
        let show = bindings::refresh(&store, &show_route, &doc, &renamed)?;
        assert_eq!(effective(&doc, &renamed, &show, &c("contact_email"))?, (Effective::Show, Origin::Explicit));
        doc.policy.columns.clear();
        let records = bindings::refresh(&store, &route, &doc, &renamed)?;
        assert_eq!(renamed.column_id(&c("contact_email")), Some(&email_id));
        assert_eq!(effective(&doc, &renamed, &records, &c("contact_email"))?, (Effective::BlindPrivate { domain: email_domain }, Origin::RouteDefault));
        client.batch_execute(&format!("ALTER TABLE {schema}.customers DROP COLUMN note; ALTER TABLE {schema}.customers ADD COLUMN note text")).await?;
        let recreated = load_pg_snapshot(&client, &opts, 3).await?;
        let records = bindings::refresh(&store, &route, &doc, &recreated)?;
        repair(&doc, &recreated, &records, &c("note"))?;
        assert_eq!(records.get(&note_id).ok_or("old note")?.status, BindingStatus::Retired);
        assert!(records.get(recreated.column_id(&c("note")).ok_or("new note id")?).ok_or("new note")?.domain.is_none());
        client.batch_execute(&format!("ALTER TABLE {schema}.customers ALTER COLUMN id TYPE text USING id::text")).await?;
        let changed = load_pg_snapshot(&client, &opts, 4).await?;
        assert_eq!(changed.column_id(&c("id")), Some(&id), "PostgreSQL keeps attnum");
        let records = bindings::refresh(&store, &route, &doc, &changed)?;
        repair(&doc, &changed, &records, &c("id"))?;
        assert_eq!(records.get(&id).ok_or("changed id")?.domain, id_domain);
        unique(&records);
        drop(store);
        let store = RedbStore::open(&temp.0, &root)?;
        assert_eq!(bindings::load(&store, &route)?, records);
        assert_eq!(bindings::refresh(&store, &route, &doc, &changed)?, records);
        Ok(())
    }.await;
    let cleanup = client
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await;
    driver.abort();
    result?;
    cleanup?;
    Ok(())
}
fn ddl(rest: &str, database: &str, statements: &[&str]) -> Result<()> {
    let body = serde_json::json!({"statements": statements}).to_string();
    let output = std::process::Command::new("curl.exe")
        .args([
            "--silent",
            "--show-error",
            "--fail-with-body",
            "-X",
            "PATCH",
            "-H",
            "Content-Type: application/json",
            "--data-binary",
            &body,
            &format!("http://{rest}/v1/{database}/ddl"),
        ])
        .output()?;
    if !output.status.success() {
        return Err("emulator DDL request failed".into());
    }
    let response: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    if response.get("error").is_some() {
        return Err("emulator DDL operation failed".into());
    }
    Ok(())
}
#[tokio::test]
#[ignore = "requires NUNYA_TEST_BINDINGS_SPANNER_EMULATOR, NUNYA_TEST_BINDINGS_SPANNER_REST and NUNYA_TEST_BINDINGS_SPANNER_DB"]
async fn spanner_ambiguous_changes_fail_closed() -> Result<()> {
    let endpoint = std::env::var("NUNYA_TEST_BINDINGS_SPANNER_EMULATOR")?;
    let rest = std::env::var("NUNYA_TEST_BINDINGS_SPANNER_REST")?;
    let database = std::env::var("NUNYA_TEST_BINDINGS_SPANNER_DB")?;
    let temp = Temp::new()?;
    let table = temp
        .0
        .file_name()
        .ok_or("missing nonce")?
        .to_string_lossy()
        .replace('-', "_");
    ddl(
        &rest,
        &database,
        &[&format!(
            "CREATE TABLE {table} (Id INT64 NOT NULL, A STRING(MAX), C STRING(MAX)) PRIMARY KEY (Id)"
        )],
    )?;
    let result: Result<()> = async {
        let runner = CatalogRunner::new(
            &SpannerUpstream::Emulator {
                endpoint: format!("http://{endpoint}"),
            },
            database.clone(),
        )
        .await?;
        let store = RedbStore::open(&temp.0, &RootKey::from_hex(&"01".repeat(32))?)?;
        let route = RouteId::new("spanner-bindings").ok_or("invalid route")?;
        let doc = document();
        let c = |name: &str| column("", &table, name);
        let snapshot = load_gsql_snapshot(&runner, 1).await?;
        let records = bindings::refresh(&store, &route, &doc, &snapshot)?;
        for name in ["Id", "A", "C"] {
            let r = records
                .get(snapshot.column_id(&c(name)).ok_or("initial identity")?)
                .ok_or("initial binding")?;
            assert_eq!(r.status, BindingStatus::Active);
            assert!(r.domain.is_some());
        }
        ddl(
            &rest,
            &database,
            &[
                &format!("ALTER TABLE {table} DROP COLUMN A"),
                &format!("ALTER TABLE {table} ADD COLUMN A INT64"),
            ],
        )?;
        let snapshot = load_gsql_snapshot(&runner, 2).await?;
        let records = bindings::refresh(&store, &route, &doc, &snapshot)?;
        repair(&doc, &snapshot, &records, &c("A"))?;
        ddl(
            &rest,
            &database,
            &[
                &format!("ALTER TABLE {table} DROP COLUMN C"),
                &format!("ALTER TABLE {table} ADD COLUMN D STRING(MAX)"),
            ],
        )?;
        let snapshot = load_gsql_snapshot(&runner, 3).await?;
        let records = bindings::refresh(&store, &route, &doc, &snapshot)?;
        repair(&doc, &snapshot, &records, &c("D"))?;
        ddl(
            &rest,
            &database,
            &[&format!("ALTER TABLE {table} ADD COLUMN E STRING(MAX)")],
        )?;
        let snapshot = load_gsql_snapshot(&runner, 4).await?;
        let records = bindings::refresh(&store, &route, &doc, &snapshot)?;
        let r = records
            .get(snapshot.column_id(&c("E")).ok_or("added identity")?)
            .ok_or("added binding")?;
        assert_eq!(r.status, BindingStatus::Active);
        assert!(r.domain.is_some());
        unique(&records);
        Ok(())
    }
    .await;
    let cleanup = ddl(&rest, &database, &[&format!("DROP TABLE {table}")]);
    result?;
    cleanup
}
