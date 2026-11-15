//! Real PostgreSQL lane; never starts Docker or other infrastructure. The
//! existing harness supplies SKEDADDLE_TEST_CONFIG with `pg_chunk` or `pg` DSN,
//! logical WAL, slot/publication privileges, and SKEDADDLE_TEST_ROOT under J:.
use futures::FutureExt;
use skedaddle::{
    buffer::format::SegmentRecord,
    event::{Event, EventKind, OpaqueValue, Position, Row, SourceIdentity, TxnId},
    mapping::{PgType, Value},
    pg::{
        chunk::{
            ChunkRange, ChunkTransition, Direction, Key, KeyColumn, QualifiedName, TableKey,
            Watermark, WatermarkKind, Window, plan_chunks, unfinished_chunks,
        },
        pgoutput::{Column, Decoder, Message, OldTuple, Tuple},
    },
};
use std::{
    error::Error,
    io::Write,
    panic::AssertUnwindSafe,
    path::{Path, PathBuf},
};
use tokio_postgres::Client;

#[derive(Debug)]
struct UuidBytes([u8; 16]);
impl<'a> tokio_postgres::types::FromSql<'a> for UuidBytes {
    fn from_sql(_: &tokio_postgres::types::Type, raw: &'a [u8]) -> TestResult<Self> {
        Ok(Self(raw.try_into()?))
    }
    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        *ty == tokio_postgres::types::Type::UUID
    }
}

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

fn skip(test: &str, reason: &str) {
    // Direct stderr remains visible under libtest capture in plain cargo test.
    let _ = writeln!(
        std::io::stderr().lock(),
        "SKIP {test}: {reason}; real PostgreSQL qualification NOT RUN"
    );
}
fn durable_write(path: &Path, bytes: &[u8]) -> TestResult {
    let mut file = std::fs::File::create(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn qualified(schema: &str, name: &str) -> QualifiedName {
    QualifiedName {
        schema: schema.into(),
        name: name.into(),
    }
}
fn column(name: &str, pg_type: PgType, sql_type: &str, direction: Direction) -> KeyColumn {
    KeyColumn {
        name: name.into(),
        pg_type,
        sql_type: qualified("pg_catalog", sql_type),
        collation: None,
        direction,
    }
}

#[test]
fn selected_chunk_query_excludes_columns_and_requires_keys() {
    let key = TableKey {
        table: "public.rows".into(),
        relation: qualified("public", "rows"),
        columns: vec![column("id", PgType::Int4, "int4", Direction::Asc)],
    };
    let range = plan_chunks(&key, 10, &[]).unwrap().remove(0);
    let sql = key
        .read_query_columns(&range, &["id".into(), "public\"value".into()])
        .unwrap()
        .sql;
    assert!(sql.starts_with("SELECT \"id\", \"public\"\"value\" FROM"));
    assert!(!sql.contains("secret"));
    assert!(key.read_query_columns(&range, &["value".into()]).is_err());
    assert!(
        key.read_query_columns(&range, &["id".into(), "id".into()])
            .is_err()
    );
}
fn opaque(value: Value, type_id: &str) -> skedaddle::pg::chunk::Result<OpaqueValue> {
    Ok(OpaqueValue {
        type_id: type_id.into(),
        bytes: Some(value.canonical_bytes()?),
    })
}
fn int_row(row: tokio_postgres::Row) -> skedaddle::pg::chunk::Result<Row> {
    Ok([
        ("id".into(), opaque(Value::Int4(row.try_get(0)?), "int4")?),
        (
            "body".into(),
            opaque(
                Value::parse_pg_text(
                    &PgType::Text {
                        encoding: "UTF8".into(),
                    },
                    Some(row.try_get::<_, String>(1)?.as_bytes()),
                )?,
                "text",
            )?,
        ),
    ]
    .into())
}

struct Fixture {
    dsn: String,
    client: Client,
    driver: tokio::task::JoinHandle<()>,
    id: String,
    slot: String,
    publication: String,
}
impl Fixture {
    async fn open(test: &str) -> TestResult<Option<Self>> {
        let Some(config) = std::env::var_os("SKEDADDLE_TEST_CONFIG") else {
            skip(test, "SKEDADDLE_TEST_CONFIG absent");
            return Ok(None);
        };
        let config: serde_json::Value = serde_json::from_slice(&std::fs::read(config)?)?;
        let Some(dsn) = config["pg_chunk"]
            .as_str()
            .or_else(|| config["pg"].as_str())
        else {
            skip(test, "harness config lacks pg_chunk/pg DSN");
            return Ok(None);
        };
        let (client, driver) = connect(dsn).await?;
        let id = format!("p5d_{}", uuid::Uuid::new_v4().simple());
        let slot = format!("{id}_slot");
        let publication = format!("{id}_pub");
        client.batch_execute(&format!("CREATE SCHEMA {id}; CREATE TABLE {id}.rows (id integer PRIMARY KEY, body text NOT NULL); INSERT INTO {id}.rows SELECT i, 'body-'||i FROM generate_series(1,8) i; CREATE PUBLICATION {publication} FOR TABLE {id}.rows")).await?;
        // Configured-but-broken infrastructure fails, never silently skips.
        if let Err(error) = client
            .query_one(
                "SELECT slot_name FROM pg_create_logical_replication_slot($1, 'pgoutput')",
                &[&slot],
            )
            .await
        {
            client
                .batch_execute(&format!(
                    "DROP PUBLICATION {publication}; DROP SCHEMA {id} CASCADE"
                ))
                .await?;
            return Err(error.into());
        }
        Ok(Some(Self {
            dsn: dsn.into(),
            client,
            driver,
            id,
            slot,
            publication,
        }))
    }
    fn table(&self) -> TableKey {
        TableKey {
            table: format!("{}.rows", self.id),
            relation: qualified(&self.id, "rows"),
            columns: vec![column("id", PgType::Int4, "int4", Direction::Asc)],
        }
    }
    fn source(&self) -> SourceIdentity {
        SourceIdentity {
            engine: "postgres".into(),
            system_id: "harness".into(),
            lineage: "harness".into(),
            db_name: self.id.clone(),
            stream: self.slot.clone(),
        }
    }
    async fn emit(&self, chunk: &ChunkRange, wm: WatermarkKind, attempt: u64) -> TestResult {
        Watermark {
            wm,
            chunk: chunk.id.clone(),
            attempt,
        }
        .emit(&self.client)
        .await?;
        Ok(())
    }
    async fn drain(&self, window: &mut Window) -> TestResult {
        let rows=self.client.query("SELECT data FROM pg_logical_slot_get_binary_changes($1, NULL, NULL, 'proto_version', '1', 'publication_names', $2, 'messages', 'true')",&[&self.slot,&self.publication]).await?;
        let mut relation = None;
        let mut high_seen = false;
        let mut markers = Vec::new();
        for row in rows {
            let data: Vec<u8> = row.get(0);
            let message = Decoder::default().decode(&data)?;
            let (kind, key, after) = match &message {
                Message::LogicalMessage {
                    prefix: skedaddle::pg::chunk::MESSAGE_PREFIX,
                    ..
                } => {
                    if let Ok(marker) = Watermark::parse(&message)
                        && marker.chunk.table == self.table().table
                    {
                        let Message::LogicalMessage { lsn, .. } = &message else {
                            unreachable!()
                        };
                        markers.push((marker, *lsn));
                    }
                    continue;
                }
                Message::Commit(commit) => {
                    for (marker, lsn) in markers.drain(..) {
                        high_seen |= marker.wm == WatermarkKind::High;
                        let bytes = marker.encode()?;
                        window.watermark(
                            &Message::LogicalMessage {
                                transactional: true,
                                lsn,
                                prefix: skedaddle::pg::chunk::MESSAGE_PREFIX,
                                content: &bytes,
                            },
                            commit.commit_lsn,
                        )?;
                    }
                    continue;
                }
                Message::Relation(r) if r.namespace == self.id && r.name == "rows" => {
                    relation = Some(r.id);
                    continue;
                }
                Message::Insert { relation_id, new } if Some(*relation_id) == relation => {
                    let new = tuple(new)?;
                    (EventKind::Insert, key_row(&new), Some(new))
                }
                Message::Update {
                    relation_id,
                    old,
                    new,
                } if Some(*relation_id) == relation => {
                    let new = tuple(new)?;
                    let old = match old {
                        Some(OldTuple::Key(t) | OldTuple::Full(t)) => tuple(t)?,
                        None => new.clone(),
                    };
                    (EventKind::Update, key_row(&old), Some(new))
                }
                Message::Delete { relation_id, old } if Some(*relation_id) == relation => {
                    let (OldTuple::Key(t) | OldTuple::Full(t)) = old;
                    (EventKind::Delete, key_row(&tuple(t)?), None)
                }
                _ => continue,
            };
            window.event(&Event {
                source: self.source(),
                txn: TxnId(vec![1]),
                ordinal_in_txn: 0,
                position: Position::new("pg", 0u64.to_be_bytes()),
                schema_version: 1,
                kind,
                table: Some(self.table().table),
                key,
                before: None,
                after,
                unchanged_cols: Default::default(),
            })?;
        }
        assert!(high_seen, "configured PG lane did not decode H");
        Ok(())
    }
    async fn close(mut self) -> TestResult {
        if self.client.is_closed() {
            let (client, driver) = connect(&self.dsn).await?;
            self.client = client;
            self.driver.abort();
            self.driver = driver;
        }
        self.client
            .query_one("SELECT pg_drop_replication_slot($1)", &[&self.slot])
            .await?;
        self.client
            .batch_execute(&format!(
                "DROP PUBLICATION {}; DROP SCHEMA {} CASCADE",
                self.publication, self.id
            ))
            .await?;
        drop(self.client);
        self.driver.await?;
        Ok(())
    }
}
async fn connect(dsn: &str) -> TestResult<(Client, tokio::task::JoinHandle<()>)> {
    let (client, driver) = tls_connect(dsn).await?;
    Ok((
        client,
        tokio::spawn(async move {
            let _ = driver.await;
        }),
    ))
}
fn key_row(row: &Row) -> Row {
    [("id".into(), row["id"].clone())].into()
}
fn tuple(tuple: &Tuple<'_>) -> TestResult<Row> {
    let mut row = Row::new();
    for (i, value) in tuple.iter().enumerate() {
        let (name, ty) = if i == 0 {
            ("id", PgType::Int4)
        } else {
            (
                "body",
                PgType::Text {
                    encoding: "UTF8".into(),
                },
            )
        };
        let bytes = match value {
            Column::Text(b) => Some(*b),
            Column::Null => None,
            Column::UnchangedToast => continue,
            Column::Binary(_) => return Err("unexpected binary tuple".into()),
        };
        row.insert(
            name.into(),
            opaque(
                Value::parse_pg_text(&ty, bytes)?,
                if i == 0 { "int4" } else { "text" },
            )?,
        );
    }
    Ok(row)
}
fn remaining(record: SegmentRecord) -> Vec<i32> {
    let bytes = record.canonical_bytes().unwrap();
    assert_eq!(SegmentRecord::from_canonical_bytes(&bytes).unwrap(), record);
    let SegmentRecord::ChunkRows(txn) = record else {
        panic!("expected one chunk record")
    };
    txn.events
        .iter()
        .map(|e| {
            assert_eq!(e.position, txn.commit_position);
            let Value::Int4(id) =
                Value::from_canonical_bytes(e.key["id"].bytes.as_ref().unwrap()).unwrap()
            else {
                panic!()
            };
            id
        })
        .collect()
}

#[tokio::test]
async fn chunk_inwindow_update_delete_insert_excluded() -> TestResult {
    let Some(mut f) = Fixture::open("chunk_inwindow_update_delete_insert_excluded").await? else {
        return Ok(());
    };
    let result = AssertUnwindSafe(async {
        let table = f.table();
        let chunk =
            plan_chunks(&table, 10, &[vec![Value::Int4(2)], vec![Value::Int4(8)]])?[1].clone();
        f.emit(&chunk, WatermarkKind::Low, 0).await?;
        f.client
            .batch_execute(&format!(
                "UPDATE {0}.rows SET body='updated' WHERE id=2;
             DELETE FROM {0}.rows WHERE id=3;
             INSERT INTO {0}.rows VALUES(0,'inserted'),(3,'reinserted')",
                f.id
            ))
            .await?;
        let rows = table.read(&mut f.client, &chunk, int_row).await?;
        // Changes before and after the snapshot, and key changes across each
        // boundary. Without L/H exclusion these stale rows overwrite/resurrect.
        f.client
            .batch_execute(&format!(
                "UPDATE {0}.rows SET id=30 WHERE id=4;
             DELETE FROM {0}.rows WHERE id=5;
             UPDATE {0}.rows SET id=5 WHERE id=0;
             UPDATE {0}.rows SET id=0 WHERE id=7;
             UPDATE {0}.rows SET id=7 WHERE id=8",
                f.id
            ))
            .await?;
        f.emit(&chunk, WatermarkKind::High, 0).await?;
        let mut window = Window::new(table, chunk, rows, 0)?;
        f.drain(&mut window).await?;
        assert_eq!(remaining(window.finish(f.source(), "pg", 1)?), vec![6]);
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .catch_unwind()
    .await;
    f.close().await?;
    match result {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

#[tokio::test]
async fn chunk_resume_after_crash() -> TestResult {
    let Some(mut f) = Fixture::open("chunk_resume_after_crash").await? else {
        return Ok(());
    };
    let result = AssertUnwindSafe(async {
        let table = f.table();
        let plan = plan_chunks(&table, 4, &[vec![Value::Int4(5)]])?;
        let journal = vec![ChunkTransition::ChunkStarted {
            chunk: plan[0].clone(),
            attempt: 0,
        }];
        let root = PathBuf::from(
            std::env::var_os("SKEDADDLE_TEST_ROOT")
                .ok_or("harness must set SKEDADDLE_TEST_ROOT under J:/gigglesdb-work")?,
        )
        .join(&f.id);
        if !root
            .to_string_lossy()
            .replace('\\', "/")
            .to_lowercase()
            .starts_with("j:/gigglesdb-work/")
        {
            return Err("test root must stay under J:/gigglesdb-work".into());
        }
        std::fs::create_dir_all(&root)?;
        let path = root.join("chunk-journal.json");
        durable_write(&path, &serde_json::to_vec(&journal)?)?;
        f.emit(&plan[0], WatermarkKind::Low, 0).await?;
        let discarded = table.read(&mut f.client, &plan[0], int_row).await?;
        // Kill the real source reader connection after L/read and before H or
        // publication. A new connection/window must redo the unfinished range.
        let pid: i32 = f
            .client
            .query_one("SELECT pg_backend_pid()", &[])
            .await?
            .get(0);
        let (killer, killer_driver) = connect(&f.dsn).await?;
        killer
            .query_one("SELECT pg_terminate_backend($1)", &[&pid])
            .await?;
        drop(killer);
        killer_driver.await?;
        assert!(f.client.simple_query("SELECT 1").await.is_err());
        drop(discarded);
        let (client, driver) = connect(&f.dsn).await?;
        f.client = client;
        f.driver.abort();
        f.driver = driver;
        let recovered: Vec<ChunkTransition> = serde_json::from_slice(&std::fs::read(&path)?)?;
        assert_eq!(plan, plan_chunks(&table, 4, &[vec![Value::Int4(5)]])?);
        assert_eq!(unfinished_chunks(&plan, &recovered)?, plan);
        let mut completed = recovered;
        completed.push(ChunkTransition::ChunkStarted {
            chunk: plan[0].clone(),
            attempt: 1,
        });
        durable_write(&path, &serde_json::to_vec(&completed)?)?;
        // The abandoned attempt's L is still in the retained slot. Replay must
        // ignore it, rather than treating it as the replacement attempt's L.
        f.emit(&plan[0], WatermarkKind::Low, 1).await?;
        let rows = table.read(&mut f.client, &plan[0], int_row).await?;
        f.emit(&plan[0], WatermarkKind::High, 1).await?;
        let mut window = Window::new(table, plan[0].clone(), rows, 1)?;
        f.drain(&mut window).await?;
        let record = window.finish(f.source(), "pg", 1)?;
        let high_lsn = u64::from_be_bytes(record.position().bytes().try_into()?);
        durable_write(&root.join("chunk-record.bin"), &record.canonical_bytes()?)?;
        assert_eq!(remaining(record), vec![1, 2, 3, 4]);
        completed.push(ChunkTransition::ChunkDone {
            chunk: plan[0].clone(),
            attempt: 1,
            high_lsn,
        });
        durable_write(&path, &serde_json::to_vec(&completed)?)?;
        let recovered = serde_json::from_slice::<Vec<ChunkTransition>>(&std::fs::read(&path)?)?;
        assert_eq!(unfinished_chunks(&plan, &recovered)?, plan[1..]);
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .catch_unwind()
    .await;
    f.close().await?;
    match result {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

async fn check_boundaries(f: &mut Fixture, table: TableKey, projection: &str) -> TestResult {
    // Read keys as source text representations. PG casts in SQL, not lexical
    // client comparisons, decide range membership for UUID/text/composite DESC.
    let types = table
        .columns
        .iter()
        .map(|c| c.pg_type.clone())
        .collect::<Vec<_>>();
    let samples = table
        .sample(&mut f.client, 2, |row| {
            types
                .iter()
                .enumerate()
                .map(|(i, ty)| {
                    let text: String = row.try_get(i)?;
                    Ok(Value::parse_pg_text(ty, Some(text.as_bytes()))?)
                })
                .collect::<skedaddle::pg::chunk::Result<Key>>()
        })
        .await?;
    let plan = plan_chunks(&table, 2, &samples)?;
    let mut actual = Vec::new();
    for chunk in &plan {
        let q = table.read_query(chunk)?;
        let rows = q.execute(&mut f.client).await?;
        assert!(rows.len() <= 2, "chunk larger than configured stride");
        for row in rows {
            actual.push(row.get::<_, String>("label"));
        }
    }
    let expected = f
        .client
        .query(projection, &[])
        .await?
        .into_iter()
        .map(|r| r.get::<_, String>(0))
        .collect::<Vec<_>>();
    assert_eq!(
        actual, expected,
        "ranges must partition actual source ORDER BY without overlap/gaps"
    );
    assert_eq!(plan, plan_chunks(&table, 2, &samples)?);
    Ok(())
}

#[tokio::test]
async fn chunk_boundaries_composite_text_uuid_desc_keys() -> TestResult {
    let Some(mut f) = Fixture::open("chunk_boundaries_composite_text_uuid_desc_keys").await? else {
        return Ok(());
    };
    let result = AssertUnwindSafe(async {
        f.client.batch_execute(&format!(
            "CREATE TABLE {0}.mixed (a text COLLATE \"C\" NOT NULL,
                 b text COLLATE \"C\" NOT NULL,label text,PRIMARY KEY(a,b));
             CREATE INDEX mixed_desc ON {0}.mixed(a ASC,b DESC);
             INSERT INTO {0}.mixed VALUES ('a','z','az'),('a','m','am'),('a','A','aA'),
                 ('B','z','Bz'),('B','a','Ba'),('z','0','z0');
             CREATE TABLE {0}.uuids (id uuid PRIMARY KEY,label text);
             INSERT INTO {0}.uuids SELECT
                 ('00000000-0000-0000-0000-'||lpad(i::text,12,'0'))::uuid,i::text
                 FROM generate_series(1,7) i", f.id
        )).await?;
        let mut a = column("a", PgType::Text { encoding: "UTF8".into() }, "text", Direction::Asc);
        a.collation = Some(qualified("pg_catalog", "C"));
        let mut b = a.clone();
        b.name = "b".into();
        b.direction = Direction::Desc;
        let table = TableKey { table: format!("{}.mixed",f.id), relation: qualified(&f.id,"mixed"), columns: vec![a,b] };
        let expected = format!("SELECT label FROM {}.mixed ORDER BY a COLLATE \"C\" ASC,b COLLATE \"C\" DESC", f.id);
        check_boundaries(&mut f, table.clone(), &expected).await?;
        for direction in [Direction::Asc, Direction::Desc] {
            let mut uniform = table.clone();
            for c in &mut uniform.columns { c.direction = direction; }
            let order = if direction == Direction::Asc { "ASC" } else { "DESC" };
            let expected = format!("SELECT label FROM {}.mixed ORDER BY a COLLATE \"C\" {order},b COLLATE \"C\" {order}",f.id);
            check_boundaries(&mut f, uniform, &expected).await?;
        }
        if let Some(collation) = f.client.query_opt(
            "SELECT n.nspname,c.collname FROM pg_collation c
             JOIN pg_namespace n ON n.oid=c.collnamespace
             WHERE c.collname='en-US-x-icu' AND c.collisdeterministic", &[]
        ).await? {
            let mut collated = table.clone();
            let collation = qualified(collation.get(0), collation.get(1));
            for c in &mut collated.columns { c.collation = Some(collation.clone()); }
            let sql_collation = collation.sql()?;
            let expected = format!("SELECT label FROM {}.mixed ORDER BY a COLLATE {sql_collation} ASC,b COLLATE {sql_collation} DESC", f.id);
            check_boundaries(&mut f, collated, &expected).await?;
        } else {
            skip("chunk ICU collation subcase", "en-US-x-icu unavailable; explicit C collation still tested");
        }
        let native = TableKey { table: format!("{}.uuids",f.id), relation: qualified(&f.id,"uuids"), columns: vec![column("id",PgType::Uuid,"uuid",Direction::Desc)] };
        let samples = native.sample(&mut f.client, 2, |r| Ok(vec![Value::Uuid(r.try_get::<_,UuidBytes>(0)?.0)])).await?;
        let plan = plan_chunks(&native, 2, &samples)?;
        let mut labels = Vec::new();
        for c in &plan {
            for r in native.read_query(c)?.execute(&mut f.client).await? { labels.push(r.get::<_,String>("label")); }
        }
        assert_eq!(labels, vec!["7","6","5","4","3","2","1"]);
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    }).catch_unwind().await;
    f.close().await?;
    match result {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

// The harness CA is explicit; missing trust is a failure once a DB gate is set.
fn tls_config(postgres: tokio_postgres::Config) -> skedaddle::pg::tls::Config {
    let path = std::env::var_os("SKEDADDLE_TEST_CONFIG").expect("TLS harness config required");
    let config: serde_json::Value =
        serde_json::from_slice(&std::fs::read(path).expect("read TLS harness config"))
            .expect("TLS harness JSON");
    let ca_pem = config["ca_pem"]
        .as_str()
        .or_else(|| config["replication"]["ca_pem"].as_str())
        .expect("harness ca_pem or replication.ca_pem required");
    skedaddle::pg::tls::Config::new(postgres, ca_pem)
}

async fn tls_connect(
    dsn: &str,
) -> std::result::Result<
    (tokio_postgres::Client, skedaddle::pg::tls::Driver),
    skedaddle::pg::tls::Error,
> {
    let config = tls_config(dsn.parse().expect("harness PostgreSQL configuration"));
    let (client, driver, transport) =
        skedaddle::pg::tls::connect(&config, &skedaddle::security::TlsPolicy::Required).await?;
    assert!(transport.encrypted());
    assert!(transport.warnings.is_empty());
    Ok((client, driver))
}
