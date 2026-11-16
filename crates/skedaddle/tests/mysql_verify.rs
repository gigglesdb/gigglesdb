use mysql_async::{Params, prelude::Queryable};
use skedaddle::{
    coordinator::chunk::ChunkPlan,
    mapping::{PgType, Value, mysql::plan_column},
    mysql::{
        MysqlPosition,
        chunk::map_row,
        chunk_key::{MysqlKeyColumn, MysqlKeyDirection, MysqlTableKey},
        client::MysqlConfig,
        schema::{TableDescriptor, load_descriptors},
    },
    verify::{
        self, BoundarySource, Difference, Key, KeyOrder, Projection, Row, VerifyError,
        mysql::{MysqlRowSource, SnapshotBoundary},
        sql::RangeBudget,
    },
};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
fn config() -> TestResult<Option<MysqlConfig>> {
    let Some(path) = std::env::var_os("SKEDADDLE_MYSQL_CONFIG") else {
        eprintln!(
            "MYSQL SKIPPED: SKEDADDLE_MYSQL_CONFIG absent; use harness mysql84 GTID ON / mysql80 GTID OFF --test mysql_verify"
        );
        return Ok(None);
    };
    Ok(Some(serde_json::from_slice(&std::fs::read(path)?)?))
}
fn key(table: &TableDescriptor) -> MysqlTableKey {
    MysqlTableKey {
        event_table: format!("{}.{}", table.schema, table.table),
        schema: table.schema.clone(),
        table: table.table.clone(),
        columns: table
            .primary_key
            .iter()
            .map(|name| {
                let c = table.columns.iter().find(|c| &c.name == name).unwrap();
                MysqlKeyColumn {
                    name: c.name.clone(),
                    data_type: c.data_type.clone(),
                    column_type: c.column_type.clone(),
                    character_set: c.character_set.clone(),
                    collation: c.collation.clone(),
                    type_id: plan_column(c).unwrap().type_id,
                    direction: MysqlKeyDirection::Asc,
                }
            })
            .collect(),
    }
}

/// Fixture-only finite comparator, using weights fetched from the real MySQL
/// server. No guessed Rust/PG text order or byte order defines source collation.
#[derive(Clone)]
struct ServerWeights(BTreeMap<String, Vec<u8>>);
impl KeyOrder for ServerWeights {
    fn compare_keys(&self, a: &Key, b: &Key) -> verify::Result<Ordering> {
        let unpack = |k: &Key| -> verify::Result<(Vec<u8>, u64)> {
            if k.len() != 2 {
                return Err(VerifyError::Invalid("fixture key width"));
            }
            let text = String::from_utf8(
                k[0].format_pg_text(&PgType::Varchar {
                    encoding: "UTF8".into(),
                })?
                .ok_or(VerifyError::Invalid("fixture text"))?,
            )
            .map_err(|_| VerifyError::Invalid("fixture text"))?;
            let weight = self
                .0
                .get(&text)
                .ok_or(VerifyError::Invalid("fixture key outside server weights"))?
                .clone();
            let number = String::from_utf8(
                k[1].format_pg_text(&PgType::Numeric)?
                    .ok_or(VerifyError::Invalid("fixture number"))?,
            )
            .map_err(|_| VerifyError::Invalid("fixture number"))?
            .parse()
            .map_err(|_| VerifyError::Invalid("fixture number"))?;
            Ok((weight, number))
        };
        Ok(unpack(a)?.cmp(&unpack(b)?))
    }
}
fn mapped_row(
    raw: skedaddle::event::Row,
    key: &MysqlTableKey,
    projection: &Projection,
) -> TestResult<Row> {
    let value = |name: &str| -> TestResult<Value> {
        Ok(raw[name]
            .bytes
            .as_deref()
            .map(Value::from_canonical_bytes)
            .transpose()?
            .unwrap_or(Value::Null))
    };
    Ok(Row {
        key: key
            .columns
            .iter()
            .map(|c| value(&c.name))
            .collect::<TestResult<_>>()?,
        columns: projection
            .columns()
            .iter()
            .map(|n| Ok((n.clone(), value(n)?)))
            .collect::<TestResult<_>>()?,
    })
}

#[tokio::test]
async fn snapshot_paging_and_mapped_comparison_with_concurrent_commits() -> TestResult {
    let Some(config) = config()? else {
        return Ok(());
    };
    let mut writer = config.connect().await?;
    writer.conn.query_drop("SET NAMES utf8mb4").await?;
    writer.conn.query_drop("CREATE TABLE type_zoo.verify_composite (txt VARCHAR(12) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL, u BIGINT UNSIGNED NOT NULL, payload VARBINARY(32), sentinel LONGBLOB, unsupported POINT, PRIMARY KEY(txt,u)) ENGINE=InnoDB").await?;
    for (text, number) in [
        ("a", 1u64),
        ("A", 2),
        ("á", 9223372036854775808),
        ("a", u64::MAX),
        ("b", 1),
        ("B", 9223372036854775809),
        ("c", u64::MAX),
    ] {
        writer
            .conn
            .exec_drop(
                "INSERT INTO type_zoo.verify_composite (txt,u,payload,sentinel) VALUES(?,?,?,REPEAT('excluded',16384))",
                (text, number, vec![0u8, 255]),
            )
            .await?;
    }
    let table = load_descriptors(
        &mut writer.conn,
        &[("type_zoo".into(), "verify_composite".into())],
    )
    .await?
    .remove(0);
    let key = key(&table);
    let range = ChunkPlan::new(key.clone(), 100, vec![])?.ranges.remove(0);
    let projection = Projection::new(vec!["payload".into()])?;
    let mut weights = BTreeMap::new();
    for text in ["a", "A", "á", "b", "B", "c", "y", "z"] {
        let weight: Vec<u8> = writer
            .conn
            .exec_first(
                "SELECT WEIGHT_STRING(CONVERT(? USING utf8mb4) COLLATE utf8mb4_0900_ai_ci)",
                (text,),
            )
            .await?
            .ok_or("missing server weight")?;
        weights.insert(text.into(), weight);
    }
    let order = ServerWeights(weights);
    let budget = RangeBudget {
        max_rows: 20,
        max_bytes: 8192,
    };
    let mut source = MysqlRowSource::new(
        config.clone(),
        "verify-live".into(),
        table.clone(),
        key.clone(),
        order.clone(),
        budget,
        2,
    )?;
    assert!(matches!(
        source.read_source(&range, &projection).await,
        Err(VerifyError::Invalid(
            "complete MySQL change evidence required"
        ))
    ));

    // Independently map a full source SELECT to the in-test destination. There
    // is no PG connection in this lane; this exercises byte-exact capture mapping.
    let selected: Vec<_> = table
        .columns
        .iter()
        .filter(|c| {
            key.columns.iter().any(|k| k.name == c.name) || projection.columns().contains(&c.name)
        })
        .cloned()
        .collect();
    let query = key.read_query_columns(&range, &selected)?;
    let raw: Vec<mysql_async::Row> = writer
        .conn
        .exec(query.sql, Params::Positional(query.parameters))
        .await?;
    let destination = raw
        .into_iter()
        .map(|r| mapped_row(map_row(&selected, r)?, &key, &projection))
        .collect::<TestResult<Vec<_>>>()?;
    assert_eq!(destination.len(), 7);

    // Leave one transaction in flight before snapshot acquisition. Commit it
    // after begin; neither its insert nor any post-begin update/delete may leak.
    writer.conn.query_drop("START TRANSACTION").await?;
    writer
        .conn
        .query_drop("INSERT INTO type_zoo.verify_composite (txt,u,payload,sentinel) VALUES('z',1,'after',NULL)")
        .await?;
    let mut snapshot = source.begin_snapshot().await?;
    assert!(matches!(
        snapshot.boundary,
        SnapshotBoundary::UpperBound(_) | SnapshotBoundary::UncertifiedGtidSample(_)
    ));
    match snapshot.boundary.native() {
        MysqlPosition::Gtid { .. } => assert_eq!(config.gtid, "ON"),
        MysqlPosition::File { .. } => assert_eq!(config.gtid, "OFF"),
    }
    writer.conn.query_drop("COMMIT").await?;
    writer
        .conn
        .query_drop("UPDATE type_zoo.verify_composite SET payload='new' WHERE txt='b' AND u=1")
        .await?;
    writer
        .conn
        .query_drop("DELETE FROM type_zoo.verify_composite WHERE txt='c'")
        .await?;
    let first = source
        .read_snapshot(&mut snapshot, &range, &projection)
        .await?;
    assert_eq!(
        first.rows, destination,
        "all pre-snapshot rows and old values persist across every page"
    );
    let second = source
        .read_snapshot(&mut snapshot, &range, &projection)
        .await?;
    assert_eq!(first, second);
    assert_eq!(
        first
            .rows
            .iter()
            .map(|r| serde_json::to_vec(&r.key).unwrap())
            .collect::<BTreeSet<_>>()
            .len(),
        7
    );
    assert!(
        first
            .rows
            .iter()
            .all(|r| !r.columns.contains_key("sentinel"))
    );
    assert_eq!(
        MysqlPosition::decode(first.position.bytes())?,
        *snapshot.boundary.native()
    );
    assert!(matches!(
        snapshot.boundary.durable_report(),
        Err(VerifyError::NonPgVerificationReportUnsupported)
    ));

    let matching = verify::compare_rows(&order, &range, &projection, &first.rows, &destination)?;
    assert!(matching.differences.is_empty());
    assert_eq!(matching.source_count, 7);
    let mut damaged = destination.clone();
    damaged.remove(0);
    damaged[0]
        .columns
        .insert("payload".into(), Value::Bytea(vec![1, 2, 3]));
    let mut extra = damaged.last().unwrap().clone();
    extra.key[0] = Value::parse_pg_text(&plan_column(&table.columns[0])?.pg_type, Some(b"y"))?;
    damaged.push(extra);
    let compared = verify::compare_rows(&order, &range, &projection, &first.rows, &damaged)?;
    assert_eq!(compared.differences.len(), 3);
    assert_eq!(
        compared
            .differences
            .iter()
            .filter(|d| matches!(d, Difference::Missing { .. }))
            .count(),
        1
    );
    assert_eq!(
        compared
            .differences
            .iter()
            .filter(|d| matches!(d, Difference::Extra { .. }))
            .count(),
        1
    );
    assert_eq!(
        compared
            .differences
            .iter()
            .filter(|d| matches!(d, Difference::Changed { .. }))
            .count(),
        1
    );
    snapshot.finish().await?;
    let (fresh, boundary) = source.read_with_boundary(&range, &projection).await?;
    assert_ne!(
        fresh.rows, first.rows,
        "new snapshot must observe committed changes"
    );
    assert_eq!(source.last_boundary(), Some(&boundary));

    // Explicit range upper/lower and page-size-one coverage.
    let mut bounded = range.clone();
    bounded.lo = Some(first.rows[1].key.clone());
    bounded.hi = Some(first.rows[4].key.clone());
    let mut limited = MysqlRowSource::new(
        config.clone(),
        "verify-live".into(),
        table.clone(),
        key.clone(),
        order.clone(),
        budget,
        1,
    )?;
    let (read, _) = limited.read_with_boundary(&bounded, &projection).await?;
    assert_eq!(read.rows.len(), 3);
    assert_eq!(
        read.rows.iter().map(|r| &r.key).collect::<Vec<_>>(),
        first.rows[1..4].iter().map(|r| &r.key).collect::<Vec<_>>()
    );
    let mut capped = MysqlRowSource::new(
        config.clone(),
        "verify-live".into(),
        table.clone(),
        key.clone(),
        order.clone(),
        RangeBudget {
            max_rows: 2,
            max_bytes: 8192,
        },
        1,
    )?;
    assert!(
        capped
            .read_with_boundary(&range, &projection)
            .await
            .is_err()
    );
    assert!(capped.last_boundary().is_none());
    let sentinel = Projection::new(vec!["sentinel".into()])?;
    assert!(
        source.read_with_boundary(&range, &sentinel).await.is_err(),
        "oversized row refused before transfer"
    );
    assert!(
        source
            .read_with_boundary(&range, &Projection::new(vec!["outside".into()])?)
            .await
            .is_err()
    );
    let unsupported = Projection::new(vec!["unsupported".into()])?;
    assert!(matches!(
        source.read_with_boundary(&range, &unsupported).await,
        Err(VerifyError::Mapping(_))
    ));
    let mut empty = range.clone();
    empty.lo = Some(fresh.rows.last().unwrap().key.clone());
    // Even an empty page must reject an unsupported selected mapping.
    empty.lo.as_mut().unwrap()[1] =
        Value::parse_pg_text(&PgType::Numeric, Some(b"18446744073709551615"))?;
    assert!(matches!(
        source.read_with_boundary(&empty, &unsupported).await,
        Err(VerifyError::Mapping(_))
    ));
    // A failed snapshot read must not leave a transaction on the writer; the
    // reader owns separate connections and closes/rolls back its own failures.
    writer
        .conn
        .query_drop("DROP TABLE type_zoo.verify_composite")
        .await?;
    eprintln!(
        "MYSQL VERIFIED: {} GTID {}; in-test mapped destination; snapshot/paging/missing/extra/corruption/bounds",
        config.profile, config.gtid
    );
    Ok(())
}
