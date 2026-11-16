//! Exact-T snapshot planning. Persist encode() with Runtime::marker before
//! executing partitions; restoring always reuses saved tokens, never repartitions.
pub mod copy;
use super::{client::Client, frontier::SpannerOrder, stream::timestamp};
use crate::{definition::SpannerDialect, event::PositionOrder};
use google_cloud_spanner::{
    batch::Partition,
    model::PartitionOptions,
    result::{ResultSet, Row},
    transaction::TimestampBound,
    value::Timestamp,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, fmt};

#[derive(Debug)]
pub enum Error {
    Encoding,
    Version {
        surface: &'static str,
        found: Option<u64>,
    },
    BindingMismatch,
    Metadata(String),
    SessionPartitionExpired(google_cloud_spanner::Error),
    RetentionBlocked(google_cloud_spanner::Error),
    Service(google_cloud_spanner::Error),
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Spanner snapshot: {self:?}")
    }
}
impl std::error::Error for Error {}
type Result<T> = std::result::Result<T, Error>;
fn service(e: google_cloud_spanner::Error) -> Error {
    let message = e
        .status()
        .map(|s| s.message.to_ascii_lowercase())
        .unwrap_or_default();
    if message.contains("retention")
        || message.contains("too old")
        || message.contains("earliest version")
    {
        Error::RetentionBlocked(e)
    } else if (message.contains("session")
        || message.contains("partition")
        || message.contains("transaction"))
        && (message.contains("expired")
            || message.contains("not found")
            || message.contains("invalid"))
    {
        Error::SessionPartitionExpired(e)
    } else {
        Error::Service(e)
    }
}
fn canonical(value: &impl Serialize) -> Result<Vec<u8>> {
    let mut value = serde_json::to_value(value).map_err(|_| Error::Encoding)?;
    value.sort_all_objects();
    serde_json::to_vec(&value).map_err(|_| Error::Encoding)
}
fn digest(value: &impl Serialize) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(canonical(value)?)))
}
fn version(value: &Value, surface: &'static str) -> Result<()> {
    let found = value["version"].as_u64();
    if found != Some(1) {
        return Err(Error::Version { surface, found });
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableName {
    pub schema: String,
    pub name: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Column {
    pub name: String,
    pub spanner_type: String,
    pub nullable: String,
    pub key_ordinal: Option<i64>,
    pub key_order: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Table {
    pub name: TableName,
    pub columns: Vec<Column>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    project: String,
    instance: String,
    database: String,
    stream: String,
    dialect: SpannerDialect,
    emulator: bool,
    host: Option<String>,
    timestamp: Timestamp,
    domain: String,
    metadata_digest: String,
}
impl Binding {
    fn new(client: &Client, ts: Timestamp, tables: &[Table]) -> Result<Self> {
        let (_, e, host) = client.snapshot_connection();
        let order = SpannerOrder::new(
            &e.project,
            &e.instance,
            &e.database,
            &e.change_stream,
            timestamp(ts),
        )
        .map_err(|_| Error::BindingMismatch)?;
        Ok(Self {
            project: e.project.clone(),
            instance: e.instance.clone(),
            database: e.database.clone(),
            stream: e.change_stream.clone(),
            dialect: e.dialect,
            emulator: e.emulator,
            host: host.map(str::to_owned),
            timestamp: ts,
            domain: order.domain().into(),
            metadata_digest: digest(&tables)?,
        })
    }
    pub fn timestamp(&self) -> Timestamp {
        self.timestamp
    }
    pub fn domain(&self) -> &str {
        &self.domain
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PartitionEnvelope {
    version: u32,
    sdk: Value,
}
impl PartitionEnvelope {
    fn executable(&self) -> Result<Partition> {
        version(&serde_json::json!({"version": self.version}), "partition")?;
        // The SDK accepts missing/zero versions. Check before invoking its decoder.
        version(&self.sdk, "sdk partition")?;
        serde_json::from_value(self.sdk.clone()).map_err(|_| Error::Encoding)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlannedPartition {
    id: String,
    table: TableName,
    envelope: PartitionEnvelope,
}
impl PlannedPartition {
    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn table(&self) -> &TableName {
        &self.table
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Data {
    binding: Binding,
    tables: Vec<Table>,
    partitions: Vec<PlannedPartition>,
}
#[derive(Clone, Debug, Serialize)]
pub struct Plan {
    version: u32,
    digest: String,
    plan: Data,
}
impl Plan {
    /// This is the only path that chooses a strong timestamp and plans tokens.
    pub async fn create(client: &Client, selected: &[TableName]) -> Result<Self> {
        let (db, _, _) = client.snapshot_connection();
        let strong = db
            .batch_read_only_transaction()
            .build()
            .await
            .map_err(service)?;
        let ts = strong.read_timestamp().ok_or(Error::Encoding)?;
        let mut names = selected.to_vec();
        names.sort();
        if names.is_empty() || names.windows(2).any(|w| w[0] == w[1]) {
            return Err(Error::Encoding);
        }
        let tables = discover(client, ts, &names).await?;
        let binding = Binding::new(client, ts, &tables)?;
        let tx = db
            .batch_read_only_transaction()
            .set_timestamp_bound(TimestampBound::read_timestamp(ts))
            .build()
            .await
            .map_err(service)?;
        let mut partitions = Vec::new();
        for table in &tables {
            for sdk in tx
                .partition_query(query(table, binding.dialect), PartitionOptions::default())
                .await
                .map_err(service)?
            {
                let envelope = PartitionEnvelope {
                    version: 1,
                    sdk: serde_json::to_value(sdk).map_err(|_| Error::Encoding)?,
                };
                let id = digest(&(&table.name, &envelope))?;
                partitions.push(PlannedPartition {
                    id,
                    table: table.name.clone(),
                    envelope,
                });
            }
        }
        let plan = Data {
            binding,
            tables,
            partitions,
        };
        Ok(Self {
            version: 1,
            digest: digest(&plan)?,
            plan,
        })
    }
    pub fn binding(&self) -> &Binding {
        &self.plan.binding
    }
    pub fn tables(&self) -> &[Table] {
        &self.plan.tables
    }
    pub fn partitions(&self) -> &[PlannedPartition] {
        &self.plan.partitions
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        canonical(self)
    }
    pub fn decode(bytes: &[u8], expected: &Binding) -> Result<Self> {
        let value: Value = serde_json::from_slice(bytes).map_err(|_| Error::Encoding)?;
        version(&value, "plan")?;
        for partition in value["plan"]["partitions"]
            .as_array()
            .ok_or(Error::Encoding)?
        {
            version(&partition["envelope"], "partition")?;
            version(&partition["envelope"]["sdk"], "sdk partition")?;
        }
        let decoded = Self {
            version: 1,
            digest: value["digest"].as_str().ok_or(Error::Encoding)?.into(),
            plan: serde_json::from_value(value["plan"].clone()).map_err(|_| Error::Encoding)?,
        };
        if &decoded.plan.binding != expected {
            return Err(Error::BindingMismatch);
        }
        if decoded.encode()? != bytes
            || decoded.digest != digest(&decoded.plan)?
            || expected.metadata_digest != digest(&decoded.plan.tables)?
        {
            return Err(Error::Encoding);
        }
        let mut ids = BTreeSet::new();
        for p in decoded.partitions() {
            p.envelope.executable()?;
            if p.id != digest(&(&p.table, &p.envelope))?
                || !ids.insert(&p.id)
                || !decoded.tables().iter().any(|t| t.name == p.table)
            {
                return Err(Error::Encoding);
            }
        }
        Ok(decoded)
    }
    pub async fn restore(client: &Client, bytes: &[u8], expected: &Binding) -> Result<Self> {
        let plan = Self::decode(bytes, expected)?;
        plan.check_client(client)?;
        let names = plan
            .tables()
            .iter()
            .map(|t| t.name.clone())
            .collect::<Vec<_>>();
        let tables = discover(client, expected.timestamp, &names).await?;
        if digest(&tables)? != expected.metadata_digest {
            return Err(Error::BindingMismatch);
        }
        // No partition_query here: even an unfinished partition keeps its saved token.
        Ok(plan)
    }
    fn check_client(&self, client: &Client) -> Result<()> {
        if Binding::new(client, self.binding().timestamp, self.tables())? != *self.binding() {
            return Err(Error::BindingMismatch);
        }
        Ok(())
    }
    pub async fn execute(&self, client: &Client, id: &str) -> Result<Rows> {
        self.check_client(client)?;
        let p = self
            .partitions()
            .iter()
            .find(|p| p.id == id)
            .ok_or(Error::BindingMismatch)?;
        Ok(Rows(
            p.envelope
                .executable()?
                .execute(client.snapshot_connection().0)
                .await
                .map_err(service)?,
        ))
    }
}
/// Preserve typed recovery errors from both RPC startup and streaming rows.
pub struct Rows(ResultSet);
impl Rows {
    pub async fn next(&mut self) -> Option<Result<Row>> {
        self.0.next().await.map(|r| r.map_err(service))
    }
}
fn quote(s: &str, dialect: SpannerDialect) -> String {
    let q = if dialect == SpannerDialect::GoogleSql {
        '`'
    } else {
        '"'
    };
    let escaped = if q == '`' {
        s.replace('\\', "\\\\").replace('`', "\\`")
    } else {
        s.replace('"', "\"\"")
    };
    format!("{q}{escaped}{q}")
}
fn query(table: &Table, dialect: SpannerDialect) -> String {
    let name = if table.name.schema.is_empty() {
        quote(&table.name.name, dialect)
    } else {
        format!(
            "{}.{}",
            quote(&table.name.schema, dialect),
            quote(&table.name.name, dialect)
        )
    };
    format!(
        "SELECT {} FROM {name}",
        table
            .columns
            .iter()
            .map(|c| quote(&c.name, dialect))
            .collect::<Vec<_>>()
            .join(",")
    )
}
async fn discover(client: &Client, ts: Timestamp, names: &[TableName]) -> Result<Vec<Table>> {
    let (db, endpoint, _) = client.snapshot_connection();
    let tx = db
        .read_only_transaction()
        .set_timestamp_bound(TimestampBound::read_timestamp(ts))
        .build()
        .await
        .map_err(service)?;
    let literal = |s: &str| {
        let s = if endpoint.dialect == SpannerDialect::GoogleSql {
            s.replace('\\', "\\\\")
        } else {
            s.to_owned()
        };
        format!("'{}'", s.replace('\'', "''"))
    };
    let mut tables = Vec::new();
    for name in names {
        let sql = format!(
            "SELECT c.column_name,c.spanner_type,c.is_nullable,i.ordinal_position,i.column_ordering
            FROM information_schema.columns c JOIN information_schema.tables t
            ON c.table_catalog=t.table_catalog AND c.table_schema=t.table_schema AND c.table_name=t.table_name
            LEFT JOIN information_schema.index_columns i ON c.table_catalog=i.table_catalog
            AND c.table_schema=i.table_schema AND c.table_name=i.table_name AND c.column_name=i.column_name
            AND i.index_name='PRIMARY_KEY' WHERE c.table_schema={} AND c.table_name={}
            AND t.table_type='BASE TABLE' ORDER BY c.ordinal_position",
            literal(&name.schema),
            literal(&name.name)
        );
        let mut result = tx.execute_query(sql).await.map_err(service)?;
        let mut columns = Vec::new();
        while let Some(row) = result.next().await {
            let row = row.map_err(service)?;
            let column = (|| -> std::result::Result<_, google_cloud_spanner::Error> {
                Ok(Column {
                    name: row.get(0)?,
                    spanner_type: row.get(1)?,
                    nullable: row.get(2)?,
                    key_ordinal: row.get(3)?,
                    key_order: row.get(4)?,
                })
            })()
            .map_err(|e| Error::Metadata(e.to_string()))?;
            columns.push(column);
        }
        if columns.is_empty() {
            return Err(Error::Metadata(format!("selected table missing: {name:?}")));
        }
        tables.push(Table {
            name: name.clone(),
            columns,
        });
    }
    Ok(tables)
}

#[cfg(test)]
mod tests {
    use super::*;
    pub(super) fn fixture() -> Plan {
        let table = Table {
            name: TableName {
                schema: "".into(),
                name: "t".into(),
            },
            columns: vec![],
        };
        let binding = Binding {
            project: "p".into(),
            instance: "i".into(),
            database: "d".into(),
            stream: "s".into(),
            dialect: SpannerDialect::GoogleSql,
            emulator: true,
            host: Some("http://localhost:1".into()),
            timestamp: Timestamp::new(1, 0).unwrap(),
            domain: "spanner:p/i/d/s/1000000000".into(),
            metadata_digest: digest(&vec![&table]).unwrap(),
        };
        // Wire-format fixture only, never a substitute for a real database test.
        let envelope = PartitionEnvelope {
            version: 1,
            sdk: serde_json::json!({"version":1,"inner":{"Query":{
            "session":"projects/p/instances/i/databases/d/sessions/s", "transaction":{"id":"dHg="},
            "sql":"SELECT * FROM t", "partitionToken":"cA=="}}}),
        };
        let id = digest(&(&table.name, &envelope)).unwrap();
        let plan = Data {
            binding,
            partitions: vec![PlannedPartition {
                id,
                table: table.name.clone(),
                envelope,
            }],
            tables: vec![table],
        };
        Plan {
            version: 1,
            digest: digest(&plan).unwrap(),
            plan,
        }
    }
    #[test]
    fn canonical_round_trip_and_corruption() {
        let p = fixture();
        let bytes = p.encode().unwrap();
        assert_eq!(
            Plan::decode(&bytes, p.binding()).unwrap().encode().unwrap(),
            bytes
        );
        let mut bad = p.clone();
        bad.digest.push('0');
        assert!(matches!(
            Plan::decode(&bad.encode().unwrap(), p.binding()),
            Err(Error::Encoding)
        ));
        let mut spaced = bytes;
        spaced.push(b' ');
        assert!(matches!(
            Plan::decode(&spaced, p.binding()),
            Err(Error::Encoding)
        ));
    }
    #[test]
    fn reject_all_version_fallbacks() {
        let p = fixture();
        for surface in ["plan", "partition", "sdk partition"] {
            for replacement in [None, Some(0), Some(2)] {
                let mut value = serde_json::to_value(&p).unwrap();
                let target = match surface {
                    "plan" => &mut value,
                    "partition" => &mut value["plan"]["partitions"][0]["envelope"],
                    _ => &mut value["plan"]["partitions"][0]["envelope"]["sdk"],
                };
                target.as_object_mut().unwrap().remove("version");
                if let Some(v) = replacement {
                    target["version"] = v.into();
                }
                assert!(
                    matches!(Plan::decode(&canonical(&value).unwrap(), p.binding()),
                    Err(Error::Version { surface: s, .. }) if s == surface)
                );
            }
        }
    }
    #[test]
    fn reject_binding_changes() {
        let p = fixture();
        let mut variants = vec![p.binding().clone(); 6];
        variants[0].host = None;
        variants[1].database.push('x');
        variants[2].dialect = SpannerDialect::Postgresql;
        variants[3].timestamp = Timestamp::new(2, 0).unwrap();
        variants[4].metadata_digest.push('x');
        variants[5].domain.push('x');
        for b in variants {
            assert!(matches!(
                Plan::decode(&p.encode().unwrap(), &b),
                Err(Error::BindingMismatch)
            ));
        }
    }
}
