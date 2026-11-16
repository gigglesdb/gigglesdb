//! Thin official-client adapter. SecretRef is deliberately never resolved here.
use crate::definition::{SpannerDialect, SpannerEndpoint};
use google_cloud_spanner::{
    client::{DatabaseClient, Spanner},
    value::Timestamp,
};
use serde_json::Value;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub struct Client {
    database: DatabaseClient,
    endpoint: SpannerEndpoint,
    host: Option<String>,
}
impl Client {
    /// Explicit host takes precedence over SPANNER_EMULATOR_HOST; otherwise ADC.
    /// Production ADC has not been qualified by the emulator tests.
    pub async fn connect(endpoint: &SpannerEndpoint, host: Option<&str>) -> Result<Self, Error> {
        let host = host
            .map(str::to_owned)
            .or_else(|| std::env::var("SPANNER_EMULATOR_HOST").ok());
        if endpoint.emulator && host.is_none() {
            return Err("emulator endpoint requires a host".into());
        }
        let mut builder = Spanner::builder();
        let mut resolved_host = None;
        if let Some(host) = host {
            let host = host.trim();
            if host.is_empty() {
                return Err("empty emulator host".into());
            }
            let url = if host.starts_with("http://") {
                host.to_owned()
            } else {
                format!("http://{host}")
            };
            resolved_host = Some(url.clone());
            builder = builder.with_endpoint(url).with_credentials(
                google_cloud_auth::credentials::anonymous::Builder::new().build(),
            );
        }
        let spanner = builder.build().await?;
        let name = format!(
            "projects/{}/instances/{}/databases/{}",
            endpoint.project, endpoint.instance, endpoint.database
        );
        Ok(Self {
            database: spanner.database_client(name).build().await?,
            endpoint: endpoint.clone(),
            host: resolved_host,
        })
    }
    pub(crate) fn snapshot_connection(&self) -> (&DatabaseClient, &SpannerEndpoint, Option<&str>) {
        (&self.database, &self.endpoint, self.host.as_deref())
    }
    /// The bounded query includes its start timestamp, including on reconnect.
    pub async fn change_stream(
        &self,
        token: Option<&str>,
        start: &str,
        end: &str,
        heartbeat_ms: u32,
    ) -> Result<Vec<Value>, Error> {
        let start_ts: Timestamp = serde_json::from_value(Value::String(start.into()))?;
        let end_ts: Timestamp = serde_json::from_value(Value::String(end.into()))?;
        if start_ts > end_ts || !(100..=300_000).contains(&heartbeat_ms) {
            return Err("invalid stream bounds or heartbeat".into());
        }
        let start = serde_json::to_value(start_ts)?;
        let end = serde_json::to_value(end_ts)?;
        let (start, end) = (
            start.as_str().ok_or("invalid timestamp")?,
            end.as_str().ok_or("invalid timestamp")?,
        );
        let stream = &self.endpoint.change_stream;
        if stream.is_empty()
            || !stream
                .bytes()
                .enumerate()
                .all(|(i, c)| c.is_ascii_alphabetic() || c == b'_' || (i > 0 && c.is_ascii_digit()))
        {
            return Err("invalid change stream identifier".into());
        }
        let token = token
            .map(|t| {
                let t = if self.endpoint.dialect == SpannerDialect::GoogleSql {
                    t.replace('\\', "\\\\")
                } else {
                    t.to_owned()
                };
                format!("'{}'", t.replace('\'', "''"))
            })
            .unwrap_or_else(|| "NULL".into());
        let sql = match self.endpoint.dialect {
            SpannerDialect::GoogleSql => format!(
                "SELECT ChangeRecord FROM READ_{stream}(TIMESTAMP '{start}', TIMESTAMP '{end}', {token}, {heartbeat_ms})"
            ),
            SpannerDialect::Postgresql => format!(
                "SELECT * FROM spanner.read_json_{stream}('{start}'::timestamptz, '{end}'::timestamptz, {token}, {heartbeat_ms})"
            ),
        };
        let mut result = self
            .database
            .single_use()
            .build()
            .execute_query(sql)
            .await?;
        let mut rows = Vec::new();
        while let Some(row) = result.next().await {
            rows.push(Value::Array(
                row?.raw_values()
                    .iter()
                    .zip(result.metadata().column_types())
                    .map(|(v, t)| decode(v, &t.clone().into()))
                    .collect::<Result<_, _>>()?,
            ));
        }
        Ok(rows)
    }
    /// One tagged read-write transaction; the SDK owns abort retries.
    pub async fn write(&self, statements: &[String], tag: &str) -> Result<Timestamp, Error> {
        let result = self
            .database
            .read_write_transaction()
            .set_transaction_tag(tag)
            .build()
            .run(async |tx| {
                for sql in statements {
                    tx.execute_update(sql.as_str()).await?;
                }
                Ok(())
            })
            .await?;
        result
            .commit_timestamp()
            .ok_or_else(|| "missing commit timestamp".into())
    }
}
fn decode(
    v: &google_cloud_spanner::value::Value,
    ty: &google_cloud_spanner::model::Type,
) -> Result<Value, Error> {
    if let Some(list) = v.as_list() {
        if let Some(element) = ty.array_element_type.as_deref() {
            return list
                .iter()
                .map(|v| decode(v, element))
                .collect::<Result<Vec<_>, _>>()
                .map(Value::Array);
        }
        if let Some(s) = &ty.struct_type {
            if s.fields.len() != list.len() {
                return Err("STRUCT field count mismatch".into());
            }
            return s
                .fields
                .iter()
                .zip(list.iter())
                .map(|(f, v)| {
                    Ok((
                        f.name.clone(),
                        decode(v, f.r#type.as_deref().ok_or("missing STRUCT type")?)?,
                    ))
                })
                .collect::<Result<serde_json::Map<_, _>, Error>>()
                .map(Value::Object);
        }
        return Err("untyped list value".into());
    }
    Ok(if let Some(s) = v.as_str() {
        Value::String(s.into())
    } else if let Some(b) = v.as_bool() {
        Value::Bool(b)
    } else if let Some(n) = v.as_f64() {
        serde_json::json!(n)
    } else {
        Value::Null
    })
}
