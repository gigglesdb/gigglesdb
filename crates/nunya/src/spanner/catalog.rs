//! Trusted Spanner information-schema query runner.
use super::proxy::{self, AuthInterceptor, Client, SpannerUpstream};
use crate::{
    error::{NyCode, Refusal},
    schema::spanner_gsql::{QueryResult, SqlRunner},
};
use googleapis_tonic_google_spanner_v1::google::spanner::v1::*;
use prost_types::{Value, value::Kind};
use std::sync::Arc;
use tonic::transport::Channel;
fn unavailable() -> Refusal {
    Refusal::new(NyCode::SchemaStale, "catalog snapshot unavailable")
}
/// Executes catalog SQL with gateway credentials and strong read-only transactions.
pub struct CatalogRunner {
    client: Client,
    channel: Channel,
    provider: Option<Arc<dyn gcp_auth::TokenProvider>>,
    database: String,
}
impl CatalogRunner {
    /// Connect using the same trusted transport as the proxy.
    pub async fn new(upstream: &SpannerUpstream, database: String) -> Result<Self, Refusal> {
        let (channel, provider) = proxy::connect(upstream).await.map_err(|_| unavailable())?;
        Ok(Self {
            client: spanner_client::SpannerClient::with_interceptor(
                channel.clone(),
                AuthInterceptor::default(),
            ),
            channel,
            provider,
            database,
        })
    }
    async fn client(&self) -> Result<Client, Refusal> {
        if let Some(provider) = &self.provider {
            let token = provider
                .token(&["https://www.googleapis.com/auth/spanner.data"])
                .await
                .map_err(|_| unavailable())?;
            let token = format!("Bearer {}", token.as_str())
                .parse()
                .map_err(|_| unavailable())?;
            Ok(spanner_client::SpannerClient::with_interceptor(
                self.channel.clone(),
                AuthInterceptor { token: Some(token) },
            ))
        } else {
            Ok(self.client.clone())
        }
    }
}
fn cell(value: Value) -> Result<Option<String>, Refusal> {
    match value.kind {
        Some(Kind::StringValue(v)) => Ok(Some(v)),
        Some(Kind::BoolValue(v)) => Ok(Some(v.to_string())),
        Some(Kind::NumberValue(v)) if v.is_finite() => Ok(Some(v.to_string())),
        Some(Kind::NullValue(_)) => Ok(None),
        _ => Err(unavailable()),
    }
}
impl SqlRunner for CatalogRunner {
    fn query<'a>(&'a self, sql: &'a str) -> QueryResult<'a> {
        Box::pin(async move {
            let mut client = self.client().await?;
            let session = client
                .create_session(CreateSessionRequest {
                    database: self.database.clone(),
                    ..Default::default()
                })
                .await
                .map_err(|_| unavailable())?
                .into_inner()
                .name;
            let result = client
                .execute_sql(ExecuteSqlRequest {
                    session: session.clone(),
                    sql: sql.into(),
                    transaction: Some(TransactionSelector {
                        selector: Some(transaction_selector::Selector::SingleUse(
                            TransactionOptions {
                                mode: Some(transaction_options::Mode::ReadOnly(
                                    transaction_options::ReadOnly {
                                        timestamp_bound: Some(
                                            transaction_options::read_only::TimestampBound::Strong(
                                                true,
                                            ),
                                        ),
                                        ..Default::default()
                                    },
                                )),
                                ..Default::default()
                            },
                        )),
                    }),
                    ..Default::default()
                })
                .await;
            // Catalog sessions never become agent sessions and are deleted after each query.
            let deleted = client
                .delete_session(DeleteSessionRequest { name: session })
                .await;
            let result = result.map_err(|_| unavailable())?.into_inner();
            deleted.map_err(|_| unavailable())?;
            result
                .rows
                .into_iter()
                .map(|r| r.values.into_iter().map(cell).collect())
                .collect()
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cells_are_text_or_null_and_composites_fail_closed() {
        for (kind, expected) in [
            (Kind::StringValue("x".into()), Some("x")),
            (Kind::BoolValue(true), Some("true")),
            (Kind::NumberValue(12.5), Some("12.5")),
            (Kind::NullValue(0), None),
        ] {
            assert_eq!(
                cell(Value { kind: Some(kind) }).unwrap().as_deref(),
                expected
            );
        }
        for kind in [
            Kind::ListValue(Default::default()),
            Kind::StructValue(Default::default()),
            Kind::NumberValue(f64::NAN),
        ] {
            assert_eq!(
                cell(Value { kind: Some(kind) }).unwrap_err().code,
                NyCode::SchemaStale
            );
        }
        assert!(cell(Value::default()).is_err());
    }
}
