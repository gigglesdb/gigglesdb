//! Hand-written tonic 0.12 server for the Spanner API shipped by googleapis 0.16.1.
//!
//! That schema exposes sixteen RPCs. Unsupported and future routes fail closed
//! with UNIMPLEMENTED, rather than being forwarded without policy enforcement.

use std::{
    convert::Infallible,
    sync::Arc,
    task::{Context, Poll},
};

use google_cloud_googleapis::spanner::v1::*;
use tonic::{Request, Response, Status, body::BoxBody, codegen::BoxFuture};

/// Implements the sixteen RPCs in the pinned Spanner API schema.
#[tonic::async_trait]
pub trait SpannerService: Send + Sync + 'static {
    /// Handles the `CreateSession` RPC; the default refuses it.
    async fn create_session(
        &self,
        _request: Request<CreateSessionRequest>,
    ) -> Result<Response<Session>, Status> {
        Err(Status::unimplemented("RPC not implemented"))
    }

    /// Handles the `BatchCreateSessions` RPC; the default refuses it.
    async fn batch_create_sessions(
        &self,
        _request: Request<BatchCreateSessionsRequest>,
    ) -> Result<Response<BatchCreateSessionsResponse>, Status> {
        Err(Status::unimplemented("RPC not implemented"))
    }

    /// Handles the `GetSession` RPC; the default refuses it.
    async fn get_session(
        &self,
        _request: Request<GetSessionRequest>,
    ) -> Result<Response<Session>, Status> {
        Err(Status::unimplemented("RPC not implemented"))
    }

    /// Handles the `ListSessions` RPC; the default refuses it.
    async fn list_sessions(
        &self,
        _request: Request<ListSessionsRequest>,
    ) -> Result<Response<ListSessionsResponse>, Status> {
        Err(Status::unimplemented("RPC not implemented"))
    }

    /// Handles the `DeleteSession` RPC; the default refuses it.
    async fn delete_session(
        &self,
        _request: Request<DeleteSessionRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::unimplemented("RPC not implemented"))
    }

    /// Handles the `ExecuteSql` RPC; the default refuses it.
    async fn execute_sql(
        &self,
        _request: Request<ExecuteSqlRequest>,
    ) -> Result<Response<ResultSet>, Status> {
        Err(Status::unimplemented("RPC not implemented"))
    }

    /// Response stream for `ExecuteStreamingSql`.
    type ExecuteStreamingSqlStream: futures::Stream<Item = Result<PartialResultSet, Status>>
        + Send
        + 'static;

    /// Handles the `ExecuteStreamingSql` RPC; the default refuses it.
    async fn execute_streaming_sql(
        &self,
        _request: Request<ExecuteSqlRequest>,
    ) -> Result<Response<Self::ExecuteStreamingSqlStream>, Status> {
        Err(Status::unimplemented("RPC not implemented"))
    }

    /// Handles the `ExecuteBatchDml` RPC; the default refuses it.
    async fn execute_batch_dml(
        &self,
        _request: Request<ExecuteBatchDmlRequest>,
    ) -> Result<Response<ExecuteBatchDmlResponse>, Status> {
        Err(Status::unimplemented("RPC not implemented"))
    }

    /// Handles the `Read` RPC; the default refuses it.
    async fn read(&self, _request: Request<ReadRequest>) -> Result<Response<ResultSet>, Status> {
        Err(Status::unimplemented("RPC not implemented"))
    }

    /// Response stream for `StreamingRead`.
    type StreamingReadStream: futures::Stream<Item = Result<PartialResultSet, Status>>
        + Send
        + 'static;

    /// Handles the `StreamingRead` RPC; the default refuses it.
    async fn streaming_read(
        &self,
        _request: Request<ReadRequest>,
    ) -> Result<Response<Self::StreamingReadStream>, Status> {
        Err(Status::unimplemented("RPC not implemented"))
    }

    /// Handles the `BeginTransaction` RPC; the default refuses it.
    async fn begin_transaction(
        &self,
        _request: Request<BeginTransactionRequest>,
    ) -> Result<Response<Transaction>, Status> {
        Err(Status::unimplemented("RPC not implemented"))
    }

    /// Handles the `Commit` RPC; the default refuses it.
    async fn commit(
        &self,
        _request: Request<CommitRequest>,
    ) -> Result<Response<CommitResponse>, Status> {
        Err(Status::unimplemented("RPC not implemented"))
    }

    /// Handles the `Rollback` RPC; the default refuses it.
    async fn rollback(&self, _request: Request<RollbackRequest>) -> Result<Response<()>, Status> {
        Err(Status::unimplemented("RPC not implemented"))
    }

    /// Handles the `PartitionQuery` RPC; the default refuses it.
    async fn partition_query(
        &self,
        _request: Request<PartitionQueryRequest>,
    ) -> Result<Response<PartitionResponse>, Status> {
        Err(Status::unimplemented("RPC not implemented"))
    }

    /// Handles the `PartitionRead` RPC; the default refuses it.
    async fn partition_read(
        &self,
        _request: Request<PartitionReadRequest>,
    ) -> Result<Response<PartitionResponse>, Status> {
        Err(Status::unimplemented("RPC not implemented"))
    }

    /// Response stream for `BatchWrite`.
    type BatchWriteStream: futures::Stream<Item = Result<BatchWriteResponse, Status>>
        + Send
        + 'static;

    /// Handles the `BatchWrite` RPC; the default refuses it.
    async fn batch_write(
        &self,
        _request: Request<BatchWriteRequest>,
    ) -> Result<Response<Self::BatchWriteStream>, Status> {
        Err(Status::unimplemented("RPC not implemented"))
    }
}

/// Routes Spanner gRPC requests to a shared service implementation.
pub struct SpannerServer<T: SpannerService> {
    inner: Arc<T>,
}

// A manual implementation avoids imposing T: Clone on the shared handler.
impl<T: SpannerService> Clone for SpannerServer<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T: SpannerService> SpannerServer<T> {
    /// Wraps a service implementation in a Spanner gRPC server.
    pub fn new(inner: T) -> Self {
        Self::from_arc(Arc::new(inner))
    }

    /// Wraps an already shared service implementation.
    pub fn from_arc(inner: Arc<T>) -> Self {
        Self { inner }
    }
}

impl<T: SpannerService> tonic::server::NamedService for SpannerServer<T> {
    const NAME: &'static str = "google.spanner.v1.Spanner";
}

impl<T: SpannerService> tower::Service<http::Request<BoxBody>> for SpannerServer<T> {
    type Response = http::Response<BoxBody>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<BoxBody>) -> Self::Future {
        match request.uri().path() {
            "/google.spanner.v1.Spanner/CreateSession" => {
                struct Rpc<T: SpannerService>(Arc<T>);

                impl<T: SpannerService> tonic::server::UnaryService<CreateSessionRequest> for Rpc<T> {
                    type Response = Session;
                    type Future = BoxFuture<Response<Self::Response>, Status>;

                    fn call(&mut self, request: Request<CreateSessionRequest>) -> Self::Future {
                        let inner = Arc::clone(&self.0);
                        Box::pin(async move { inner.create_session(request).await })
                    }
                }

                let inner = Arc::clone(&self.inner);
                Box::pin(async move {
                    let codec = tonic::codec::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec);
                    Ok(grpc.unary(Rpc(inner), request).await)
                })
            }
            "/google.spanner.v1.Spanner/BatchCreateSessions" => {
                struct Rpc<T: SpannerService>(Arc<T>);

                impl<T: SpannerService> tonic::server::UnaryService<BatchCreateSessionsRequest> for Rpc<T> {
                    type Response = BatchCreateSessionsResponse;
                    type Future = BoxFuture<Response<Self::Response>, Status>;

                    fn call(
                        &mut self,
                        request: Request<BatchCreateSessionsRequest>,
                    ) -> Self::Future {
                        let inner = Arc::clone(&self.0);
                        Box::pin(async move { inner.batch_create_sessions(request).await })
                    }
                }

                let inner = Arc::clone(&self.inner);
                Box::pin(async move {
                    let codec = tonic::codec::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec);
                    Ok(grpc.unary(Rpc(inner), request).await)
                })
            }
            "/google.spanner.v1.Spanner/GetSession" => {
                struct Rpc<T: SpannerService>(Arc<T>);

                impl<T: SpannerService> tonic::server::UnaryService<GetSessionRequest> for Rpc<T> {
                    type Response = Session;
                    type Future = BoxFuture<Response<Self::Response>, Status>;

                    fn call(&mut self, request: Request<GetSessionRequest>) -> Self::Future {
                        let inner = Arc::clone(&self.0);
                        Box::pin(async move { inner.get_session(request).await })
                    }
                }

                let inner = Arc::clone(&self.inner);
                Box::pin(async move {
                    let codec = tonic::codec::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec);
                    Ok(grpc.unary(Rpc(inner), request).await)
                })
            }
            "/google.spanner.v1.Spanner/ListSessions" => {
                struct Rpc<T: SpannerService>(Arc<T>);

                impl<T: SpannerService> tonic::server::UnaryService<ListSessionsRequest> for Rpc<T> {
                    type Response = ListSessionsResponse;
                    type Future = BoxFuture<Response<Self::Response>, Status>;

                    fn call(&mut self, request: Request<ListSessionsRequest>) -> Self::Future {
                        let inner = Arc::clone(&self.0);
                        Box::pin(async move { inner.list_sessions(request).await })
                    }
                }

                let inner = Arc::clone(&self.inner);
                Box::pin(async move {
                    let codec = tonic::codec::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec);
                    Ok(grpc.unary(Rpc(inner), request).await)
                })
            }
            "/google.spanner.v1.Spanner/DeleteSession" => {
                struct Rpc<T: SpannerService>(Arc<T>);

                impl<T: SpannerService> tonic::server::UnaryService<DeleteSessionRequest> for Rpc<T> {
                    type Response = ();
                    type Future = BoxFuture<Response<Self::Response>, Status>;

                    fn call(&mut self, request: Request<DeleteSessionRequest>) -> Self::Future {
                        let inner = Arc::clone(&self.0);
                        Box::pin(async move { inner.delete_session(request).await })
                    }
                }

                let inner = Arc::clone(&self.inner);
                Box::pin(async move {
                    let codec = tonic::codec::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec);
                    Ok(grpc.unary(Rpc(inner), request).await)
                })
            }
            "/google.spanner.v1.Spanner/ExecuteSql" => {
                struct Rpc<T: SpannerService>(Arc<T>);

                impl<T: SpannerService> tonic::server::UnaryService<ExecuteSqlRequest> for Rpc<T> {
                    type Response = ResultSet;
                    type Future = BoxFuture<Response<Self::Response>, Status>;

                    fn call(&mut self, request: Request<ExecuteSqlRequest>) -> Self::Future {
                        let inner = Arc::clone(&self.0);
                        Box::pin(async move { inner.execute_sql(request).await })
                    }
                }

                let inner = Arc::clone(&self.inner);
                Box::pin(async move {
                    let codec = tonic::codec::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec);
                    Ok(grpc.unary(Rpc(inner), request).await)
                })
            }
            "/google.spanner.v1.Spanner/ExecuteStreamingSql" => {
                struct Rpc<T: SpannerService>(Arc<T>);

                impl<T: SpannerService> tonic::server::ServerStreamingService<ExecuteSqlRequest> for Rpc<T> {
                    type Response = PartialResultSet;
                    type ResponseStream = T::ExecuteStreamingSqlStream;
                    type Future = BoxFuture<Response<Self::ResponseStream>, Status>;

                    fn call(&mut self, request: Request<ExecuteSqlRequest>) -> Self::Future {
                        let inner = Arc::clone(&self.0);
                        Box::pin(async move { inner.execute_streaming_sql(request).await })
                    }
                }

                let inner = Arc::clone(&self.inner);
                Box::pin(async move {
                    let codec = tonic::codec::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec);
                    Ok(grpc.server_streaming(Rpc(inner), request).await)
                })
            }
            "/google.spanner.v1.Spanner/ExecuteBatchDml" => {
                struct Rpc<T: SpannerService>(Arc<T>);

                impl<T: SpannerService> tonic::server::UnaryService<ExecuteBatchDmlRequest> for Rpc<T> {
                    type Response = ExecuteBatchDmlResponse;
                    type Future = BoxFuture<Response<Self::Response>, Status>;

                    fn call(&mut self, request: Request<ExecuteBatchDmlRequest>) -> Self::Future {
                        let inner = Arc::clone(&self.0);
                        Box::pin(async move { inner.execute_batch_dml(request).await })
                    }
                }

                let inner = Arc::clone(&self.inner);
                Box::pin(async move {
                    let codec = tonic::codec::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec);
                    Ok(grpc.unary(Rpc(inner), request).await)
                })
            }
            "/google.spanner.v1.Spanner/Read" => {
                struct Rpc<T: SpannerService>(Arc<T>);

                impl<T: SpannerService> tonic::server::UnaryService<ReadRequest> for Rpc<T> {
                    type Response = ResultSet;
                    type Future = BoxFuture<Response<Self::Response>, Status>;

                    fn call(&mut self, request: Request<ReadRequest>) -> Self::Future {
                        let inner = Arc::clone(&self.0);
                        Box::pin(async move { inner.read(request).await })
                    }
                }

                let inner = Arc::clone(&self.inner);
                Box::pin(async move {
                    let codec = tonic::codec::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec);
                    Ok(grpc.unary(Rpc(inner), request).await)
                })
            }
            "/google.spanner.v1.Spanner/StreamingRead" => {
                struct Rpc<T: SpannerService>(Arc<T>);

                impl<T: SpannerService> tonic::server::ServerStreamingService<ReadRequest> for Rpc<T> {
                    type Response = PartialResultSet;
                    type ResponseStream = T::StreamingReadStream;
                    type Future = BoxFuture<Response<Self::ResponseStream>, Status>;

                    fn call(&mut self, request: Request<ReadRequest>) -> Self::Future {
                        let inner = Arc::clone(&self.0);
                        Box::pin(async move { inner.streaming_read(request).await })
                    }
                }

                let inner = Arc::clone(&self.inner);
                Box::pin(async move {
                    let codec = tonic::codec::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec);
                    Ok(grpc.server_streaming(Rpc(inner), request).await)
                })
            }
            "/google.spanner.v1.Spanner/BeginTransaction" => {
                struct Rpc<T: SpannerService>(Arc<T>);

                impl<T: SpannerService> tonic::server::UnaryService<BeginTransactionRequest> for Rpc<T> {
                    type Response = Transaction;
                    type Future = BoxFuture<Response<Self::Response>, Status>;

                    fn call(&mut self, request: Request<BeginTransactionRequest>) -> Self::Future {
                        let inner = Arc::clone(&self.0);
                        Box::pin(async move { inner.begin_transaction(request).await })
                    }
                }

                let inner = Arc::clone(&self.inner);
                Box::pin(async move {
                    let codec = tonic::codec::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec);
                    Ok(grpc.unary(Rpc(inner), request).await)
                })
            }
            "/google.spanner.v1.Spanner/Commit" => {
                struct Rpc<T: SpannerService>(Arc<T>);

                impl<T: SpannerService> tonic::server::UnaryService<CommitRequest> for Rpc<T> {
                    type Response = CommitResponse;
                    type Future = BoxFuture<Response<Self::Response>, Status>;

                    fn call(&mut self, request: Request<CommitRequest>) -> Self::Future {
                        let inner = Arc::clone(&self.0);
                        Box::pin(async move { inner.commit(request).await })
                    }
                }

                let inner = Arc::clone(&self.inner);
                Box::pin(async move {
                    let codec = tonic::codec::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec);
                    Ok(grpc.unary(Rpc(inner), request).await)
                })
            }
            "/google.spanner.v1.Spanner/Rollback" => {
                struct Rpc<T: SpannerService>(Arc<T>);

                impl<T: SpannerService> tonic::server::UnaryService<RollbackRequest> for Rpc<T> {
                    type Response = ();
                    type Future = BoxFuture<Response<Self::Response>, Status>;

                    fn call(&mut self, request: Request<RollbackRequest>) -> Self::Future {
                        let inner = Arc::clone(&self.0);
                        Box::pin(async move { inner.rollback(request).await })
                    }
                }

                let inner = Arc::clone(&self.inner);
                Box::pin(async move {
                    let codec = tonic::codec::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec);
                    Ok(grpc.unary(Rpc(inner), request).await)
                })
            }
            "/google.spanner.v1.Spanner/PartitionQuery" => {
                struct Rpc<T: SpannerService>(Arc<T>);

                impl<T: SpannerService> tonic::server::UnaryService<PartitionQueryRequest> for Rpc<T> {
                    type Response = PartitionResponse;
                    type Future = BoxFuture<Response<Self::Response>, Status>;

                    fn call(&mut self, request: Request<PartitionQueryRequest>) -> Self::Future {
                        let inner = Arc::clone(&self.0);
                        Box::pin(async move { inner.partition_query(request).await })
                    }
                }

                let inner = Arc::clone(&self.inner);
                Box::pin(async move {
                    let codec = tonic::codec::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec);
                    Ok(grpc.unary(Rpc(inner), request).await)
                })
            }
            "/google.spanner.v1.Spanner/PartitionRead" => {
                struct Rpc<T: SpannerService>(Arc<T>);

                impl<T: SpannerService> tonic::server::UnaryService<PartitionReadRequest> for Rpc<T> {
                    type Response = PartitionResponse;
                    type Future = BoxFuture<Response<Self::Response>, Status>;

                    fn call(&mut self, request: Request<PartitionReadRequest>) -> Self::Future {
                        let inner = Arc::clone(&self.0);
                        Box::pin(async move { inner.partition_read(request).await })
                    }
                }

                let inner = Arc::clone(&self.inner);
                Box::pin(async move {
                    let codec = tonic::codec::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec);
                    Ok(grpc.unary(Rpc(inner), request).await)
                })
            }
            "/google.spanner.v1.Spanner/BatchWrite" => {
                struct Rpc<T: SpannerService>(Arc<T>);

                impl<T: SpannerService> tonic::server::ServerStreamingService<BatchWriteRequest> for Rpc<T> {
                    type Response = BatchWriteResponse;
                    type ResponseStream = T::BatchWriteStream;
                    type Future = BoxFuture<Response<Self::ResponseStream>, Status>;

                    fn call(&mut self, request: Request<BatchWriteRequest>) -> Self::Future {
                        let inner = Arc::clone(&self.0);
                        Box::pin(async move { inner.batch_write(request).await })
                    }
                }

                let inner = Arc::clone(&self.inner);
                Box::pin(async move {
                    let codec = tonic::codec::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec);
                    Ok(grpc.server_streaming(Rpc(inner), request).await)
                })
            }
            _ => Box::pin(async {
                let mut response = http::Response::new(tonic::body::empty_body());
                response.headers_mut().insert(
                    http::header::CONTENT_TYPE,
                    http::HeaderValue::from_static("application/grpc"),
                );
                response
                    .headers_mut()
                    .insert("grpc-status", http::HeaderValue::from_static("12"));
                Ok(response)
            }),
        }
    }
}
