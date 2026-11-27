//! Bounded receive buffers and ownership carried through response delivery.
use super::{
    dispatch::{CallOwnership, ResponseLease},
    error::ApiError,
    identity::ConnectionIdentity,
    net::{ActivityTicket, FlushReceipt, FlushTracker},
};
use crate::Service;
use bytes::{Buf, Bytes, BytesMut};
use http_body::{Body, Frame, SizeHint};
use http_body_util::BodyExt;
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Instant,
};

pub(crate) async fn collect<B>(body: B, limit: usize, deadline: Instant) -> Result<Bytes, ApiError>
where
    B: Body<Data = Bytes> + Send + 'static,
{
    let read = async move {
        if Instant::now() >= deadline {
            return Err(ApiError::DeadlineExceeded);
        }
        let mut body = Box::pin(body);
        let mut bytes = BytesMut::new();
        while let Some(frame) = body.frame().await {
            if Instant::now() >= deadline {
                return Err(ApiError::DeadlineExceeded);
            }
            let frame = frame.map_err(|_| ApiError::InvalidArgument)?;
            match frame.into_data() {
                Ok(data) => {
                    if data.len() > limit.saturating_sub(bytes.len()) {
                        return Err(ApiError::PayloadTooLarge);
                    }
                    bytes.extend_from_slice(&data);
                }
                Err(frame) => {
                    if let Ok(trailers) = frame.into_trailers() {
                        super::net::validate_headers(&trailers)?;
                        if [
                            "x-giggles-session",
                            "grpc-timeout",
                            "content-type",
                            "content-encoding",
                        ]
                        .iter()
                        .any(|name| trailers.contains_key(*name))
                        {
                            return Err(ApiError::InvalidArgument);
                        }
                    }
                }
            }
        }
        if Instant::now() >= deadline {
            return Err(ApiError::DeadlineExceeded);
        }
        Ok(bytes.freeze())
    };
    tokio::time::timeout_at(deadline.into(), read)
        .await
        .map_err(|_| ApiError::DeadlineExceeded)?
}

/// The body and queued bytes retain delivery admission. HTTP releases the
/// session before its final DATA slice; other protocols retain the response lease.
pub(crate) struct ResponseBody {
    inner: tonic::body::Body,
    pending: Bytes,
    delivery: Option<Arc<Mutex<DeliveryOwnership>>>,
    done: bool,
    http_completion: bool,
    inner_done: bool,
    next: Option<Result<Frame<Bytes>, tonic::Status>>,
    transport: bool,
    final_data: bool,
    flush_tracker: Option<Arc<FlushTracker>>,
    last_data: Option<Arc<FlushReceipt>>,
}

struct DeliveryOwnership {
    _ownership: Option<Arc<CallOwnership>>,
    lease: Option<ResponseLease>,
    binding: Option<(ConnectionIdentity, Arc<Service>)>,
    ticket: ActivityTicket,
}

// Hyper may drop a Body after enqueueing its final frame, before h2 has written
// that frame. Bytes' owner follows the frame through both protocol send queues.
struct DeliveryChunk {
    bytes: Bytes,
    _delivery: Arc<Mutex<DeliveryOwnership>>,
}

impl AsRef<[u8]> for DeliveryChunk {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl ResponseBody {
    pub(crate) fn new(
        inner: tonic::body::Body,
        ownership: Option<Arc<CallOwnership>>,
        lease: Option<ResponseLease>,
        ticket: ActivityTicket,
    ) -> Self {
        ticket.write_stage();
        Self {
            inner,
            pending: Bytes::new(),
            delivery: Some(Arc::new(Mutex::new(DeliveryOwnership {
                _ownership: ownership,
                lease,
                binding: None,
                ticket,
            }))),
            done: false,
            http_completion: false,
            inner_done: false,
            next: None,
            transport: false,
            final_data: false,
            flush_tracker: None,
            last_data: None,
        }
    }

    pub(crate) fn new_http(
        inner: tonic::body::Body,
        ownership: Option<Arc<CallOwnership>>,
        lease: Option<ResponseLease>,
        ticket: ActivityTicket,
    ) -> Self {
        let mut body = Self::new(inner, ownership, lease, ticket);
        body.http_completion = true;
        body
    }

    pub(crate) fn guard_binding(&mut self, identity: ConnectionIdentity, service: Arc<Service>) {
        self.delivery
            .as_ref()
            .expect("live delivery")
            .lock()
            .expect("delivery ownership lock")
            .binding = Some((identity, service));
    }

    fn finish(&mut self) {
        self.done = true;
        self.delivery.take();
    }

    fn release_session(&mut self) {
        if let Some(delivery) = &self.delivery {
            delivery
                .lock()
                .expect("delivery ownership lock")
                .lease
                .take();
        }
    }

    fn chunk(&mut self) -> Result<Frame<Bytes>, tonic::Status> {
        // Revalidate the retained session owner on every delivered chunk, so
        // role revocation also stops an already streaming response.
        {
            let checked = self
                .delivery
                .as_ref()
                .expect("live delivery")
                .lock()
                .map_err(|_| ApiError::Internal)
                .and_then(|delivery| {
                    if let Some((identity, service)) = &delivery.binding {
                        identity.check(service)?;
                    }
                    delivery
                        .lease
                        .as_ref()
                        .map_or(Ok(()), ResponseLease::check_release)
                });
            if let Err(error) = checked {
                self.pending = Bytes::new();
                self.finish();
                return Err(error.grpc_status());
            }
        }
        self.delivery
            .as_ref()
            .expect("live delivery")
            .lock()
            .map_err(|_| ApiError::Internal.grpc_status())?
            .ticket
            .write_stage();
        let final_slice =
            self.http_completion && self.inner_done && self.pending.len() <= 16 * 1024;
        self.final_data = final_slice;
        if final_slice && !self.transport {
            // Session state is no longer used by the producer. Restore it before
            // a client can observe the complete payload, even if Hyper retains
            // this Body and every queued DeliveryChunk indefinitely.
            self.release_session();
        }
        let bytes = self.pending.split_to(self.pending.len().min(16 * 1024));
        Ok(Frame::data(Bytes::from_owner(DeliveryChunk {
            bytes,
            _delivery: self.delivery.as_ref().expect("live delivery").clone(),
        })))
    }
}

impl Body for ResponseBody {
    type Data = Bytes;
    type Error = tonic::Status;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        loop {
            if !this.pending.is_empty() {
                // HTTP JSON has no trailers. A one-frame lookahead establishes
                // EOF for unknown-length streams before their final DATA slice
                // escapes. Never buffer more than one producer frame ahead.
                if this.http_completion
                    && this.pending.len() <= 16 * 1024
                    && !this.inner_done
                    && this.next.is_none()
                {
                    match Pin::new(&mut this.inner).poll_frame(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(None) => this.inner_done = true,
                        Poll::Ready(Some(Ok(frame)))
                            if frame.data_ref().is_some_and(Bytes::is_empty) =>
                        {
                            // Ignore empty DATA while proving EOF, with bounded work per poll.
                            cx.waker().wake_by_ref();
                            return Poll::Pending;
                        }
                        Poll::Ready(Some(frame)) => this.next = Some(frame),
                    }
                }
                return Poll::Ready(Some(this.chunk()));
            }
            let frame = if let Some(frame) = this.next.take() {
                Poll::Ready(Some(frame))
            } else if this.inner_done {
                Poll::Ready(None)
            } else {
                Pin::new(&mut this.inner).poll_frame(cx)
            };
            match frame {
                Poll::Ready(None) => {
                    if this.http_completion && !this.transport {
                        this.release_session();
                    }
                    this.finish();
                    return Poll::Ready(None);
                }
                Poll::Ready(Some(Err(error))) => {
                    // Buf consumption alone leaves bytes in the protocol codec.
                    // Defer the terminal error until the connection has flushed
                    // after consuming the last preceding DATA in full.
                    if let Some(receipt) = &this.last_data
                        && receipt.poll_flushed(cx).is_pending()
                    {
                        this.next = Some(Err(error));
                        return Poll::Pending;
                    }
                    this.finish();
                    return Poll::Ready(Some(Err(error)));
                }
                Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                    Ok(bytes) if !bytes.is_empty() => this.pending = bytes,
                    Ok(bytes) => return Poll::Ready(Some(Ok(Frame::data(bytes)))),
                    Err(frame) => return Poll::Ready(Some(Ok(frame))),
                },
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.done
    }
    fn size_hint(&self) -> SizeHint {
        if self.done {
            return SizeHint::with_exact(0);
        }
        let inner = self.inner.size_hint();
        let buffered = self.pending.len() as u64
            + self
                .next
                .as_ref()
                .and_then(|frame| frame.as_ref().ok())
                .and_then(Frame::data_ref)
                .map_or(0, |bytes| bytes.len() as u64);
        let mut hint = SizeHint::new();
        hint.set_lower(inner.lower().saturating_add(buffered));
        if let Some(upper) = inner.upper().and_then(|upper| upper.checked_add(buffered)) {
            hint.set_upper(upper);
        }
        hint
    }
}

/// Hyper consumes Buf only as protocol capacity accepts DATA. In particular h2
/// can enqueue a complete frame while consuming only its flow-control window.
/// Keeping the lease in the final Buf prevents producer EOF from releasing it.
pub(crate) struct TransportBody(ResponseBody);

pub(crate) struct TransportData {
    bytes: Bytes,
    completion: Option<Arc<Mutex<DeliveryOwnership>>>,
    flush: Option<(Arc<FlushTracker>, Arc<FlushReceipt>)>,
}

impl Buf for TransportData {
    fn remaining(&self) -> usize {
        self.bytes.remaining()
    }
    fn chunk(&self) -> &[u8] {
        self.bytes.chunk()
    }
    fn advance(&mut self, count: usize) {
        self.bytes.advance(count);
        if self.bytes.is_empty()
            && let Some((tracker, receipt)) = self.flush.take()
        {
            tracker.consumed(receipt);
        }
        if self.bytes.is_empty()
            && let Some(delivery) = self.completion.take()
        {
            delivery
                .lock()
                .expect("delivery ownership lock")
                .lease
                .take();
        }
    }
}

impl ResponseBody {
    pub(crate) fn with_flush_tracker(mut self, tracker: Arc<FlushTracker>) -> Self {
        self.flush_tracker = Some(tracker);
        self
    }

    pub(crate) fn into_transport(mut self) -> TransportBody {
        self.transport = true;
        TransportBody(self)
    }
}

impl Body for TransportBody {
    type Data = TransportData;
    type Error = tonic::Status;
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        this.0.final_data = false;
        let frame = std::task::ready!(Pin::new(&mut this.0).poll_frame(cx));
        let completion = if this.0.final_data {
            this.0.delivery.clone()
        } else {
            None
        };
        Poll::Ready(frame.map(|frame| {
            frame.map(|frame| {
                frame.map_data(|bytes| {
                    let flush = if !bytes.is_empty() && this.0.http_completion {
                        this.0.flush_tracker.as_ref().map(|tracker| {
                            let receipt = Arc::new(FlushReceipt::default());
                            this.0.last_data = Some(receipt.clone());
                            (tracker.clone(), receipt)
                        })
                    } else {
                        None
                    };
                    TransportData {
                        bytes,
                        completion,
                        flush,
                    }
                })
            })
        }))
    }
    fn is_end_stream(&self) -> bool {
        self.0.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.0.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::super::{ApiLimits, ApiRuntime, json};
    use super::*;
    use crate::{Binding, Role, admission::ServiceLimits, clock::Clock};
    use giggles_core::{Admin, Limits};
    use http_body_util::Full;
    use std::{
        collections::BTreeSet,
        sync::{
            Weak,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        task::Waker,
        time::{SystemTime, UNIX_EPOCH},
    };

    static NEXT_FIXTURE: AtomicUsize = AtomicUsize::new(0);

    struct ReleaseFixture {
        runtime: Arc<ApiRuntime>,
        identity: ConnectionIdentity,
        binding: Binding,
    }

    impl ReleaseFixture {
        fn new() -> Self {
            Self::with_connections(1)
        }

        fn with_connections(connections: usize) -> Self {
            let path = crate::test_support::fixture_root().join(format!(
                "api-body-{}-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&path).unwrap();
            let admin = Admin::create(path.join("db.redb"), Limits::default()).unwrap();
            admin
                .sql_session()
                .execute("CREATE TABLE app (id BIGINT PRIMARY KEY)", &[], None)
                .unwrap();
            let binding = Binding {
                fingerprint: "33".repeat(32),
                role: Role::Application {
                    domain: admin.authority_metadata().unwrap().0.to_string(),
                    tables: BTreeSet::from(["app".into()]),
                },
            };
            let service = Service::with_limits(
                admin,
                vec![binding.clone()],
                Clock::new(0, 3600).unwrap(),
                ServiceLimits {
                    sessions: connections,
                    ..ServiceLimits::default()
                },
            )
            .unwrap();
            let runtime = ApiRuntime::new(service, ApiLimits::default()).unwrap();
            let identity = runtime.authenticate(&binding.fingerprint).unwrap();
            Self {
                runtime,
                identity,
                binding,
            }
        }

        fn payload(&self) -> Bytes {
            Bytes::from(json::encode_open(&self.runtime.open(&self.identity).unwrap()).unwrap())
        }

        fn body(&self, inner: tonic::body::Body) -> (ResponseBody, Weak<CallOwnership>) {
            let service = self.runtime.service();
            let call = self
                .runtime
                .admit_call(service.admission.connect().unwrap())
                .unwrap();
            let weak = Arc::downgrade(&call);
            let activity = Arc::new(super::super::net::ConnectionActivity::new());
            let mut body = ResponseBody::new(inner, Some(call), None, activity.begin());
            body.guard_binding(self.identity.clone(), service.clone());
            (body, weak)
        }

        fn remove_readd(&self) {
            self.runtime.service().reload_bindings(vec![]).unwrap();
            self.runtime
                .service()
                .reload_bindings(vec![self.binding.clone()])
                .unwrap();
        }
    }

    async fn execute_ping(
        f: &ReleaseFixture,
        token: &str,
    ) -> Result<super::super::dispatch::Completion, ApiError> {
        let service = f.runtime.service();
        f.runtime
            .execute(
                &f.identity,
                token,
                super::super::proto::ExecuteRequest {
                    request: Some(super::super::proto::execute_request::Request::Ping(
                        super::super::proto::PingRequest {},
                    )),
                    ..Default::default()
                },
                Instant::now(),
                None,
                f.runtime
                    .admit_call(service.admission.connect().unwrap())
                    .unwrap(),
                super::super::dispatch::Encoding::Json,
                true,
            )
            .await
    }

    async fn session_body(f: &ReleaseFixture, inner: tonic::body::Body) -> (String, ResponseBody) {
        let token = f.runtime.open(&f.identity).unwrap().session_id;
        let completion = execute_ping(f, &token).await.unwrap();
        let activity = Arc::new(super::super::net::ConnectionActivity::new());
        (
            token,
            ResponseBody::new_http(inner, None, Some(completion.lease), activity.begin()),
        )
    }

    async fn assert_busy(f: &ReleaseFixture, token: &str) {
        let error = execute_ping(f, token).await.err().unwrap();
        assert_eq!(error, ApiError::SessionBusy);
        assert_eq!(error.http_status(), 409);
    }

    #[tokio::test]
    async fn http_final_slice_releases_session_with_all_delivery_frames_retained() {
        for len in [1, 16 * 1024, 16 * 1024 + 1, 50000] {
            let f = ReleaseFixture::with_connections(8);
            let (token, mut body) = session_body(
                &f,
                tonic::body::Body::new(Full::new(Bytes::from(vec![7; len]))),
            )
            .await;
            let delivery = Arc::downgrade(body.delivery.as_ref().unwrap());
            let mut frames = Vec::new();
            let mut received = 0;
            while received < len {
                assert_busy(&f, &token).await;
                let frame = body.frame().await.unwrap().unwrap().into_data().unwrap();
                received += frame.len();
                frames.push(frame);
            }
            // Neither a subsequent EOF poll nor Body/queued-frame cleanup has occurred.
            drop(
                execute_ping(&f, &token)
                    .await
                    .expect("complete HTTP payload must release session"),
            );
            assert!(delivery.upgrade().is_some());
            drop(body);
            assert!(delivery.upgrade().is_some());
            drop(frames);
            assert!(delivery.upgrade().is_none());
            f.runtime.shutdown().await;
        }
    }

    #[tokio::test]
    async fn http_stream_final_boundary_waits_for_eof_and_retains_concurrent_exclusion() {
        let f = ReleaseFixture::with_connections(8);
        let (sender, receiver) =
            tokio::sync::mpsc::channel::<Result<Frame<Bytes>, tonic::Status>>(4);
        let stream = tonic::codegen::tokio_stream::wrappers::ReceiverStream::new(receiver);
        let (token, mut body) = session_body(
            &f,
            tonic::body::Body::new(http_body_util::StreamBody::new(stream)),
        )
        .await;
        sender
            .send(Ok(Frame::data(Bytes::from(vec![1; 16 * 1024 + 1]))))
            .await
            .unwrap();
        let first = body.frame().await.unwrap().unwrap();
        assert_busy(&f, &token).await;
        // The final one-byte slice cannot escape while the producer is still live.
        assert!(
            Pin::new(&mut body)
                .poll_frame(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert_busy(&f, &token).await;
        drop(sender);
        let last = body.frame().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(last.len(), 1);
        drop(execute_ping(&f, &token).await.unwrap());
        drop((first, last, body));
        f.runtime.shutdown().await;
    }

    #[tokio::test]
    async fn http_stream_error_and_cancellation_keep_queued_cleanup_ownership() {
        for fail in [false, true] {
            let f = ReleaseFixture::with_connections(8);
            let (sender, receiver) =
                tokio::sync::mpsc::channel::<Result<Frame<Bytes>, tonic::Status>>(4);
            let stream = tonic::codegen::tokio_stream::wrappers::ReceiverStream::new(receiver);
            let (token, mut body) = session_body(
                &f,
                tonic::body::Body::new(http_body_util::StreamBody::new(stream)),
            )
            .await;
            sender
                .send(Ok(Frame::data(Bytes::from(vec![1; 16 * 1024 + 1]))))
                .await
                .unwrap();
            let first = body.frame().await.unwrap().unwrap();
            if fail {
                sender
                    .send(Err(tonic::Status::internal("producer failed")))
                    .await
                    .unwrap();
                let last = body.frame().await.unwrap().unwrap();
                assert_busy(&f, &token).await;
                assert!(body.frame().await.unwrap().is_err());
                drop(last);
            }
            drop(body);
            assert_busy(&f, &token).await;
            drop(first);
            drop(execute_ping(&f, &token).await.unwrap());
            drop(sender);
            f.runtime.shutdown().await;
        }
    }

    #[tokio::test]
    async fn http_final_slice_rechecks_authority_after_pending_lookahead() {
        let f = ReleaseFixture::with_connections(8);
        let (sender, receiver) =
            tokio::sync::mpsc::channel::<Result<Frame<Bytes>, tonic::Status>>(4);
        let stream = tonic::codegen::tokio_stream::wrappers::ReceiverStream::new(receiver);
        let (_, mut body) = session_body(
            &f,
            tonic::body::Body::new(http_body_util::StreamBody::new(stream)),
        )
        .await;
        sender
            .send(Ok(Frame::data(Bytes::from(vec![1; 16 * 1024 + 1]))))
            .await
            .unwrap();
        let first = body.frame().await.unwrap().unwrap();
        assert!(
            Pin::new(&mut body)
                .poll_frame(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        f.remove_readd();
        drop(sender);
        assert_eq!(
            body.frame().await.unwrap().unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
        drop((first, body));
        f.runtime.shutdown().await;
    }

    #[tokio::test]
    async fn transport_final_data_waits_for_full_buf_consumption_even_after_eof() {
        for len in [1, 16 * 1024, 16 * 1024 + 1] {
            let f = ReleaseFixture::with_connections(8);
            let (token, body) = session_body(
                &f,
                tonic::body::Body::new(Full::new(Bytes::from(vec![7; len]))),
            )
            .await;
            let delivery = Arc::downgrade(body.delivery.as_ref().unwrap());
            let mut body = body.into_transport();
            let mut frames = Vec::new();
            let mut received = 0;
            while received < len {
                let data = body
                    .frame()
                    .await
                    .unwrap()
                    .unwrap()
                    .into_data()
                    .ok()
                    .unwrap();
                received += data.remaining();
                frames.push(data);
            }
            // Hyper may poll EOF and discard the Body while DATA remains queued.
            assert!(body.frame().await.is_none());
            assert_busy(&f, &token).await;
            let mut last = frames.pop().unwrap();
            drop(body);
            // Model h2's flow-control-limited Take<Buf>, including a zero advance.
            last.advance(0);
            assert_busy(&f, &token).await;
            let partial = last.remaining() - 1;
            let mut window = (&mut last).take(partial);
            window.advance(partial);
            assert_eq!(last.remaining(), 1);
            assert_busy(&f, &token).await;
            last.advance(1);
            drop(
                execute_ping(&f, &token)
                    .await
                    .expect("protocol accepted final DATA"),
            );
            // Delivery admission still follows queued bytes after session release.
            assert!(delivery.upgrade().is_some());
            drop((last, frames));
            assert!(delivery.upgrade().is_none());
            f.runtime.shutdown().await;
        }
    }

    #[tokio::test]
    async fn transport_cancelled_partial_data_keeps_lease_until_last_owner_drops() {
        for body_first in [false, true] {
            let f = ReleaseFixture::with_connections(8);
            let (token, body) = session_body(
                &f,
                tonic::body::Body::new(Full::new(Bytes::from_static(b"payload"))),
            )
            .await;
            let delivery = Arc::downgrade(body.delivery.as_ref().unwrap());
            let mut body = body.into_transport();
            let mut data = body
                .frame()
                .await
                .unwrap()
                .unwrap()
                .into_data()
                .ok()
                .unwrap();
            data.advance(1);
            assert_busy(&f, &token).await;
            if body_first {
                drop(body);
                assert_busy(&f, &token).await;
                drop(data);
            } else {
                drop(data);
                assert_busy(&f, &token).await;
                drop(body);
            }
            assert!(delivery.upgrade().is_none());
            drop(
                execute_ping(&f, &token)
                    .await
                    .expect("cancelled delivery cleaned up"),
            );
            f.runtime.shutdown().await;
        }
    }

    #[tokio::test]
    async fn transport_error_data_consumption_does_not_signal_success() {
        let f = ReleaseFixture::with_connections(8);
        let (sender, receiver) =
            tokio::sync::mpsc::channel::<Result<Frame<Bytes>, tonic::Status>>(4);
        let stream = tonic::codegen::tokio_stream::wrappers::ReceiverStream::new(receiver);
        let (token, body) = session_body(
            &f,
            tonic::body::Body::new(http_body_util::StreamBody::new(stream)),
        )
        .await;
        let delivery = Arc::downgrade(body.delivery.as_ref().unwrap());
        let mut body = body.into_transport();
        sender
            .send(Ok(Frame::data(Bytes::from_static(b"diagnostic"))))
            .await
            .unwrap();
        sender
            .send(Err(tonic::Status::internal("producer failed")))
            .await
            .unwrap();
        let mut data = body
            .frame()
            .await
            .unwrap()
            .unwrap()
            .into_data()
            .ok()
            .unwrap();
        data.advance(data.remaining());
        assert_busy(&f, &token).await;
        assert!(body.frame().await.unwrap().is_err());
        assert_busy(&f, &token).await;
        drop(body);
        assert_busy(&f, &token).await;
        drop(data);
        assert!(delivery.upgrade().is_none());
        drop(execute_ping(&f, &token).await.unwrap());
        drop(sender);
        f.runtime.shutdown().await;
    }

    struct ExactFrames(std::collections::VecDeque<Bytes>);
    impl Body for ExactFrames {
        type Data = Bytes;
        type Error = std::convert::Infallible;
        fn poll_frame(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
            Poll::Ready(self.0.pop_front().map(|bytes| Ok(Frame::data(bytes))))
        }
        fn size_hint(&self) -> SizeHint {
            SizeHint::with_exact(self.0.iter().map(|bytes| bytes.len() as u64).sum())
        }
    }

    #[tokio::test]
    async fn http_size_hint_includes_prefetched_data() {
        let activity = Arc::new(super::super::net::ConnectionActivity::new());
        let inner = ExactFrames([Bytes::from_static(b"a"), Bytes::from_static(b"bcd")].into());
        let mut body =
            ResponseBody::new_http(tonic::body::Body::new(inner), None, None, activity.begin());
        assert_eq!(body.size_hint().exact(), Some(4));
        assert_eq!(
            body.frame().await.unwrap().unwrap().into_data().unwrap(),
            "a"
        );
        assert_eq!(body.size_hint().exact(), Some(3));
        assert_eq!(
            body.frame().await.unwrap().unwrap().into_data().unwrap(),
            "bcd"
        );
        assert_eq!(body.size_hint().exact(), Some(0));
        assert!(body.frame().await.is_none());
    }

    #[tokio::test]
    async fn http_trailing_empty_data_proves_eof_before_final_payload() {
        let f = ReleaseFixture::with_connections(8);
        let (sender, receiver) =
            tokio::sync::mpsc::channel::<Result<Frame<Bytes>, tonic::Status>>(4);
        let stream = tonic::codegen::tokio_stream::wrappers::ReceiverStream::new(receiver);
        let (token, mut body) = session_body(
            &f,
            tonic::body::Body::new(http_body_util::StreamBody::new(stream)),
        )
        .await;
        sender
            .send(Ok(Frame::data(Bytes::from_static(b"payload"))))
            .await
            .unwrap();
        sender.send(Ok(Frame::data(Bytes::new()))).await.unwrap();
        drop(sender);
        let payload = body.frame().await.unwrap().unwrap();
        drop(
            execute_ping(&f, &token)
                .await
                .expect("empty trailing DATA must not delay release"),
        );
        drop((payload, body));
        f.runtime.shutdown().await;
    }

    #[tokio::test]
    async fn http_trailers_preserve_order_and_session_until_eof() {
        let f = ReleaseFixture::with_connections(8);
        let (sender, receiver) =
            tokio::sync::mpsc::channel::<Result<Frame<Bytes>, tonic::Status>>(4);
        let stream = tonic::codegen::tokio_stream::wrappers::ReceiverStream::new(receiver);
        let (token, mut body) = session_body(
            &f,
            tonic::body::Body::new(http_body_util::StreamBody::new(stream)),
        )
        .await;
        sender
            .send(Ok(Frame::data(Bytes::from_static(b"payload"))))
            .await
            .unwrap();
        sender
            .send(Ok(Frame::trailers(http::HeaderMap::new())))
            .await
            .unwrap();
        drop(sender);
        let data = body.frame().await.unwrap().unwrap();
        assert_busy(&f, &token).await;
        assert!(body.frame().await.unwrap().unwrap().is_trailers());
        assert_busy(&f, &token).await;
        assert!(body.frame().await.is_none());
        drop(execute_ping(&f, &token).await.unwrap());
        drop((data, body));
        f.runtime.shutdown().await;
    }

    #[tokio::test]
    async fn open_reply_withholds_secret_after_remove_readd_and_releases_permits() {
        let fixture = ReleaseFixture::new();
        let (mut body, call) = fixture.body(tonic::body::Body::new(Full::new(fixture.payload())));
        assert!(fixture.runtime.service().admission.connect().is_none());
        fixture.remove_readd();
        let error = body.frame().await.unwrap().unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unauthenticated);
        assert!(body.frame().await.is_none());
        assert!(call.upgrade().is_none());
        assert!(fixture.runtime.service().admission.connect().is_some());
        fixture.runtime.shutdown().await;
    }

    #[tokio::test]
    async fn unchanged_binding_releases_open_payload_with_queued_byte_ownership() {
        let fixture = ReleaseFixture::new();
        let payload = fixture.payload();
        let (mut body, call) = fixture.body(tonic::body::Body::new(Full::new(payload.clone())));
        fixture
            .runtime
            .service()
            .reload_bindings(vec![fixture.binding.clone()])
            .unwrap();
        let data = body.frame().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(data, payload);
        drop(body);
        assert!(call.upgrade().is_some(), "queued bytes retain the call");
        assert!(fixture.runtime.service().admission.connect().is_none());
        drop(data);
        assert!(call.upgrade().is_none());
        assert!(fixture.runtime.service().admission.connect().is_some());
        fixture.runtime.shutdown().await;
    }

    #[tokio::test]
    async fn role_revocation_stops_stream_after_first_chunk() {
        let fixture = ReleaseFixture::new();
        let service = fixture.runtime.service();
        service
            .admin
            .roles()
            .create(giggles_core::roles::NewRole {
                name: "stream_role".into(),
                login: true,
                ..Default::default()
            })
            .unwrap();
        let config: crate::auth::RoleApiConfig = serde_json::from_value(serde_json::json!({
            "http":"127.0.0.1:0", "tls":"disabled"
        }))
        .unwrap();
        let listener = Arc::new(config.build().unwrap());
        let mut session = crate::auth::signin::open_role_session(service, "stream_role").unwrap();
        session.database = Some("postgres".into());
        let owner = ConnectionIdentity::role_signed_in(&session, listener);
        let (mut body, _) = fixture.body(tonic::body::Body::new(Full::new(Bytes::from(vec![
            1;
            50000
        ]))));
        body.guard_binding(owner, service.clone());
        let first = body.frame().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(first.len(), 16 * 1024);
        service
            .admin
            .roles()
            .alter(
                "stream_role",
                giggles_core::roles::RoleChanges {
                    login: Some(false),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(
            body.frame().await.unwrap().unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
        assert!(body.frame().await.is_none());
        drop(first);
        fixture.runtime.shutdown().await;
    }

    struct DeferredBody {
        ready: Arc<AtomicBool>,
        bytes: Option<Bytes>,
    }
    impl Body for DeferredBody {
        type Data = Bytes;
        type Error = std::convert::Infallible;
        fn poll_frame(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
            let this = self.get_mut();
            if !this.ready.load(Ordering::Acquire) {
                return Poll::Pending;
            }
            Poll::Ready(this.bytes.take().map(|bytes| Ok(Frame::data(bytes))))
        }
    }

    #[tokio::test]
    async fn inner_pending_does_not_consume_the_binding_release_check() {
        let fixture = ReleaseFixture::new();
        let ready = Arc::new(AtomicBool::new(false));
        let (mut body, call) = fixture.body(tonic::body::Body::new(DeferredBody {
            ready: ready.clone(),
            bytes: Some(fixture.payload()),
        }));
        assert!(
            Pin::new(&mut body)
                .poll_frame(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        fixture.remove_readd();
        ready.store(true, Ordering::Release);
        let error = body.frame().await.unwrap().unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unauthenticated);
        assert!(call.upgrade().is_none());
        assert!(fixture.runtime.service().admission.connect().is_some());
        fixture.runtime.shutdown().await;
    }

    #[tokio::test]
    async fn queued_chunks_hold_delivery_after_body_drop_and_are_frame_bounded() {
        let activity = Arc::new(super::super::net::ConnectionActivity::new());
        let mut body = ResponseBody::new(
            tonic::body::Body::new(Full::new(Bytes::from(vec![1; 50000]))),
            None,
            None,
            activity.begin(),
        );
        let delivery = Arc::downgrade(body.delivery.as_ref().unwrap());
        let frame = body.frame().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(frame.len(), 16 * 1024);
        assert!(!body.is_end_stream());
        drop(body);
        assert!(
            delivery.upgrade().is_some(),
            "protocol send queue still owns delivery"
        );
        drop(frame);
        assert!(
            delivery.upgrade().is_none(),
            "last queued bytes release delivery"
        );
    }

    #[tokio::test]
    async fn receive_bound_is_inclusive_and_counts_actual_bytes() {
        let deadline = Instant::now() + std::time::Duration::from_secs(1);
        assert_eq!(
            collect(Full::new(Bytes::from_static(b"abc")), 3, deadline)
                .await
                .unwrap(),
            "abc"
        );
        assert!(matches!(
            collect(Full::new(Bytes::from_static(b"abcd")), 3, deadline).await,
            Err(ApiError::PayloadTooLarge)
        ));
        assert!(
            collect(Full::new(Bytes::new()), 0, deadline)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(matches!(
            collect(Full::new(Bytes::new()), 0, Instant::now()).await,
            Err(ApiError::DeadlineExceeded)
        ));
    }
}
