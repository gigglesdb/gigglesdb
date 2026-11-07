//! Bounded receive buffers and ownership carried through response delivery.
use super::{
    dispatch::{CallOwnership, ResponseLease},
    error::ApiError,
    identity::ConnectionIdentity,
    net::ActivityTicket,
};
use crate::Service;
use bytes::{Bytes, BytesMut};
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

/// Both the body and its queued output bytes own the response lease. The final
/// owner releases it even when the caller has stopped awaiting the response.
pub(crate) struct ResponseBody {
    inner: tonic::body::Body,
    pending: Bytes,
    delivery: Option<Arc<Mutex<DeliveryOwnership>>>,
    release_checked: bool,
    done: bool,
}

struct DeliveryOwnership {
    _ownership: Option<Arc<CallOwnership>>,
    lease: Option<ResponseLease>,
    binding: Option<(ConnectionIdentity, Arc<Service>)>,
    _ticket: ActivityTicket,
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
                _ticket: ticket,
            }))),
            release_checked: false,
            done: false,
        }
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

    fn chunk(&mut self) -> Result<Frame<Bytes>, tonic::Status> {
        // Check only when the first nonempty DATA frame is actually ready.
        // An inner Pending must not consume the final release check.
        if !self.release_checked {
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
            self.release_checked = true;
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
        if !this.pending.is_empty() {
            return Poll::Ready(Some(this.chunk()));
        }
        match Pin::new(&mut this.inner).poll_frame(cx) {
            Poll::Ready(None) => {
                this.finish();
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(error))) => {
                this.finish();
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                Ok(bytes) if !bytes.is_empty() => {
                    this.pending = bytes;
                    Poll::Ready(Some(this.chunk()))
                }
                Ok(bytes) => Poll::Ready(Some(Ok(Frame::data(bytes)))),
                Err(frame) => Poll::Ready(Some(Ok(frame))),
            },
            Poll::Pending => Poll::Pending,
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
        let mut hint = SizeHint::new();
        hint.set_lower(inner.lower().saturating_add(self.pending.len() as u64));
        if let Some(upper) = inner
            .upper()
            .and_then(|upper| upper.checked_add(self.pending.len() as u64))
        {
            hint.set_upper(upper);
        }
        hint
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
                    sessions: 1,
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
