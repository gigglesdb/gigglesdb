// Deterministic transport failures prove batch delivery and uncertainty separately.
struct FailingSend {
    accepted: Arc<std::sync::atomic::AtomicUsize>,
    fail_after: Arc<std::sync::atomic::AtomicUsize>,
}
impl tokio::io::AsyncRead for FailingSend {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        _: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Pending
    }
}
impl tokio::io::AsyncWrite for FailingSend {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let accepted = self.accepted.load(Ordering::Relaxed);
        let fail_after = self.fail_after.load(Ordering::Relaxed);
        if accepted >= fail_after {
            return std::task::Poll::Ready(Err(std::io::ErrorKind::BrokenPipe.into()));
        }
        let n = bytes.len().min(fail_after - accepted);
        self.accepted.fetch_add(n, Ordering::Relaxed);
        std::task::Poll::Ready(Ok(n))
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}
#[tokio::test]
async fn send_failure_distinguishes_unsent_from_uncertain_writes() {
    for (earlier_write, fail_after, expected) in [
        (false, 0, NyCode::ProtocolNotAllowed),
        (true, 0, NyCode::WriteOutcomeUnknown),
        (false, 1, NyCode::WriteOutcomeUnknown),
    ] {
        let f = Fixture::new();
        let mut s = f.session();
        prepare_admission_portal(&mut s, "INSERT INTO items(value) VALUES (NULL)");
        let accepted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let cutoff = Arc::new(std::sync::atomic::AtomicUsize::new(usize::MAX));
        let mut upstream = Upstream::test_transport(FailingSend {
            accepted: accepted.clone(),
            fail_after: cutoff.clone(),
        });
        if earlier_write {
            let prior = s.frontend(Frontend::Execute {
                portal: "p".into(),
                max_rows: 0,
            });
            assert!(!prior.upstream.is_empty());
            assert!(s.write_in_flight());
            upstream.send(&prior.upstream).await.unwrap();
            assert!(accepted.load(Ordering::Relaxed) > 0);
        }
        let before = s.write_in_flight();
        let batch = s.frontend(Frontend::Execute {
            portal: "p".into(),
            max_rows: 0,
        });
        assert!(!batch.upstream.is_empty());
        assert!(s.write_in_flight());
        let already_accepted = accepted.load(Ordering::Relaxed);
        cutoff.store(already_accepted + fail_after, Ordering::Relaxed);
        let error = upstream.send(&batch.upstream).await.unwrap_err();
        assert_eq!(
            error.reason == super::super::upstream::SEND_BEFORE_ANY_BYTE,
            fail_after == 0
        );
        assert_eq!(
            accepted.load(Ordering::Relaxed) - already_accepted,
            fail_after
        );
        let error = s.upstream_lost(error, before);
        assert_eq!(error.code, expected);
        let records = f.lifecycle.lock().unwrap();
        assert_eq!(
            records.len(),
            usize::from(expected == NyCode::WriteOutcomeUnknown)
        );
        if let Some(record) = records.first() {
            assert_eq!(record.reason, "upstream_lost");
            assert_eq!(record.kind, LifecycleKind::WriteOutcomeUnknown);
        }
    }
}
