use giggles_faultlab::proxy::*;
use std::time::{Duration, Instant};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
async fn echo() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut s, _)) = listener.accept().await {
            tokio::spawn(async move {
                let (mut r, mut w) = s.split();
                let _ = tokio::io::copy(&mut r, &mut w).await;
            });
        }
    });
    address
}
async fn exchange(p: &Proxy, bytes: &[u8]) -> Vec<u8> {
    let mut s = TcpStream::connect(p.address()).await.unwrap();
    s.write_all(bytes).await.unwrap();
    s.shutdown().await.unwrap();
    let mut b = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), s.read_to_end(&mut b))
        .await
        .unwrap()
        .unwrap();
    b
}
#[tokio::test]
async fn opaque_binary_roundtrip_and_half_close() {
    let p = Proxy::start(echo().await, ProxyLimits::default())
        .await
        .unwrap();
    let bytes = (0..20000).map(|v| (v % 256) as u8).collect::<Vec<_>>();
    assert_eq!(exchange(&p, &bytes).await, bytes);
    let t = p.stop().await;
    assert!(!t.overflow);
    for d in [Direction::Upstream, Direction::Downstream] {
        assert_eq!(
            t.events
                .iter()
                .filter(|e| e.direction == Some(d) && e.kind == "forwarded")
                .map(|e| e.bytes)
                .sum::<usize>(),
            20000
        );
    }
}
#[tokio::test]
async fn directional_partition_resumes_without_loss() {
    for direction in [Direction::Upstream, Direction::Downstream] {
        let p = Proxy::start(echo().await, ProxyLimits::default())
            .await
            .unwrap();
        p.set(direction, Fault::Partition).unwrap();
        let mut s = TcpStream::connect(p.address()).await.unwrap();
        s.write_all(b"held").await.unwrap();
        let mut b = [0; 4];
        assert!(
            tokio::time::timeout(Duration::from_millis(60), s.read_exact(&mut b))
                .await
                .is_err()
        );
        p.set(direction, Fault::Pass).unwrap();
        tokio::time::timeout(Duration::from_secs(1), s.read_exact(&mut b))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&b, b"held");
        p.stop().await;
    }
}
#[tokio::test]
async fn delay_is_measured() {
    let p = Proxy::start(echo().await, ProxyLimits::default())
        .await
        .unwrap();
    p.set(Direction::Upstream, Fault::Delay { millis: 50 })
        .unwrap();
    let start = Instant::now();
    assert_eq!(exchange(&p, b"x").await, b"x");
    assert!(start.elapsed() >= Duration::from_millis(50));
    p.stop().await;
}
#[tokio::test]
async fn disconnect_terminates_existing_and_new() {
    let p = Proxy::start(echo().await, ProxyLimits::default())
        .await
        .unwrap();
    let mut s = TcpStream::connect(p.address()).await.unwrap();
    s.write_all(b"ok").await.unwrap();
    let mut b = [0; 2];
    s.read_exact(&mut b).await.unwrap();
    p.set(Direction::Downstream, Fault::Disconnect).unwrap();
    let mut b = [0; 1];
    let result = tokio::time::timeout(Duration::from_secs(1), s.read(&mut b))
        .await
        .unwrap();
    assert!(result.is_err() || result.unwrap() == 0);
    let mut s = TcpStream::connect(p.address()).await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(1), s.read(&mut b))
        .await
        .unwrap();
    assert!(result.is_err() || result.unwrap() == 0);
    p.set(Direction::Downstream, Fault::Pass).unwrap();
    assert_eq!(exchange(&p, b"restored").await, b"restored");
    p.stop().await;
}
#[tokio::test]
async fn admission_is_bounded() {
    let p = Proxy::start(
        echo().await,
        ProxyLimits {
            connections: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut first = TcpStream::connect(p.address()).await.unwrap();
    first.write_all(b"a").await.unwrap();
    let mut b = [0; 1];
    first.read_exact(&mut b).await.unwrap();
    let mut second = TcpStream::connect(p.address()).await.unwrap();
    let r = tokio::time::timeout(Duration::from_secs(1), second.read(&mut b))
        .await
        .unwrap();
    assert!(r.is_err() || r.unwrap() == 0);
    let t = p.stop().await;
    assert_eq!(t.events.iter().filter(|e| e.kind == "accepted").count(), 1);
    assert!(t.events.iter().any(|e| e.kind == "rejected"));
}
#[tokio::test]
async fn overflow_is_explicit() {
    let p = Proxy::start(
        echo().await,
        ProxyLimits {
            events: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let _ = exchange(&p, b"test").await;
    let t = p.stop().await;
    assert!(t.overflow);
    assert_eq!(t.events.len(), 1);
}
#[tokio::test]
async fn invalid_limits_and_remote_upstream() {
    let address = echo().await;
    for limits in [
        ProxyLimits {
            connections: 0,
            ..Default::default()
        },
        ProxyLimits {
            buffer_bytes: 0,
            ..Default::default()
        },
        ProxyLimits {
            events: 0,
            ..Default::default()
        },
        ProxyLimits {
            connect_millis: 1001,
            ..Default::default()
        },
        ProxyLimits {
            lifetime_millis: 0,
            ..Default::default()
        },
        ProxyLimits {
            idle_millis: 30001,
            ..Default::default()
        },
    ] {
        assert!(Proxy::start(address, limits).await.is_err());
    }
    for address in ["192.0.2.1:1234", "127.0.0.1:0"] {
        assert!(
            Proxy::start(address.parse().unwrap(), ProxyLimits::default())
                .await
                .is_err()
        );
    }
    let p = Proxy::start(address, ProxyLimits::default()).await.unwrap();
    assert!(
        p.set(Direction::Upstream, Fault::Delay { millis: 10001 })
            .is_err()
    );
    p.stop().await;
}
#[tokio::test]
async fn repeated_fault_state_machine() {
    let p = Proxy::start(echo().await, ProxyLimits::default())
        .await
        .unwrap();
    let mut seed = 0x9876_u64;
    for i in 0..40 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let d = if seed & 1 == 0 {
            Direction::Upstream
        } else {
            Direction::Downstream
        };
        p.set(d, Fault::Partition).unwrap();
        p.set(d, Fault::Delay { millis: 1 }).unwrap();
        let bytes = vec![i; 1 + (seed as usize % 200)];
        assert_eq!(exchange(&p, &bytes).await, bytes);
        p.set(d, Fault::Pass).unwrap();
    }
    let t = p.stop().await;
    assert!(!t.overflow);
    for (n, e) in t.events.iter().enumerate() {
        assert_eq!(e.sequence, n as u64);
    }
}
#[tokio::test]
async fn partition_expires_at_lifetime() {
    let p = Proxy::start(
        echo().await,
        ProxyLimits {
            lifetime_millis: 150,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    p.set(Direction::Upstream, Fault::Partition).unwrap();
    let mut s = TcpStream::connect(p.address()).await.unwrap();
    s.write_all(b"held").await.unwrap();
    let mut b = [0; 1];
    let r = tokio::time::timeout(Duration::from_secs(1), s.read(&mut b))
        .await
        .unwrap();
    assert!(r.is_err() || r.unwrap() == 0);
    let t = p.stop().await;
    assert!(t.events.iter().any(|e| e.kind == "lifetime"));
}
#[tokio::test]
async fn delay_is_interruptible_by_restore() {
    let p = Proxy::start(echo().await, ProxyLimits::default())
        .await
        .unwrap();
    p.set(Direction::Upstream, Fault::Delay { millis: 10000 })
        .unwrap();
    let mut s = TcpStream::connect(p.address()).await.unwrap();
    s.write_all(b"x").await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    p.set(Direction::Upstream, Fault::Pass).unwrap();
    let mut b = [0; 1];
    tokio::time::timeout(Duration::from_millis(500), s.read_exact(&mut b))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(b, [b'x']);
    p.stop().await;
}
#[tokio::test]
async fn idle_connections_expire() {
    let p = Proxy::start(
        echo().await,
        ProxyLimits {
            idle_millis: 100,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut s = TcpStream::connect(p.address()).await.unwrap();
    let mut b = [0; 1];
    let r = tokio::time::timeout(Duration::from_secs(1), s.read(&mut b))
        .await
        .unwrap();
    assert!(r.is_err() || r.unwrap() == 0);
    p.stop().await;
}
#[tokio::test]
async fn refused_upstream_is_recorded() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let p = Proxy::start(address, ProxyLimits::default()).await.unwrap();
    let mut s = TcpStream::connect(p.address()).await.unwrap();
    let mut b = [0; 1];
    let r = tokio::time::timeout(Duration::from_secs(2), s.read(&mut b))
        .await
        .unwrap();
    assert!(r.is_err() || r.unwrap() == 0);
    assert!(p.stop().await.events.iter().any(|e| e.kind == "io_error"));
}

#[tokio::test]
async fn retained_trace_matches_memory_and_never_overwrites() {
    let root = giggles_faultlab::process::fresh_directory("proxy-events").unwrap();
    let path = root.join("events.jsonl");
    let p = Proxy::start_retained(echo().await, ProxyLimits::default(), &path)
        .await
        .unwrap();
    assert_eq!(exchange(&p, b"retained").await, b"retained");
    let trace = p.stop().await;
    let bytes = std::fs::read(&path).unwrap();
    let from_disk = String::from_utf8(bytes)
        .unwrap()
        .lines()
        .map(|s| serde_json::from_str::<Event>(s).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        serde_json::to_value(from_disk).unwrap(),
        serde_json::to_value(trace.events).unwrap()
    );
    assert!(
        Proxy::start_retained(echo().await, ProxyLimits::default(), &path)
            .await
            .is_err()
    );
}
#[tokio::test]
async fn rapid_disconnect_restore_still_cuts_existing_socket() {
    let p = Proxy::start(echo().await, ProxyLimits::default())
        .await
        .unwrap();
    let mut s = TcpStream::connect(p.address()).await.unwrap();
    s.write_all(b"a").await.unwrap();
    let mut b = [0; 1];
    s.read_exact(&mut b).await.unwrap();
    p.set(Direction::Upstream, Fault::Disconnect).unwrap();
    p.set(Direction::Upstream, Fault::Pass).unwrap();
    let r = tokio::time::timeout(Duration::from_secs(1), s.read(&mut b))
        .await
        .unwrap();
    assert!(r.is_err() || r.unwrap() == 0);
    assert_eq!(exchange(&p, b"new").await, b"new");
    p.stop().await;
}
