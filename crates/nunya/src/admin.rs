//! Small, bounded HTTP endpoint for operator health, metrics, and reloads.

use std::{io, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    sync::mpsc,
    time::timeout,
};

const REQUEST_LIMIT: usize = 8192;
const READ_TIMEOUT: Duration = Duration::from_secs(5);
type Status = Arc<dyn Fn() -> String + Send + Sync>;
type Ready = Arc<dyn Fn() -> (bool, String) + Send + Sync>;
type Metrics = Arc<dyn Fn() -> Vec<(String, u64)> + Send + Sync>;

/// Serve one bounded HTTP/1.1 request per connection until the listener fails.
pub async fn serve_admin(
    addr: SocketAddr,
    metrics: Arc<dyn Fn() -> Vec<(String, u64)> + Send + Sync>,
    reload: mpsc::Sender<()>,
    status: Status,
    ready: Ready,
) -> io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        let metrics = metrics.clone();
        let reload = reload.clone();
        let status = status.clone();
        let ready = ready.clone();
        tokio::spawn(async move {
            // Client failures contain no diagnostics and do not stop the listener.
            let _ = handle(stream, &metrics, &reload, &status, &ready, READ_TIMEOUT).await;
        });
    }
}

#[derive(Debug, PartialEq)]
struct Request<'a> {
    method: &'a str,
    path: &'a str,
    body_len: usize,
}

fn token(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte)
}

fn parse_head(head: &[u8]) -> Option<Request<'_>> {
    let text = std::str::from_utf8(head).ok()?;
    let mut lines = text.strip_suffix("\r\n\r\n")?.split("\r\n");
    let mut first = lines.next()?.split(' ');
    let method = first.next()?;
    let path = first.next()?;
    if first.next()? != "HTTP/1.1"
        || first.next().is_some()
        || method.is_empty()
        || !method.bytes().all(token)
        || !path.starts_with('/')
        || !path.bytes().all(|b| b.is_ascii_graphic())
    {
        return None;
    }
    let mut host = false;
    let mut length = None;
    for line in lines {
        let (name, value) = line.split_once(':')?;
        if name.is_empty()
            || !name.bytes().all(token)
            || !value.bytes().all(|b| b == b'\t' || (32..=126).contains(&b))
        {
            return None;
        }
        let value = value.trim_matches([' ', '\t']);
        if name.eq_ignore_ascii_case("host") {
            if host || value.is_empty() {
                return None;
            }
            host = true;
        } else if name.eq_ignore_ascii_case("content-length") {
            if length.is_some() || value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            length = Some(value.parse::<usize>().ok()?);
        } else if name.eq_ignore_ascii_case("transfer-encoding")
            || name.eq_ignore_ascii_case("expect")
        {
            return None;
        }
    }
    host.then_some(Request {
        method,
        path,
        body_len: length.unwrap_or(0),
    })
}

async fn read_request<S: AsyncRead + Unpin>(stream: &mut S) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut expected = None;
    loop {
        if let Some(end) = expected
            && bytes.len() >= end
        {
            bytes.truncate(end);
            return Ok(bytes);
        }
        if bytes.len() == REQUEST_LIMIT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "request too large",
            ));
        }
        let mut chunk = [0; 1024];
        let available = chunk.len().min(REQUEST_LIMIT - bytes.len());
        let n = stream.read(&mut chunk[..available]).await?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "incomplete request",
            ));
        }
        bytes.extend_from_slice(&chunk[..n]);
        if expected.is_none()
            && let Some(pos) = bytes.windows(4).position(|w| w == b"\r\n\r\n")
        {
            let end = pos + 4;
            let request = parse_head(&bytes[..end])
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid request"))?;
            let total = end
                .checked_add(request.body_len)
                .filter(|n| *n <= REQUEST_LIMIT)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "request too large"))?;
            expected = Some(total);
        }
    }
}

fn metric_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    matches!(bytes.next(), Some(b) if b.is_ascii_alphabetic() || b == b'_' || b == b':')
        && bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b':')
}

async fn handle<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    metrics: &Metrics,
    reload: &mpsc::Sender<()>,
    status: &Status,
    ready: &Ready,
    read_timeout: Duration,
) -> io::Result<()> {
    let (status, body, content_type) = match timeout(read_timeout, read_request(&mut stream)).await
    {
        Err(_) => (
            "408 Request Timeout",
            "request timeout\n".to_owned(),
            "text/plain",
        ),
        Ok(Err(_)) => ("400 Bad Request", "bad request\n".to_owned(), "text/plain"),
        Ok(Ok(bytes)) => {
            let end = bytes
                .windows(4)
                .position(|w| w == b"\r\n\r\n")
                .map(|p| p + 4);
            let request = end.and_then(|end| parse_head(&bytes[..end]));
            match request.map(|r| (r.method, r.path)) {
                Some(("GET", "/status")) => ("200 OK", status(), "text/plain"),
                Some(("GET", "/ready")) => {
                    let (ok, body) = ready();
                    (if ok { "200 OK" } else { "503 Service Unavailable" }, body, "text/plain")
                }
                Some(("GET", "/health")) => ("200 OK", "ok".to_owned(), "text/plain"),
                Some(("GET", "/metrics")) => {
                    let mut body = String::new();
                    for (name, value) in metrics() {
                        if metric_name(&name) {
                            use std::fmt::Write;
                            let _ = writeln!(body, "{name} {value}");
                        }
                    }
                    ("200 OK", body, "text/plain; version=0.0.4")
                }
                Some(("POST", "/reload")) => match reload.try_send(()) {
                    Ok(()) => ("202 Accepted", "accepted\n".to_owned(), "text/plain"),
                    Err(_) => (
                        "503 Service Unavailable",
                        "reload unavailable\n".to_owned(),
                        "text/plain",
                    ),
                },
                Some(_) => ("404 Not Found", "not found\n".to_owned(), "text/plain"),
                None => ("400 Bad Request", "bad request\n".to_owned(), "text/plain"),
            }
        }
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn exchange(request: &[u8]) -> (String, mpsc::Receiver<()>) {
        let (mut client, server) = tokio::io::duplex(32 * 1024);
        let metrics: Metrics = Arc::new(|| {
            vec![
                ("nunya_requests_total".into(), 7),
                ("bad\nsecret".into(), 9),
                ("nunya_policy_reloads_total".into(), 2),
                ("nunya_policy_reload_failures_total".into(), 1),
                ("nunya_policy_applied_unix_ms".into(), 123),
            ]
        });
        let (tx, rx) = mpsc::channel(1);
        let task = tokio::spawn(async move {
            handle(server, &metrics, &tx, &(Arc::new(|| format!("policy_sha256={}\npolicy_applied_unix_ms=1\nlast_reload=none\nlast_reload_unix_ms=0\npolicy_watch=off\n", "a".repeat(64))) as Status), &(Arc::new(|| (false, "unavailable".into())) as Ready), Duration::from_millis(20)).await
        });
        client.write_all(request).await.unwrap();
        let mut response = String::new();
        client.read_to_string(&mut response).await.unwrap();
        task.await.unwrap().unwrap();
        (response, rx)
    }

    #[tokio::test]
    async fn readiness_status_and_body_are_passed_through() {
        for ok in [false, true] {
            let (mut client, server) = tokio::io::duplex(2048);
            let (tx, _rx) = mpsc::channel(1);
            let ready: Ready = Arc::new(move || (ok, "route=postgres policy=ok catalog=ok audit=ok store=ok\n".into()));
            let task = tokio::spawn(async move {
                handle(server, &(Arc::new(Vec::new) as Metrics), &tx,
                    &(Arc::new(String::new) as Status), &ready, READ_TIMEOUT).await
            });
            client.write_all(b"GET /ready HTTP/1.1\r\nHost: local\r\n\r\n").await.unwrap();
            let mut response = String::new();
            client.read_to_string(&mut response).await.unwrap();
            task.await.unwrap().unwrap();
            assert!(response.starts_with(if ok { "HTTP/1.1 200 OK" } else { "HTTP/1.1 503 Service Unavailable" }));
            assert!(response.ends_with("route=postgres policy=ok catalog=ok audit=ok store=ok\n"));
        }
    }

    #[test]
    fn parsing_refuses_ambiguous_or_invalid_headers() {
        for request in [
            "POST /reload HTTP/1.1\r\n\r\n",
            "POST /reload HTTP/1.0\r\nHost: local\r\n\r\n",
            "POST /reload HTTP/1.1\r\nHost: local\r\nContent-Length: 0\r\nContent-Length: 0\r\n\r\n",
            "POST /reload HTTP/1.1\r\nHost: local\r\nTransfer-Encoding: chunked\r\n\r\n",
            "POST /reload HTTP/1.1\r\nHost: local\r\n bad: value\r\n\r\n",
        ] {
            assert!(parse_head(request.as_bytes()).is_none());
        }
    }

    #[tokio::test]
    async fn health_metrics_reload_and_missing_routes() {
        let (status, _) = exchange(b"GET /status HTTP/1.1\r\nHost: local\r\n\r\n").await;
        assert!(status.starts_with("HTTP/1.1 200"));
        assert!(status.contains("Content-Type: text/plain\r\n"));
        assert!(status.ends_with(&format!("policy_sha256={}\npolicy_applied_unix_ms=1\nlast_reload=none\nlast_reload_unix_ms=0\npolicy_watch=off\n", "a".repeat(64))));
        let (health, _) = exchange(b"GET /health HTTP/1.1\r\nHost: local\r\n\r\n").await;
        assert!(health.starts_with("HTTP/1.1 200"));
        assert!(health.ends_with("\r\n\r\nok"));
        let (metrics, _) = exchange(b"GET /metrics HTTP/1.1\r\nHost: local\r\n\r\n").await;
        assert!(metrics.contains("nunya_requests_total 7\n"));
        for name in [
            "nunya_policy_reloads_total",
            "nunya_policy_reload_failures_total",
            "nunya_policy_applied_unix_ms",
        ] {
            assert!(metrics.contains(name));
        }
        assert!(!metrics.contains("secret"));
        let (reload, mut rx) =
            exchange(b"POST /reload HTTP/1.1\r\nHost: local\r\nContent-Length: 3\r\n\r\nabc").await;
        assert!(reload.starts_with("HTTP/1.1 202"));
        assert_eq!(rx.try_recv(), Ok(()));
        let (missing, _) = exchange(b"GET /reload HTTP/1.1\r\nHost: local\r\n\r\n").await;
        assert!(missing.starts_with("HTTP/1.1 404"));
        let (unknown, _) = exchange(b"GET /unknown HTTP/1.1\r\nHost: local\r\n\r\n").await;
        assert!(unknown.starts_with("HTTP/1.1 404"));
    }

    #[tokio::test]
    async fn malformed_oversized_and_incomplete_requests_never_reload() {
        let oversized = format!(
            "POST /reload HTTP/1.1\r\nHost: local\r\nContent-Length: {REQUEST_LIMIT}\r\n\r\n"
        );
        for request in [
            b"POST /reload HTTP/1.1\r\n\r\n".as_slice(),
            oversized.as_bytes(),
        ] {
            let (response, mut rx) = exchange(request).await;
            assert!(response.starts_with("HTTP/1.1 400"));
            assert!(rx.try_recv().is_err());
        }
        let (response, mut rx) =
            exchange(b"POST /reload HTTP/1.1\r\nHost: local\r\nContent-Length: 1\r\n\r\n").await;
        assert!(response.starts_with("HTTP/1.1 408"));
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn unavailable_reload_channels_return_service_unavailable() {
        for closed in [false, true] {
            let (mut client, server) = tokio::io::duplex(1024);
            let metrics: Metrics = Arc::new(Vec::new);
            let (tx, rx) = mpsc::channel(1);
            if closed {
                drop(rx);
            } else {
                tx.try_send(()).unwrap();
            }
            let task = tokio::spawn(async move {
                handle(server, &metrics, &tx, &(Arc::new(|| format!("policy_sha256={}\npolicy_applied_unix_ms=1\nlast_reload=none\nlast_reload_unix_ms=0\npolicy_watch=off\n", "a".repeat(64))) as Status), &(Arc::new(|| (false, "unavailable".into())) as Ready), READ_TIMEOUT).await
            });
            client
                .write_all(b"POST /reload HTTP/1.1\r\nHost: local\r\n\r\n")
                .await
                .unwrap();
            let mut response = String::new();
            client.read_to_string(&mut response).await.unwrap();
            task.await.unwrap().unwrap();
            assert!(response.starts_with("HTTP/1.1 503"));
        }
    }

    #[tokio::test]
    async fn exact_limit_is_accepted_and_pipelining_is_not_executed() {
        let prefix = "GET /health HTTP/1.1\r\nHost: local\r\nX-Padding: ";
        let request = format!(
            "{prefix}{}\r\n\r\n",
            "a".repeat(REQUEST_LIMIT - prefix.len() - 4)
        );
        let (response, _) = exchange(request.as_bytes()).await;
        assert!(response.starts_with("HTTP/1.1 200"));
        let (response, mut rx) = exchange(b"GET /health HTTP/1.1\r\nHost: local\r\n\r\nPOST /reload HTTP/1.1\r\nHost: local\r\n\r\n").await;
        assert_eq!(response.matches("HTTP/1.1").count(), 1);
        assert!(rx.try_recv().is_err());
    }
}
