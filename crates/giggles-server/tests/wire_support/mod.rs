// Shared raw PostgreSQL frames used by the existing network tests and capture replay.
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub(crate) async fn pg_send<S: tokio::io::AsyncWrite + Unpin>(s: &mut S, tag: u8, b: &[u8]) {
    s.write_u8(tag).await.unwrap();
    s.write_u32((b.len() + 4) as u32).await.unwrap();
    s.write_all(b).await.unwrap();
    s.flush().await.unwrap();
}

pub(crate) async fn pg_read<S: tokio::io::AsyncRead + Unpin>(s: &mut S) -> (u8, Vec<u8>) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let tag = s.read_u8().await.unwrap();
        let n = s.read_u32().await.unwrap();
        assert!((4..=8 * 1024 * 1024).contains(&n));
        let mut b = vec![0; n as usize - 4];
        s.read_exact(&mut b).await.unwrap();
        (tag, b)
    })
    .await
    .expect("wire response timeout")
}

pub(crate) async fn pg_sync<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    s: &mut S,
) -> Vec<(u8, Vec<u8>)> {
    pg_send(s, b'S', &[]).await;
    let mut frames = vec![];
    loop {
        let f = pg_read(s).await;
        let end = f.0 == b'Z';
        frames.push(f);
        if end {
            return frames;
        }
    }
}

pub(crate) fn cstr(s: &str, b: &mut Vec<u8>) {
    b.extend(s.as_bytes());
    b.push(0);
}

pub(crate) fn parse(name: &str, sql: &str) -> Vec<u8> {
    let mut b = vec![];
    cstr(name, &mut b);
    cstr(sql, &mut b);
    b.extend(0u16.to_be_bytes());
    b
}

pub(crate) fn bind(
    portal: &str,
    stmt: &str,
    params: &[Option<&str>],
    binary_result: bool,
) -> Vec<u8> {
    let mut b = vec![];
    cstr(portal, &mut b);
    cstr(stmt, &mut b);
    b.extend(0u16.to_be_bytes());
    b.extend((params.len() as u16).to_be_bytes());
    for v in params {
        if let Some(v) = v {
            b.extend((v.len() as i32).to_be_bytes());
            b.extend(v.as_bytes());
        } else {
            b.extend((-1i32).to_be_bytes());
        }
    }
    b.extend(1u16.to_be_bytes());
    b.extend(u16::from(binary_result).to_be_bytes());
    b
}

pub(crate) fn parse_oids(name: &str, sql: &str, oids: &[u32]) -> Vec<u8> {
    let mut b = Vec::new();
    cstr(name, &mut b);
    cstr(sql, &mut b);
    b.extend((oids.len() as u16).to_be_bytes());
    for oid in oids {
        b.extend(oid.to_be_bytes());
    }
    b
}

pub(crate) fn execute(portal: &str, max: u32) -> Vec<u8> {
    let mut b = vec![];
    cstr(portal, &mut b);
    b.extend(max.to_be_bytes());
    b
}
