//! HTTP/1.1 and SCRAM client helpers shared by role-listener and dev-process tests.
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use serde_json::Value;
use std::net::SocketAddr;
use tokio::net::TcpStream;
pub async fn http(
    addr: SocketAddr,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: Value,
) -> (u16, Value) {
    let stream = TcpStream::connect(addr).await.unwrap();
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .unwrap();
    let task = tokio::spawn(async move {
        let _ = connection.await;
    });
    let mut request = hyper::Request::builder()
        .method(method)
        .uri(path)
        .header("host", "localhost")
        .header("content-type", "application/json");
    if let Some(token) = token {
        request = request.header("x-giggles-session", token);
    }
    let bytes = if method == "DELETE" {
        vec![]
    } else {
        serde_json::to_vec(&body).unwrap()
    };
    let response = sender
        .send_request(request.body(Full::new(Bytes::from(bytes))).unwrap())
        .await
        .unwrap();
    let status = response.status().as_u16();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    task.abort();
    let _ = task.await;
    (status, value)
}
// Small RFC 5802 client: verifies the server signature as well as producing proof.
fn mac(key: &[u8], data: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut block = [0; 64];
    if key.len() > 64 {
        block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        block[..key.len()].copy_from_slice(key);
    }
    let mut inner = Sha256::new();
    inner.update(block.map(|v| v ^ 0x36));
    inner.update(data);
    let mut outer = Sha256::new();
    outer.update(block.map(|v| v ^ 0x5c));
    outer.update(inner.finalize());
    outer.finalize().into()
}
pub const FIRST: &str = "n,,n=r,r=gd83-client-nonce";
pub fn proof(password: &str, first: &str) -> (String, String) {
    use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
    use sha2::{Digest, Sha256};
    let attrs: Vec<_> = first
        .split(',')
        .map(|a| a.split_once('=').unwrap().1)
        .collect();
    let mut salt = B64.decode(attrs[1]).unwrap();
    salt.extend_from_slice(&1u32.to_be_bytes());
    let mut u = mac(password.as_bytes(), &salt);
    let mut salted = u;
    for _ in 1..attrs[2].parse::<u32>().unwrap() {
        u = mac(password.as_bytes(), &u);
        for (a, b) in salted.iter_mut().zip(u) {
            *a ^= b;
        }
    }
    let key = mac(&salted, b"Client Key");
    let final_bare = format!("c=biws,r={}", attrs[0]);
    let auth = format!("{},{first},{final_bare}", &FIRST[3..]);
    let signature = mac(&Sha256::digest(key), auth.as_bytes());
    let proof: [u8; 32] = std::array::from_fn(|i| key[i] ^ signature[i]);
    (
        format!("{final_bare},p={}", B64.encode(proof)),
        format!(
            "v={}",
            B64.encode(mac(&mac(&salted, b"Server Key"), auth.as_bytes()))
        ),
    )
}
