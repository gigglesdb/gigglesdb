//! Bounded real mTLS peer transport. Certificates bind exact fixed node IDs.
//! No administrative commands are accepted on this listener.
use crate::{MAX_FRAME, Result, VOTERS};
use protobuf::Message as Pb;
use raft::eraftpb::Message;
use rustls::{ClientConfig, RootCertStore, ServerConfig, pki_types::ServerName};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, io::BufReader, path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Semaphore, mpsc},
};
use tokio_rustls::{TlsAcceptor, TlsConnector};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Peer {
    pub address: String,
    pub fingerprint: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Configuration {
    pub id: u64,
    pub domain: u128,
    pub database: PathBuf,
    pub listen: String,
    pub certificate: PathBuf,
    pub key: PathBuf,
    pub ca: PathBuf,
    pub peers: BTreeMap<u64, Peer>,
}
pub enum Event {
    Message(u64, Message),
    SnapshotResult(u64, bool),
}
async fn snapshot_result(events: &mpsc::Sender<Event>, to: u64, success: bool) {
    let _ = events.send(Event::SnapshotResult(to, success)).await;
}
async fn admit_outbound(
    permits: &Arc<Semaphore>,
    events: &mpsc::Sender<Event>,
    message: &Message,
) -> Option<tokio::sync::OwnedSemaphorePermit> {
    match permits.clone().try_acquire_owned() {
        Ok(permit) => Some(permit),
        Err(_) => {
            if message.has_snapshot() {
                snapshot_result(events, message.to, false).await;
            }
            None
        }
    }
}
pub fn fingerprint(der: &[u8]) -> String {
    hex::encode(Sha256::digest(der))
}
const MAGIC: &[u8; 8] = b"GDRAFT01";
pub fn encode_message(domain: u128, message: &Message) -> Result<Vec<u8>> {
    let mut bytes = MAGIC.to_vec();
    bytes.extend(domain.to_be_bytes());
    bytes.extend(message.write_to_bytes()?);
    if bytes.len() > MAX_FRAME {
        return Err("peer message limit".into());
    }
    Ok(bytes)
}
pub fn decode_message(domain: u128, bytes: &[u8]) -> Result<Message> {
    if bytes.len() < 24
        || bytes.len() > MAX_FRAME
        || &bytes[..8] != MAGIC
        || bytes[8..24] != domain.to_be_bytes()
    {
        return Err("peer cluster domain/format/size".into());
    }
    Ok(Message::parse_from_bytes(&bytes[24..])?)
}
pub async fn read_frame<R: AsyncRead + Unpin>(r: &mut R) -> Result<Vec<u8>> {
    let n = r.read_u32().await? as usize;
    if n == 0 || n > MAX_FRAME {
        return Err("frame limit".into());
    }
    let mut b = vec![0; n];
    r.read_exact(&mut b).await?;
    Ok(b)
}
pub async fn write_frame<W: AsyncWrite + Unpin>(w: &mut W, b: &[u8]) -> Result<()> {
    if b.is_empty() || b.len() > MAX_FRAME {
        return Err("frame limit".into());
    }
    w.write_u32(b.len() as u32).await?;
    w.write_all(b).await?;
    w.flush().await?;
    Ok(())
}
fn chain(p: &PathBuf) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    Ok(
        rustls_pemfile::certs(&mut BufReader::new(std::fs::File::open(p)?))
            .collect::<std::io::Result<_>>()?,
    )
}
fn key(p: &PathBuf) -> Result<rustls::pki_types::PrivateKeyDer<'static>> {
    rustls_pemfile::private_key(&mut BufReader::new(std::fs::File::open(p)?))?
        .ok_or_else(|| "missing key".into())
}
impl Configuration {
    pub fn validate(&self) -> Result<()> {
        if !VOTERS.contains(&self.id) || self.peers.keys().copied().collect::<Vec<_>>() != VOTERS {
            return Err("fixed three member config required".into());
        }
        let mut seen = std::collections::BTreeSet::new();
        for p in self.peers.values() {
            if p.fingerprint.len() != 64
                || hex::decode(&p.fingerprint)?.len() != 32
                || !seen.insert(p.fingerprint.clone())
            {
                return Err("invalid/duplicate fingerprint".into());
            }
        }
        let own = chain(&self.certificate)?;
        if own.is_empty() || fingerprint(&own[0]) != self.peers[&self.id].fingerprint {
            return Err("own certificate binding".into());
        }
        Ok(())
    }
    fn tls(&self) -> Result<(TlsAcceptor, TlsConnector)> {
        self.validate()?;
        let mut roots = RootCertStore::empty();
        for c in chain(&self.ca)? {
            roots.add(c)?;
        }
        let verifier =
            rustls::server::WebPkiClientVerifier::builder(Arc::new(roots.clone())).build()?;
        let server = ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(chain(&self.certificate)?, key(&self.key)?)?;
        let client = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_auth_cert(chain(&self.certificate)?, key(&self.key)?)?;
        Ok((
            TlsAcceptor::from(Arc::new(server)),
            TlsConnector::from(Arc::new(client)),
        ))
    }
}
pub struct Transport {
    pub listener: TcpListener,
    pub acceptor: TlsAcceptor,
    pub connector: TlsConnector,
    pub config: Configuration,
}
impl Transport {
    pub async fn bind(config: Configuration) -> Result<Self> {
        let (acceptor, connector) = config.tls()?;
        let listener = TcpListener::bind(&config.listen).await?;
        Ok(Self {
            listener,
            acceptor,
            connector,
            config,
        })
    }
    pub fn run(self, events: mpsc::Sender<Event>) -> mpsc::Sender<Message> {
        let (out, mut outgoing) = mpsc::channel::<Message>(128);
        let peers = Arc::new(self.config.peers);
        let id = self.config.id;
        let domain = self.config.domain;
        let connector = self.connector;
        let send_events = events.clone();
        let send_peers = peers.clone();
        tokio::spawn(async move {
            let permits = Arc::new(Semaphore::new(8));
            while let Some(m) = outgoing.recv().await {
                let Some(permit) = admit_outbound(&permits, &send_events, &m).await else {
                    continue;
                };
                let connector = connector.clone();
                let peers = send_peers.clone();
                let events = send_events.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    let to = m.to;
                    let snap = m.has_snapshot();
                    let sent = tokio::time::timeout(Duration::from_secs(2), async {
                        let peer = peers.get(&to).ok_or("unknown peer")?;
                        let tcp = TcpStream::connect(&peer.address).await?;
                        let mut tls = connector
                            .connect(ServerName::try_from("localhost")?, tcp)
                            .await?;
                        let cert = tls
                            .get_ref()
                            .1
                            .peer_certificates()
                            .and_then(|c| c.first())
                            .ok_or("missing server cert")?;
                        if fingerprint(cert) != peer.fingerprint {
                            return Err::<(), Box<dyn std::error::Error + Send + Sync>>(
                                "server node binding".into(),
                            );
                        }
                        write_frame(&mut tls, &encode_message(domain, &m)?).await?;
                        let ack = read_frame(&mut tls).await?;
                        if ack != b"ok" {
                            return Err("peer rejected".into());
                        }
                        Ok(())
                    })
                    .await;
                    if snap {
                        snapshot_result(&events, to, matches!(sent, Ok(Ok(())))).await;
                    }
                });
            }
        });
        tokio::spawn(async move {
            let permits = Arc::new(Semaphore::new(8));
            while let Ok((tcp, _)) = self.listener.accept().await {
                let Ok(permit) = permits.clone().try_acquire_owned() else {
                    continue;
                };
                let acceptor = self.acceptor.clone();
                let peers = peers.clone();
                let events = events.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    let _ = tokio::time::timeout(Duration::from_secs(2), async {
                        let mut tls = acceptor.accept(tcp).await?;
                        let cert = tls
                            .get_ref()
                            .1
                            .peer_certificates()
                            .and_then(|c| c.first())
                            .ok_or("missing client cert")?;
                        let fp = fingerprint(cert);
                        let from = peers
                            .iter()
                            .find_map(|(id, p)| (p.fingerprint == fp).then_some(*id))
                            .ok_or("unbound certificate")?;
                        let b = read_frame(&mut tls).await?;
                        let m = decode_message(domain, &b)?;
                        if m.from != from || m.to != id || from == id {
                            return Err::<(), Box<dyn std::error::Error + Send + Sync>>(
                                "node identity mismatch".into(),
                            );
                        }
                        events
                            .try_send(Event::Message(from, m))
                            .map_err(|_| "inbound queue full")?;
                        write_frame(&mut tls, b"ok").await?;
                        Ok(())
                    })
                    .await;
                });
            }
        });
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{future::Future, task::Poll};

    #[tokio::test]
    async fn saturated_outbound_reports_snapshot_failure_and_preserves_permits() {
        let permits = Arc::new(Semaphore::new(1));
        let held = permits.clone().acquire_owned().await.unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        let mut message = Message {
            to: 2,
            ..Default::default()
        };
        message.mut_snapshot().mut_metadata().index = 7;
        assert!(admit_outbound(&permits, &tx, &message).await.is_none());
        assert!(matches!(
            rx.recv().await,
            Some(Event::SnapshotResult(2, false))
        ));
        assert_eq!(permits.available_permits(), 0);
        drop(held);
        assert!(admit_outbound(&permits, &tx, &message).await.is_some());
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn snapshot_completion_waits_for_event_capacity_instead_of_disappearing() {
        for success in [false, true] {
            let (tx, mut rx) = mpsc::channel(1);
            tx.send(Event::SnapshotResult(3, false)).await.unwrap();
            let mut completion = Box::pin(snapshot_result(&tx, 2, success));
            std::future::poll_fn(|cx| {
                assert!(completion.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            assert!(matches!(
                rx.recv().await,
                Some(Event::SnapshotResult(3, false))
            ));
            completion.await;
            assert!(
                matches!(rx.recv().await, Some(Event::SnapshotResult(2, value)) if value == success)
            );
        }
    }
}
