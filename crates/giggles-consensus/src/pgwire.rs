//! Closed PostgreSQL v3 TASK adapter. Metadata never conveys authority.
use crate::{
    Result, TaskAction,
    authority::Session,
    native::{self, Event, Limits},
};
use giggles_server::{
    Request,
    pgwire::{Reader, decode, error, format_at, formats, frame, oid, row_description, z},
    task_statement::TaskStatement,
};
use rustls::{ServerConfig, pki_types::UnixTime};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};
use tokio::net::TcpStream;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{mpsc, oneshot},
    time::{Instant, timeout_at},
};
use tokio_rustls::{TlsAcceptor, server::TlsStream};
type Code<T> = std::result::Result<T, &'static str>;
const OBJECTS: usize = 32;
const LIFETIME: u64 = 60;
#[derive(Clone)]
struct Portal {
    statement: TaskStatement,
    action: TaskAction,
    formats: Vec<u16>,
    bytes: usize,
}
#[derive(Default)]
struct Protocol {
    statements: BTreeMap<String, TaskStatement>,
    portals: BTreeMap<String, Portal>,
    drain: bool,
}
enum Work {
    None,
    Quit,
    Task { action: TaskAction, simple: bool },
}
fn action(r: Request) -> Code<TaskAction> {
    match r {
        Request::Preview { request } => Ok(TaskAction::Preview(request)),
        Request::Execute { request, approval } => Ok(TaskAction::Execute(request, approval)),
        Request::Status { operation } => Ok(TaskAction::Status(operation)),
        _ => Err("0A000"),
    }
}
fn ready(out: &mut Vec<u8>) -> Code<()> {
    frame(b'Z', b"I", out)
}
impl Protocol {
    fn handle(&mut self, tag: u8, b: &[u8], out: &mut Vec<u8>) -> Code<Work> {
        let mut r = Reader { b, at: 0 };
        if self.drain && tag != b'S' && tag != b'X' {
            return Ok(Work::None);
        }
        match tag {
            b'X' => {
                r.end()?;
                return Ok(Work::Quit);
            }
            b'S' => {
                r.end()?;
                self.drain = false;
                self.portals.clear();
                ready(out)?;
            }
            b'H' => {
                r.end()?;
            }
            b'Q' => {
                let sql = r.text()?;
                r.end()?;
                self.statements.remove("");
                self.portals.clear();
                let json = sql.strip_prefix("GIGGLES ").ok_or("0A000")?;
                let req: Request = serde_json::from_str(json).map_err(|_| "08P01")?;
                // Core Request nesting does not use deny_unknown_fields. Enforce
                // the closed envelope without changing the shared P2 parser.
                let mut input: serde_json::Value =
                    serde_json::from_str(json).map_err(|_| "08P01")?;
                if matches!(req, Request::Execute { .. })
                    && let Some(o) = input.as_object_mut()
                {
                    o.entry("approval").or_insert(serde_json::Value::Null);
                }
                if serde_json::to_value(&req).map_err(|_| "XX000")? != input {
                    return Err("08P01");
                }

                return Ok(Work::Task {
                    action: action(req)?,
                    simple: true,
                });
            }
            b'P' => {
                let name = r.text()?;
                let sql = r.text()?;
                let n = r.u16()?;
                if name.len() > 63 || n > 7 {
                    return Err("54000");
                }
                if !name.is_empty() && self.statements.contains_key(&name) {
                    return Err("42P05");
                }
                if !self.statements.contains_key(&name) && self.statements.len() >= OBJECTS {
                    return Err("54000");
                }
                let s = TaskStatement::parse(&sql)?;
                let d = s.description();
                if n > d.parameters.len() {
                    return Err("08P01");
                }
                for ty in d.parameters.iter().take(n) {
                    let o = r.u32()?;
                    if o != 0 && o != oid(ty) {
                        return Err("0A000");
                    }
                }
                r.end()?;
                self.statements.insert(name, s);
                frame(b'1', &[], out)?;
            }
            b'B' => {
                let name = r.text()?;
                let stmt = r.text()?;
                let s = *self.statements.get(&stmt).ok_or("26000")?;
                let d = s.description();
                let f = formats(&mut r, d.parameters.len())?;
                let n = r.u16()?;
                if n != d.parameters.len() {
                    return Err("08P01");
                }
                let mut values = vec![];
                for (i, ty) in d.parameters.iter().enumerate() {
                    let n = r.u32()?;
                    values.push(if n == u32::MAX {
                        giggles_core::relational::Value::Null
                    } else {
                        decode(r.take(n as usize)?, ty, format_at(&f, i))?
                    });
                }
                let a = action(s.bind(&values)?)?;
                let output = formats(&mut r, 1)?;
                r.end()?;
                if name.len() > 63
                    || (!self.portals.contains_key(&name) && self.portals.len() >= OBJECTS)
                {
                    return Err("54000");
                }
                if !name.is_empty() && self.portals.contains_key(&name) {
                    return Err("42P03");
                }
                let retained: usize = self
                    .portals
                    .iter()
                    .filter(|(k, _)| **k != name)
                    .map(|(_, p)| p.bytes)
                    .sum();
                if retained.checked_add(b.len()).ok_or("54000")? > native::MAX_FRAME {
                    return Err("54000");
                }
                self.portals.insert(
                    name,
                    Portal {
                        statement: s,
                        action: a,
                        formats: output,
                        bytes: b.len(),
                    },
                );
                frame(b'2', &[], out)?;
            }
            b'D' => {
                let kind = r.byte()?;
                let name = r.text()?;
                r.end()?;
                match kind {
                    b'S' => {
                        let d = self.statements.get(&name).ok_or("26000")?.description();
                        let mut b = vec![];
                        b.extend((d.parameters.len() as u16).to_be_bytes());
                        for t in &d.parameters {
                            b.extend(oid(t).to_be_bytes());
                        }
                        frame(b't', &b, out)?;
                        row_description(&d, &[], out)?;
                    }
                    b'P' => {
                        let p = self.portals.get(&name).ok_or("34000")?;
                        row_description(&p.statement.description(), &p.formats, out)?;
                    }
                    _ => return Err("08P01"),
                }
            }
            b'E' => {
                let name = r.text()?;
                let max = r.u32()?;
                r.end()?;
                if max != 0 {
                    return Err("0A000");
                }
                let p = self.portals.get(&name).ok_or("34000")?;
                return Ok(Work::Task {
                    action: p.action.clone(),
                    simple: false,
                });
            }
            b'C' => {
                let kind = r.byte()?;
                let name = r.text()?;
                r.end()?;
                match kind {
                    b'S' => {
                        self.statements.remove(&name).ok_or("26000")?;
                    }
                    b'P' => {
                        self.portals.remove(&name).ok_or("34000")?;
                    }
                    _ => return Err("08P01"),
                };
                frame(b'3', &[], out)?;
            }
            _ => return Err("0A000"),
        }
        Ok(Work::None)
    }
}
async fn body(tls: &mut TlsStream<TcpStream>) -> Result<Vec<u8>> {
    let n = tls.read_u32().await? as usize;
    if !(4..=native::MAX_FRAME).contains(&n) {
        return Err("pgwire frame limit".into());
    }
    let mut b = vec![0; n - 4];
    tls.read_exact(&mut b).await?;
    Ok(b)
}
fn startup(b: &[u8]) -> Code<()> {
    let mut r = Reader { b, at: 0 };
    if r.u32()? != 196608 {
        return Err("0A000");
    }
    let mut seen = BTreeSet::new();
    loop {
        let key = r.text()?;
        if key.is_empty() {
            break;
        }
        let value = r.text()?;
        if key.len() > 63 || value.len() > 256 || !seen.insert(key.clone()) {
            return Err("08P01");
        }
        if !matches!(
            key.as_str(),
            "user" | "database" | "application_name" | "client_encoding"
        ) {
            return Err("0A000");
        }
        if key == "client_encoding" && value != "UTF8" {
            return Err("0A000");
        }
    }
    r.end()
}
async fn gate(stage: &str, operation: u64) -> Result<()> {
    let Some(root) = std::env::var_os("GIGGLES_PGWIRE_TEST_HOOK_ROOT") else {
        return Ok(());
    };
    let root = std::path::PathBuf::from(root);
    if !root.join(format!("pause-{stage}-{operation}")).exists() {
        return Ok(());
    }
    tokio::fs::write(
        root.join(format!("reached-{stage}-{operation}")),
        b"reached",
    )
    .await?;
    while !root.join(format!("resume-{stage}-{operation}")).exists() {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    Ok(())
}
struct QueryContext<'a> {
    session: &'a Session,
    events: &'a mpsc::Sender<Event>,
    limits: &'a Limits,
    verifier: &'a dyn rustls::server::danger::ClientCertVerifier,
    certs: &'a [rustls::pki_types::CertificateDer<'static>],
    deadline: Instant,
    cancellation: Arc<native::Cancellation>,
}
async fn task(
    tls: &mut TlsStream<TcpStream>,
    action: TaskAction,
    context: QueryContext<'_>,
) -> Code<(Vec<u8>, tokio::sync::OwnedSemaphorePermit)> {
    let QueryContext {
        session,
        events,
        limits,
        verifier,
        certs,
        deadline,
        cancellation,
    } = context;
    let operation = match &action {
        TaskAction::Preview(r) | TaskAction::Execute(r, _) => r.operation.0,
        TaskAction::Status(id) => id.0,
    };
    gate("before", operation).await.map_err(|_| "08007")?;
    verifier
        .verify_client_cert(&certs[0], &certs[1..], UnixTime::now())
        .map_err(|_| "28000")?;
    if !limits.work() {
        return Err("53300");
    }
    let permit = limits
        .requests
        .clone()
        .try_acquire_owned()
        .map_err(|_| "53300")?;
    let (tx, rx) = oneshot::channel();
    events
        .try_send(Event::Decide {
            session: session.clone(),
            action,
            cancellation: Some(cancellation),
            deadline,
            permit,
            reply: tx,
        })
        .map_err(|_| "53300")?;
    let held = rx.await.map_err(|_| "08007")?.map_err(|e| {
        if e == "deadline_before_submission" {
            "57014"
        } else if e == "unavailable" {
            "55000"
        } else {
            "08007"
        }
    })?;
    gate("held", operation).await.map_err(|_| "08007")?;
    tls.get_ref().0.writable().await.map_err(|_| "08007")?;
    verifier
        .verify_client_cert(&certs[0], &certs[1..], UnixTime::now())
        .map_err(|_| "28000")?;
    let (tx, rx) = oneshot::channel();
    events
        .try_send(Event::Finish {
            ready: held,
            deadline,
            reply: tx,
        })
        .map_err(|_| "08007")?;
    let released = rx.await.map_err(|_| "08007")?.map_err(|_| "08007")?;
    verifier
        .verify_client_cert(&certs[0], &certs[1..], UnixTime::now())
        .map_err(|_| "28000")?;
    if released.reply.code != 0 {
        return Err("42501");
    }
    let code = released
        .reply
        .data
        .get("code")
        .and_then(|v| v.as_str())
        .ok_or("XX000")?;
    if !matches!(code, "preview" | "committed" | "absent") {
        return Err(match code {
            "28000" => "28000",
            "42501" => "42501",
            "55000" => "55000",
            "54000" => "54000",
            "GD001" => "GD001",
            "GD002" => "GD002",
            "22023" => "22023",
            "57014" => "57014",
            "53300" => "53300",
            "08007" => "08007",
            _ => "XX000",
        });
    }
    let data = serde_json::to_vec(&released.reply.data).map_err(|_| "XX000")?;
    // NativeTask reserved this exact JSON payload plus 1024 bytes. DataRow,
    // RowDescription, completion and ReadyForQuery together use less than 128.
    if data.len() > 1024 {
        return Err("54000");
    }
    Ok((data, released.permit))
}
struct Backend {
    fingerprint: String,
    epoch: u64,
    active: Option<Arc<native::Cancellation>>,
}
type Backends = Arc<std::sync::Mutex<BTreeMap<[u8; 8], Backend>>>;
struct Registration {
    key: [u8; 8],
    backends: Backends,
}
impl Drop for Registration {
    fn drop(&mut self) {
        if let Ok(mut b) = self.backends.lock() {
            b.remove(&self.key);
        }
    }
}
fn register(backends: &Backends, session: &Session) -> Result<Registration> {
    let mut b = backends.lock().map_err(|_| "backend registry poisoned")?;
    if b.len() >= native::MAX_SESSIONS {
        return Err("backend limit".into());
    }
    for _ in 0..4 {
        let mut key = [0; 8];
        rustls::crypto::ring::default_provider()
            .secure_random
            .fill(&mut key)
            .map_err(|_| "backend entropy")?;
        if let std::collections::btree_map::Entry::Vacant(e) = b.entry(key) {
            e.insert(Backend {
                fingerprint: session.binding.fingerprint.clone(),
                epoch: session.epoch,
                active: None,
            });
            return Ok(Registration {
                key,
                backends: backends.clone(),
            });
        }
    }
    Err("backend collision".into())
}
pub async fn bind(
    config: native::Configuration,
    peers: BTreeSet<String>,
    events: mpsc::Sender<Event>,
    limits: Limits,
) -> Result<()> {
    let verifier =
        rustls::server::WebPkiClientVerifier::builder(Arc::new(native::roots(&config.ca)?))
            .build()?;
    let server = ServerConfig::builder()
        .with_client_cert_verifier(verifier.clone())
        .with_single_cert(
            native::chain(&config.certificate)?,
            native::key(&config.key)?,
        )?;
    let acceptor = TlsAcceptor::from(Arc::new(server));
    let listener = TcpListener::bind(&config.listen).await?;
    let peers = Arc::new(peers);
    let backends: Backends = Arc::new(std::sync::Mutex::new(BTreeMap::new()));
    tokio::spawn(async move {
        while let Ok((mut tcp, _)) = listener.accept().await {
            let receipt = Instant::now();
            let Some(permit) = limits.accept() else {
                continue;
            };
            let (acceptor, verifier, peers, events, limits, backends) = (
                acceptor.clone(),
                verifier.clone(),
                peers.clone(),
                events.clone(),
                limits.clone(),
                backends.clone(),
            );
            tokio::spawn(async move {
                let _session_permit = permit;
                let work = async {
                    let mut tls = timeout_at(receipt + Duration::from_secs(2), async {
                        giggles_server::pgwire::ssl_request(&mut tcp).await?;
                        acceptor.accept(tcp).await
                    })
                    .await??;
                    let certs = tls
                        .get_ref()
                        .1
                        .peer_certificates()
                        .ok_or("certificate")?
                        .to_vec();
                    let fp = crate::transport::fingerprint(certs.first().ok_or("certificate")?);
                    if peers.contains(&fp) {
                        return Err::<(), Box<dyn std::error::Error + Send + Sync>>(
                            "peer role denied".into(),
                        );
                    }
                    let (tx, rx) = oneshot::channel();
                    events
                        .try_send(Event::Authenticate {
                            fingerprint: fp,
                            deadline: receipt + Duration::from_secs(5),
                            reply: tx,
                        })
                        .map_err(|_| "capacity")?;
                    let session = timeout_at(receipt + Duration::from_secs(5), rx)
                        .await??
                        .map_err(|_| "unauthenticated")?;
                    let startup_body =
                        timeout_at(receipt + Duration::from_secs(5), body(&mut tls)).await??;
                    if startup_body.get(..4) == Some(&80877102u32.to_be_bytes()) {
                        if startup_body.len() != 12 {
                            return Err("cancel frame".into());
                        }
                        let key: [u8; 8] = startup_body[4..].try_into()?;
                        let b = backends.lock().map_err(|_| "backend registry")?;
                        if let Some(target) = b.get(&key)
                            && target.fingerprint == session.binding.fingerprint
                            && target.epoch == session.epoch
                            && let Some(active) = &target.active
                        {
                            active.cancel();
                        }
                        return Ok(());
                    }
                    startup(&startup_body).map_err(|_| "startup rejected")?;
                    let registration = register(&backends, &session)?;
                    let mut out = vec![];
                    frame(b'R', &0u32.to_be_bytes(), &mut out)?;
                    frame(b'K', &registration.key, &mut out)?;
                    for (k, v) in [
                        ("server_version", "14.0"),
                        ("client_encoding", "UTF8"),
                        ("standard_conforming_strings", "on"),
                    ] {
                        let mut b = vec![];
                        z(k, &mut b);
                        z(v, &mut b);
                        frame(b'S', &b, &mut out)?;
                    }
                    ready(&mut out)?;
                    timeout_at(receipt + Duration::from_secs(5), async {
                        tls.write_all(&out).await?;
                        tls.flush().await
                    })
                    .await??;
                    let mut protocol = Protocol::default();
                    loop {
                        let deadline =
                            Instant::now() + Duration::from_millis(native::MAX_DEADLINE_MS);
                        let tag = timeout_at(deadline, tls.read_u8()).await??;
                        let b = timeout_at(deadline, body(&mut tls)).await??;
                        let mut out = vec![];
                        let result = protocol.handle(tag, &b, &mut out);
                        let mut keep = None;
                        let code = match result {
                            Ok(Work::Quit) => break,
                            Ok(Work::None) => None,
                            Err(code) => Some(code),
                            Ok(Work::Task { action, simple }) => {
                                let cancellation = Arc::new(native::Cancellation::default());
                                if let Some(b) = backends
                                    .lock()
                                    .map_err(|_| "backend registry")?
                                    .get_mut(&registration.key)
                                {
                                    b.active = Some(cancellation.clone());
                                }
                                let run = async {
                                    tokio::select! {biased; _=cancellation.wait()=>Err("08007"), r=task(&mut tls,action,QueryContext {session:&session,events:&events,limits:&limits,verifier:verifier.as_ref(),certs:&certs,deadline,cancellation:cancellation.clone()})=>r}
                                };
                                let result = timeout_at(deadline, run).await;
                                if let Some(b) = backends
                                    .lock()
                                    .map_err(|_| "backend registry")?
                                    .get_mut(&registration.key)
                                {
                                    b.active = None;
                                }
                                match result {
                                    Err(_) => break, // Submitted work can still commit. Close => indeterminate.
                                    Ok(Err(code)) => Some(code),
                                    Ok(Ok((data, permit))) => {
                                        keep = Some(permit);
                                        if simple {
                                            row_description(
                                                &TaskStatement::Status.description(),
                                                &[],
                                                &mut out,
                                            )?;
                                        }
                                        let mut row = vec![];
                                        row.extend(1u16.to_be_bytes());
                                        row.extend((data.len() as u32).to_be_bytes());
                                        row.extend(data);
                                        frame(b'D', &row, &mut out)?;
                                        frame(b'C', b"SELECT 1\0", &mut out)?;
                                        if simple {
                                            ready(&mut out)?;
                                        }
                                        None
                                    }
                                }
                            }
                        };
                        if let Some(code) = code {
                            error(code, &mut out);
                            if tag == b'Q' {
                                ready(&mut out)?;
                            } else {
                                protocol.drain = true;
                            }
                        }
                        timeout_at(deadline, async {
                            tls.write_all(&out).await?;
                            tls.flush().await
                        })
                        .await??;
                        drop(keep);
                    }
                    Ok(())
                };
                let _ = timeout_at(receipt + Duration::from_secs(LIFETIME), work).await;
            });
        }
    });
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    fn parse(name: &str, sql: &str) -> Vec<u8> {
        let mut b = vec![];
        z(name, &mut b);
        z(sql, &mut b);
        b.extend(0u16.to_be_bytes());
        b
    }
    #[test]
    fn truncated_frames_never_panic_or_grant_task_work() {
        let b = parse("s", "GIGGLES EXECUTE RANGE ($1,$2,$3,$4,$5,$6,$7)");
        for end in 0..b.len() {
            let mut p = Protocol::default();
            assert!(
                p.handle(b'P', &b[..end], &mut vec![]).is_err(),
                "prefix {end}"
            );
        }
        let mut p = Protocol::default();
        assert!(matches!(p.handle(b'P', &b, &mut vec![]), Ok(Work::None)));
    }
    #[test]
    fn bounded_named_statements_and_no_metadata_authority() {
        let mut p = Protocol::default();
        for i in 0..OBJECTS {
            assert!(matches!(
                p.handle(
                    b'P',
                    &parse(&format!("s{i}"), "GIGGLES STATUS ($1)"),
                    &mut vec![]
                ),
                Ok(Work::None)
            ));
        }
        assert_eq!(
            p.handle(b'P', &parse("extra", "GIGGLES STATUS ($1)"), &mut vec![])
                .err(),
            Some("54000")
        );
        assert_eq!(
            p.handle(b'P', &parse("s0", "GIGGLES STATUS ($1)"), &mut vec![])
                .err(),
            Some("42P05")
        );
        assert_eq!(
            p.handle(b'P', &parse("bad", "SELECT 1"), &mut vec![]).err(),
            Some("54000")
        );
    }
    #[test]
    fn bind_null_arity_binary_text_formats_and_limits() {
        let mut p = Protocol::default();
        p.handle(b'P', &parse("s", "GIGGLES STATUS ($1)"), &mut vec![])
            .unwrap();
        for f in [0u16, 1, 2] {
            let mut b = b"p\0s\0".to_vec();
            b.extend(1u16.to_be_bytes());
            b.extend(f.to_be_bytes());
            b.extend(1u16.to_be_bytes());
            let value = if f == 0 {
                b"\\x0000000000000001".to_vec()
            } else {
                1u64.to_be_bytes().to_vec()
            };
            b.extend((value.len() as u32).to_be_bytes());
            b.extend(value);
            b.extend(0u16.to_be_bytes());
            let result = p.handle(b'B', &b, &mut vec![]);
            if f <= 1 {
                assert!(matches!(result, Ok(Work::None)));
                let mut execute = b"p\0".to_vec();
                execute.extend(0u32.to_be_bytes());
                assert!(matches!(
                    p.handle(b'E', &execute, &mut vec![]),
                    Ok(Work::Task {
                        action: TaskAction::Status(giggles_core::OperationId(1)),
                        ..
                    })
                ));
                p.portals.clear();
            } else {
                assert_eq!(result.err(), Some("0A000"));
            }
        }
        for n in [0, 2, 65535] {
            let mut b = b"p\0s\0\0\0".to_vec();
            b.extend((n as u16).to_be_bytes());
            assert_eq!(p.handle(b'B', &b, &mut vec![]).err(), Some("08P01"));
        }
    }
    #[test]
    fn startup_identity_is_not_task_authority_and_closed_envelope() {
        let mut b = 196608u32.to_be_bytes().to_vec();
        b.extend(b"user\0root\0database\0other\0\0");
        assert!(startup(&b).is_ok());
        b.extend(b"junk");
        assert_eq!(startup(&b), Err("08P01"));
        for req in [
            r#"GIGGLES {"kind":"Sql","sql":"SELECT 1"}"#,
            r#"GIGGLES {"kind":"Ping"}"#,
            r#"GIGGLES {"kind":"Status","operation":1,"principal":2}"#,
            r#"GIGGLES {"kind":"Preview","request":{"operation":1,"selection":{"Keys":[{"tenant":42,"row":0}]},"column":"Status","after":"Queued","principal":2}}"#,
        ] {
            let mut b = req.as_bytes().to_vec();
            b.push(0);
            assert!(Protocol::default().handle(b'Q', &b, &mut vec![]).is_err());
        }
    }
    #[test]
    fn exact_output_frame_overhead_fits_reserved_allowance() {
        let data=br#"{"version":1,"code":"absent","operation":18446744073709551615,"count":null,"digest":null,"sql":null}"#;
        let mut out = vec![];
        row_description(&TaskStatement::Status.description(), &[], &mut out).unwrap();
        let mut row = vec![];
        row.extend(1u16.to_be_bytes());
        row.extend((data.len() as u32).to_be_bytes());
        row.extend(data);
        frame(b'D', &row, &mut out).unwrap();
        frame(b'C', b"SELECT 1\0", &mut out).unwrap();
        ready(&mut out).unwrap();
        assert_eq!(out.len() - data.len(), 65);
        assert!(out.len() <= data.len() + 1024);
    }
}
