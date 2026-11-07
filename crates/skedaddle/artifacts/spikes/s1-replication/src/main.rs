//! Assertion-driven PG16+ pgoutput v1 spike. Deliberately no automatic flush advancement.
use anyhow::{Context, Result, bail, ensure};
use bytes::{BufMut, BytesMut};
use md5::{Digest, Md5};
use postgres_protocol::{
    authentication::sasl::{ChannelBinding, ScramSha256},
    message::frontend,
};
use rustls::{ClientConfig, RootCertStore, pki_types::ServerName};
use serde_json::{Value, json};
use std::{
    collections::{BTreeSet, HashMap},
    env,
    fs::{File, OpenOptions},
    io::{BufReader, Write},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_rustls::{TlsConnector, client::TlsStream};

fn lsn(s: &str) -> u64 {
    let (a, b) = s.split_once('/').unwrap();
    (u64::from_str_radix(a, 16).unwrap() << 32) | u64::from_str_radix(b, 16).unwrap()
}
fn fmt(n: u64) -> String {
    format!("{:X}/{:X}", n >> 32, n & 0xffff_ffff)
}
fn u32be(b: &[u8]) -> u32 {
    u32::from_be_bytes(b[..4].try_into().unwrap())
}
fn u64be(b: &[u8]) -> u64 {
    u64::from_be_bytes(b[..8].try_into().unwrap())
}
fn pgtime() -> i64 {
    (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64)
        - 946684800000000
}
fn md5hex(v: &[u8]) -> String {
    Md5::digest(v).iter().map(|b| format!("{b:02x}")).collect()
}
struct Evidence {
    out: File,
    checks: usize,
    kinds: BTreeSet<String>,
}
impl Evidence {
    fn new() -> Result<Self> {
        Ok(Self {
            out: OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(std::path::Path::new(&env::var("S1_OUT")?).join("events.jsonl"))?,
            checks: 0,
            kinds: BTreeSet::new(),
        })
    }
    fn log(&mut self, v: Value) -> Result<()> {
        writeln!(self.out, "{v}")?;
        self.out.flush()?;
        Ok(())
    }
    fn durable(&mut self, records: &[Value]) -> Result<u64> {
        let point = end(records);
        let path = std::path::Path::new(&env::var("S1_OUT")?)
            .join(format!("checkpoint-{point:016x}.json"));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        serde_json::to_writer(
            &mut file,
            &json!({"durable_lsn":fmt(point),"records":records}),
        )?;
        file.flush()?;
        file.sync_all()?;
        self.log(json!({"checkpoint_fsynced":path,"durable_lsn":fmt(point)}))?;
        Ok(point)
    }
    fn check(&mut self, name: &str, yes: bool, detail: Value) -> Result<()> {
        self.log(json!({"check":name,"pass":yes,"detail":detail}))?;
        ensure!(yes, "assertion failed: {name}: {detail}");
        self.checks += 1;
        println!("PASS {name}");
        Ok(())
    }
}
struct Conn {
    io: TlsStream<TcpStream>,
    incoming: BytesMut,
    pid: u32,
    tls: String,
    scram: bool,
}
impl Conn {
    async fn connect(port: u16, replication: bool, user: &str, password: &str) -> Result<Self> {
        let mut tcp = TcpStream::connect(("127.0.0.1", port)).await?;
        tcp.set_nodelay(true)?;
        let mut b = BytesMut::new();
        frontend::ssl_request(&mut b);
        tcp.write_all(&b).await?;
        ensure!(tcp.read_u8().await? == b'S', "server refused TLS");
        let cert = File::open(env::var("S1_CERT")?)?;
        let mut roots = RootCertStore::empty();
        for c in rustls_pemfile::certs(&mut BufReader::new(cert)) {
            roots.add(c?)?;
        }
        let cfg = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let io = TlsConnector::from(Arc::new(cfg))
            .connect(ServerName::try_from("localhost")?, tcp)
            .await?;
        let tls = format!(
            "{:?}/{:?}",
            io.get_ref().1.protocol_version(),
            io.get_ref().1.negotiated_cipher_suite()
        );
        let mut c = Self {
            io,
            incoming: BytesMut::new(),
            pid: 0,
            tls,
            scram: false,
        };
        b.clear();
        let mut params = vec![
            ("user", user),
            ("database", "skedaddle"),
            ("application_name", "s1-rust-probe"),
        ];
        if replication {
            params.push(("replication", "database"));
        }
        frontend::startup_message(params, &mut b)?;
        c.io.write_all(&b).await?;
        let mut sasl: Option<ScramSha256> = None;
        loop {
            let (tag, p) = c.frame().await?;
            match tag {
                b'R' => match u32be(&p) {
                    10 => {
                        ensure!(
                            p[4..].split(|v| *v == 0).any(|v| v == b"SCRAM-SHA-256"),
                            "SCRAM not offered"
                        );
                        let s =
                            ScramSha256::new(password.as_bytes(), ChannelBinding::unsupported());
                        b.clear();
                        frontend::sasl_initial_response("SCRAM-SHA-256", s.message(), &mut b)?;
                        c.io.write_all(&b).await?;
                        sasl = Some(s);
                    }
                    11 => {
                        let s = sasl.as_mut().context("unexpected SASL continue")?;
                        s.update(&p[4..])?;
                        b.clear();
                        frontend::sasl_response(s.message(), &mut b)?;
                        c.io.write_all(&b).await?;
                    }
                    12 => {
                        sasl.as_mut()
                            .context("unexpected SASL final")?
                            .finish(&p[4..])?;
                        c.scram = true;
                    }
                    0 => ensure!(c.scram, "authenticated without SCRAM"),
                    x => bail!("unsupported auth {x}"),
                },
                b'K' => c.pid = u32be(&p),
                b'E' => bail!("auth error {}", String::from_utf8_lossy(&p)),
                b'Z' => break,
                _ => {}
            }
        }
        Ok(c)
    }
    async fn frame(&mut self) -> Result<(u8, Vec<u8>)> {
        loop {
            if self.incoming.len() >= 5 {
                let n = u32be(&self.incoming[1..]) as usize;
                ensure!(
                    (4..=16 * 1024 * 1024).contains(&n),
                    "invalid wire length {n}"
                );
                if self.incoming.len() >= n + 1 {
                    let frame = self.incoming.split_to(n + 1);
                    return Ok((frame[0], frame[5..].to_vec()));
                }
            }
            ensure!(
                self.io.read_buf(&mut self.incoming).await? > 0,
                "connection EOF"
            );
        }
    }
    async fn send_query(&mut self, q: &str) -> Result<()> {
        let mut b = BytesMut::new();
        frontend::query(q, &mut b)?;
        self.io.write_all(&b).await?;
        Ok(())
    }
    async fn query(&mut self, q: &str) -> Result<Vec<Vec<String>>> {
        self.send_query(q).await?;
        let mut rows = vec![];
        let mut error = None;
        loop {
            let (t, p) = self.frame().await?;
            match t {
                b'D' => {
                    let mut d = Cursor::new(&p);
                    let n = d.u16()?;
                    let mut row = vec![];
                    for _ in 0..n {
                        let len = d.u32()?;
                        row.push(if len == u32::MAX {
                            "NULL".into()
                        } else {
                            String::from_utf8_lossy(d.take(len as usize)?).into_owned()
                        });
                    }
                    rows.push(row);
                }
                b'E' => error = Some(String::from_utf8_lossy(&p).into_owned()),
                b'Z' => break,
                _ => {}
            }
        }
        if let Some(e) = error {
            bail!("SQL {q}: {e}");
        }
        Ok(rows)
    }
    async fn start(&mut self, slot: &str, pos: u64, proto: u8, origin: &str) -> Result<()> {
        self.send_query(&format!("START_REPLICATION SLOT {slot} LOGICAL {} (proto_version '{proto}', publication_names 's1_pub', messages 'true', origin '{origin}', streaming 'off')",fmt(pos))).await?;
        loop {
            let (t, p) = self.frame().await?;
            match t {
                b'W' => return Ok(()),
                b'E' => bail!("START: {}", String::from_utf8_lossy(&p)),
                _ => {}
            }
        }
    }
    async fn status(&mut self, write: u64, flush: u64) -> Result<()> {
        ensure!(flush <= write, "flush exceeds write");
        let mut payload = BytesMut::new();
        payload.put_u8(b'r');
        payload.put_u64(write);
        payload.put_u64(flush);
        payload.put_u64(flush);
        payload.put_i64(pgtime());
        payload.put_u8(0);
        let mut b = BytesMut::new();
        b.put_u8(b'd');
        b.put_u32((payload.len() + 4) as u32);
        b.extend_from_slice(&payload);
        self.io.write_all(&b).await?;
        Ok(())
    }
    async fn close(mut self, copy: bool) -> Result<()> {
        let mut b = BytesMut::new();
        if copy {
            frontend::copy_done(&mut b);
        }
        frontend::terminate(&mut b);
        self.io.write_all(&b).await?;
        self.io.shutdown().await?;
        Ok(())
    }
}
struct Cursor<'a> {
    b: &'a [u8],
    n: usize,
}
impl<'a> Cursor<'a> {
    fn new(b: &'a [u8]) -> Self {
        Self { b, n: 0 }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        ensure!(self.n + n <= self.b.len(), "short pgoutput");
        let x = &self.b[self.n..self.n + n];
        self.n += n;
        Ok(x)
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into()?))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32be(self.take(4)?))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64be(self.take(8)?))
    }
    fn z(&mut self) -> Result<String> {
        let len = self.b[self.n..]
            .iter()
            .position(|b| *b == 0)
            .context("unterminated string")?;
        let x = String::from_utf8(self.take(len)?.to_vec())?;
        self.take(1)?;
        Ok(x)
    }
    fn tuple(&mut self) -> Result<Value> {
        let n = self.u16()?;
        let mut cols = vec![];
        for _ in 0..n {
            let kind = self.u8()? as char;
            match kind {
                'n' | 'u' => cols.push(json!({"kind":kind.to_string()})),
                't' | 'b' => {
                    let len = self.u32()? as usize;
                    let v = self.take(len)?;
                    cols.push(json!({"kind":kind.to_string(),"len":len,"md5":md5hex(v),"text":if len<=100 {Some(String::from_utf8_lossy(v).into_owned())}else{None}}));
                }
                _ => bail!("unknown tuple kind {kind}"),
            }
        }
        Ok(json!(cols))
    }
}
#[derive(Default)]
struct Decoder {
    relations: HashMap<u32, String>,
    xid: u32,
}
impl Decoder {
    fn decode(&mut self, b: &[u8]) -> Result<Value> {
        let mut d = Cursor::new(b);
        let tag = d.u8()? as char;
        let v = match tag {
            'B' => {
                let final_lsn = d.u64()?;
                let time = d.u64()?;
                self.xid = d.u32()?;
                json!({"kind":"Begin","final_lsn":fmt(final_lsn),"time":time,"xid":self.xid})
            }
            'C' => {
                let flags = d.u8()?;
                let commit = d.u64()?;
                let end = d.u64()?;
                let time = d.u64()?;
                json!({"kind":"Commit","flags":flags,"commit":fmt(commit),"end":fmt(end),"time":time,"xid":self.xid})
            }
            'R' => {
                let oid = d.u32()?;
                let ns = d.z()?;
                let name = d.z()?;
                let identity = d.u8()? as char;
                let n = d.u16()?;
                let mut cols = vec![];
                for _ in 0..n {
                    cols.push(
                        json!({"flags":d.u8()?,"name":d.z()?,"oid":d.u32()?,"typmod":d.u32()?}),
                    );
                }
                self.relations.insert(oid, name.clone());
                json!({"kind":"Relation","oid":oid,"namespace":ns,"name":name,"identity":identity.to_string(),"columns":cols})
            }
            'Y' => json!({"kind":"Type","oid":d.u32()?,"namespace":d.z()?,"name":d.z()?}),
            'O' => json!({"kind":"Origin","lsn":fmt(d.u64()?),"name":d.z()?}),
            'I' | 'U' | 'D' => {
                let oid = d.u32()?;
                let mut tuples = vec![];
                while d.n < d.b.len() {
                    let part = d.u8()? as char;
                    ensure!(['N', 'O', 'K'].contains(&part), "tuple part {part}");
                    tuples.push(json!({"part":part.to_string(),"columns":d.tuple()?}));
                }
                json!({"kind":match tag {'I'=>"Insert",'U'=>"Update",_=>"Delete"},"oid":oid,"table":self.relations.get(&oid),"tuples":tuples,"xid":self.xid})
            }
            'T' => {
                let n = d.u32()?;
                let flags = d.u8()?;
                let mut ids = vec![];
                for _ in 0..n {
                    ids.push(d.u32()?);
                }
                json!({"kind":"Truncate","flags":flags,"relations":ids})
            }
            'M' => {
                let flags = d.u8()?;
                let pos = d.u64()?;
                let prefix = d.z()?;
                let len = d.u32()? as usize;
                let content = String::from_utf8(d.take(len)?.to_vec())?;
                json!({"kind":"Message","transactional":flags==1,"lsn":fmt(pos),"prefix":prefix,"content":content})
            }
            _ => bail!("unexpected pgoutput tag {tag}; streaming must be off"),
        };
        ensure!(d.n == b.len(), "trailing pgoutput bytes for {tag}");
        Ok(v)
    }
}
struct Stream {
    c: Conn,
    decoder: Decoder,
    write: u64,
    flush: u64,
    keepalives: usize,
    requests: usize,
}
impl Stream {
    async fn open(port: u16, slot: &str, pos: u64, origin: &str) -> Result<Self> {
        let mut c = Conn::connect(port, true, "postgres", "skedaddle").await?;
        c.start(slot, pos, 1, origin).await?;
        Ok(Self {
            c,
            decoder: Decoder::default(),
            write: pos,
            flush: 0,
            keepalives: 0,
            requests: 0,
        })
    }
    async fn receive(&mut self, e: &mut Evidence, reply: bool) -> Result<Option<Value>> {
        let (t, p) = self.c.frame().await?;
        if t == b'E' {
            bail!("stream server error: {}", String::from_utf8_lossy(&p));
        }
        ensure!(t == b'd', "expected CopyData, got {}", t as char);
        match p[0] {
            b'k' => {
                ensure!(p.len() == 18, "bad keepalive");
                self.keepalives += 1;
                if p[17] != 0 {
                    self.requests += 1;
                }
                if reply && p[17] != 0 {
                    self.c.status(self.write, self.flush).await?;
                }
                e.log(json!({"keepalive":fmt(u64be(&p[1..])),"request":p[17],"write":fmt(self.write),"flush":fmt(self.flush),"replied":reply&&p[17]!=0}))?;
                Ok(None)
            }
            b'w' => {
                ensure!(p.len() > 25, "short XLogData");
                let start = u64be(&p[1..]);
                self.write = self.write.max(start);
                let v = self.decoder.decode(&p[25..])?;
                if v["kind"] == "Commit" {
                    self.write = self.write.max(lsn(v["end"].as_str().unwrap()));
                }
                e.kinds.insert(v["kind"].as_str().unwrap().into());
                e.log(
                    json!({"record":v,"wal_start":fmt(start),"server_wal_end":fmt(u64be(&p[9..]))}),
                )?;
                Ok(Some(v))
            }
            x => bail!("unknown replication wrapper {}", x as char),
        }
    }
    async fn drain(&mut self, e: &mut Evidence, seconds: f64, reply: bool) -> Result<Vec<Value>> {
        let until = Instant::now() + Duration::from_secs_f64(seconds);
        let mut records = vec![];
        loop {
            let now = Instant::now();
            if now >= until {
                break;
            }
            match tokio::time::timeout(until - now, self.receive(e, reply)).await {
                Ok(v) => {
                    if let Some(v) = v? {
                        records.push(v);
                    }
                }
                Err(_) => break,
            }
        }
        Ok(records)
    }
    async fn commits(&mut self, e: &mut Evidence, n: usize) -> Result<Vec<Value>> {
        let until = Instant::now() + Duration::from_secs(12);
        let mut records = vec![];
        let mut count = 0;
        while count < n {
            let v = tokio::time::timeout(
                until.saturating_duration_since(Instant::now()),
                self.receive(e, true),
            )
            .await
            .context("waiting for commit")??;
            if let Some(v) = v {
                if v["kind"] == "Commit" {
                    count += 1;
                }
                records.push(v);
            }
        }
        Ok(records)
    }
}
async fn admin(port: u16) -> Result<Conn> {
    Conn::connect(port, false, "postgres", "skedaddle").await
}
async fn slot(a: &mut Conn, name: &str) -> Result<u64> {
    let rows = a
        .query(&format!(
            "SELECT lsn FROM pg_create_logical_replication_slot('{name}','pgoutput')"
        ))
        .await?;
    Ok(lsn(&rows[0][0]))
}
async fn inactive(a: &mut Conn, name: &str) -> Result<()> {
    for _ in 0..40 {
        if a.query(&format!(
            "select active from pg_replication_slots where slot_name='{name}'"
        ))
        .await?[0][0]
            == "f"
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    bail!("slot remained active {name}")
}
fn end(records: &[Value]) -> u64 {
    records
        .iter()
        .rev()
        .find(|r| r["kind"] == "Commit")
        .map(|r| lsn(r["end"].as_str().unwrap()))
        .unwrap()
}
fn ids(records: &[Value]) -> Vec<String> {
    records
        .iter()
        .filter(|v| v["kind"] == "Insert" && v["table"] == "markers")
        .filter_map(|v| {
            v["tuples"][0]["columns"][0]["text"]
                .as_str()
                .map(str::to_owned)
        })
        .collect()
}
async fn confirmed(a: &mut Conn, name: &str) -> Result<u64> {
    Ok(lsn(&a
        .query(&format!(
            "select confirmed_flush_lsn from pg_replication_slots where slot_name='{name}'"
        ))
        .await?[0][0]))
}
async fn wait_confirmed(a: &mut Conn, name: &str, at: u64) -> Result<u64> {
    for _ in 0..40 {
        let v = confirmed(a, name).await?;
        if v >= at {
            return Ok(v);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    bail!("confirmed_flush_lsn did not reach {}", fmt(at))
}
async fn toxi(enabled: bool) -> Result<()> {
    let port: u16 = env::var("S1_TOXI_API")?.parse()?;
    let body = format!("{{\"enabled\":{enabled}}}");
    let mut c = TcpStream::connect(("127.0.0.1", port)).await?;
    c.write_all(format!("POST /proxies/pg HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await?;
    let mut response = vec![];
    c.read_to_end(&mut response).await?;
    ensure!(
        String::from_utf8_lossy(&response).starts_with("HTTP/1.1 200"),
        "toxiproxy failed {}",
        String::from_utf8_lossy(&response)
    );
    Ok(())
}

async fn run(e: &mut Evidence) -> Result<()> {
    let port: u16 = env::var("S1_PG_PORT")?.parse()?;
    let proxy: u16 = env::var("S1_TOXI_PORT")?.parse()?;
    let mut a = admin(port).await?;
    a.query("CREATE TYPE s1_mood AS ENUM ('happy','sad'); CREATE TABLE markers(id text primary key); CREATE TABLE shapes(id int primary key, n int, mood s1_mood, body text); CREATE TABLE toast_default(id int primary key,n int,body text); CREATE TABLE toast_full(id int primary key,n int,body text); ALTER TABLE toast_full REPLICA IDENTITY FULL; CREATE TABLE spill(id int primary key,body text); CREATE PUBLICATION s1_pub FOR ALL TABLES;").await?;
    let mut repl = Conn::connect(port, true, "postgres", "skedaddle").await?;
    let ssl=a.query(&format!("select s.ssl,s.version,s.cipher,r.application_name,r.state from pg_stat_ssl s join pg_stat_replication r using(pid) where s.pid={}",repl.pid)).await?;
    e.check(
        "tls_scram",
        repl.scram && ssl.len() == 1 && ssl[0][0] == "t",
        json!({"client_tls":repl.tls,"mechanism":"SCRAM-SHA-256","server":ssl,"pid":repl.pid}),
    )?;
    e.log(json!({"identify_system":repl.query("IDENTIFY_SYSTEM").await?}))?;
    for (name, suffix, expect) in [
        ("modern", "(SNAPSHOT 'nothing')", true),
        ("legacy", "NOEXPORT_SNAPSHOT", true),
        ("bare", "SNAPSHOT 'nothing'", false),
    ] {
        let r = repl
            .query(&format!(
                "CREATE_REPLICATION_SLOT syntax_{name} LOGICAL pgoutput {suffix}"
            ))
            .await;
        e.check(
            &format!("create_syntax_{name}"),
            r.is_ok() == expect,
            json!({"syntax":suffix,"result":format!("{r:?}")}),
        )?;
        if r.is_ok() {
            a.query(&format!("select pg_drop_replication_slot('syntax_{name}')"))
                .await?;
        }
    }
    repl.close(false).await?;
    let pos = slot(&mut a, "protocols").await?;
    let mut accepted = vec![];
    for proto in 0..=6 {
        let mut c = Conn::connect(port, true, "postgres", "skedaddle").await?;
        let r = c.start("protocols", pos, proto, "none").await;
        e.log(json!({"proto_version":proto,"messages":true,"origin":"none","streaming":"off","accepted":r.is_ok(),"error":r.as_ref().err().map(ToString::to_string)}))?;
        if r.is_ok() {
            accepted.push(proto);
            c.close(true).await?;
        } else {
            drop(c);
        }
        inactive(&mut a, "protocols").await?;
    }
    e.check(
        "protocol_versions",
        accepted == vec![1, 2, 3, 4],
        json!({"accepted":accepted,"pin":1,"rejected_tested":[0,5,6]}),
    )?;
    a.query("select pg_drop_replication_slot('protocols')")
        .await?;

    // Every required record kind, explicit origin filtering and transactional/nontransactional messages.
    let pos = slot(&mut a, "records_none").await?;
    let pos_any = slot(&mut a, "records_any").await?;
    let mut none = Stream::open(port, "records_none", pos, "none").await?;
    for q in [
        "INSERT INTO shapes VALUES (1,0,'happy','small')",
        "UPDATE shapes SET n=1,mood='sad' WHERE id=1",
        "DELETE FROM shapes WHERE id=1",
        "TRUNCATE shapes",
        "BEGIN; SELECT pg_logical_emit_message(true,'s1','transactional'); COMMIT",
        "SELECT pg_logical_emit_message(false,'s1','nontransactional')",
        "SELECT pg_replication_origin_create('remote_s1'); SELECT pg_replication_origin_session_setup('remote_s1')",
        "BEGIN; SELECT pg_replication_origin_xact_setup('0/1234',now()); INSERT INTO markers VALUES ('remote'); COMMIT",
        "SELECT pg_replication_origin_session_reset()",
        "INSERT INTO markers VALUES ('local')",
    ] {
        a.query(q).await?;
    }
    let records = none.commits(e, 6).await?;
    e.check(
        "origin_none",
        ids(&records) == vec!["local"],
        json!({"ids":ids(&records)}),
    )?;
    e.check(
        "message_modes",
        records
            .iter()
            .any(|r| r["kind"] == "Message" && r["transactional"] == true)
            && records
                .iter()
                .any(|r| r["kind"] == "Message" && r["transactional"] == false),
        json!({"records":records.len()}),
    )?;
    none.c.close(true).await?;
    let mut any = Stream::open(port, "records_any", pos_any, "any").await?;
    let all = any.commits(e, 7).await?;
    e.check("origin_any",ids(&all)==vec!["remote","local"]&&all.iter().any(|r|r["kind"]=="Origin"&&r["name"]=="remote_s1"),json!({"ids":ids(&all),"origins":all.iter().filter(|r|r["kind"]=="Origin").collect::<Vec<_>>()}))?;
    any.c.close(true).await?;
    let required = [
        "Begin", "Commit", "Relation", "Insert", "Update", "Delete", "Truncate", "Message",
        "Origin", "Type",
    ];
    e.check(
        "all_record_kinds",
        required.iter().all(|k| e.kinds.contains(*k)),
        json!({"observed":e.kinds}),
    )?;

    // Match S4's complete function-grant boundary; none of these grants implies superuser.
    a.query("CREATE ROLE s1_app LOGIN PASSWORD 'app' NOSUPERUSER NOREPLICATION; GRANT USAGE ON SCHEMA public TO s1_app; GRANT SELECT,INSERT ON markers TO s1_app;").await?;
    let mut app = Conn::connect(port, false, "s1_app", "app").await?;
    let denied = app
        .query("select pg_replication_origin_create('app_origin')")
        .await;
    e.check(
        "origin_privilege_denied",
        denied.is_err(),
        json!({"error":denied.err().map(|e|e.to_string())}),
    )?;
    let funcs = [
        "pg_replication_origin_create(text)",
        "pg_replication_origin_advance(text,pg_lsn)",
        "pg_replication_origin_session_setup(text)",
        "pg_replication_origin_session_is_setup()",
        "pg_replication_origin_session_progress(boolean)",
        "pg_replication_origin_session_reset()",
        "pg_replication_origin_xact_setup(pg_lsn,timestamp with time zone)",
        "pg_replication_origin_xact_reset()",
        "pg_replication_origin_progress(text,boolean)",
        "pg_replication_origin_drop(text)",
    ];
    for f in funcs {
        a.query(&format!("GRANT EXECUTE ON FUNCTION {f} TO s1_app"))
            .await?;
    }
    let granted=app.query("SELECT pg_replication_origin_create('app_origin'); SELECT pg_replication_origin_advance('app_origin','0/100'); SELECT pg_replication_origin_session_setup('app_origin'); SELECT pg_replication_origin_session_is_setup(); SELECT pg_replication_origin_session_progress(false); BEGIN; SELECT pg_replication_origin_xact_setup('0/200',now()); INSERT INTO markers VALUES ('non_super_origin'); COMMIT; SELECT pg_replication_origin_xact_reset(); SELECT pg_replication_origin_session_reset(); SELECT pg_replication_origin_progress('app_origin',false); SELECT pg_replication_origin_drop('app_origin'); SELECT rolsuper,rolreplication FROM pg_roles WHERE rolname=current_user;").await?;
    e.check(
        "origin_privilege_grants",
        granted.last() == Some(&vec!["f".into(), "f".into()]),
        json!({"functions":funcs,"results":granted}),
    )?;
    app.close(false).await?;

    // Unacknowledged A/B/C are identified by row values rather than WAL positions alone.
    let pos = slot(&mut a, "redelivery").await?;
    let mut s = Stream::open(port, "redelivery", pos, "none").await?;
    a.query("INSERT INTO markers VALUES ('ack_A')").await?;
    let ra = s.commits(e, 1).await?;
    let durable = e.durable(&ra)?;
    s.flush = durable;
    s.c.status(s.write, s.flush).await?;
    wait_confirmed(&mut a, "redelivery", durable).await?;
    a.query("INSERT INTO markers VALUES ('unack_B')").await?;
    let rb = s.commits(e, 1).await?;
    s.c.status(s.write, s.flush).await?;
    e.check(
        "flush_held_before_disconnect",
        confirmed(&mut a, "redelivery").await? == durable,
        json!({"durable":fmt(durable),"unacked_end":fmt(end(&rb)),"write":fmt(s.write)}),
    )?;
    s.c.close(true).await?;
    inactive(&mut a, "redelivery").await?;
    let mut s = Stream::open(proxy, "redelivery", 0, "none").await?;
    s.flush = durable;
    let rb = s.commits(e, 1).await?;
    let mut observed = ids(&rb);
    e.check("clean_redelivery",observed==vec!["unack_B"],json!({"ids":observed,"requested_start":"0/0","confirmed":fmt(confirmed(&mut a,"redelivery").await?)}))?;
    a.query("INSERT INTO markers VALUES ('unack_C')").await?;
    let rc = s.commits(e, 1).await?;
    observed.extend(ids(&rc));
    let end_c = end(&rc);
    s.c.status(s.write, durable).await?;
    toxi(false).await?;
    let cut = s.drain(e, 2.0, true).await;
    e.check(
        "toxiproxy_connection_cut",
        cut.is_err(),
        json!({"error":cut.err().map(|e|e.to_string())}),
    )?;
    drop(s);
    inactive(&mut a, "redelivery").await?;
    toxi(true).await?;
    let mut s = Stream::open(port, "redelivery", 0, "none").await?;
    s.flush = durable;
    let replay = s.commits(e, 2).await?;
    e.check(
        "toxi_redelivery",
        ids(&replay) == vec!["unack_B", "unack_C"],
        json!({"ids":ids(&replay),"durable":fmt(durable)}),
    )?;
    s.flush = e.durable(&replay)?;
    s.c.status(s.write, s.flush).await?;
    wait_confirmed(&mut a, "redelivery", end_c).await?;
    s.c.close(true).await?;
    inactive(&mut a, "redelivery").await?;
    let mut s = Stream::open(port, "redelivery", 0, "none").await?;
    s.flush = end_c;
    let no_replay = s.drain(e, 1.0, true).await?;
    e.check(
        "acknowledged_not_redelivered",
        ids(&no_replay).is_empty(),
        json!({"ids":ids(&no_replay),"confirmed":fmt(end_c),"requested_start":"0/0"}),
    )?;
    s.c.close(true).await?;
    inactive(&mut a, "redelivery").await?;
    a.query("INSERT INTO markers VALUES ('start_skip_D')")
        .await?;
    let high = lsn(&a.query("SELECT pg_current_wal_flush_lsn()").await?[0][0]);
    let mut s = Stream::open(port, "redelivery", high, "none").await?;
    let skipped = s.drain(e, 0.8, true).await?;
    e.check(
        "starting_lsn_can_skip",
        ids(&skipped).is_empty() && confirmed(&mut a, "redelivery").await? == end_c,
        json!({"requested":fmt(high),"confirmed":fmt(end_c)}),
    )?;
    s.c.close(true).await?;
    inactive(&mut a, "redelivery").await?;
    let mut s = Stream::open(port, "redelivery", 0, "none").await?;
    let recovered = s.commits(e, 1).await?;
    e.check(
        "skipped_unacked_recoverable",
        ids(&recovered) == vec!["start_skip_D"],
        json!({"ids":ids(&recovered)}),
    )?;
    let durable = e.durable(&recovered)?;
    s.flush = durable;
    s.c.status(s.write, durable).await?;
    wait_confirmed(&mut a, "redelivery", durable).await?;

    // Respond ONLY to requested primary keepalives for > 2 timeout intervals, holding flush.
    a.query("INSERT INTO markers VALUES ('durability_lag')")
        .await?;
    let lag = s.commits(e, 1).await?;
    let start = Instant::now();
    let before = s.requests;
    let idle = s.drain(e, 10.0, true).await?;
    let stats = a
        .query(&format!(
            "select state,write_lsn,flush_lsn from pg_stat_replication where pid={}",
            s.c.pid
        ))
        .await?;
    e.check("durable_flush_lag_liveness",s.requests>before&&s.write>durable&&confirmed(&mut a,"redelivery").await?==durable&&!stats.is_empty(),json!({"seconds":start.elapsed().as_secs_f64(),"timeout_seconds":4,"requested_replies":s.requests-before,"flush":fmt(durable),"write":fmt(s.write),"lag_commit":fmt(end(&lag)),"server":stats,"records_while_idle":idle.len()}))?;
    s.c.close(true).await?;
    inactive(&mut a, "redelivery").await?;
    let mut s = Stream::open(port, "redelivery", 0, "none").await?;
    let start = Instant::now();
    let timeout = s.drain(e, 9.0, false).await;
    e.check("no_keepalive_reply_times_out",timeout.is_err()&&s.requests>0&&start.elapsed()>=Duration::from_secs(3),json!({"seconds":start.elapsed().as_secs_f64(),"requests_ignored":s.requests,"error":timeout.err().map(|e|e.to_string())}))?;
    drop(s);
    inactive(&mut a, "redelivery").await?;

    // Incompressible 64 KiB text forces out-of-line TOAST.
    let pos = slot(&mut a, "toast").await?;
    a.query("INSERT INTO toast_default SELECT 1,0,string_agg(md5(g::text),'') FROM generate_series(1,2048) g").await?;
    a.query("INSERT INTO toast_full SELECT * FROM toast_default")
        .await?;
    let mut s = Stream::open(port, "toast", pos, "none").await?;
    s.commits(e, 2).await?;
    a.query("UPDATE toast_default SET n=n+1 WHERE id=1").await?;
    a.query("UPDATE toast_full SET n=n+1 WHERE id=1").await?;
    let updates = s.commits(e, 2).await?;
    let default = updates
        .iter()
        .find(|v| v["kind"] == "Update" && v["table"] == "toast_default")
        .context("missing default update")?;
    let full = updates
        .iter()
        .find(|v| v["kind"] == "Update" && v["table"] == "toast_full")
        .context("missing full update")?;
    let newcol = |v: &Value| {
        v["tuples"]
            .as_array()
            .unwrap()
            .iter()
            .find(|t| t["part"] == "N")
            .unwrap()["columns"][2]
            .clone()
    };
    e.check(
        "toast_default_marker",
        newcol(default)["kind"] == "u",
        json!({"update":default}),
    )?;
    e.check(
        "toast_full_observed",
        full["tuples"].as_array().unwrap().iter().any(|t| {
            t["part"] == "O" && t["columns"][2]["kind"] == "t" && t["columns"][2]["len"] == 65536
        }) && newcol(full)["kind"] == "u",
        json!({"update":full,"new_toast_column":newcol(full)}),
    )?;
    s.c.close(true).await?;

    // streaming off: no rows escape before COMMIT, then all 3000 rows arrive in one envelope.
    let pos = slot(&mut a, "spill_slot").await?;
    let mut s = Stream::open(port, "spill_slot", pos, "none").await?;
    let mut writer = admin(port).await?;
    writer.query("BEGIN; INSERT INTO spill SELECT g,repeat(md5(g::text),32) FROM generate_series(1,3000) g;").await?;
    let early = s.drain(e, 1.0, true).await?;
    e.check(
        "spill_no_precommit_rows",
        early.is_empty(),
        json!({"rows_before_commit":early.len(),"payload_bytes":3000*1024,"work_mem":"64kB"}),
    )?;
    writer.query("COMMIT").await?;
    let rows = s.commits(e, 1).await?;
    let count = rows
        .iter()
        .filter(|r| r["kind"] == "Insert" && r["table"] == "spill")
        .count();
    let distinct: BTreeSet<_> = rows
        .iter()
        .filter(|r| r["kind"] == "Insert" && r["table"] == "spill")
        .map(|r| {
            r["tuples"][0]["columns"][0]["text"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    let expected: HashMap<String, String> = a
        .query("select id::text,md5(body) from spill")
        .await?
        .into_iter()
        .map(|r| (r[0].clone(), r[1].clone()))
        .collect();
    let intact = rows
        .iter()
        .filter(|r| r["kind"] == "Insert" && r["table"] == "spill")
        .all(|r| {
            let cols = &r["tuples"][0]["columns"];
            cols[1]["len"] == 1024
                && expected
                    .get(cols[0]["text"].as_str().unwrap())
                    .map(String::as_str)
                    == cols[1]["md5"].as_str()
        });
    e.check("spill_intact_at_commit",count==3000&&distinct.len()==3000&&intact&&rows.iter().filter(|r|r["kind"]=="Begin").count()==1,json!({"inserts":count,"unique_ids":distinct.len(),"all_payload_lengths_and_hashes_match":intact,"commit":fmt(end(&rows))}))?;
    // Spill validation is read-only: no durable sink is built for these rows, so do not ACK them.
    s.c.close(true).await?;
    inactive(&mut a, "spill_slot").await?;
    let spill_stats=a.query("select spill_txns,spill_count,spill_bytes from pg_stat_replication_slots where slot_name='spill_slot'").await?;
    e.check(
        "spill_server_stats",
        spill_stats[0][0].parse::<u64>()? > 0 && spill_stats[0][2].parse::<u64>()? > 65536,
        json!({"stats":spill_stats}),
    )?;
    let size = a.query("select pg_database_size('skedaddle')").await?;
    e.check(
        "database_below_1gb",
        size[0][0].parse::<u64>()? <= 1_000_000_000,
        json!({"bytes":size[0][0]}),
    )?;
    writer.close(false).await?;
    a.close(false).await?;
    e.log(json!({"summary":{"passed":e.checks,"failed":0,"record_kinds":e.kinds,"pg_major":env::var("S1_MAJOR")?}}))?;
    Ok(())
}
#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    let mut e = Evidence::new()?;
    match run(&mut e).await {
        Ok(()) => {
            println!("S1 PASS: {} assertions", e.checks);
            Ok(())
        }
        Err(error) => {
            e.log(json!({"fatal":format!("{error:#}"),"passed":e.checks,"failed":1}))?;
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn lsn_roundtrip() {
        for n in [0, 1, u32::MAX as u64, u64::MAX] {
            assert_eq!(lsn(&fmt(n)), n);
        }
    }
    #[test]
    fn short_tuple_rejected() {
        let mut d = Decoder::default();
        assert!(d.decode(b"I\0\0\0\x01N\0\x01t\0\0\0\x05x").is_err());
    }
    #[test]
    fn unchanged_toast_decoded() {
        let mut d = Decoder::default();
        let v = d.decode(b"U\0\0\0\x01N\0\x01u").unwrap();
        assert_eq!(v["tuples"][0]["columns"][0]["kind"], "u");
    }
    #[test]
    fn unknown_streaming_tag_rejected() {
        assert!(Decoder::default().decode(b"S\0\0\0\x01\x01").is_err());
    }
}
