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
    failures: usize,
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
            failures: 0,
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
        if !yes { self.failures += 1; }
        self.checks += 1;
        println!("{} {name}", if yes {"PASS"} else {"FAIL"});
        Ok(())
    }
}
struct Conn {
    io: TcpStream,
    incoming: BytesMut,
    pid: u32,
    tls: String,
    scram: bool,
}
impl Conn {
    async fn connect(port: u16, replication: bool, user: &str, password: &str) -> Result<Self> {
        let io = TcpStream::connect(("127.0.0.1", port)).await?;
        io.set_nodelay(true)?;
        let tls = "plaintext-local-loopback".to_string();
        let mut b = BytesMut::new();
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
        self.send_query(&format!("START_REPLICATION SLOT {slot} LOGICAL {} (proto_version '{proto}', publication_names 's3_pub', messages 'true', origin '{origin}', streaming 'off')",fmt(pos))).await?;
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
async fn admin(port: u16) -> Result<Conn> { Conn::connect(port,false,"postgres","skedaddle").await }
fn end(r:&[Value])->u64{lsn(r.iter().rev().find(|v|v["kind"]=="Commit").unwrap()["end"].as_str().unwrap())}
async fn fingerprint(a:&mut Conn)->Result<String>{Ok(a.query(include_str!("../fingerprint.sql")).await?[0][0].clone())}
async fn finish_query(c:&mut Conn)->Result<()> {
    let mut err=None;
    loop {let (t,p)=c.frame().await?;if t==b'E'{err=Some(String::from_utf8_lossy(&p).to_string());}if t==b'Z'{break}}
    if let Some(err)=err{bail!("{err}")}Ok(())
}
async fn target_snapshot(a:&mut Conn)->Result<Value>{
    let r=a.query("SELECT jsonb_build_object('filenode',c.relfilenode,'rows',(SELECT coalesce(jsonb_agg(to_jsonb(t)),'[]'::jsonb) FROM public.target t),'columns',(SELECT jsonb_agg(jsonb_build_object('attnum',a.attnum,'name',a.attname,'type',format_type(a.atttypid,a.atttypmod),'notnull',a.attnotnull,'dropped',a.attisdropped,'identity',a.attidentity,'generated',a.attgenerated,'missing',a.atthasmissing,'missing_value',a.attmissingval::text,'default',pg_get_expr(d.adbin,d.adrelid)) ORDER BY a.attnum) FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE a.attrelid=c.oid AND a.attnum>0))::text FROM pg_class c WHERE c.oid='public.target'::regclass").await?;
    Ok(serde_json::from_str(&r[0][0])?)
}
fn markers(r:&[Value])->Vec<String>{r.iter().filter(|v|v["kind"]=="Insert"&&v["table"]=="markers").filter_map(|v|v["tuples"][0]["columns"][0]["text"].as_str().map(str::to_string)).collect()}
fn messages(r:&[Value])->Vec<Value>{r.iter().filter(|v|v["kind"]=="Message"&&v["prefix"]=="skedaddle").map(|v|serde_json::from_str(v["content"].as_str().unwrap()).unwrap()).collect()}
fn envelopes(r:&[Value])->bool{
    let mut xid=None; let mut last=0; let mut final_lsn=0;
    for v in r {match v["kind"].as_str().unwrap(){
        "Begin"=>{if xid.is_some(){return false} xid=Some(v["xid"].as_u64().unwrap()); final_lsn=lsn(v["final_lsn"].as_str().unwrap());},
        "Message"=>{if xid.is_none()||v["transactional"]!=true||lsn(v["lsn"].as_str().unwrap())>final_lsn{return false}
            if v["prefix"]=="skedaddle" {let p:Value=serde_json::from_str(v["content"].as_str().unwrap()).unwrap(); if p["xid"].as_str().unwrap().parse::<u64>().unwrap()!=xid.unwrap(){return false}}},
        "Commit"=>{let n=lsn(v["commit"].as_str().unwrap());if xid!=v["xid"].as_u64()||n!=final_lsn||n<=last{return false} last=n;xid=None;},_=>{}
    }} xid.is_none()
}
async fn collect(a:&mut Conn,s:&mut Stream,e:&mut Evidence,name:&str)->Result<Vec<Value>>{
    let marker=format!("barrier_{name}"); a.query(&format!("INSERT INTO markers VALUES ('{marker}')")).await?;
    let until=Instant::now()+Duration::from_secs(20); let mut r=vec![]; let mut found=false;
    loop {
        if let Some(v)=tokio::time::timeout(until.saturating_duration_since(Instant::now()),s.receive(e,true)).await.context("barrier timeout")?? {
            if markers(std::slice::from_ref(&v)).contains(&marker){found=true}
            let done=found&&v["kind"]=="Commit";r.push(v);if done{break}
        }
    } Ok(r)
}
async fn case(a:&mut Conn,s:&mut Stream,e:&mut Evidence,name:&str,sql:&str,seen:usize,drift:bool)->Result<()> {
    let before=fingerprint(a).await?;let target_before=target_snapshot(a).await?;
    let result=a.query(sql).await; let ok=result.is_ok(); let error=result.as_ref().err().map(|v|format!("{v:#}"));
    if !ok {a.query("ROLLBACK").await?;}
    let after=fingerprint(a).await?;let target_after=target_snapshot(a).await?;
    let r=collect(a,s,e,name).await?;let m=messages(&r);
    e.log(json!({"case":name,"sql":sql,"success":ok,"error":error,"fingerprint_before":before,"fingerprint_after":after,"drift":before!=after,"target_before":target_before,"target_after":target_after,"messages":m,"records":r,"envelopes":envelopes(&r)}))?;
    e.check(name,ok&&m.len()==seen&&(before!=after)==drift&&envelopes(&r),json!({"messages":m.len(),"expected_messages":seen,"drift":before!=after,"expected_drift":drift,"error":error}))
}
async fn run(e:&mut Evidence)->Result<()> {
    let port=env::var("S1_PG_PORT")?.parse()?;let mut a=admin(port).await?;
    a.query("CREATE TABLE markers(id text PRIMARY KEY); CREATE TABLE target(id int PRIMARY KEY); INSERT INTO target VALUES(1); CREATE TABLE victim(id int); CREATE TABLE part(id int) PARTITION BY RANGE(id); CREATE TABLE child(LIKE part); CREATE TYPE mood AS ENUM ('a','b'); CREATE ROLE grant_target; CREATE FUNCTION public.noop() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN END $$; CREATE PUBLICATION s3_pub FOR TABLE markers,target,part; ").await?;
    a.query(include_str!("../trigger.sql")).await?;
    let pos=lsn(&a.query("SELECT lsn FROM pg_create_logical_replication_slot('s3_slot','pgoutput')").await?[0][0]);
    let mut s=Stream::open(port,"s3_slot",pos,"any").await?;
    e.log(json!({"server":a.query("SELECT version();").await?,"probe_base":"S1 Conn framing, SCRAM authentication, pgoutput v1 Decoder and Stream; local loopback TCP"}))?;
    // One DDL transaction stays open while unrelated DML commits around it.
    let mut b=admin(port).await?; let mut c=admin(port).await?;
    a.query("INSERT INTO markers VALUES('before')").await?;
    b.query("BEGIN; ALTER TABLE target ADD COLUMN ordered text; INSERT INTO markers VALUES('ddl')").await?;
    c.query("BEGIN; INSERT INTO markers VALUES('concurrent'); COMMIT").await?;
    let mut r=s.drain(e,0.6,true).await?;
    e.check("no_ddl_before_commit",messages(&r).is_empty()&&markers(&r)==vec!["before","concurrent"],json!({"records":r}))?;
    b.query("COMMIT").await?;a.query("INSERT INTO markers VALUES('after')").await?;
    r.extend(collect(&mut a,&mut s,e,"order").await?);
    e.log(json!({"ordering_records":r}))?;
    e.check("commit_order",markers(&r)==vec!["before","concurrent","ddl","after","barrier_order"]&&messages(&r).len()==1&&envelopes(&r),json!({"markers":markers(&r),"messages":messages(&r)}))?;
    c.query("BEGIN; INSERT INTO markers VALUES('dml_started_first')").await?;
    b.query("BEGIN; ALTER TABLE target ADD COLUMN ordered_second int; INSERT INTO markers VALUES('ddl_committed_first'); COMMIT").await?;
    c.query("COMMIT").await?;
    let r=collect(&mut a,&mut s,e,"reverse_order").await?;
    e.log(json!({"reverse_ordering_records":r}))?;
    e.check("reverse_commit_order",markers(&r)==vec!["ddl_committed_first","dml_started_first","barrier_reverse_order"]&&messages(&r).len()==1&&envelopes(&r),json!({"markers":markers(&r)}))?;
    // Version updates and emit_message both undo with transaction/subtransaction rollback.
    let v=a.query("SELECT version FROM s3.schema_version").await?[0][0].clone();
    case(&mut a,&mut s,e,"rollback","BEGIN; ALTER TABLE target ADD COLUMN rolled_back int; ROLLBACK",0,false).await?;
    e.check("version_rollback",a.query("SELECT version FROM s3.schema_version").await?[0][0]==v,json!({"before":v}))?;
    case(&mut a,&mut s,e,"savepoint","BEGIN; SAVEPOINT p; ALTER TABLE target ADD COLUMN savepoint_gone int; ROLLBACK TO p; INSERT INTO markers VALUES('savepoint_survivor'); COMMIT",0,false).await?;
    e.check("version_savepoint",a.query("SELECT version FROM s3.schema_version").await?[0][0]==v,json!({"before":v}))?;
    case(&mut a,&mut s,e,"savepoint_surviving_ddl","BEGIN; ALTER TABLE target ADD COLUMN savepoint_kept_a int; SAVEPOINT p; ALTER TABLE target ADD COLUMN savepoint_gone int; ROLLBACK TO p; ALTER TABLE target ADD COLUMN savepoint_kept_b int; COMMIT",2,true).await?;
    case(&mut a,&mut s,e,"multistmt","BEGIN; INSERT INTO markers VALUES('multi_first'); ALTER TABLE target ADD COLUMN multi_a int; INSERT INTO markers VALUES('multi_middle'); ALTER TABLE target ADD COLUMN multi_b text DEFAULT 'constant'; INSERT INTO markers VALUES('multi_last'); COMMIT",2,true).await?;
    case(&mut a,&mut s,e,"multi_action","ALTER TABLE target ADD COLUMN ma int, ADD COLUMN mb text DEFAULT 'x'",1,true).await?;
    case(&mut a,&mut s,e,"nullable","ALTER TABLE target ADD COLUMN nullable_new text",1,true).await?;
    case(&mut a,&mut s,e,"constant_default","ALTER TABLE target ADD COLUMN default_new int DEFAULT 7",1,true).await?;
    case(&mut a,&mut s,e,"stable_default","ALTER TABLE target ADD COLUMN stable_new timestamptz DEFAULT now()",1,true).await?;
    case(&mut a,&mut s,e,"notnull_default","ALTER TABLE target ADD COLUMN constrained_new int NOT NULL DEFAULT 7",1,true).await?;
    case(&mut a,&mut s,e,"volatile_default","ALTER TABLE target ADD COLUMN volatile_new float8 DEFAULT random()",1,true).await?;
    case(&mut a,&mut s,e,"ctas","CREATE TABLE ctas AS SELECT id FROM target",1,true).await?;
    case(&mut a,&mut s,e,"select_into","SELECT id INTO selected FROM target",1,true).await?;
    case(&mut a,&mut s,e,"attach","ALTER TABLE part ATTACH PARTITION child FOR VALUES FROM(0) TO(10)",1,true).await?;
    case(&mut a,&mut s,e,"detach","ALTER TABLE part DETACH PARTITION child",1,true).await?;
    case(&mut a,&mut s,e,"reattach","ALTER TABLE part ATTACH PARTITION child FOR VALUES FROM(0) TO(10)",1,true).await?;
    // Hold a parent-table reader through DETACH's first internal commit.
    let before=fingerprint(&mut a).await?;
    c.query("BEGIN; SELECT count(*) FROM part").await?;
    b.send_query("ALTER TABLE part DETACH PARTITION child CONCURRENTLY").await?;
    let mut pending=false;
    for _ in 0..100 {
        if a.query("SELECT inhdetachpending FROM pg_inherits WHERE inhrelid='child'::regclass").await?.first().map(|r|r[0].as_str())==Some("t"){pending=true;break}
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let intermediate=fingerprint(&mut a).await?;
    let mut records=s.drain(e,0.2,true).await?;let early=messages(&records).len();
    let waiting=a.query(&format!("SELECT wait_event_type,wait_event FROM pg_stat_activity WHERE pid={}",b.pid)).await?;
    c.query("COMMIT").await?;
    tokio::time::timeout(Duration::from_secs(15),finish_query(&mut b)).await??;
    let after=fingerprint(&mut a).await?;
    records.extend(collect(&mut a,&mut s,e,"detach_concurrently").await?);
    e.log(json!({"case":"detach_concurrently","sql":"ALTER TABLE part DETACH PARTITION child CONCURRENTLY","success":true,"messages":messages(&records),"records":records,"drift":before!=after,"fingerprint_before":before,"fingerprint_intermediate":intermediate,"fingerprint_after":after,"pending_before_message":pending,"early_messages":early,"waiting":waiting,"envelopes":envelopes(&records)}))?;
    e.check("detach_concurrently",pending&&before!=intermediate&&early==0&&messages(&records).len()==1&&before!=after&&envelopes(&records),json!({"pending":pending,"early_messages":early,"messages":messages(&records).len(),"waiting":waiting}))?;
    case(&mut a,&mut s,e,"rename","ALTER TABLE target RENAME COLUMN nullable_new TO renamed_new",1,true).await?;
    case(&mut a,&mut s,e,"rename_table","ALTER TABLE victim RENAME TO victim_renamed",1,true).await?;
    case(&mut a,&mut s,e,"drop","DROP TABLE victim_renamed",1,true).await?;
    case(&mut a,&mut s,e,"drop_column","BEGIN; INSERT INTO markers VALUES('unsafe_before_ddl'); ALTER TABLE target DROP COLUMN ma; INSERT INTO markers VALUES('unsafe_after_ddl'); COMMIT",1,true).await?;
    case(&mut a,&mut s,e,"alter_type_enum","ALTER TYPE mood ADD VALUE 'c'",1,true).await?;
    case(&mut a,&mut s,e,"alter_column_type","ALTER TABLE target ALTER COLUMN mb TYPE varchar(40)",1,true).await?;
    case(&mut a,&mut s,e,"create_index_concurrently","CREATE INDEX CONCURRENTLY target_ordered_idx ON target(ordered)",1,true).await?;
    case(&mut a,&mut s,e,"truncate","TRUNCATE target",0,false).await?;
    case(&mut a,&mut s,e,"comment","COMMENT ON TABLE target IS 'observed comment'",1,true).await?;
    case(&mut a,&mut s,e,"grant","GRANT SELECT ON target TO grant_target",1,true).await?;
    case(&mut a,&mut s,e,"revoke","REVOKE SELECT ON target FROM grant_target",1,true).await?;
    case(&mut a,&mut s,e,"create_role","CREATE ROLE invisible_role",0,true).await?;
    case(&mut a,&mut s,e,"alter_role","ALTER ROLE invisible_role LOGIN",0,true).await?;
    case(&mut a,&mut s,e,"role_membership","GRANT grant_target TO invisible_role",0,true).await?;
    case(&mut a,&mut s,e,"create_database","CREATE DATABASE invisible_database",0,true).await?;
    case(&mut a,&mut s,e,"alter_database","ALTER DATABASE invisible_database CONNECTION LIMIT 3",0,true).await?;
    case(&mut a,&mut s,e,"tablespace","ALTER TABLESPACE pg_default SET(random_page_cost=1.1)",0,true).await?;
    case(&mut a,&mut s,e,"parameter_privileges","GRANT SET ON PARAMETER work_mem TO invisible_role",0,true).await?;
    case(&mut a,&mut s,e,"alter_system","ALTER SYSTEM SET work_mem='5MB'",0,true).await?;
    case(&mut a,&mut s,e,"create_event_trigger","CREATE EVENT TRIGGER invisible_evt ON ddl_command_end EXECUTE FUNCTION public.noop()",0,true).await?;
    case(&mut a,&mut s,e,"alter_event_trigger","ALTER EVENT TRIGGER invisible_evt DISABLE",0,true).await?;
    case(&mut a,&mut s,e,"drop_event_trigger","DROP EVENT TRIGGER invisible_evt",0,true).await?;
    case(&mut a,&mut s,e,"app_upgrade",include_str!("../miniflux-upgrade.sql"),2,true).await?;
    e.check("app_version",a.query("SELECT version FROM miniflux.schema_version").await?[0][0]=="13",json!({"upgrade":"Miniflux 2.0.2 schema 12 -> 2.0.3 schema 13"}))?;
    case(&mut a,&mut s,e,"disable_boundary","ALTER EVENT TRIGGER s3_boundary DISABLE",0,true).await?;
    case(&mut a,&mut s,e,"fallback_while_disabled","ALTER TABLE target RENAME COLUMN mb TO renamed_again",0,true).await?;
    case(&mut a,&mut s,e,"fallback_reverted_change","BEGIN; ALTER TABLE target RENAME COLUMN renamed_again TO transient_name; ALTER TABLE target RENAME COLUMN transient_name TO renamed_again; COMMIT",0,false).await?;
    case(&mut a,&mut s,e,"enable_boundary","ALTER EVENT TRIGGER s3_boundary ENABLE",0,true).await?;
    let size=a.query("SELECT datname,pg_database_size(oid) FROM pg_database").await?;
    e.check("db_cap",size.iter().all(|v|v[1].parse::<u64>().unwrap()<=1_000_000_000),json!({"databases":size}))?;
    s.c.close(true).await?;b.close(false).await?;c.close(false).await?;a.close(false).await?;
    e.log(json!({"summary":{"passed":e.checks-e.failures,"failed":e.failures,"major":env::var("S3_MAJOR")?}}))?;
    ensure!(e.failures==0,"{} assertions failed",e.failures);
    Ok(())
}
#[tokio::main]
async fn main()->Result<()>{
    let mut e=Evidence::new()?;
    match run(&mut e).await {Ok(())=>{println!("S3 PASS {} assertions",e.checks);Ok(())},Err(err)=>{e.log(json!({"fatal":format!("{err:#}"),"passed":e.checks}))?;Err(err)}}
}
