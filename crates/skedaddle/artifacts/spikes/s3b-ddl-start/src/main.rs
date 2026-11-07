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
        let mut f=OpenOptions::new().create(true).append(true).open(std::path::Path::new(&env::var("S1_OUT")?).join("commands.jsonl"))?;
        writeln!(f,"{}",json!({"pid":self.pid,"sql":q}))?;

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
async fn admin(port:u16)->Result<Conn>{Conn::connect(port,false,"postgres","skedaddle").await}
fn end(r:&[Value])->u64{lsn(r.iter().rev().find(|v|v["kind"]=="Commit").unwrap()["end"].as_str().unwrap())}
async fn finish_query(c:&mut Conn)->Result<()> {
    let mut err=None;
    loop {let(t,p)=c.frame().await?;if t==b'E'{err=Some(String::from_utf8_lossy(&p).to_string());}if t==b'Z'{break}}
    if let Some(err)=err{bail!("{err}")}Ok(())
}
fn messages(r:&[Value])->Vec<Value>{
    let mut out=vec![];let mut xid=Value::Null;let mut commit=Value::Null;
    for v in r {if v["kind"]=="Begin"{xid=v["xid"].clone();commit=v["final_lsn"].clone();}
        if v["kind"]=="Message"&&v["prefix"]=="skedaddle" {
            let mut m:Value=serde_json::from_str(v["content"].as_str().unwrap()).unwrap();
            m["message_lsn"]=v["lsn"].clone();m["envelope_xid"]=xid.clone();m["commit_lsn"]=commit.clone();out.push(m);
        }
    }out
}
fn envelopes(r:&[Value])->bool{
    let mut xid=None;let mut last=0;let mut final_lsn=0;
    for v in r {match v["kind"].as_str().unwrap(){
        "Begin"=>{if xid.is_some(){return false}xid=v["xid"].as_u64();final_lsn=lsn(v["final_lsn"].as_str().unwrap());},
        "Message"=>{if xid.is_none()||v["transactional"]!=true||lsn(v["lsn"].as_str().unwrap())>final_lsn{return false}
            let m:Value=serde_json::from_str(v["content"].as_str().unwrap()).unwrap();if m["xid"].as_str().unwrap().parse::<u64>().unwrap()!=xid.unwrap(){return false}},
        "Commit"=>{let n=lsn(v["commit"].as_str().unwrap());if xid!=v["xid"].as_u64()||n!=final_lsn||n<=last{return false}last=n;xid=None;},_=>{}
    }}xid.is_none()
}
fn pairs(m:&[Value],n:usize,same_txn:bool)->bool{
    m.len()==2*n&&m.chunks(2).all(|p|p[0]["ddl"]=="start"&&p[1]["ddl"]=="finish"&&p[0]["id"]==p[1]["id"]&&p[0]["tag"]==p[1]["tag"]&&
        (p[0]["envelope_xid"]==p[1]["envelope_xid"])==same_txn&&lsn(p[0]["message_lsn"].as_str().unwrap())<lsn(p[1]["message_lsn"].as_str().unwrap()))
}
async fn collect(a:&mut Conn,s:&mut Stream,e:&mut Evidence,name:&str)->Result<Vec<Value>>{
    let marker=format!("barrier_{name}");a.query(&format!("INSERT INTO markers VALUES('{marker}')")).await?;
    let until=Instant::now()+Duration::from_secs(20);let mut r=vec![];let mut found=false;
    loop {if let Some(v)=tokio::time::timeout(until.saturating_duration_since(Instant::now()),s.receive(e,true)).await.context("barrier timeout")?? {
        if v["kind"]=="Insert"&&v["table"]=="markers"&&v["tuples"][0]["columns"][0]["text"]==marker {found=true}
        let done=found&&v["kind"]=="Commit";r.push(v);if done{break}
    }}Ok(r)
}
async fn ordinary(a:&mut Conn,s:&mut Stream,e:&mut Evidence,name:&str,sql:&str,n:usize)->Result<()> {
    a.query(sql).await?;let r=collect(a,s,e,name).await?;let m=messages(&r);
    let all_same=m.iter().all(|v|v["envelope_xid"]==m[0]["envelope_xid"]);
    e.log(json!({"case":name,"sql":sql,"messages":m,"records":r}))?;
    e.check(name,pairs(&m,n,true)&&all_same&&envelopes(&r),json!({"messages":m}))?;
    if name=="add_column" {e.check("start_helper_returns_empty",m[0]["start_helper_rows"]==0&&m[0]["start_helper_error"].is_null(),json!({"start":m[0]}))?;}
    Ok(())
}
async fn wait_catalog(a:&mut Conn,sql:&str)->Result<Value>{
    for _ in 0..150 {let r=a.query(sql).await?;if !r.is_empty(){return Ok(serde_json::from_str(&r[0][0])?)}tokio::time::sleep(Duration::from_millis(40)).await;}
    bail!("catalog transition not observed: {sql}")
}
async fn split(a:&mut Conn,b:&mut Conn,c:&mut Conn,s:&mut Stream,e:&mut Evidence,name:&str,kind:&str,fail:bool)->Result<()> {
    let detach=kind=="detach";
    let (sql,catalog)=if detach {
        c.query("BEGIN; SELECT count(*) FROM part").await?;
        ("ALTER TABLE part DETACH PARTITION child CONCURRENTLY".to_string(),"SELECT jsonb_build_object('relation','child'::regclass::oid,'parent',inhparent,'xmin',xmin::text,'pending',inhdetachpending,'observer_lsn',pg_current_wal_lsn()::text)::text FROM pg_inherits WHERE inhrelid='child'::regclass AND inhdetachpending".to_string())
    }else{
        c.query("BEGIN; UPDATE target SET val=val WHERE id=1").await?;
        let unique=kind=="unique";let index=format!("{name}_idx");
        (format!("CREATE {}INDEX CONCURRENTLY {index} ON target(val)",if unique{"UNIQUE "}else{""}),
        format!("SELECT jsonb_build_object('relation',i.indrelid,'index',i.indexrelid,'xmin',i.xmin::text,'class_xmin',cl.xmin::text,'valid',i.indisvalid,'ready',i.indisready,'live',i.indislive,'observer_lsn',pg_current_wal_lsn()::text)::text FROM pg_index i JOIN pg_class cl ON cl.oid=i.indexrelid WHERE cl.relname='{index}' AND NOT i.indisvalid"))
    };
    b.send_query(&sql).await?;
    let first=wait_catalog(a,&catalog).await?;
    let waiting=a.query(&format!("SELECT wait_event_type,wait_event FROM pg_stat_activity WHERE pid={}",b.pid)).await?;
    let early=collect(a,s,e,&format!("{name}_early")).await?;let em=messages(&early);
    let first_ok=em.len()==1&&em[0]["ddl"]=="start"&&em[0]["xid"]==first["xmin"]&&
        lsn(em[0]["message_lsn"].as_str().unwrap())<lsn(em[0]["commit_lsn"].as_str().unwrap())&&
        lsn(em[0]["commit_lsn"].as_str().unwrap())<=lsn(first["observer_lsn"].as_str().unwrap())&&envelopes(&early);
    e.check(&format!("{name}_first_commit"),first_ok,json!({"catalog":first,"early":em,"waiting":waiting}))?;
    let error=if fail&&detach {
        a.query(&format!("SELECT pg_cancel_backend({})",b.pid)).await?;
        let result=tokio::time::timeout(Duration::from_secs(15),finish_query(b)).await?;
        c.query("COMMIT").await?;result.err().map(|x|format!("{x:#}"))
    }else{
        if fail {c.query("INSERT INTO target(id,val) VALUES(99,1)").await?;}
        c.query("COMMIT").await?;
        tokio::time::timeout(Duration::from_secs(15),finish_query(b)).await?.err().map(|x|format!("{x:#}"))
    };
    let late=collect(a,s,e,&format!("{name}_late")).await?;let lm=messages(&late);
    let saved=b.query("SELECT current_setting('s3b.command_id',true)").await?;
    let after=if detach {a.query("SELECT inhdetachpending::text,xmin::text FROM pg_inherits WHERE inhrelid='child'::regclass").await?}
        else{a.query(&format!("SELECT indisvalid::text,indisready::text,indislive::text,xmin::text FROM pg_index WHERE indexrelid='{name}_idx'::regclass")).await?};
    let mut records=early;records.extend(late);let m=messages(&records);
    e.log(json!({"case":name,"sql":sql,"first_catalog":first,"waiting":waiting,"early_messages":em,"late_messages":lm,"messages":m,"records":records,"error":error,"session_id_after":saved,"after_catalog":after}))?;
    if fail {
        let signal=if detach{after.first().map(|r|r[0].as_str())==Some("true")}else{after.first().map(|r|r[0].as_str())==Some("false")};
        e.check(&format!("{name}_unmatched"),error.is_some()&&m.len()==1&&lm.is_empty()&&signal&&envelopes(&records),json!({"error":error,"catalog":after,"messages":m}))?;
    }else{
        let valid=if detach{after.is_empty()}else{after.first().map(|r|r[0].as_str())==Some("true")};
        e.check(&format!("{name}_finish"),error.is_none()&&pairs(&m,1,false)&&lm.len()==1&&saved[0][0]==m[0]["id"].as_str().unwrap()&&valid&&envelopes(&records),json!({"messages":m,"catalog":after,"error":error}))?;
    }
    Ok(())
}
async fn run(e:&mut Evidence)->Result<()> {
    let port=env::var("S1_PG_PORT")?.parse()?;let mut a=admin(port).await?;let mut b=admin(port).await?;let mut c=admin(port).await?;
    a.query("CREATE TABLE markers(id text PRIMARY KEY); CREATE TABLE target(id int PRIMARY KEY,val int); INSERT INTO target VALUES(1,1),(2,2); CREATE TABLE victim(id int); CREATE TABLE part(id int) PARTITION BY RANGE(id); CREATE TABLE child PARTITION OF part FOR VALUES FROM(0) TO(10); CREATE PUBLICATION s3_pub FOR TABLE markers,target,part").await?;
    a.query(include_str!("../trigger.sql")).await?;
    let pos=lsn(&a.query("SELECT lsn FROM pg_create_logical_replication_slot('s3b_slot','pgoutput')").await?[0][0]);
    let mut s=Stream::open(port,"s3b_slot",pos,"any").await?;
    e.log(json!({"server":a.query("SELECT version()").await?,"probe":"S3 Conn/SCRAM/pgoutput v1; transactional messages true; streaming off"}))?;
    ordinary(&mut a,&mut s,e,"add_column","ALTER TABLE target ADD COLUMN extra text",1).await?;
    ordinary(&mut a,&mut s,e,"create_index","CREATE INDEX ordinary_idx ON target(id)",1).await?;
    ordinary(&mut a,&mut s,e,"drop_index","DROP INDEX ordinary_idx",1).await?;
    ordinary(&mut a,&mut s,e,"drop_table","DROP TABLE victim",1).await?;
    ordinary(&mut a,&mut s,e,"multistmt","BEGIN; ALTER TABLE target ADD COLUMN multi_a int; CREATE INDEX multi_idx ON target(multi_a); ALTER TABLE target ADD COLUMN multi_b text; COMMIT",3).await?;
    b.query("BEGIN; ALTER TABLE target ADD COLUMN rollback_col int").await?;
    let r=collect(&mut a,&mut s,e,"before_rollback").await?;
    e.check("no_markers_before_commit",messages(&r).is_empty(),json!({"records":r}))?;
    b.query("ROLLBACK").await?;
    let r=collect(&mut a,&mut s,e,"after_rollback").await?;
    let absent=a.query("SELECT count(*) FROM pg_attribute WHERE attrelid='target'::regclass AND attname='rollback_col'").await?[0][0]=="0";
    e.check("rollback_no_markers",messages(&r).is_empty()&&absent,json!({"records":r,"column_absent":absent}))?;
    split(&mut a,&mut b,&mut c,&mut s,e,"detach_success","detach",false).await?;
    ordinary(&mut a,&mut s,e,"reattach","ALTER TABLE part ATTACH PARTITION child FOR VALUES FROM(0) TO(10)",1).await?;
    split(&mut a,&mut b,&mut c,&mut s,e,"cic_nonunique","nonunique",false).await?;
    split(&mut a,&mut b,&mut c,&mut s,e,"cic_unique","unique",false).await?;
    ordinary(&mut a,&mut s,e,"drop_unique","DROP INDEX cic_unique_idx",1).await?;
    split(&mut a,&mut b,&mut c,&mut s,e,"cic_failure","unique",true).await?;
    split(&mut a,&mut b,&mut c,&mut s,e,"detach_cancel","detach",true).await?;
    ordinary(&mut a,&mut s,e,"finalize","ALTER TABLE part DETACH PARTITION child FINALIZE",1).await?;
    e.check("finalize_clears_pending",a.query("SELECT count(*) FROM pg_inherits WHERE inhrelid='child'::regclass").await?[0][0]=="0",json!({"recovery":"FINALIZE emits its own start/finish id; original cancelled start requires explicit recovery association"}))?;
    ordinary(&mut a,&mut s,e,"reattach_strict","ALTER TABLE part ATTACH PARTITION child FOR VALUES FROM(0) TO(10)",1).await?;
    b.query("SET s3b.strict='on'").await?;
    for (name,sql,check_sql) in [
        ("strict_detach","ALTER TABLE part DETACH PARTITION child CONCURRENTLY","SELECT jsonb_build_object('parent',inhparent,'child',inhrelid,'pending',inhdetachpending,'xmin',xmin::text)::text FROM pg_inherits WHERE inhrelid='child'::regclass"),
        ("strict_cic","CREATE INDEX CONCURRENTLY blocked_idx ON target(val)","SELECT count(*) FROM pg_class WHERE relname='blocked_idx'"),
        ("strict_unique_cic","CREATE UNIQUE INDEX CONCURRENTLY blocked_unique_idx ON target(val)","SELECT count(*) FROM pg_class WHERE relname='blocked_unique_idx'")
    ] {
        let before=a.query(check_sql).await?;let result=b.query(sql).await;let error=result.err().map(|x|format!("{x:#}"));let after=a.query(check_sql).await?;
        let r=collect(&mut a,&mut s,e,name).await?;
        e.check(name,error.as_ref().map(|x|x.contains("SKEDADDLE strict: DDL class blocked before execution; split-transaction forms not permitted")).unwrap_or(false)&&before==after&&messages(&r).is_empty(),json!({"sql":sql,"error":error,"before":before,"after":after,"records":r}))?;
    }
    b.query("SET s3b.strict='off'").await?;
    // TG_TAG is identical for ordinary/CIC and ordinary/CONCURRENTLY/FINALIZE ALTER TABLE.
    let helper=a.query("SELECT pg_event_trigger_ddl_commands()").await.err().map(|x|format!("{x:#}"));
    e.log(json!({"helper_outside_event":helper}))?;
    let size=a.query("SELECT datname,pg_database_size(oid) FROM pg_database ORDER BY datname").await?;
    e.check("db_cap",size.iter().all(|r|r[1].parse::<u64>().unwrap()<=1_000_000_000),json!({"databases":size}))?;
    s.c.close(true).await?;b.close(false).await?;c.close(false).await?;a.close(false).await?;
    e.log(json!({"summary":{"passed":e.checks-e.failures,"failed":e.failures,"major":env::var("S3_MAJOR")?}}))?;
    ensure!(e.failures==0,"{} failed assertions",e.failures);Ok(())
}
#[tokio::main]
async fn main()->Result<()>{let mut e=Evidence::new()?;match run(&mut e).await {Ok(())=>{println!("S3b PASS {} assertions",e.checks);Ok(())},Err(err)=>{e.log(json!({"fatal":format!("{err:#}"),"checks":e.checks}))?;Err(err)}}}
