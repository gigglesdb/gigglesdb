//! Opaque loopback TCP forwarding with bounded application buffers and event storage.
use crate::Result;
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::Write,
    net::SocketAddr,
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Semaphore, watch},
    task::{JoinHandle, JoinSet},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    Upstream,
    Downstream,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Fault {
    Pass,
    Delay { millis: u64 },
    Partition,
    Disconnect,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProxyLimits {
    pub connections: usize,
    pub buffer_bytes: usize,
    pub events: usize,
    pub connect_millis: u64,
    pub lifetime_millis: u64,
    pub idle_millis: u64,
}
impl Default for ProxyLimits {
    fn default() -> Self {
        Self {
            connections: 16,
            buffer_bytes: 4096,
            events: 4096,
            connect_millis: 1000,
            lifetime_millis: 30000,
            idle_millis: 10000,
        }
    }
}
impl ProxyLimits {
    pub fn validate(&self) -> Result<()> {
        if !(1..=64).contains(&self.connections)
            || !(1..=65536).contains(&self.buffer_bytes)
            || !(1..=65536).contains(&self.events)
            || !(1..=1000).contains(&self.connect_millis)
            || !(1..=30000).contains(&self.lifetime_millis)
            || !(1..=30000).contains(&self.idle_millis)
        {
            return Err("proxy limits out of bounds".into());
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Event {
    pub sequence: u64,
    pub micros: u64,
    pub connection: u64,
    pub direction: Option<Direction>,
    pub kind: String,
    pub bytes: usize,
    pub fault: Option<Fault>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Trace {
    pub events: Vec<Event>,
    pub overflow: bool,
}
struct Log {
    start: Instant,
    cap: usize,
    trace: Mutex<Trace>,
    file: Option<Mutex<File>>,
}
impl Log {
    fn event(
        &self,
        connection: u64,
        direction: Option<Direction>,
        kind: &str,
        bytes: usize,
        fault: Option<Fault>,
    ) {
        let mut t = self.trace.lock().expect("trace mutex");
        if t.events.len() == self.cap {
            t.overflow = true;
            return;
        }
        let sequence = t.events.len() as u64;
        t.events.push(Event {
            sequence,
            micros: self.start.elapsed().as_micros() as u64,
            connection,
            direction,
            kind: kind.into(),
            bytes,
            fault,
        });
        if let Some(file) = &self.file {
            let result = (|| -> Result<()> {
                let mut f = file.lock().map_err(|_| "event file mutex")?;
                serde_json::to_writer(&mut *f, t.events.last().expect("just pushed"))?;
                f.write_all(b"\n")?;
                f.flush()?;
                Ok(())
            })();
            if result.is_err() {
                t.overflow = true;
            }
        }
    }
    fn overflow(&self) -> bool {
        self.trace.lock().expect("trace mutex").overflow
    }
}
#[derive(Clone, Copy)]
struct Control {
    faults: [Fault; 2],
    cut: u64,
    stop: bool,
}
fn index(d: Direction) -> usize {
    match d {
        Direction::Upstream => 0,
        Direction::Downstream => 1,
    }
}
pub struct Proxy {
    address: SocketAddr,
    control: watch::Sender<Control>,
    log: Arc<Log>,
    task: Option<JoinHandle<()>>,
}
impl Proxy {
    pub async fn start(upstream: SocketAddr, limits: ProxyLimits) -> Result<Self> {
        Self::start_inner(upstream, limits, None, Instant::now()).await
    }
    /// Retain bounded JSONL events as they occur, including incomplete failed runs.
    pub async fn start_retained(
        upstream: SocketAddr,
        limits: ProxyLimits,
        path: &Path,
    ) -> Result<Self> {
        Self::start_retained_at(upstream, limits, path, Instant::now()).await
    }
    /// Share a monotonic origin with the campaign's invocation recorder.
    pub async fn start_retained_at(
        upstream: SocketAddr,
        limits: ProxyLimits,
        path: &Path,
        start: Instant,
    ) -> Result<Self> {
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        Self::start_inner(upstream, limits, Some(file), start).await
    }
    async fn start_inner(
        upstream: SocketAddr,
        limits: ProxyLimits,
        file: Option<File>,
        start: Instant,
    ) -> Result<Self> {
        limits.validate()?;
        if !upstream.ip().is_loopback() || upstream.port() == 0 {
            return Err("upstream must be loopback with nonzero port".into());
        }
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (control, rx) = watch::channel(Control {
            faults: [Fault::Pass; 2],
            cut: 0,
            stop: false,
        });
        let log = Arc::new(Log {
            file: file.map(Mutex::new),
            start,
            cap: limits.events,
            trace: Mutex::new(Trace {
                events: Vec::new(),
                overflow: false,
            }),
        });
        let task = tokio::spawn(serve(listener, upstream, limits, rx, log.clone()));
        Ok(Self {
            address,
            control,
            log,
            task: Some(task),
        })
    }
    pub fn address(&self) -> SocketAddr {
        self.address
    }
    pub fn set(&self, direction: Direction, fault: Fault) -> Result<()> {
        if matches!(fault,Fault::Delay{millis} if millis>10000) {
            return Err("delay exceeds 10 seconds".into());
        }
        // Serialize control records with state publication across concurrent setters.
        self.control.send_modify(|c| {
            self.log
                .event(0, Some(direction), "control", 0, Some(fault));
            c.faults[index(direction)] = fault;
            if fault == Fault::Disconnect {
                c.cut += 1;
            }
        });
        Ok(())
    }
    pub fn trace(&self) -> Trace {
        self.log.trace.lock().expect("trace mutex").clone()
    }
    pub async fn stop(mut self) -> Trace {
        self.control.send_modify(|c| c.stop = true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
        self.trace()
    }
}
impl Drop for Proxy {
    fn drop(&mut self) {
        self.control.send_modify(|c| c.stop = true);
    }
}
async fn serve(
    listener: TcpListener,
    upstream: SocketAddr,
    limits: ProxyLimits,
    mut control: watch::Receiver<Control>,
    log: Arc<Log>,
) {
    let permits = Arc::new(Semaphore::new(limits.connections));
    let mut children = JoinSet::new();
    let mut id = 0;
    loop {
        tokio::select! { biased;
            _=control.changed()=>{if control.borrow().stop {break;}}
            Some(_)=children.join_next(), if !children.is_empty()=>{}
            incoming=listener.accept()=>{
                let Ok((socket,_))=incoming else {break}; id+=1;
                let current=*control.borrow();
                let permit=permits.clone().try_acquire_owned();
                if log.overflow() || permit.is_err() || current.faults.contains(&Fault::Disconnect) {
                    log.event(id,None,"rejected",0,None); continue;
                }
                let permit=permit.expect("checked permit");
                log.event(id,None,"accepted",0,None);
                let l=log.clone();let c=control.clone();let lim=limits.clone();
                children.spawn(async move {
                    let _permit=permit;
                    let cancel=c.clone(); let cut=current.cut;
                    let reason=tokio::select! {
                        result=tokio::time::timeout(Duration::from_millis(lim.lifetime_millis), forward(socket,upstream,&lim,c,l.clone(),id))=>match result { Ok(Ok(()))=>"closed",Ok(Err(_))=>"io_error",Err(_)=>"lifetime" },
                        _=cancelled(cancel,cut)=>"disconnected",
                    };
                    l.event(id,None,reason,0,None);
                });
            }
        }
    }
    // Each child observes stop and records its terminal event before joining.
    while children.join_next().await.is_some() {}
    log.event(0, None, "stopped", 0, None);
}
async fn cancelled(mut control: watch::Receiver<Control>, cut: u64) {
    loop {
        if control.borrow().stop || control.borrow().cut != cut {
            return;
        }
        if control.changed().await.is_err() {
            return;
        }
    }
}
async fn forward(
    socket: TcpStream,
    upstream: SocketAddr,
    limits: &ProxyLimits,
    control: watch::Receiver<Control>,
    log: Arc<Log>,
    id: u64,
) -> Result<()> {
    let remote = tokio::time::timeout(
        Duration::from_millis(limits.connect_millis),
        TcpStream::connect(upstream),
    )
    .await??;
    socket.set_nodelay(true)?;
    remote.set_nodelay(true)?;
    let (a, b) = socket.into_split();
    let (c, d) = remote.into_split();
    tokio::try_join!(
        pump(
            a,
            d,
            Direction::Upstream,
            limits,
            control.clone(),
            log.clone(),
            id
        ),
        pump(c, b, Direction::Downstream, limits, control, log, id)
    )?;
    Ok(())
}
async fn pump<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    mut read: R,
    mut write: W,
    direction: Direction,
    limits: &ProxyLimits,
    mut control: watch::Receiver<Control>,
    log: Arc<Log>,
    id: u64,
) -> Result<()> {
    let mut buffer = vec![0; limits.buffer_bytes];
    loop {
        let n = tokio::time::timeout(
            Duration::from_millis(limits.idle_millis),
            read.read(&mut buffer),
        )
        .await??;
        if n == 0 {
            write.shutdown().await?;
            log.event(id, Some(direction), "eof", 0, None);
            return Ok(());
        }
        log.event(id, Some(direction), "read", n, None);
        loop {
            let fault = control.borrow_and_update().faults[index(direction)];
            match fault {
                Fault::Pass => break,
                Fault::Disconnect => return Err("disconnected".into()),
                Fault::Partition => {
                    control.changed().await?;
                }
                Fault::Delay { millis } => {
                    tokio::select! { _=tokio::time::sleep(Duration::from_millis(millis))=>break, r=control.changed()=>{r?;} }
                }
            }
        }
        // A blocked socket write is still bounded by the connection lifetime.
        write.write_all(&buffer[..n]).await?;
        log.event(id, Some(direction), "forwarded", n, None);
    }
}
