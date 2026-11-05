//! Trusted stdin controls plus separately configured external native task TLS.
use giggles_consensus::authority::{Binding, LeaderClock, OsClock};
use giggles_consensus::{
    transport::{Configuration, Event, Transport},
    *,
};
use serde::Deserialize;
use serde_json::json;
use std::{
    collections::BTreeMap,
    io::{BufRead, Read, Write},
    time::Duration,
};
use tokio::sync::mpsc;

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum Control {
    Propose {
        id: u64,
        command: Command,
    },
    ReloadCredentials {
        id: u64,
        expected: u64,
        bindings: Vec<Binding>,
    },
    Authenticate {
        fingerprint: String,
    },
    NativeDecide {
        id: u64,
        session: u64,
        action: TaskAction,
    },
    NativeRelease {
        id: u64,
        decision: u64,
    },
    Read {
        id: u64,
    },
    Checkpoint,
    Diagnostics,
    PausePeers {
        paused: bool,
    },
    Campaign,
    CrashNextApply {
        after_commit: bool,
    },
}
fn emit(v: serde_json::Value) {
    println!("{v}");
    let _ = std::io::stdout().flush();
}
#[tokio::main]
async fn main() -> Result<()> {
    let handle = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || handle.block_on(run())).await?
}
async fn run() -> Result<()> {
    let config: Configuration = serde_json::from_slice(&std::fs::read(
        std::env::args().nth(1).ok_or("config path required")?,
    )?)?;
    let store = if config.database.exists() {
        Store::open(&config.database, config.id, config.domain)?
    } else {
        Store::create(&config.database, config.id, config.domain)?
    };
    let mut node = Node::new(store)?;
    let (events, mut incoming) = mpsc::channel(64);
    let (native_events, mut native_input) = mpsc::channel(32);
    if let Some(task) = config.task.clone() {
        if task.listen == config.listen {
            return Err("task and peer listeners must differ".into());
        }
        native::bind(
            task,
            config
                .peers
                .values()
                .map(|p| p.fingerprint.clone())
                .collect(),
            native_events,
        )
        .await?;
    }
    let mut native_actor = native::Actor::new(config.id)?;
    let transport = Transport::bind(config).await?;
    let outbound = transport.run(events);
    let (controls, mut input) = mpsc::channel(16);
    std::thread::spawn(move || {
        let stdin = std::io::stdin();
        let mut reader = stdin.lock();
        loop {
            let mut line = Vec::new();
            match reader
                .by_ref()
                .take((MAX_COMMAND + 1) as u64)
                .read_until(b'\n', &mut line)
            {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    if line.len() > MAX_COMMAND {
                        break;
                    }
                    if controls.blocking_send(line).is_err() {
                        break;
                    }
                }
            }
        }
    });
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    let mut reads = BTreeMap::new();
    let mut peers_paused = false;
    let parameter = |name: &str, default: u64| -> Result<u64> {
        Ok(std::env::var(name)
            .ok()
            .map(|v| v.parse())
            .transpose()?
            .unwrap_or(default))
    };
    let mut clock = LeaderClock::new(
        OsClock::default(),
        parameter("GIGGLES_CLOCK_SKEW_SECONDS", 1)?,
        parameter("GIGGLES_CLOCK_MAX_PAUSE_SECONDS", 10)?,
    )?;
    let mut clock_term = 0;
    let mut sessions = BTreeMap::new();
    let mut next_session = 0u64;
    let mut decisions = std::collections::BTreeSet::new();
    let mut releases = BTreeMap::new();
    let mut held = BTreeMap::new();
    emit(json!({"ready":true,"diagnostics":node.diagnostics()}));
    loop {
        tokio::select! {
         _=tick.tick()=>{
             node.tick();
             // Observe even while idle or following, so the pause bound measures
             // actor scheduling gaps rather than time since the last request.
             if !clock.is_fenced()
                 && let Err(e)=clock.interval(node.store.authority()?.watermark,0) {
                 eprintln!("runtime clock unavailable: {e}");
             }
         },
         Some(event)=native_input.recv()=>native_actor.handle(event,&mut node,&mut clock),
         Some(event)=incoming.recv()=>match event{Event::Message(from,m)=>if !peers_paused && let Err(e)=node.step(from,m){eprintln!("peer rejected: {e}");},Event::SnapshotResult(to,ok)=>node.report_snapshot(to,ok)},
         Some(line)=input.recv()=>{match serde_json::from_slice::<Control>(&line){
          Ok(Control::Propose{id,command})=>{let result=node.propose(id.to_be_bytes().to_vec(),&command);if let Err(e)=result{emit(json!({"id":id,"error":e.to_string(),"leader":node.leader()}));}},
          Ok(Control::ReloadCredentials{id,expected,bindings})=>{if let Err(e)=node.reload_credentials(id.to_be_bytes().to_vec(),expected,bindings){emit(json!({"id":id,"error":e.to_string()}));}},
          Ok(Control::Authenticate{fingerprint})=>{if sessions.len()>=32{emit(json!({"error":"session limit"}));}else{match node.authenticate(&fingerprint){Ok(session)=>{next_session=next_session.checked_add(1).ok_or("session overflow")?;sessions.insert(next_session,session);emit(json!({"session":next_session}));},Err(e)=>emit(json!({"error":e.to_string()}))}}},
          Ok(Control::NativeDecide{id,session,action})=>{let result=(||->Result<()>{if held.len()+decisions.len()+releases.len()>=32 || decisions.contains(&id) || held.contains_key(&id) || releases.contains_key(&id){return Err("held/pending limit or duplicate id".into());}node.decide(id.to_be_bytes().to_vec(),sessions.get(&session).ok_or("unknown session")?,action,&mut clock)?;decisions.insert(id);Ok(())})();if let Err(e)=result{emit(json!({"id":id,"error":e.to_string()}));}},
          Ok(Control::NativeRelease{id,decision})=>{let result=(||->Result<()>{if releases.contains_key(&id) || decisions.contains(&id) || held.contains_key(&id){return Err("duplicate id".into());}let value=held.remove(&decision).ok_or("unknown held decision")?;node.request_release(id.to_be_bytes().to_vec(),&value,&mut clock)?;releases.insert(id,value);Ok(())})();if let Err(e)=result{emit(json!({"id":id,"error":e.to_string()}));}},
          Ok(Control::Read{id})=>{if reads.len()>=32{emit(json!({"id":id,"error":"read queue full"}));}else{let ctx=id.to_be_bytes().to_vec();match node.read_barrier(ctx.clone()){Ok(())=>{reads.insert(ctx,id);},Err(e)=>emit(json!({"id":id,"error":e.to_string(),"leader":node.leader()}))}}},
          Ok(Control::Checkpoint)=>match node.store.checkpoint(){Ok(s)=>emit(json!({"checkpoint":s.get_metadata().index})),Err(e)=>emit(json!({"error":e.to_string()}))},
          Ok(Control::PausePeers{paused})=>{peers_paused=paused;emit(json!({"peers_paused":paused}));},
          Ok(Control::Diagnostics)=>emit(json!({"diagnostics":node.diagnostics(),"leader":node.leader(),"is_leader":node.is_leader(),"authority_ready":node.is_leader() && !clock.is_fenced() && node.store.authority()?.term==node.term()})),
          Ok(Control::Campaign)=>{node.campaign()?;},
          Ok(Control::CrashNextApply{after_commit})=>{node.store.arm_apply_crash(if after_commit{store::ApplyCrash::AfterCommit}else{store::ApplyCrash::BeforeCommit})?;emit(json!({"crash_armed":true}));},
          Err(e)=>emit(json!({"error":e.to_string()})),
         }},
        }
        if node.is_leader() && node.term() != clock_term {
            match node.establish_time(Vec::new(), &mut clock) {
                Ok(()) => clock_term = node.term(),
                Err(e) => {
                    eprintln!("leader time unavailable: {e}");
                    clock_term = node.term();
                }
            }
        }
        native_actor.reap(&node);
        let output = node.drain()?;
        for m in output.messages {
            if peers_paused {
                continue;
            }
            let to = m.to;
            let snapshot = m.has_snapshot();
            if outbound.try_send(m).is_err() && snapshot {
                node.report_snapshot(to, false);
            }
        }
        for a in output.applied {
            let Some(a) = native_actor.applied(a, &mut node, &mut clock) else {
                continue;
            };
            if a.context.len() == 8 {
                let id = u64::from_be_bytes(a.context.as_slice().try_into()?);
                if decisions.remove(&id) {
                    let index = a.index;
                    match node.hold(a) {
                        Ok(value) => {
                            held.insert(id, value);
                            emit(json!({"id":id,"held":index}));
                        }
                        Err(e) => emit(json!({"id":id,"error":e.to_string()})),
                    }
                } else if let Some(value) = releases.remove(&id) {
                    let result = node
                        .release_proof(a)
                        .and_then(|proof| node.finish_release(value, proof, &mut clock));
                    match result {
                        Ok(reply) => emit(json!({"id":id,"released":true,"reply":reply})),
                        Err(e) => emit(json!({"id":id,"error":e.to_string()})),
                    }
                } else if !a.is_authenticated() {
                    emit(json!({"id":id,"applied":a.index,"reply":a.reply}));
                }
            }
        }
        for (ctx, index) in output.reads {
            if let Some(id) = reads.remove(&ctx) {
                let image = node.store.image()?;
                let (_, reply) =
                    image.evaluate(&Command::Admin(br#"{"command":"inspect"}"#.to_vec()))?;
                emit(
                    json!({"id":id,"barrier":index,"applied":node.store.applied()?,"reply":reply}),
                );
            }
        }
    }
}
