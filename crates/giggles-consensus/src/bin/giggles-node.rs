//! Trusted stdin control only. Network listener accepts authenticated Raft peers.
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
    Propose { id: u64, command: Command },
    Read { id: u64 },
    Checkpoint,
    Diagnostics,
    Campaign,
    CrashNextApply { after_commit: bool },
}
fn emit(v: serde_json::Value) {
    println!("{v}");
    let _ = std::io::stdout().flush();
}
#[tokio::main]
async fn main() -> Result<()> {
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
    emit(json!({"ready":true,"diagnostics":node.diagnostics()}));
    loop {
        tokio::select! {
         _=tick.tick()=>node.tick(),
         Some(event)=incoming.recv()=>match event{Event::Message(from,m)=>if let Err(e)=node.step(from,m){eprintln!("peer rejected: {e}");},Event::SnapshotResult(to,ok)=>node.report_snapshot(to,ok)},
         Some(line)=input.recv()=>{match serde_json::from_slice::<Control>(&line){
          Ok(Control::Propose{id,command})=>{let result=node.propose(id.to_be_bytes().to_vec(),&command);if let Err(e)=result{emit(json!({"id":id,"error":e.to_string(),"leader":node.leader()}));}},
          Ok(Control::Read{id})=>{if reads.len()>=32{emit(json!({"id":id,"error":"read queue full"}));}else{let ctx=id.to_be_bytes().to_vec();match node.read_barrier(ctx.clone()){Ok(())=>{reads.insert(ctx,id);},Err(e)=>emit(json!({"id":id,"error":e.to_string(),"leader":node.leader()}))}}},
          Ok(Control::Checkpoint)=>match node.store.checkpoint(){Ok(s)=>emit(json!({"checkpoint":s.get_metadata().index})),Err(e)=>emit(json!({"error":e.to_string()}))},
          Ok(Control::Diagnostics)=>emit(json!({"diagnostics":node.diagnostics(),"leader":node.leader(),"is_leader":node.is_leader()})),
          Ok(Control::Campaign)=>{node.campaign()?;},
          Ok(Control::CrashNextApply{after_commit})=>{node.store.arm_apply_crash(if after_commit{store::ApplyCrash::AfterCommit}else{store::ApplyCrash::BeforeCommit})?;emit(json!({"crash_armed":true}));},
          Err(e)=>emit(json!({"error":e.to_string()})),
         }},
        }
        let output = node.drain()?;
        for m in output.messages {
            let to = m.to;
            let snapshot = m.has_snapshot();
            if outbound.try_send(m).is_err() && snapshot {
                node.report_snapshot(to, false);
            }
        }
        for a in output.applied {
            if a.context.len() == 8 {
                let id = u64::from_be_bytes(a.context.as_slice().try_into()?);
                emit(json!({"id":id,"applied":a.index,"reply":a.reply}));
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
