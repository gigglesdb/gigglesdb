
// S2 source write fencing. Transport, SCRAM, pgoutput and flush controls above are reused from S1.
const APPS: &[&str] = &["s2_idle","s2_mid","s2_ro","s2_member","s2_setmember","s2_job","s2_pool_s","s2_pool_t","s2_load"];
fn quoted(names: &[String]) -> String { names.iter().map(|n| format!("\"{}\"",n.replace('"',"\"\""))).collect::<Vec<_>>().join(",") }
fn app_pred() -> String { APPS.iter().map(|s| format!("'{s}'")).collect::<Vec<_>>().join(",") }
async fn sql(a: &mut Conn, e: &mut Evidence, query: &str) -> Result<Vec<Vec<String>>> {
    let started=Instant::now();
    let result=a.query(query).await;
    e.log(json!({"sql":query,"elapsed_ms":started.elapsed().as_secs_f64()*1000.,"rows":result.as_ref().ok(),"error":result.as_ref().err().map(ToString::to_string)}))?;
    result
}
struct Saved { login: Vec<Vec<String>>, acl: Vec<Vec<String>>, grantees: Vec<String> }
async fn save(a: &mut Conn,e: &mut Evidence) -> Result<Saved> {
    let login=sql(a,e,&format!("SELECT rolname,rolcanlogin FROM pg_roles WHERE rolname IN ({}) ORDER BY 1",app_pred())).await?;
    // Conservative transitive closure: include INHERIT and SET ROLE paths (including NOINHERIT).
    let paths=format!("WITH RECURSIVE reachable(oid) AS (SELECT oid FROM pg_roles WHERE rolname IN ({}) UNION SELECT m.roleid FROM pg_auth_members m JOIN reachable r ON m.member=r.oid) ",app_pred());
    let acl=sql(a,e,&format!("{paths} SELECT n.nspname,c.relname,CASE WHEN x.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(x.grantee)::text END,x.privilege_type,x.is_grantable,pg_get_userbyid(x.grantor) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl,acldefault('r',c.relowner))) x WHERE c.oid IN ('public.markers'::regclass,'public.owner_markers'::regclass) AND x.privilege_type IN ('INSERT','UPDATE','DELETE','TRUNCATE') AND (x.grantee IN (SELECT oid FROM reachable) OR x.grantee=0) ORDER BY 1,2,3,4")).await?;
    let grantees=acl.iter().map(|r|r[2].clone()).collect::<BTreeSet<_>>().into_iter().collect();
    sql(a,e,&format!("{paths} SELECT r.rolname,r.rolinherit,has_table_privilege(r.oid,'markers','INSERT') FROM pg_roles r WHERE r.oid IN (SELECT oid FROM reachable) ORDER BY 1")).await?;
    Ok(Saved{login,acl,grantees})
}
async fn unfence(a: &mut Conn,e: &mut Evidence,s: &Saved) -> Result<()> {
    let mut query="BEGIN; ".to_string();
    for r in &s.login { query+=&format!("ALTER ROLE \"{}\" {}; ",r[0],if r[1]=="t" {"LOGIN"} else {"NOLOGIN"}); }
    for r in &s.acl {
        query+=&format!("SET LOCAL ROLE \"{}\"; GRANT {} ON \"{}\".\"{}\" TO {}{}; RESET ROLE; ",r[5],r[3],r[0],r[1],if r[2]=="PUBLIC" {"PUBLIC".into()} else {format!("\"{}\"",r[2])},if r[4]=="t" {" WITH GRANT OPTION"} else {""});
    }
    query+="COMMIT"; sql(a,e,&query).await?;
    let current=save(a,e).await?;
    e.check("saved_state_restored",current.login==s.login&&current.acl==s.acl,json!({"login":current.login,"acl":current.acl}))?;
    Ok(())
}
fn revoke(s: &Saved) -> String {
    let names=s.grantees.iter().map(|x|if x=="PUBLIC" {"PUBLIC".into()} else {format!("\"{x}\"")}).collect::<Vec<String>>().join(",");
    format!("REVOKE INSERT,UPDATE,DELETE,TRUNCATE ON public.markers,public.owner_markers FROM {names}")
}
struct Fence { drain:u64, marker:u64, pause_ms:f64, residual:Vec<Vec<String>> }
async fn fence(a: &mut Conn,e: &mut Evidence,s: &Saved,epoch:usize,deadline_ms:u64) -> Result<Option<Fence>> {
    let start=Instant::now();
    let mut step3="BEGIN; ".to_string();
    for name in APPS { step3+=&format!("ALTER ROLE {name} NOLOGIN; "); }
    step3+="COMMIT"; sql(a,e,&step3).await?;
    sql(a,e,&revoke(s)).await?;
    sql(a,e,&format!("SELECT pid,usename,pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=current_database() AND usename IN ({}) AND pid<>pg_backend_pid()",app_pred())).await?;
    let until=Instant::now()+Duration::from_millis(deadline_ms);
    loop {
        // Stronger than transactions alone: no application backend at all may remain.
        let blockers=sql(a,e,&format!("SELECT 'backend',pid::text,usename,COALESCE(state,''),COALESCE(xact_start::text,'') FROM pg_stat_activity WHERE datname=current_database() AND usename IN ({}) UNION ALL SELECT 'prepared',gid,owner::text,prepared::text,database::text FROM pg_prepared_xacts WHERE database=current_database()",app_pred())).await?;
        if blockers.is_empty() {break;}
        if Instant::now()>=until {
            e.log(json!({"fence_blocked":true,"epoch":epoch,"blockers":blockers,"elapsed_ms":start.elapsed().as_secs_f64()*1000.}))?;
            return Ok(None);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let drain=lsn(&sql(a,e,"SELECT pg_current_wal_insert_lsn()").await?[0][0]);
    let residual=sql(a,e,"SELECT r.rolname,r.rolsuper,r.rolbypassrls,n.nspname||'.'||c.relname,(r.oid=c.relowner) AS table_owner,has_table_privilege(r.oid,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE') AS acl_writer FROM pg_roles r CROSS JOIN pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid IN ('public.markers'::regclass,'public.owner_markers'::regclass) AND (r.rolsuper OR r.rolbypassrls OR r.oid=c.relowner OR has_table_privilege(r.oid,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE')) ORDER BY 1,4").await?;
    // Last privilege fence, authority/epoch and the transactional final marker are one transaction.
    let query=format!("BEGIN; {}; INSERT INTO _skedaddle.authority VALUES(1,'destination',{epoch}) ON CONFLICT(id) DO UPDATE SET source=excluded.source,epoch=excluded.epoch; SELECT pg_logical_emit_message(true,'s2-final','epoch={epoch};drain={}'); COMMIT",revoke(s),fmt(drain));
    let marker=lsn(&sql(a,e,&query).await?[0][0]);
    let pause_ms=start.elapsed().as_secs_f64()*1000.;
    e.log(json!({"fence":{"epoch":epoch,"drain":fmt(drain),"marker":fmt(marker),"pause_ms":pause_ms,"residual":residual}}))?;
    Ok(Some(Fence{drain,marker,pause_ms,residual}))
}
async fn app(port:u16,name:&str) -> Result<Conn> {
    tokio::time::timeout(Duration::from_secs(8),Conn::connect(port,false,name,"app")).await.context("app connect timeout")?
}
async fn insert(c: &mut Conn,id:&str,actor:&str,table:&str) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(8),c.query(&format!("INSERT INTO {table}(id,actor) VALUES('{id}','{actor}')"))).await.context("write timeout")??;
    Ok(())
}
struct Client { name:String, conn:Conn, table:String, mid:bool }
#[derive(Default)]
struct Writer { ack:Vec<String>, attempted:Vec<String>, errors:Vec<String>, elapsed:f64 }
async fn writer(port:u16,name:&'static str,prefix:String,stop:Arc<std::sync::atomic::AtomicBool>,delay_ms:u64) -> Result<Writer> {
    let mut c=app(port,name).await?;
    let start=Instant::now(); let mut r=Writer::default(); let mut i=0;
    while !stop.load(std::sync::atomic::Ordering::SeqCst) {
        let id=format!("{prefix}_{i}"); i+=1; r.attempted.push(id.clone());
        match insert(&mut c,&id,name,"markers").await { Ok(())=>r.ack.push(id), Err(error)=>{r.errors.push(error.to_string());break;} }
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
    r.elapsed=start.elapsed().as_secs_f64();
    Ok(r)
}
fn oracle(records:&[Value]) -> Vec<Value> {
    let mut pending=vec![]; let mut committed=vec![];
    for record in records {
        if record["kind"]=="Begin" {pending.clear();}
        if record["kind"]=="Insert" && (record["table"]=="markers" || record["table"]=="owner_markers") {
            pending.push(json!({"id":record["tuples"][0]["columns"][0]["text"],"actor":record["tuples"][0]["columns"][1]["text"],"table":record["table"],"xid":record["xid"]}));
        }
        if record["kind"]=="Commit" {
            for mut row in pending.drain(..) {row["commit"]=record["commit"].clone();committed.push(row);}
        }
    }
    committed
}
async fn run(e:&mut Evidence) -> Result<()> {
    let port:u16=env::var("S1_PG_PORT")?.parse()?;
    let session:u16=env::var("S2_SESSION_PORT")?.parse()?;
    let transaction:u16=env::var("S2_TRANSACTION_PORT")?.parse()?;
    let mut a=admin(port).await?;
    sql(&mut a,e,"SET statement_timeout='8s'; SET lock_timeout='2s'").await?;
    let mut setup="CREATE ROLE s2_writers NOLOGIN; CREATE ROLE s2_bridge NOLOGIN; GRANT s2_writers TO s2_bridge; ".to_string();
    for name in APPS {setup+=&format!("CREATE ROLE {name} LOGIN PASSWORD 'app'; ");}
    setup+="ALTER ROLE s2_setmember NOINHERIT; GRANT s2_bridge TO s2_member,s2_setmember; ALTER ROLE s2_ro SET default_transaction_read_only=on; CREATE ROLE s2_owner LOGIN PASSWORD 'app'; CREATE ROLE s2_out LOGIN PASSWORD 'app'; CREATE ROLE s2_bypass LOGIN BYPASSRLS PASSWORD 'app'; CREATE TABLE markers(id text PRIMARY KEY,actor text); CREATE TABLE owner_markers(id text PRIMARY KEY,actor text); ALTER TABLE owner_markers OWNER TO s2_owner; CREATE SCHEMA _skedaddle; CREATE TABLE _skedaddle.authority(id int PRIMARY KEY,source text,epoch bigint); INSERT INTO _skedaddle.authority VALUES(1,'source',0); CREATE PUBLICATION s1_pub FOR ALL TABLES; ";
    let direct=APPS.iter().filter(|n|!matches!(**n,"s2_member"|"s2_setmember")).map(|n|n.to_string()).collect::<Vec<_>>();
    setup+=&format!("GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE ON markers,owner_markers TO {},s2_writers; GRANT INSERT ON markers TO s2_out,s2_bypass; GRANT INSERT ON markers TO s2_idle WITH GRANT OPTION",quoted(&direct));
    sql(&mut a,e,&setup).await?;
    let saved=save(&mut a,e).await?;
    let membership_rows=sql(&mut a,e,"SELECT pg_get_userbyid(roleid),pg_get_userbyid(member),inherit_option,set_option FROM pg_auth_members ORDER BY 1,2").await?;
    e.check("membership_closure",saved.grantees.contains(&"s2_writers".into())&&!saved.grantees.contains(&"s2_member".into()),json!({"grantees":saved.grantees,"membership_rows":membership_rows}))?;
    let pos=slot(&mut a,"s2_detector").await?;
    let _breach_pos=slot(&mut a,"s2_breach").await?;
    let mut all_ack=BTreeSet::<String>::new();
    // Empirically isolate NOLOGIN's limit before exercising the complete fence.
    let mut old=app(port,"s2_idle").await?;
    sql(&mut a,e,"ALTER ROLE s2_idle NOLOGIN").await?;
    insert(&mut old,"preflight_nologin_existing","s2_idle","markers").await?;all_ack.insert("preflight_nologin_existing".into());
    let denied=app(port,"s2_idle").await;
    e.check("nologin_existing_connection_survives",denied.is_err(),json!({"existing_connection_committed":true,"new_connection_error":denied.err().map(|x|x.to_string())}))?;
    sql(&mut a,e,"ALTER ROLE s2_idle LOGIN").await?;drop(old);
    let mut member=app(port,"s2_member").await?;
    sql(&mut a,e,"REVOKE INSERT,UPDATE,DELETE,TRUNCATE ON markers,owner_markers FROM s2_member").await?;
    insert(&mut member,"preflight_naive_revoke","s2_member","markers").await?;all_ack.insert("preflight_naive_revoke".into());
    e.check("direct_revoke_misses_inherited_grant",true,json!({"acknowledged":1,"inherited_from":"s2_bridge -> s2_writers"}))?;
    sql(&mut a,e,"REVOKE INSERT,UPDATE,DELETE,TRUNCATE ON markers,owner_markers FROM s2_writers").await?;
    for query in ["INSERT INTO markers VALUES('must_deny','member')","UPDATE markers SET actor='bad'","DELETE FROM markers","TRUNCATE markers"] {
        let result=member.query(query).await;
        e.check("membership_all_dml_revoked",result.is_err(),json!({"sql":query,"error":result.err().map(|x|x.to_string())}))?;
    }
    drop(member);unfence(&mut a,e,&saved).await?;
    let mut records=vec![];let mut stream:Option<Stream>=None;
    let mut pauses=vec![];let mut scenario_results=vec![];let mut pool_errors=vec![];let mut trials=vec![];
    for trial in 0..11usize {
        let loaded=trial>=6; let full=trial==0;let prefix=format!("t{trial}");
        let mut clients=vec![];
        if full {
            for (name,p,mid) in [("s2_idle",port,false),("s2_mid",port,true),("s2_ro",port,false),("s2_member",port,false),("s2_setmember",port,false),("s2_pool_s",session,false),("s2_pool_t",transaction,false)] {
                let mut c=app(p,name).await?;
                if name=="s2_ro" {c.query("SET default_transaction_read_only=off; SET transaction_read_only=off").await?;}
                if name=="s2_setmember" {c.query("SET ROLE s2_writers").await?;}
                if mid {c.query("BEGIN; INSERT INTO markers VALUES('t0_mid_uncommitted','s2_mid')").await?;}
                else {let id=format!("{prefix}_before_{name}");insert(&mut c,&id,name,"markers").await?;all_ack.insert(id);}
                clients.push(Client{name:name.into(),conn:c,table:"markers".into(),mid});
            }
        }
        let stop=Arc::new(std::sync::atomic::AtomicBool::new(false));let mut tasks=vec![];
        if full {tasks.push(tokio::spawn(writer(port,"s2_job",format!("{prefix}_scheduled"),stop.clone(),10)));}
        if loaded { for i in 0..12 {tasks.push(tokio::spawn(writer(port,"s2_load",format!("{prefix}_load{i}"),stop.clone(),10)));} }
        tokio::time::sleep(Duration::from_millis(if loaded {1500} else if full {150} else {10})).await;
        let f=fence(&mut a,e,&saved,trial+1,1500).await?.context("unexpected drain deadline")?;
        pauses.push(json!({"trial":trial,"mode":if loaded {"load"} else if full {"scenarios"} else {"idle"},"ms":f.pause_ms}));
        stop.store(true,std::sync::atomic::Ordering::SeqCst);
        let mut trial_ack=vec![];let mut trial_attempted=vec![];let mut task_errors=vec![];let mut elapsed=0f64;
        for task in tasks {let r=task.await??;elapsed=elapsed.max(r.elapsed);all_ack.extend(r.ack.clone());trial_ack.extend(r.ack);trial_attempted.extend(r.attempted);task_errors.extend(r.errors);}
        if full {
            for mut c in clients {
                let id=format!("{prefix}_after_{}",c.name);
                let result=if c.mid {tokio::time::timeout(Duration::from_secs(8),c.conn.query("COMMIT")).await.context("commit timeout")?.map(|_|())} else {insert(&mut c.conn,&id,&c.name,&c.table).await};
                e.check(&format!("fenced_{}",c.name),result.is_err(),json!({"error":result.as_ref().err().map(ToString::to_string),"mid_transaction":c.mid,"drain":fmt(f.drain)}))?;
                scenario_results.push(json!({"scenario":c.name,"pass":result.is_err(),"client_error":result.err().map(|x|x.to_string()),"drain":fmt(f.drain)}));
            }
            for (name,p) in [("s2_pool_s",session),("s2_pool_t",transaction),("s2_idle",port),("s2_member",port),("s2_setmember",port),("s2_job",port)] {
                let connected=app(p,name).await;
                let authenticated=connected.is_ok();
                let result=match connected {Ok(mut c)=>insert(&mut c,&format!("{prefix}_reconnect_{name}"),name,"markers").await,Err(error)=>Err(error)};
                e.check(&format!("reconnect_denied_{name}"),result.is_err(),json!({"client_authenticated":authenticated,"write_error":result.as_ref().err().map(ToString::to_string)}))?;
                if p!=port {pool_errors.push(json!({"mode":if p==session {"session"} else {"transaction"},"client_authenticated":authenticated,"error":result.err().map(|x|x.to_string())}));}
            }
            e.check("scheduled_job_stopped",!trial_ack.is_empty()&&!task_errors.is_empty(),json!({"acknowledged_before_fence":trial_ack.len(),"errors":task_errors}))?;
            let mut breach=Stream::open(port,"s2_breach",f.drain,"none").await?;
            e.log(json!({"breach_detector_start":{"slot":"s2_breach","requested_lsn":fmt(f.drain),"origin":"none","tls":breach.c.tls,"scram":breach.c.scram}}))?;
            for (name,table) in [("postgres","markers"),("s2_owner","owner_markers"),("s2_out","markers"),("s2_bypass","markers")] {
                let mut c=if name=="postgres" {admin(port).await?} else {app(port,name).await?};
                let id=format!("{prefix}_residual_{name}");insert(&mut c,&id,name,table).await?;all_ack.insert(id.clone());
                scenario_results.push(json!({"scenario":format!("residual_{name}"),"id":id,"pass":true,"drain":fmt(f.drain)}));
            }
            let mut breach_records=vec![];let until=Instant::now()+Duration::from_secs(10);
            while oracle(&breach_records).len()<4 {
                breach_records.extend(breach.drain(e,0.2,true).await?);
                ensure!(Instant::now()<until,"breach detector timeout");
            }
            let hits=oracle(&breach_records);
            for name in ["postgres","s2_owner","s2_out","s2_bypass"] {
                let id=format!("{prefix}_residual_{name}");
                e.check(&format!("drain_start_detector_caught_{name}"),hits.iter().any(|r|r["id"]==id&&lsn(r["commit"].as_str().unwrap())>f.drain),json!({"requested_start":fmt(f.drain),"hits":hits,"origin":"none"}))?;
            }
            e.check("drain_start_detector_no_apps",hits.iter().all(|r|!APPS.contains(&r["actor"].as_str().unwrap())),json!({"hits":hits}))?;
            breach.c.close(true).await?;
            e.log(json!({"breach_detector_end":{"slot":"s2_breach","committed_inserts":hits.len()}}))?;
            e.check("residual_inventory",["postgres","s2_owner","s2_out","s2_bypass"].iter().all(|name|f.residual.iter().any(|r|&r[0]==name)),json!({"rows":f.residual}))?;
            // Open the detector only AFTER drain; replay uses its pre-workload slot so all three oracles can reconcile.
            let s=Stream::open(port,"s2_detector",pos,"none").await?;
            e.check("detector_tls_scram",s.c.scram,json!({"tls":s.c.tls,"origin":"none","start":fmt(pos),"opened_after_drain":fmt(f.drain)}))?;
            stream=Some(s);
        }
        let s=stream.as_mut().unwrap();
        let until=Instant::now()+Duration::from_secs(10);
        loop {
            records.extend(s.drain(e,0.2,true).await?);
            let committed_marker=records.iter().position(|r|r["kind"]=="Message"&&r["lsn"]==fmt(f.marker)).map(|at|records[at+1..].iter().any(|r|r["kind"]=="Commit")).unwrap_or(false);
            if committed_marker {break;}
            ensure!(Instant::now()<until,"detector did not reach committed fence marker {}",fmt(f.marker));
        }
        let decoded=oracle(&records);
        let breaches=decoded.iter().filter(|r|lsn(r["commit"].as_str().unwrap())>f.drain&&r["id"].as_str().unwrap().starts_with(&prefix)&&APPS.contains(&r["actor"].as_str().unwrap())).collect::<Vec<_>>();
        e.check("no_app_commit_after_drain",breaches.is_empty(),json!({"trial":trial,"drain":fmt(f.drain),"breaches":breaches}))?;
        if full {
            for name in ["postgres","s2_owner","s2_out","s2_bypass"] {
                let id=format!("{prefix}_residual_{name}");
                let hits=decoded.iter().filter(|r|r["id"]==id&&lsn(r["commit"].as_str().unwrap())>f.drain).collect::<Vec<_>>();
                e.check(&format!("detector_caught_{name}"),hits.len()==1,json!({"hits":hits,"drain":fmt(f.drain)}))?;
            }
            let mid_rows=sql(&mut a,e,"SELECT count(*) FROM markers WHERE id='t0_mid_uncommitted'").await?;
            e.check("mid_uncommitted_rolled_back",mid_rows[0][0]=="0",json!({"row_count":mid_rows}))?;
        }
        let marker_seen=records.iter().any(|r|r["kind"]=="Message"&&r["transactional"]==true&&r["lsn"]==fmt(f.marker));
        e.check("final_marker_observed",marker_seen,json!({"trial":trial,"lsn":fmt(f.marker)}))?;
        let pre_rows=sql(&mut a,e,&format!("SELECT id FROM markers WHERE id LIKE '{prefix}_load%' OR id LIKE '{prefix}_scheduled%' ORDER BY id")).await?;
        let db_ids=pre_rows.iter().map(|r|r[0].clone()).collect::<BTreeSet<_>>();let ack_set=trial_ack.iter().cloned().collect::<BTreeSet<_>>();
        let unknown=db_ids.difference(&ack_set).cloned().collect::<Vec<_>>();
        e.check("acknowledged_writes_present",ack_set.is_subset(&db_ids),json!({"trial":trial,"ack":ack_set.len(),"db":db_ids.len(),"unacknowledged_committed":unknown}))?;
        trials.push(json!({"trial":trial,"mode":if loaded {"load"} else if full {"scenarios"} else {"idle"},"ack":trial_ack.len(),"attempted":trial_attempted.len(),"database":db_ids.len(),"unacknowledged_committed":unknown,"client_seconds":elapsed,"ack_tps":if elapsed>0. {trial_ack.len() as f64/elapsed}else{0.},"drain":fmt(f.drain),"marker":fmt(f.marker),"pause_ms":f.pause_ms}));
        let window_end=sql(&mut a,e,"SELECT pg_current_wal_insert_lsn()").await?[0][0].clone();
        trials.last_mut().unwrap()["fence_window_end"]=json!(window_end);
        unfence(&mut a,e,&saved).await?;
        // All original access paths must work again, including the two pool modes.
        for (name,p) in [("s2_idle",port),("s2_member",port),("s2_setmember",port),("s2_ro",port),("s2_pool_s",session),("s2_pool_t",transaction)] {
            let id=format!("{prefix}_unfence_{name}");let start=Instant::now();let mut errors=vec![];
            loop {
                let result=async {
                    let mut c=app(p,name).await?;
                    if name=="s2_ro" {c.query("SET default_transaction_read_only=off; SET transaction_read_only=off").await?;}
                    if name=="s2_setmember" {c.query("SET ROLE s2_writers").await?;}
                    insert(&mut c,&id,name,"markers").await
                }.await;
                match result {Ok(())=>break,Err(error)=>{errors.push(error.to_string());if start.elapsed()>Duration::from_secs(20) {bail!("unfence failed {name}: {errors:?}");}}}
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            e.log(json!({"unfence_resumed":name,"elapsed_ms":start.elapsed().as_secs_f64()*1000.,"transient_errors":errors}))?;
            all_ack.insert(id);
        }
        e.check("unfence_writes_resume",true,json!({"trial":trial,"successful_paths":6}))?;
    }
    // Own named prepared transaction only: demonstrate a drain refusal without resolving an unrelated transaction.
    let mut prepared=admin(port).await?;
    prepared.query("BEGIN; INSERT INTO markers VALUES('own_prepared_never_committed','postgres'); PREPARE TRANSACTION 's2_owned_prepared_blocker'").await?;
    let before=sql(&mut a,e,"SELECT epoch FROM _skedaddle.authority").await?;
    let blocked=fence(&mut a,e,&saved,999,180).await?;
    let after=sql(&mut a,e,"SELECT epoch FROM _skedaddle.authority; SELECT gid,owner,database FROM pg_prepared_xacts WHERE database=current_database()").await?;
    e.check("prepared_transaction_blocks_authority",blocked.is_none()&&after[0]==before[0]&&after.iter().any(|r|r[0]=="s2_owned_prepared_blocker"),json!({"before":before,"after":after,"authority_epoch_999_not_written":true}))?;
    sql(&mut a,e,"ROLLBACK PREPARED 's2_owned_prepared_blocker'").await?; // Fixture-owned GID; no other's prepared transaction is touched.
    unfence(&mut a,e,&saved).await?;
    records.extend(stream.as_mut().unwrap().drain(e,0.8,true).await?);
    let decoded=oracle(&records);
    for trial in &trials {
        let lo=lsn(trial["drain"].as_str().unwrap());let hi=lsn(trial["fence_window_end"].as_str().unwrap());
        let breaches=decoded.iter().filter(|r|{let commit=lsn(r["commit"].as_str().unwrap());commit>lo&&commit<=hi&&APPS.contains(&r["actor"].as_str().unwrap())}).collect::<Vec<_>>();
        e.check("final_fence_window_oracle",breaches.is_empty(),json!({"trial":trial["trial"],"drain":fmt(lo),"end":fmt(hi),"app_breaches":breaches}))?;
    }
    let mut xid=0u64;let mut authority_xid=None;let mut marker_xid=None;let mut atomic_epochs=0;
    for record in &records {
        if record["kind"]=="Begin" {xid=record["xid"].as_u64().unwrap();authority_xid=None;marker_xid=None;}
        if record["table"]=="authority" {authority_xid=Some(xid);}
        if record["kind"]=="Message"&&record["prefix"]=="s2-final" {marker_xid=Some(xid);}
        if record["kind"]=="Commit"&&authority_xid==Some(xid)&&marker_xid==Some(xid) {atomic_epochs+=1;}
    }
    e.check("authority_and_final_marker_atomic",atomic_epochs==11,json!({"transactions_with_authority_and_transactional_marker":atomic_epochs,"expected":11}))?;
    let rows=sql(&mut a,e,"SELECT id,actor,'markers' FROM markers UNION ALL SELECT id,actor,'owner_markers' FROM owner_markers ORDER BY 1").await?;
    let db=rows.iter().map(|r|r[0].clone()).collect::<BTreeSet<_>>();let wal=decoded.iter().map(|r|r["id"].as_str().unwrap().to_string()).collect::<BTreeSet<_>>();
    let unacknowledged=db.difference(&all_ack).cloned().collect::<Vec<_>>();
    e.check("oracle_reconciliation",all_ack.is_subset(&db)&&db==wal&&decoded.len()==db.len(),json!({"client_acknowledged":all_ack.len(),"database_rows":db.len(),"detector_inserts":decoded.len(),"detector_unique_ids":wal.len(),"unacknowledged_committed":unacknowledged,"db_minus_wal":db.difference(&wal).collect::<Vec<_>>(),"wal_minus_db":wal.difference(&db).collect::<Vec<_>>()}))?;
    let durable=e.durable(&records)?;let s=stream.as_mut().unwrap();s.flush=durable;s.c.status(s.write,s.flush).await?;
    let size=sql(&mut a,e,"SELECT pg_database_size(current_database()); SHOW server_version; SELECT count(*) FROM pg_prepared_xacts WHERE database=current_database()").await?;
    e.check("database_below_1gb",size[0][0].parse::<u64>()?<1_000_000_000,json!({"bytes":size[0][0]}))?;
    e.check("no_prepared_left",size[2][0]=="0",json!({"rows":size}))?;
    let summary=json!({"major":env::var("S1_MAJOR")?,"checks":e.checks,"scenarios":scenario_results,"pool_errors":pool_errors,"pauses":pauses,"trials":trials,"client_acknowledged":all_ack.len(),"database_rows":db.len(),"detector_inserts":decoded.len(),"unacknowledged_committed":unacknowledged,"database_bytes":size[0][0],"detector_origin":"none","after_fence_definition":"commit_lsn > drain_lsn"});
    let path=std::path::Path::new(&env::var("S1_OUT")?).join("summary.json");let mut out=OpenOptions::new().write(true).create_new(true).open(path)?;serde_json::to_writer_pretty(&mut out,&summary)?;out.flush()?;out.sync_all()?;
    e.log(json!({"summary":summary}))?;
    stream.take().unwrap().c.close(true).await?;
    println!("S2 PASS: {} assertions; ack={} db={} detector={}",e.checks,all_ack.len(),db.len(),decoded.len());
    Ok(())
}
#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider().install_default().ok();
    let mut e=Evidence::new()?;
    match run(&mut e).await {Ok(())=>Ok(()),Err(error)=>{e.log(json!({"fatal":format!("{error:#}"),"passed":e.checks,"failed":1}))?;Err(error)}}
}
