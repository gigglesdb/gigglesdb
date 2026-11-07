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
