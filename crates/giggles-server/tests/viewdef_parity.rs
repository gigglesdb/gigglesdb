#![cfg(feature = "engine-rocksdb")]
use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{Service, auth::{PostgresqlConfig, TlsMode, serve_postgresql}, clock::Clock};
use std::{collections::BTreeMap, sync::Arc};
#[tokio::test]
async fn oracle_views_calling_forms_corpus_and_lock_sequences() {
    let path = std::env::temp_dir().join(format!("viewdef-parity-{}-{}", std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    admin.roles().create(NewRole { name: "owner".into(), login: true, superuser: true, ..Default::default() }).unwrap();
    let service = Service::new(admin, vec![], Clock::new(0,3600).unwrap()).unwrap();
    let config = PostgresqlConfig { listen: "127.0.0.1:0".parse().unwrap(), tls: TlsMode::Disabled,
        certificate: None, private_key: None, client_ca: None, certificates: BTreeMap::new(),
        databases: vec!["postgres".into()], loopback_trust: true, connections: 8 };
    let running = serve_postgresql(config.listen, service.clone(), Arc::new(config.build().unwrap())).await.unwrap();
    let (client, connection) = tokio_postgres::Config::new().hostaddr(running.address.ip()).port(running.address.port())
        .user("owner").dbname("postgres").connect(tokio_postgres::NoTls).await.unwrap();
    let connection = tokio::spawn(connection);
    client.batch_execute("CREATE TABLE vt(id int PRIMARY KEY,v text,n numeric,d date,b boolean); CREATE TABLE vu(id int PRIMARY KEY,vt_id int,note text)").await.unwrap();
    for (name,select,plain,pretty) in [
        ("v1","id,v FROM vt"," SELECT id,\n    v\n   FROM vt;"," SELECT id,\n    v\n   FROM vt;"),
        ("v2","id,v AS label FROM vt WHERE id>0"," SELECT id,\n    v AS label\n   FROM vt\n  WHERE (id > 0);"," SELECT id,\n    v AS label\n   FROM vt\n  WHERE id > 0;"),
        ("v3","* FROM vt"," SELECT id,\n    v,\n    n,\n    d,\n    b\n   FROM vt;"," SELECT id,\n    v,\n    n,\n    d,\n    b\n   FROM vt;"),
        ("v4","id,v FROM vt WHERE (id>0 AND v<>'x') OR b"," SELECT id,\n    v\n   FROM vt\n  WHERE (((id > 0) AND (v <> 'x'::text)) OR b);"," SELECT id,\n    v\n   FROM vt\n  WHERE id > 0 AND v <> 'x'::text OR b;"),
        ("v10","id FROM vt WHERE NOT b AND v IS NULL"," SELECT id\n   FROM vt\n  WHERE ((NOT b) AND (v IS NULL));"," SELECT id\n   FROM vt\n  WHERE NOT b AND v IS NULL;"),
        ("v11","id AS a,v AS bb FROM vt"," SELECT id AS a,\n    v AS bb\n   FROM vt;"," SELECT id AS a,\n    v AS bb\n   FROM vt;"),
        ("V Mixed",r#"id AS "Id", v AS "select" FROM vt"#," SELECT id AS \"Id\",\n    v AS \"select\"\n   FROM vt;"," SELECT id AS \"Id\",\n    v AS \"select\"\n   FROM vt;")
    ] {
        client.batch_execute(&format!("CREATE VIEW \"{name}\" AS SELECT {select}")).await.unwrap();
        for (arg,want) in [(format!("'\"{name}\"'"),plain),(format!("'\"{name}\"'::regclass"),plain),
            (format!("'\"{name}\"',false"),plain),(format!("'\"{name}\"',true"),pretty),(format!("'\"{name}\"'::regclass,0"),pretty)] {
            assert_eq!(client.query_one(&format!("SELECT pg_get_viewdef({arg})"),&[]).await.unwrap().get::<_,String>(0),want,"{name}: {arg}");
        }
    }
    for select in [
        "id,upper(v) AS up,n*2 AS dbl,id+1 FROM vt ORDER BY id DESC LIMIT 10",
        "vt.id,vu.note FROM vt JOIN vu ON vu.vt_id=vt.id WHERE vu.note IS NOT NULL",
        "v,count(*) AS c,sum(n) AS sum FROM vt GROUP BY v HAVING count(*)>1",
        "DISTINCT v FROM vt",
        "id,CASE WHEN b THEN 'yes' ELSE 'no' END AS yn,v::varchar(10) AS vv,d+1 AS tomorrow FROM vt WHERE v LIKE 'a%' AND id IN (1,2,3)",
        "t.id,t.v FROM vt t LEFT JOIN vu u ON u.vt_id=t.id",
        "id,COALESCE(v,'') AS v,'lit' AS l,1.5 AS f,NULL::int AS z,true AS t FROM vt"
    ] {
        assert_eq!(client.batch_execute(&format!("CREATE VIEW rejected AS SELECT {select}")).await.unwrap_err().code().unwrap().code(),"0A000","{select}");
    }
    assert_eq!(client.query_one("SELECT pg_get_viewdef('v2'::regclass,40)",&[]).await.unwrap().get::<_,String>(0)," SELECT id, v AS label\n   FROM vt\n  WHERE id > 0;");
    let oid: u32 = client.query_one("SELECT oid FROM pg_class WHERE relname='v2'",&[]).await.unwrap().get(0);
    assert!(client.query_one("SELECT pg_get_viewdef($1::oid)",&[&oid]).await.unwrap().get::<_,String>(0).ends_with("WHERE (id > 0);"));
    assert_eq!(client.query_one("SELECT pg_get_viewdef(0::oid),pg_get_viewdef('vt')",&[]).await.unwrap().get::<_,Option<String>>(0),None);
    assert_eq!(client.query_one("SELECT pg_get_viewdef('nope')",&[]).await.unwrap_err().code().unwrap().code(),"42P01");
    client.batch_execute("SET search_path TO pg_catalog").await.unwrap();
    assert!(client.query_one("SELECT pg_get_viewdef('public.v2')",&[]).await.unwrap().get::<_,String>(0).contains("FROM public.vt"));
    client.batch_execute("SET search_path TO public").await.unwrap();
    for sql in [include_str!("../../../compat/corpus/pg_silent/pgs_viewdef.sql"),include_str!("../../../compat/corpus/pg_silent/pgs_viewdef_pretty.sql")] {
        client.simple_query(sql).await.unwrap();
    }
    for (sql,expected) in [
        ("LOCK vt IN ACCESS SHARE MODE",vec![("AccessShareLock",true)]),
        ("LOCK vt IN ROW EXCLUSIVE MODE; LOCK vt IN ROW SHARE MODE",vec![("RowExclusiveLock",true),("RowShareLock",true)]),
        ("INSERT INTO vt(id) VALUES(1)",vec![("RowExclusiveLock",true)]),
        ("LOCK vt IN SHARE MODE; INSERT INTO vt(id) VALUES(1)",vec![("RowExclusiveLock",false),("ShareLock",false)]),
        ("INSERT INTO vt(id) VALUES(1); LOCK vt IN SHARE MODE",vec![("RowExclusiveLock",false),("ShareLock",false)])
    ] {
        client.batch_execute("BEGIN").await.unwrap(); client.batch_execute(sql).await.unwrap();
        let actual = client.query("SELECT mode,fastpath FROM pg_locks WHERE relation='vt'::regclass ORDER BY mode",&[]).await.unwrap().iter()
            .map(|r| (r.get::<_,String>(0),r.get::<_,bool>(1))).collect::<Vec<_>>();
        assert_eq!(actual,expected.iter().map(|(s,b)| ((*s).to_owned(),*b)).collect::<Vec<_>>(),"{sql}");
        client.batch_execute("ROLLBACK").await.unwrap();
    }
    drop(client); connection.await.unwrap().unwrap(); running.shutdown().await.unwrap(); drop(service); std::fs::remove_dir_all(path).unwrap();
}
