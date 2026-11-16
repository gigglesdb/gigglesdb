use giggles_core::{Admin, Limits, roles::NewRole};
use giggles_server::{
    Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{collections::BTreeMap, sync::Arc};

// Exact Skedaddle FK-section query, including the bound relation parameter.
const FK_QUERY: &str = "SELECT c.conname::text, n.nspname::text, p.relname::text, c.confmatchtype::text, array_agg(a.attname::text ORDER BY k.ord), array_agg(b.attname::text ORDER BY k.ord), array_agg(o.oprname::text ORDER BY k.ord), array_agg(onsp.nspname::text ORDER BY k.ord) FROM pg_constraint c JOIN pg_class p ON p.oid=c.confrelid JOIN pg_namespace n ON n.oid=p.relnamespace CROSS JOIN LATERAL unnest(c.conkey,c.confkey,c.conpfeqop) WITH ORDINALITY k(child,parent,eqop,ord) JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=k.child JOIN pg_attribute b ON b.attrelid=c.confrelid AND b.attnum=k.parent JOIN pg_operator o ON o.oid=k.eqop JOIN pg_namespace onsp ON onsp.oid=o.oprnamespace WHERE c.conrelid=$1::text::regclass AND c.contype='f' GROUP BY c.oid,c.conname,c.confmatchtype,n.nspname,p.relname ORDER BY c.conname";

#[tokio::test]
async fn exact_skedaddle_fk_section_composite_key() {
    let path = std::env::temp_dir().join(format!(
        "fk-section-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let admin = Admin::create(&path, Limits::default()).unwrap();
    admin
        .roles()
        .create(NewRole {
            name: "owner".into(),
            login: true,
            superuser: true,
            ..Default::default()
        })
        .unwrap();
    let service = Service::new(admin, vec![], Clock::new(0, 3600).unwrap()).unwrap();
    let config = PostgresqlConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        tls: TlsMode::Disabled,
        certificate: None,
        private_key: None,
        client_ca: None,
        certificates: BTreeMap::new(),
        databases: vec!["postgres".into()],
        loopback_trust: true,
        connections: 8,
    };
    let running = serve_postgresql(
        config.listen,
        service.clone(),
        Arc::new(config.build().unwrap()),
    )
    .await
    .unwrap();
    let (client, connection) = tokio_postgres::Config::new()
        .hostaddr(running.address.ip())
        .port(running.address.port())
        .user("owner")
        .dbname("postgres")
        .connect(tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(connection);
    let result = async {
        client.batch_execute("CREATE TABLE parent (a integer, b integer, PRIMARY KEY (a,b)); CREATE TABLE child (id integer PRIMARY KEY, x integer, y integer, CONSTRAINT child_parent_fk FOREIGN KEY (y,x) REFERENCES parent(a,b)); INSERT INTO parent VALUES (10,20); INSERT INTO child VALUES (1,20,10)").await?;
        let mut counts = vec![client.query_one("SELECT count(*) FROM pg_constraint WHERE contype='f'", &[]).await?.get::<_, i64>(0)];
        let joined = FK_QUERY.split_once(" FROM ").unwrap().1.split_once(" GROUP BY ").unwrap().0;
        for sql in [
            "SELECT count(*) FROM pg_constraint c WHERE c.conrelid=$1::text::regclass AND c.contype='f'".to_owned(),
            "SELECT count(*) FROM pg_constraint c CROSS JOIN LATERAL unnest(c.conkey,c.confkey,c.conpfeqop) WITH ORDINALITY k(child,parent,eqop,ord) WHERE c.conrelid=$1::text::regclass AND c.contype='f'".to_owned(),
            format!("SELECT count(*) FROM {joined}"),
        ] {
            counts.push(client.query_one(&sql, &[&"public.child"]).await?.get::<_, i64>(0));
        }
        let rows = client.query(FK_QUERY, &[&"public.child"]).await?;
        // Run the same statement in simple protocol with the parameter inlined.
        let simple = client.simple_query(&FK_QUERY.replace("$1", "'public.child'")).await?;
        let simple = simple.into_iter().filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r), _ => None,
        }).collect::<Vec<_>>();
        Ok::<_, tokio_postgres::Error>((rows, simple, counts))
    }.await;
    drop(client);
    connection.await.unwrap().unwrap();
    running.shutdown().await.unwrap();
    drop(service);
    std::fs::remove_dir_all(path).unwrap();
    let (rows, simple, counts) = result.unwrap();
    assert_eq!(
        counts,
        [1, 1, 2, 2],
        "FK, bound relation, zipped keys, final joins"
    );
    assert_eq!(rows.len(), 1);
    let r = &rows[0];
    for (i, expected) in ["child_parent_fk", "public", "parent", "s"]
        .iter()
        .enumerate()
    {
        assert_eq!(r.get::<_, String>(i), *expected);
    }
    for (i, expected) in [
        (4, vec!["y", "x"]),
        (5, vec!["a", "b"]),
        (6, vec!["=", "="]),
        (7, vec!["pg_catalog", "pg_catalog"]),
    ] {
        assert_eq!(r.get::<_, Vec<String>>(i), expected);
    }
    assert_eq!(simple.len(), 1);
    for (i, expected) in [
        "child_parent_fk",
        "public",
        "parent",
        "s",
        "{y,x}",
        "{a,b}",
        "{=,=}",
        "{pg_catalog,pg_catalog}",
    ]
    .iter()
    .enumerate()
    {
        let actual = simple[0].get(i).unwrap();
        // This fixture uses plain ASCII names/operators. Both quoted and
        // unquoted array elements represent the same values and order.
        if i < 4 {
            assert_eq!(actual, *expected);
        } else {
            assert_eq!(actual.replace('"', ""), *expected);
        }
    }
}
