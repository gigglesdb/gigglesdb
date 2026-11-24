use giggles_core::{Admin, Limits, roles::NewRole};
use giggles_server::{
    Running, Service,
    auth::{PostgresqlConfig, TlsMode, scram::Verifier, serve_postgresql},
    clock::Clock,
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio_postgres::{Client, Config, NoTls, SimpleQueryMessage, config::SslMode};
static SERIAL: AtomicU64 = AtomicU64::new(0);
#[derive(Clone, Copy, Debug)]
enum Protocol {
    Simple,
    Extended,
}
impl Protocol {
    async fn ok(self, client: &Client, sql: &str) {
        match self {
            Self::Simple => {
                client
                    .simple_query(sql)
                    .await
                    .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
            }
            Self::Extended => {
                client
                    .execute(sql, &[])
                    .await
                    .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
            }
        }
    }
    async fn boolean(self, client: &Client, expression: &str, expected: bool) {
        let sql = format!("SELECT {expression}");
        let actual = match self {
            Self::Simple => client
                .simple_query(&sql)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
                .into_iter()
                .find_map(|r| {
                    if let SimpleQueryMessage::Row(r) = r {
                        Some(r.get(0).unwrap() == "t")
                    } else {
                        None
                    }
                })
                .unwrap(),
            Self::Extended => client
                .query_one(&sql, &[])
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
                .get(0),
        };
        assert_eq!(actual, expected, "{self:?}: {expression}");
    }
    async fn acl(self, client: &Client, expected: &[&str]) {
        let sql = "SELECT x.privilege_type FROM pg_catalog.pg_class c CROSS JOIN LATERAL aclexplode(c.relacl) x WHERE c.relname='t' AND x.grantee=(SELECT oid FROM pg_roles WHERE rolname='app') ORDER BY x.privilege_type";
        let actual: Vec<String> = match self {
            Self::Simple => client
                .simple_query(sql)
                .await
                .unwrap()
                .into_iter()
                .filter_map(|r| {
                    if let SimpleQueryMessage::Row(r) = r {
                        Some(r.get(0).unwrap().to_owned())
                    } else {
                        None
                    }
                })
                .collect(),
            Self::Extended => client
                .query(sql, &[])
                .await
                .unwrap()
                .into_iter()
                .map(|r| r.get(0))
                .collect(),
        };
        assert_eq!(actual, expected, "{self:?}: real relacl grants");
    }
    async fn invalid_privilege(self, client: &Client) {
        let sql = "SELECT has_table_privilege('t','invalid')";
        let error = match self {
            Self::Simple => client.simple_query(sql).await.unwrap_err(),
            Self::Extended => client.query(sql, &[]).await.unwrap_err(),
        };
        assert_eq!(error.code().unwrap().code(), "22023");
    }
}
struct Fixture {
    pg: Running,
    path: std::path::PathBuf,
}
impl Fixture {
    async fn new() -> Self { Self::new_engine(giggles_core::Engine::Legacy).await }
    async fn new_engine(engine: giggles_core::Engine) -> Self {
        let root = std::path::PathBuf::from(
            std::env::var_os("TEMP").expect("set task TEMP under J:/gigglesdb-work"),
        );
        assert!(
            root.to_string_lossy()
                .replace('\\', "/")
                .to_ascii_lowercase()
                .starts_with("j:/gigglesdb-work/")
        );
        let path = root.join(format!(
            "privilege-functions-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let admin = Admin::create_engine(path.join("db.redb"), Limits::default(), engine).unwrap();
        admin
            .roles()
            .create(NewRole {
                name: "owner".into(),
                login: true,
                superuser: true,
                password: Some(Verifier::generate("owner-pw")),
                ..NewRole::default()
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
            loopback_trust: false,
            connections: 16,
        };
        let listener = config.build().unwrap();
        let pg = serve_postgresql(config.listen, service, Arc::new(listener))
            .await
            .unwrap();
        Self { pg, path }
    }
    async fn connect(&self, user: &str, password: &str) -> Client {
        let mut config = Config::new();
        config
            .host("localhost")
            .hostaddr(self.pg.address.ip())
            .port(self.pg.address.port())
            .user(user)
            .password(password)
            .dbname("postgres")
            .ssl_mode(SslMode::Disable);
        let (client, connection) = config.connect(NoTls).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }
    async fn finish(self) {
        self.pg.shutdown().await.unwrap();
        std::fs::remove_dir_all(self.path).unwrap();
    }
}
async fn privileges(protocol: Protocol) {
    let f = Fixture::new().await;
    let owner = f.connect("owner", "owner-pw").await;
    protocol
        .ok(&owner, "CREATE ROLE app LOGIN PASSWORD 'app-pw'")
        .await;
    protocol
        .ok(&owner, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .await;
    protocol
        .ok(&owner, "GRANT SELECT, INSERT ON t TO app")
        .await;
    let app = f.connect("app", "app-pw").await;
    for (expression, expected) in [
        ("has_table_privilege('t','SELECT')", true),
        ("has_table_privilege('t','UPDATE')", false),
        ("has_table_privilege('t','INSERT')", true),
        ("has_table_privilege('app','public.t','DELETE')", false),
        ("has_column_privilege('t','id','SELECT')", true),
        ("has_column_privilege('t','id','UPDATE')", false),
    ] {
        protocol.boolean(&app, expression, expected).await;
    }
    protocol
        .boolean(&owner, "has_table_privilege('app','t','UPDATE')", false)
        .await;
    protocol.acl(&app, &["INSERT", "SELECT"]).await;
    protocol.invalid_privilege(&app).await;
    protocol.ok(&owner, "GRANT UPDATE ON t TO app").await;
    protocol
        .boolean(&app, "has_table_privilege('t','UPDATE')", true)
        .await;
    protocol
        .boolean(&owner, "has_table_privilege('app','t','UPDATE')", true)
        .await;
    protocol.acl(&app, &["INSERT", "SELECT", "UPDATE"]).await;
    protocol.ok(&owner, "REVOKE UPDATE ON t FROM app").await;
    protocol
        .boolean(&app, "has_table_privilege('t','UPDATE')", false)
        .await;
    protocol.acl(&app, &["INSERT", "SELECT"]).await;
    drop(app);
    drop(owner);
    f.finish().await;
}
#[tokio::test]
async fn simple_real_grants_and_revoke() {
    privileges(Protocol::Simple).await;
}
#[tokio::test]
async fn extended_real_grants_and_revoke() {
    privileges(Protocol::Extended).await;
}

// GD-0079 follow-up: privilege inquiries must also bind on application rows.
#[tokio::test]
async fn application_rows_privilege_inquiries() {
    let f = Fixture::new().await;
    let owner = f.connect("owner", "owner-pw").await;
    for sql in [
        "CREATE ROLE inquiry_group",
        "CREATE ROLE inquiry_member IN ROLE inquiry_group",
        "CREATE TABLE inquiry_rows (id INTEGER PRIMARY KEY, who TEXT)",
        "INSERT INTO inquiry_rows VALUES (1,'inquiry_member')",
    ] {
        Protocol::Simple.ok(&owner, sql).await;
    }
    let rows = owner.query("SELECT id, pg_has_role(who,'inquiry_group','MEMBER') FROM inquiry_rows", &[])
        .await.expect("privilege inquiry on application rows");
    assert_eq!(rows.len(), 1);
    assert!(rows[0].get::<_, bool>(1));
    drop(owner);
    f.finish().await;
}

// PG18.6: rolesfu2/w1b-unknown-oid-oracle.sql. A missing role OID is
// still usable for identity comparisons and PUBLIC ACLs; a missing object
// OID instead yields NULL. Keep an application relation in this query.
#[tokio::test]
async fn application_rows_privilege_unknown_oids() {
    let f = Fixture::new().await;
    let owner = f.connect("owner", "owner-pw").await;
    Protocol::Simple.ok(&owner, "CREATE TABLE inquiry_oid_rows (id INTEGER PRIMARY KEY)").await;
    Protocol::Simple.ok(&owner, "INSERT INTO inquiry_oid_rows VALUES (1)").await;
    let rows = owner.query(
        "SELECT pg_has_role(4294967294::oid,4294967294::oid,'MEMBER'), \
         pg_has_role(4294967294::oid,'owner'::name,'MEMBER'), \
         pg_has_role('owner'::name,4294967294::oid,'MEMBER'), \
         has_table_privilege(4294967294::oid,'pg_class','SELECT'), \
         has_table_privilege('owner'::name,4294967294::oid,'SELECT') \
         FROM inquiry_oid_rows", &[],
    ).await.expect("function-specific unknown OID behavior on application rows");
    assert_eq!(rows.len(), 1);
    assert!(rows[0].get::<_, bool>(0));
    assert!(!rows[0].get::<_, bool>(1));
    assert!(rows[0].get::<_, bool>(2));
    assert!(rows[0].get::<_, bool>(3));
    assert_eq!(rows[0].get::<_, Option<bool>>(4), None);
    drop(owner);
    f.finish().await;
}

// Describe must bind the return type without evaluating a privilege inquiry
// or needing execution identity. oracle-b.out establishes the boolean type.
#[tokio::test]
async fn application_rows_privilege_describe() {
    let f = Fixture::new().await;
    let owner = f.connect("owner", "owner-pw").await;
    Protocol::Simple.ok(&owner, "CREATE TABLE inquiry_describe_rows (id INTEGER PRIMARY KEY)").await;
    let statement = owner.prepare(
        "SELECT pg_has_role('owner','owner','MEMBER'), \
         has_table_privilege('inquiry_describe_rows','SELECT'), \
         has_schema_privilege('public','USAGE'), \
         has_column_privilege('inquiry_describe_rows','id','SELECT'), \
         has_any_column_privilege('inquiry_describe_rows','SELECT'), \
         has_database_privilege(current_database(),'CONNECT') \
         FROM inquiry_describe_rows",
    ).await.expect("Describe privilege inquiries without evaluation");
    assert_eq!(statement.columns().len(), 6);
    for column in statement.columns() {
        assert_eq!(column.type_(), &tokio_postgres::types::Type::BOOL);
    }
    drop(owner);
    f.finish().await;
}

#[tokio::test]
async fn catalog_inquiry_group_schema_database_sequence() {
    let f = Fixture::new().await;
    let owner = f.connect("owner", "owner-pw").await;
    let p = Protocol::Extended;
    for sql in ["CREATE ROLE w1c_g", "CREATE ROLE w1c_u IN ROLE w1c_g",
        "CREATE TABLE w1c_t(id INTEGER)", "GRANT SELECT ON w1c_t TO w1c_g",
        "CREATE SEQUENCE w1c_s"] {p.ok(&owner,sql).await;}
    for expression in ["has_table_privilege('w1c_u','w1c_t','SELECT')",
        "has_column_privilege('w1c_u','w1c_t','id','SELECT')",
        "has_any_column_privilege('w1c_u','w1c_t','SELECT')",
        "has_schema_privilege('w1c_u','public','USAGE')",
        "has_database_privilege('w1c_u','postgres','CONNECT')",
        "has_sequence_privilege('w1c_s','USAGE')",
        "pg_has_role(4294967294::oid,4294967294::oid,'MEMBER')",
        "has_table_privilege(4294967294::oid,'pg_class','SELECT')"] {
        p.boolean(&owner,expression,true).await;
    }
    for (expression,code) in [
        ("has_table_privilege('w1c_missing','w1c_t','SELECT')","42704"),
        ("has_table_privilege('w1c_u','w1c_missing','SELECT')","42P01"),
        ("has_sequence_privilege('w1c_t','USAGE')","42809"),
        ("has_schema_privilege('w1c_u','public','BOGUS')","22023"),
    ] {
        let e = owner.query(&format!("SELECT {expression}"),&[]).await.unwrap_err();
        assert_eq!(e.code().unwrap().code(),code,"{expression}");
    }
    drop(owner);
    f.finish().await;
}

// W1d: identical evaluator behavior on catalog and application execution paths.
#[tokio::test]
async fn application_rows_privilege_equivalence_and_parameters() {
    let f = Fixture::new().await;
    let owner = f.connect("owner", "owner-pw").await;
    for sql in [
        "CREATE ROLE w1d_g",
        "CREATE ROLE w1d_u IN ROLE w1d_g",
        "CREATE TABLE w1d_t(id INTEGER PRIMARY KEY)",
        "INSERT INTO w1d_t VALUES (1)",
        "GRANT SELECT ON w1d_t TO w1d_g",
    ] { Protocol::Simple.ok(&owner, sql).await; }
    for role in ["owner", "w1d_u"] {
        Protocol::Simple.ok(&owner, &format!("SET ROLE {role}")).await;
        for expression in [
            "pg_has_role('w1d_g','USAGE')",
            "pg_has_role('w1d_u','w1d_g','MEMBER')",
            "has_table_privilege('w1d_t','SELECT')",
            "has_column_privilege('w1d_t','id','SELECT')",
            "has_any_column_privilege('w1d_t','SELECT')",
            "has_schema_privilege('public','USAGE')",
            "has_database_privilege('postgres','CONNECT')",
            "has_type_privilege('int4','USAGE')",
            "pg_has_role(NULL,'w1d_g','MEMBER')",
        ] {
            let catalog: Option<bool> = owner.query_one(&format!("SELECT {expression}"), &[]).await.unwrap().get(0);
            let application: Option<bool> = owner.query_one(&format!("SELECT {expression} FROM w1d_t"), &[]).await.unwrap().get(0);
            assert_eq!(application, catalog, "role={role}: {expression}");
        }
        for (expression, expected) in [
            ("pg_has_role('w1d_missing','w1d_g','MEMBER')", "42704"),
            ("pg_has_role('w1d_u','w1d_g','BOGUS')", "22023"),
        ] {
            for suffix in ["", " FROM w1d_t"] {
                let e = owner.query(&format!("SELECT {expression}{suffix}"), &[]).await.unwrap_err();
                assert_eq!(e.code().unwrap().code(), expected, "{expression}{suffix}");
            }
        }
        let statement = owner.prepare("SELECT pg_has_role($1::text,'w1d_g',$2::text) FROM w1d_t").await.unwrap();
        assert!(owner.query_one(&statement, &[&"w1d_u", &"MEMBER"]).await.unwrap().get::<_, bool>(0));
        Protocol::Simple.ok(&owner, "RESET ROLE").await;
    }
    drop(owner);
    f.finish().await;
}

#[tokio::test]
async fn application_rows_privilege_cursor() {
    let f = Fixture::new().await;
    let owner = f.connect("owner", "owner-pw").await;
    for sql in [
        "CREATE TABLE w1d_cursor_t(id INTEGER PRIMARY KEY)",
        "INSERT INTO w1d_cursor_t VALUES (1),(2)",
        "BEGIN",
        "DECLARE w1d_c CURSOR FOR SELECT id, pg_has_role('owner','owner','MEMBER') FROM w1d_cursor_t ORDER BY id",
    ] { Protocol::Simple.ok(&owner, sql).await; }
    for expected in ["1", "2"] {
        let rows = owner.simple_query("FETCH 1 FROM w1d_c").await.unwrap();
        let row = rows.iter().find_map(|r| match r { SimpleQueryMessage::Row(r) => Some(r), _ => None }).unwrap();
        assert_eq!(row.get(0), Some(expected));
        assert_eq!(row.get(1), Some("t"));
    }
    Protocol::Simple.ok(&owner, "CLOSE w1d_c").await;
    Protocol::Simple.ok(&owner, "COMMIT").await;
    drop(owner);
    f.finish().await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn application_rows_privilege_transaction_private_table() {
    let f = Fixture::new_engine(giggles_core::Engine::V2).await;
    let owner = f.connect("owner", "owner-pw").await;
    for sql in ["BEGIN", "CREATE TABLE w1d_private(id INTEGER PRIMARY KEY)",
        "INSERT INTO w1d_private VALUES (1)"] { Protocol::Simple.ok(&owner, sql).await; }
    let row = owner.query_one("SELECT has_table_privilege('w1d_private','SELECT'), has_column_privilege('w1d_private','id','SELECT') FROM w1d_private", &[]).await.unwrap();
    assert!(row.get::<_,bool>(0));
    assert!(row.get::<_,bool>(1));
    Protocol::Simple.ok(&owner,"ROLLBACK").await;
    drop(owner);
    f.finish().await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn application_rows_privilege_transaction_private_non_superuser_table() {
    let f = Fixture::new_engine(giggles_core::Engine::V2).await;
    let owner = f.connect("owner", "owner-pw").await;
    for sql in ["CREATE ROLE w1d_private_owner LOGIN PASSWORD 'private-pw'",
        "GRANT CREATE ON SCHEMA public TO w1d_private_owner"] { Protocol::Simple.ok(&owner, sql).await; }
    let owner = f.connect("w1d_private_owner", "private-pw").await;
    for sql in ["BEGIN", "CREATE TABLE w1d_private(id INTEGER PRIMARY KEY)",
        "INSERT INTO w1d_private VALUES (1)"] { Protocol::Simple.ok(&owner, sql).await; }
    let row = owner.query_one("SELECT has_table_privilege('w1d_private','SELECT'), has_column_privilege('w1d_private','id','SELECT') FROM w1d_private", &[]).await.unwrap();
    assert!(row.get::<_,bool>(0));
    assert!(row.get::<_,bool>(1));
    Protocol::Simple.ok(&owner,"ROLLBACK").await;
    drop(owner);
    f.finish().await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn application_rows_privilege_task_only_refused() {
    let f = Fixture::new_engine(giggles_core::Engine::V2).await;
    let owner = f.connect("owner", "owner-pw").await;
    for sql in ["CREATE ROLE w1d_task LOGIN PASSWORD 'task-pw'",
        "CREATE TABLE w1d_denied(id INTEGER PRIMARY KEY)",
        "INSERT INTO w1d_denied VALUES (1)"] { Protocol::Simple.ok(&owner,sql).await; }
    let baseline = f.connect("w1d_task", "task-pw").await;
    let inquiry = f.connect("w1d_task", "task-pw").await;
    Protocol::Simple.ok(&owner, "ALTER ROLE w1d_task TASKONLY").await;
    // Main fences these live identities before dispatch, closing the connection
    // without a SQLSTATE. An evaluator call would instead report missing role.
    for (client, sql) in [(&baseline, "SELECT id FROM w1d_denied"),
        (&inquiry, "SELECT id, pg_has_role('missing','owner','MEMBER') FROM w1d_denied")] {
        let e = client.simple_query(sql).await.unwrap_err();
        assert!(e.is_closed(), "{sql}: {e:?}");
        assert!(e.code().is_none(), "{sql}: {e:?}");
    }
    drop(baseline);
    drop(inquiry);
    drop(owner);
    f.finish().await;
}

#[tokio::test]
async fn application_inquiry_contextual_casts() {
    application_inquiry_contextual_casts_on(giggles_core::Engine::Legacy).await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn application_inquiry_contextual_casts_v2() {
    application_inquiry_contextual_casts_on(giggles_core::Engine::V2).await;
}

async fn application_inquiry_contextual_casts_on(engine: giggles_core::Engine) {
    let f = Fixture::new_engine(engine).await;
    let owner = f.connect("owner", "owner-pw").await;
    for sql in ["CREATE TABLE w1d_cast(id INTEGER PRIMARY KEY)", "INSERT INTO w1d_cast VALUES (1)"] {
        Protocol::Simple.ok(&owner, sql).await;
    }
    for expression in ["pg_has_role('-1'::oid,'4294967295'::oid,'MEMBER')",
        "pg_has_role('owner'::name,'owner','MEMBER')"] {
        assert!(owner.query_one(&format!("SELECT {expression} FROM w1d_cast"), &[]).await.unwrap().get::<_, bool>(0));
    }
    let long_role = "n".repeat(63);
    Protocol::Simple.ok(&owner, &format!("CREATE ROLE {long_role}")).await;
    assert!(owner.query_one(&format!("SELECT pg_has_role('{long_role}suffix'::name,'{long_role}','MEMBER') FROM w1d_cast"), &[]).await.unwrap().get::<_, bool>(0));
    for (input, state) in [("'4294967296'", "22003"), ("'abc'", "22P02"), ("99999999999", "22003")] {
        let e = owner.query(&format!("SELECT pg_has_role({input}::oid,'owner','MEMBER') FROM w1d_cast"), &[]).await.unwrap_err();
        assert_eq!(e.code().unwrap().code(), state);
    }
    // These casts are available in their supported contexts. An ordinary
    // application projection, predicate, or sort key still refuses them.
    for ty in ["oid", "name", r#""char""#] {
        for (sql, state) in [
            (format!("SELECT '1'::{ty} FROM w1d_cast"), "0A000"),
            (format!("SELECT id FROM w1d_cast WHERE '1'::{ty} = '1'::{ty}"), "0A000"),
            (format!("SELECT id FROM w1d_cast ORDER BY '1'::{ty}"), "0A000"),
            (format!("CREATE TABLE cast_col (id int PRIMARY KEY, val {ty})"), "42704"),
        ] {
            for protocol in [Protocol::Simple, Protocol::Extended] {
                let err = match protocol {
                    Protocol::Simple => owner.simple_query(&sql).await.unwrap_err(),
                    Protocol::Extended => owner.query(&sql, &[]).await.unwrap_err(),
                };
                assert_eq!(err.code().unwrap().code(), state, "{protocol:?}: {sql}");
            }
        }
    }
    application_parameter_refusals_wire(&owner).await;
    drop(owner);
    f.finish().await;
}

#[tokio::test]
async fn w1e_contextual_oid_spellings() {
    let f = Fixture::new().await;
    let owner = f.connect("owner", "owner-pw").await;
    Protocol::Simple.ok(&owner, "CREATE TABLE w1e_cast(id INT PRIMARY KEY)").await;
    Protocol::Simple.ok(&owner, "INSERT INTO w1e_cast VALUES (1)").await;
    for text in ["0x10", "0X10", "0b10000", "0B10000"] {
        let row = owner.query_one(&format!("SELECT pg_has_role('{text}'::oid,'owner','MEMBER') = pg_has_role(16::oid,'owner','MEMBER') FROM w1e_cast"), &[]).await.unwrap();
        assert!(row.get::<_,bool>(0));
    }
    for (text, state) in [("0o20","22P02"),("1_0","22P02"),("0x100000000","22003"),("-0x80000001","22003")] {
        let e = owner.query(&format!("SELECT pg_has_role('{text}'::oid,'owner','MEMBER') FROM w1e_cast"), &[]).await.unwrap_err();
        assert_eq!(e.code().unwrap().code(), state);
    }
    drop(owner); f.finish().await;
}
#[tokio::test]
async fn w1e_execution_search_path() {
    let f = Fixture::new().await;
    let owner = f.connect("owner", "owner-pw").await;
    for sql in ["CREATE SCHEMA w1e_path", "CREATE TABLE w1e_path.t(id INT PRIMARY KEY)", "INSERT INTO w1e_path.t VALUES (1)", "SET search_path=w1e_path"] {
        Protocol::Simple.ok(&owner, sql).await;
    }
    let row = owner.query_one("SELECT has_table_privilege('t','SELECT'),has_column_privilege('t','id','SELECT') FROM w1e_path.t", &[]).await.unwrap();
    assert!(row.get::<_,bool>(0)); assert!(row.get::<_,bool>(1));
    Protocol::Simple.ok(&owner, "CREATE TABLE public.t(other INT PRIMARY KEY)").await;
    let prepared = owner.prepare("SELECT has_column_privilege('t','id','SELECT') FROM w1e_path.t").await.unwrap();
    assert!(owner.query_one(&prepared, &[]).await.unwrap().get::<_,bool>(0));
    Protocol::Simple.ok(&owner, "BEGIN").await;
    Protocol::Simple.ok(&owner, "DECLARE w1e_c CURSOR FOR SELECT has_column_privilege('t','id','SELECT') FROM w1e_path.t").await;
    Protocol::Simple.ok(&owner, "SET search_path=public").await;
    let e = owner.query(&prepared, &[]).await.unwrap_err();
    assert_eq!(e.code().unwrap().code(), "42703");
    Protocol::Simple.ok(&owner, "ROLLBACK").await;
    // Reopen the cursor after the prepared-statement error aborted the transaction.
    Protocol::Simple.ok(&owner, "SET search_path=w1e_path").await;
    Protocol::Simple.ok(&owner, "BEGIN").await;
    Protocol::Simple.ok(&owner, "DECLARE w1e_c CURSOR FOR SELECT has_column_privilege('t','id','SELECT') FROM w1e_path.t").await;
    Protocol::Simple.ok(&owner, "SET search_path=public").await;
    // PG18.6 w1e-path-oracle-exact.out: FETCH uses the changed path, so public.t has no id.
    let e = owner.simple_query("FETCH 1 FROM w1e_c").await.unwrap_err();
    assert_eq!(e.code().unwrap().code(), "42703");
    Protocol::Simple.ok(&owner, "ROLLBACK").await;
    // Populate the inquiry cache, then change the path between FETCH evaluations.
    Protocol::Simple.ok(&owner, "INSERT INTO w1e_path.t VALUES (2),(3)").await;
    Protocol::Simple.ok(&owner, "SET search_path=w1e_path").await;
    Protocol::Simple.ok(&owner, "BEGIN").await;
    Protocol::Simple.ok(&owner, "DECLARE w1e_cached CURSOR FOR SELECT has_column_privilege('t','id','SELECT') FROM w1e_path.t").await;
    Protocol::Simple.ok(&owner, "FETCH 1 FROM w1e_cached").await;
    Protocol::Simple.ok(&owner, "SET search_path=public").await;
    let e = owner.simple_query("FETCH ALL FROM w1e_cached").await.unwrap_err();
    assert_eq!(e.code().unwrap().code(), "42703");
    Protocol::Simple.ok(&owner, "ROLLBACK").await;
    drop(owner); f.finish().await;
}

async fn application_parameter_refusals_wire(client: &tokio_postgres::Client) {
    use tokio_postgres::types::Type;

    for sql in [
        "CREATE TABLE t(a int PRIMARY KEY, s text UNIQUE)",
        "INSERT INTO t VALUES (1, 'x')",
        "CREATE INDEX t_s ON t(s)",
    ] {
        Protocol::Simple.ok(client, sql).await;
    }

    // Main rejects the application declaration after binding. Preserve the
    // binding error where the query cannot infer the parameter on its own.
    for (sql, code) in [
        (r#"PREPARE p6("char") AS SELECT $1"#, "0A000"),
        ("EXECUTE p6('a')", "26000"),
        (r#"EXECUTE p6('\303')"#, "26000"),
        ("EXECUTE p6('ab')", "26000"),
        (r#"PREPARE pchar_array("char"[]) AS SELECT $1"#, "0A000"),
        ("EXECUTE pchar_array('{a}')", "26000"),
        ("PREPARE p2(oid) AS SELECT $1", "0A000"),
        ("EXECUTE p2('0x10')", "26000"),
        ("EXECUTE p2('-1')", "26000"),
        ("PREPARE p9(oid) AS SELECT $1 FROM t ORDER BY $1", "42804"),
        ("EXECUTE p9('0x10')", "26000"),
        ("PREPARE p5(oid) AS SELECT a FROM t WHERE $1 IS NOT NULL", "42804"),
        ("EXECUTE p5(3)", "26000"),
        ("PREPARE p10(name) AS SELECT $1", "0A000"),
        ("PREPARE p1 AS SELECT $1::oid", "0A000"),
        ("EXECUTE p1('1')", "26000"),
        ("EXECUTE p1('0x10')", "26000"),
        ("EXECUTE p1('4294967296')", "26000"),
        ("PREPARE p3(text) AS SELECT $1::oid", "0A000"),
        ("EXECUTE p3('5')", "26000"),
        ("PREPARE p4(int) AS SELECT $1::oid", "0A000"),
        ("EXECUTE p4(5)", "26000"),
        ("PREPARE p8(oid) AS SELECT $1::regconfig", "0A000"),
        ("EXECUTE p8(3748)", "26000"),
        ("ALTER INDEX missing_idx RENAME TO x", "42601"),
        ("ALTER INDEX IF EXISTS missing_idx RENAME TO x", "42601"),
        ("ALTER INDEX t RENAME TO x", "42601"),
        ("ALTER INDEX t_pkey RENAME TO x", "42601"),
        ("ALTER INDEX t_s_key RENAME TO x", "42601"),
        ("ALTER INDEX t_s RENAME TO x", "42601"),
        ("ALTER INDEX missing_schema.missing_idx RENAME TO x", "42601"),
        ("ALTER INDEX t_s SET (fillfactor = 50)", "42601"),
        ("ALTER INDEX t_s RENAME x", "42601"),
    ] {
        let error = client.simple_query(sql).await.unwrap_err();
        assert_eq!(error.code().unwrap().code(), code, "{sql}");
    }
    for (sql, value, code) in [
        ("SELECT $1::oid", "1", "0A000"),
        ("SELECT $1::oid", "0x10", "0A000"),
        ("SELECT $1::oid", "-1", "0A000"),
        ("SELECT $1::oid", "4294967296", "0A000"),
        ("SELECT $1::oid", "abc", "0A000"),
        ("SELECT $1::oid, a FROM t", "7", "0A000"),
        ("SELECT CAST($1 AS oid)", "7", "0A000"),
        ("SELECT a FROM t WHERE a = $1::oid", "1", "0A000"),
        ("SELECT a FROM t WHERE $1::oid IS NOT NULL", "7", "0A000"),
        ("SELECT $1::oid::text", "7", "0A000"),
        (r#"SELECT $1::"char""#, "a", "0A000"),
        (r#"SELECT $1::"char"[]"#, "{a}", "0A000"),
        ("SELECT $1::name", "x", "0A000"),
        ("SELECT pg_has_role($1::oid, 'owner', 'MEMBER') FROM t", "10", "42804"),
    ] {
        let error = client.query(sql, &[&value]).await.unwrap_err();
        assert_eq!(error.code().unwrap().code(), code, "{sql} [{value}]");
    }
    for ty in [Type::OID, Type::CHAR, Type::CHAR_ARRAY, Type::NAME] {
        for (sql, code) in [
            ("SELECT $1", "0A000"),
            ("SELECT $1 FROM t ORDER BY $1", "42804"),
            ("SELECT a FROM t WHERE $1 IS NOT NULL", "42804"),
        ] {
            let error = client.prepare_typed(sql, std::slice::from_ref(&ty)).await.unwrap_err();
            assert_eq!(error.code().unwrap().code(), code, "{sql}: {ty}");
        }
    }
    let error = client.prepare_typed("SELECT $1::oid::regconfig", &[Type::OID]).await.unwrap_err();
    assert_eq!(error.code().unwrap().code(), "0A000");
    for (sql, ty) in [
        ("SELECT oid FROM pg_catalog.pg_class WHERE oid=$1", Type::OID),
        ("SELECT relkind FROM pg_catalog.pg_class WHERE relkind=$1", Type::CHAR),
        ("SELECT relkind FROM pg_catalog.pg_class WHERE relkind=ANY($1)", Type::CHAR_ARRAY),
    ] {
        let statement = client.prepare_typed(sql, std::slice::from_ref(&ty)).await.unwrap();
        assert_eq!(statement.params(), &[ty], "{sql}");
    }
    for sql in [
        "PREPARE unused_oid(oid) AS SELECT 1",
        r#"PREPARE unused_char("char") AS SELECT 1"#,
        "PREPARE p7(regconfig) AS SELECT to_tsvector($1, 'cats')",
    ] {
        client.simple_query(sql).await.unwrap();
    }
    let rows = client.simple_query("EXECUTE p7('english')").await.unwrap();
    assert!(rows.iter().any(|message| matches!(message,
        tokio_postgres::SimpleQueryMessage::Row(row) if row.get(0) == Some("'cat':1"))));
}
