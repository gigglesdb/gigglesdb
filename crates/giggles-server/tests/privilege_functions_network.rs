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
    async fn new() -> Self {
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
        let admin = Admin::create(path.join("db.redb"), Limits::default()).unwrap();
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
