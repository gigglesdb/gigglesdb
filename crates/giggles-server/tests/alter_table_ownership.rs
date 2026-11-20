use giggles_core::{
    Admin, CrashPoint, Limits,
    roles::{NewRole, SchemaPrivilege, TablePrivilege},
};
use giggles_server::{Binding, Role, Service, clock::Clock, sqlstate};
use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn add_column_if_not_exists_notices_and_ownership_over_pgwire() {
    use giggles_server::{
        auth::{PostgresqlConfig, TlsMode, serve_postgresql},
        pgwire::{frame, z},
    };
    use std::{collections::BTreeMap, sync::Arc, time::Duration};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };

    async fn cycle(stream: &mut TcpStream) -> Vec<(u8, Vec<u8>)> {
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut messages = Vec::new();
            loop {
                let tag = stream.read_u8().await.unwrap();
                let len = stream.read_u32().await.unwrap();
                assert!((4..65536).contains(&len));
                let mut body = vec![0; len as usize - 4];
                stream.read_exact(&mut body).await.unwrap();
                messages.push((tag, body));
                if tag == b'Z' {
                    return messages;
                }
            }
        })
        .await
        .unwrap()
    }
    async fn connect(address: std::net::SocketAddr, user: &str) -> TcpStream {
        let mut stream = TcpStream::connect(address).await.unwrap();
        let mut body = 196608u32.to_be_bytes().to_vec();
        z("user", &mut body);
        z(user, &mut body);
        z("database", &mut body);
        z("postgres", &mut body);
        body.push(0);
        stream.write_u32((body.len() + 4) as u32).await.unwrap();
        stream.write_all(&body).await.unwrap();
        let messages = cycle(&mut stream).await;
        assert!(!messages.iter().any(|m| m.0 == b'E'), "{messages:?}");
        stream
    }
    async fn query(stream: &mut TcpStream, sql: &str) -> Vec<(u8, Vec<u8>)> {
        let mut body = Vec::new();
        z(sql, &mut body);
        let mut packet = Vec::new();
        frame(b'Q', &body, &mut packet).unwrap();
        stream.write_all(&packet).await.unwrap();
        cycle(stream).await
    }
    fn field(body: &[u8], tag: u8) -> &str {
        let mut fields = body.split(|b| *b == 0).filter(|f| !f.is_empty());
        let value = fields.find(|f| f[0] == tag).unwrap();
        std::str::from_utf8(&value[1..]).unwrap()
    }
    fn completed(messages: &[(u8, Vec<u8>)], command: &str) {
        assert!(!messages.iter().any(|m| m.0 == b'E'), "{messages:?}");
        let completion = messages.iter().find(|m| m.0 == b'C').unwrap();
        assert_eq!(&completion.1[..completion.1.len() - 1], command.as_bytes());
    }

    let root = std::env::var_os("GIGGLES_TEST_ARTIFACTS")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let path = root.join(format!(
        "alter-add-wire-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&path).unwrap();
    let admin = Admin::create_engine(
        path.join("db.redb"),
        Limits::default(),
        giggles_core::Engine::V2,
    )
    .unwrap();
    for (name, superuser) in [("owner", true), ("other", false)] {
        admin
            .roles()
            .create(NewRole {
                name: name.into(),
                login: true,
                superuser,
                ..NewRole::default()
            })
            .unwrap();
    }
    let service = Service::new(admin.clone(), vec![], Clock::new(0, 3600).unwrap()).unwrap();
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
    let pg = serve_postgresql(
        config.listen,
        service.clone(),
        Arc::new(config.build().unwrap()),
    )
    .await
    .unwrap();
    let mut owner = connect(pg.address, "owner").await;
    for (sql, command) in [
        ("CREATE SCHEMA _skedaddle", "CREATE SCHEMA"),
        (
            "CREATE TABLE _skedaddle.apply_progress (stream text PRIMARY KEY)",
            "CREATE TABLE",
        ),
    ] {
        completed(&query(&mut owner, sql).await, command);
    }
    let sql = "ALTER TABLE _skedaddle.apply_progress ADD COLUMN IF NOT EXISTS record_kind smallint, ADD COLUMN IF NOT EXISTS manifest_digest bytea, ADD COLUMN IF NOT EXISTS previous_manifest_digest bytea";
    let first = query(&mut owner, sql).await;
    completed(&first, "ALTER TABLE");
    assert!(!first.iter().any(|m| m.0 == b'N'));
    let second = query(&mut owner, sql).await;
    completed(&second, "ALTER TABLE");
    let notices: Vec<_> = second.iter().filter(|m| m.0 == b'N').collect();
    assert_eq!(notices.len(), 3);
    for (notice, column) in
        notices
            .iter()
            .zip(["record_kind", "manifest_digest", "previous_manifest_digest"])
    {
        assert_eq!(field(&notice.1, b'C'), "42701");
        assert_eq!(field(&notice.1, b'S'), "NOTICE");
        assert_eq!(
            field(&notice.1, b'M'),
            format!("column \"{column}\" of relation \"apply_progress\" already exists, skipping")
        );
    }
    let existing = query(&mut owner, "DROP VIEW IF EXISTS missing_view").await;
    completed(&existing, "DROP VIEW");
    let notice = existing.iter().find(|m| m.0 == b'N').unwrap();
    assert_eq!(field(&notice.1, b'C'), "00000");
    assert_eq!(
        field(&notice.1, b'M'),
        "view \"missing_view\" does not exist, skipping"
    );
    for (kind, command) in [("table", "TABLE"), ("sequence", "SEQUENCE"), ("index", "INDEX")] {
        let object = format!("missing_{kind}");
        let response = query(&mut owner, &format!("DROP {command} IF EXISTS {object}")).await;
        completed(&response, &format!("DROP {command}"));
        let notice = response.iter().find(|m| m.0 == b'N').unwrap();
        assert_eq!(field(&notice.1, b'C'), "00000");
        assert_eq!(field(&notice.1, b'M'), format!("{kind} \"{object}\" does not exist, skipping"));
    }
    for command in ["VIEW", "SEQUENCE", "TABLE", "INDEX"] {
        let response = query(&mut owner, &format!("DROP {command} IF EXISTS r0c_w1_absent_schema.missing")).await;
        completed(&response, &format!("DROP {command}"));
        let notice = response.iter().find(|m| m.0 == b'N').unwrap();
        assert_eq!(field(&notice.1, b'C'), "00000");
        assert_eq!(field(&notice.1, b'M'), "schema \"r0c_w1_absent_schema\" does not exist, skipping");
    }
    for (kind, command, code) in [("view", "VIEW", "42P01"), ("table", "TABLE", "42P01"),
        ("sequence", "SEQUENCE", "42P01"), ("index", "INDEX", "42704")] {
        let object = format!("missing_{kind}");
        let response = query(&mut owner, &format!("DROP {command} {object}")).await;
        let error = response.iter().find(|m| m.0 == b'E').unwrap();
        assert_eq!(field(&error.1, b'C'), code);
        assert_eq!(field(&error.1, b'M'), format!("{kind} \"{object}\" does not exist"));
    }
    let mut other = connect(pg.address, "other").await;
    for denied in [
        sql,
        "ALTER TABLE _skedaddle.apply_progress ADD IF NOT EXISTS record_kind varchar(0)",
    ] {
        let response = query(&mut other, denied).await;
        let error = response.iter().find(|m| m.0 == b'E').unwrap();
        assert_eq!(field(&error.1, b'C'), "42501");
        assert!(!response.iter().any(|m| m.0 == b'N' || m.0 == b'C'));
    }
    drop(other);
    drop(owner);
    pg.shutdown().await.unwrap();
    drop(service);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn alter_requires_owner_and_rename_publishes_authority_atomically() {
    let root = std::env::var_os("GIGGLES_TEST_ARTIFACTS")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let path = root.join(format!(
        "alter-owner-{}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
    ));
    std::fs::create_dir_all(&path).unwrap();
    let admin = Admin::create(path.join("db.redb"), Limits::default()).unwrap();
    let roles = admin.roles();
    for name in ["owner", "other"] {
        roles
            .create(NewRole {
                name: name.into(),
                login: true,
                ..NewRole::default()
            })
            .unwrap();
        roles
            .grant_schema(
                "public",
                name,
                &[SchemaPrivilege::Create, SchemaPrivilege::Usage],
            )
            .unwrap();
    }
    let fingerprint = "a".repeat(64);
    let binding = Binding {
        fingerprint: fingerprint.clone(),
        role: Role::NoSql { domain: admin.authority_metadata().unwrap().0.to_string() },
    };
    let service = Service::new(admin.clone(), vec![binding], Clock::new(0, 3600).unwrap()).unwrap();
    let mut task = service.session(&fingerprint).unwrap();
    let mut owner = service.role_session("owner").unwrap();
    let mut other = service.role_session("other").unwrap();
    owner
        .execute_sql(
            "CREATE TABLE original (id BIGINT PRIMARY KEY, value TEXT)",
            &[],
        )
        .unwrap();
    owner
        .execute_sql("CREATE TABLE occupied (id BIGINT PRIMARY KEY)", &[])
        .unwrap();
    roles
        .grant_table("original", "other", &[TablePrivilege::Select])
        .unwrap();
    // Every supported action denies both session categories before resolving
    // either the relation or its columns. SELECT grants do not authorize DDL.
    let actions = [
        "ADD COLUMN added BIGINT",
        "DROP COLUMN missing",
        "RENAME COLUMN missing TO changed",
        "RENAME TO stolen",
        "ALTER COLUMN missing SET DEFAULT 1",
        "ALTER COLUMN missing DROP DEFAULT",
        "ALTER COLUMN missing SET NOT NULL",
        "ALTER COLUMN missing DROP NOT NULL",
        "ADD CONSTRAINT denied_unique UNIQUE (missing)",
    ];
    for table in ["original", "missing"] {
        for action in actions {
            let sql = format!("ALTER TABLE {table} {action}");
            for (category, session) in [("non-owner", &mut other), ("task", &mut task)] {
                assert_eq!(
                    sqlstate(&session.execute_sql(&sql, &[]).unwrap_err()),
                    "42501",
                    "{category}: {sql}"
                );
            }
        }
    }
    // Reserved names are semantic failures, so authority still precedes them.
    for sql in [
        "ALTER TABLE original ADD COLUMN giggles_rowid BIGINT",
        "ALTER TABLE original RENAME COLUMN value TO giggles_rowid",
        "ALTER TABLE giggles_rowid ADD COLUMN added BIGINT",
    ] {
        for session in [&mut other, &mut task] {
            assert_eq!(
                sqlstate(&session.execute_sql(sql, &[]).unwrap_err()),
                "42501"
            );
        }
    }
    // Comments must not bypass the parse-only preauthorization entry point.
    for sql in [
        "/* authority witness */ ALTER TABLE missing DROP COLUMN absent",
        "-- authority witness\nALTER TABLE original DROP COLUMN absent",
    ] {
        for (category, session) in [("non-owner", &mut other), ("task", &mut task)] {
            assert_eq!(
                sqlstate(&session.execute_sql(sql, &[]).unwrap_err()),
                "42501",
                "{category}: {sql}"
            );
        }
    }
    // E939: task sessions cannot learn unsupported SQL classifications.
    assert_eq!(
        sqlstate(
            &task
                .execute_sql("ALTER TABLE original ADD CHECK (id > 0)", &[])
                .unwrap_err()
        ),
        "42501"
    );
    // A malformed later action must be parsed before any ownership decision.
    for session in [&mut other, &mut task] {
        assert_eq!(
            sqlstate(
                &session
                    .execute_sql(
                        "ALTER TABLE original ADD COLUMN parsed BIGINT, DROP COLUMN",
                        &[]
                    )
                    .unwrap_err()
            ),
            "42601"
        );
    }
    owner.execute_sql("BEGIN", &[]).unwrap();
    assert_eq!(
        sqlstate(
            &owner
                .execute_sql(
                    "ALTER TABLE original ADD COLUMN parsed BIGINT, DROP COLUMN",
                    &[]
                )
                .unwrap_err()
        ),
        "42601"
    );
    assert!(owner.is_aborted());
    owner.execute_sql("ROLLBACK", &[]).unwrap();
    assert!(!owner.is_aborted());
    assert_eq!(
        sqlstate(
            &owner
                .execute_sql("ALTER TABLE original RENAME TO occupied", &[])
                .unwrap_err()
        ),
        "42P07"
    );
    assert_eq!(
        roles.table_owner("original").unwrap().as_deref(),
        Some("owner")
    );
    assert_eq!(
        roles.table_owner("occupied").unwrap().as_deref(),
        Some("owner")
    );
    owner.execute_sql("BEGIN", &[]).unwrap();
    owner
        .execute_sql("ALTER TABLE original RENAME TO staged", &[])
        .unwrap();
    assert_eq!(
        roles.table_owner("original").unwrap().as_deref(),
        Some("owner")
    );
    assert_eq!(roles.table_owner("staged").unwrap(), None);
    owner
        .execute_sql("ALTER TABLE staged RENAME COLUMN value TO content", &[])
        .unwrap();
    owner.execute_sql("ROLLBACK", &[]).unwrap();
    assert_eq!(
        roles.table_owner("original").unwrap().as_deref(),
        Some("owner")
    );
    assert_eq!(roles.table_owner("staged").unwrap(), None);
    owner.execute_sql("BEGIN", &[]).unwrap();
    owner
        .execute_sql("ALTER TABLE original RENAME TO renamed", &[])
        .unwrap();
    owner.execute_sql("COMMIT", &[]).unwrap();
    assert_eq!(roles.table_owner("original").unwrap(), None);
    assert_eq!(
        roles.table_owner("renamed").unwrap().as_deref(),
        Some("owner")
    );
    assert!(
        roles
            .table_privileges("original", "other")
            .unwrap()
            .is_empty()
    );
    assert!(
        roles
            .table_privileges("renamed", "other")
            .unwrap()
            .contains(&TablePrivilege::Select)
    );
    assert_eq!(
        sqlstate(
            &other
                .execute_sql("ALTER TABLE renamed RENAME TO stolen", &[])
                .unwrap_err()
        ),
        "42501"
    );
    owner
        .execute_sql("ALTER TABLE renamed RENAME TO final_name", &[])
        .unwrap();
    assert_eq!(roles.table_owner("renamed").unwrap(), None);
    assert_eq!(
        roles.table_owner("final_name").unwrap().as_deref(),
        Some("owner")
    );
    // Stale destination-only grants are replaced with source authority.
    roles
        .grant_table("blocked", "other", &[TablePrivilege::Update])
        .unwrap();
    owner
        .execute_sql("ALTER TABLE final_name RENAME TO blocked", &[])
        .unwrap();
    assert_eq!(roles.table_owner("final_name").unwrap(), None);
    assert_eq!(
        roles.table_owner("blocked").unwrap().as_deref(),
        Some("owner")
    );
    let privileges = roles.table_privileges("blocked", "other").unwrap();
    assert!(privileges.contains(&TablePrivilege::Select));
    assert!(!privileges.contains(&TablePrivilege::Update));
    owner
        .execute_sql("ALTER TABLE blocked RENAME TO final_name", &[])
        .unwrap();
    owner
        .execute_sql("CREATE TABLE sequence_owner (id serial PRIMARY KEY)", &[])
        .unwrap();
    owner.execute_sql("BEGIN", &[]).unwrap();
    owner
        .execute_sql(
            "ALTER TABLE sequence_owner RENAME TO staged_sequence_owner",
            &[],
        )
        .unwrap();
    owner
        .execute_sql("INSERT INTO staged_sequence_owner DEFAULT VALUES", &[])
        .unwrap();
    owner
        .execute_sql("SELECT currval('sequence_owner_id_seq')", &[])
        .unwrap();
    owner.execute_sql("ROLLBACK", &[]).unwrap();
    assert_eq!(
        roles.table_owner("sequence_owner").unwrap().as_deref(),
        Some("owner")
    );
    assert_eq!(roles.table_owner("staged_sequence_owner").unwrap(), None);
    owner.execute_sql("BEGIN", &[]).unwrap();
    owner
        .execute_sql(
            "ALTER TABLE sequence_owner RENAME TO committed_sequence_owner",
            &[],
        )
        .unwrap();
    owner
        .execute_sql("INSERT INTO committed_sequence_owner DEFAULT VALUES", &[])
        .unwrap();
    owner.execute_sql("COMMIT", &[]).unwrap();
    owner
        .execute_sql("INSERT INTO committed_sequence_owner DEFAULT VALUES", &[])
        .unwrap();
    // Renaming the table preserves its sequence's original name.
    owner
        .execute_sql("SELECT currval('sequence_owner_id_seq')", &[])
        .unwrap();
    assert_eq!(roles.table_owner("sequence_owner").unwrap(), None);
    assert_eq!(
        roles
            .table_owner("committed_sequence_owner")
            .unwrap()
            .as_deref(),
        Some("owner")
    );
    let rename = owner
        .prepare("ALTER TABLE final_name RENAME TO failed_name")
        .unwrap();
    admin
        .set_crash_point(CrashPoint::ErrorBeforeCommit)
        .unwrap();
    assert!(owner.execute_prepared(&rename, &[]).is_err());
    drop(task);
    drop(other);
    drop(owner);
    drop(service);
    drop(roles);
    drop(admin);
    let reopened = Admin::open(path.join("db.redb"), Limits::default()).unwrap();
    let names: Vec<_> = reopened
        .catalog()
        .unwrap()
        .1
        .into_iter()
        .map(|t| t.name)
        .collect();
    assert!(names.iter().any(|name| name == "final_name"));
    assert!(!names.iter().any(|name| name == "failed_name"));
    assert_eq!(
        reopened
            .roles()
            .table_owner("final_name")
            .unwrap()
            .as_deref(),
        Some("owner")
    );
    assert_eq!(reopened.roles().table_owner("failed_name").unwrap(), None);
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}
