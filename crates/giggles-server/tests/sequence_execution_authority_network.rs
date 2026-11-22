//! V2 execution-time sequence authority (GD-0084 Q24).
//!
//! nextval/currval/setval are authorized on the sequence the call actually
//! resolves to, at execution, against the latest committed role catalog and
//! ownership link. A dynamic name (`$1`, a text expression) gets the same
//! answer as a literal, and a denied call leaves the counter untouched.
//! GigglesDB has no sequence ACLs: the owner-analogue (superuser, or a member
//! of the owner of the owning table or standalone sequence) holds USAGE,
//! SELECT and UPDATE together; table grants confer nothing. Referential
//! actions run as the referencing table's owner, as in PostgreSQL 18.
//! The fixture is explicitly V2; tests/sequence_authority_network.rs covers
//! Legacy.
#![cfg(feature = "engine-rocksdb")]
mod api_support;
use api_support::{Fixture as ApiFixture, rpc};
use giggles_core::{Engine, roles::NewRole};
use giggles_server::{
    Running,
    api::json as wire_json,
    auth::{PostgresqlConfig, serve_postgresql},
};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

#[derive(Clone, Copy, Debug)]
enum Protocol {
    Simple,
    Extended,
}
const PROTOCOLS: [Protocol; 2] = [Protocol::Simple, Protocol::Extended];

impl Protocol {
    /// The first column of the first row, as i64.
    async fn value(self, client: &Client, sql: &str) -> Result<i64, tokio_postgres::Error> {
        match self {
            Self::Simple => {
                for message in client.simple_query(sql).await? {
                    if let SimpleQueryMessage::Row(row) = message {
                        return Ok(row.get(0).expect("non-null").parse().unwrap());
                    }
                }
                panic!("{sql}: no row")
            }
            Self::Extended => Ok(client.query_one(sql, &[]).await?.get::<_, i64>(0)),
        }
    }
    async fn ok(self, client: &Client, sql: &str) {
        match self {
            Self::Simple => client.simple_query(sql).await.map(|_| ()),
            Self::Extended => client.execute(sql, &[]).await.map(|_| ()),
        }
        .unwrap_or_else(|e| panic!("{self:?}: {sql}: {e}"));
    }
    async fn error(self, client: &Client, sql: &str, expected: &str) {
        let result = match self {
            Self::Simple => client.simple_query(sql).await.map(|_| ()),
            Self::Extended => client.execute(sql, &[]).await.map(|_| ()),
        };
        assert_eq!(code(result), expected, "{self:?}: {sql}");
    }
}
/// Run `;`-separated statements one at a time: an implicit multi-statement block
/// refuses DDL (25001).
async fn run(client: &Client, sql: &str) {
    for statement in sql.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        client.batch_execute(statement).await.unwrap_or_else(|e| panic!("{statement}: {:?}", e.code()));
    }
}
fn code<T>(result: Result<T, tokio_postgres::Error>) -> String {
    match result {
        Ok(_) => "ok".into(),
        Err(error) => error.code().unwrap_or_else(|| panic!("no SQLSTATE: {error}")).code().into(),
    }
}

struct Fixture {
    api: ApiFixture,
    pg: Running,
    root: Client,
}
impl Fixture {
    /// root is superuser; alice owns a_t (BIGSERIAL a_t_id_seq) and standalone a_s;
    /// bob has SELECT, INSERT, UPDATE on a_t; carol is an INHERIT member of alice;
    /// dave is a NOINHERIT member of alice. bob, carol and dave each own a sequence of
    /// their own, so their statement-level sequence scope is not empty and a dynamic
    /// name reaches execution.
    async fn new() -> Self {
        let api = ApiFixture::catalog_empty_engine(Engine::V2).await;
        assert_eq!(api.admin.engine(), Engine::V2);
        for (name, superuser, inherit) in [
            ("root", true, None),
            ("alice", false, None),
            ("bob", false, None),
            ("carol", false, None),
            ("dave", false, Some(false)),
        ] {
            api.admin
                .roles()
                .create(NewRole {
                    name: name.into(),
                    login: true,
                    superuser,
                    inherit,
                    password: Some(giggles_server::auth::scram::Verifier::generate("secret")),
                    ..Default::default()
                })
                .unwrap();
        }
        let config: PostgresqlConfig = serde_json::from_value(json!({
            "listen":"127.0.0.1:0", "tls":"disabled", "loopback_trust":true
        }))
        .unwrap();
        let pg = serve_postgresql(
            "127.0.0.1:0".parse().unwrap(),
            api.service.clone(),
            Arc::new(config.build().unwrap()),
        )
        .await
        .unwrap();
        let root = Self::connect_to(&pg, "root").await;
        for role in ["alice", "bob", "carol", "dave"] {
            run(&root, &format!("GRANT CREATE ON SCHEMA public TO {role}")).await;
        }
        let f = Self { api, pg, root };
        let alice = f.connect("alice").await;
        run(&alice, "CREATE TABLE a_t (id BIGSERIAL PRIMARY KEY, v BIGINT); CREATE SEQUENCE a_s; \
                 GRANT SELECT, INSERT, UPDATE ON a_t TO bob",).await;
        for (role, sequence) in [("bob", "b_own"), ("carol", "c_own"), ("dave", "d_own")] {
            run(&f.connect(role).await, &format!("CREATE SEQUENCE {sequence}")).await;
        }
        run(&f.root, "GRANT alice TO carol; GRANT alice TO dave").await;
        f
    }
    async fn connect_to(pg: &Running, role: &str) -> Client {
        let (client, connection) = tokio_postgres::Config::new()
            .host("127.0.0.1")
            .port(pg.address.port())
            .user(role)
            .password("secret")
            .dbname("postgres")
            .connect(NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }
    async fn connect(&self, role: &str) -> Client {
        Self::connect_to(&self.pg, role).await
    }
    /// The next counter value, drawn by the superuser.
    async fn next(&self, sequence: &str) -> i64 {
        Protocol::Extended.value(&self.root, &format!("SELECT nextval('{sequence}')")).await.unwrap()
    }
    async fn stop(self) {
        drop(self.root);
        self.pg.shutdown().await.unwrap();
        self.api.stop().await;
    }
}

/// Every dynamic spelling of the three functions for `sequence`, per protocol.
fn dynamic_forms(sequence: &str) -> Vec<String> {
    let (head, tail) = sequence.split_at(2);
    let expr = format!("'{head}' || '{tail}'");
    vec![
        format!("SELECT nextval({expr})"),
        format!("SELECT setval({expr}, 900)"),
        format!("SELECT setval({expr}, 900, false)"),
        format!("SELECT currval({expr})"),
    ]
}
/// `$1`-parameter forms through a named prepared statement (extended protocol).
async fn parameter_outcomes(client: &Client, sequence: &str) -> Vec<String> {
    let next = client.prepare("SELECT nextval($1)").await.unwrap();
    let set = client.prepare("SELECT setval($1, $2)").await.unwrap();
    let current = client.prepare("SELECT currval($1)").await.unwrap();
    vec![
        code(client.query_one(&next, &[&sequence]).await),
        code(client.query_one(&set, &[&sequence, &900_i64]).await),
        code(client.query_one(&current, &[&sequence]).await),
    ]
}

#[tokio::test]
async fn dynamic_calls_follow_the_resolved_sequence_owner() {
    let f = Fixture::new().await;
    let alice = f.connect("alice").await;
    let bob = f.connect("bob").await;
    for protocol in PROTOCOLS {
        for sequence in ["a_s", "a_t_id_seq"] {
            // The owner-analogue may call every function dynamically.
            protocol.value(&alice, &format!("SELECT nextval('{sequence}')")).await.unwrap();
            for sql in dynamic_forms(sequence) {
                protocol.value(&alice, &sql).await.unwrap_or_else(|e| panic!("{protocol:?} alice {sql}: {e}"));
            }
            // An outsider with SELECT, INSERT and UPDATE on the owning table may not,
            // and the denial leaves the counter where it was.
            let before = f.next(sequence).await;
            for sql in dynamic_forms(sequence) {
                protocol.error(&bob, &sql, "42501").await;
            }
            assert_eq!(f.next(sequence).await, before + 1, "{protocol:?} {sequence}: bob moved the counter");
            // The superuser may.
            for sql in dynamic_forms(sequence) {
                protocol.value(&f.root, &sql).await.unwrap_or_else(|e| panic!("{protocol:?} root {sql}: {e}"));
            }
            // Restore a counter that keeps the next checks independent of setval.
            run(&f.root, &format!("SELECT setval('{sequence}', 1000)")).await;
        }
        // A missing name keeps the missing-relation answer for a role with some scope.
        protocol.error(&bob, "SELECT nextval('mis' || 'sing')", "42P01").await;
        // The role's own sequence stays callable dynamically.
        protocol.value(&bob, "SELECT nextval('b_' || 'own')").await.unwrap();
    }
    // `$1` parameters, extended protocol, named statements.
    for sequence in ["a_s", "a_t_id_seq"] {
        let before = f.next(sequence).await;
        assert_eq!(parameter_outcomes(&bob, sequence).await, ["42501", "42501", "42501"], "bob {sequence}");
        assert_eq!(f.next(sequence).await, before + 1, "bob moved {sequence} through $1");
        assert_eq!(parameter_outcomes(&alice, sequence).await, ["ok", "ok", "ok"], "alice {sequence}");
        assert_eq!(parameter_outcomes(&f.root, sequence).await, ["ok", "ok", "ok"], "root {sequence}");
    }
    assert_eq!(code(bob.query_one("SELECT nextval($1)", &[&"missing"]).await), "42P01");
    // Literal names keep the statement-level answer.
    Protocol::Extended.error(&bob, "SELECT nextval('a_s')", "42501").await;
    Protocol::Extended.value(&alice, "SELECT nextval('a_s')").await.unwrap();
    f.stop().await;
}

#[tokio::test]
async fn membership_and_set_role_decide_dynamic_calls() {
    let f = Fixture::new().await;
    let carol = f.connect("carol").await;
    let dave = f.connect("dave").await;
    for protocol in PROTOCOLS {
        for sequence in ["a_s", "a_t_id_seq"] {
            // An INHERIT member of the owner holds the owner's privileges.
            for sql in dynamic_forms(sequence) {
                protocol.value(&carol, &sql).await.unwrap_or_else(|e| panic!("{protocol:?} carol {sql}: {e}"));
            }
            // A NOINHERIT member does not, until SET ROLE.
            let before = f.next(sequence).await;
            for sql in dynamic_forms(sequence) {
                protocol.error(&dave, &sql, "42501").await;
            }
            assert_eq!(f.next(sequence).await, before + 1, "{protocol:?} {sequence}: dave moved the counter");
            protocol.ok(&dave, "SET ROLE alice").await;
            for sql in dynamic_forms(sequence) {
                protocol.value(&dave, &sql).await.unwrap_or_else(|e| panic!("{protocol:?} dave as alice {sql}: {e}"));
            }
            protocol.ok(&dave, "RESET ROLE").await;
            protocol.error(&dave, &format!("SELECT nextval('{sequence}' || '')"), "42501").await;
        }
    }
    f.stop().await;
}

#[tokio::test]
async fn committed_authority_changes_apply_at_the_next_statement() {
    let f = Fixture::new().await;
    let carol = f.connect("carol").await;
    let alice = f.connect("alice").await;
    let carol_next = carol.prepare("SELECT nextval($1)").await.unwrap();
    let alice_next = alice.prepare("SELECT nextval($1)").await.unwrap();
    for isolation in [None, Some("READ COMMITTED"), Some("REPEATABLE READ")] {
        // Membership dropped by another session.
        if let Some(level) = isolation {
            run(&carol, &format!("BEGIN ISOLATION LEVEL {level}")).await;
        }
        assert_eq!(code(carol.query_one(&carol_next, &[&"a_s"]).await), "ok", "{isolation:?} warm");
        run(&f.root, "REVOKE alice FROM carol").await;
        let before = f.next("a_s").await;
        assert_eq!(code(carol.query_one(&carol_next, &[&"a_s"]).await), "42501", "{isolation:?} revoked");
        assert_eq!(f.next("a_s").await, before + 1, "{isolation:?}: carol moved the counter");
        if isolation.is_some() {
            run(&carol, "ROLLBACK").await;
        }
        run(&f.root, "GRANT alice TO carol").await;
        assert_eq!(code(carol.query_one(&carol_next, &[&"a_s"]).await), "ok", "{isolation:?} regranted");

        // Owner change committed by another session (V2 has no ALTER SEQUENCE OWNER TO).
        if let Some(level) = isolation {
            run(&alice, &format!("BEGIN ISOLATION LEVEL {level}")).await;
        }
        assert_eq!(code(alice.query_one(&alice_next, &[&"a_s"]).await), "ok", "{isolation:?} warm owner");
        f.api.admin.roles().set_table_owner("a_s", "bob").unwrap();
        let before = f.next("a_s").await;
        assert_eq!(code(alice.query_one(&alice_next, &[&"a_s"]).await), "42501", "{isolation:?} owner moved");
        assert_eq!(f.next("a_s").await, before + 1, "{isolation:?}: alice moved the counter");
        if isolation.is_some() {
            run(&alice, "ROLLBACK").await;
        }
        f.api.admin.roles().set_table_owner("a_s", "alice").unwrap();
        assert_eq!(code(alice.query_one(&alice_next, &[&"a_s"]).await), "ok", "{isolation:?} owner back");
    }
    f.stop().await;
}

#[tokio::test]
async fn repeatable_read_follows_a_committed_owned_by_transfer() {
    let f = Fixture::new().await;
    let alice = f.connect("alice").await;
    let bob = f.connect("bob").await;
    run(&bob, "CREATE TABLE b_t (id BIGINT PRIMARY KEY)").await;
    run(&alice, "CREATE SEQUENCE x_s").await;
    let dynamic = alice.prepare("SELECT nextval($1)").await.unwrap();
    let literal = alice.prepare("SELECT nextval('x_s')").await.unwrap();
    run(&alice, "BEGIN ISOLATION LEVEL REPEATABLE READ").await;
    assert_eq!(code(alice.query_one(&dynamic, &[&"x_s"]).await), "ok");
    assert_eq!(code(alice.query_one(&literal, &[]).await), "ok");
    run(&f.root, "ALTER SEQUENCE x_s OWNED BY b_t.id").await;
    let before = f.next("x_s").await;
    assert_eq!(code(alice.query_one(&dynamic, &[&"x_s"]).await), "42501", "dynamic after transfer");
    run(&alice, "ROLLBACK").await;
    assert_eq!(f.next("x_s").await, before + 1, "alice moved x_s after the transfer (dynamic)");
    // PostgreSQL 18.6 keeps the owner across OWNED BY NONE. Use a fresh
    // standalone sequence of alice's for the literal-name transfer.
    run(&alice, "CREATE SEQUENCE literal_s").await;
    let literal = alice.prepare("SELECT nextval('literal_s')").await.unwrap();
    run(&alice, "BEGIN ISOLATION LEVEL REPEATABLE READ").await;
    assert_eq!(code(alice.query_one(&literal, &[]).await), "ok", "literal warm");
    run(&f.root, "ALTER SEQUENCE literal_s OWNED BY b_t.id").await;
    let before = f.next("literal_s").await;
    assert_eq!(code(alice.query_one(&literal, &[]).await), "42501", "literal after transfer");
    run(&alice, "ROLLBACK").await;
    assert_eq!(f.next("literal_s").await, before + 1, "alice moved literal_s after the transfer");
    // bob now owns both through b_t.
    for sequence in ["x_s", "literal_s"] {
        assert_eq!(code(bob.query_one("SELECT nextval($1)", &[&sequence]).await), "ok");
    }
    f.stop().await;
}

#[tokio::test]
async fn defaults_and_referential_actions_keep_their_authority() {
    let f = Fixture::new().await;
    let alice = f.connect("alice").await;
    let bob = f.connect("bob").await;
    let carol = f.connect("carol").await;
    for protocol in PROTOCOLS {
        // Serial defaults: owner and owner-member insert, an outsider with INSERT may not.
        protocol.ok(&alice, "INSERT INTO a_t (v) VALUES (1)").await;
        protocol.ok(&carol, "INSERT INTO a_t (v) VALUES (2)").await;
        let before = f.next("a_t_id_seq").await;
        protocol.error(&bob, "INSERT INTO a_t (v) VALUES (3)", "42501").await;
        assert_eq!(f.next("a_t_id_seq").await, before + 1, "{protocol:?}: bob's serial INSERT drew a value");
    }
    // Identity defaults keep today's static answer.
    run(&alice, "CREATE TABLE i_t (id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v BIGINT); \
             GRANT SELECT, INSERT ON i_t TO bob",).await;
    for protocol in PROTOCOLS {
        protocol.ok(&alice, "INSERT INTO i_t (v) VALUES (1)").await;
        protocol.ok(&carol, "INSERT INTO i_t (v) VALUES (2)").await;
        let before = Protocol::Extended.value(&alice, "INSERT INTO i_t (v) VALUES (8) RETURNING id").await.unwrap();
        protocol.error(&bob, "INSERT INTO i_t (v) VALUES (3)", "42501").await;
        let after = Protocol::Extended.value(&alice, "INSERT INTO i_t (v) VALUES (9) RETURNING id").await.unwrap();
        assert_eq!(after, before + 1, "{protocol:?}: bob's identity INSERT drew a value");
    }

    // Referential actions run as the referencing table's owner.
    run(&alice, "CREATE TABLE p (id BIGINT PRIMARY KEY); INSERT INTO p VALUES (1), (2), (6), (7), (8), (9); \
             CREATE SEQUENCE c_s; \
             CREATE TABLE c (id BIGINT PRIMARY KEY, pid BIGINT DEFAULT nextval('c_s') REFERENCES p (id) ON DELETE SET DEFAULT); \
             INSERT INTO c VALUES (1, 9), (2, 6); \
             CREATE TABLE cc (id BIGINT PRIMARY KEY, pid BIGINT REFERENCES p (id) ON DELETE CASCADE); \
             INSERT INTO cc VALUES (1, 8); \
             GRANT SELECT, DELETE ON p TO bob",).await;
    // bob may delete parents; the child default (alice's c_s) is drawn as alice.
    Protocol::Simple.ok(&bob, "DELETE FROM p WHERE id = 9").await;
    assert_eq!(Protocol::Extended.value(&alice, "SELECT pid FROM c WHERE id = 1").await.unwrap(), 1);
    Protocol::Extended.ok(&bob, "DELETE FROM p WHERE id = 8").await;
    assert_eq!(Protocol::Extended.value(&alice, "SELECT count(*) FROM cc").await.unwrap(), 0);
    // A child whose owner has no authority on its default's sequence: the action is
    // denied for every deleting role, and the sequence does not move.
    run(&f.root, "CREATE SEQUENCE root_s; \
             CREATE TABLE c2 (id BIGINT PRIMARY KEY, pid BIGINT DEFAULT nextval('root_s') REFERENCES p (id) ON DELETE SET DEFAULT); \
             INSERT INTO c2 VALUES (1, 7)",).await;
    f.api.admin.roles().set_table_owner("c2", "alice").unwrap();
    for protocol in PROTOCOLS {
        protocol.error(&bob, "DELETE FROM p WHERE id = 7", "42501").await;
        // The superuser who owns root_s is refused too: the child owner is judged.
        protocol.error(&f.root, "DELETE FROM p WHERE id = 7", "42501").await;
    }
    assert_eq!(f.next("root_s").await, 1, "the denied action drew from root_s");
    // With authority back on the child owner, the action succeeds.
    run(&f.root, "GRANT root TO alice").await;
    Protocol::Extended.ok(&bob, "DELETE FROM p WHERE id = 7").await;
    assert_eq!(Protocol::Extended.value(&alice, "SELECT pid FROM c2 WHERE id = 1").await.unwrap(), 1 + 1);
    f.stop().await;
}

#[tokio::test]
async fn a_replacement_sequence_is_judged_on_its_own_owner() {
    let f = Fixture::new().await;
    let alice = f.connect("alice").await;
    let bob = f.connect("bob").await;
    run(&alice, "CREATE SEQUENCE r_s").await;
    let dynamic = alice.prepare("SELECT nextval($1)").await.unwrap();
    assert_eq!(code(alice.query_one(&dynamic, &[&"r_s"]).await), "ok");
    run(&f.root, "DROP SEQUENCE r_s").await;
    run(&bob, "CREATE SEQUENCE r_s").await;
    assert_eq!(code(alice.query_one(&dynamic, &[&"r_s"]).await), "42501", "replacement owned by bob");
    for protocol in PROTOCOLS {
        protocol.error(&alice, "SELECT nextval('r_' || 's')", "42501").await;
    }
    assert_eq!(Protocol::Extended.value(&bob, "SELECT nextval('r_s')").await.unwrap(), 1);
    f.stop().await;
}

// Native wire: typed nextval/currval/setval reach the same V2 execution check.
async fn native(f: &ApiFixture, token: &str, request: Value, grpc: bool) -> String {
    let reply: Value = if grpc {
        let wire = wire_json::decode_execute(&serde_json::to_vec(&request).unwrap()).unwrap();
        let reply = f.grpc_client(&f.app).await.execute(rpc(token, wire)).await.unwrap().into_inner();
        serde_json::from_slice(&wire_json::encode_reply(&reply).unwrap()).unwrap()
    } else {
        let (status, bytes) = f
            .http(&f.app, "POST", "/v1/session/execute", Some(token), serde_json::to_vec(&request).unwrap(), false)
            .await
            .unwrap();
        assert_eq!(status, 200, "{}", String::from_utf8_lossy(&bytes));
        serde_json::from_slice(&bytes).unwrap()
    };
    if reply["code"] == "native_error" && !reply["nativeError"]["sqlstate"].is_null() {
        reply["nativeError"]["sqlstate"].as_str().unwrap().into()
    } else if reply["code"] == "native_error" {
        reply["nativeError"]["kind"].as_str().unwrap().into()
    } else {
        reply["code"].as_str().unwrap().into()
    }
}
async fn native_session(f: &ApiFixture, role: &str) -> String {
    let mut bindings = f.bindings.clone();
    bindings[0].role = giggles_server::Role::Database { name: role.into() };
    f.service.reload_bindings(bindings).unwrap();
    f.open_http(&f.app, false).await
}
fn sql_next(f: &ApiFixture, sequence: &str) -> i64 {
    let result = f.service.role_session("root").unwrap().execute_sql(&format!("SELECT nextval('{sequence}')"), &[]).unwrap();
    let giggles_core::relational::Value::Int(value) = result.rows[0].values[0] else { panic!("{result:?}") };
    value
}

#[tokio::test]
async fn native_sequence_calls_follow_the_sequence_owner() {
    let f = ApiFixture::catalog_empty_engine(Engine::V2).await;
    assert_eq!(f.admin.engine(), Engine::V2);
    for (name, superuser, inherit) in [("root", true, None), ("alice", false, None), ("bob", false, None),
        ("carol", false, None), ("dave", false, Some(false))] {
        f.admin.roles().create(NewRole { name: name.into(), login: true, superuser, inherit, ..Default::default() }).unwrap();
    }
    for role in ["alice", "bob"] {
        f.admin.roles().grant_schema("public", role, &[giggles_core::roles::SchemaPrivilege::Create]).unwrap();
    }
    f.admin.roles().grant_membership("alice", "carol").unwrap();
    f.admin.roles().grant_membership("alice", "dave").unwrap();
    f.service.role_session("alice").unwrap().execute_sql("CREATE SEQUENCE n_s", &[]).unwrap();
    f.service.role_session("bob").unwrap().execute_sql("CREATE SEQUENCE nb_own", &[]).unwrap();
    let call = |kind: &str| json!({"sequence":{kind:{"name":"n_s"}}});
    let set = json!({"sequence":{"setval":{"name":"n_s","value":"500"}}});
    for grpc in [false, true] {
        for (role, expected) in [("alice", "ok"), ("carol", "ok"), ("root", "ok"), ("bob", "42501"), ("dave", "42501")] {
            let token = native_session(&f, role).await;
            let before = sql_next(&f, "n_s");
            // nextval first so that currval is defined for the allowed roles.
            assert_eq!(native(&f, &token, call("nextval"), grpc).await, expected, "{role} nextval grpc={grpc}");
            assert_eq!(native(&f, &token, call("currval"), grpc).await, expected, "{role} currval grpc={grpc}");
            assert_eq!(native(&f, &token, set.clone(), grpc).await, expected, "{role} setval grpc={grpc}");
            let after = sql_next(&f, "n_s");
            if expected == "ok" {
                assert_eq!(after, 501, "{role} setval took effect");
            } else {
                assert_eq!(after, before + 1, "{role} moved the counter natively");
            }
        }
    }
    // A native INSERT gets no static sequence check: its default (alice's n_s) is
    // judged at execution for the inserting role. bob holds INSERT on the table.
    f.service.role_session("alice").unwrap()
        .execute_sql("CREATE TABLE nt (id BIGINT PRIMARY KEY, v BIGINT DEFAULT nextval('n_s'))", &[]).unwrap();
    f.admin.roles().grant_table("nt", "bob", &[giggles_core::roles::TablePrivilege::Insert]).unwrap();
    let insert = |id: i64| json!({"operation":{"request":{"insert":{"table":"nt","values":[
        {"column":"id","value":{"literal":{"intValue":id.to_string()}}}]}},"bindings":[]}});
    for grpc in [false, true] {
        let before = sql_next(&f, "n_s");
        let token = native_session(&f, "bob").await;
        assert_eq!(native(&f, &token, insert(if grpc { 2 } else { 1 }), grpc).await, "42501",
            "bob native INSERT grpc={grpc}");
        assert_eq!(sql_next(&f, "n_s"), before + 1, "bob's native INSERT drew n_s grpc={grpc}");
        let token = native_session(&f, "alice").await;
        assert_eq!(native(&f, &token, insert(if grpc { 22 } else { 21 }), grpc).await, "ok", "alice native INSERT grpc={grpc}");
    }
    f.stop().await;
}

// ---- Fix round (reviews of 61e5e886d): column defaults, FK edges, principals ----

#[tokio::test]
async fn repeatable_read_insert_default_follows_a_committed_owned_by_transfer() {
    let f = Fixture::new().await;
    let alice = f.connect("alice").await;
    let bob = f.connect("bob").await;
    run(&bob, "CREATE TABLE b_t (id BIGINT PRIMARY KEY)").await;
    run(&alice, "CREATE SEQUENCE d_s; CREATE TABLE d (id BIGINT DEFAULT nextval('d_s'), v BIGINT)").await;
    for protocol in PROTOCOLS {
        // Warm the snapshot without touching d: a transaction holding d would make the
        // OWNED BY change (d's default depends on d_s) wait for it.
        protocol.ok(&alice, "INSERT INTO d (v) VALUES (1)").await;
        run(&alice, "BEGIN ISOLATION LEVEL REPEATABLE READ").await;
        protocol.value(&alice, "SELECT nextval('d_s')").await.unwrap();
        run(&f.root, "ALTER SEQUENCE d_s OWNED BY b_t.id").await;
        let before = f.next("d_s").await;
        protocol.error(&alice, "INSERT INTO d (v) VALUES (2)", "42501").await;
        run(&alice, "ROLLBACK").await;
        assert_eq!(f.next("d_s").await, before + 1, "{protocol:?}: the denied default drew from d_s");
        // Restore alice as the effective owner before unlinking.
        run(&f.root, "ALTER SEQUENCE d_s OWNED BY d.id; ALTER SEQUENCE d_s OWNED BY NONE").await;
        protocol.ok(&alice, "INSERT INTO d (v) VALUES (3)").await;
    }
    f.stop().await;
}

#[tokio::test]
async fn a_sequence_call_nested_in_an_extension_default_is_checked() {
    let f = Fixture::new().await;
    let alice = f.connect("alice").await;
    let bob = f.connect("bob").await;
    run(&f.root, "CREATE EXTENSION \"uuid-ossp\"").await;
    run(
        &alice,
        "CREATE TABLE e (id BIGINT PRIMARY KEY, \
         u UUID DEFAULT uuid_generate_v5('6ba7b810-9dad-11d1-80b4-00c04fd430c8', nextval('a_s')::text)); \
         GRANT SELECT, INSERT ON e TO bob",
    )
    .await;
    Protocol::Extended.ok(&alice, "INSERT INTO e (id) VALUES (1)").await;
    for (protocol, id) in [(Protocol::Simple, 2), (Protocol::Extended, 3)] {
        let before = f.next("a_s").await;
        protocol.error(&bob, &format!("INSERT INTO e (id) VALUES ({id})"), "42501").await;
        assert_eq!(f.next("a_s").await, before + 1, "{protocol:?}: bob drew a_s through the nested default");
    }
    f.stop().await;
}

#[tokio::test]
async fn a_same_named_sequence_earlier_in_search_path_grants_nothing() {
    let f = Fixture::new().await;
    let bob = f.connect("bob").await;
    run(&f.root, "CREATE SCHEMA bx").await;
    f.api
        .admin
        .roles()
        .grant_schema("bx", "bob", &[giggles_core::roles::SchemaPrivilege::Usage, giggles_core::roles::SchemaPrivilege::Create])
        .unwrap();
    run(&bob, "CREATE SEQUENCE bx.a_t_id_seq").await;
    run(&bob, "SET search_path = bx, public").await;
    for protocol in PROTOCOLS {
        let before = f.next("a_t_id_seq").await;
        protocol.error(&bob, "INSERT INTO a_t (v) VALUES (3)", "42501").await;
        assert_eq!(f.next("a_t_id_seq").await, before + 1, "{protocol:?}: bob drew alice's public.a_t_id_seq");
    }
    // bob's own bx.a_t_id_seq stays his.
    assert_eq!(Protocol::Extended.value(&bob, "SELECT nextval('bx.a_t_id_seq')").await.unwrap(), 1);
    f.stop().await;
}

#[tokio::test]
async fn on_update_set_default_runs_as_the_child_owner() {
    let f = Fixture::new().await;
    let alice = f.connect("alice").await;
    let bob = f.connect("bob").await;
    run(
        &alice,
        "CREATE TABLE pu (id BIGINT PRIMARY KEY); INSERT INTO pu VALUES (1), (2), (5), (6); \
         CREATE SEQUENCE cu_s; \
         CREATE TABLE cu (id BIGINT PRIMARY KEY, pid BIGINT DEFAULT nextval('cu_s') REFERENCES pu (id) ON UPDATE SET DEFAULT); \
         INSERT INTO cu VALUES (1, 5); \
         GRANT SELECT, UPDATE ON pu TO bob",
    )
    .await;
    // Allowed: alice (child owner) owns cu_s; bob only updates the parent.
    Protocol::Simple.ok(&bob, "UPDATE pu SET id = 50 WHERE id = 5").await;
    assert_eq!(Protocol::Extended.value(&alice, "SELECT pid FROM cu WHERE id = 1").await.unwrap(), 1);
    // Denied: the child's owner (alice) has no authority on root's sequence.
    run(
        &f.root,
        "CREATE SEQUENCE ru_s; \
         CREATE TABLE cu2 (id BIGINT PRIMARY KEY, pid BIGINT DEFAULT nextval('ru_s') REFERENCES pu (id) ON UPDATE SET DEFAULT); \
         INSERT INTO cu2 VALUES (1, 6)",
    )
    .await;
    f.api.admin.roles().set_table_owner("cu2", "alice").unwrap();
    for protocol in PROTOCOLS {
        protocol.error(&bob, "UPDATE pu SET id = 60 WHERE id = 6", "42501").await;
        // The superuser who owns ru_s is refused too: the child owner is judged.
        protocol.error(&f.root, "UPDATE pu SET id = 60 WHERE id = 6", "42501").await;
    }
    assert_eq!(f.next("ru_s").await, 1, "the denied action drew from ru_s");
    f.stop().await;
}

#[tokio::test]
async fn a_child_without_an_owner_record_falls_back_to_the_caller() {
    let f = Fixture::new().await;
    let alice = f.connect("alice").await;
    let bob = f.connect("bob").await;
    let carol = f.connect("carol").await;
    run(
        &alice,
        "CREATE TABLE p3 (id BIGINT PRIMARY KEY); INSERT INTO p3 VALUES (1), (2), (7), (8); \
         CREATE SEQUENCE ns_s; GRANT SELECT, DELETE ON p3 TO bob; GRANT SELECT, DELETE ON p3 TO carol",
    )
    .await;
    run(
        &f.root,
        "CREATE TABLE c3 (id BIGINT PRIMARY KEY, pid BIGINT DEFAULT nextval('ns_s') REFERENCES p3 (id) ON DELETE SET DEFAULT); \
         INSERT INTO c3 VALUES (1, 7), (2, 8)",
    )
    .await;
    f.api.admin.roles().forget_table("c3").unwrap();
    assert_eq!(f.api.admin.roles().table_owner("c3").unwrap(), None);
    // bob could not call ns_s directly, so the ownerless child's action is refused.
    for protocol in PROTOCOLS {
        protocol.error(&bob, "DELETE FROM p3 WHERE id = 7", "42501").await;
    }
    // carol (member of ns_s's owner) could, so the action runs; it draws ns_s's
    // first value, proving the refusals drew nothing.
    Protocol::Extended.ok(&carol, "DELETE FROM p3 WHERE id = 8").await;
    assert_eq!(Protocol::Extended.value(&f.root, "SELECT pid FROM c3 WHERE id = 2").await.unwrap(), 1);
    f.stop().await;
}

#[tokio::test]
async fn set_session_authorization_narrows_and_restores_dynamic_calls() {
    let f = Fixture::new().await;
    for protocol in PROTOCOLS {
        protocol.ok(&f.root, "SET SESSION AUTHORIZATION bob").await;
        let before = Protocol::Extended.value(&f.connect("alice").await, "SELECT nextval('a_s')").await.unwrap();
        protocol.error(&f.root, "SELECT nextval('a_' || 's')", "42501").await;
        protocol.error(&f.root, "SELECT setval('a_' || 's', 5)", "42501").await;
        protocol.ok(&f.root, "RESET SESSION AUTHORIZATION").await;
        assert_eq!(protocol.value(&f.root, "SELECT nextval('a_' || 's')").await.unwrap(), before + 1, "{protocol:?}");
    }
    f.stop().await;
}

#[tokio::test]
async fn warmed_currval_and_setval_are_denied_after_revoke() {
    let f = Fixture::new().await;
    let carol = f.connect("carol").await;
    let next = carol.prepare("SELECT nextval($1)").await.unwrap();
    let current = carol.prepare("SELECT currval($1)").await.unwrap();
    let set = carol.prepare("SELECT setval($1, $2)").await.unwrap();
    assert_eq!(code(carol.query_one(&next, &[&"a_s"]).await), "ok");
    assert_eq!(code(carol.query_one(&current, &[&"a_s"]).await), "ok");
    assert_eq!(code(carol.query_one(&set, &[&"a_s", &700_i64]).await), "ok");
    run(&f.root, "REVOKE alice FROM carol").await;
    assert_eq!(code(carol.query_one(&current, &[&"a_s"]).await), "42501", "currval after revoke");
    assert_eq!(code(carol.query_one(&set, &[&"a_s", &900_i64]).await), "42501", "setval after revoke");
    assert_eq!(f.next("a_s").await, 701, "a denied call moved a_s");
    f.stop().await;
}

#[tokio::test]
async fn native_application_identity_follows_its_tables() {
    let f = ApiFixture::catalog_empty_engine(Engine::V2).await;
    assert_eq!(f.admin.engine(), Engine::V2);
    let mut setup = f.admin.sql_session();
    for sql in ["DROP TABLE app", "CREATE TABLE app (id BIGSERIAL PRIMARY KEY, v BIGINT)", "CREATE SEQUENCE other_s",
        "CREATE TABLE other_t (id BIGSERIAL PRIMARY KEY)"] {
        setup.execute(sql, &[], None).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
    let call = |name: &str| json!({"sequence":{"nextval":{"name":name}}});
    let insert = json!({"operation":{"request":{"insert":{"table":"app","values":[
        {"column":"v","value":{"literal":{"intValue":"1"}}}]}},"bindings":[]}});
    for grpc in [false, true] {
        // A fresh Application session: its table's sequence and its serial default
        // are in scope, other sequences are not.
        let token = f.open_http(&f.app, false).await;
        assert_eq!(native(&f, &token, call("app_id_seq"), grpc).await, "ok", "grpc={grpc}");
        assert_eq!(native(&f, &token, insert.clone(), grpc).await, "ok", "insert grpc={grpc}");
        assert_eq!(native(&f, &token, call("other_s"), grpc).await, "42501", "grpc={grpc}");
        assert_eq!(native(&f, &token, call("other_t_id_seq"), grpc).await, "42501", "grpc={grpc}");
    }
    // A native INSERT into its own table whose default draws a standalone sequence
    // (never in an Application's scope) is refused at execution.
    for sql in ["CREATE SEQUENCE fk_s", "ALTER TABLE app ALTER COLUMN v SET DEFAULT nextval('fk_s')"] {
        setup.execute(sql, &[], None).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
    let token = f.open_http(&f.app, false).await;
    let bare = json!({"operation":{"request":{"insert":{"table":"app","values":[
        {"column":"id","value":{"literal":{"intValue":"500"}}}]}},"bindings":[]}});
    for grpc in [false, true] {
        assert_eq!(native(&f, &token, bare.clone(), grpc).await, "42501", "Application INSERT drawing fk_s grpc={grpc}");
    }
    // The refusals drew nothing.
    f.admin.roles().create(NewRole { name: "root".into(), login: true, superuser: true, ..Default::default() }).unwrap();
    assert_eq!((sql_next(&f, "other_s"), sql_next(&f, "other_t_id_seq"), sql_next(&f, "fk_s")), (1, 1, 1));
    f.stop().await;
}

// B1 (re-check P2-1): a transaction's own rename of the owning table keeps its
// owner's serial/identity defaults and direct calls authorized.
#[tokio::test]
async fn a_private_rename_of_the_owning_table_keeps_owner_authority() {
    let f = Fixture::new().await;
    let alice = f.connect("alice").await;
    let bob = f.connect("bob").await;
    for (protocol, n) in [(Protocol::Simple, 1), (Protocol::Extended, 2)] {
        run(&alice, &format!("CREATE TABLE rt{n} (id BIGSERIAL PRIMARY KEY, v BIGINT); \
            CREATE TABLE ri{n} (id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v BIGINT)")).await;
        run(&alice, "BEGIN").await;
        run(&alice, &format!("ALTER TABLE rt{n} RENAME TO ru{n}")).await;
        protocol.ok(&alice, &format!("INSERT INTO ru{n} (v) VALUES (1)")).await;
        protocol.value(&alice, &format!("SELECT nextval('rt{n}_id_seq')")).await.unwrap();
        protocol.value(&alice, &format!("SELECT nextval('rt{n}' || '_id_seq')")).await.unwrap();
        run(&alice, &format!("ALTER TABLE ri{n} RENAME TO rj{n}")).await;
        protocol.ok(&alice, &format!("INSERT INTO rj{n} (v) VALUES (1)")).await;
        run(&alice, "COMMIT").await;
        // Committed: the outsider is still refused, and draws nothing.
        run(&alice, &format!("GRANT SELECT, INSERT ON ru{n} TO bob")).await;
        let before = f.next(&format!("rt{n}_id_seq")).await;
        protocol.error(&bob, &format!("INSERT INTO ru{n} (v) VALUES (2)"), "42501").await;
        protocol.error(&bob, &format!("SELECT nextval('rt{n}' || '_id_seq')"), "42501").await;
        assert_eq!(f.next(&format!("rt{n}_id_seq")).await, before + 1, "{protocol:?}: bob drew rt{n}_id_seq");
    }
    f.stop().await;
}

// Re-check 2 P3-1: an image holding a nondeterministic ICU text primary key makes
// paged DML ineligible, so V2 runs referential actions through the general
// constraints::enforce_with_rows fallback (constraints.rs SET DEFAULT site).
#[tokio::test]
async fn set_default_actions_on_the_general_constraint_path_keep_their_principal() {
    let f = Fixture::new().await;
    let alice = f.connect("alice").await;
    let bob = f.connect("bob").await;
    let carol = f.connect("carol").await;
    run(&f.root, "CREATE COLLATION ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false); \
        CREATE TABLE marker (id TEXT COLLATE ci PRIMARY KEY)").await;
    run(&alice, "CREATE TABLE pf (id BIGINT PRIMARY KEY); \
        INSERT INTO pf VALUES (1), (2), (3), (4), (101), (102), (103), (104), (105), (106), (111), (112), (113), (114), (115), (116); \
        CREATE SEQUENCE cf_s; CREATE SEQUENCE of_s; \
        CREATE TABLE cf1 (id BIGINT PRIMARY KEY, pid BIGINT DEFAULT nextval('cf_s') REFERENCES pf (id) ON DELETE SET DEFAULT ON UPDATE SET DEFAULT); \
        INSERT INTO cf1 VALUES (1, 101), (2, 102), (11, 111), (12, 112); \
        GRANT SELECT, DELETE, UPDATE ON pf TO bob").await;
    run(&f.root, "CREATE SEQUENCE rf_s; \
        CREATE TABLE cf2 (id BIGINT PRIMARY KEY, pid BIGINT DEFAULT nextval('rf_s') REFERENCES pf (id) ON DELETE SET DEFAULT ON UPDATE SET DEFAULT); \
        INSERT INTO cf2 VALUES (3, 103), (4, 104), (13, 113), (14, 114); \
        CREATE TABLE cf3 (id BIGINT PRIMARY KEY, pid BIGINT DEFAULT nextval('of_s') REFERENCES pf (id) ON DELETE SET DEFAULT ON UPDATE SET DEFAULT); \
        INSERT INTO cf3 VALUES (5, 105), (6, 106), (15, 115), (16, 116)").await;
    f.api.admin.roles().set_table_owner("cf2", "alice").unwrap();
    f.api.admin.roles().forget_table("cf3").unwrap();
    for (protocol, k) in [(Protocol::Simple, 0), (Protocol::Extended, 10)] {
        // Allowed: the child owner (alice) owns cf_s; bob only changes the parent.
        protocol.ok(&bob, &format!("DELETE FROM pf WHERE id = {}", 101 + k)).await;
        protocol.ok(&bob, &format!("UPDATE pf SET id = {} WHERE id = {}", 1102 + k, 102 + k)).await;
        // Refused for the superuser: the child owner (alice) lacks access to rf_s.
        protocol.error(&f.root, &format!("DELETE FROM pf WHERE id = {}", 103 + k), "42501").await;
        protocol.error(&f.root, &format!("UPDATE pf SET id = {} WHERE id = {}", 1104 + k, 104 + k), "42501").await;
        // Ownerless child: judged as a direct call by the caller.
        protocol.error(&bob, &format!("DELETE FROM pf WHERE id = {}", 105 + k), "42501").await;
        protocol.error(&bob, &format!("UPDATE pf SET id = {} WHERE id = {}", 1106 + k, 106 + k), "42501").await;
        protocol.ok(&carol, &format!("DELETE FROM pf WHERE id = {}", 105 + k)).await;
        protocol.ok(&carol, &format!("UPDATE pf SET id = {} WHERE id = {}", 1106 + k, 106 + k)).await;
    }
    // cf1 drew 1..4 as alice; rf_s never moved; of_s moved only for carol.
    assert_eq!(Protocol::Extended.value(&alice, "SELECT max(pid) FROM cf1").await.unwrap(), 4);
    assert_eq!(Protocol::Extended.value(&alice, "SELECT count(*) FROM cf1 WHERE pid <= 4").await.unwrap(), 4);
    assert_eq!(f.next("rf_s").await, 1, "a refused action drew from rf_s");
    assert_eq!(f.next("of_s").await, 5, "a refused action drew from of_s");
    f.stop().await;
}

#[tokio::test]
#[ignore = "seqauth2: CREATE VIEW rejects sequence expressions in the projection with 0A000"]
async fn seqauth2_views_check_the_callers_sequence_privilege() {
    let f = Fixture::new().await;
    let alice = f.connect("alice").await;
    let bob = f.connect("bob").await;
    run(&bob, "CREATE TABLE view_owned_by (id BIGINT PRIMARY KEY)").await;
    run(
        &alice,
        "CREATE VIEW seq_view AS SELECT nextval('a_s') AS n; \
         GRANT SELECT ON seq_view TO bob",
    )
    .await;
    let sql = "SELECT * FROM seq_view";
    for protocol in PROTOCOLS {
        let before = f.next("a_s").await;
        protocol.error(&bob, sql, "42501").await;
        assert_eq!(protocol.value(&alice, sql).await.unwrap(), before + 1, "{protocol:?}: {sql}");
    }

    run(&f.root, "ALTER SEQUENCE a_s OWNED BY view_owned_by.id").await;
    for protocol in PROTOCOLS {
        let before = f.next("a_s").await;
        protocol.error(&alice, sql, "42501").await;
        assert_eq!(protocol.value(&bob, sql).await.unwrap(), before + 1, "{protocol:?}: {sql}");
    }
    f.stop().await;
}

#[tokio::test]
async fn seqauth2_insert_returning_checks_sequence_privilege_before_inserting() {
    let f = Fixture::new().await;
    let alice = f.connect("alice").await;
    let bob = f.connect("bob").await;
    run(
        &alice,
        "CREATE TABLE seq_returning (id BIGINT PRIMARY KEY, v BIGINT); \
         GRANT SELECT, INSERT ON seq_returning TO bob",
    )
    .await;
    // Supplying an id lets bob use the table without drawing its serial sequence.
    run(&bob, "INSERT INTO a_t (id, v) VALUES (1000, 0)").await;

    for (round, protocol) in PROTOCOLS.into_iter().enumerate() {
        let v = round + 1;
        let serial_insert = format!("INSERT INTO a_t (v) VALUES ({v}) RETURNING id");
        let serial_count = format!("SELECT count(*) FROM a_t WHERE v = {v}");
        let before = f.next("a_t_id_seq").await;
        protocol.error(&bob, &serial_insert, "42501").await;
        assert_eq!(protocol.value(&alice, &serial_count).await.unwrap(), 0);
        assert_eq!(protocol.value(&alice, &serial_insert).await.unwrap(), before + 1);

        for (form, sequence) in ["'a_s'", "'a_' || 's'"].into_iter().enumerate() {
            let next_id = 1 + round * 4 + form * 2;
            let current_id = next_id + 1;
            let next = format!("INSERT INTO seq_returning VALUES ({next_id}, 1) RETURNING nextval({sequence})");
            let current = format!("INSERT INTO seq_returning VALUES ({current_id}, 1) RETURNING currval({sequence})");
            let count = format!("SELECT count(*) FROM seq_returning WHERE id IN ({next_id}, {current_id})");

            let before = f.next("a_s").await;
            protocol.error(&bob, &next, "42501").await;
            assert_eq!(protocol.value(&alice, &count).await.unwrap(), 0);
            let drawn = protocol.value(&alice, &next).await.unwrap();
            assert_eq!(drawn, before + 1, "{protocol:?}: {next}");

            protocol.error(&bob, &current, "42501").await;
            assert_eq!(protocol.value(&alice, &count).await.unwrap(), 1);
            assert_eq!(protocol.value(&alice, &current).await.unwrap(), drawn, "{protocol:?}: {current}");
            assert_eq!(f.next("a_s").await, drawn + 1, "{protocol:?}: {current}");
        }
    }
    f.stop().await;
}

#[tokio::test]
async fn seqauth2_scalar_subquery_checks_the_resolved_sequence() {
    let f = Fixture::new().await;
    let alice = f.connect("alice").await;
    let bob = f.connect("bob").await;
    for protocol in PROTOCOLS {
        for sql in [
            "SELECT (SELECT nextval('a_s'))",
            "SELECT (SELECT nextval('a_' || 's'))",
        ] {
            let before = f.next("a_s").await;
            protocol.error(&bob, sql, "42501").await;
            assert_eq!(protocol.value(&alice, sql).await.unwrap(), before + 1, "{protocol:?}: {sql}");
        }
    }
    f.stop().await;
}
