use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};
use tokio_postgres::{
    types::{ToSql, Type},
    Client, NoTls, Row,
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
fn table() -> String {
    format!(
        "{}_items",
        std::env::var("R0_PREFIX").unwrap_or("r0d_tokio".into())
    )
}
// Aborting on every exit also covers cancellation by the outer case deadline.
struct ConnectionTask(tokio::task::JoinHandle<std::result::Result<(), tokio_postgres::Error>>);
impl Drop for ConnectionTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}
#[derive(Debug)]
struct Assertion(&'static str);
impl std::fmt::Display for Assertion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for Assertion {}
#[derive(Debug)]
struct Null;
impl ToSql for Null {
    fn to_sql(&self, _: &Type, _: &mut bytes::BytesMut) -> Result<tokio_postgres::types::IsNull> {
        Ok(tokio_postgres::types::IsNull::Yes)
    }
    fn accepts(_: &Type) -> bool {
        true
    }
    tokio_postgres::types::to_sql_checked!();
}
fn type_name(t: &Type) -> &'static str {
    match *t {
        Type::BOOL => "bool",
        Type::BYTEA => "bytea",
        Type::INT8 => "int8",
        Type::INT2 => "int2",
        Type::INT4 => "int4",
        Type::TEXT => "text",
        Type::FLOAT4 => "float4",
        Type::FLOAT8 => "float8",
        Type::VARCHAR => "varchar",
        Type::TIMESTAMPTZ => "timestamptz",
        Type::NUMERIC => "numeric",
        Type::UUID => "uuid",
        _ => "unknown",
    }
}
fn require(ok: bool, reason: &'static str) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(Box::new(Assertion(reason)))
    }
}
fn error(e: &(dyn std::error::Error + Send + Sync + 'static)) -> Value {
    if let Some(a) = e.downcast_ref::<Assertion>() {
        json!({"sqlstate":null,"exception_class":"ProbeAssertion","reason":a.0})
    } else if let Some(p) = e.downcast_ref::<tokio_postgres::Error>() {
        json!({"sqlstate":p.code().map(|s|s.code()),"exception_class":"tokio_postgres::Error"})
    } else {
        json!({"sqlstate":null,"exception_class":"ProbeRuntimeError"})
    }
}
fn observation(r: Result<Value>) -> Value {
    match r {
        Ok(v) => json!({"status":"OK","data":v}),
        Err(e) => {
            let mut v = error(e.as_ref());
            v["status"] = json!("ERR");
            v
        }
    }
}
macro_rules! obs {
    ($e:expr) => {
        observation($e.await)
    };
}
async fn connect() -> Result<(Client, ConnectionTask)> {
    let prefix = std::env::var("R0_PREFIX").unwrap_or("r0d_tokio".into());
    require(
        !prefix.is_empty()
            && prefix.as_bytes()[0].is_ascii_lowercase()
            && prefix
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'),
        "invalid_prefix",
    )?;
    let mut c = tokio_postgres::Config::new();
    c.host(std::env::var("PGHOST").unwrap_or("localhost".into()))
        .port(std::env::var("PGPORT").unwrap_or("5432".into()).parse()?)
        .user(std::env::var("PGUSER").unwrap_or("postgres".into()))
        .dbname(std::env::var("PGDATABASE").unwrap_or("postgres".into()))
        .password(std::env::var("PGPASSWORD").unwrap_or_default())
        .application_name(std::env::var("PGAPPNAME").unwrap_or_default())
        .connect_timeout(std::time::Duration::from_secs(10));
    let (client, connection) = c.connect(NoTls).await?;
    Ok((client, ConnectionTask(tokio::spawn(connection))))
}
fn cell(r: &Row, i: usize) -> Result<Value> {
    macro_rules! get {
        ($t:ty) => {
            r.try_get::<_, Option<$t>>(i)?
        };
    }
    Ok(match *r.columns()[i].type_() {
        Type::BOOL => json!(get!(bool)),
        Type::INT2 => json!(get!(i16)),
        Type::INT4 => json!(get!(i32)),
        Type::INT8 => json!(get!(i64).map(|v| v.to_string())),
        Type::FLOAT8 => json!(get!(f64)),
        Type::BYTEA => {
            json!(get!(Vec<u8>).map(|b| b.iter().map(|x| format!("{x:02x}")).collect::<String>()))
        }
        Type::NUMERIC => json!(get!(rust_decimal::Decimal).map(|v| v.to_string())),
        Type::TIMESTAMPTZ => json!(get!(chrono::DateTime<chrono::Utc>)
            .map(|v| v.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))),
        Type::UUID => json!(get!(uuid::Uuid).map(|v| v.to_string())),
        _ => json!(get!(String)),
    })
}
async fn query(c: &Client, sql: &str, p: &[&(dyn ToSql + Sync)]) -> Result<Value> {
    let s = c.prepare(sql).await?;
    query_stmt(c, &s, p).await
}
async fn query_stmt(
    c: &Client,
    s: &tokio_postgres::Statement,
    p: &[&(dyn ToSql + Sync)],
) -> Result<Value> {
    let rs = c.query(s, p).await?;
    let rows: Result<Vec<Vec<Value>>> = rs
        .iter()
        .map(|r| (0..r.len()).map(|i| cell(r, i)).collect())
        .collect();
    Ok(
        json!({"rows":rows?,"fields":s.columns().iter().map(|f|json!({"name":f.name(),"type":type_name(f.type_())})).collect::<Vec<_>>() }),
    )
}
async fn checked(
    c: &Client,
    sql: &str,
    p: &[&(dyn ToSql + Sync)],
    expected: Value,
) -> Result<Value> {
    let r = query(c, sql, p).await?;
    require(r["rows"] == expected, "rows_mismatch")?;
    Ok(r)
}
async fn typed_checked(
    c: &Client,
    sql: &str,
    types: &[Type],
    p: &[&(dyn ToSql + Sync)],
    expected: Value,
) -> Result<Value> {
    let s = c.prepare_typed(sql, types).await?;
    let r = query_stmt(c, &s, p).await?;
    require(r["rows"] == expected, "rows_mismatch")?;
    Ok(r)
}
async fn exec(c: &Client, sql: &str, p: &[&(dyn ToSql + Sync)]) -> Result<Value> {
    let n = c.execute(sql, p).await?;
    require(n == 1, "rowcount_mismatch")?;
    Ok(json!({"rowcount":n}))
}
async fn batch(c: &Client, sql: &str) -> Result<Value> {
    c.batch_execute(sql).await?;
    Ok(json!({"completed": true}))
}
async fn point(c: &Client, id: i32, expected: Value) -> Result<Value> {
    let table = table();
    let r = checked(
        c,
        &format!("SELECT id,name,score FROM {table} WHERE id=$1"),
        &[&id],
        expected,
    )
    .await?;
    require(
        r["fields"]
            == json!([{"name":"id","type":"int4"},{"name":"name","type":"text"},{"name":"score","type":"int8"}]),
        "point_fields_mismatch",
    )?;
    Ok(r)
}
async fn insert(c: &Client, id: i32) -> Result<Value> {
    let table = table();
    exec(
        c,
        &format!("INSERT INTO {table} (id,name) VALUES ($1,$2)"),
        &[&id, &format!("tx{id}")],
    )
    .await
}
async fn expected_error(c: &Client, sql: &str, state: &str) -> Result<Value> {
    match c.batch_execute(sql).await {
        Err(e) => {
            require(e.code().map(|s| s.code()) == Some(state), "wrong_sqlstate")?;
            Ok(json!({"sqlstate":state}))
        }
        Ok(_) => Err(Box::new(Assertion("expected_error_missing"))),
    }
}
async fn run(c: &Client, id: &str) -> Result<Value> {
    let table = table();
    Ok(match id {
        "connect" => {
            checked(
                c,
                "SELECT current_user,current_database()",
                &[],
                json!([[
                    std::env::var("PGUSER").unwrap_or("postgres".into()),
                    std::env::var("PGDATABASE").unwrap_or("postgres".into())
                ]]),
            )
            .await?
        }
        "keepalive" => {
            json!({"simple":obs!(checked(c,"SELECT 1",&[],json!([[1]]))),"parameter":obs!(checked(c,"SELECT $1::integer",&[&1i32],json!([[1]])))})
        }
        "ddl_create" => {
            batch(
                c,
                &format!("CREATE TABLE {table} (id integer PRIMARY KEY,name text,score bigint)"),
            )
            .await?
        }
        "prepared_insert" => {
            let mut o = json!({});
            for (id, name, score) in [(1, "a", Some(20i64)), (2, "b", None), (3, "c", Some(20))] {
                o[format!("row_{id}")] = obs!(exec(
                    c,
                    &format!("INSERT INTO {table} (id,name,score) VALUES ($1,$2,$3)"),
                    &[&id, &name, &score]
                ));
            }
            o
        }
        "prepared_point_select" => {
            json!({"row_1":obs!(point(c,1,json!([[1,"a","20"]]))),"row_2":obs!(point(c,2,json!([[2,"b",null]])))})
        }
        "prepared_repeat" => {
            let s = c
                .prepare(&format!("SELECT id,name,score FROM {table} WHERE id=$1"))
                .await?;
            let mut a = vec![];
            for _ in 0..7 {
                a.push(observation(
                    async {
                        let r = query_stmt(c, &s, &[&1i32]).await?;
                        require(r["rows"] == json!([[1, "a", "20"]]), "rows_mismatch")?;
                        Ok(r)
                    }
                    .await,
                ));
            }
            json!({"executions":a})
        }
        "select_ordered" => {
            checked(
                c,
                &format!("SELECT id,name,score FROM {table} ORDER BY id"),
                &[],
                json!([[1, "a", "20"], [2, "b", null], [3, "c", "20"]]),
            )
            .await?
        }
        "update_delete_params" => {
            let update = obs!(exec(
                c,
                &format!("UPDATE {table} SET score=score+$1 WHERE id=$2"),
                &[&5i64, &1i32]
            ));
            let delete = obs!(exec(
                c,
                &format!("DELETE FROM {table} WHERE id=$1"),
                &[&3i32]
            ));
            let point = observation(
                async {
                    let r = query(
                        c,
                        &format!("SELECT id,name,score FROM {table} WHERE id=$1"),
                        &[&1i32],
                    )
                    .await?;
                    require(
                        r["rows"].as_array().map(|a| a.len()) == Some(1)
                            && r["rows"][0][0] == 1
                            && r["rows"][0][1] == "a",
                        "updated_identity_mismatch",
                    )?;
                    if update["status"] == "OK" {
                        require(r["rows"][0][2] == "25", "updated_score_mismatch")?;
                    }
                    Ok(r)
                }
                .await,
            );
            json!({"update":update,"delete":delete,"point":point})
        }
        "tx_commit" | "tx_rollback" => {
            let commit = id == "tx_commit";
            let n = if commit { 10 } else { 11 };
            let mut o = json!({"begin":obs!(batch(c,"BEGIN")),"insert":obs!(insert(c,n))});
            o["finish"] = obs!(batch(c, if commit { "COMMIT" } else { "ROLLBACK" }));
            o["visibility"] = observation(
                async {
                    let (d, t) = connect().await?;
                    let r = point(
                        &d,
                        n,
                        if commit {
                            json!([[10, "tx10", null]])
                        } else {
                            json!([])
                        },
                    )
                    .await;
                    drop(d);
                    drop(t);
                    r
                }
                .await,
            );
            o
        }
        "tx_error_recovery" => {
            json!({"begin":obs!(batch(c,"BEGIN")),"duplicate":obs!(expected_error(c,&format!("INSERT INTO {table} (id) VALUES (1)"),"23505")),"aborted":obs!(expected_error(c,"SELECT 1","25P02")),"rollback":obs!(batch(c,"ROLLBACK")),"recovered":obs!(checked(c,"SELECT 1",&[],json!([[1]])))})
        }
        "savepoint" => {
            let mut o = json!({});
            for (k, s) in [
                ("begin", "BEGIN"),
                (
                    "insert20",
                    "INSERT INTO r0d_tokio_items (id,name) VALUES (20,'tx20')",
                ),
                ("savepoint", "SAVEPOINT r0_sp"),
                (
                    "insert21",
                    "INSERT INTO r0d_tokio_items (id,name) VALUES (21,'tx21')",
                ),
                ("rollback_to", "ROLLBACK TO SAVEPOINT r0_sp"),
                ("release", "RELEASE SAVEPOINT r0_sp"),
                ("commit", "COMMIT"),
            ] {
                o[k] = obs!(batch(c, &s.replace("r0d_tokio_items", &table)));
            }
            o["point20"] = obs!(point(c, 20, json!([[20, "tx20", null]])));
            o["point21"] = obs!(point(c, 21, json!([])));
            o
        }
        "types_roundtrip" => types(c).await?,
        "param_bytea_position" => {
            let a = b"bc".to_vec();
            let b = b"abcd".to_vec();
            json!({"cast":obs!(typed_checked(c,"SELECT position($1::bytea IN $2::bytea)",&[Type::BYTEA,Type::BYTEA],&[&a,&b],json!([[2]]))),"uncast":obs!(typed_checked(c,"SELECT position($1 IN $2)",&[Type::BYTEA,Type::BYTEA],&[&a,&b],json!([[2]])))})
        }
        "param_no_from_aggregate" => {
            json!({"count":obs!(checked(c,"SELECT count(*)",&[],json!([["1"]]))),"max":obs!(typed_checked(c,"SELECT max($1) AS value",&[Type::INT4],&[&7i32],json!([[7]])))})
        }
        "simple_query" => {
            let messages = c.simple_query("SELECT 1 AS a; SELECT 2 AS b").await?;
            let mut out = vec![];
            let mut values = vec![];
            for m in messages {
                match m {tokio_postgres::SimpleQueryMessage::Row(r)=>{let v=r.get(0).map(str::to_owned);values.push(v.clone());out.push(json!({"kind":"row","value":v}));},tokio_postgres::SimpleQueryMessage::CommandComplete(n)=>out.push(json!({"kind":"command_complete","rows":n})),tokio_postgres::SimpleQueryMessage::RowDescription(cols)=>out.push(json!({"kind":"row_description","columns":cols.iter().map(|x|x.name()).collect::<Vec<_>>()})),_=>return Err(Box::new(Assertion("unexpected_message_kind")))}
            }
            require(
                values == vec![Some("1".into()), Some("2".into())],
                "rows_mismatch",
            )?;
            json!({"messages":out})
        }
        "statement_types" => {
            let s = c
                .prepare(&format!("SELECT id,name,score FROM {table} WHERE id=$1"))
                .await?;
            let p = s.params().iter().map(|t| t.name()).collect::<Vec<_>>();
            let cols = s
                .columns()
                .iter()
                .map(|t| t.type_().name())
                .collect::<Vec<_>>();
            require(
                p == ["int4"] && cols == ["int4", "text", "int8"],
                "statement_types_mismatch",
            )?;
            json!({"params":p,"columns":cols,"point":query_stmt(c,&s,&[&2i32]).await?})
        }
        "typed_prepare" => {
            let s = c
                .prepare_typed(
                    &format!("SELECT id,name,score FROM {table} WHERE id=$1"),
                    &[Type::INT8],
                )
                .await?;
            let r = query_stmt(c, &s, &[&2i64]).await?;
            require(
                s.params() == [Type::INT8] && r["rows"] == json!([[2, "b", null]]),
                "typed_prepare_mismatch",
            )?;
            json!({"params":["int8"],"point":r})
        }
        _ => return Err(Box::new(Assertion("unknown_case"))),
    })
}
async fn types(c: &Client) -> Result<Value> {
    let values: Vec<(&str, &str, Box<dyn ToSql + Sync + Send>, Value)> = vec![
        ("boolean", "boolean", Box::new(true), json!(true)),
        (
            "bytea",
            "bytea",
            Box::new(vec![0u8, 1, 255]),
            json!("0001ff"),
        ),
        (
            "numeric",
            "numeric(10,2)",
            Box::new(rust_decimal::Decimal::new(1230, 2)),
            json!("12.30"),
        ),
        ("smallint", "smallint", Box::new(123i16), json!(123)),
        ("double", "double precision", Box::new(1.25f64), json!(1.25)),
        (
            "varchar",
            "varchar(20)",
            Box::new("hello".to_owned()),
            json!("hello"),
        ),
        (
            "timestamptz",
            "timestamptz",
            Box::new("2020-01-02T03:04:05Z".parse::<chrono::DateTime<chrono::Utc>>()?),
            json!("2020-01-02T03:04:05Z"),
        ),
        (
            "uuid",
            "uuid",
            Box::new(uuid::Uuid::parse_str(
                "12345678-1234-5678-1234-567812345678",
            )?),
            json!("12345678-1234-5678-1234-567812345678"),
        ),
    ];
    let mut out = json!({});
    for (name, decl, value, expected) in values {
        let table = format!("{}_t_{name}", table().trim_end_matches("_items"));
        let mut steps = json!({"create":obs!(batch(c,&format!("CREATE TABLE {table} (id integer PRIMARY KEY,v {decl})")))});
        steps["insert_1"] = obs!(exec(
            c,
            &format!("INSERT INTO {table} (id,v) VALUES ($1,$2)"),
            &[&1i32, value.as_ref()]
        ));
        steps["insert_2"] = obs!(exec(
            c,
            &format!("INSERT INTO {table} (id,v) VALUES ($1,$2)"),
            &[&2i32, &Null]
        ));
        for id in [1i32, 2] {
            steps[format!("point_{id}")] = observation(
                async {
                    let mut r = checked(
                        c,
                        &format!("SELECT v FROM {table} WHERE id=$1"),
                        &[&id],
                        json!([[if id == 1 {
                            expected.clone()
                        } else {
                            Value::Null
                        }]]),
                    )
                    .await?;
                    r["was_null"] = json!(r["rows"][0][0].is_null());
                    Ok(r)
                }
                .await,
            );
        }
        let ok = steps
            .as_object()
            .unwrap()
            .values()
            .all(|v| v["status"] == "OK");
        out[name] = json!({"status":if ok {"OK"}else{"ERR"},"steps":steps});
    }
    Ok(out)
}
#[tokio::main]
async fn main() {
    println!("VERSION\ttokio-postgres\t0.7.15");
    let ids = [
        "connect",
        "keepalive",
        "ddl_create",
        "prepared_insert",
        "prepared_point_select",
        "prepared_repeat",
        "select_ordered",
        "update_delete_params",
        "tx_commit",
        "tx_rollback",
        "tx_error_recovery",
        "savepoint",
        "types_roundtrip",
        "param_bytea_position",
        "param_no_from_aggregate",
        "simple_query",
        "statement_types",
        "typed_prepare",
    ];
    for id in ids {
        let r = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let (c, t) = connect().await?;
            let r = run(&c, id).await;
            drop(c);
            drop(t);
            r
        })
        .await;
        let r = match r {
            Ok(r) => r,
            Err(_) => {
                Err(Box::new(Assertion("case_timeout")) as Box<dyn std::error::Error + Send + Sync>)
            }
        };
        let (status, data) = match r {
            Ok(v) => ("OK", v),
            Err(e) => ("ERR", error(e.as_ref())),
        };
        println!(
            "CASE\t{id}\t{status}\t{}",
            STANDARD.encode(serde_json::to_vec(&data).expect("JSON value"))
        );
    }
    println!("DONE\t{}", ids.len());
}
