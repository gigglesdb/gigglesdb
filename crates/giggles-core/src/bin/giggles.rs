use giggles_core::*;
use std::collections::BTreeSet;
fn run() -> local::Reply {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let input = |s: &str| -> Result<Vec<u8>, CoreError> {
        use std::io::Read;
        if let Some(path) = s.strip_prefix('@') {
            let mut b = Vec::new();
            std::fs::File::open(path)
                .map_err(|e| CoreError::Storage(e.to_string()))?
                .take((local::MAX_INPUT + 1) as u64)
                .read_to_end(&mut b)
                .map_err(|e| CoreError::Storage(e.to_string()))?;
            if b.len() > local::MAX_INPUT {
                return Err(CoreError::Limit);
            }
            Ok(b)
        } else {
            Ok(s.as_bytes().to_vec())
        }
    };
    let work = || -> Result<local::Reply, CoreError> {
        let a = args.iter().map(String::as_str).collect::<Vec<_>>();
        match a.as_slice() {
            ["demo", path] => {
                let db = Admin::create(path, Limits::default())?;
                for i in 0..3 {
                    db.put(
                        Key {
                            tenant: TenantId(42),
                            row: RowId(i),
                        },
                        Row {
                            status: Status::Failed,
                            failed_at: 103,
                            attempt_count: 0,
                            version: 0,
                        },
                    )?;
                }
                let e = db.grant(Grant {
                    task: TaskId(1),
                    issuer: Principal(1),
                    executor: Principal(2),
                    approver: Principal(3),
                    policy: Policy {
                        tenant: TenantId(42),
                        columns: BTreeSet::from([Column::Status]),
                        transitions: BTreeSet::from([(Status::Failed, Status::Queued)]),
                        incident: (100, 107),
                        distinct_limit: 2,
                        transition_limit: 2,
                        approval_above: 5,
                        valid_from: 1,
                        expires: 1000,
                    },
                })?;
                let permitted=local::executor(&e,br#"{"command":"sql","operation":1,"sql":"UPDATE deliveries SET status = 'queued' WHERE tenant = 42 AND id = 0","time":{"Certified":10},"approval":null}"#);
                let forbidden=local::executor(&e,br#"{"command":"sql","operation":2,"sql":"UPDATE deliveries SET status = 'queued' WHERE tenant = 43 AND id = 0","time":{"Certified":11},"approval":null}"#);
                Ok(local::Reply::ok(
                    serde_json::json!({"permitted":permitted,"forbidden":forbidden}),
                ))
            }
            ["create", path] => {
                let db = Admin::create(path, Limits::default())?;
                Ok(local::admin(&db, br#"{"command":"metadata"}"#))
            }
            ["restore", file, path] => {
                let db = Admin::restore(file, path, Limits::default())?;
                Ok(local::admin(&db, br#"{"command":"metadata"}"#))
            }
            ["verify", file] => {
                let s = Admin::verify_export(file)?;
                Ok(local::Reply::ok(
                    serde_json::json!({"domain":s.domain.to_string(),"rows":s.rows.len()}),
                ))
            }
            ["upgrade", "--recover", path] => {
                let outcome = Admin::recover_upgrade(path)?;
                Ok(local::Reply::ok(
                    serde_json::json!({"database": path, "recovery": outcome}),
                ))
            }
            ["upgrade", path] => {
                let report = Admin::upgrade_to_v2(path, Limits::default())?;
                Ok(local::Reply::ok(
                    serde_json::json!({"database": path, "export": report.export, "legacy": report.legacy}),
                ))
            }
            [
                cmd @ ("admin" | "seed" | "grant" | "approve" | "revoke" | "revoke_approval"),
                path,
                json,
            ] => {
                let db = Admin::open(path, Limits::default())?;
                let data = input(json)?;
                if *cmd != "admin" {
                    let v: serde_json::Value =
                        serde_json::from_slice(&data).map_err(|_| CoreError::Unsupported)?;
                    if v["command"] != *cmd {
                        return Err(CoreError::Unsupported);
                    }
                }
                Ok(local::admin(&db, &data))
            }
            [
                cmd @ ("executor" | "preview" | "execute"),
                path,
                task,
                principal,
                json,
            ] => {
                let db = Admin::open(path, Limits::default())?;
                let task = task.parse().map_err(|_| CoreError::Unsupported)?;
                let principal = principal.parse().map_err(|_| CoreError::Unsupported)?;
                let data = input(json)?;
                if *cmd != "executor" {
                    let v: serde_json::Value =
                        serde_json::from_slice(&data).map_err(|_| CoreError::Unsupported)?;
                    if v["command"] != *cmd {
                        return Err(CoreError::Unsupported);
                    }
                }
                Ok(local::executor(
                    &db.executor(TaskId(task), Principal(principal))?,
                    &data,
                ))
            }
            ["repair", path, task, principal, op, time, sql] => {
                let db = Admin::open(path, Limits::default())?;
                let num = |v: &str| v.parse::<u64>().map_err(|_| CoreError::Unsupported);
                let e = db.executor(TaskId(num(task)?), Principal(num(principal)?))?;
                Ok(local::executor(
                    &e,
                    &serde_json::to_vec(&local::ExecutorCommand::Sql {
                        operation: OperationId(num(op)?),
                        sql: sql.to_string(),
                        time: Time::Certified(num(time)?),
                        approval: None,
                    })
                    .expect("serialize"),
                ))
            }
            ["status", path, task, principal, op] => {
                let db = Admin::open(path, Limits::default())?;
                let num = |v: &str| v.parse::<u64>().map_err(|_| CoreError::Unsupported);
                let e = db.executor(TaskId(num(task)?), Principal(num(principal)?))?;
                Ok(local::executor(
                    &e,
                    &serde_json::to_vec(&local::ExecutorCommand::Status {
                        operation: OperationId(num(op)?),
                    })
                    .expect("serialize"),
                ))
            }
            ["inspect", path] => {
                let db = Admin::open(path, Limits::default())?;
                Ok(local::admin(&db, br#"{"command":"inspect"}"#))
            }
            ["export", path, file] => {
                let db = Admin::open(path, Limits::default())?;
                Ok(local::admin(
                    &db,
                    &serde_json::to_vec(&local::AdminCommand::Export {
                        path: file.to_string(),
                    })
                    .expect("serialize"),
                ))
            }
            _ => Err(CoreError::Unsupported),
        }
    };
    work().unwrap_or_else(local::failure)
}
fn main() {
    let reply = run();
    println!("{}", serde_json::to_string(&reply).expect("reply"));
    std::process::exit(if reply.code == 0 {
        0
    } else if (100..200).contains(&reply.code) {
        2
    } else {
        3
    });
}
