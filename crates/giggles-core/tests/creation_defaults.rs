#[cfg(not(feature = "engine-rocksdb"))]
use giggles_core::CoreError;
use giggles_core::{Admin, Limits};
use serde_json::{Value, json};
use std::{path::Path, process::Command};

fn cli(args: &[&str], path: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_giggles"))
        .args(args)
        .arg(path)
        .output()
        .unwrap()
}

#[test]
fn creation_defaults_and_explicit_engines() {
    let root = std::env::temp_dir().join(format!("creation-defaults-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let incomplete = Command::new(env!("CARGO_BIN_EXE_giggles"))
        .args(["create", "--engine"])
        .current_dir(&root)
        .output()
        .unwrap();
    assert!(!incomplete.status.success(), "{incomplete:?}");
    assert!(!root.join("--engine").exists());
    for (name, args, v2) in [
        ("default", vec!["create"], cfg!(feature = "engine-rocksdb")),
        ("legacy", vec!["create", "--engine", "legacy"], false),
        ("v2", vec!["create", "--engine", "v2"], true),
    ] {
        let path = root.join(name);
        let output = cli(&args, &path);
        let reply: Value = serde_json::from_slice(&output.stdout).unwrap();
        if v2 && !cfg!(feature = "engine-rocksdb") {
            assert!(!output.status.success(), "{output:?}");
            assert!(
                String::from_utf8_lossy(&output.stdout).contains("engine-rocksdb not compiled in")
            );
            assert!(!path.exists());
            continue;
        }
        assert!(output.status.success(), "{output:?}");
        assert_eq!(reply["code"], 0);
        let admin = Admin::open(&path, Limits::default()).unwrap();
        assert_eq!(admin.snapshot().is_err(), v2);
        assert_eq!(path.join("data.redb").exists(), !v2);
        assert_eq!(reply["data"]["engine"], if v2 { "v2" } else { "redb" });
        let (domain, last_time, next_approval, origin) = admin.authority_metadata().unwrap();
        let mut expected = json!({"version":1,"code":0,"data":{
            "domain":domain.to_string(),"last_time":last_time,"next_approval":next_approval,
            "origin":origin.map(|v|v.to_string()),"write_format":3,"durability":"immediate","engine":"redb"
        }});
        if v2 {
            expected["data"]
                .as_object_mut()
                .unwrap()
                .remove("write_format");
            expected["data"]["engine"] = json!("v2");
            expected["data"]["durability"] = json!("sync");
        }
        let metadata = giggles_core::local::admin(&admin, br#"{"command":"metadata"}"#);
        assert_eq!(
            serde_json::to_vec(&metadata).unwrap(),
            format!(r#"{{"version":1,"code":0,"data":{}}}"#, expected["data"]).as_bytes()
        );
        let export = root.join(format!("{name}.gdexp"));
        if v2 {
            admin.export(&export).unwrap();
            assert_eq!(&std::fs::read(&export).unwrap()[..8], b"GDEXP002");
        }
        drop(admin);
        if v2 {
            let restored = root.join(format!("{name}-restored"));
            let output = cli(&["restore", export.to_str().unwrap()], &restored);
            assert!(output.status.success(), "{output:?}");
            let reply: Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(reply["code"], 0);
            assert_eq!(reply["data"]["engine"], "v2");
            assert_eq!(reply["data"]["durability"], "sync");
            assert!(reply["data"].get("write_format").is_none());
            assert_eq!(reply["data"]["origin"], domain.to_string());
        }
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn cli_opens_existing_legacy_without_upgrade_or_content_change() {
    let path = std::env::temp_dir().join(format!("creation-legacy-{}", std::process::id()));
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let metadata = admin.authority_metadata().unwrap();
    let mut session = admin.sql_session();
    session
        .execute(
            "CREATE TABLE kept (id BIGINT PRIMARY KEY, body TEXT)",
            &[],
            None,
        )
        .unwrap();
    session
        .execute("INSERT INTO kept VALUES (1, 'legacy')", &[], None)
        .unwrap();
    let before = session.execute("SELECT * FROM kept", &[], None).unwrap();
    drop(session);
    drop(admin);
    let output = Command::new(env!("CARGO_BIN_EXE_giggles"))
        .arg("admin")
        .arg(&path)
        .arg(json!({"command":"sql", "sql":"SELECT * FROM kept", "params":[]}).to_string())
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let reply: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        reply["data"]["rows"][0]["values"],
        json!([{"Int":1},{"Text":"legacy"}])
    );
    let admin = Admin::open(&path, Limits::default()).unwrap();
    assert_eq!(admin.authority_metadata().unwrap(), metadata);
    assert_eq!(
        admin
            .sql_session()
            .execute("SELECT * FROM kept", &[], None)
            .unwrap(),
        before
    );
    assert!(admin.snapshot().is_ok());
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[cfg(not(feature = "engine-rocksdb"))]
#[test]
fn explicit_v2_keeps_engine_unsupported_error() {
    assert!(matches!(
        Admin::create_engine("unused-v2", Limits::default(), giggles_core::Engine::V2),
        Err(CoreError::EngineUnsupported {
            engine: "v2",
            feature: "engine-rocksdb not compiled in"
        })
    ));
}
