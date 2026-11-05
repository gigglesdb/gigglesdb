use std::{path::PathBuf, process::Command};
fn fixture() -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "giggles-client-cli-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}
#[test]
fn usage_does_not_open_transport() {
    let output = Command::new(env!("CARGO_BIN_EXE_giggles-client"))
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("usage:"));
}
#[test]
fn protected_operator_input_and_oversize_files_rejected_without_echo() {
    let p = fixture();
    let cfg = p.join("config.json");
    let req = p.join("request.json");
    std::fs::write(&cfg,r#"{"endpoints":["127.0.0.1:1"],"ca":"missing","certificate":"missing","key":"missing","server_name":"localhost"}"#).unwrap();
    for payload in [
        br#"{"op":"reload_credentials","secret":"do-not-echo"}"#.to_vec(),
        vec![b'x'; 65537],
        br#"{"version":1,"id":1,"deadline_ms":5000,"action":{"Admin":"do-not-echo"}}"#.to_vec(),
    ] {
        std::fs::write(&req, payload).unwrap();
        let output = Command::new(env!("CARGO_BIN_EXE_giggles-client"))
            .arg(&cfg)
            .arg(&req)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        assert!(!String::from_utf8_lossy(&output.stderr).contains("do-not-echo"));
    }
    std::fs::write(&cfg, vec![b'x'; 65537]).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_giggles-client"))
        .arg(cfg)
        .arg(req)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
}
