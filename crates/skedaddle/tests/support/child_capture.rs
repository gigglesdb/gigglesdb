//! Drain both pipes continuously into task-local diagnostic files.
use crate::test_root;

use std::{
    fs, io,
    path::PathBuf,
    process::{Child, Output},
    thread::JoinHandle,
};

pub struct CapturedChild(pub Option<Child>, Vec<JoinHandle<io::Result<u64>>>, PathBuf);
impl CapturedChild {
    pub fn new(mut child: Option<Child>) -> Self {
        let root = test_root::scratch_root().join(format!("child-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        let mut tasks = Vec::new();
        let child_ref = child.as_mut().unwrap();
        if let Some(mut pipe) = child_ref.stdout.take() {
            let mut file = fs::File::create(root.join("stdout")).unwrap();
            tasks.push(std::thread::spawn(move || io::copy(&mut pipe, &mut file)));
        }
        if let Some(mut pipe) = child_ref.stderr.take() {
            let mut file = fs::File::create(root.join("stderr")).unwrap();
            tasks.push(std::thread::spawn(move || io::copy(&mut pipe, &mut file)));
        }
        Self(child, tasks, root)
    }
    pub fn output(&mut self) -> Output {
        let status = self.0.as_mut().unwrap().wait().unwrap();
        self.0.take();
        for task in self.1.drain(..) {
            task.join().unwrap().unwrap();
        }
        let stdout = fs::read(self.2.join("stdout")).unwrap_or_default();
        let stderr = fs::read(self.2.join("stderr")).unwrap_or_default();
        let output = Output {
            status,
            stdout,
            stderr,
        };
        if !status.success() {
            eprintln!("child exit: {}", checked_diagnostic(&output));
        }
        output
    }
    pub fn assert_running(&mut self, context: &str) {
        if self.0.as_mut().unwrap().try_wait().unwrap().is_some() {
            let output = self.output();
            let diagnostic = checked_diagnostic(&output);
            panic!("{context}: {diagnostic}");
        }
    }
}
fn checked_diagnostic(output: &Output) -> String {
    let mut redactor = skedaddle::security::Redactor::default();
    if let Some(path) = std::env::var_os("SKEDADDLE_TEST_CONFIG") {
        let config: serde_json::Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        for value in config
            .as_object()
            .unwrap()
            .values()
            .filter_map(|v| v.as_str())
        {
            redactor.register_value(value);
        }
    }
    let mut streams = Vec::new();
    for bytes in [&output.stdout, &output.stderr] {
        let text = String::from_utf8_lossy(bytes);
        // Existing sentinel and secret checks happen before any printing. On
        // leakage, suppress the stream (also safe during unwinding in Drop).
        if [
            "D6_PRIVATE_SENTINEL_f4792cb08a954cfa",
            "runner_private_password",
            "runner_private_user",
            "private-missing-ca.pem",
            "verify_private_user",
            "verify_private_password",
        ]
        .iter()
        .any(|secret| text.contains(secret))
        {
            streams.push("[SUPPRESSED: secret/sentinel check failed]".to_owned());
        } else {
            streams.push(redactor.redact(&text));
        }
    }
    format!(
        "status={:?}; code={:?}; stdout:\n{}\nstderr:\n{}",
        output.status,
        output.status.code(),
        streams[0],
        streams[1]
    )
}
impl Drop for CapturedChild {
    fn drop(&mut self) {
        if let Some(child) = self.0.as_mut() {
            let _ = child.kill();
            let output = self.output();
            eprintln!("owner cleanup: {}", checked_diagnostic(&output));
        }
    }
}
