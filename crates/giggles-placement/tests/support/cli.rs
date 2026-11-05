use serde_json::Value;
use std::{
    io::{BufRead, BufReader, Write},
    path::Path,
    process::{Child, ChildStdin, Command, Stdio},
    sync::mpsc,
    time::Duration,
};
pub struct Cli {
    child: Child,
    input: Option<ChildStdin>,
    output: mpsc::Receiver<Value>,
}
impl Cli {
    pub fn start(config: &Path, logs: &Path) -> Self {
        let stderr = std::fs::File::create(logs.with_extension("stderr.log")).unwrap();
        let mut stdout = std::fs::File::create(logs.with_extension("stdout.jsonl")).unwrap();
        let mut child = Command::new(env!("CARGO_BIN_EXE_giggles-placement"))
            .arg(config)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(stderr)
            .spawn()
            .unwrap();
        let input = Some(child.stdin.take().unwrap());
        let reader = child.stdout.take().unwrap();
        let (tx, output) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(reader).lines() {
                let line = line.unwrap();
                writeln!(stdout, "{line}").unwrap();
                let v = serde_json::from_str(&line).unwrap();
                if tx.send(v).is_err() {
                    break;
                }
            }
        });
        let c = Self {
            child,
            input,
            output,
        };
        assert_eq!(
            c.output.recv_timeout(Duration::from_secs(30)).unwrap()["ready"],
            true
        );
        c
    }
    pub fn request(&mut self, v: Value) -> Value {
        let input = self.input.as_mut().unwrap();
        writeln!(input, "{v}").unwrap();
        input.flush().unwrap();
        self.output.recv_timeout(Duration::from_secs(60)).unwrap()
    }
}
impl Drop for Cli {
    fn drop(&mut self) {
        drop(self.input.take());
        let _ = self.child.wait();
    }
}
