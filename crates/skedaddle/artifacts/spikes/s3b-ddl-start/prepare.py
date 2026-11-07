"""Reuse S3's actual protocol decoder. All writes are under Skedaddle; no deletion."""
import pathlib
R=pathlib.Path(__file__).resolve().parent
S3=R.parent/'s3-ddl'
(R/'src').mkdir(exist_ok=True)
base=(S3/'src/main.rs').read_text().split('async fn admin(')[0]
base=base.replace('    async fn send_query(&mut self, q: &str) -> Result<()> {','''    async fn send_query(&mut self, q: &str) -> Result<()> {
        let mut f=OpenOptions::new().create(true).append(true).open(std::path::Path::new(&env::var("S1_OUT")?).join("commands.jsonl"))?;
        writeln!(f,"{}",json!({"pid":self.pid,"sql":q}))?;
''')
(R/'src/main.rs').write_text(base+(R/'run.rs').read_text())
for name in ('Cargo.toml','Cargo.lock'):
    (R/name).write_text((S3/name).read_text().replace('s3-ddl','s3b-ddl-start'))
