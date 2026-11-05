use giggles_core::{Admin, Limits};
use giggles_language::{Bindings, Engine, Error, MAX_INPUT};
fn run() -> std::result::Result<serde_json::Value, Error> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.iter().any(|s| s.len() > MAX_INPUT) {
        return Err(Error::Limit);
    }
    match args.as_slice() {
        [command] if command == "capabilities" => {
            serde_json::to_value(giggles_language::profile()).map_err(|_| Error::Type)
        }
        [command, path, schema] if command == "init" => {
            let admin =
                Admin::create(path, Limits::default()).map_err(|e| Error::Core(e.to_string()))?;
            let mut session = admin.sql_session();
            let prepared = session
                .prepare(schema)
                .map_err(|e| Error::Core(e.to_string()))?;
            if prepared.command() != "CREATE TABLE" {
                return Err(Error::Unsupported);
            }
            session
                .execute_prepared(&prepared, &[], None)
                .map_err(|e| Error::Core(e.to_string()))?;
            Ok(serde_json::json!({"initialized":true}))
        }
        [command, path, text] if command == "run" => execute(path, text, &Bindings::new()),
        [command, path, text, bindings] if command == "run" => {
            let b: Bindings =
                serde_json::from_str(bindings).map_err(|_| Error::Bind("bindings JSON".into()))?;
            execute(path, text, &b)
        }
        _ => Err(Error::Bind(
            "usage: capabilities | init PATH SQL_SCHEMA | run PATH NATIVE [BINDINGS_JSON]".into(),
        )),
    }
}
fn execute(
    path: &str,
    text: &str,
    bindings: &Bindings,
) -> std::result::Result<serde_json::Value, Error> {
    let admin = Admin::open(path, Limits::default()).map_err(|e| Error::Core(e.to_string()))?;
    let out = Engine::new(admin).execute_text(text, bindings)?;
    serde_json::to_value(out).map_err(|_| Error::Type)
}
fn main() {
    match run() {
        Ok(value) => println!("{value}"),
        Err(error) => {
            eprintln!("{}", serde_json::json!({"error":error}));
            std::process::exit(1)
        }
    }
}
