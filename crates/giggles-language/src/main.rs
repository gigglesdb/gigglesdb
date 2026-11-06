use giggles_core::{Admin, Limits};
use giggles_language::{
    Bindings, Engine, Error, LanguageVersion, MAX_INPUT, ParsedInput, parse_input_with_version,
};
fn run() -> std::result::Result<serde_json::Value, Error> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.iter().any(|s| s.len() > MAX_INPUT) {
        return Err(Error::Limit);
    }
    match args.as_slice() {
        [command] if command == "capabilities" => {
            serde_json::to_value(giggles_language::profile()).map_err(|_| Error::Type)
        }
        [command] if command == "capability-manifest" => {
            serde_json::to_value(giggles_language::capability_manifest()).map_err(|_| Error::Type)
        }
        [command, option, version] if command == "capabilities" && option == "--language-version" => {
            serde_json::to_value(giggles_language::profile_for(language_version(version)?))
                .map_err(|_| Error::Type)
        }
        [command, option, version] if command == "capability-manifest" && option == "--language-version" => {
            serde_json::to_value(giggles_language::capability_manifest_for(language_version(version)?))
                .map_err(|_| Error::Type)
        }
        [command, path] if command == "init" => {
            Admin::create(path, Limits::default()).map_err(|e| Error::Core(e.to_string()))?;
            Ok(serde_json::json!({"initialized":true}))
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
        [command, option, version, path, text] if command == "run" && option == "--language-version" => {
            execute(path, text, &Bindings::new(), language_version(version)?)
        }
        [command, option, version, path, text, bindings] if command == "run" && option == "--language-version" => {
            let version = language_version(version)?;
            let bindings = serde_json::from_str(bindings).map_err(|_| Error::Bind("bindings JSON".into()))?;
            execute(path, text, &bindings, version)
        }
        [command, option, rest @ ..] if command == "run" && option == "--language-version" => {
            if let Some(version) = rest.first() {
                language_version(version)?;
            }
            Err(Error::Bind("usage: run --language-version VERSION PATH NATIVE [BINDINGS_JSON]".into()))
        }
        [command, path, text] if command == "run" => execute(path, text, &Bindings::new(), LanguageVersion::Embedded01),
        [command, path, text, bindings] if command == "run" => {
            let b: Bindings =
                serde_json::from_str(bindings).map_err(|_| Error::Bind("bindings JSON".into()))?;
            execute(path, text, &b, LanguageVersion::Embedded01)
        }
        _ => Err(Error::Bind(
            "usage: capabilities [--language-version VERSION] | capability-manifest [--language-version VERSION] | init PATH [SQL_SCHEMA] | run [--language-version VERSION] PATH NATIVE [BINDINGS_JSON]".into(),
        )),
    }
}
fn language_version(version: &str) -> std::result::Result<LanguageVersion, Error> {
    match version {
        "0.1" => Ok(LanguageVersion::Embedded01),
        "0.2" => Ok(LanguageVersion::Embedded02),
        "0.3" => Ok(LanguageVersion::Embedded03),
        "0.4" => Ok(LanguageVersion::Embedded04),
        "0.5" => Ok(LanguageVersion::Embedded05),
        "0.6" => Ok(LanguageVersion::Embedded06),
        "0.7" => Ok(LanguageVersion::Embedded07),
        "0.8" => Ok(LanguageVersion::Embedded08),
        "0.9" => Ok(LanguageVersion::Embedded09),
        "0.10" => Ok(LanguageVersion::Embedded010),
        "0.11" => Ok(LanguageVersion::Embedded011),
        _ => Err(Error::Unsupported),
    }
}
fn execute(
    path: &str,
    text: &str,
    bindings: &Bindings,
    version: LanguageVersion,
) -> std::result::Result<serde_json::Value, Error> {
    let input = parse_input_with_version(text, version)?;
    // Retry identity must retain the caller's original domain/scope, and new
    // decisions require a certified clock. This CLI supplies neither context.
    if matches!(input, ParsedInput::Once(_) | ParsedInput::Outcome(_)) {
        return Err(Error::Unsupported);
    }
    let admin = Admin::open(path, Limits::default()).map_err(|e| Error::Core(e.to_string()))?;
    let engine = Engine::with_version(admin, version);
    match input {
        ParsedInput::Statement(statement) => {
            serde_json::to_value(engine.execute(&statement, bindings)?)
        }
        ParsedInput::Transaction(transaction) => {
            serde_json::to_value(engine.execute_transaction(&transaction, bindings)?)
        }
        ParsedInput::Catalog(statement) => {
            if !bindings.is_empty() {
                return Err(Error::Bind("catalog bindings".into()));
            }
            serde_json::to_value(engine.execute_catalog(&statement)?)
        }
        ParsedInput::Once(_) | ParsedInput::Outcome(_) => return Err(Error::Unsupported),
    }
    .map_err(|_| Error::Type)
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
