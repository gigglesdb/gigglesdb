use giggles_core::{Admin, Engine as StorageEngine, Limits};
use giggles_language::{
    Bindings, Engine, Error, InspectionOutcome, InspectionStatement, LanguageVersion, MAX_INPUT,
    ParsedInput, parse_input_with_version,
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
        [command, rest @ ..] if command == "init" => initialize(rest),
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
            "usage: capabilities [--language-version VERSION] | capability-manifest [--language-version VERSION] | init [--engine legacy|v2] PATH [SQL_SCHEMA] | run [--language-version VERSION] PATH NATIVE [BINDINGS_JSON]".into(),
        )),
    }
}
// Parse all init options before calling core, which may create the directory.
fn initialize(args: &[String]) -> Result<serde_json::Value, Error> {
    let (engine, rest) = match args {
        [option, engine, rest @ ..] if option == "--engine" => {
            let engine = match engine.as_str() {
                "legacy" => StorageEngine::Legacy,
                "v2" => StorageEngine::V2,
                _ => return Err(Error::Bind("unknown engine: expected legacy or v2".into())),
            };
            (engine, rest)
        }
        _ => (StorageEngine::Legacy, args),
    };
    let (path, schema) = match rest {
        [path] if !path.starts_with("--") => (path, None),
        [path, schema] if !path.starts_with("--") => (path, Some(schema)),
        _ => {
            return Err(Error::Bind(
                "usage: init [--engine legacy|v2] PATH [SQL_SCHEMA]".into(),
            ));
        }
    };
    let admin = Admin::create_engine(path, Limits::default(), engine)
        .map_err(|e| Error::Core(e.to_string()))?;
    if let Some(schema) = schema {
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
    }
    Ok(serde_json::json!({"initialized":true}))
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
        "0.12" => Ok(LanguageVersion::Embedded012),
        "0.13" => Ok(LanguageVersion::Embedded013),
        "0.14" => Ok(LanguageVersion::Embedded014),
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
    if let ParsedInput::Inspection(statement) = &input {
        if !bindings.is_empty() {
            return Err(Error::Bind("inspection bindings".into()));
        }
        // Discovery describes the selected library profile, not a database.
        let local = match statement {
            InspectionStatement::ShowCapabilities => Some(InspectionOutcome::Capabilities(
                giggles_language::capability_manifest_for(version),
            )),
            InspectionStatement::ShowLimits => Some(InspectionOutcome::Limits(
                giggles_language::capability_manifest_for(version).limits,
            )),
            InspectionStatement::ShowTables | InspectionStatement::DescribeTable { .. } => None,
        };
        if let Some(outcome) = local {
            return serde_json::to_value(outcome).map_err(|_| Error::Type);
        }
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
        ParsedInput::Inspection(statement) => serde_json::to_value(engine.inspect(&statement)?),
        ParsedInput::Explain(statement) => {
            return explain_json(&engine.explain(&statement, bindings)?, MAX_INPUT);
        }
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

// Measure the exact JSON encoding without copying output before allocating it.
// This transport bound is distinct from core's logical metadata admission.
fn explain_json(
    outcome: &giggles_language::ReadExplainOutcome,
    budget: usize,
) -> Result<serde_json::Value, Error> {
    struct Meter {
        bytes: usize,
        budget: usize,
    }
    impl std::io::Write for Meter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.bytes = self
                .bytes
                .checked_add(bytes.len())
                .filter(|n| *n <= self.budget)
                .ok_or_else(|| std::io::Error::other("explain JSON limit"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(&mut Meter { bytes: 0, budget }, outcome).map_err(|_| Error::Limit)?;
    serde_json::to_value(outcome).map_err(|_| Error::Type)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn explain_json_exact_bound() {
        let outcome: giggles_language::ReadExplainOutcome = serde_json::from_value(serde_json::json!({
            "contract_version":1,"database_domain":"340282366920938463463374607431768211455",
            "catalog_epoch":1,"table_id":1,"table_name":"a\"\n", "projection_column_ids":[1],
            "order":[],"has_filter":false,"stages":[],"estimated_rows":null,"estimated_cost":null,
            "limits":{"input_rows":1024,"output_rows":100,"row_bytes":1,"result_bytes":1,
                "request_bytes":1,"binding_count":1,"binding_bytes":1,"bound_value_bytes":1,
                "expression_depth":1,"expression_nodes":1,"admitted":1,"selected_keys":1}
        })).unwrap();
        let bytes = serde_json::to_vec(&outcome).unwrap();
        assert_eq!(
            explain_json(&outcome, bytes.len()).unwrap(),
            serde_json::to_value(&outcome).unwrap()
        );
        assert_eq!(explain_json(&outcome, bytes.len() - 1), Err(Error::Limit));
        assert_eq!(explain_json(&outcome, 0), Err(Error::Limit));
    }
}
