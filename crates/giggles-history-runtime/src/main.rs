fn main() {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() != 3 {
        eprintln!("usage: giggles-history-runtime NODE_BINARY ARTIFACT_PARENT SEED");
        std::process::exit(64);
    }
    let seed = args[2].to_str().and_then(|s| s.parse().ok());
    let Some(seed) = seed else {
        std::process::exit(64)
    };
    if !std::path::Path::new(&args[0]).is_file() {
        eprintln!("node executable does not exist");
        std::process::exit(64);
    }
    match giggles_history_runtime::run(args[0].as_ref(), args[1].as_ref(), seed) {
        Ok(result) => {
            println!("{result}");
            let code = match result["verdict"].as_str() {
                Some("PASS") => 0,
                Some("FAIL") => 1,
                _ => 2,
            };
            std::process::exit(code);
        }
        Err(error) => {
            eprintln!(
                "{}",
                serde_json::json!({"verdict":"INCONCLUSIVE","error":error.to_string()})
            );
            std::process::exit(2);
        }
    }
}
