use giggles_history::{Limits, Verdict, check, generate, minimize, parse_jsonl, write_jsonl};
use std::io::{self, BufReader, Write};

fn main() {
    let code = match run() {
        Ok(code) => code,
        Err(e) => {
            eprintln!("invalid input: {e}");
            64
        }
    };
    std::process::exit(code);
}
fn run() -> Result<i32, String> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() > 9 || args.iter().any(|a| a.len() > 4096) {
        return Err("argument bounds exceeded".into());
    }
    if args.first().is_some_and(|s| s == "--help") && args.len() == 1 {
        println!(
            "giggles-history check|replay|generate|minimize\n  check|replay FILE|- [--nodes N] [--memory BYTES]\n  generate SEED COUNT\n  minimize FILE|- [--nodes N] [--memory BYTES]\ncheck/replay exits: 0 PASS, 1 FAIL, 2 INCONCLUSIVE, 64 invalid input.\nminimize writes reduced JSONL; generate writes synthetic JSONL. No system qualification."
        );
        return Ok(0);
    }
    let command = args.first().ok_or("missing command; use --help")?;
    if command == "generate" {
        if args.len() != 3 {
            return Err("generate requires SEED COUNT".into());
        }
        let h = generate(
            args[1].parse().map_err(|_| "invalid seed")?,
            args[2].parse().map_err(|_| "invalid count")?,
        )?;
        write_jsonl(&h, io::stdout().lock())?;
        return Ok(0);
    }
    if !["check", "replay", "minimize"].contains(&command.as_str())
        || args.len() < 2
        || args.len() % 2 != 0
    {
        return Err("expected check|replay|minimize FILE|- [--nodes N] [--memory BYTES]".into());
    }
    let mut limits = Limits::default();
    let mut seen = std::collections::BTreeSet::new();
    for flag in args[2..].as_chunks::<2>().0 {
        if !seen.insert(&flag[0]) {
            return Err("duplicate limit flag".into());
        }
        match flag[0].as_str() {
            "--nodes" => limits.max_nodes = flag[1].parse().map_err(|_| "invalid node bound")?,
            "--memory" => {
                limits.max_memory_bytes = flag[1].parse().map_err(|_| "invalid memory bound")?
            }
            _ => return Err("unknown flag".into()),
        }
    }
    let h = if args[1] == "-" {
        parse_jsonl(io::stdin().lock())?
    } else {
        parse_jsonl(BufReader::new(
            std::fs::File::open(&args[1]).map_err(|e| e.to_string())?,
        ))?
    };
    if command == "minimize" {
        let (reduced, checks) = minimize(&h, limits, 128)?;
        write_jsonl(&reduced, io::stdout().lock())?;
        eprintln!(
            "reduction checks={checks}; logical deletion reduction only, no minimum-size proof"
        );
        return Ok(0);
    }
    let result = check(&h, limits)?;
    let mut stdout = io::stdout().lock();
    serde_json::to_writer(&mut stdout, &result).map_err(|e| e.to_string())?;
    writeln!(stdout).map_err(|e| e.to_string())?;
    Ok(match result.verdict {
        Verdict::Pass => 0,
        Verdict::Fail => 1,
        Verdict::Inconclusive => 2,
    })
}
