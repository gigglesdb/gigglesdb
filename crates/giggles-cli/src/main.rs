use std::io::{self, IsTerminal, Write};
fn main() {
    let args: std::result::Result<Vec<String>, _> = std::env::args_os()
        .skip(1)
        .map(|arg| arg.into_string())
        .collect();
    let args = match args {
        Ok(args) => args,
        Err(_) => {
            eprintln!("arguments must be valid Unicode");
            std::process::exit(2);
        }
    };
    if args.as_slice() == ["--help"] {
        if let Err(error) = io::stdout().lock().write_all(giggles_cli::HELP.as_bytes()) {
            eprintln!("IO error: {error}");
            std::process::exit(4);
        }
        return;
    }
    let stdin = io::stdin();
    let mut stdout = io::stdout().lock();
    let mut stderr = io::stderr().lock();
    let result = giggles_cli::Options::parse(args).and_then(|options| {
        giggles_cli::run(
            options,
            &mut stdin.lock(),
            &mut stdout,
            &mut stderr,
            stdin.is_terminal(),
        )
    });
    if let Err(error) = result {
        let _ = writeln!(stderr, "{error}");
        std::process::exit(i32::from(error.status));
    }
}
