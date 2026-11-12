use std::process::ExitCode;

fn main() -> ExitCode {
    ExitCode::from(skedaddle::cli::main_entry())
}
