#[path = "support/mod.rs"]
mod support;

use giggles_pg_regex::engine::{Regex, RegexLocale};
use support::{Expected, aligned_field, cases, info_row, match_rows};

const COMPILE_ONLY: bool = false;
const CORPUS: &[(&str, &str)] = &[
    (
        "test_regex.out",
        include_str!("../upstream/test_regex/test_regex.out"),
    ),
    (
        "test_regex_utf8.out",
        include_str!("../upstream/test_regex/test_regex_utf8.out"),
    ),
];
// Output file, SQL statement line, reason.
const SKIPS: &[(&str, usize, &str)] = &[];

#[test]
fn postgres_18_6() {
    let mut compared = 0;
    let mut passed = 0;
    let mut skipped_exec = 0;
    let mut other_skips = Vec::new();
    let mut failures = Vec::new();
    for &(file, output) in CORPUS {
        let output = output.replace("\r\n", "\n");
        for case in cases(&output) {
            let line = case.line;
            if let Some(&(_, _, reason)) = SKIPS.iter().find(|&&(f, n, _)| f == file && n == line) {
                other_skips.push(format!("{file}:{line} ({reason})"));
                continue;
            }
            let compiled = Regex::compile(&case.pattern, case.flags.cflags, RegexLocale::CUtf8);
            let result = match (compiled, case.expected) {
                (Err(error), Expected::CompileError(expected)) => {
                    let actual = format!("invalid regular expression: {}", error.message());
                    (actual == expected)
                        .then_some(())
                        .ok_or_else(|| format!("expected {expected:?}, got {actual:?}"))
                }
                (Ok(regex), Expected::Rows { width, rows: expected }) => {
                    let row = info_row(&regex, &case.flags);
                    if COMPILE_ONLY {
                        skipped_exec += 1;
                        (row == expected[0])
                            .then_some(())
                            .ok_or_else(|| format!("expected {:?}, got {row:?}", expected[0]))
                    } else {
                        match_rows(&regex, &case.input, &case.flags)
                            .map_err(|e| e.message().to_owned())
                            .and_then(|rows| {
                                let actual: Vec<_> = std::iter::once(row)
                                    .chain(rows.into_iter().map(|row| aligned_field(&row, width)))
                                    .collect();
                                (actual == expected)
                                    .then_some(())
                                    .ok_or_else(|| format!("expected {expected:?}, got {actual:?}"))
                            })
                    }
                }
                (Ok(regex), Expected::ExecError(expected)) => {
                    if COMPILE_ONLY {
                        skipped_exec += 1;
                        continue;
                    }
                    let actual = match_rows(&regex, &case.input, &case.flags);
                    match actual {
                        Err(error)
                            if expected
                                == format!("regular expression failed: {}", error.message()) =>
                        {
                            Ok(())
                        }
                        _ => Err(format!(
                            "expected execution error {expected:?}, got {actual:?}"
                        )),
                    }
                }
                (Ok(_), Expected::CompileError(expected)) => {
                    Err(format!("expected {expected:?}, compiled successfully"))
                }
                (Err(error), Expected::Rows { .. } | Expected::ExecError(_)) => {
                    Err(format!("unexpected compile error: {}", error.message()))
                }
            };
            compared += 1;
            match result {
                Ok(()) => passed += 1,
                Err(failure) => failures.push(format!("{file}:{line} {failure}")),
            }
        }
    }
    println!(
        "compared {compared}, passed {passed}, skipped execution checks {skipped_exec}, other skips {}",
        other_skips.len()
    );
    for skip in other_skips {
        println!("skipped {skip}");
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
