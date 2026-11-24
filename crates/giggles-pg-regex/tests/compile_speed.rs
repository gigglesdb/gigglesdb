use std::time::{Duration, Instant};

use giggles_pg_regex::engine::{Chr, REG_ADVANCED, RegMatch, Regex, RegexLocale};

fn chars(text: &str) -> Vec<Chr> {
    text.chars().map(Chr::from).collect()
}

fn compile(pattern: &[Chr]) -> Regex {
    Regex::compile(pattern, REG_ADVANCED, RegexLocale::CUtf8).expect("pattern compiles")
}

fn check_time(case: &str, elapsed: Duration, limit: Duration) {
    eprintln!("{case}: {elapsed:?}");
    #[cfg(not(debug_assertions))]
    assert!(elapsed < limit, "{case} took {elapsed:?} (limit {limit:?})");
    #[cfg(debug_assertions)]
    let _ = limit;
}

#[test]
#[ignore = "release performance regression"]
fn many_bracket_expressions() {
    let pattern = chars(&"[ab]".repeat(40_000));
    let start = Instant::now();
    compile(&pattern);
    check_time(
        "repeated bracket expressions",
        start.elapsed(),
        Duration::from_millis(2150),
    );
}

#[test]
#[ignore = "release performance regression"]
fn nested_bounded_repetitions() {
    let pattern = chars("((a{1,25}){1,25}){1,25}");
    let start = Instant::now();
    compile(&pattern);
    check_time(
        "nested bounded repetitions",
        start.elapsed(),
        Duration::from_secs(1),
    );
}

#[test]
#[ignore = "release performance regression"]
fn backreference_dissection() {
    let pattern = chars(r"(.*)\1$");
    let subject = vec![Chr::from('a'); 2_001];
    let mut matches = [RegMatch::default(); 2];
    let start = Instant::now();
    let regex = compile(&pattern);
    assert!(regex.exec(&subject, 0, None, &mut matches, 0).unwrap());
    check_time(
        "backreference dissection",
        start.elapsed(),
        Duration::from_secs(1),
    );
    assert_eq!(
        matches,
        [RegMatch { so: 1, eo: 2_001 }, RegMatch { so: 1, eo: 1_001 }]
    );
}
