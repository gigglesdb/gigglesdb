use giggles_pg_regex::api::{RegexError, RegexLocale, RegexpContext, textregexeq};

const COMPILE_ERROR: &str = "invalid regular expression: regular expression is too complex";
const EXEC_ERROR: &str = "regular expression failed: regular expression is too complex";

fn two_megabyte_stack(test: impl FnOnce() + Send + 'static) {
    std::thread::Builder::new()
        .stack_size(2 * 1024 * 1024)
        .spawn(test)
        .unwrap()
        .join()
        .unwrap();
}

fn assert_complexity(result: Result<bool, RegexError>, message: &str, case: &str) {
    let error = result.expect_err(case);
    assert_eq!(error.sqlstate, "2201B", "{case}");
    assert_eq!(error.message, message, "{case}");
}

fn boundary(
    name: &str,
    last: usize,
    pattern: impl Fn(usize) -> String,
    matched: bool,
    message: &str,
) {
    let ctx = RegexpContext::new(RegexLocale::CUtf8);
    assert_eq!(
        textregexeq("x", &pattern(last), &ctx),
        Ok(matched),
        "{name}"
    );
    assert_complexity(textregexeq("x", &pattern(last + 1), &ctx), message, name);
}

// Last accepted depths for top-level SELECT 'x' ~ pattern on the Debian
// x86-64 PostgreSQL 18.6 oracle with max_stack_depth = '2MB'.
#[test]
fn compile_boundaries() {
    two_megabyte_stack(|| {
        boundary("literal", 43_546, |n| "a".repeat(n), false, COMPILE_ERROR);
        boundary(
            "bracket",
            43_546,
            |n| "[ab]".repeat(n),
            false,
            COMPILE_ERROR,
        );
        boundary(
            "fixed repeat",
            170,
            |n| "x{255}".repeat(n),
            false,
            COMPILE_ERROR,
        );
        boundary(
            "parse group",
            8_165,
            |n| "(".repeat(n) + "x" + &")".repeat(n),
            true,
            COMPILE_ERROR,
        );
        boundary(
            "parse tail",
            13_063,
            |n| "(x)".repeat(n),
            false,
            COMPILE_ERROR,
        );
        boundary(
            "fragment deletion",
            43_544,
            |n| format!("(?:{}){{0}}", "a".repeat(n)),
            true,
            COMPILE_ERROR,
        );
        boundary(
            "fragment duplication",
            43_537,
            |n| format!("((?:{}){{2}}){{0}}", "a".repeat(n)),
            true,
            COMPILE_ERROR,
        );
        boundary(
            "doubled fragment",
            21_769,
            |n| format!("(?:{}){{2}}", "a".repeat(n)),
            false,
            COMPILE_ERROR,
        );
        boundary(
            "constraint removal",
            26_122,
            |n| format!(r"(?:(^{})\1){{0}}", "a".repeat(n)),
            true,
            COMPILE_ERROR,
        );
        boundary(
            "backreference graph",
            21_769,
            |n| format!(r"(^{})\1", "a".repeat(n)),
            false,
            COMPILE_ERROR,
        );
        boundary(
            "constraint search",
            26_128,
            |n| "^".repeat(n) + "x",
            true,
            COMPILE_ERROR,
        );
        boundary(
            "constraint cloning",
            10_049,
            |n| format!("(?:{})*x", "^".repeat(n)),
            true,
            COMPILE_ERROR,
        );
        boundary(
            "epsilon chain",
            21_771,
            |n| "(?:|)*".repeat(n) + "x",
            true,
            COMPILE_ERROR,
        );
    });
}

#[test]
fn lookaround_boundaries() {
    two_megabyte_stack(|| {
        boundary(
            "mixed lookaround execution",
            1_809,
            |n| "(?=".repeat(5_000) + &"(?<=".repeat(n) + "x" + &")".repeat(5_000 + n),
            true,
            EXEC_ERROR,
        );
        boundary(
            "lookahead execution",
            6_121,
            |n| "(?=".repeat(n) + "x" + &")".repeat(n),
            true,
            EXEC_ERROR,
        );
        boundary(
            "lookbehind parsing",
            8_165,
            |n| "(?<=".repeat(n) + "x" + &")".repeat(n),
            true,
            COMPILE_ERROR,
        );
    });
}
