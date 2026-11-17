//! GD-0061 R3-314: the release seal of the server sources.
//!
//! Server code reaches core only through the unreleased exits, releases the
//! result with `Session::release_page`, and encodes only `Released<..>`. The
//! `clippy.toml` `disallowed-methods` list is the first net; this scan is the
//! second, in the style of `every_test_file_is_registered`: it reads
//! `src/**/*.rs` outside test modules and fails on a plain exit. The scan is
//! textual (comments and literals removed), so it sees calls written on a
//! `SqlSession` receiver (`.sql.`, `sql_session()`); the lint sees the rest.
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// Exits whose name is unique to a plain exit: banned on any receiver.
const PLAIN_NAMES: &[&str] = &[
    // core `SqlSession`
    "execute_at",
    "execute_prepared",
    "execute_prepared_at",
    "execute_typed_read",
    "execute_typed_join",
    "execute_native",
    "execute_native_transaction",
    // core `Cursor`
    "fetch",
    // the server `Session` plain wrappers (kept public for the tests)
    "execute_sql",
];

/// `SqlSession::execute` shares its name with other `execute` methods, so it
/// is banned on a `SqlSession` receiver.
const PLAIN_RECEIVER_CALLS: &[&str] = &[".sql.execute(", "sql_session().execute(", "SqlSession::execute("];

/// The paths `clippy.toml` must list.
const CLIPPY_PATHS: &[&str] = &[
    "giggles_core::SqlSession::execute\"",
    "giggles_core::SqlSession::execute_at\"",
    "giggles_core::SqlSession::execute_prepared\"",
    "giggles_core::SqlSession::execute_prepared_at\"",
    "giggles_core::SqlSession::execute_typed_read\"",
    "giggles_core::SqlSession::execute_typed_join\"",
    "giggles_core::SqlSession::execute_native\"",
    "giggles_core::SqlSession::execute_native_transaction\"",
    "giggles_core::Cursor::fetch\"",
    "giggles_server::service::Session::execute_sql\"",
    "giggles_server::service::Session::execute_prepared\"",
];

fn src() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("src")
}

/// `text` with comments blanked and the contents of string and character
/// literals blanked, byte for byte, so offsets and lines are kept.
fn code_only(text: &str) -> String {
    let b = text.as_bytes();
    let mut out = b.to_vec();
    let blank = |out: &mut Vec<u8>, from: usize, to: usize| {
        for c in &mut out[from..to] {
            if *c != b'\n' {
                *c = b' ';
            }
        }
    };
    let ident = |c: u8| c.is_ascii_alphanumeric() || c == b'_';
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'/' if b.get(i + 1) == Some(&b'/') => {
                let end = b[i..].iter().position(|&c| c == b'\n').map_or(b.len(), |n| i + n);
                blank(&mut out, i, end);
                i = end;
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                let (mut depth, mut j) = (1, i + 2);
                while j < b.len() && depth > 0 {
                    if b[j] == b'/' && b.get(j + 1) == Some(&b'*') {
                        depth += 1;
                        j += 2;
                    } else if b[j] == b'*' && b.get(j + 1) == Some(&b'/') {
                        depth -= 1;
                        j += 2;
                    } else {
                        j += 1;
                    }
                }
                blank(&mut out, i, j);
                i = j;
            }
            b'r' if (i == 0 || !ident(b[i - 1]) || (b[i - 1] == b'b' && (i < 2 || !ident(b[i - 2]))))
                && matches!(b.get(i + 1), Some(b'"' | b'#')) =>
            {
                let mut j = i + 1;
                let mut hashes = 0;
                while b.get(j) == Some(&b'#') {
                    hashes += 1;
                    j += 1;
                }
                if b.get(j) != Some(&b'"') {
                    i += 1;
                    continue;
                }
                let start = j + 1;
                let mut k = start;
                loop {
                    assert!(k < b.len(), "unterminated raw string");
                    if b[k] == b'"' && b[k + 1..].iter().take(hashes).filter(|&&c| c == b'#').count() == hashes {
                        break;
                    }
                    k += 1;
                }
                blank(&mut out, start, k);
                i = k + 1 + hashes;
            }
            b'"' => {
                let mut j = i + 1;
                while j < b.len() && b[j] != b'"' {
                    j += if b[j] == b'\\' { 2 } else { 1 };
                }
                blank(&mut out, i + 1, j);
                i = j + 1;
            }
            b'\'' => {
                // A character literal, or a lifetime / label.
                if b.get(i + 1) == Some(&b'\\') {
                    let end = b[i + 3..].iter().position(|&c| c == b'\'').map_or(b.len(), |n| i + 3 + n);
                    blank(&mut out, i + 1, end);
                    i = end + 1;
                } else {
                    let width = std::str::from_utf8(&b[i + 1..]).ok().and_then(|s| s.chars().next()).map_or(1, char::len_utf8);
                    if b.get(i + 1 + width) == Some(&b'\'') {
                        blank(&mut out, i + 1, i + 1 + width);
                        i += 2 + width;
                    } else {
                        i += 1;
                    }
                }
            }
            _ => i += 1,
        }
    }
    String::from_utf8(out).expect("blanking keeps UTF-8")
}

/// Whether an attribute group makes the item it annotates test-only.
fn test_only(attribute: &str) -> bool {
    let a: String = attribute.chars().filter(|c| !c.is_whitespace()).collect();
    a.starts_with("#[cfg(test)]") || a.starts_with("#[cfg(all(test,")
}

/// One scanned source file: its code outside test-only items, and the module
/// files it declares (path, test-only, mod-rs style).
struct Scan {
    code: String,
    children: Vec<(PathBuf, bool, bool)>,
}

fn scan(path: &Path, mod_rs: bool) -> Scan {
    let original = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let code = code_only(&original);
    let mut kept = code.clone().into_bytes();
    let dir = path.parent().unwrap();
    let stem = path.file_stem().unwrap().to_str().unwrap();
    let mut children = Vec::new();
    let b = code.as_bytes();
    let mut i = 0;
    while i < b.len() {
        // An attribute group: consecutive `#[...]` (not `#![...]`).
        if b[i] == b'#' && b.get(i + 1) == Some(&b'[') && (i == 0 || b[i - 1] != b'!') {
            let start = i;
            let mut test = false;
            let mut path_attr = None;
            loop {
                let mut depth = 0;
                let mut j = i + 1;
                while j < b.len() {
                    match b[j] {
                        b'[' => depth += 1,
                        b']' => {
                            depth -= 1;
                            if depth == 0 {
                                break;
                            }
                        }
                        _ => {}
                    }
                    j += 1;
                }
                let attr = &original[i..=j];
                test |= test_only(&code[i..=j]);
                let compact: String = attr.chars().filter(|c| !c.is_whitespace()).collect();
                if let Some(p) = compact.strip_prefix("#[path=\"").and_then(|p| p.strip_suffix("\"]")) {
                    path_attr = Some(p.to_owned());
                }
                i = j + 1;
                while i < b.len() && b[i].is_ascii_whitespace() {
                    i += 1;
                }
                if !(b[i] == b'#' && b.get(i + 1) == Some(&b'[')) {
                    break;
                }
            }
            // The item: up to its `;`, or its balanced `{ ... }`.
            let item = i;
            let mut j = item;
            while j < b.len() && b[j] != b';' && b[j] != b'{' {
                j += 1;
            }
            let end = if b[j] == b';' {
                j + 1
            } else {
                let mut depth = 0;
                loop {
                    match b[j] {
                        b'{' => depth += 1,
                        b'}' => {
                            depth -= 1;
                            if depth == 0 {
                                break j + 1;
                            }
                        }
                        _ => {}
                    }
                    j += 1;
                }
            };
            let words: Vec<&str> = code[item..end].split_whitespace().collect();
            if let Some(name) = module_declaration(&words) {
                let child = module_file(dir, stem, mod_rs, name, path_attr.as_deref());
                // A file loaded through `#[path]` resolves its own modules
                // as a mod-rs file does.
                let child_mod_rs = path_attr.is_some() || child.ends_with("mod.rs");
                children.push((child, test, child_mod_rs));
            }
            if test {
                for c in &mut kept[start..end] {
                    if *c != b'\n' {
                        *c = b' ';
                    }
                }
            }
            // Re-scan inside a non-test item (impl blocks, inline modules).
            i = if test { end } else { item };
            if !test {
                // Skip past the attribute group only.
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            continue;
        }
        // A `mod name;` with no attribute.
        if (i == 0 || !b[i - 1].is_ascii_alphanumeric() && b[i - 1] != b'_') && code[i..].starts_with("mod ") {
            let end = code[i..].find([';', '{']).map_or(b.len(), |n| i + n);
            if b.get(end) == Some(&b';') {
                let words: Vec<&str> = code[i..=end].split_whitespace().collect();
                if let Some(name) = module_declaration(&words) {
                    let child = module_file(dir, stem, mod_rs, name, None);
                    let child_mod_rs = child.ends_with("mod.rs");
                    children.push((child, false, child_mod_rs));
                }
            }
        }
        i += 1;
    }
    Scan { code: String::from_utf8(kept).unwrap(), children }
}

/// `mod name;` / `pub(crate) mod name;` → `name`.
fn module_declaration<'a>(words: &[&'a str]) -> Option<&'a str> {
    let at = words.iter().position(|w| *w == "mod")?;
    let name = words.get(at + 1)?;
    let name = name.strip_suffix(';')?;
    (at + 2 == words.len()).then_some(name)
}

fn module_file(dir: &Path, stem: &str, mod_rs: bool, name: &str, path_attr: Option<&str>) -> PathBuf {
    if let Some(p) = path_attr {
        return dir.join(p);
    }
    let base = if mod_rs { dir.to_path_buf() } else { dir.join(stem) };
    let flat = base.join(format!("{name}.rs"));
    if flat.exists() { flat } else { base.join(name).join("mod.rs") }
}

/// Every source file under `src/` with its code outside test-only modules and
/// items; a file reached only through a test-only module is left out.
fn production_sources() -> Vec<(PathBuf, String)> {
    let mut roots = vec![(src().join("lib.rs"), false)];
    for entry in std::fs::read_dir(src().join("bin")).expect("src/bin") {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|e| e == "rs") {
            roots.push((path, false));
        }
    }
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    let mut queue: Vec<(PathBuf, bool, bool)> = roots.into_iter().map(|(p, t)| (p, t, true)).collect();
    while let Some((path, test, mod_rs)) = queue.pop() {
        let path = std::fs::canonicalize(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        if !seen.insert(path.clone()) {
            continue;
        }
        let scanned = scan(&path, mod_rs);
        for (child, child_test, mod_rs) in scanned.children {
            queue.push((child, test || child_test, mod_rs));
        }
        if !test {
            out.push((path, scanned.code));
        }
    }
    // A file the module walk never reached is scanned whole.
    let mut stack = vec![src()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "rs") {
                let path = std::fs::canonicalize(&path).unwrap();
                if !seen.contains(&path) {
                    let text = std::fs::read_to_string(&path).unwrap();
                    out.push((path, code_only(&text)));
                }
            }
        }
    }
    out
}

fn relative(path: &Path) -> String {
    let src = std::fs::canonicalize(src()).unwrap();
    path.strip_prefix(&src).unwrap_or(path).display().to_string().replace('\\', "/")
}

/// The code of `text` with all whitespace removed, so a call split across
/// lines (`session\n    .sql\n    .execute(`) reads as one.
fn compact(text: &str) -> String {
    text.chars().filter(|c| !c.is_whitespace()).collect()
}

fn plain_exits(code: &str) -> Vec<String> {
    let compact = compact(code);
    let mut found = Vec::new();
    for name in PLAIN_NAMES {
        for prefix in [".", "::"] {
            let needle = format!("{prefix}{name}(");
            if compact.contains(&needle) {
                found.push(needle);
            }
        }
    }
    for needle in PLAIN_RECEIVER_CALLS {
        if compact.contains(needle) {
            found.push((*needle).to_owned());
        }
    }
    found
}

#[test]
fn no_plain_core_exit_in_server_sources() {
    let sources = production_sources();
    assert!(
        sources.iter().any(|(p, _)| relative(p) == "pgwire.rs"),
        "the module walk must reach the server sources"
    );
    let mut offenders = Vec::new();
    for (path, code) in &sources {
        for exit in plain_exits(code) {
            offenders.push(format!("{}: {exit}", relative(path)));
        }
    }
    assert!(
        offenders.is_empty(),
        "plain exits outside test modules (use the unreleased twin and release_page, GD-0061 R3-314): {offenders:#?}"
    );
    // The scan is a second net behind the lint: both list the same exits.
    let clippy = include_str!("../clippy.toml");
    for path in CLIPPY_PATHS {
        assert!(clippy.contains(path), "clippy.toml disallowed-methods must list {path}");
    }
}

/// The scanner itself: it finds plain exits in production code, including
/// split calls, and ignores comments, literals and test-only items.
#[test]
fn scan_sees_split_calls_and_skips_tests_comments_and_literals() {
    let code = code_only(
        "fn a(s: &mut S) { s.sql\n    .execute(\"x\", &[], None); }\n\
         // s.sql.execute_native(&r)\n\
         fn b() { let _ = \"cursor.fetch(1, 2)\"; let _ = 'x'; }\n",
    );
    assert_eq!(plain_exits(&code), vec![".sql.execute(".to_owned()]);
    assert!(plain_exits(&code_only("fn c(c: &mut Cursor) { c.fetch_unreleased(1, 2); }")).is_empty());
    assert_eq!(plain_exits(&code_only("fn d(c: &mut Cursor) { c\n.fetch(1, 2); }")), vec![".fetch(".to_owned()]);
    let dir = std::env::temp_dir().join(format!("release-seal-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join("lib.rs");
    std::fs::write(
        &file,
        "fn live(s: &mut S) { s.execute_sql(\"x\", &[]); }\n\
         #[cfg(test)]\nmod tests {\n    fn t(s: &mut S) { s.sql.execute(\"x\", &[], None); }\n}\n\
         impl S {\n    #[cfg(test)]\n    fn helper(&mut self) { self.execute_prepared(&p, &[]); }\n}\n\
         #[cfg(test)]\n#[path = \"other.rs\"]\nmod other;\n",
    )
    .unwrap();
    let scanned = scan(&file, true);
    std::fs::remove_dir_all(&dir).unwrap();
    assert_eq!(plain_exits(&scanned.code), vec![".execute_sql(".to_owned()]);
    assert_eq!(scanned.children, vec![(dir.join("other.rs"), true, true)]);
}

/// The encoders take only `Released<..>`, and `Response.sql` is built only
/// from a released result. The compile-time half is the `compile_fail`
/// doctest on `Response::released_sql` (a plain `SqlResult` does not
/// compile) and the encoder signatures, which this test pins.
#[test]
fn encoders_accept_only_released() {
    let file = |name: &str| code_only(&std::fs::read_to_string(src().join(name)).unwrap());
    let signature = |code: &str, head: &str| -> String {
        let at = code.find(head).unwrap_or_else(|| panic!("{head} not found"));
        let end = at + code[at..].find('{').unwrap();
        compact(&code[at..end])
    };
    let pgwire = file("pgwire.rs");
    assert!(signature(&pgwire, "fn rows(").contains("result:&Released<SqlResult>,"));
    assert!(signature(&pgwire, "fn complete(").contains("r:&Released<SqlResult>,"));
    let catalog = file("pg_catalog_wire.rs");
    assert!(signature(&catalog, "pub(crate) fn rows(").contains("result:&Released<ResultSet>,"));
    let native = file("api/native_codec.rs");
    assert!(signature(&native, "pub(crate) fn encode_outcome(").contains("(v:Released<o::Outcome>)"));
    assert!(signature(&native, "pub(crate) fn encode_transaction_outcome(").contains("v:Released<o::TransactionOutcome>,"));
    // `Response { sql: Some(..) }` is written once, in `released_sql`.
    let mut builders = Vec::new();
    for (path, code) in production_sources() {
        let compact = compact(&code);
        builders.extend(std::iter::repeat_n(relative(&path), compact.matches("sql:Some(").count()));
    }
    assert_eq!(builders, vec!["service.rs".to_owned()], "Response.sql must be built only by Response::released_sql");
    let service = file("service.rs");
    assert!(signature(&service, "pub fn released_sql(").contains("(result:Released<SqlResult>)"));
}
