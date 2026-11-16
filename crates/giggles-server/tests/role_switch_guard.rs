//! R3-203 (GD-0061 section 7): no new role-acquisition path appears without a
//! task-only check. Scans non-test source of giggles-server and giggles-core.
use std::path::{Path, PathBuf};

const MESSAGE: &str = "new role-acquisition path: add a task-only check and update this allow-list";

/// Files allowed to call `memberships()` outside tests (privilege checks only;
/// none of them changes the session's role).
const MEMBERSHIPS_ALLOW: &[&str] = &[
    "giggles-core/src/storage/roles.rs",
    "giggles-core/src/storage/roles_engine.rs",
    "giggles-server/src/service.rs",
    "giggles-server/src/pg_catalog.rs",
];

fn files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            let name = path.file_name().unwrap().to_string_lossy();
            if !name.ends_with("_tests.rs") && name != "tests.rs" {
                out.push(path);
            }
        }
    }
}

/// Source with `#[cfg(test)]` items (modules or functions) cut out, brace-matched.
fn non_test(text: &str) -> String {
    let mut out = String::new();
    let mut rest = text;
    while let Some(at) = rest.find("#[cfg(test)]") {
        out.push_str(&rest[..at]);
        let after = &rest[at..];
        let Some(open) = after.find('{') else {
            rest = "";
            break;
        };
        // `#[cfg(test)] mod x;` has no body.
        if after[..open].contains(';') {
            rest = &after[after.find(';').unwrap() + 1..];
            continue;
        }
        let mut depth = 0usize;
        let mut end = after.len();
        for (i, c) in after[open..].char_indices() {
            match c {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        end = open + i + 1;
                        break;
                    }
                }
                _ => {}
            }
        }
        rest = &after[end..];
    }
    out.push_str(rest);
    out
}

#[test]
fn role_acquisition_paths_are_guarded() {
    let crates = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let mut paths = Vec::new();
    for krate in ["giggles-server", "giggles-core"] {
        files(&crates.join(krate).join("src"), &mut paths);
    }
    let mut session_literals = Vec::new();
    let mut problems = Vec::new();
    for path in paths {
        let rel = path
            .strip_prefix(crates)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        let text = non_test(&std::fs::read_to_string(&path).unwrap());
        for (n, line) in text.lines().enumerate() {
            let code = line.split("//").next().unwrap_or("");
            if code.contains("Ok(Session {")
                || code.trim_start().starts_with("Session {")
                || code.contains("= Session {")
            {
                session_literals.push(format!("{rel}:{}", n + 1));
            }
            if code.contains(".role =") && !code.contains(".role ==") {
                problems.push(format!("{rel}:{}: assignment to .role", n + 1));
            }
            let upper = code.to_ascii_uppercase();
            if (upper.contains("SET ROLE") || upper.contains("SESSION AUTHORIZATION"))
                && !code.contains("\"0A000\"")
            {
                problems.push(format!("{rel}:{}: role switch syntax", n + 1));
            }
            if code.contains("set_role(") {
                problems.push(format!("{rel}:{}: set_role", n + 1));
            }
            if code.contains("memberships(") && !MEMBERSHIPS_ALLOW.contains(&rel.as_str()) {
                problems.push(format!("{rel}:{}: memberships()", n + 1));
            }
        }
    }
    // role_session, session and agent_session (R3-204).
    let in_service = session_literals
        .iter()
        .all(|s| s.starts_with("giggles-server/src/service.rs:"));
    assert!(
        session_literals.len() == 3 && in_service,
        "{MESSAGE}: Session literals {session_literals:?}"
    );
    assert!(problems.is_empty(), "{MESSAGE}: {problems:?}");
}
