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
    // COPY server-file/program privilege check; read-only.
    "giggles-server/src/pgwire_copy_external.rs",
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
    let mut effective_acquisitions = 0;
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
            if code.contains(".effective_role =") || code.contains(".effective_session =") {
                effective_acquisitions += 1;
                assert_eq!(rel, "giggles-server/src/service.rs", "{MESSAGE}");
                assert!(matches!(code.trim(), "self.effective_session = session;" | "self.effective_role = current.map(|name| Role::Database { name });"), "{MESSAGE}");
                let start = text.find("pub(crate) fn sync_role_settings(").unwrap();
                let end = start + text[start..].find("pub(crate) fn zone(").unwrap();
                let guarded = &text[start..end];
                for check in ["!self.agent && self.sql.task().is_none()", "!role.task_only", "Some(role.oid) == settings.selected_role_oid", "Some(role.oid) == settings.session_authorization_oid", "roles.can_set(session_name, name)", "auth.superuser || name == authenticated"] {
                    assert!(guarded.contains(check), "{MESSAGE}: missing {check}");
                }
                assert!(guarded.lines().any(|line| line.trim() == code.trim()), "{MESSAGE}");
            }
            if code.contains(".role =") && !code.contains(".role ==") {
                problems.push(format!("{rel}:{}: assignment to .role", n + 1));
            }
            let upper = code.to_ascii_uppercase();
            if (upper.contains("SET ROLE") || upper.contains("SESSION AUTHORIZATION"))
                && !code.contains("\"0A000\"")
                && !(rel == "giggles-server/src/pgwire_simple.rs"
                    && (code.trim() == "else { format!(\"permission denied to set session authorization \\\"{value}\\\"\") });"
                        || code.trim() == "else if *name == crate::pg_settings::Show::Role { format!(\"permission denied to set role \\\"{value}\\\"\") }"))
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
    // role_session, session, agent_session (R3-204) and credential_session
    // (R3-1304: executor_role filter, task bound, role generation fence).
    let in_service = session_literals
        .iter()
        .all(|s| s.starts_with("giggles-server/src/service.rs:"));
    assert!(
        session_literals.len() == 4 && in_service,
        "{MESSAGE}: Session literals {session_literals:?}"
    );
    assert_eq!(effective_acquisitions, 2, "{MESSAGE}");
    assert!(problems.is_empty(), "{MESSAGE}: {problems:?}");
}
