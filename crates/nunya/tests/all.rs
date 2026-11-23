//! Combined integration test registration guard.
#[test]
fn every_test_file_is_registered() {
    let source = include_str!("all.rs");
    let tests = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    for entry in std::fs::read_dir(tests).expect("read tests directory") {
        let path = entry.expect("read test entry").path();
        if path.extension().is_some_and(|ext| ext == "rs")
            && path.file_name().is_some_and(|name| name != "all.rs")
        {
            let name = path
                .file_name()
                .expect("test filename")
                .to_str()
                .expect("UTF-8 test filename");
            assert!(
                source.contains(&format!(r#"#[path = "{name}"]"#)),
                "unregistered test file: {name}"
            );
        }
    }
}

#[path = "common/mod.rs"]
mod common;
#[path = "management_api.rs"]
mod management_api;
#[path = "corpus.rs"]
mod corpus;
#[path = "noninterference.rs"]
mod noninterference;
#[path = "serve_pg.rs"]
mod serve_pg;
#[path = "access_sessions.rs"]
mod access_sessions;

#[path = "spanner_native.rs"]
mod spanner_native;
#[path = "spanner_pg.rs"]
mod spanner_pg;
#[path = "readlog_rotation.rs"]
mod readlog_rotation;

#[path = "bindings_live.rs"] mod bindings_live;
#[path = "json_filter.rs"]
mod json_filter;
#[path = "upstream_tls.rs"]
mod upstream_tls;

#[path = "route_default_live.rs"]
mod route_default_live;

#[path = "json_paths_live.rs"]
mod json_paths_live;

#[path = "shared_domains_live.rs"]
mod shared_domains_live;

#[path = "policy_revisions_live.rs"]
mod policy_revisions_live;

#[path = "policy_fencing_live.rs"]
mod policy_fencing_live;
