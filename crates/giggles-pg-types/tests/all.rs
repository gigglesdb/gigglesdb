// Single integration-test binary: every tests/*.rs file is a module here, so the
// crate links once instead of once per file. Files that re-run the test binary
// by exact test name (current_exe) stay separate [[test]] targets in Cargo.toml.
#[path = "bytea_input.rs"]
mod bytea_input;
#[path = "character.rs"]
mod character;
#[path = "e2_interval_unitless.rs"]
mod e2_interval_unitless;
#[path = "float_pg18.rs"]
mod float_pg18;
#[path = "integer_input_bounds.rs"]
mod integer_input_bounds;
#[path = "integer_input_oracle.rs"]
mod integer_input_oracle;
#[path = "numeric_audit.rs"]
mod numeric_audit;
#[path = "numeric_math.rs"]
mod numeric_math;
#[path = "numeric_nonlinear.rs"]
mod numeric_nonlinear;
#[path = "numeric_power.rs"]
mod numeric_power;
#[path = "oracle.rs"]
mod oracle;
#[path = "representation.rs"]
mod representation;
#[path = "temporal.rs"]
mod temporal;
#[path = "temporal_arithmetic.rs"]
mod temporal_arithmetic;
#[path = "temporal_calendar.rs"]
mod temporal_calendar;
#[path = "temporal_compat.rs"]
mod temporal_compat;
#[path = "temporal_functions.rs"]
mod temporal_functions;
#[path = "temporal_input_grammar.rs"]
mod temporal_input_grammar;
#[path = "temporal_interval_rounding.rs"]
mod temporal_interval_rounding;
#[path = "temporal_overloads.rs"]
mod temporal_overloads;
#[path = "temporal_zones.rs"]
mod temporal_zones;
#[path = "time_zone_displacement.rs"]
mod time_zone_displacement;
#[path = "typmod.rs"]
mod typmod;
/// With `autotests = false`, a new `tests/*.rs` file is silently skipped unless it
/// is registered. Fail instead: every file must be a module above or a [[test]].
#[test]
fn every_test_file_is_registered() {
    let all = include_str!("all.rs");
    let manifest = include_str!("../Cargo.toml");
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let mut missing = Vec::new();
    for entry in std::fs::read_dir(&dir).expect("read tests dir") {
        let path = entry.expect("dir entry").path();
        if path.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        let stem = path.file_stem().and_then(|s| s.to_str()).expect("utf-8 name");
        if stem == "all" {
            continue;
        }
        let as_module = all.contains(&format!("mod {stem};"));
        let as_target = manifest.contains(&format!("path = \"tests/{stem}.rs\""));
        if !as_module && !as_target {
            missing.push(stem.to_owned());
        }
    }
    assert!(
        missing.is_empty(),
        "tests/*.rs files not registered in tests/all.rs or Cargo.toml [[test]]: {missing:?}"
    );
}
