/// Root for test fixtures: $GIGGLES_TEST_ARTIFACTS when set, else the OS temp dir. Never the crate directory.
pub(crate) fn fixture_root() -> std::path::PathBuf {
    std::env::var_os("GIGGLES_TEST_ARTIFACTS")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
}
