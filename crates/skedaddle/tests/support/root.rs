//! Shared test-only storage policy. Keep Windows paths unchanged; resolve Unix aliases.
#![allow(dead_code)]
use std::path::{Path, PathBuf};

pub fn assert_under_work_root(path: &Path) {
    #[cfg(windows)]
    assert!(
        path.starts_with("J:/gigglesdb-work"),
        "all scratch must remain on J:"
    );
    #[cfg(not(windows))]
    {
        let work = std::env::var_os("SKEDADDLE_TEST_WORK_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("J:/gigglesdb-work"))
            .canonicalize()
            .expect("set SKEDADDLE_TEST_WORK_ROOT to the task work directory");
        assert!(
            path.is_absolute() && path.starts_with(&work),
            "test scratch {path:?} must be absolute and under {work:?}"
        );
    }
}

fn native_root(path: PathBuf) -> PathBuf {
    #[cfg(not(windows))]
    let path = {
        std::fs::create_dir_all(&path).expect("create test scratch root");
        path.canonicalize().expect("resolve test scratch root")
    };
    assert_under_work_root(&path);
    path
}

pub fn scratch_root() -> PathBuf {
    #[cfg(windows)]
    let root = std::env::var_os("SKEDADDLE_TEST_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| temp_root().join("skedaddle-test-scratch"));
    #[cfg(not(windows))]
    let root = PathBuf::from(
        std::env::var_os("SKEDADDLE_TEST_ROOT").expect("set task SKEDADDLE_TEST_ROOT"),
    );
    native_root(root)
}

pub fn temp_root() -> PathBuf {
    #[cfg(windows)]
    {
        native_root(PathBuf::from(
            std::env::var_os("TEMP").expect("set task TEMP"),
        ))
    }
    #[cfg(not(windows))]
    {
        scratch_root()
    }
}
