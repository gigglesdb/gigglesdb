mod common;

use giggles_core::{Admin, CoreError, Limits, relational::SqlError};
use std::{env, process::Command, thread};

#[test]
fn trim_depth_fits_stack_margin() {
    const CHILD: &str = "GIGGLES_SQL_STACK_MARGIN_CHILD";
    const STACK_SIZE: usize = 1792 * 1024;
    const STDERR_TAIL: usize = 4096;

    // A stack overflow aborts the child, so the test harness can report it
    // with the name of this test and the thread's diagnostics.
    if env::var_os(CHILD).is_none() {
        let output = Command::new(env::current_exe().unwrap())
            .args([
                "--exact",
                stringify!(trim_depth_fits_stack_margin),
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .expect("run the SQL stack margin child");
        let tail = &output.stderr[output.stderr.len().saturating_sub(STDERR_TAIL)..];
        assert!(
            output.status.success(),
            "nested TRIM failed on a {STACK_SIZE}-byte stack ({}):\n{}",
            output.status,
            String::from_utf8_lossy(tail)
        );
        // A renamed or moved test would make the child run nothing and pass.
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("test result: ok. 1 passed"),
            "the SQL stack margin child did not run exactly this test:\n{}",
            String::from_utf8_lossy(&output.stdout)
        );
        return;
    }

    thread::Builder::new()
        .name("nested-trim-bind".into())
        .stack_size(STACK_SIZE)
        .spawn(|| {
            let admin =
                Admin::create(common::fresh("sql-stack-margin"), Limits::default()).unwrap();
            let mut session = admin.sql_session();
            for depth in [32, 33] {
                let trim = format!("SELECT {}' x '{}", "TRIM(".repeat(depth), ")".repeat(depth));
                let normalized = format!(
                    "SELECT {}' x '{}",
                    "btrim(".repeat(depth),
                    ")".repeat(depth)
                );
                let trim = session.execute(&trim, &[], None);
                let normalized = session.execute(&normalized, &[], None);
                assert_eq!(trim, normalized, "depth {depth}");
                if depth == 32 {
                    trim.expect("TRIM should bind and execute at depth 32");
                } else {
                    assert_eq!(trim, Err(CoreError::Sql(SqlError::Limit)));
                }
            }
        })
        .expect("spawn the SQL stack margin thread")
        .join()
        .expect("join the SQL stack margin thread");
}
