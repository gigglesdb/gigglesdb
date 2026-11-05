mod common;
use common::*;
use giggles_core::{Admin, Limits};
use giggles_export::*;
use std::{path::Path, process::Command};
#[test]
fn verified_target_held_until_marker() {
    let (p, a) = populated("verified-lock");
    export(&a, p.join("copy"), Bounds::default()).unwrap();
    import_with_hook(
        p.join("copy"),
        p.join("fresh"),
        Bounds::default(),
        &mut |pt| {
            if pt == Point::ImportVerified {
                assert!(!p.join("fresh/logical-import-complete.json").exists());
                assert!(Admin::open(p.join("fresh"), Limits::default()).is_err());
            }
            Ok(())
        },
    )
    .unwrap();
    assert!(p.join("fresh/logical-import-complete.json").exists());
    assert!(Admin::open(p.join("fresh"), Limits::default()).is_ok());
}
fn cli(args: &[&Path]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_giggles-export"))
        .args(args)
        .output()
        .unwrap()
}
fn child(mode: &str, p: &Path) {
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "process_worker", "--nocapture"])
        .env("GDEX_TEST_MODE", mode)
        .env("GDEX_TEST_DIR", p)
        .output()
        .unwrap();
    std::fs::write(p.join(format!("{mode}-stdout.log")), &output.stdout).unwrap();
    std::fs::write(p.join(format!("{mode}-stderr.log")), &output.stderr).unwrap();
    assert_eq!(
        output.status.code(),
        Some(77),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
#[test]
fn process_worker() {
    let Ok(mode) = std::env::var("GDEX_TEST_MODE") else {
        return;
    };
    let p = std::path::PathBuf::from(std::env::var_os("GDEX_TEST_DIR").unwrap());
    let point = match mode.as_str() {
        "export" => Point::StageSynced,
        "created" => Point::ImportCreated,
        "staged" => Point::ImportStaged,
        "committed" => Point::ImportCommitted,
        "verified" => Point::ImportVerified,
        _ => panic!("mode"),
    };
    let mut hook = |at| {
        if at == point {
            std::process::exit(77)
        }
        Ok(())
    };
    if mode == "export" {
        let a = Admin::open(p.join("db"), Limits::default()).unwrap();
        export_with_hook(&a, p.join("copy"), Bounds::default(), &mut hook).unwrap();
    } else {
        import_with_hook(
            p.join("copy"),
            p.join("interrupted"),
            Bounds::default(),
            &mut hook,
        )
        .unwrap();
    }
    panic!("expected interruption")
}
#[test]
fn export_no_clobber() {
    let (p, a) = populated("no-clobber");
    std::fs::write(p.join("copy"), b"sentinel").unwrap();
    assert!(matches!(
        export(&a, p.join("copy"), Bounds::default()),
        Err(Error::Exists)
    ));
    assert_eq!(std::fs::read(p.join("copy")).unwrap(), b"sentinel");
}
#[test]
fn import_no_clobber() {
    let (p, a) = populated("no-clobber-import");
    export(&a, p.join("copy"), Bounds::default()).unwrap();
    std::fs::create_dir(p.join("existing")).unwrap();
    assert!(import(p.join("copy"), p.join("existing"), Bounds::default()).is_err());
    assert_eq!(std::fs::read_dir(p.join("existing")).unwrap().count(), 0);
}
#[test]
fn retained_stage_on_failure() {
    let (p, a) = populated("retained");
    assert!(
        export_with_hook(&a, p.join("copy"), Bounds::default(), &mut |at| if at
            == Point::StageSynced
        {
            Err(Error::Injected)
        } else {
            Ok(())
        })
        .is_err()
    );
    assert!(!p.join("copy").exists());
    let staging = std::fs::read_dir(&p)
        .unwrap()
        .filter_map(|e| {
            let p = e.unwrap().path();
            p.extension().is_some_and(|e| e == "staging").then_some(p)
        })
        .collect::<Vec<_>>();
    assert_eq!(staging.len(), 1);
    assert!(verify(&staging[0], Bounds::default()).is_ok());
}
#[test]
fn subprocess_export_abort_restart() {
    let (p, a) = populated("process-export");
    drop(a);
    child("export", &p);
    assert!(!p.join("copy").exists());
    let a = Admin::open(p.join("db"), Limits::default()).unwrap();
    export(&a, p.join("copy"), Bounds::default()).unwrap();
    assert!(verify(p.join("copy"), Bounds::default()).is_ok());
    assert!(export(&a, p.join("copy"), Bounds::default()).is_err());
}
#[test]
fn subprocess_import_abort_restart() {
    for mode in ["created", "staged", "committed", "verified"] {
        let (p, a) = populated(mode);
        let expected = capture(&a, Bounds::default()).unwrap();
        export(&a, p.join("copy"), Bounds::default()).unwrap();
        drop(a);
        child(mode, &p);
        assert!(p.join("interrupted").exists());
        assert!(!p.join("interrupted/logical-import-complete.json").exists());
        assert!(import(p.join("copy"), p.join("interrupted"), Bounds::default()).is_err());
        let b = Admin::open(p.join("interrupted"), Limits::default()).unwrap();
        if mode == "created" || mode == "staged" {
            assert!(b.catalog().unwrap().1.is_empty())
        } else {
            assert_eq!(capture(&b, Bounds::default()).unwrap(), expected)
        }
        drop(b);
        import(p.join("copy"), p.join("fresh"), Bounds::default()).unwrap();
        assert!(p.join("fresh/logical-import-complete.json").is_file());
    }
}
#[test]
fn cli_real_export_import_verify() {
    let (p, a) = populated("cli");
    let expected = capture(&a, Bounds::default()).unwrap();
    drop(a);
    for args in [
        vec![Path::new("export"), &p.join("db"), &p.join("copy")],
        vec![Path::new("verify"), &p.join("copy")],
        vec![Path::new("import"), &p.join("copy"), &p.join("fresh")],
    ] {
        let out = cli(&args);
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let r: Report = serde_json::from_slice(&out.stdout).unwrap();
        assert_eq!(r.rows, 2);
    }
    let b = Admin::open(p.join("fresh"), Limits::default()).unwrap();
    assert_eq!(capture(&b, Bounds::default()).unwrap(), expected);
}
#[test]
fn cli_invalid_arguments() {
    for args in [
        vec![],
        vec![Path::new("invalid")],
        vec![Path::new("export")],
        vec![Path::new("import"), Path::new("missing")],
    ] {
        let out = cli(&args);
        assert!(!out.status.success());
        assert!(out.stdout.is_empty());
    }
}
#[test]
fn publish_race_never_clobbers() {
    let (p, a) = populated("race");
    let target = p.join("copy");
    assert!(
        export_with_hook(&a, &target, Bounds::default(), &mut |pt| {
            if pt == Point::StageSynced {
                std::fs::write(&target, b"racer")?;
            }
            Ok(())
        })
        .is_err()
    );
    assert_eq!(std::fs::read(target).unwrap(), b"racer");
}
#[test]
fn concurrent_publish_one_winner() {
    let (p, a) = populated("publish-race");
    let target = p.join("copy");
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let mut handles = Vec::new();
    for _ in 0..2 {
        let a = a.clone();
        let target = target.clone();
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            export_with_hook(&a, target, Bounds::default(), &mut |pt| {
                if pt == Point::StageSynced {
                    barrier.wait();
                }
                Ok(())
            })
        }));
    }
    let successes = handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .filter(Result::is_ok)
        .count();
    assert_eq!(successes, 1);
    assert!(verify(target, Bounds::default()).is_ok());
}
