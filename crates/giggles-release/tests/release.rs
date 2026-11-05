use giggles_release::*;

#[test]
fn review_include_space_preserves_compiled_asset_identity() {
    let (p, r) = fixture();
    fs::write(
        p.join("src/main.rs"),
        "fn main(){let _=include_str!(\"data file.txt\");}",
    )
    .unwrap();
    fs::write(p.join("src/data file.txt"), "actual A").unwrap();
    fs::write(p.join("src/datafile.txt"), "decoy").unwrap();
    let a = inventory(&p, &r.manifest).unwrap().0;
    assert!(a.iter().any(|s| s.path == "src/data file.txt"));
    fs::write(p.join("src/data file.txt"), "actual B").unwrap();
    assert_ne!(a, inventory(&p, &r.manifest).unwrap().0);
}

#[test]
fn review_custom_build_script_rejected_before_execution() {
    let (p, r) = fixture();
    let manifest = fs::read_to_string(p.join("Cargo.toml"))
        .unwrap()
        .replace("edition='2024'", "edition='2024'\nbuild='custom.rs'");
    fs::write(p.join("Cargo.toml"), manifest).unwrap();
    fs::write(
        p.join("custom.rs"),
        "fn main(){std::fs::write(\"script-ran\", b\"bad\").unwrap();}",
    )
    .unwrap();
    let br = BuildRequest {
        request: r,
        package: "different-package".into(),
        binary: "actual-bin".into(),
        target_dir: p.join("target-custom"),
        timeout_seconds: 60,
    };
    assert!(build(&br, &p.join("receipt.json")).is_err());
    assert!(!p.join("script-ran").exists());
    assert!(!br.target_dir.exists());
}

#[test]
fn review_non_rs_cargo_target_rejected() {
    let (p, r) = fixture();
    let manifest = fs::read_to_string(p.join("Cargo.toml"))
        .unwrap()
        .replace("src/main.rs", "src/main.txt");
    fs::write(p.join("Cargo.toml"), manifest).unwrap();
    fs::write(p.join("src/main.txt"), "fn main(){}").unwrap();
    assert!(inventory(&p, &r.manifest).is_err());
}

#[test]
fn review_explicit_module_path_rejected() {
    let (p, r) = fixture();
    fs::write(
        p.join("src/main.rs"),
        "#[path=\"module.txt\"] mod redirected; fn main(){}",
    )
    .unwrap();
    fs::write(p.join("src/module.txt"), "pub fn value()->u8 {1}").unwrap();
    assert!(inventory(&p, &r.manifest).is_err());
}

#[test]
fn review_conditional_module_path_rejected() {
    let (p, r) = fixture();
    fs::write(
        p.join("src/main.rs"),
        "#[cfg_attr(windows,path=\"module.txt\")] mod redirected; fn main(){}",
    )
    .unwrap();
    fs::write(p.join("src/module.txt"), "pub fn value()->u8 {1}").unwrap();
    assert!(inventory(&p, &r.manifest).is_err());
}

#[test]
fn review_raw_include_and_comments_preserve_asset() {
    let (p, r) = fixture();
    fs::write(p.join("src/main.rs"), "// include_str!(not_a_literal)\nfn main(){let _=include_bytes! /* spacing */ (r#\"data file.txt\"#);}").unwrap();
    fs::write(p.join("src/data file.txt"), "actual").unwrap();
    let a = inventory(&p, &r.manifest).unwrap().0;
    assert!(
        a.iter()
            .any(|s| s.path == "src/data file.txt" && s.sha256 == hash(b"actual"))
    );
}

#[test]
fn review_workspace_link_rejected_before_canonicalization() {
    let (p, r) = fixture();
    let parent = temp();
    let linked = parent.join("linked");
    #[cfg(windows)]
    assert!(
        Command::new("cmd")
            .args(["/c", "mklink", "/J"])
            .arg(&linked)
            .arg(&p)
            .status()
            .unwrap()
            .success()
    );
    #[cfg(unix)]
    std::os::unix::fs::symlink(&p, &linked).unwrap();
    assert!(inventory(&linked, &r.manifest).is_err());
}

#[test]
fn review_dependency_link_rejected_before_metadata_normalization() {
    let (p, r) = fixture();
    let dep = p.join("dep-real");
    fs::create_dir_all(dep.join("src")).unwrap();
    fs::write(
        dep.join("Cargo.toml"),
        "[package]\nname='local-dep'\nversion='0.1.0'\nedition='2024'\n",
    )
    .unwrap();
    fs::write(dep.join("src/lib.rs"), "pub fn f(){}\n").unwrap();
    let linked = p.join("dep-linked");
    #[cfg(windows)]
    assert!(
        Command::new("cmd")
            .args(["/c", "mklink", "/J"])
            .arg(&linked)
            .arg(&dep)
            .status()
            .unwrap()
            .success()
    );
    #[cfg(unix)]
    std::os::unix::fs::symlink(&dep, &linked).unwrap();
    let manifest = fs::read_to_string(p.join("Cargo.toml")).unwrap()
        + "\n[dependencies]\nlocal-dep={path='dep-linked'}\n";
    fs::write(p.join("Cargo.toml"), manifest).unwrap();
    assert!(
        Command::new("cargo")
            .args(["generate-lockfile", "--offline", "--manifest-path"])
            .arg(p.join("Cargo.toml"))
            .status()
            .unwrap()
            .success()
    );
    assert!(inventory(&p, &r.manifest).is_err());
}
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, Barrier},
};
fn temp() -> PathBuf {
    unique_dir(&std::env::temp_dir().join("giggles-release-retained")).unwrap()
}
fn fixture() -> (PathBuf, CreateRequest) {
    let p = temp();
    fs::create_dir(p.join("src")).unwrap();
    fs::write(p.join("Cargo.toml"),"[package]\nname='different-package'\nversion='0.1.0'\nedition='2024'\n[[bin]]\nname='actual-bin'\npath='src/main.rs'\n[workspace]\n").unwrap();
    fs::write(
        p.join("Cargo.lock"),
        "version = 4\n[[package]]\nname='different-package'\nversion='0.1.0'\n",
    )
    .unwrap();
    fs::write(p.join("src/main.rs"), "fn main() {}\n").unwrap();
    fs::write(p.join("program.exe"), b"actual executable fixture").unwrap();
    fs::write(p.join("README.md"), b"product usage").unwrap();
    let r = CreateRequest {
        workspace: p.clone(),
        manifest: "Cargo.toml".into(),
        release: "0.1.0-local".into(),
        target: "x86_64-pc-windows-msvc".into(),
        profile: "release".into(),
        source_revision: "explicit-uncommitted-fixture".into(),
        dirty: true,
        build_receipt: None,
        files: vec![
            InputFile {
                source: "program.exe".into(),
                path: "bin/giggles.exe".into(),
                role: Role::Binary,
            },
            InputFile {
                source: "README.md".into(),
                path: "README.md".into(),
                role: Role::Readme,
            },
        ],
    };
    (p, r)
}
fn manifest() -> ReleaseManifest {
    ReleaseManifest {
        format: "giggles-release-json-v1".into(),
        version: 1,
        release: "v1".into(),
        target: "host".into(),
        profile: "release".into(),
        rustc: "rustc fixture".into(),
        cargo: "cargo fixture".into(),
        source_revision: "dirty-fixture".into(),
        dirty: true,
        sources: vec![SourceRecord {
            path: "Cargo.toml".into(),
            size: 1,
            sha256: hash(b"s"),
        }],
        dependencies: vec![],
        files: vec![FileRecord {
            path: "bin/a.exe".into(),
            size: 1,
            sha256: hash(b"a"),
            role: Role::Binary,
        }],
        readiness_issues: vec![],
        build: None,
    }
}
fn archive() -> Archive {
    Archive {
        manifest: manifest(),
        files: BTreeMap::from([("bin/a.exe".into(), "61".into())]),
    }
}
fn check(a: &Archive) -> bool {
    verify_bytes(&serde_json::to_vec(a).unwrap()).is_ok()
}
#[test]
fn schema_valid_manifest() {
    manifest().validate().unwrap();
    assert!(check(&archive()));
}
#[test]
fn schema_unsafe_paths_table() {
    for p in [
        "", "/x", "../x", "a/../x", "a/./x", "C:/x", "a\\b", "a:b", "a//b", "a/", "CON", "nul.txt",
        "COM1.exe", "LPT9", "a.", "a ", "a\n", "a*", "a?", "a|b", "é",
    ] {
        assert!(portable_path(p).is_err(), "{p:?}");
    }
    for p in [
        "bin/giggles.exe",
        "README.md",
        "license/LICENSE.txt",
        "a-b_1.rs",
    ] {
        portable_path(p).unwrap();
    }
}
#[test]
fn schema_wrong_version() {
    let mut a = archive();
    a.manifest.version = 2;
    assert!(!check(&a));
}
#[test]
fn schema_wrong_format() {
    let mut a = archive();
    a.manifest.format = "zip".into();
    assert!(!check(&a));
}
#[test]
fn schema_unknown_fields() {
    let mut v = serde_json::to_value(archive()).unwrap();
    v["manifest"]["tested"] = true.into();
    assert!(verify_bytes(&serde_json::to_vec(&v).unwrap()).is_err());
}
#[test]
fn schema_invalid_hash() {
    for h in ["", "00", &"A".repeat(64), &"g".repeat(64)] {
        let mut m = manifest();
        m.files[0].sha256 = h.into();
        assert!(m.validate().is_err());
    }
}
#[test]
fn schema_duplicate_case_paths() {
    let mut m = manifest();
    let mut f = m.files[0].clone();
    f.path = "BIN/A.EXE".into();
    m.files.push(f);
    assert!(m.validate().is_err());
}
#[test]
fn schema_duplicate_sources() {
    let mut m = manifest();
    m.sources.push(m.sources[0].clone());
    assert!(m.validate().is_err());
}
#[test]
fn schema_duplicate_dependencies() {
    let mut m = manifest();
    let d = Dependency {
        name: "a".into(),
        version: "1".into(),
        source: None,
        checksum: None,
        license: None,
        manifest_path: Some("Cargo.toml".into()),
    };
    m.dependencies = vec![d.clone(), d];
    assert!(m.validate().is_err());
}
#[test]
fn schema_bounds() {
    let mut m = manifest();
    m.files[0].size = MAX_FILE + 1;
    assert!(m.validate().is_err());
    m = manifest();
    m.release = "x".repeat(8193);
    assert!(m.validate().is_err());
    m = manifest();
    m.files.clear();
    assert!(m.validate().is_err());
    m = manifest();
    m.sources.clear();
    assert!(m.validate().is_err());
}
#[test]
fn bundle_flip_byte() {
    let mut a = archive();
    a.files.insert("bin/a.exe".into(), "62".into());
    assert!(!check(&a));
}
#[test]
fn bundle_missing_file() {
    let mut a = archive();
    a.files.clear();
    assert!(!check(&a));
}
#[test]
fn bundle_extra_file() {
    let mut a = archive();
    a.files.insert("secret".into(), "61".into());
    assert!(!check(&a));
}
#[test]
fn bundle_size_mismatch() {
    let mut a = archive();
    a.manifest.files[0].size = 2;
    assert!(!check(&a));
}
#[test]
fn bundle_invalid_hex() {
    for h in ["6", "zz", "6A"] {
        let mut a = archive();
        a.files.insert("bin/a.exe".into(), h.into());
        assert!(!check(&a));
    }
}
#[test]
fn bundle_permutation_equal() {
    let (p, mut r) = fixture();
    create(&r, &p.join("a.ggr")).unwrap();
    r.files.reverse();
    create(&r, &p.join("b.ggr")).unwrap();
    assert_eq!(
        fs::read(p.join("a.ggr")).unwrap(),
        fs::read(p.join("b.ggr")).unwrap()
    );
}
#[test]
fn bundle_no_clobber() {
    let (p, r) = fixture();
    let out = p.join("a.ggr");
    create(&r, &out).unwrap();
    let bytes = fs::read(&out).unwrap();
    assert!(create(&r, &out).is_err());
    assert_eq!(bytes, fs::read(out).unwrap());
}
#[test]
fn bundle_concurrent_one_winner() {
    let (p, r) = fixture();
    let b = Arc::new(Barrier::new(2));
    let handles: Vec<_> = (0..2)
        .map(|_| {
            let b = b.clone();
            let r = r.clone();
            let out = p.join("same.ggr");
            std::thread::spawn(move || {
                b.wait();
                create(&r, &out).is_ok()
            })
        })
        .collect();
    let n = handles
        .into_iter()
        .filter(|h| h.thread().id() != std::thread::current().id())
        .map(|h| usize::from(h.join().unwrap()))
        .sum::<usize>();
    assert_eq!(n, 1);
    verify(&p.join("same.ggr")).unwrap();
}
#[test]
fn bundle_incomplete_staging() {
    let p = temp();
    fs::write(p.join("incomplete.ggr"), b"{\"manifest\":").unwrap();
    assert!(verify(&p.join("incomplete.ggr")).is_err());
}
#[test]
fn bundle_after_publish_verified() {
    let (p, r) = fixture();
    let m = create(&r, &p.join("a.ggr")).unwrap();
    assert_eq!(m, verify(&p.join("a.ggr")).unwrap());
    assert!(fs::read_dir(&p).unwrap().any(|e| {
        e.unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with("staging-")
    }));
}
#[test]
fn bundle_allowlist_rejects_log() {
    let (p, mut r) = fixture();
    r.files[1].path = "process.log".into();
    assert!(create(&r, &p.join("a.ggr")).is_err());
}
#[test]
fn bundle_duplicate_name() {
    let (p, mut r) = fixture();
    r.files.push(r.files[0].clone());
    assert!(create(&r, &p.join("a.ggr")).is_err());
}
#[test]
fn bundle_path_traversal() {
    let (p, mut r) = fixture();
    r.files[0].path = "../evil.exe".into();
    assert!(create(&r, &p.join("a.ggr")).is_err());
}
#[test]
fn bundle_link_rejected() {
    let (p, mut r) = fixture();
    let link = p.join("link.exe");
    #[cfg(unix)]
    std::os::unix::fs::symlink(p.join("program.exe"), &link).unwrap();
    #[cfg(windows)]
    {
        let status = Command::new("cmd")
            .args(["/c", "mklink", "/H"])
            .arg(&link)
            .arg(p.join("program.exe"))
            .status()
            .unwrap();
        assert!(status.success()); /* hardlinks are regular files, unlike reparse points */
        assert!(read_bounded(&link, MAX_FILE).is_ok());
        let junction = p.join("junction");
        let s = Command::new("cmd")
            .args(["/c", "mklink", "/J"])
            .arg(&junction)
            .arg(&p)
            .status()
            .unwrap();
        assert!(s.success());
        r.files[0].source = junction.join("program.exe");
        assert!(create(&r, &p.join("a.ggr")).is_err());
    }
    #[cfg(unix)]
    {
        r.files[0].source = link;
        assert!(create(&r, &p.join("a.ggr")).is_err());
    }
}
#[test]
fn inventory_dirty_source_fingerprint() {
    let (p, r) = fixture();
    let a = inventory(&p, &r.manifest).unwrap();
    fs::write(p.join("src/main.rs"), "fn main(){println!(\"changed\");}").unwrap();
    let b = inventory(&p, &r.manifest).unwrap();
    assert_ne!(a.0, b.0);
    assert_eq!(a.1, b.1);
}
#[test]
fn inventory_package_binary_names() {
    let (p, r) = fixture();
    let (s, d, i) = inventory(&p, &r.manifest).unwrap();
    assert_eq!(d[0].name, "different-package");
    assert!(s.iter().any(|x| x.path == "src/main.rs"));
    assert!(i.iter().any(|x| x.contains("missing license")));
}
#[test]
fn inventory_path_dependency_closure() {
    let (p, r) = fixture();
    fs::create_dir_all(p.join("dep/src")).unwrap();
    fs::write(
        p.join("dep/Cargo.toml"),
        "[package]\nname='dep'\nversion='0.1.0'\nedition='2024'\n",
    )
    .unwrap();
    fs::write(p.join("dep/src/lib.rs"), "pub fn dependency() {}\n").unwrap();
    let c = fs::read_to_string(p.join("Cargo.toml")).unwrap();
    fs::write(
        p.join("Cargo.toml"),
        format!("{c}\n[dependencies]\ndep={{path='dep'}}\n"),
    )
    .unwrap();
    assert!(
        Command::new("cargo")
            .args(["generate-lockfile", "--offline", "--manifest-path"])
            .arg(p.join("Cargo.toml"))
            .status()
            .unwrap()
            .success()
    );
    let (s, d, _) = inventory(&p, &r.manifest).unwrap();
    assert!(s.iter().any(|s| s.path == "dep/src/lib.rs"));
    assert!(d.iter().any(|d| d.name == "dep"));
}
#[test]
fn inventory_missing_dependency() {
    let (p, r) = fixture();
    let c = fs::read_to_string(p.join("Cargo.toml")).unwrap();
    fs::write(
        p.join("Cargo.toml"),
        format!("{c}\n[dependencies]\nmissing={{path='missing'}}\n"),
    )
    .unwrap();
    assert!(inventory(&p, &r.manifest).is_err());
}
#[test]
fn inventory_outside_workspace_rejected() {
    let (p, r) = fixture();
    let (other, _) = fixture();
    assert!(inventory(&p, &other.join(r.manifest)).is_err());
}
fn cli() -> Command {
    Command::new(env!("CARGO_BIN_EXE_giggles-release"))
}
fn request(p: &Path, r: &CreateRequest) -> PathBuf {
    let f = p.join("request.json");
    fs::write(&f, serde_json::to_vec(r).unwrap()).unwrap();
    f
}
#[test]
fn cli_lifecycle() {
    let (p, r) = fixture();
    let f = request(&p, &r);
    let out = p.join("out.ggr");
    assert!(
        cli()
            .arg("create")
            .arg(f)
            .arg(&out)
            .output()
            .unwrap()
            .status
            .success()
    );
    for action in ["verify", "inspect"] {
        let o = cli().arg(action).arg(&out).output().unwrap();
        assert!(o.status.success());
        let m: ReleaseManifest = serde_json::from_slice(&o.stdout).unwrap();
        assert!(m.dirty);
    }
    assert!(
        !cli()
            .arg("create")
            .arg(p.join("request.json"))
            .arg(out)
            .output()
            .unwrap()
            .status
            .success()
    );
}
#[test]
fn cli_bad_arguments() {
    for args in [
        vec![],
        vec!["verify"],
        vec!["--help"],
        vec!["verify", "a", "b"],
    ] {
        assert!(!cli().args(args).output().unwrap().status.success());
    }
}
#[test]
fn cli_argument_injection_data() {
    let (p, r) = fixture();
    let f = request(&p, &r);
    let out = p.join("--version;echo-injected.ggr");
    assert!(
        cli()
            .arg("create")
            .arg(f)
            .arg(&out)
            .output()
            .unwrap()
            .status
            .success()
    );
    verify(&out).unwrap();
}
#[test]
fn cli_oversized_request() {
    let p = temp();
    let f = p.join("large.json");
    fs::write(&f, vec![b' '; MAX_REQUEST as usize + 1]).unwrap();
    assert!(
        !cli()
            .arg("create")
            .arg(f)
            .arg(p.join("out.ggr"))
            .output()
            .unwrap()
            .status
            .success()
    );
}
#[test]
fn cli_no_stdin() {
    assert!(
        !cli()
            .args(["create", "-", "out.ggr"])
            .output()
            .unwrap()
            .status
            .success()
    );
}
#[test]
fn failed_build_receipt() {
    let (p, r) = fixture();
    fs::write(p.join("src/main.rs"), "this is not Rust").unwrap();
    let br = BuildRequest {
        request: r,
        package: "different-package".into(),
        binary: "actual-bin".into(),
        target_dir: p.join("target-private"),
        timeout_seconds: 60,
    };
    let receipt = p.join("failure.json");
    let b = build(&br, &receipt).unwrap();
    assert!(!b.success);
    assert!(receipt.exists());
    assert!(!p.join("success.ggr").exists());
}
#[test]
fn build_existing_target_rejected() {
    let (p, r) = fixture();
    let br = BuildRequest {
        request: r,
        package: "different-package".into(),
        binary: "actual-bin".into(),
        target_dir: p.clone(),
        timeout_seconds: 60,
    };
    assert!(build(&br, &p.join("receipt.json")).is_err());
}
#[test]
fn build_timeout_bound() {
    let (p, r) = fixture();
    let br = BuildRequest {
        request: r,
        package: "different-package".into(),
        binary: "actual-bin".into(),
        target_dir: p.join("target-private"),
        timeout_seconds: 0,
    };
    assert!(build(&br, &p.join("receipt.json")).is_err());
}
#[test]
fn bundle_duplicate_json_keys_reject() {
    let s = serde_json::to_string(&archive()).unwrap();
    let s = s.replace(
        "\"bin/a.exe\":\"61\"",
        "\"bin/a.exe\":\"62\",\"bin/a.exe\":\"61\"",
    );
    assert!(verify_bytes(s.as_bytes()).is_err());
}
#[test]
fn schema_omitted_dependency_manifest() {
    let mut m = manifest();
    m.dependencies.push(Dependency {
        name: "local".into(),
        version: "1".into(),
        source: None,
        checksum: None,
        license: None,
        manifest_path: Some("dep/Cargo.toml".into()),
    });
    assert!(m.validate().is_err());
}
#[test]
fn bundle_payload_mutation_property() {
    for byte in 0..=255u8 {
        if byte == b'a' {
            continue;
        }
        let mut a = archive();
        a.files.insert("bin/a.exe".into(), format!("{byte:02x}"));
        assert!(!check(&a), "byte={byte}");
    }
}
#[test]
fn schema_size_mutation_property() {
    for size in 0..256 {
        if size == 1 {
            continue;
        }
        let mut a = archive();
        a.manifest.files[0].size = size;
        assert!(!check(&a));
    }
}
#[test]
fn bundle_truncation_property() {
    let b = serde_json::to_vec(&archive()).unwrap();
    for n in 0..b.len() {
        assert!(verify_bytes(&b[..n]).is_err(), "prefix {n}");
    }
}
#[test]
fn inventory_unresolved_include_rejected() {
    let (p, r) = fixture();
    fs::write(
        p.join("src/main.rs"),
        "fn main(){let _=include_str!(\"untracked.txt\");}",
    )
    .unwrap();
    assert!(inventory(&p, &r.manifest).is_err());
}
#[test]
fn inventory_unknown_lock_version() {
    let (p, r) = fixture();
    fs::write(p.join("Cargo.lock"), "version=999\n").unwrap();
    assert!(inventory(&p, &r.manifest).is_err());
}
#[test]
fn cli_build_failure_no_success() {
    let (p, r) = fixture();
    fs::write(p.join("src/main.rs"), "not Rust").unwrap();
    let br = BuildRequest {
        request: r,
        package: "different-package".into(),
        binary: "actual-bin".into(),
        target_dir: p.join("target-cli"),
        timeout_seconds: 60,
    };
    let request = p.join("build.json");
    fs::write(&request, serde_json::to_vec(&br).unwrap()).unwrap();
    let receipt = p.join("build-receipt.json");
    let out = cli()
        .arg("build")
        .arg(request)
        .arg(&receipt)
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(out.stdout.is_empty());
    let r: BuildReceipt = serde_json::from_slice(&fs::read(receipt).unwrap()).unwrap();
    assert!(!r.success);
    assert!(r.artifact.is_none());
}
#[test]
fn build_success_receipt_matches_bundle() {
    let (p, mut r) = fixture();
    let br = BuildRequest {
        request: r.clone(),
        package: "different-package".into(),
        binary: "actual-bin".into(),
        target_dir: p.join("target-success"),
        timeout_seconds: 60,
    };
    let receipt = p.join("build-receipt.json");
    let b = build(&br, &receipt).unwrap();
    assert!(b.success, "{}", b.output);
    let suffix = if cfg!(windows) { ".exe" } else { "" };
    r.files[0].source = br
        .target_dir
        .join(&r.target)
        .join("release")
        .join(format!("actual-bin{suffix}"));
    r.build_receipt = Some(receipt);
    let m = create(&r, &p.join("bundle.ggr")).unwrap();
    assert!(m.build.is_some());
    verify(&p.join("bundle.ggr")).unwrap();
    r.profile = "dev".into();
    assert!(create(&r, &p.join("mismatched.ggr")).is_err());
}
#[test]
fn build_source_changed_during_build() {
    let (p, r) = fixture();
    fs::write(p.join("build.rs"), "fn main(){}\n").unwrap();
    let br = BuildRequest {
        request: r,
        package: "different-package".into(),
        binary: "actual-bin".into(),
        target_dir: p.join("target-unsafe"),
        timeout_seconds: 60,
    };
    assert!(build(&br, &p.join("receipt.json")).is_err());
}
#[test]
fn bundle_stale_build_receipt_rejected() {
    let (p, mut r) = fixture();
    let br = BuildRequest {
        request: r.clone(),
        package: "different-package".into(),
        binary: "actual-bin".into(),
        target_dir: p.join("target-stale"),
        timeout_seconds: 60,
    };
    let receipt = p.join("receipt.json");
    let b = build(&br, &receipt).unwrap();
    assert!(b.success);
    r.build_receipt = Some(receipt);
    fs::write(p.join("src/main.rs"), "fn main(){ /* changed */ }\n").unwrap();
    assert!(create(&r, &p.join("bundle.ggr")).is_err());
}
#[test]
fn build_selector_injection_rejected() {
    let (p, r) = fixture();
    let br = BuildRequest {
        request: r,
        package: "--version".into(),
        binary: "a".into(),
        target_dir: p.join("target-injection"),
        timeout_seconds: 60,
    };
    assert!(build(&br, &p.join("receipt.json")).is_err());
}
#[test]
fn inventory_registry_git_optional_entries() {
    let lock:toml::Value=toml::from_str(&format!("version=4\n[[package]]\nname='registry-optional'\nversion='2.3.4'\nsource='registry+https://github.com/rust-lang/crates.io-index'\nchecksum='{}'\n[[package]]\nname='git-platform'\nversion='5.6.7'\nsource='git+https://example.invalid/repo?rev=abc#abc'\n","a".repeat(64))).unwrap();
    let (d, i) = dependency_inventory(&lock, &[], Path::new(".")).unwrap();
    assert_eq!(d.len(), 2);
    assert_eq!(d[1].version, "2.3.4");
    assert_eq!(d[1].checksum.as_deref(), Some("a".repeat(64).as_str()));
    assert!(d[0].source.as_ref().unwrap().starts_with("git+"));
    assert_eq!(i.len(), 4);
    assert!(d.iter().all(|d| d.license.is_none()));
}
#[test]
fn inventory_literal_include_fingerprinted() {
    let (p, r) = fixture();
    fs::write(p.join("src/data.txt"), b"before").unwrap();
    fs::write(
        p.join("src/main.rs"),
        "fn main(){let _=include_str!(\"data.txt\");}",
    )
    .unwrap();
    let a = inventory(&p, &r.manifest).unwrap().0;
    assert!(a.iter().any(|s| s.path == "src/data.txt"));
    fs::write(p.join("src/data.txt"), b"after!").unwrap();
    let b = inventory(&p, &r.manifest).unwrap().0;
    assert_ne!(a, b);
}
#[test]
fn inventory_dynamic_include_rejected() {
    let (p, r) = fixture();
    fs::write(
        p.join("src/main.rs"),
        "fn main(){let _=include_str!(concat!(\"a\",\"b\"));}",
    )
    .unwrap();
    assert!(inventory(&p, &r.manifest).is_err());
}
#[test]
fn build_actual_source_change_during_build() {
    let (p, r) = fixture();
    let br = BuildRequest {
        request: r,
        package: "different-package".into(),
        binary: "actual-bin".into(),
        target_dir: p.join("target-mutation"),
        timeout_seconds: 60,
    };
    let target = br.target_dir.clone();
    let receipt = p.join("receipt.json");
    let handle = std::thread::spawn(move || build(&br, &receipt).unwrap());
    let start = std::time::Instant::now();
    while !target.exists() {
        assert!(start.elapsed() < std::time::Duration::from_secs(30));
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    fs::write(
        p.join("src/main.rs"),
        "fn main(){println!(\"changed during build\");}\n",
    )
    .unwrap();
    let result = handle.join().unwrap();
    assert!(!result.success);
    assert_ne!(result.sources_before, result.sources_after);
}
#[test]
fn schema_build_receipt_false_success_rejected() {
    let mut m = manifest();
    m.build = Some(BuildReceipt {
        command: vec!["cargo".into()],
        target: m.target.clone(),
        profile: m.profile.clone(),
        source_revision: m.source_revision.clone(),
        dirty: m.dirty,
        rustc: m.rustc.clone(),
        cargo: m.cargo.clone(),
        success: true,
        exit_code: Some(0),
        output: String::new(),
        sources_before: m.sources.clone(),
        sources_after: m.sources.clone(),
        artifact: Some(m.files[0].clone()),
    });
    m.validate().unwrap();
    m.build.as_mut().unwrap().success = false;
    assert!(m.validate().is_err());
}
#[test]
fn schema_build_artifact_digest_mismatch() {
    let mut m = manifest();
    m.build = Some(BuildReceipt {
        command: vec!["cargo".into()],
        target: m.target.clone(),
        profile: m.profile.clone(),
        source_revision: m.source_revision.clone(),
        dirty: m.dirty,
        rustc: m.rustc.clone(),
        cargo: m.cargo.clone(),
        success: true,
        exit_code: Some(0),
        output: String::new(),
        sources_before: m.sources.clone(),
        sources_after: m.sources.clone(),
        artifact: Some(FileRecord {
            path: "bin/b.exe".into(),
            size: 1,
            sha256: hash(b"b"),
            role: Role::Binary,
        }),
    });
    assert!(m.validate().is_err());
}
#[test]
fn inventory_workspace_members() {
    let p = temp();
    fs::write(
        p.join("Cargo.toml"),
        "[workspace]\nmembers=['a','b']\nresolver='3'\n",
    )
    .unwrap();
    for name in ["a", "b"] {
        fs::create_dir_all(p.join(name).join("src")).unwrap();
        fs::write(
            p.join(name).join("Cargo.toml"),
            format!("[package]\nname='{name}'\nversion='0.1.0'\nedition='2024'\n"),
        )
        .unwrap();
        fs::write(p.join(name).join("src/lib.rs"), "pub fn f() {}\n").unwrap();
    }
    assert!(
        Command::new("cargo")
            .args(["generate-lockfile", "--offline", "--manifest-path"])
            .arg(p.join("Cargo.toml"))
            .status()
            .unwrap()
            .success()
    );
    let (s, d, _) = inventory(&p, Path::new("Cargo.toml")).unwrap();
    assert_eq!(d.len(), 2);
    assert!(s.iter().any(|x| x.path == "a/src/lib.rs"));
    assert!(s.iter().any(|x| x.path == "b/src/lib.rs"));
}
#[test]
fn schema_file_directory_collision() {
    let mut m = manifest();
    m.files[0].path = "bin".into();
    m.files.push(FileRecord {
        path: "bin/a.exe".into(),
        size: 1,
        sha256: hash(b"a"),
        role: Role::Binary,
    });
    assert!(m.validate().is_err());
}
#[test]
fn inventory_workspace_manifest_and_config() {
    let (p, r) = fixture();
    fs::create_dir(p.join(".cargo")).unwrap();
    fs::write(p.join(".cargo/config.toml"), "[net]\noffline=true\n").unwrap();
    let (s, _, _) = inventory(&p, &r.manifest).unwrap();
    assert!(s.iter().any(|x| x.path == ".cargo/config.toml"));
}
#[test]
fn cli_crash_before_publish_retains_incomplete() {
    let (p, r) = fixture();
    let f = request(&p, &r);
    let out = p.join("killed.ggr");
    let mut child = cli()
        .arg("create")
        .arg(f)
        .arg(&out)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let start = std::time::Instant::now();
    loop {
        if fs::read_dir(&p).unwrap().any(|e| {
            e.unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("staging-")
        }) {
            let _ = child.kill();
            let _ = child.wait();
            break;
        }
        if child.try_wait().unwrap().is_some() {
            break;
        }
        assert!(start.elapsed() < std::time::Duration::from_secs(30));
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    if out.exists() {
        verify(&out).unwrap();
    } else {
        assert!(fs::read_dir(&p).unwrap().any(|e| {
            e.unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("staging-")
        }));
    }
}
#[test]
fn smoke_requires_explicit_flag() {
    assert!(
        !cli()
            .args(["smoke", "receipt", "exe", "scratch"])
            .output()
            .unwrap()
            .status
            .success()
    );
}
#[test]
fn smoke_rejects_unmatched_executable() {
    let p = temp();
    let r = BuildReceipt {
        command: vec!["cargo".into()],
        target: "host".into(),
        profile: "release".into(),
        source_revision: "fixture".into(),
        dirty: true,
        rustc: "rustc".into(),
        cargo: "cargo".into(),
        success: true,
        exit_code: Some(0),
        output: String::new(),
        sources_before: vec![],
        sources_after: vec![],
        artifact: Some(FileRecord {
            path: "bin/a.exe".into(),
            size: 1,
            sha256: hash(b"a"),
            role: Role::Binary,
        }),
    };
    let f = p.join("receipt.json");
    fs::write(&f, serde_json::to_vec(&r).unwrap()).unwrap();
    let exe = p.join("untrusted.exe");
    fs::write(&exe, b"untrusted").unwrap();
    assert!(smoke_local(&f, &exe, &p).is_err());
}
#[test]
fn build_concurrent_private_target_one_winner() {
    let (p, r) = fixture();
    let br = BuildRequest {
        request: r,
        package: "different-package".into(),
        binary: "actual-bin".into(),
        target_dir: p.join("target-race"),
        timeout_seconds: 60,
    };
    let barrier = Arc::new(Barrier::new(2));
    let threads: Vec<_> = (0..2)
        .map(|i| {
            let br = br.clone();
            let b = barrier.clone();
            let receipt = p.join(format!("receipt-{i}.json"));
            std::thread::spawn(move || {
                b.wait();
                build(&br, &receipt).map(|r| r.success).unwrap_or(false)
            })
        })
        .collect();
    assert_eq!(
        threads
            .into_iter()
            .map(|t| usize::from(t.join().unwrap()))
            .sum::<usize>(),
        1
    );
}
#[test]
fn bundle_actual_total_bound() {
    let (p, mut r) = fixture();
    for name in ["program.exe", "second.exe"] {
        fs::File::create(p.join(name))
            .unwrap()
            .set_len(33 * 1024 * 1024)
            .unwrap();
    }
    r.files.push(InputFile {
        source: "second.exe".into(),
        path: "bin/second.exe".into(),
        role: Role::Binary,
    });
    let error = create(&r, &p.join("oversized.ggr")).unwrap_err();
    assert!(error.to_string().contains("total artifact input bound"));
    assert!(!p.join("oversized.ggr").exists());
}
