use std::{env, path::Path, process::Command};

fn git(manifest: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(manifest)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8(output.stdout).ok()?.trim().to_owned())
}

fn watch_git_path(manifest: &Path, name: &str) {
    if let Some(path) = git(manifest, &["rev-parse", "--git-path", name]) {
        println!("cargo:rerun-if-changed={}", manifest.join(path).display());
    }
}

fn main() {
    let manifest = env::var_os("CARGO_MANIFEST_DIR").expect("Cargo manifest directory");
    let manifest = Path::new(&manifest);
    println!("cargo:rerun-if-env-changed=GIGGLES_BUILD_ID");
    watch_git_path(manifest, "HEAD");
    if let Some(reference) = git(manifest, &["symbolic-ref", "-q", "HEAD"]) {
        watch_git_path(manifest, &reference);
    }
    watch_git_path(manifest, "packed-refs");
    let build_id = env::var("GIGGLES_BUILD_ID")
        .ok()
        .or_else(|| git(manifest, &["rev-parse", "--short=12", "HEAD"]))
        .unwrap_or_else(|| "unknown".into());
    println!("cargo:rustc-env=GIGGLES_BUILD_ID={build_id}");
}
