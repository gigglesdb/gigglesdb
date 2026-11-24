use std::{env, fs, path::PathBuf};

fn main() {
    if env::var_os("CARGO_FEATURE_ENGINE_ROCKSDB").is_some() {
        build_autorecovery_veto();
    }
}

fn build_autorecovery_veto() {
    let shim_source = "native/autorecovery_veto.cc";
    println!("cargo:rerun-if-changed={shim_source}");
    for name in [
        "ROCKSDB_COMPILE",
        "ROCKSDB_LIB_DIR",
        "ROCKSDB_INCLUDE_DIR",
        "ROCKSDB_CXX_STD",
    ] {
        println!("cargo:rerun-if-env-changed={name}");
    }
    let forced_vendored = env::var("ROCKSDB_COMPILE")
        .is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("true"));
    let requirement = "the auto-recovery veto requires the vendored static librocksdb-sys build";
    assert!(
        forced_vendored || env::var_os("ROCKSDB_LIB_DIR").is_none(),
        "{requirement}"
    );
    let rocks_out = PathBuf::from(env::var_os("DEP_ROCKSDB_OUT_DIR").expect(requirement));
    assert!(
        rocks_out.join("librocksdb.a").is_file(),
        "{requirement}: missing librocksdb.a in {}",
        rocks_out.display()
    );
    let source = PathBuf::from(env::var_os("DEP_ROCKSDB_CARGO_MANIFEST_DIR").expect(requirement))
        .join("rocksdb");
    if let Some(include) = env::var_os("ROCKSDB_INCLUDE_DIR") {
        assert_eq!(
            fs::canonicalize(include).ok(),
            Some(source.join("include").canonicalize().expect(requirement)),
            "{requirement}: ROCKSDB_INCLUDE_DIR must point to the vendored headers"
        );
    }

    // The public C API leaves these types opaque. Verify the definitions in
    // the pinned source that also supplies the static library.
    let c_api = source.join("db/c.cc");
    println!("cargo:rerun-if-changed={}", c_api.display());
    let c_api = fs::read_to_string(c_api)
        .expect("read vendored RocksDB C API source")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    for layout in [
        "struct rocksdb_options_t { Options rep; };",
        "struct rocksdb_env_t { Env* rep; bool is_default; };",
    ] {
        assert!(
            c_api.contains(layout),
            "RocksDB C API layout changed: {layout}"
        );
    }

    // Mirror the pinned librocksdb-sys build for the features used here
    // (static + lz4, with no USE_RTTI or mt_static). Both use the compiler's
    // RTTI default and cc's target CRT selection, including crt-static.
    let target = env::var("TARGET").expect("Cargo target");
    let mut shim = cc::Build::new();
    shim.cpp(true)
        .include(&source)
        .include(source.join("include"))
        .define("NDEBUG", "1")
        .define("LZ4", "1")
        .define("HAVE_ALIGNED_NEW", None)
        .file(shim_source);
    if !target.contains("msvc") {
        shim.define("HAVE_UINT128_EXTENSION", None);
    }
    if env::var("CARGO_CFG_TARGET_POINTER_WIDTH").as_deref() != Ok("64")
        && target != "armv7-linux-androideabi"
    {
        shim.define("_FILE_OFFSET_BITS", "64")
            .define("_LARGEFILE64_SOURCE", "1");
    }
    if target.contains("windows") {
        for name in [
            "DWIN32",
            "OS_WIN",
            "_MBCS",
            "WIN64",
            "NOMINMAX",
            "ROCKSDB_WINDOWS_UTF8_FILENAMES",
        ] {
            shim.define(name, None);
        }
        if target == "x86_64-pc-windows-gnu" {
            shim.define("_POSIX_C_SOURCE", "1")
                .define("_WIN32_WINNT", "_WIN32_WINNT_VISTA");
        }
    } else {
        shim.define("ROCKSDB_PLATFORM_POSIX", None)
            .define("ROCKSDB_LIB_IO_POSIX", None)
            .flag("-include")
            .flag("cstdint");
        if target.contains("linux") {
            for name in [
                "OS_LINUX",
                "ROCKSDB_SCHED_GETCPU_PRESENT",
                "ROCKSDB_AUXV_GETAUXVAL_PRESENT",
                "ROCKSDB_FALLOCATE_PRESENT",
                "ROCKSDB_RANGESYNC_PRESENT",
            ] {
                shim.define(name, None);
            }
        } else if target.contains("darwin") {
            shim.define("OS_MACOSX", None);
        }
    }
    if target.contains("msvc") {
        shim.flag("-EHsc").flag("-std:c++20");
    } else {
        let standard = env::var("ROCKSDB_CXX_STD").unwrap_or_else(|_| "c++20".into());
        shim.flag(if standard.starts_with("-std=") {
            standard
        } else {
            format!("-std={standard}")
        });
    }
    shim.compile("giggles_autorecovery_veto");
    // Cargo build scripts cannot see cfg(test). Build a separate archive and
    // link it only from the Rust unit test module.
    shim.cargo_metadata(false)
        .define("GIGGLES_ROCKSDB_AUTORECOVERY_TEST", None)
        .compile("giggles_autorecovery_veto_test");
}
