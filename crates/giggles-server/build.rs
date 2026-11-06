fn main() -> Result<(), Box<dyn std::error::Error>> {
    let schema = "proto/giggles/v1/giggles.proto";
    println!("cargo:rerun-if-changed={schema}");
    println!("cargo:rerun-if-changed=build.rs");
    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    tonic_prost_build::configure()
        .file_descriptor_set_path(
            std::path::PathBuf::from(std::env::var_os("OUT_DIR").ok_or("OUT_DIR missing")?)
                .join("giggles_descriptor.bin"),
        )
        .emit_rerun_if_changed(false)
        .compile_with_config(config, &[schema], &["proto"])?;
    Ok(())
}
