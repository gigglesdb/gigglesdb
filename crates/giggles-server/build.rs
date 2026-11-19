fn main() -> Result<(), Box<dyn std::error::Error>> {
    let schema = "proto/giggles/v1/giggles.proto";
    println!("cargo:rerun-if-changed={schema}");
    println!("cargo:rerun-if-changed=build.rs");
    let out = std::path::PathBuf::from(std::env::var_os("OUT_DIR").ok_or("OUT_DIR missing")?);
    let filtered;
    let schema = if std::env::var_os("CARGO_FEATURE_LEGACY_TASK").is_some() {
        std::path::Path::new(schema)
    } else {
        // Keep the source schema and its wire assignments intact for legacy builds.
        // Only the four prototype request arms and their private types are retired.
        let mut source = std::fs::read_to_string(schema)?.replace("\r\n", "\n");
        let start = source
            .find("message PreviewRequest {")
            .ok_or("legacy schema start missing")?;
        let end = source
            .find("enum TransactionState {")
            .ok_or("legacy schema end missing")?;
        source.replace_range(start..end, "");
        for field in [
            "    PreviewRequest preview = 3;\n",
            "    TaskExecuteRequest execute = 4;\n",
            "    RepairSqlRequest repair_sql = 5;\n",
            "    StatusRequest status = 6;\n",
        ] {
            if !source.contains(field) {
                return Err("legacy schema field missing".into());
            }
            source = source.replace(field, "");
        }
        source = source.replace("message ExecuteRequest {", "message ExecuteRequest {\n  reserved 3, 4, 5, 6;\n  reserved \"preview\", \"execute\", \"repair_sql\", \"status\";");
        filtered = out.join("giggles.proto");
        std::fs::write(&filtered, source)?;
        filtered.as_path()
    };
    let include = schema.parent().ok_or("schema parent missing")?;
    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    tonic_prost_build::configure()
        .file_descriptor_set_path(
            std::path::PathBuf::from(std::env::var_os("OUT_DIR").ok_or("OUT_DIR missing")?)
                .join("giggles_descriptor.bin"),
        )
        .emit_rerun_if_changed(false)
        .compile_with_config(config, &[schema], &[include])?;
    Ok(())
}
