//! Writes the generated descriptor without requiring protoc or server reflection.
fn main() -> std::io::Result<()> {
    let path = std::env::args()
        .nth(1)
        .ok_or_else(|| std::io::Error::other("usage: export_descriptor OUTPUT.bin"))?;
    std::fs::write(path, giggles_server::api::proto::FILE_DESCRIPTOR_SET)
}
