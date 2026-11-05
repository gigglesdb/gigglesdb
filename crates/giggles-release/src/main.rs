use giggles_release::*;
use std::path::Path;
fn run() -> Result<()> {
    let a: Vec<String> = std::env::args().skip(1).collect();
    if a.iter().any(|s| s.len() > 32768) {
        return Err("argument bound".into());
    }
    match a.iter().map(String::as_str).collect::<Vec<_>>().as_slice() {
        ["verify" | "inspect", path] => println!("{}", serde_json::to_string_pretty(&verify(Path::new(path))?)?),
        ["create", request, output] => { let r: CreateRequest = serde_json::from_slice(&read_bounded(Path::new(request),MAX_REQUEST)?)?; println!("{}",serde_json::to_string_pretty(&create(&r,Path::new(output))?)?); },
        ["build", request, receipt] => { let r: BuildRequest = serde_json::from_slice(&read_bounded(Path::new(request),MAX_REQUEST)?)?; let r = build(&r,Path::new(receipt))?; if !r.success { return Err("build failed; retained receipt".into()); } println!("{}",serde_json::to_string_pretty(&r)?); },
        ["smoke", "--local-built", receipt, executable, scratch] => println!("{}",serde_json::to_string_pretty(&smoke_local(Path::new(receipt),Path::new(executable),Path::new(scratch))?)?),
        _ => return Err("usage: giggles-release create REQUEST OUTPUT | verify ARCHIVE | inspect ARCHIVE | build REQUEST RECEIPT | smoke --local-built RECEIPT EXECUTABLE SCRATCH_PARENT".into()),
    }
    Ok(())
}
fn main() {
    if let Err(e) = run() {
        eprintln!("{e}");
        std::process::exit(1);
    }
}
