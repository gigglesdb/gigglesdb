//! Generation-scoped requests only: the live runtime retains GC authority.
use super::verification::{VerifyResponse, active, write_json};
use crate::{coordinator, engine::Runtime};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

#[derive(Serialize, Deserialize)]
struct Request {
    generation: String,
    request: String,
    #[serde(default)]
    drop_objects: bool,
    #[serde(default)]
    deadline_ms: Option<u64>,
}
fn failed(kind: &str, exit_code: u8) -> VerifyResponse {
    VerifyResponse {
        exit_code,
        data: json!({"error":{"kind":kind,"message":"Owner request could not complete safely; inspect owner status."}}),
    }
}
/// This observer never acquires migration authority or opens the buffer store.
pub async fn request_gc(root: &Path, id: &str) -> VerifyResponse {
    request(root, id, "gc").await
}

pub(super) async fn request(root: &Path, id: &str, action: &str) -> VerifyResponse {
    request_options(root, id, action, false).await
}

pub(super) async fn request_options(
    root: &Path,
    id: &str,
    action: &str,
    drop_objects: bool,
) -> VerifyResponse {
    request_with_deadline(root, id, action, drop_objects, None).await
}

pub(super) async fn request_with_deadline(
    root: &Path,
    id: &str,
    action: &str,
    drop_objects: bool,
    deadline_ms: Option<u64>,
) -> VerifyResponse {
    if id.is_empty()
        || id.len() > 128
        || !id
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
    {
        return failed("invalid_request", 3);
    }
    let control = root.join(format!("{id}.control"));
    let generation = match active(&control) {
        Ok(Some(generation)) => generation,
        Ok(None) => return failed("no_owner", 6),
        Err(_) => return failed("infrastructure", 6),
    };
    let request = Request {
        deadline_ms,
        drop_objects,
        generation: generation.clone(),
        request: uuid::Uuid::new_v4().to_string(),
    };
    let path = control.join(format!("{action}-{generation}-{}.request", request.request));
    if write_json(&path, &request).is_err() {
        return failed("infrastructure", 6);
    }
    let start = Instant::now();
    let response = loop {
        match std::fs::read(path.with_extension("response")) {
            Ok(bytes) => {
                break serde_json::from_slice(&bytes)
                    .unwrap_or_else(|_| failed("infrastructure", 6));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => break failed("infrastructure", 6),
        }
        match active(&control) {
            Ok(Some(current)) if current == generation => {}
            Ok(Some(_)) => break failed("stale_generation", 8),
            Ok(None) => break failed("owner_stopped", 6),
            Err(_) => break failed("infrastructure", 6),
        }
        if start.elapsed() >= Duration::from_secs(120) {
            break failed("infrastructure", 6);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("response"));
    response
}

pub(super) async fn serve(
    runtime: Runtime,
    root: PathBuf,
    joined: tokio::sync::watch::Receiver<bool>,
) -> coordinator::Result<()> {
    // Pin once to this owner lifetime; a replacement generation is never adopted.
    let generation = loop {
        if *joined.borrow() {
            return Ok(());
        }
        if let Some(generation) = active(&root)? {
            break generation;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    let mut handled = BTreeSet::new();
    loop {
        if active(&root)? != Some(generation.clone()) {
            return Ok(());
        }
        for entry in std::fs::read_dir(&root)? {
            let path = entry?.path();
            let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
                continue;
            };
            if !name.starts_with("gc-") || !name.ends_with(".request") || handled.contains(name) {
                continue;
            }
            let bytes = match std::fs::read(&path) {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            let response = match serde_json::from_slice::<Request>(&bytes) {
                Ok(request) if request.generation != generation => failed("stale_generation", 8),
                Ok(request) if name == format!("gc-{generation}-{}.request", request.request) => {
                    match runtime.collect_buffer_once().await {
                        Ok(data) => VerifyResponse { exit_code: 0, data },
                        Err(coordinator::Error::LostOwnership | coordinator::Error::Observer) => {
                            failed("lost_ownership", 8)
                        }
                        Err(_) => failed("infrastructure", 6),
                    }
                }
                _ => failed("invalid_request", 3),
            };
            handled.insert(name.to_owned());
            write_json(&path.with_extension("response"), &response)?;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn no_owner_and_invalid_request_are_typed() {
        let root = PathBuf::from(std::env::var_os("SKEDADDLE_TEST_ROOT").unwrap())
            .join(uuid::Uuid::new_v4().to_string());
        let response = request_gc(&root, "migration").await;
        assert_eq!(response.exit_code, 6);
        assert_eq!(response.data["error"]["kind"], "no_owner");
        assert!(!root.exists());
        assert_eq!(request_gc(&root, "../invalid").await.exit_code, 3);
    }
}

#[cfg(test)]
mod request_tests {
    use super::*;
    #[tokio::test]
    async fn live_request_response_and_owner_generation_changes() {
        for action in ["response", "stopped", "replacement"] {
            let root = PathBuf::from(std::env::var_os("SKEDADDLE_TEST_ROOT").unwrap())
                .join(uuid::Uuid::new_v4().to_string());
            let control = root.join("migration.control");
            std::fs::create_dir_all(&control).unwrap();
            let owner = std::fs::OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(control.join("owner.lock"))
                .unwrap();
            owner.lock().unwrap();
            let generation = uuid::Uuid::new_v4().to_string();
            std::fs::write(control.join("owner-generation"), &generation).unwrap();
            let controller = async {
                let path = loop {
                    let path = std::fs::read_dir(&control)
                        .unwrap()
                        .map(|e| e.unwrap().path())
                        .find(|p| p.extension().is_some_and(|e| e == "request"));
                    if let Some(path) = path {
                        break path;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                };
                let request: Request =
                    serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
                assert_eq!(request.generation, generation);
                match action {
                    "response" => write_json(&path.with_extension("response"), &VerifyResponse { exit_code: 0, data: json!({"deleted_objects":2,"deleted_bytes":123,"retained_mseq":null}) }).unwrap(),
                    "stopped" => owner.unlock().unwrap(),
                    _ => std::fs::write(control.join("owner-generation"), uuid::Uuid::new_v4().to_string()).unwrap(),
                }
            };
            let (response, ()) = tokio::time::timeout(Duration::from_secs(5), async {
                tokio::join!(request_gc(&root, "migration"), controller)
            })
            .await
            .unwrap();
            match action {
                "response" => {
                    assert_eq!(response.exit_code, 0);
                    assert_eq!(response.data["deleted_objects"], 2);
                }
                "stopped" => {
                    assert_eq!(response.exit_code, 6);
                    assert_eq!(response.data["error"]["kind"], "owner_stopped");
                }
                _ => {
                    assert_eq!(response.exit_code, 8);
                    assert_eq!(response.data["error"]["kind"], "stale_generation");
                }
            }
            drop(owner);
            std::fs::remove_dir_all(root).unwrap();
        }
    }
}
