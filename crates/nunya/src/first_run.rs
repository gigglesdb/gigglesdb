//! Startup-only creation of local state and first-run credentials.
use crate::{
    config::{Config, PolicyStore, key},
    pgwire::auth::ScramVerifier,
    private_file,
    state::{authority::Authority, model::Verifier, schema::STORE_FILE},
};
use std::{
    fs::{self, File},
    io::{self, Read, Write},
    path::Path,
    time::{Duration, Instant},
};
use zeroize::Zeroizing;

pub(crate) const DEFAULT_POLICY: &[u8] = b"version = 2\ndefault = \"tokenize\"\n\n# To mark a column, add a [[column]] rule with its table, name, and treatment.\n";
const ROOT_ERROR: &str = "root key: missing, unreadable or invalid credential";

fn root_env() -> Result<Zeroizing<String>, std::env::VarError> {
    std::env::var("NUNYA_ROOT_KEY").map(Zeroizing::new)
}

fn kubernetes(config: &Config) -> bool {
    config.kubernetes.is_some() || config.policy_store == PolicyStore::Kubernetes
}

/// Policy checking can validate configured credentials without needing a local root key.
pub(crate) fn check_explicit_root_key(config: &Config) -> Result<(), String> {
    let env = root_env();
    if !matches!(env, Err(std::env::VarError::NotPresent))
        || config.root_key_file.is_some()
        || kubernetes(config)
    {
        resolve_root_key(config, env, false)?;
    }
    Ok(())
}

pub(crate) fn load_root_key(config: &mut Config, create: bool) -> Result<(), String> {
    config.root_key = Some(resolve_root_key(config, root_env(), create)?);
    Ok(())
}

pub(crate) fn prepare_state_dir(path: &Path) -> Result<(), String> {
    private_file::create_dir_all(path)
        .map_err(|_| format!("cannot create state directory {}", path.display()))
}

fn resolve_root_key(
    config: &Config,
    env: Result<Zeroizing<String>, std::env::VarError>,
    create: bool,
) -> Result<Zeroizing<[u8; 32]>, String> {
    if let Some(path) = &config.root_key_file {
        return fs::read(path)
            .ok()
            .map(Zeroizing::new)
            .and_then(|bytes| key(&bytes))
            .ok_or_else(|| ROOT_ERROR.into());
    }
    match env {
        Ok(value) => return key(value.as_bytes()).ok_or_else(|| ROOT_ERROR.into()),
        Err(std::env::VarError::NotUnicode(_)) => return Err(ROOT_ERROR.into()),
        Err(std::env::VarError::NotPresent) => {}
    }
    if kubernetes(config) {
        return Err(ROOT_ERROR.into());
    }

    let path = config.state_dir.join("root.key");
    let read_error = || format!("cannot read root key file {}", path.display());
    match read_default_key(&path) {
        Ok(key) => return Ok(key),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(_) => return Err(read_error()),
    }
    if !create {
        return Err(format!("root key file {} is missing", path.display()));
    }
    match fs::symlink_metadata(config.state_dir.join(STORE_FILE)) {
        Ok(_) => {
            return match read_default_key(&path) {
                Ok(key) => Ok(key),
                Err(error) if error.kind() == io::ErrorKind::NotFound => Err(format!(
                    "root key file {} is missing but state in {} already exists; restore the key or set NUNYA_ROOT_KEY / root_key_file",
                    path.display(),
                    config.state_dir.display(),
                )),
                Err(_) => Err(read_error()),
            };
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(_) => {
            return Err(format!(
                "cannot inspect state in {}",
                config.state_dir.display()
            ));
        }
    }
    prepare_state_dir(&config.state_dir)?;
    let mut bytes = Zeroizing::new([0; 32]);
    getrandom::fill(bytes.as_mut()).map_err(|_| "randomness unavailable")?;
    let encoded = Zeroizing::new(data_encoding::HEXLOWER.encode(bytes.as_ref()));
    let write_error = || format!("cannot create root key at {}", path.display());
    match private_file::create(&path) {
        Ok(mut file) => {
            let line = Zeroizing::new(format!("{encoded}\n", encoded = encoded.as_str()));
            if file
                .lock()
                .and_then(|()| file.write_all(line.as_bytes()))
                .and_then(|()| file.sync_all())
                .is_err()
            {
                let _ = file.set_len(0);
                drop(file);
                let _ = fs::remove_file(&path);
                return Err(write_error());
            }
            eprintln!("generated root key at {}", path.display());
            Ok(bytes)
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            read_default_key(&path).map_err(|_| read_error())
        }
        Err(_) => Err(write_error()),
    }
}

fn read_default_key(path: &Path) -> io::Result<Zeroizing<[u8; 32]>> {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let mut file = File::open(path)?;
        file.lock_shared()?;
        let mut bytes = Zeroizing::new(Vec::new());
        file.read_to_end(&mut bytes)?;
        if !bytes.is_empty() || Instant::now() >= deadline {
            return key(&bytes).ok_or(io::ErrorKind::InvalidData.into());
        }
        // A reader can open a just-created file before its writer takes the exclusive lock.
        drop(file);
        std::thread::sleep(Duration::from_millis(10));
    }
}

pub(crate) fn create_default_policy(config: &Config) -> Result<(), String> {
    let routes = [
        (&config.postgres_policy, config.postgres_policy_is_default),
        (&config.spanner_policy, config.spanner_policy_is_default),
    ];
    let Some((Some(path), _)) = routes.iter().find(|(_, implicit)| *implicit) else {
        return Ok(());
    };
    if routes
        .iter()
        .any(|(policy, implicit)| !implicit && policy.as_ref() == Some(path))
    {
        // An explicit route can share the default filename, but it still requires the file.
        return if path.is_file() {
            Ok(())
        } else {
            Err(format!("policy file unavailable: {}", path.display()))
        };
    }
    match File::options().write(true).create_new(true).open(path) {
        Ok(mut file) => {
            if file
                .write_all(DEFAULT_POLICY)
                .and_then(|()| file.sync_all())
                .is_err()
            {
                drop(file);
                let _ = fs::remove_file(path);
                return Err(format!("cannot write default policy at {}", path.display()));
            }
            eprintln!("created default policy at {}", path.display());
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            // An interrupted create_new can leave an empty file, which would parse as
            // a legacy policy that shows unlisted columns.
            if !fs::metadata(path).is_ok_and(|metadata| metadata.is_file() && metadata.len() != 0) {
                return Err(format!(
                    "default policy file unavailable or empty: {}",
                    path.display()
                ));
            }
        }
        Err(_) => {
            return Err(format!(
                "cannot create default policy at {}",
                path.display()
            ));
        }
    }
    Ok(())
}

pub(crate) fn bootstrap_admin(config: &Config, authority: &Authority) -> Result<(), String> {
    if kubernetes(config)
        || config
            .management
            .as_ref()
            .is_none_or(|m| !m.admins.is_empty())
        || authority.principals().iter().any(|p| p.administrator)
    {
        return Ok(());
    }
    if authority.principal_by_name("operator").is_some() {
        eprintln!("skipped administrator bootstrap: principal operator already exists");
        return Ok(());
    }
    let path = config.state_dir.join("operator.secret");
    let mut password = Zeroizing::new([0; 32]);
    let mut salt = [0; 16];
    getrandom::fill(password.as_mut())
        .and_then(|()| getrandom::fill(&mut salt))
        .map_err(|_| "randomness unavailable")?;
    let secret = Zeroizing::new(data_encoding::HEXLOWER.encode(password.as_ref()));
    let (secret, message) = match private_file::create(&path) {
        Ok(mut file) => {
            let written = writeln!(file, "{}", secret.as_str()).and_then(|()| file.sync_all());
            if written.is_err() {
                drop(file);
                let _ = fs::remove_file(&path);
                return Err(format!(
                    "cannot write administrator secret at {}",
                    path.display()
                ));
            }
            (secret, "created administrator operator; secret in")
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => (
            read_operator_secret(&path)?,
            "created administrator operator from existing secret in",
        ),
        Err(error) => {
            return Err(format!(
                "cannot create administrator secret at {}: {error}",
                path.display()
            ));
        }
    };
    let verifier = Verifier(ScramVerifier::from_password(&secret, &salt, 4096));
    authority
        .bootstrap_operator(verifier)
        .map_err(|error| format!("cannot persist administrator operator: {error}"))?;
    eprintln!("{message} {}", path.display());
    Ok(())
}

pub(crate) fn read_operator_secret(path: &Path) -> Result<Zeroizing<String>, String> {
    let invalid = || {
        format!(
            "administrator secret in {} must be one nonempty UTF-8 line",
            path.display()
        )
    };
    let mut text = Zeroizing::new(String::new());
    File::open(path)
        .and_then(|mut file| file.read_to_string(&mut text))
        .map_err(|error| {
            if error.kind() == io::ErrorKind::InvalidData {
                invalid()
            } else {
                format!("cannot read administrator secret at {}: {error}", path.display())
            }
        })?;
    if text.ends_with('\n') {
        text.pop();
        if text.ends_with('\r') {
            text.pop();
        }
    }
    if text.is_empty() || text.contains(['\r', '\n']) {
        return Err(invalid());
    }
    Ok(text)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{path::PathBuf, sync::Barrier};

    struct Fixture(PathBuf);

    impl Fixture {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "nf7-key-{}",
                crate::token::SessionId::random().to_hex(),
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }

        fn config(&self) -> Config {
            let path = self.0.join("nunya.toml");
            fs::write(
                &path,
                "[spanner_native]\ndatabase='projects/p/instances/i/databases/d'\n",
            )
            .unwrap();
            Config::load(&path).unwrap()
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn explicit_credentials_take_precedence_and_never_fall_back() {
        let dir = Fixture::new();
        let mut config = dir.config();
        let env = || Ok(Zeroizing::new("12".repeat(32)));
        assert_eq!(*resolve_root_key(&config, env(), true).unwrap(), [0x12; 32]);
        let path = dir.0.join("explicit.key");
        config.root_key_file = Some(path.clone());
        assert_eq!(
            resolve_root_key(&config, env(), true).unwrap_err(),
            ROOT_ERROR,
        );
        fs::write(&path, [0x78; 32]).unwrap();
        assert_eq!(
            *resolve_root_key(&config, env(), true).unwrap(),
            [0x78; 32],
        );
        assert_eq!(
            *resolve_root_key(&config, Ok(Zeroizing::new("invalid".into())), true).unwrap(),
            [0x78; 32],
        );
        fs::write(&path, b"invalid").unwrap();
        assert_eq!(
            resolve_root_key(&config, env(), true).unwrap_err(),
            ROOT_ERROR,
        );
        assert!(!config.state_dir.exists());
    }

    #[test]
    fn default_policy_errors_name_the_file() {
        let dir = Fixture::new();
        let mut config = dir.config();
        let path = config.spanner_policy.clone().unwrap();
        config.postgres_policy = Some(path.clone());
        assert_eq!(
            create_default_policy(&config).unwrap_err(),
            format!("policy file unavailable: {}", path.display()),
        );
        config.postgres_policy = None;
        fs::write(&path, b"").unwrap();
        assert_eq!(
            create_default_policy(&config).unwrap_err(),
            format!("default policy file unavailable or empty: {}", path.display()),
        );
    }

    #[test]
    fn concurrent_first_starts_observe_one_key() {
        let dir = Fixture::new();
        let config = dir.config();
        let start = Barrier::new(2);
        let resolve = || {
            start.wait();
            resolve_root_key(&config, Err(std::env::VarError::NotPresent), true).unwrap()
        };
        let (first, second) = std::thread::scope(|s| {
            let first = s.spawn(resolve);
            let second = s.spawn(resolve);
            (first.join().unwrap(), second.join().unwrap())
        });
        assert_eq!(*first, *second);
        assert_eq!(
            *first,
            *read_default_key(&config.state_dir.join("root.key")).unwrap()
        );
    }

    #[test]
    fn invalid_existing_operator_secret_names_its_path() {
        let dir = Fixture::new();
        let path = dir.0.join("operator.secret");
        for content in [b"\r\n".as_slice(), b"one\ntwo\n", b"one\n\n", b"\xff"] {
            fs::write(&path, content).unwrap();
            assert_eq!(
                read_operator_secret(&path).unwrap_err(),
                format!("administrator secret in {} must be one nonempty UTF-8 line", path.display())
            );
        }
    }

    #[test]
    fn read_only_key_resolution_names_missing_default_path() {
        let dir = Fixture::new();
        let config = dir.config();
        let path = config.state_dir.join("root.key");
        assert_eq!(
            resolve_root_key(&config, Err(std::env::VarError::NotPresent), false).unwrap_err(),
            format!("root key file {} is missing", path.display()),
        );
        assert!(!config.state_dir.exists());
    }

    #[cfg(feature = "kubernetes")]
    #[test]
    fn kubernetes_requires_explicit_credentials() {
        let dir = Fixture::new();
        let mut config = dir.config();
        config.policy_store = PolicyStore::Kubernetes;
        assert_eq!(
            resolve_root_key(&config, Err(std::env::VarError::NotPresent), true).unwrap_err(),
            ROOT_ERROR,
        );
        assert!(!config.state_dir.exists());
        prepare_state_dir(&config.state_dir).unwrap();
        fs::write(config.state_dir.join("root.key"), "12".repeat(32)).unwrap();
        assert_eq!(
            resolve_root_key(&config, Err(std::env::VarError::NotPresent), true).unwrap_err(),
            ROOT_ERROR,
        );
        assert_eq!(
            *resolve_root_key(&config, Ok(Zeroizing::new("13".repeat(32))), true).unwrap(),
            [0x13; 32],
        );
    }
}
