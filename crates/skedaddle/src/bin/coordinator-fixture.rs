//! Subprocess-only crash/rebuild driver, excluded from normal builds.
#[path = "../../tests/support/root.rs"]
mod test_root;

use object_store::local::LocalFileSystem;
use skedaddle::coordinator::{epoch::Coordinator, journal::Journal, local::Local, *};
use std::{path::PathBuf, sync::Arc, time::Duration};

#[tokio::main]
async fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 4 {
        return Err(Error::Rejected(
            "fixture requires operation, object root, local path, output/effect root".into(),
        ));
    }
    let object_root = PathBuf::from(&args[1]);
    let path = PathBuf::from(&args[2]);
    let output = PathBuf::from(&args[3]);
    for p in [&object_root, &path, &output] {
        #[cfg(windows)]
        if !p.starts_with("J:/gigglesdb-work") {
            return Err(Error::Rejected(
                "fixture paths must stay under J:/gigglesdb-work".into(),
            ));
        }
        #[cfg(not(windows))]
        test_root::assert_under_work_root(p);
    }
    let store = Arc::new(
        LocalFileSystem::new_with_prefix(object_root)?
            .with_automatic_cleanup(false)
            .with_fsync(true),
    );
    let j = Journal::new(store, "p2", "fixture", Duration::from_secs(3))?;
    if args[0] == "rebuild" {
        let local = Local::recover(path, &j).await?;
        std::fs::write(output, serde_json::to_vec(local.state())?)?;
        return Ok(());
    }
    let local = Local::create(path)?;
    let mut coordinator = Coordinator::observer(j, local, "child")?;
    coordinator.acquire().await?;
    if args[0].starts_with("effect-pg") {
        let cfg: serde_json::Value = serde_json::from_slice(&std::fs::read(output)?)?;
        let (mut client, connection, transport) = skedaddle::pg::tls::connect(
            &skedaddle::pg::tls::Config::new(
                cfg["pg"]
                    .as_str()
                    .ok_or_else(|| Error::Rejected("missing PG configuration".into()))?
                    .parse()?,
                cfg["ca_pem"]
                    .as_str()
                    .or_else(|| cfg["replication"]["ca_pem"].as_str())
                    .ok_or_else(|| Error::Rejected("missing TLS CA configuration".into()))?,
            ),
            &skedaddle::security::TlsPolicy::Required,
        )
        .await
        .map_err(|_| Error::Rejected("verified TLS connection failed".into()))?;
        assert!(transport.encrypted());
        tokio::spawn(async move {
            connection.await.expect("PG connection");
        });
        coordinator.set_crash_seam(if args[0] == "effect-pg-before" {
            CrashSeam::AfterLocalApply
        } else {
            CrashSeam::AfterEffect
        });
        coordinator
            .effect(
                EffectKind::Activation,
                "INSERT INTO business_effects VALUES (1)".into(),
                |token| async move { epoch::apply_pg_effect(&mut client, token).await },
            )
            .await?;
        return Err(Error::Rejected("PG crash seam was not reached".into()));
    }
    let seam = match args[0].as_str() {
        "put" => CrashSeam::AfterJournalPut,
        "local" => CrashSeam::AfterLocalApply,
        name if name.starts_with("effect") => CrashSeam::AfterEffect,
        _ => return Err(Error::Rejected("unknown crash operation".into())),
    };
    coordinator.set_crash_seam(seam);
    if seam == CrashSeam::AfterEffect {
        let kind = match args[0].as_str() {
            "effect-fence" => EffectKind::Fence,
            "effect-unfence" => EffectKind::Unfence,
            "effect-marker" => EffectKind::FinalMarker,
            "effect-activation" => EffectKind::Activation,
            "effect-rollback" => EffectKind::Rollback,
            _ => EffectKind::BufferPublication,
        };
        coordinator
            .effect(
                kind,
                "fixture publication".into(),
                move |token| async move {
                    use std::io::Write;
                    let mut file = std::fs::OpenOptions::new()
                        .create_new(true)
                        .write(true)
                        .open(output.join(format!("effect-{}-{}", token.epoch(), token.seq())))?;
                    file.write_all(token.payload().as_bytes())?;
                    file.sync_all()?;
                    Ok(())
                },
            )
            .await?;
    } else {
        coordinator
            .commit(Transition::Progress {
                stream: "s".into(),
                marker: "m".into(),
                position: "1".into(),
            })
            .await?;
    }
    Err(Error::Rejected(
        "requested crash seam was not reached".into(),
    ))
}
