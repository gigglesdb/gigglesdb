//! The frozen F0 qualification workload; all persistent state is recovered from MVCC records.
use crate::{
    commit::GroupCommit,
    keys,
    kv::{self, Backend, Result},
    report,
};
use serde::Serialize;
use serde_json::json;
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

#[derive(Debug, Serialize)]
pub struct Config {
    pub engine: String,
    pub dir: PathBuf,
    pub cache_mb: u64,
    pub value_bytes: usize,
    pub out: PathBuf,
    pub allow_any_dir: bool,
}
#[derive(Debug, Serialize)]
pub enum Phase {
    Load {
        dataset_gb: f64,
    },
    Reopen,
    Gc,
    Run {
        write_mix: bool,
        threads: usize,
        duration_secs: u64,
        warmup_secs: u64,
        group_max: usize,
        group_wait_us: u64,
        zipf_theta: Option<f64>,
        pin_snapshot_secs: Option<u64>,
    },
}

/// Resolve existing ancestors first, so junctions, symlinks and `..` cannot bypass the guard.
pub fn checked_directory(path: &Path, allow_any: bool) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut ancestor = absolute.as_path();
    let mut suffix = Vec::new();
    while !ancestor.exists() {
        suffix.push(
            ancestor
                .file_name()
                .ok_or("directory has no existing ancestor")?
                .to_owned(),
        );
        ancestor = ancestor
            .parent()
            .ok_or("directory has no existing ancestor")?;
    }
    let mut resolved = ancestor.canonicalize()?;
    for part in suffix.into_iter().rev() {
        resolved.push(part);
    }
    // Canonical Windows paths have a verbatim prefix; compare canonical roots, not spelling.
    if !allow_any {
        let root = Path::new("J:/").canonicalize().map_err(|e| {
            format!("J:/ is unavailable ({e}); use --allow-any-dir only intentionally")
        })?;
        if !resolved.starts_with(root) {
            return Err(format!(
                "refusing database directory {} outside J:/; pass --allow-any-dir to override",
                resolved.display()
            )
            .into());
        }
    }
    Ok(resolved)
}

fn user(pk: u64) -> [u8; 17] {
    keys::encode_user_key(0, pk as i64)
}
fn discover(db: &dyn Backend) -> Result<(u64, u64)> {
    let mut max_ts = 0;
    let mut next_pk = 0;
    for record in db.iter_from(&[])? {
        let (key, _) = record?;
        let (logical, ts) = keys::decode_key(&key)?;
        max_ts = max_ts.max(ts);
        if logical[..8] != 0u64.to_be_bytes() {
            return Err("unexpected table in benchmark database".into());
        }
        let pk = (u64::from_be_bytes(logical[9..].try_into()?) ^ (1 << 63)) as i64;
        if pk < 0 {
            return Err("negative primary key in benchmark database".into());
        }
        next_pk = next_pk.max((pk as u64).checked_add(1).ok_or("primary key exhausted")?);
    }
    Ok((max_ts, next_pk))
}

pub fn execute(mut config: Config, phase: Phase) -> Result<()> {
    config.dir = checked_directory(&config.dir, config.allow_any_dir)?;
    let cache_bytes = usize::try_from(
        config
            .cache_mb
            .checked_mul(1024 * 1024)
            .ok_or("cache size overflow")?,
    )?;
    let db = kv::open(&config.engine, &config.dir, cache_bytes)?;
    let before = db.disk_bytes()?;
    let (initial_ts, initial_keys) = if matches!(&phase, Phase::Reopen) {
        (0, 0)
    } else {
        discover(db.as_ref())?
    };
    let (name, results) = match &phase {
        Phase::Load { dataset_gb } => {
            if !dataset_gb.is_finite() || *dataset_gb <= 0.0 {
                return Err("--dataset-gb must be finite and positive".into());
            }
            if initial_keys != 0 {
                return Err("load requires an empty database directory".into());
            }
            let rows_f = (dataset_gb * 1e9 / (26.0 + config.value_bytes as f64)).floor();
            if rows_f < 1.0 || rows_f > i64::MAX as f64 {
                return Err("dataset size must produce between 1 and i64::MAX rows".into());
            }
            let rows = rows_f as u64;
            let mut order: Vec<u64> = (0..rows).collect();
            fastrand::Rng::with_seed(0x4744_0054).shuffle(&mut order);
            let mut payload_rng = fastrand::Rng::with_seed(0x0054_5041_594c_4f41);
            let started = Instant::now();
            let mut ts = 0;
            let mut batches = 0u64;
            for chunk in order.chunks(10_000) {
                let ops: Vec<_> = chunk
                    .iter()
                    .map(|&pk| {
                        ts += 1;
                        let mut payload = vec![0; config.value_bytes];
                        payload_rng.fill(&mut payload);
                        (
                            keys::encode_key(&user(pk), ts),
                            Some(keys::encode_value(Some(&payload))),
                        )
                    })
                    .collect();
                db.write_batch(&ops, false)?;
                batches += 1;
            }
            db.flush()?;
            let elapsed = started.elapsed();
            (
                "load",
                json!({"rows": rows,"load_batches":batches,"visible_ts":ts,"seconds":elapsed.as_secs_f64(),"rows_per_second":report::throughput(rows,elapsed),"logical_mb_per_second":(rows as f64*(26.0+config.value_bytes as f64)/1e6)/elapsed.as_secs_f64(),"disk_before_bytes":before,"disk_after_bytes":db.disk_bytes()?,"rss_bytes":report::rss_bytes()}),
            )
        }
        Phase::Reopen => {
            drop(db);
            let start = Instant::now();
            let reopened = kv::open(&config.engine, &config.dir, cache_bytes)?;
            let open_time = start.elapsed();
            let start = Instant::now();
            let found = keys::get_at(reopened.as_ref(), &user(0), u64::MAX)?.is_some();
            let first_get_time = start.elapsed();
            let (recovered_ts, recovered_keys) = discover(reopened.as_ref())?;
            (
                "reopen",
                json!({"open_ns":open_time.as_nanos(),"first_get_at_ns":first_get_time.as_nanos(),"first_get_found":found,"visible_ts":recovered_ts,"existing_keys":recovered_keys,"disk_before_bytes":before,"disk_after_bytes":reopened.disk_bytes()?}),
            )
        }
        Phase::Gc => {
            let start = Instant::now();
            let (scanned, removed) = gc_pass(db.as_ref(), initial_ts)?;
            let after_gc = db.disk_bytes()?;
            db.compact()?;
            (
                "gc",
                json!({"watermark":initial_ts,"scanned_versions":scanned,"deleted_versions":removed,"seconds":start.elapsed().as_secs_f64(),"disk_before_bytes":before,"disk_after_gc_bytes":after_gc,"disk_after_compact_bytes":db.disk_bytes()?,"compaction_exposed":db.supports_compaction()}),
            )
        }
        Phase::Run {
            write_mix,
            threads,
            duration_secs,
            warmup_secs,
            group_max,
            group_wait_us,
            zipf_theta,
            pin_snapshot_secs,
        } => {
            if *threads == 0 || *duration_secs == 0 || *group_max == 0 {
                return Err("threads, duration and group-max must be positive".into());
            }
            if initial_keys == 0 {
                return Err("run requires a loaded database".into());
            }
            if zipf_theta.is_some_and(|t| !t.is_finite() || t <= 0.0) {
                return Err("--zipf theta must be finite and positive".into());
            }
            let visible = Arc::new(AtomicU64::new(initial_ts));
            let next_pk = Arc::new(AtomicU64::new(initial_keys));
            let writer = GroupCommit::start(
                db.clone(),
                visible.clone(),
                *group_max,
                Duration::from_micros(*group_wait_us),
            )?;
            let handle = writer.handle();
            let settings = WorkerSettings {
                write_mix: *write_mix,
                threads: *threads,
                value_bytes: config.value_bytes,
                initial_keys,
                theta: *zipf_theta,
            };
            if *warmup_secs > 0 {
                run_workers(
                    &db,
                    &visible,
                    &next_pk,
                    &handle,
                    &settings,
                    Duration::from_secs(*warmup_secs),
                )?;
            }
            handle.reset_stats()?;
            let disk_start = db.disk_bytes()?;
            let pin = if let Some(seconds) = pin_snapshot_secs {
                let held = db
                    .pin_snapshot()?
                    .ok_or("engine does not expose a pinnable snapshot")?;
                let pinned_db = db.clone();
                let seconds = *seconds;
                Some(thread::spawn(move || -> Result<_> {
                    let before = pinned_db.disk_bytes()?;
                    let rss_before = report::rss_bytes();
                    thread::sleep(Duration::from_secs(seconds));
                    let held_bytes = pinned_db.disk_bytes()?;
                    drop(held);
                    Ok(
                        json!({"seconds":seconds,"disk_before_bytes":before,"disk_held_bytes":held_bytes,"disk_after_release_bytes":pinned_db.disk_bytes()?,"rss_before_bytes":rss_before,"rss_after_release_bytes":report::rss_bytes()}),
                    )
                }))
            } else {
                None
            };
            let sampling = Arc::new(AtomicBool::new(true));
            let sampler_flag = sampling.clone();
            let sampler = thread::spawn(move || {
                let mut samples = Vec::new();
                while sampler_flag.load(Ordering::Relaxed) {
                    samples.push(report::rss_bytes());
                    // Check stop promptly while retaining one-second sampling cadence.
                    for _ in 0..10 {
                        thread::sleep(Duration::from_millis(100));
                        if !sampler_flag.load(Ordering::Relaxed) {
                            break;
                        }
                    }
                }
                samples
            });
            let start = Instant::now();
            let outcome = run_workers(
                &db,
                &visible,
                &next_pk,
                &handle,
                &settings,
                Duration::from_secs(*duration_secs),
            );
            let elapsed = start.elapsed();
            sampling.store(false, Ordering::Relaxed);
            let rss = sampler.join().map_err(|_| "RSS sampler panicked")?;
            let disk_end = db.disk_bytes()?;
            let metrics = outcome?;
            let groups = writer.shutdown()?;
            let pinned = pin
                .map(|p| p.join().map_err(|_| "snapshot pin thread panicked")?)
                .transpose()?;
            (
                "run",
                json!({"seconds":elapsed.as_secs_f64(),"operations":metrics.operations,"operations_per_second":report::throughput(metrics.operations,elapsed),"point_reads":metrics.points,"point_reads_per_second":report::throughput(metrics.points,elapsed),"scans":metrics.scans,"scans_per_second":report::throughput(metrics.scans,elapsed),"scan_rows":metrics.scan_rows,"committed_transactions":metrics.writes,"committed_rows":metrics.writes*4,"transactions_per_second":report::throughput(metrics.writes,elapsed),"rows_per_second":report::throughput(metrics.writes*4,elapsed),"point_latency":metrics.point_lat.summary(),"scan_latency":metrics.scan_lat.summary(),"write_latency":metrics.write_lat.summary(),"group_commit":groups,"visible_ts":visible.load(Ordering::Acquire),"next_pk":next_pk.load(Ordering::Relaxed),"disk_before_bytes":disk_start,"disk_after_bytes":disk_end,"rss_peak_bytes":rss.iter().flatten().copied().max(),"rss_samples_bytes":rss,"pin_snapshot":pinned}),
            )
        }
    };
    let output = report::Envelope {
        engine: config.engine.clone(),
        version: kv::engine_version(&config.engine).to_owned(),
        phase: name.to_owned(),
        parameters: json!({"common":config,"phase":phase}),
        results,
        host: report::Host::capture(),
    };
    report::append_jsonl(&config.out, &output)?;
    println!("{}", serde_json::to_string(&output)?);
    Ok(())
}

/// One streaming GC pass; the caller chooses a watermark safe for its snapshot policy.
pub fn gc_pass(db: &dyn Backend, watermark: u64) -> Result<(u64, u64)> {
    let mut logical = None;
    let mut resolved = false;
    let mut deletes = Vec::with_capacity(10_000);
    let mut removed = 0u64;
    let mut scanned = 0u64;
    for record in db.iter_from(&[])? {
        let (key, value) = record?;
        scanned += 1;
        let (this_user, ts) = keys::decode_key(&key)?;
        if logical != Some(this_user) {
            logical = Some(this_user);
            resolved = false;
        }
        if ts > watermark {
            continue;
        }
        let remove = resolved || value.first() == Some(&1);
        resolved = true;
        if remove {
            deletes.push((key, None));
            removed += 1;
        }
        if deletes.len() == 10_000 {
            db.write_batch(&deletes, false)?;
            deletes.clear();
        }
    }
    if !deletes.is_empty() {
        db.write_batch(&deletes, false)?;
    }
    db.flush()?;
    Ok((scanned, removed))
}

struct WorkerSettings {
    write_mix: bool,
    threads: usize,
    value_bytes: usize,
    initial_keys: u64,
    theta: Option<f64>,
}
#[derive(Default)]
struct Metrics {
    operations: u64,
    points: u64,
    scans: u64,
    scan_rows: u64,
    writes: u64,
    point_lat: report::Latencies,
    scan_lat: report::Latencies,
    write_lat: report::Latencies,
}

fn run_workers(
    db: &Arc<dyn Backend>,
    visible: &Arc<AtomicU64>,
    next_pk: &Arc<AtomicU64>,
    handle: &crate::commit::CommitHandle,
    settings: &WorkerSettings,
    duration: Duration,
) -> Result<Metrics> {
    let deadline = Instant::now() + duration;
    thread::scope(|scope| -> Result<Metrics> {
        let mut workers = Vec::new();
        for worker in 0..settings.threads {
            let handle = handle.clone();
            workers.push(scope.spawn(move || -> Result<Metrics> {
                let mut rng = fastrand::Rng::with_seed(0x4744_0054 + worker as u64);
                let mut metrics = Metrics::default();
                while Instant::now() < deadline {
                    let choice = rng.u8(0..100);
                    let point = if settings.write_mix {
                        choice < 20
                    } else {
                        choice < 90
                    };
                    let scan = !settings.write_mix && (90..95).contains(&choice);
                    let start = Instant::now();
                    if point {
                        let pk = select_key(&mut rng, settings.initial_keys, settings.theta);
                        keys::get_at(db.as_ref(), &user(pk), visible.load(Ordering::Acquire))?;
                        metrics.points += 1;
                        metrics.point_lat.record(start.elapsed())?;
                    } else if scan {
                        let pk = select_key(&mut rng, settings.initial_keys, settings.theta);
                        metrics.scan_rows += keys::scan_at(
                            db.as_ref(),
                            &user(pk),
                            100,
                            visible.load(Ordering::Acquire),
                        )?
                        .len() as u64;
                        metrics.scans += 1;
                        metrics.scan_lat.record(start.elapsed())?;
                    } else {
                        let mut mutations = Vec::with_capacity(4);
                        for _ in 0..4 {
                            let pk = if settings.write_mix && rng.u8(0..100) >= 90 {
                                let pk = next_pk.fetch_add(1, Ordering::Relaxed);
                                if pk > i64::MAX as u64 {
                                    return Err("primary key space exhausted".into());
                                }
                                pk
                            } else {
                                select_key(&mut rng, settings.initial_keys, settings.theta)
                            };
                            let mut payload = vec![0; settings.value_bytes];
                            rng.fill(&mut payload);
                            mutations.push((user(pk).to_vec(), Some(payload)));
                        }
                        let ack = handle.submit(mutations)?;
                        metrics.writes += 1;
                        metrics.write_lat.record(ack.latency)?;
                    }
                    metrics.operations += 1;
                }
                Ok(metrics)
            }));
        }
        let mut total = Metrics::default();
        for worker in workers {
            let result = worker.join().map_err(|_| "workload worker panicked")??;
            total.operations += result.operations;
            total.points += result.points;
            total.scans += result.scans;
            total.scan_rows += result.scan_rows;
            total.writes += result.writes;
            total.point_lat.merge(&result.point_lat)?;
            total.scan_lat.merge(&result.scan_lat)?;
            total.write_lat.merge(&result.write_lat)?;
        }
        Ok(total)
    })
}

/// Rejection-inversion sampling gives a discrete truncated Zipf distribution without an N-sized table.
fn select_key(rng: &mut fastrand::Rng, count: u64, theta: Option<f64>) -> u64 {
    let Some(theta) = theta else {
        return rng.u64(0..count);
    };
    let integral = |x: f64| {
        if (theta - 1.0).abs() < 1e-10 {
            x.ln()
        } else {
            ((1.0 - theta) * x.ln()).exp_m1() / (1.0 - theta)
        }
    };
    let inverse = |x: f64| {
        if (theta - 1.0).abs() < 1e-10 {
            x.exp()
        } else {
            (((1.0 - theta) * x).ln_1p() / (1.0 - theta)).exp()
        }
    };
    let lo = integral(1.5) - 1.0;
    let hi = integral(count as f64 + 0.5);
    let squeeze = 2.0 - inverse(integral(2.5) - 2f64.powf(-theta));
    loop {
        let u = hi + rng.f64() * (lo - hi);
        let x = inverse(u);
        let k = (x + 0.5).floor().clamp(1.0, count as f64);
        if k - x <= squeeze || u >= integral(k + 0.5) - k.powf(-theta) {
            return k as u64 - 1;
        }
    }
}
