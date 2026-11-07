//! Common JSONL envelopes and microsecond latency distributions.
use crate::kv::Result;
use hdrhistogram::Histogram;
use serde::Serialize;
use std::{fs::OpenOptions, io::Write, path::Path, time::Duration};

#[derive(Debug, Serialize)]
pub struct LatencySummary {
    pub count: u64,
    pub p50_us: u64,
    pub p90_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    pub max_us: u64,
}
pub struct Latencies {
    histogram: Histogram<u64>,
}
impl Default for Latencies {
    fn default() -> Self {
        Self {
            histogram: Histogram::new(3).expect("valid histogram precision"),
        }
    }
}
impl Latencies {
    pub fn merge(&mut self, other: &Self) -> Result<()> {
        self.histogram.add(&other.histogram)?;
        Ok(())
    }
    pub fn record(&mut self, elapsed: Duration) -> Result<()> {
        self.histogram
            .record(elapsed.as_micros().min(u64::MAX as u128) as u64)?;
        Ok(())
    }
    pub fn summary(&self) -> LatencySummary {
        LatencySummary {
            count: self.histogram.len(),
            p50_us: self.histogram.value_at_quantile(0.5),
            p90_us: self.histogram.value_at_quantile(0.9),
            p99_us: self.histogram.value_at_quantile(0.99),
            p999_us: self.histogram.value_at_quantile(0.999),
            max_us: self.histogram.max(),
        }
    }
}
#[derive(Debug, Serialize)]
pub struct Host {
    pub hostname: String,
    pub os: &'static str,
    pub architecture: &'static str,
    pub logical_cpus: usize,
    pub total_ram_bytes: Option<u64>,
}
impl Host {
    pub fn capture() -> Self {
        Self {
            hostname: std::env::var("COMPUTERNAME")
                .or_else(|_| std::env::var("HOSTNAME"))
                .unwrap_or_default(),
            os: std::env::consts::OS,
            architecture: std::env::consts::ARCH,
            logical_cpus: std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(1),
            total_ram_bytes: total_ram(),
        }
    }
}
#[derive(Debug, Serialize)]
pub struct Envelope<P, R> {
    pub engine: String,
    pub version: String,
    pub phase: String,
    pub parameters: P,
    pub results: R,
    pub host: Host,
}
pub fn append_jsonl(path: impl AsRef<Path>, record: &impl Serialize) -> Result<()> {
    let mut bytes = serde_json::to_vec(record)?;
    bytes.push(b'\n');
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    file.write_all(&bytes)?;
    file.flush()?;
    Ok(())
}
pub fn throughput(operations: u64, elapsed: Duration) -> f64 {
    if elapsed.is_zero() {
        0.0
    } else {
        operations as f64 / elapsed.as_secs_f64()
    }
}
pub fn rss_bytes() -> Option<usize> {
    memory_stats::memory_stats().map(|stats| stats.physical_mem)
}

#[cfg(windows)]
fn total_ram() -> Option<u64> {
    #[repr(C)]
    struct MemoryStatus {
        length: u32,
        load: u32,
        total_phys: u64,
        avail_phys: u64,
        total_page: u64,
        avail_page: u64,
        total_virtual: u64,
        avail_virtual: u64,
        avail_extended: u64,
    }
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GlobalMemoryStatusEx(status: *mut MemoryStatus) -> i32;
    }
    let mut status = MemoryStatus {
        length: std::mem::size_of::<MemoryStatus>() as u32,
        load: 0,
        total_phys: 0,
        avail_phys: 0,
        total_page: 0,
        avail_page: 0,
        total_virtual: 0,
        avail_virtual: 0,
        avail_extended: 0,
    };
    // SAFETY: status is an initialized, correctly sized C-layout structure and remains live for the call.
    if unsafe { GlobalMemoryStatusEx(&mut status) } != 0 {
        Some(status.total_phys)
    } else {
        None
    }
}
#[cfg(not(windows))]
fn total_ram() -> Option<u64> {
    std::fs::read_to_string("/proc/meminfo")
        .ok()?
        .lines()
        .find_map(|line| {
            let mut fields = line.split_whitespace();
            if fields.next()? == "MemTotal:" {
                fields.next()?.parse::<u64>().ok()?.checked_mul(1024)
            } else {
                None
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn histogram_reports_microseconds_and_counts() {
        let mut latencies = Latencies::default();
        for micros in 1..=1000 {
            latencies.record(Duration::from_micros(micros)).unwrap();
        }
        let summary = latencies.summary();
        assert_eq!(summary.count, 1000);
        assert!((499..=501).contains(&summary.p50_us));
        assert!((989..=991).contains(&summary.p99_us));
        assert!(summary.max_us >= 1000);
        assert_eq!(throughput(10, Duration::from_secs(2)), 5.0);
        assert_eq!(throughput(10, Duration::ZERO), 0.0);
        assert!(
            serde_json::to_value(summary)
                .unwrap()
                .get("p999_us")
                .is_some()
        );
    }
}
