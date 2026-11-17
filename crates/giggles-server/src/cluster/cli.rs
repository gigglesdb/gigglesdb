//! `giggles-server cluster status <config.json> [--json]` (GD-0075 R5-64): asks every member
//! for its node report and prints one line per range and one per node.
use giggles_replication::call_codec::{NodeReport, RangeReport, RangeRole};
use serde::Serialize;
use std::{collections::BTreeMap, fmt::Write, time::Duration};

pub const USAGE: &str = "usage: giggles-server cluster status <config.json> [--json] | cluster enrol <config.json> <id> <address> <fingerprint>";
/// How long each member gets to answer.
pub const TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, PartialEq, Eq)]
pub struct Command {
    pub config: String,
    pub json: bool,
    pub enrol: Option<(u64, String, String)>,
}

impl Command {
    pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, &'static str> {
        let mut args = args.into_iter();
        let op = args.next().ok_or(USAGE)?;
        if op != "status" && op != "enrol" {
            return Err(USAGE);
        }
        let config = args
            .next()
            .filter(|s| !s.is_empty() && !s.starts_with('-'))
            .ok_or(USAGE)?;
        if op == "enrol" {
            let id: u64 = args
                .next()
                .filter(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
                .and_then(|s| s.parse().ok())
                .filter(|&id| id != 0)
                .ok_or(USAGE)?;
            let address = args
                .next()
                .filter(|s| s.parse::<std::net::SocketAddr>().is_ok())
                .ok_or(USAGE)?;
            let fingerprint = args
                .next()
                .filter(|s| s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit()))
                .ok_or(USAGE)?;
            if args.next().is_some() {
                return Err(USAGE);
            }
            return Ok(Self {
                config,
                json: false,
                enrol: Some((id, address, fingerprint.to_ascii_lowercase())),
            });
        }
        let json = match args.next().as_deref() {
            None => false,
            Some("--json") => true,
            Some(_) => return Err(USAGE),
        };
        if args.next().is_some() {
            return Err(USAGE);
        }
        Ok(Self {
            config,
            json,
            enrol: None,
        })
    }
}

/// One range: bounds as full hex (`end` None = unbounded), the leader the replicas report.
#[derive(Debug, Serialize)]
pub struct RangeLine {
    pub id: u64,
    pub epoch: u64,
    pub start: String,
    pub end: Option<String>,
    pub replicas: Vec<u64>,
    pub leader: Option<u64>,
    /// Leader's size, else the largest replica's; None when unknown.
    pub size: Option<u64>,
    /// Calls per second its leader served over the last minute.
    pub calls_per_sec: Option<f64>,
}

/// One member. Shares are percentages; None when nothing is known to share.
#[derive(Debug, Serialize)]
pub struct NodeLine {
    pub node: u64,
    pub reachable: bool,
    pub error: Option<String>,
    pub ranges_led: usize,
    pub ranges_replicated: usize,
    pub bytes: Option<u64>,
    pub data_share: Option<f64>,
    pub calls_total: u64,
    pub calls_per_sec: f64,
    pub load_share: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct Summary {
    pub ranges: Vec<RangeLine>,
    pub nodes: Vec<NodeLine>,
}

impl Summary {
    /// 0 when at least one member answered, else 1.
    pub fn exit_code(&self) -> i32 {
        if self.nodes.iter().any(|n| n.reachable) {
            0
        } else {
            1
        }
    }
}

fn share(part: f64, whole: f64) -> Option<f64> {
    (whole > 0.0).then(|| part * 100.0 / whole)
}

/// Joins the members' answers (in member order) into range and node lines.
pub fn summarize(answers: &[(u64, Result<NodeReport, String>)]) -> Summary {
    let mut by_range: BTreeMap<u64, Vec<(u64, &RangeReport)>> = BTreeMap::new();
    for (node, answer) in answers {
        for range in answer.iter().flat_map(|r| &r.ranges) {
            by_range
                .entry(range.descriptor.id.0)
                .or_default()
                .push((*node, range));
        }
    }
    let ranges: Vec<RangeLine> = by_range
        .iter()
        .map(|(&id, replicas)| {
            let newest = replicas
                .iter()
                .map(|(_, r)| &r.descriptor)
                .max_by_key(|d| d.epoch.0)
                .expect("a range listed by some replica");
            let leader = replicas
                .iter()
                .filter(|(_, r)| r.role == RangeRole::Leader)
                .max_by_key(|(_, r)| r.term);
            let size = leader
                .and_then(|(_, r)| r.size)
                .or_else(|| replicas.iter().filter_map(|(_, r)| r.size).max());
            RangeLine {
                id,
                epoch: newest.epoch.0,
                start: hex::encode(&newest.start),
                end: newest.end.as_ref().map(hex::encode),
                replicas: newest.replicas.iter().map(|r| r.node).collect(),
                leader: leader.map(|(node, _)| *node),
                size,
                calls_per_sec: leader.map(|(_, r)| r.calls_per_sec()),
            }
        })
        .collect();
    let mut nodes: Vec<NodeLine> = answers
        .iter()
        .map(|(node, answer)| {
            let own = answer.as_ref().map(|r| r.ranges.as_slice()).unwrap_or(&[]);
            let sizes: Vec<u64> = own.iter().filter_map(|r| r.size).collect();
            NodeLine {
                node: *node,
                reachable: answer.is_ok(),
                error: answer.as_ref().err().cloned(),
                ranges_led: ranges.iter().filter(|r| r.leader == Some(*node)).count(),
                ranges_replicated: own.len(),
                bytes: (!sizes.is_empty()).then(|| sizes.iter().sum()),
                data_share: None,
                calls_total: own.iter().map(|r| r.calls_total()).sum(),
                calls_per_sec: own.iter().map(|r| r.calls_per_sec()).sum(),
                load_share: None,
            }
        })
        .collect();
    let bytes: u64 = nodes.iter().filter_map(|n| n.bytes).sum();
    let rate: f64 = nodes.iter().map(|n| n.calls_per_sec).sum();
    for node in &mut nodes {
        node.data_share = node.bytes.and_then(|b| share(b as f64, bytes as f64));
        node.load_share = share(node.calls_per_sec, rate).filter(|_| node.reachable);
    }
    Summary { ranges, nodes }
}

/// Hex of a range key, cut to 12 digits; empty start and missing end print as min and max.
fn short(key: Option<&str>, unbounded: &str) -> String {
    match key {
        Some(k) if !k.is_empty() && k.len() > 12 => format!("{}..", &k[..12]),
        Some(k) if !k.is_empty() => k.to_string(),
        _ => unbounded.to_string(),
    }
}

fn size(bytes: Option<u64>) -> String {
    let Some(n) = bytes else {
        return "?".into();
    };
    let units = ["B", "KiB", "MiB", "GiB", "TiB"];
    let i = (0..units.len())
        .rev()
        .find(|&i| n >= 1 << (10 * i))
        .unwrap_or(0);
    if i == 0 {
        format!("{n}B")
    } else {
        format!("{:.1}{}", n as f64 / (1u64 << (10 * i)) as f64, units[i])
    }
}

fn percent(v: Option<f64>) -> String {
    v.map_or("-".into(), |v| format!("{v:.1}%"))
}

/// The text form: a range table, then a node table.
pub fn text(summary: &Summary) -> String {
    let mut out = String::new();
    let line = |out: &mut String, cells: [&str; 7]| {
        let _ = writeln!(
            out,
            "{:<6} {:<6} {:<34} {:<12} {:<7} {:<9} {}",
            cells[0], cells[1], cells[2], cells[3], cells[4], cells[5], cells[6]
        );
    };
    line(
        &mut out,
        [
            "RANGE", "EPOCH", "BOUNDS", "REPLICAS", "LEADER", "SIZE", "CALLS/S",
        ],
    );
    for r in &summary.ranges {
        let bounds = format!(
            "[{}, {})",
            short(Some(&r.start), "min"),
            short(r.end.as_deref(), "max")
        );
        let replicas: Vec<String> = r.replicas.iter().map(u64::to_string).collect();
        line(
            &mut out,
            [
                &r.id.to_string(),
                &r.epoch.to_string(),
                &bounds,
                &replicas.join(","),
                &r.leader.map_or("none".into(), |n| n.to_string()),
                &size(r.size),
                &r.calls_per_sec.map_or("-".into(), |v| format!("{v:.1}")),
            ],
        );
    }
    out.push_str("\nNODE   STATUS       LED   REPLICAS  DATA    LOAD\n");
    for n in &summary.nodes {
        if let Some(error) = &n.error {
            let _ = writeln!(out, "{:<6} unreachable  ({error})", n.node);
            continue;
        }
        let _ = writeln!(
            out,
            "{:<6} {:<12} {:<5} {:<9} {:<7} {}",
            n.node,
            "reachable",
            n.ranges_led,
            n.ranges_replicated,
            percent(n.data_share),
            percent(n.load_share)
        );
    }
    out.truncate(out.trim_end().len());
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use giggles_core::range_api::{Epoch, RangeDescriptor, RangeId, ReplicaDesc, ReplicaRole};
    use giggles_replication::call_codec::CallCounts;

    fn range(id: u64, end: Option<Vec<u8>>, role: RangeRole, size: Option<u64>) -> RangeReport {
        RangeReport {
            move_step: None,
            descriptor: RangeDescriptor {
                id: RangeId(id),
                epoch: Epoch(1),
                start: if id == 1 { vec![] } else { vec![0x80; 9] },
                end,
                replicas: [1, 2]
                    .map(|node| ReplicaDesc {
                        node,
                        role: ReplicaRole::Voter,
                    })
                    .to_vec(),
            },
            role,
            term: 3,
            leader: 0,
            applied: 9,
            size,
            calls: CallCounts {
                reads: 10,
                ..CallCounts::default()
            },
            recent: if role == RangeRole::Leader { 60 } else { 0 },
            window_ms: 60_000,
        }
    }

    #[test]
    fn parses_enrol() {
        let parse = |s: &str| Command::parse(s.split_whitespace().map(str::to_owned));
        assert!(
            parse(&format!("enrol cfg 4 127.0.0.1:1234 {}", "ab".repeat(32)))
                .unwrap()
                .enrol
                .is_some()
        );
        for bad in [
            "enrol cfg 0 addr fp",
            "enrol cfg 4 127.0.0.1:12 bad",
            "enrol cfg 4",
            "enrol",
        ] {
            assert!(parse(bad).is_err());
        }
    }

    #[test]
    fn parses_status_and_json() {
        let parse = |s: &str| Command::parse(s.split_whitespace().map(str::to_owned));
        assert_eq!(
            parse("status cfg.json").unwrap(),
            Command {
                config: "cfg.json".into(),
                json: false,
                enrol: None
            }
        );
        assert!(parse("status cfg.json --json").unwrap().json);
        for bad in [
            "",
            "status",
            "status --json",
            "stat cfg",
            "status cfg --x",
            "status cfg --json x",
        ] {
            assert!(parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn prints_ranges_and_nodes_with_unreachable_members() {
        let split = vec![0x80; 9];
        let answers = vec![
            (
                1,
                Ok(NodeReport {
                    node: 1,
                    ranges: vec![
                        range(1, Some(split.clone()), RangeRole::Leader, Some(3 << 20)),
                        range(2, None, RangeRole::Follower, Some(1 << 20)),
                    ],
                }),
            ),
            (
                2,
                Ok(NodeReport {
                    node: 2,
                    ranges: vec![
                        range(1, Some(split.clone()), RangeRole::Follower, Some(3 << 20)),
                        range(2, None, RangeRole::Candidate, Some(2048)),
                    ],
                }),
            ),
            (3, Err("timed out".to_string())),
        ];
        let summary = summarize(&answers);
        assert_eq!(summary.exit_code(), 0);
        assert_eq!(
            text(&summary),
            "\
RANGE  EPOCH  BOUNDS                             REPLICAS     LEADER  SIZE      CALLS/S
1      1      [min, 808080808080..)              1,2          1       3.0MiB    1.0
2      1      [808080808080.., max)              1,2          none    1.0MiB    -

NODE   STATUS       LED   REPLICAS  DATA    LOAD
1      reachable    1     2         57.1%   100.0%
2      reachable    0     2         42.9%   0.0%
3      unreachable  (timed out)"
        );
        let json = serde_json::to_value(&summary).unwrap();
        assert_eq!(json["ranges"][0]["end"], "808080808080808080");
        assert_eq!(json["ranges"][1]["leader"], serde_json::Value::Null);
        assert_eq!(json["nodes"][2]["reachable"], false);
        assert_eq!(json["nodes"][0]["calls_total"], 20);
        let none = summarize(&[(1, Err("refused".into()))]);
        assert_eq!(none.exit_code(), 1);
        assert!(none.ranges.is_empty() && none.nodes[0].load_share.is_none());
    }
}
