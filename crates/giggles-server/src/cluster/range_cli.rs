//! Operator commands for resumable range moves and membership status.
use giggles_core::range_api::{RangeId, ReplicaRole};
use giggles_replication::{
    call_codec::{MoveReply, NodeReport, RangeReport, RangeRole},
    cluster_node::{query_status, replace_node, request_move},
    transport::TransportConfig,
};
use std::{collections::BTreeMap, fmt::Write, time::Duration};

pub const USAGE: &str = "usage: giggles-server range move <config.json> <range> <from> <to> [--wait] | range replace-node <config.json> <lost> <new> [--timeout <s>] | range status <config.json> [<range>]";

#[derive(Debug, PartialEq, Eq)]
pub enum Action {
    Replace {
        lost: u64,
        new: u64,
        timeout: Duration,
    },
    Move {
        range: RangeId,
        from: u64,
        to: u64,
        wait: bool,
    },
    Status {
        range: Option<RangeId>,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub struct Command {
    pub config: String,
    pub action: Action,
}

impl Command {
    pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, &'static str> {
        let args: Vec<_> = args.into_iter().collect();
        let id = |s: &str| -> Result<u64, &'static str> {
            if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
                return Err(USAGE);
            }
            s.parse().ok().filter(|&n| n != 0).ok_or(USAGE)
        };
        let action = match args.as_slice() {
            [op, _, lost, new, rest @ ..] if op == "replace-node" => {
                let seconds = match rest {
                    [] => 60,
                    [flag, seconds] if flag == "--timeout" => id(seconds)?.min(86400),
                    _ => return Err(USAGE),
                };
                let (lost, new) = (id(lost)?, id(new)?);
                if lost == new {
                    return Err(USAGE);
                }
                Action::Replace {
                    lost,
                    new,
                    timeout: Duration::from_secs(seconds),
                }
            }
            [op, _, range, from, to, rest @ ..] if op == "move" => {
                let wait = match rest {
                    [] => false,
                    [flag] if flag == "--wait" => true,
                    _ => return Err(USAGE),
                };
                Action::Move {
                    range: RangeId(id(range)?),
                    from: id(from)?,
                    to: id(to)?,
                    wait,
                }
            }
            [op, _] if op == "status" => Action::Status { range: None },
            [op, _, range] if op == "status" => Action::Status {
                range: Some(RangeId(id(range)?)),
            },
            _ => return Err(USAGE),
        };
        let config = &args[1];
        if config.is_empty() || config.starts_with('-') {
            return Err(USAGE);
        }
        Ok(Self {
            config: config.clone(),
            action,
        })
    }
}

/// Pick the newest membership, preferring its leader's report for move progress.
pub fn status_text(answers: &[(u64, Result<NodeReport, String>)], only: Option<RangeId>) -> String {
    let mut ranges: BTreeMap<u64, &RangeReport> = BTreeMap::new();
    for (_, answer) in answers {
        for report in answer.iter().flat_map(|r| &r.ranges) {
            let id = report.descriptor.id;
            if only.is_some_and(|wanted| wanted != id) {
                continue;
            }
            let rank =
                |r: &RangeReport| (r.descriptor.epoch.0, r.term, r.role == RangeRole::Leader);
            let current = ranges.entry(id.0).or_insert(report);
            if rank(report) > rank(current) {
                *current = report;
            }
        }
    }
    let mut out = String::new();
    for (id, r) in ranges {
        let nodes = |role| {
            r.descriptor
                .replicas
                .iter()
                .filter(|replica| replica.role == role)
                .map(|replica| replica.node.to_string())
                .collect::<Vec<_>>()
                .join(",")
        };
        let _ = writeln!(
            out,
            "range={id} epoch={} voters=[{}] learners=[{}] leader={} move_step={}",
            r.descriptor.epoch.0,
            nodes(ReplicaRole::Voter),
            nodes(ReplicaRole::Learner),
            if r.leader == 0 {
                "none".into()
            } else {
                r.leader.to_string()
            },
            r.move_step.as_deref().unwrap_or("none")
        );
    }
    out
}

pub fn move_text(reply: &MoveReply) -> String {
    format!(
        "leader={} step={} done={} error={}",
        reply.leader.map_or("none".into(), |id| id.to_string()),
        reply.step.as_deref().unwrap_or("none"),
        reply.done,
        reply.error.as_deref().unwrap_or("none")
    )
}

/// Blocking TLS requests; the binary runs this on a blocking thread.
pub fn run(action: Action, transport: TransportConfig, members: Vec<u64>) -> i32 {
    match action {
        Action::Replace { lost, new, timeout } => {
            match replace_node(&transport, &members, lost, new, timeout) {
                Ok(report) => {
                    for (range, outcome) in &report.ranges {
                        println!("range={} {outcome:?}", range.0);
                    }
                    println!(
                        "retired={} error={}",
                        report.retired,
                        report.retirement_error.as_deref().unwrap_or("none")
                    );
                    i32::from(!report.retired)
                }
                Err(e) => {
                    eprintln!("{e}");
                    1
                }
            }
        }
        Action::Status { range } => match query_status(&transport, &members, super::cli::TIMEOUT) {
            Ok(answers) => {
                print!("{}", status_text(&answers, range));
                for (node, answer) in &answers {
                    if let Err(error) = answer {
                        eprintln!("node={node} error={error}");
                    }
                }
                i32::from(!answers.iter().any(|(_, answer)| answer.is_ok()))
            }
            Err(error) => {
                eprintln!("transport error: {error}");
                1
            }
        },
        Action::Move {
            range,
            from,
            to,
            wait,
        } => loop {
            match request_move(&transport, &members, range, from, to, super::cli::TIMEOUT) {
                Ok(reply) => {
                    println!("{}", move_text(&reply));
                    if reply.error.is_some() {
                        return 1;
                    }
                    if reply.done || !wait {
                        return 0;
                    }
                }
                Err(error) => {
                    eprintln!("transport error: {error}");
                    return 1;
                }
            }
            std::thread::sleep(Duration::from_secs(1));
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use giggles_core::range_api::{Epoch, RangeDescriptor, ReplicaDesc};
    use giggles_replication::call_codec::CallCounts;

    #[test]
    fn parses_replace_node() {
        let parse = |s: &str| Command::parse(s.split_whitespace().map(str::to_owned));
        assert_eq!(
            parse("replace-node cfg 3 4 --timeout 12").unwrap().action,
            Action::Replace {
                lost: 3,
                new: 4,
                timeout: Duration::from_secs(12)
            }
        );
        assert!(parse("replace-node cfg 3 4").is_ok());
        for bad in [
            "replace-node cfg 0 4",
            "replace-node cfg 3 3",
            "replace-node cfg 3 4 --timeout 0",
            "replace-node cfg 3 4 --timeout x",
            "replace-node cfg 3",
            "replace-node cfg 3 4 extra",
        ] {
            assert!(parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn parses_move_and_status() {
        let parse = |s: &str| Command::parse(s.split_whitespace().map(str::to_owned));
        assert_eq!(
            parse("move cfg 7 1 2 --wait").unwrap().action,
            Action::Move {
                range: RangeId(7),
                from: 1,
                to: 2,
                wait: true
            }
        );
        assert!(matches!(
            parse("move cfg 7 1 2").unwrap().action,
            Action::Move { wait: false, .. }
        ));
        assert_eq!(
            parse("status cfg").unwrap().action,
            Action::Status { range: None }
        );
        assert_eq!(
            parse("status cfg 7").unwrap().action,
            Action::Status {
                range: Some(RangeId(7))
            }
        );
        for bad in [
            "",
            "status",
            "status --wait",
            "status cfg 0",
            "status cfg 1 x",
            "move cfg 1 2",
            "move cfg x 1 2",
            "move cfg 1 0 2",
            "move cfg 1 2 3 --x",
            "move cfg 1 2 3 --wait x",
            "status cfg 18446744073709551616",
            "status cfg +1",
        ] {
            assert!(parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn formats_newest_membership_and_filters_ranges() {
        let report = |epoch, role, step: Option<&str>| RangeReport {
            descriptor: RangeDescriptor {
                id: RangeId(7),
                epoch: Epoch(epoch),
                start: vec![],
                end: None,
                replicas: vec![
                    ReplicaDesc {
                        node: 1,
                        role: ReplicaRole::Voter,
                    },
                    ReplicaDesc {
                        node: 2,
                        role: ReplicaRole::Learner,
                    },
                ],
            },
            role,
            term: 2,
            leader: 1,
            applied: 0,
            size: None,
            calls: CallCounts::default(),
            recent: 0,
            window_ms: 1,
            move_step: step.map(str::to_owned),
        };
        let answers = vec![
            (
                1,
                Ok(NodeReport {
                    node: 1,
                    ranges: vec![report(3, RangeRole::Leader, Some("CatchUp"))],
                }),
            ),
            (
                2,
                Ok(NodeReport {
                    node: 2,
                    ranges: vec![report(2, RangeRole::Follower, None)],
                }),
            ),
            (3, Err("offline".into())),
        ];
        assert_eq!(
            status_text(&answers, None),
            "range=7 epoch=3 voters=[1] learners=[2] leader=1 move_step=CatchUp\n"
        );
        assert_eq!(
            status_text(&answers, Some(RangeId(7))),
            status_text(&answers, None)
        );
        assert!(status_text(&answers, Some(RangeId(8))).is_empty());
        assert!(status_text(&[], None).is_empty());
        assert_eq!(
            move_text(&MoveReply::default()),
            "leader=none step=none done=false error=none"
        );
        assert_eq!(
            move_text(&MoveReply {
                leader: Some(1),
                step: Some("Done".into()),
                done: true,
                error: Some("failed".into())
            }),
            "leader=1 step=Done done=true error=failed"
        );
    }
}
