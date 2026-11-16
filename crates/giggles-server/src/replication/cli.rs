//! Operator command parsing and stable text output.
use giggles_replication::{
    admin::{AdminRequest, AdminResponse},
    membership::PeerRecord,
};
use std::time::Duration;

pub const USAGE: &str = "usage: giggles-server replica status <config.json> | replica replace <config.json> --lost <id> --new-id <id> --new-address <host:port> --new-fingerprint <64 hex> [--timeout-secs <1..600>]";

#[derive(Debug)]
pub struct Command {
    pub config: String,
    pub request: AdminRequest,
    pub timeout: Duration,
}
impl Command {
    pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, &'static str> {
        let mut args = args.into_iter();
        let action = args.next().ok_or(USAGE)?;
        let config = args
            .next()
            .filter(|s| !s.is_empty() && !s.starts_with('-'))
            .ok_or(USAGE)?;
        if action == "status" && args.next().is_none() {
            return Ok(Self {
                config,
                request: AdminRequest::Status,
                timeout: Duration::from_secs(600),
            });
        }
        if action != "replace" {
            return Err(USAGE);
        }
        let (mut lost, mut id, mut address, mut fingerprint, mut timeout) =
            (None, None, None, None, None);
        while let Some(flag) = args.next() {
            let value = args.next().ok_or(USAGE)?;
            match flag.as_str() {
                "--lost" if lost.is_none() => lost = Some(nonzero(&value)?),
                "--new-id" if id.is_none() => id = Some(nonzero(&value)?),
                "--new-address" if address.is_none() => {
                    // Accept DNS names as well as IP literals, without resolving them here.
                    let (host, port) = value.rsplit_once(':').ok_or(USAGE)?;
                    if host.is_empty()
                        || host.chars().any(char::is_whitespace)
                        || port.parse::<u16>().ok().filter(|p| *p != 0).is_none()
                    {
                        return Err(USAGE);
                    }
                    address = Some(value);
                }
                "--new-fingerprint" if fingerprint.is_none() => {
                    if value.len() != 64 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
                        return Err(USAGE);
                    }
                    fingerprint = Some(value.to_ascii_lowercase());
                }
                "--timeout-secs" if timeout.is_none() => {
                    let n = nonzero(&value)?;
                    if n > 600 {
                        return Err(USAGE);
                    }
                    timeout = Some(n);
                }
                _ => return Err(USAGE),
            }
        }
        let timeout = Duration::from_secs(timeout.unwrap_or(600));
        let lost = lost.ok_or(USAGE)?;
        let id = id.ok_or(USAGE)?;
        if lost == id {
            return Err(USAGE);
        }
        Ok(Self {
            config,
            timeout,
            request: AdminRequest::Replace {
                lost,
                new: PeerRecord {
                    id,
                    address: address.ok_or(USAGE)?,
                    fingerprint: fingerprint.ok_or(USAGE)?,
                },
                timeout_ms: timeout.as_millis() as u64,
            },
        })
    }
}
fn nonzero(s: &str) -> Result<u64, &'static str> {
    s.parse::<u64>().ok().filter(|n| *n != 0).ok_or(USAGE)
}

/// Exit code and text; the caller sends failures to stderr.
pub fn output(response: AdminResponse) -> (i32, String) {
    match response {
        AdminResponse::Status(s) | AdminResponse::Done(s) => (
            0,
            format!(
                "voters: {:?}\nvoters_outgoing: {:?}\nlearners: {:?}\nstep: {}\nlost: {:?}\nnew: {:?}\nlearner_lag: {:?}\nretired: {:?}\nleader: {}\nterm: {}",
                s.voters,
                s.voters_outgoing,
                s.learners,
                s.step,
                s.lost,
                s.new,
                s.learner_lag,
                s.retired,
                s.leader,
                s.term
            ),
        ),
        AdminResponse::Refused(reason) => (1, format!("refused: {reason}")),
        AdminResponse::Failed(reason) => (1, format!("failed: {reason}")),
        AdminResponse::NotLeader { leader } => (
            3,
            format!("not leader: no reachable leader (hint: {leader})"),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn parse(s: &str) -> Result<Command, &'static str> {
        Command::parse(s.split_whitespace().map(str::to_owned))
    }
    fn replacement() -> String {
        format!(
            "replace cfg --lost 1 --new-id 4 --new-address localhost:9004 --new-fingerprint {}",
            "AB".repeat(32)
        )
    }
    #[test]
    fn valid_requests() {
        assert_eq!(parse("status cfg").unwrap().request, AdminRequest::Status);
        let c = parse(&replacement()).unwrap();
        assert_eq!(c.timeout, Duration::from_secs(600));
        let AdminRequest::Replace {
            lost,
            new,
            timeout_ms,
        } = c.request
        else {
            panic!()
        };
        assert_eq!((lost, new.id, timeout_ms), (1, 4, 600_000));
        assert_eq!(new.fingerprint, "ab".repeat(32));
        assert_eq!(
            parse(&(replacement() + " --timeout-secs 1"))
                .unwrap()
                .timeout,
            Duration::from_secs(1)
        );
    }
    #[test]
    fn invalid_arguments() {
        for s in [
            "",
            "status",
            "status cfg extra",
            "wat cfg",
            "replace cfg",
            "status --bad",
        ] {
            assert!(parse(s).is_err(), "{s}");
        }
        for suffix in [
            "--lost 2",
            "--unknown 1",
            "--timeout-secs 0",
            "--timeout-secs 601",
            "--timeout-secs nope",
            "--timeout-secs",
            "extra",
        ] {
            assert!(
                parse(&format!("{} {suffix}", replacement())).is_err(),
                "{suffix}"
            );
        }
        for (a, b) in [
            ("--lost 1", "--lost 0"),
            ("--new-id 4", "--new-id 1"),
            ("localhost:9004", "localhost"),
            ("localhost:9004", "localhost:0"),
            (&"AB".repeat(32), "zz"),
        ] {
            assert!(parse(&replacement().replace(a, b)).is_err());
        }
    }
    #[test]
    fn reports_all_status_fields() {
        let report = giggles_replication::admin::StatusReport {
            voters: vec![2, 3, 4],
            voters_outgoing: vec![],
            learners: vec![],
            step: "idle".into(),
            lost: None,
            new: None,
            learner_lag: None,
            retired: vec![1],
            leader: 2,
            term: 9,
        };
        let status = output(AdminResponse::Status(report.clone()));
        assert_eq!(status, output(AdminResponse::Done(report)));
        assert_eq!(status.0, 0);
        assert_eq!(
            status.1,
            "voters: [2, 3, 4]\nvoters_outgoing: []\nlearners: []\nstep: idle\nlost: None\nnew: None\nlearner_lag: None\nretired: [1]\nleader: 2\nterm: 9"
        );
    }
    #[test]
    fn response_codes() {
        assert_eq!(
            output(AdminResponse::Refused("busy".into())),
            (1, "refused: busy".into())
        );
        assert_eq!(output(AdminResponse::Failed("disk".into())).0, 1);
        assert_eq!(output(AdminResponse::NotLeader { leader: 0 }).0, 3);
    }
}
