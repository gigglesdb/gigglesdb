//! Operator requests over the authenticated peer transport.
use crate::{
    error::{Error, Result},
    membership::{PeerRecord, Step},
    replace::{self, ReplaceError, ReplaceRequest},
    sink::{MembershipStatus, ReplicaHandle},
    transport::{self, Body, Header, TransportConfig},
};
use std::time::Duration;

const MAX: usize = 64 << 10;
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AdminRequest {
    Status,
    Replace {
        lost: u64,
        new: PeerRecord,
        timeout_ms: u64,
    },
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AdminResponse {
    Status(StatusReport),
    Done(StatusReport),
    NotLeader { leader: u64 },
    Refused(String),
    Failed(String),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StatusReport {
    pub voters: Vec<u64>,
    pub voters_outgoing: Vec<u64>,
    pub learners: Vec<u64>,
    pub step: String,
    pub lost: Option<u64>,
    pub new: Option<u64>,
    pub learner_lag: Option<u64>,
    pub retired: Vec<u64>,
    pub leader: u64,
    pub term: u64,
}
impl From<MembershipStatus> for StatusReport {
    fn from(s: MembershipStatus) -> Self {
        let active = s.membership.active.as_ref();
        let step = active.map_or("idle", |a| match a.step {
            Step::Enrolled => "enrolled",
            Step::Learner { .. } => "learner",
            Step::CaughtUp { .. } => "caught-up",
            Step::Joint => "joint",
            Step::Left => "left",
        });
        let mut report = Self {
            voters: s.conf.voters,
            voters_outgoing: s.conf.voters_outgoing,
            learners: s.conf.learners,
            step: step.into(),
            lost: active.map(|a| a.lost),
            new: active.map(|a| a.new.id),
            learner_lag: s.learner_matched.map(|m| s.last_index.saturating_sub(m)),
            retired: s.membership.retired.iter().copied().collect(),
            leader: s.role.leader,
            term: s.role.term,
        };
        report.voters.sort_unstable();
        report.voters_outgoing.sort_unstable();
        report.learners.sort_unstable();
        report
    }
}
fn invalid() -> Error {
    Error::Corrupt("admin protocol format/size".into())
}
fn number(b: &mut Vec<u8>, n: u64) {
    b.extend(n.to_be_bytes());
}
fn string(b: &mut Vec<u8>, s: &str) -> Result<()> {
    b.extend(u16::try_from(s.len()).map_err(|_| invalid())?.to_be_bytes());
    b.extend(s.as_bytes());
    Ok(())
}
fn list(b: &mut Vec<u8>, v: &[u64]) -> Result<()> {
    b.extend(u16::try_from(v.len()).map_err(|_| invalid())?.to_be_bytes());
    for n in v {
        number(b, *n);
    }
    Ok(())
}
fn option(b: &mut Vec<u8>, v: Option<u64>) {
    b.push(u8::from(v.is_some()));
    if let Some(n) = v {
        number(b, n);
    }
}
struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if n > self.0.len() {
            return Err(invalid());
        }
        let (a, b) = self.0.split_at(n);
        self.0 = b;
        Ok(a)
    }
    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn number(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn len(&mut self) -> Result<usize> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()) as usize)
    }
    fn string(&mut self) -> Result<String> {
        let n = self.len()?;
        String::from_utf8(self.take(n)?.to_vec()).map_err(|_| invalid())
    }
    fn list(&mut self) -> Result<Vec<u64>> {
        let n = self.len()?;
        (0..n).map(|_| self.number()).collect()
    }
    fn option(&mut self) -> Result<Option<u64>> {
        match self.byte()? {
            0 => Ok(None),
            1 => Ok(Some(self.number()?)),
            _ => Err(invalid()),
        }
    }
    fn start(bytes: &'a [u8]) -> Result<Self> {
        if bytes.len() > MAX {
            return Err(invalid());
        }
        let mut r = Self(bytes);
        if r.byte()? != 1 {
            return Err(invalid());
        }
        Ok(r)
    }
    fn finish(self) -> Result<()> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(invalid())
        }
    }
}
fn finish(b: Vec<u8>) -> Result<Vec<u8>> {
    if b.len() <= MAX {
        Ok(b)
    } else {
        Err(invalid())
    }
}
impl AdminRequest {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut b = vec![1];
        match self {
            Self::Status => b.push(0),
            Self::Replace {
                lost,
                new,
                timeout_ms,
            } => {
                b.push(1);
                number(&mut b, *lost);
                number(&mut b, new.id);
                string(&mut b, &new.address)?;
                string(&mut b, &new.fingerprint)?;
                number(&mut b, *timeout_ms);
            }
        }
        finish(b)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut r = Reader::start(bytes)?;
        let req = match r.byte()? {
            0 => Self::Status,
            1 => Self::Replace {
                lost: r.number()?,
                new: PeerRecord {
                    id: r.number()?,
                    address: r.string()?,
                    fingerprint: r.string()?,
                },
                timeout_ms: r.number()?,
            },
            _ => return Err(invalid()),
        };
        r.finish()?;
        Ok(req)
    }
}
impl StatusReport {
    fn encode(&self, b: &mut Vec<u8>) -> Result<()> {
        list(b, &self.voters)?;
        list(b, &self.voters_outgoing)?;
        list(b, &self.learners)?;
        string(b, &self.step)?;
        option(b, self.lost);
        option(b, self.new);
        option(b, self.learner_lag);
        list(b, &self.retired)?;
        number(b, self.leader);
        number(b, self.term);
        Ok(())
    }
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        let s = Self {
            voters: r.list()?,
            voters_outgoing: r.list()?,
            learners: r.list()?,
            step: r.string()?,
            lost: r.option()?,
            new: r.option()?,
            learner_lag: r.option()?,
            retired: r.list()?,
            leader: r.number()?,
            term: r.number()?,
        };
        if !matches!(
            s.step.as_str(),
            "idle" | "enrolled" | "learner" | "caught-up" | "joint" | "left"
        ) {
            return Err(invalid());
        }
        Ok(s)
    }
}
impl AdminResponse {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut b = vec![1];
        match self {
            Self::Status(s) => {
                b.push(0);
                s.encode(&mut b)?;
            }
            Self::Done(s) => {
                b.push(1);
                s.encode(&mut b)?;
            }
            Self::NotLeader { leader } => {
                b.push(2);
                number(&mut b, *leader);
            }
            Self::Refused(s) => {
                b.push(3);
                string(&mut b, s)?;
            }
            Self::Failed(s) => {
                b.push(4);
                string(&mut b, s)?;
            }
        }
        finish(b)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut r = Reader::start(bytes)?;
        let response = match r.byte()? {
            0 => Self::Status(StatusReport::decode(&mut r)?),
            1 => Self::Done(StatusReport::decode(&mut r)?),
            2 => Self::NotLeader {
                leader: r.number()?,
            },
            3 => Self::Refused(r.string()?),
            4 => Self::Failed(r.string()?),
            _ => return Err(invalid()),
        };
        r.finish()?;
        Ok(response)
    }
}
pub fn serve(handle: &ReplicaHandle, req: AdminRequest) -> AdminResponse {
    let result = match req {
        AdminRequest::Status => replace::status(handle)
            .and_then(|s| {
                if s.role.state != raft::StateRole::Leader {
                    Err(ReplaceError::NotLeader {
                        leader: s.role.leader,
                    })
                } else {
                    Ok(s)
                }
            })
            .map(|s| AdminResponse::Status(s.into())),
        AdminRequest::Replace {
            lost,
            new,
            timeout_ms,
        } => replace::run(
            handle,
            &ReplaceRequest { lost, new },
            Duration::from_millis(timeout_ms.clamp(1, 600_000)),
        )
        .map(|s| AdminResponse::Done(s.into())),
    };
    result.unwrap_or_else(|e| match e {
        ReplaceError::NotLeader { leader } => AdminResponse::NotLeader { leader },
        ReplaceError::Refused(s) => AdminResponse::Refused(s),
        e => AdminResponse::Failed(e.to_string()),
    })
}
/// One total deadline covers connecting, response and up to three leader redirects.
pub fn call(
    config: &TransportConfig,
    target: u64,
    req: &AdminRequest,
    timeout: Duration,
) -> Result<AdminResponse> {
    let (_, connector) = transport::tls(config)?;
    let payload = req.encode()?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        tokio::time::timeout(timeout, async {
            let mut target = target;
            for hop in 0..=3 {
                let mut stream = transport::tls::connect_admin(config, &connector, target).await?;
                let h = Header {
                    cluster: config.cluster,
                    from: config.node_id,
                    to: target,
                };
                transport::write_frame(
                    &mut stream,
                    &transport::encode(&h, &Body::AdminRequest(payload.clone()))?,
                )
                .await?;
                let h = Header {
                    from: target,
                    to: config.node_id,
                    ..h
                };
                let Body::AdminResponse(bytes) =
                    transport::decode(&h, &transport::read_frame(&mut stream).await?)?
                else {
                    return Err(invalid());
                };
                let response = AdminResponse::decode(&bytes)?;
                if let AdminResponse::NotLeader { leader } = response
                    && leader != 0
                    && config.peers.contains_key(&leader)
                    && hop < 3
                {
                    target = leader;
                    continue;
                }
                return Ok(response);
            }
            Err(invalid())
        })
        .await
        .map_err(|_| Error::Io("admin call timed out".into()))?
    })
}
