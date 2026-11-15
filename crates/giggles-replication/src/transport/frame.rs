use crate::error::{Error, Result};
use protobuf::Message as _;
use raft::eraftpb::Message;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const MAX_FRAME: usize = 2 << 20;
pub const MAX_CHUNK: usize = 512 << 10;
const MAGIC: &[u8; 8] = b"GDRP0001";
const HEADER: usize = 41;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Header {
    pub cluster: [u8; 16],
    pub from: u64,
    pub to: u64,
}

#[derive(Debug, PartialEq)]
pub enum Body {
    Raft(Message),
    SnapOffer(Vec<u8>),
    SnapResume {
        file: u32,
        offset: u64,
    },
    SnapChunk {
        file: u32,
        offset: u64,
        data: Vec<u8>,
    },
    SnapDone,
    SnapAck(std::result::Result<(), String>),
}

fn corrupt() -> Error {
    Error::Corrupt("transport frame format/size/checksum".into())
}

pub fn encode(h: &Header, b: &Body) -> Result<Vec<u8>> {
    let mut bytes = MAGIC.to_vec();
    bytes.extend(h.cluster);
    bytes.extend(h.from.to_be_bytes());
    bytes.extend(h.to.to_be_bytes());
    match b {
        Body::Raft(m) => {
            if m.from != h.from || m.to != h.to {
                return Err(Error::Identity("Raft from/to".into()));
            }
            bytes.push(1);
            bytes.extend(m.write_to_bytes()?);
        }
        Body::SnapOffer(data) => {
            bytes.push(2);
            bytes.extend(data);
        }
        Body::SnapResume { file, offset } => {
            bytes.push(3);
            bytes.extend(file.to_be_bytes());
            bytes.extend(offset.to_be_bytes());
        }
        Body::SnapChunk { file, offset, data } => {
            if data.len() > MAX_CHUNK {
                return Err(corrupt());
            }
            bytes.push(4);
            bytes.extend(file.to_be_bytes());
            bytes.extend(offset.to_be_bytes());
            bytes.extend(Sha256::digest(data));
            bytes.extend(data);
        }
        Body::SnapDone => bytes.push(5),
        Body::SnapAck(result) => {
            bytes.push(6);
            match result {
                Ok(()) => bytes.push(0),
                Err(reason) => {
                    bytes.push(1);
                    bytes.extend(reason.as_bytes());
                }
            }
        }
    }
    if bytes.len() > MAX_FRAME {
        return Err(corrupt());
    }
    Ok(bytes)
}

pub fn decode(expected: &Header, bytes: &[u8]) -> Result<Body> {
    if bytes.len() < HEADER || bytes.len() > MAX_FRAME || &bytes[..8] != MAGIC {
        return Err(corrupt());
    }
    for (field, actual, wanted) in [
        ("cluster", &bytes[8..24], expected.cluster.as_slice()),
        ("from", &bytes[24..32], &expected.from.to_be_bytes()),
        ("to", &bytes[32..40], &expected.to.to_be_bytes()),
    ] {
        if actual != wanted {
            return Err(Error::Identity(format!("frame {field}")));
        }
    }
    let p = &bytes[HEADER..];
    Ok(match bytes[40] {
        1 => {
            let m = Message::parse_from_bytes(p)?;
            if m.from != expected.from || m.to != expected.to {
                return Err(Error::Identity("Raft from/to".into()));
            }
            Body::Raft(m)
        }
        2 => Body::SnapOffer(p.to_vec()),
        3 if p.len() == 12 => Body::SnapResume {
            file: u32::from_be_bytes(p[..4].try_into().unwrap()),
            offset: u64::from_be_bytes(p[4..12].try_into().unwrap()),
        },
        4 if p.len() >= 44 && p.len() - 44 <= MAX_CHUNK => {
            if Sha256::digest(&p[44..])[..] != p[12..44] {
                return Err(corrupt());
            }
            Body::SnapChunk {
                file: u32::from_be_bytes(p[..4].try_into().unwrap()),
                offset: u64::from_be_bytes(p[4..12].try_into().unwrap()),
                data: p[44..].to_vec(),
            }
        }
        5 if p.is_empty() => Body::SnapDone,
        6 if p == [0] => Body::SnapAck(Ok(())),
        6 if p.first() == Some(&1) => Body::SnapAck(Err(
            String::from_utf8(p[1..].to_vec()).map_err(|_| corrupt())?
        )),
        _ => return Err(corrupt()),
    })
}

pub async fn read_frame<R: AsyncRead + Unpin>(r: &mut R) -> Result<Vec<u8>> {
    let n = r.read_u32().await? as usize;
    if n == 0 || n > MAX_FRAME {
        return Err(corrupt());
    }
    let mut bytes = vec![0; n];
    r.read_exact(&mut bytes).await?;
    Ok(bytes)
}

pub async fn write_frame<W: AsyncWrite + Unpin>(w: &mut W, bytes: &[u8]) -> Result<()> {
    if bytes.is_empty() || bytes.len() > MAX_FRAME {
        return Err(corrupt());
    }
    w.write_u32(bytes.len() as u32).await?;
    w.write_all(bytes).await?;
    w.flush().await?;
    Ok(())
}
