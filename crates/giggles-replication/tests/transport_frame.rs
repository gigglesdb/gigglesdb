use super::common::certs::{config_for, make_pki, stray_cert};
use giggles_replication::{error::Error, transport::*};
use protobuf::Message as _;
use raft::eraftpb::Message;
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU64, Ordering},
};
use tokio::{
    net::TcpListener,
    runtime::Runtime,
    time::{Duration, timeout},
};

const H: Header = Header {
    cluster: [74; 16],
    from: 1,
    to: 2,
};

#[test]
fn round_trips() {
    let m = Message {
        from: 1,
        to: 2,
        ..Default::default()
    };
    for b in [
        Body::Raft(m),
        Body::SnapOffer(vec![1, 2]),
        Body::SnapResume { file: 3, offset: 9 },
        Body::SnapChunk {
            file: 4,
            offset: 8,
            data: vec![7; MAX_CHUNK],
        },
        Body::SnapDone,
        Body::SnapAck(Ok(())),
        Body::SnapAck(Err("no space".into())),
    ] {
        assert_eq!(decode(&H, &encode(&H, &b).unwrap()).unwrap(), b);
    }
}

#[test]
fn malformed_frames() {
    let done = encode(&H, &Body::SnapDone).unwrap();
    for (offset, field) in [(8, "cluster"), (24, "from"), (32, "to")] {
        let mut bad = done.clone();
        bad[offset] ^= 1;
        assert!(matches!(decode(&H, &bad), Err(Error::Identity(s)) if s.contains(field)));
    }
    let mut bad = done.clone();
    bad[0] ^= 1;
    assert!(matches!(decode(&H, &bad), Err(Error::Corrupt(_))));
    for n in 0..done.len() {
        assert!(decode(&H, &done[..n]).is_err());
    }
    let mut bad = done.clone();
    bad.push(0);
    assert!(decode(&H, &bad).is_err());
    let mut bad = done.clone();
    bad[40] = 99;
    assert!(decode(&H, &bad).is_err());
    let mut bad = done[..40].to_vec();
    bad.push(1);
    bad.extend(
        Message {
            from: 3,
            to: 2,
            ..Default::default()
        }
        .write_to_bytes()
        .unwrap(),
    );
    assert!(matches!(decode(&H, &bad), Err(Error::Identity(_))));
    let mut bad = encode(
        &H,
        &Body::SnapChunk {
            file: 0,
            offset: 0,
            data: vec![0],
        },
    )
    .unwrap();
    *bad.last_mut().unwrap() ^= 1;
    assert!(matches!(decode(&H, &bad), Err(Error::Corrupt(_))));
    for n in 41..85 {
        assert!(decode(&H, &bad[..n]).is_err());
    }
    let large = Body::SnapChunk {
        file: 0,
        offset: 0,
        data: vec![0; MAX_CHUNK + 1],
    };
    assert!(encode(&H, &large).is_err());
    bad.resize(41 + 44 + MAX_CHUNK + 1, 0);
    assert!(decode(&H, &bad).is_err());
    let mut bad = done[..40].to_vec();
    bad.extend([6, 1, 255]);
    assert!(decode(&H, &bad).is_err());
}

fn fixture() -> super::common::certs::TestPki {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    make_pki(
        &std::env::temp_dir().join(format!(
            "wo1-pki-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )),
        &[1, 2, 3],
    )
}
fn addrs() -> BTreeMap<u64, String> {
    (1..=3).map(|id| (id, "127.0.0.1:0".into())).collect()
}

#[test]
fn validates_bindings() {
    let pki = fixture();
    let base = config_for(&pki, 1, H.cluster, &addrs());
    base.validate().unwrap();
    for case in 0..6 {
        let mut c = base.clone();
        let field = match case {
            0 => {
                c.certificate = pki.nodes[&2].0.clone();
                "certificate"
            }
            1 => {
                c.peers.get_mut(&2).unwrap().fingerprint = "ab".into();
                "peers[2].fingerprint"
            }
            2 => {
                c.peers.get_mut(&2).unwrap().fingerprint = c.peers[&1].fingerprint.clone();
                "peers[2].fingerprint"
            }
            3 => {
                c.peers.remove(&1);
                "node_id"
            }
            4 => {
                c.peers.get_mut(&2).unwrap().fingerprint = "G".repeat(64);
                "peers[2].fingerprint"
            }
            _ => {
                c.certificate = pki.dir.join("missing.pem");
                "certificate"
            }
        };
        match c.validate() {
            Err(Error::Identity(s) | Error::Io(s)) => assert!(s.contains(field), "{s}"),
            other => panic!("unexpected validation: {other:?}"),
        }
    }
    std::fs::remove_dir_all(pki.dir).unwrap();
}

#[test]
fn framing_io() {
    Runtime::new().unwrap().block_on(async {
        let bytes = encode(&H, &Body::SnapDone).unwrap();
        let mut wire = Vec::new();
        write_frame(&mut wire, &bytes).await.unwrap();
        assert_eq!(&wire[..4], &(bytes.len() as u32).to_be_bytes());
        assert_eq!(read_frame(&mut wire.as_slice()).await.unwrap(), bytes);
        for n in [0, MAX_FRAME + 1] {
            assert!(
                read_frame(&mut (n as u32).to_be_bytes().as_slice())
                    .await
                    .is_err()
            );
            assert!(write_frame(&mut Vec::new(), &vec![0; n]).await.is_err());
        }
        assert!(read_frame(&mut &wire[..wire.len() - 1]).await.is_err());
    });
}

#[test]
fn certificate_bound_handshakes() {
    let pki = fixture();
    Runtime::new().unwrap().block_on(async {
        timeout(Duration::from_secs(15), async {
            for case in 0..3 {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let mut addresses = addrs();
                addresses.insert(2, listener.local_addr().unwrap().to_string());
                let server = config_for(&pki, 2, H.cluster, &addresses);
                let mut client = config_for(&pki, 1, H.cluster, &addresses);
                if case == 1 {
                    (client.certificate, client.key) = stray_cert(&pki);
                    let der = rustls_pemfile::certs(&mut std::io::BufReader::new(
                        std::fs::File::open(&client.certificate).unwrap(),
                    ))
                    .next()
                    .unwrap()
                    .unwrap();
                    client.peers.get_mut(&1).unwrap().fingerprint = fingerprint(&der);
                } else if case == 2 {
                    // Swap 2/3 to retain distinct, otherwise valid fingerprints.
                    let fp = client.peers[&2].fingerprint.clone();
                    client.peers.get_mut(&2).unwrap().fingerprint =
                        client.peers[&3].fingerprint.clone();
                    client.peers.get_mut(&3).unwrap().fingerprint = fp;
                }
                let (acceptor, _) = tls(&server).unwrap();
                let (_, connector) = tls(&client).unwrap();
                let inbound = async {
                    let (tcp, _) = listener.accept().await.unwrap();
                    accept(&server, &acceptor, tcp).await
                };
                let (out, incoming) = tokio::join!(connect(&client, &connector, 2), inbound);
                match case {
                    0 => {
                        assert!(out.is_ok());
                        assert_eq!(incoming.unwrap().0, 1);
                    }
                    1 => {
                        assert!(out.is_ok());
                        assert!(matches!(incoming, Err(Error::Identity(_))));
                    }
                    _ => {
                        assert!(matches!(out, Err(Error::Identity(_))));
                        assert_eq!(incoming.unwrap().0, 1);
                    }
                }
            }
        })
        .await
        .unwrap();
    });
    std::fs::remove_dir_all(pki.dir).unwrap();
}
