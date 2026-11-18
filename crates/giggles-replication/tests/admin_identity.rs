//! Operator authorization is enforced before any admin or peer-call handler.
use super::admin_net::Cluster;
use giggles_replication::{
    admin::{self, AdminRequest, AdminResponse},
    membership::PeerRecord,
    transport::{self, Body, Header, OPERATOR, PeerConfig},
};
use std::{
    io::BufReader,
    time::{Duration, Instant},
};

fn operator_status(c: &Cluster, target: u64) -> AdminResponse {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let response = admin::call(
            &c.configs[target as usize - 1].transport,
            target,
            &AdminRequest::Status,
            deadline
                .saturating_duration_since(Instant::now())
                .min(Duration::from_secs(1)),
            Some(c.identity.clone()),
        );
        if let Ok(response @ AdminResponse::Status(_)) = response {
            return response;
        }
        assert!(
            Instant::now() < deadline,
            "operator status on node {target}: {response:?}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

// Resolve the current leader on every attempt: a published role can be stale
// by the time the own-certificate request reaches that node.
fn own_leader_status(c: &Cluster, deadline: Instant) -> u64 {
    loop {
        for (i, node) in c.nodes.iter().enumerate() {
            let Some(node) = node else { continue };
            if !matches!(
                node.roles.current(),
                giggles_replication::role::Serving::Leader { .. }
            ) {
                continue;
            }
            let target = i as u64 + 1;
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(
                !remaining.is_zero(),
                "own-certificate leader status deadline"
            );
            if matches!(
                admin::call(
                    &c.configs[i].transport,
                    target,
                    &AdminRequest::Status,
                    remaining.min(Duration::from_secs(1)),
                    None,
                ),
                Ok(AdminResponse::Status(_))
            ) {
                return target;
            }
        }
        assert!(
            Instant::now() < deadline,
            "own-certificate leader status deadline"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn own_and_operator_status_work_but_peer_admin_cannot_change_membership() {
    let c = Cluster::new();
    let leader = own_leader_status(&c, Instant::now() + Duration::from_secs(20));
    // On followers the operator can follow NotLeader back to the leader.
    for target in 1..=3 {
        assert!(matches!(
            operator_status(&c, target),
            AdminResponse::Status(_)
        ));
    }
    let peer = if leader == 1 { 2 } else { 1 };
    let config = &c.configs[peer - 1].transport;
    for req in [
        AdminRequest::Status,
        AdminRequest::Replace {
            lost: 1,
            new: PeerRecord {
                id: 4,
                address: "127.0.0.1:1".into(),
                fingerprint: "a".repeat(64),
            },
            timeout_ms: 100,
        },
    ] {
        let response = admin::call(config, leader, &req, Duration::from_secs(5), None).unwrap();
        let AdminResponse::Refused(reason) = response else {
            panic!("{response:?}")
        };
        assert!(reason.contains("replica admin needs an operator identity"));
        assert!(reason.contains(&format!(
            "run on node {leader} or pass an operator identity"
        )));
    }
    for node in c.nodes.iter().flatten() {
        let s = node.handle.membership_status().unwrap();
        let mut voters = s.conf.voters;
        voters.sort_unstable();
        assert_eq!(voters, [1, 2, 3]);
        assert!(s.membership.active.is_none());
        assert!(s.membership.retired.is_empty());
    }
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let leader = own_leader_status(&c, deadline);
        let follower = if leader == 1 { 2 } else { 1 };
        let response = admin::call(
            &c.configs[follower as usize - 1].transport,
            follower,
            &AdminRequest::Status,
            deadline
                .saturating_duration_since(Instant::now())
                .min(Duration::from_secs(1)),
            None,
        );
        if let Ok(AdminResponse::Refused(reason)) = &response
            && reason.contains(&format!(
                "run on node {leader} or pass an operator identity"
            ))
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "local follower redirect deadline: {response:?}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn unlisted_operator_certificate_is_rejected() {
    let c = Cluster::new();
    let target = c.leader().0 as u64 + 1;
    // Node 4 has a trusted CA certificate, but neither its peer identity nor its
    // fingerprint as an operator is admitted by the running nodes.
    let unknown = &c.configs[3].transport;
    let error = admin::call(
        &c.configs[0].transport,
        target,
        &AdminRequest::Status,
        Duration::from_secs(5),
        Some((unknown.certificate.clone(), unknown.key.clone())),
    )
    .unwrap_err();
    assert!(
        error.to_string().contains(&format!(
            "connection refused by node {target}: certificate not accepted for admin"
        )),
        "{error}"
    );
}

#[test]
fn operator_cannot_send_raft_or_calls_or_claim_peer_id() {
    let c = Cluster::new();
    let target = c.leader().0 as u64 + 1;
    let mut config = c.configs[0].transport.clone();
    config.certificate = c.identity.0.clone();
    config.key = c.identity.1.clone();
    let der = rustls_pemfile::certs(&mut BufReader::new(
        std::fs::File::open(&config.certificate).unwrap(),
    ))
    .next()
    .unwrap()
    .unwrap();
    config.node_id = 99;
    config.operators.clear();
    config.peers.insert(
        99,
        PeerConfig {
            address: "127.0.0.1:0".into(),
            fingerprint: transport::fingerprint(&der),
        },
    );
    let (_, connector) = transport::tls(&config).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        for (from, body) in [
            (
                OPERATOR,
                Body::Raft(raft::eraftpb::Message {
                    from: OPERATOR,
                    to: target,
                    ..Default::default()
                }),
            ),
            (
                OPERATOR,
                Body::CallRequest {
                    id: 1,
                    body: giggles_replication::call_codec::encode_call(
                        &giggles_replication::call_codec::PeerCall::Node(
                            giggles_replication::call_codec::NodeCall::Status,
                        ),
                    )
                    .unwrap(),
                },
            ),
            (
                1,
                Body::AdminRequest(AdminRequest::Status.encode().unwrap()),
            ),
        ] {
            let mut stream = transport::connect(&config, &connector, target)
                .await
                .unwrap();
            let header = Header {
                cluster: config.cluster,
                from,
                to: target,
            };
            transport::write_frame(&mut stream, &transport::encode(&header, &body).unwrap())
                .await
                .unwrap();
            let result =
                tokio::time::timeout(Duration::from_secs(5), transport::read_frame(&mut stream))
                    .await;
            assert!(matches!(result, Ok(Err(_))), "unexpected reply: {result:?}");
        }
    });
}

#[test]
fn operator_config_rejects_invalid_fingerprints_and_peer_collisions() {
    let c = Cluster::new();
    for fp in [
        "A".repeat(64),
        "a".repeat(63),
        c.configs[0].transport.peers[&1].fingerprint.clone(),
    ] {
        let mut config = c.configs[0].transport.clone();
        config.operators = vec![fp];
        assert!(config.validate().is_err());
    }
    let mut config = c.configs[0].transport.clone();
    config.peers.insert(OPERATOR, config.peers[&1].clone());
    assert!(config.validate().is_err());
}

#[test]
fn operator_replace_works_on_leader_after_redirect() {
    super::admin_net::status_and_replace_via_admin_call();
}

#[test]
fn invalid_runtime_peers_are_skipped_and_nonzero_group_admin_is_refused() {
    use giggles_replication::transport::{GroupRouter, PeerSink, Transport};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    #[derive(Default)]
    struct Sink(AtomicUsize);
    impl PeerSink for Sink {
        fn deliver(&self, _: u64, _: raft::eraftpb::Message) {}
        fn snapshot_status(&self, _: u64, _: bool) {}
        fn admin(&self, _: u64, _: AdminRequest) -> AdminResponse {
            self.0.fetch_add(1, Ordering::Relaxed);
            AdminResponse::Failed("unexpected admin dispatch".into())
        }
    }
    let c = Cluster::new();
    let mut config = c.configs[0].transport.clone();
    config.listen = "127.0.0.1:0".into();
    let router = Arc::new(GroupRouter::new());
    let sink = Arc::new(Sink::default());
    router.register(0, sink.clone(), None);
    router.register(17, sink.clone(), None);
    let transport = Transport::start_groups(config.clone(), router).unwrap();
    config.peers.get_mut(&1).unwrap().address = transport.local_addr().to_string();
    let mut updated = config.peers.clone();
    updated.insert(4, c.configs[3].transport.peers[&4].clone());
    updated.insert(OPERATOR, updated[&4].clone());
    updated.insert(
        5,
        PeerConfig {
            address: "127.0.0.1:1".into(),
            fingerprint: config.operators[0].clone(),
        },
    );
    // Repeat the same update: invalid entries must neither freeze the table nor
    // make a listed operator become a peer. Each diagnostic is emitted once.
    transport.set_peers(updated.clone()).unwrap();
    transport.set_peers(updated).unwrap();
    let mut peer = c.configs[3].transport.clone();
    peer.peers.insert(1, config.peers[&1].clone());
    assert!(matches!(
        admin::call(
            &peer,
            1,
            &AdminRequest::Status,
            Duration::from_secs(5),
            None
        )
        .unwrap(),
        AdminResponse::Refused(_)
    ));
    // With no table update, peer 4 is unlisted and cannot get a Refused reply.
    for group in [17, 18] {
        let mut grouped = config.clone();
        grouped.cluster = giggles_replication::group::group_cluster(config.cluster, group);
        for request in [
            AdminRequest::Status,
            AdminRequest::Replace {
                lost: 2,
                new: PeerRecord {
                    id: 4,
                    address: "127.0.0.1:1".into(),
                    fingerprint: "a".repeat(64),
                },
                timeout_ms: 100,
            },
        ] {
            for identity in [None, Some(c.identity.clone())] {
                assert_eq!(
                    admin::call(&grouped, 1, &request, Duration::from_secs(5), identity).unwrap(),
                    AdminResponse::Refused(
                        "replica admin is only for the replicated database group".into()
                    )
                );
            }
        }
    }
    assert_eq!(sink.0.load(Ordering::Relaxed), 0);
    transport.shutdown();
}
