use giggles_replication::transport::{PeerConfig, TransportConfig, fingerprint};
use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

pub struct TestPki {
    pub dir: PathBuf,
    pub ca: PathBuf,
    pub nodes: BTreeMap<u64, (PathBuf, PathBuf, String)>,
}

pub fn make_pki(dir: &Path, ids: &[u64]) -> TestPki {
    std::fs::create_dir_all(dir).unwrap();
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_key = KeyPair::generate().unwrap();
    let ca_cert = params.self_signed(&ca_key).unwrap();
    let ca = dir.join("ca.pem");
    std::fs::write(&ca, ca_cert.pem()).unwrap();
    let mut nodes = BTreeMap::new();
    // Pre-create a CA-signed leaf omitted from every node's peer map.
    for name in ids
        .iter()
        .map(u64::to_string)
        .chain(std::iter::once("stray".into()))
    {
        let key = KeyPair::generate().unwrap();
        let cert = CertificateParams::new(vec!["localhost".into()])
            .unwrap()
            .signed_by(&key, &ca_cert, &ca_key)
            .unwrap();
        let cert_path = dir.join(format!("{name}.pem"));
        let key_path = dir.join(format!("{name}.key"));
        std::fs::write(&cert_path, cert.pem()).unwrap();
        std::fs::write(&key_path, key.serialize_pem()).unwrap();
        if let Ok(id) = name.parse::<u64>() {
            nodes.insert(id, (cert_path, key_path, fingerprint(cert.der())));
        }
    }
    TestPki {
        dir: dir.into(),
        ca,
        nodes,
    }
}

pub fn config_for(
    pki: &TestPki,
    id: u64,
    cluster: [u8; 16],
    addrs: &BTreeMap<u64, String>,
) -> TransportConfig {
    TransportConfig {
        operators: vec![],
        node_id: id,
        cluster,
        listen: addrs[&id].clone(),
        certificate: pki.nodes[&id].0.clone(),
        key: pki.nodes[&id].1.clone(),
        ca: pki.ca.clone(),
        peers: pki
            .nodes
            .iter()
            .map(|(&id, (_, _, fp))| {
                (
                    id,
                    PeerConfig {
                        address: addrs[&id].clone(),
                        fingerprint: fp.clone(),
                    },
                )
            })
            .collect(),
    }
}

pub fn stray_cert(pki: &TestPki) -> (PathBuf, PathBuf) {
    (pki.dir.join("stray.pem"), pki.dir.join("stray.key"))
}
