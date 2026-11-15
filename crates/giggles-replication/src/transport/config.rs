use crate::error::{Error, Result};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

#[derive(Clone, Debug)]
pub struct PeerConfig {
    pub address: String,
    pub fingerprint: String,
}

#[derive(Clone, Debug)]
pub struct TransportConfig {
    pub node_id: u64,
    pub cluster: [u8; 16],
    pub listen: String,
    pub certificate: PathBuf,
    pub key: PathBuf,
    pub ca: PathBuf,
    pub peers: BTreeMap<u64, PeerConfig>,
}

pub fn fingerprint(der: &[u8]) -> String {
    hex::encode(Sha256::digest(der))
}

impl TransportConfig {
    pub fn validate(&self) -> Result<()> {
        let own = self
            .peers
            .get(&self.node_id)
            .ok_or_else(|| Error::Identity("node_id missing from peers".into()))?;
        let mut seen = BTreeSet::new();
        for (id, peer) in &self.peers {
            let fp = &peer.fingerprint;
            if fp.len() != 64
                || !fp
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                || !seen.insert(fp)
            {
                return Err(Error::Identity(format!(
                    "peers[{id}].fingerprint invalid or duplicate"
                )));
            }
        }
        let certs = super::tls::chain(&self.certificate, "certificate")?;
        if certs
            .first()
            .is_none_or(|cert| fingerprint(cert) != own.fingerprint)
        {
            return Err(Error::Identity(format!(
                "certificate does not match peers[{}].fingerprint",
                self.node_id
            )));
        }
        Ok(())
    }
}
