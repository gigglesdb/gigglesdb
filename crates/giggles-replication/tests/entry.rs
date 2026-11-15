use giggles_replication::entry::{
    ENTRY_VERSION, EntryError, EntryKind, MAX_ENTRY_BYTES, ProposalContext, decode, encode,
};
use sha2::{Digest, Sha256};

const CLUSTER: [u8; 16] = [0x42; 16];

fn sample() -> Vec<u8> {
    encode(CLUSTER, EntryKind::Batch, b"payload").unwrap()
}

#[test]
fn round_trip_both_kinds_and_empty_payloads() {
    for kind in [EntryKind::Batch, EntryKind::Admin] {
        for payload in [b"payload".as_slice(), b"".as_slice()] {
            let bytes = encode(CLUSTER, kind, payload).unwrap();
            assert_eq!(bytes.len(), 26 + payload.len());
            assert_eq!(bytes[0], ENTRY_VERSION);
            assert_eq!(&bytes[1..17], &CLUSTER);
            assert_eq!(bytes[17], kind as u8);
            assert_eq!(&bytes[18..22], &(payload.len() as u32).to_be_bytes());
            assert_eq!(&bytes[22..22 + payload.len()], payload);
            assert_eq!(
                &bytes[bytes.len() - 4..],
                &Sha256::digest(&bytes[..bytes.len() - 4])[..4]
            );
            assert_eq!(decode(CLUSTER, &bytes), Ok((kind, payload)));
        }
    }
}

#[test]
fn rejects_flipped_payload_byte() {
    let mut bytes = sample();
    bytes[22] ^= 1;
    assert_eq!(decode(CLUSTER, &bytes), Err(EntryError::BadChecksum));
}

#[test]
fn rejects_truncation_at_every_offset() {
    let bytes = sample();
    for end in 0..bytes.len() {
        assert_eq!(decode(CLUSTER, &bytes[..end]), Err(EntryError::Truncated));
    }
}

#[test]
fn rejects_trailing_byte() {
    let mut bytes = sample();
    bytes.push(0);
    assert_eq!(decode(CLUSTER, &bytes), Err(EntryError::TrailingBytes));
}

#[test]
fn rejects_oversize_declared_length_before_slicing() {
    for declared in [(MAX_ENTRY_BYTES - 25) as u32, u32::MAX] {
        let mut bytes = sample();
        bytes[18..22].copy_from_slice(&declared.to_be_bytes());
        bytes.truncate(22);
        assert_eq!(decode(CLUSTER, &bytes), Err(EntryError::Oversize));
    }
}

#[test]
fn rejects_unknown_version() {
    let mut bytes = sample();
    bytes[0] = ENTRY_VERSION + 1;
    assert_eq!(decode(CLUSTER, &bytes), Err(EntryError::UnknownVersion));
}

#[test]
fn rejects_wrong_cluster() {
    assert_eq!(decode([0; 16], &sample()), Err(EntryError::WrongCluster));
}

#[test]
fn rejects_unknown_kind() {
    let mut bytes = sample();
    bytes[17] = 3;
    assert_eq!(decode(CLUSTER, &bytes), Err(EntryError::UnknownKind));
}

#[test]
fn encode_refuses_oversize_total() {
    let payload = vec![0; MAX_ENTRY_BYTES - 25];
    assert_eq!(
        encode(CLUSTER, EntryKind::Batch, &payload),
        Err(EntryError::Oversize)
    );
}

#[test]
fn proposal_context_round_trip_and_wire_order() {
    for context in [
        ProposalContext {
            proposer: 0,
            seq: u64::MAX,
        },
        ProposalContext {
            proposer: 0x0102_0304_0506_0708,
            seq: 0x1112_1314_1516_1718,
        },
    ] {
        let bytes = context.encode();
        assert_eq!(bytes.len(), 16);
        assert_eq!(&bytes[..8], &context.proposer.to_be_bytes());
        assert_eq!(&bytes[8..], &context.seq.to_be_bytes());
        assert_eq!(ProposalContext::decode(&bytes), Some(context));
    }
}

#[test]
fn proposal_context_rejects_wrong_lengths() {
    for len in [0, 1, 8, 15, 17, 32] {
        assert_eq!(ProposalContext::decode(&vec![0; len]), None);
    }
}
