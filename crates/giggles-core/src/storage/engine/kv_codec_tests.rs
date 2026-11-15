use super::{Keyspace, KvError, WriteBatch};

fn corrupt(bytes: &[u8]) {
    assert!(matches!(
        WriteBatch::decode(bytes),
        Err(KvError::Corrupt(_))
    ));
}

fn checksum(payload: &[u8]) -> Vec<u8> {
    let mut bytes = payload.to_vec();
    bytes.extend_from_slice(&super::super::crc32c::crc32c(payload).to_be_bytes());
    bytes
}

fn sample() -> WriteBatch {
    let mut batch = WriteBatch::default();
    batch.put(Keyspace::Meta, b"k".to_vec(), b"v".to_vec());
    batch
}

#[test]
fn empty_and_all_operations_round_trip() {
    let empty = WriteBatch::default();
    assert_eq!(WriteBatch::decode(&empty.encode()).unwrap(), empty);
    let mut batch = WriteBatch::default();
    for ks in Keyspace::ALL {
        batch.put(ks, vec![], vec![]);
        batch.put(ks, vec![0, 255], vec![255, 0, 42]);
        batch.delete(ks, vec![]);
        batch.delete(ks, b"key".to_vec());
        batch.delete_range(ks, vec![], vec![]);
        batch.delete_range(ks, b"a".to_vec(), b"z".to_vec());
    }
    let decoded = WriteBatch::decode(&batch.encode()).unwrap();
    assert_eq!(decoded, batch);
    assert_eq!(decoded.ops(), batch.clone().into_ops());
}

#[test]
fn deterministic_big_endian_wire_layout() {
    let bytes = sample().encode();
    // v1, one Put in Meta, one-byte key and value, followed by CRC32C.
    let payload = [1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, b'k', 0, 0, 0, 1, b'v'];
    assert_eq!(bytes, checksum(&payload));
    assert_eq!(bytes, sample().encode());
    assert_eq!(bytes, WriteBatch::decode(&bytes).unwrap().encode());
    for (index, ks) in Keyspace::ALL.into_iter().enumerate() {
        let mut batch = WriteBatch::default();
        batch.delete(ks, vec![]);
        batch.delete_range(ks, vec![], vec![]);
        assert_eq!(
            batch.encode(),
            checksum(&[
                1,
                0,
                0,
                0,
                2,
                2,
                index as u8,
                0,
                0,
                0,
                0,
                3,
                index as u8,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
            ])
        );
    }
}

#[test]
fn rejects_every_truncation_and_single_byte_flip() {
    let bytes = sample().encode();
    for len in 0..bytes.len() {
        corrupt(&bytes[..len]);
    }
    for index in 0..bytes.len() {
        for mask in 1..=u8::MAX {
            let mut flipped = bytes.clone();
            flipped[index] ^= mask;
            corrupt(&flipped);
        }
    }
    let mut trailing = bytes;
    trailing.push(0);
    corrupt(&trailing);
}

#[test]
fn rejects_unknown_versions_even_with_valid_checksum() {
    for version in [0, 2, 255] {
        let bytes = checksum(&[version, 0, 0, 0, 0]);
        let Err(KvError::Corrupt(message)) = WriteBatch::decode(&bytes) else {
            panic!("expected corrupt version");
        };
        assert!(message.contains("unknown batch version"));
    }
}

#[test]
fn rejects_malformed_payloads_with_valid_checksums() {
    // Resealing isolates structural validation from checksum rejection.
    let bytes = sample().encode();
    let payload = &bytes[..bytes.len() - 4];
    for (index, value) in [(5, 0), (5, 4), (5, 255), (6, 7), (6, 255)] {
        let mut invalid = payload.to_vec();
        invalid[index] = value;
        corrupt(&checksum(&invalid));
    }
    for count in [0u32, 2, u32::MAX] {
        let mut invalid = payload.to_vec();
        invalid[1..5].copy_from_slice(&count.to_be_bytes());
        corrupt(&checksum(&invalid));
    }
    for index in [7, 12] {
        let mut invalid = payload.to_vec();
        invalid[index..index + 4].copy_from_slice(&u32::MAX.to_be_bytes());
        corrupt(&checksum(&invalid));
    }
    // Truncate each payload position and replace CRC to exercise the parser.
    for len in 1..payload.len() {
        corrupt(&checksum(&payload[..len]));
    }
    let mut trailing = payload.to_vec();
    trailing.push(0);
    corrupt(&checksum(&trailing));
}
