use super::*;

fn record(state: FenceState, label: String) -> WriteFenceRecord {
    WriteFenceRecord {
        state,
        epoch: 0x0102030405060708,
        holder_digest: holder_digest(b"holder"),
        changed_at: 0x1112131415161718,
        label,
    }
}

#[test]
fn round_trip_states_and_label_limits() {
    for state in [FenceState::Fenced, FenceState::Released] {
        for label in [String::new(), "?".repeat(128)] {
            let record = record(state, label);
            let bytes = record.encode();
            assert_eq!(bytes.len(), 52 + record.label.len());
            assert_eq!(&bytes[2..10], &record.epoch.to_be_bytes());
            assert_eq!(&bytes[10..42], &record.holder_digest);
            assert_eq!(&bytes[42..50], &record.changed_at.to_be_bytes());
            assert_eq!(WriteFenceRecord::decode(&bytes).unwrap(), record);
        }
    }
}

#[test]
fn digest_matches_sha256_known_answer() {
    assert_eq!(
        holder_digest(b"abc"),
        [
            0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae,
            0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61,
            0xf2, 0x00, 0x15, 0xad
        ]
    );
}

#[test]
fn rejects_corrupt_values() {
    let good = record(FenceState::Fenced, "x".into()).encode();
    let mut cases = Vec::new();
    for (offset, value) in [(0, 0), (0, 2), (1, 0), (1, 3), (52, 0xff)] {
        let mut bytes = good.clone();
        bytes[offset] = value;
        cases.push(bytes);
    }
    let mut oversized = record(FenceState::Released, "x".repeat(256)).encode();
    oversized[50..52].copy_from_slice(&257u16.to_be_bytes());
    oversized.push(b'x');
    cases.push(oversized);
    let mut past_end = good.clone();
    past_end[50..52].copy_from_slice(&2u16.to_be_bytes());
    cases.push(past_end);
    let mut trailing = good.clone();
    trailing.push(0);
    cases.push(trailing);
    for end in 0..good.len() {
        cases.push(good[..end].to_vec());
    }
    for bytes in cases {
        assert!(
            matches!(
                WriteFenceRecord::decode(&bytes),
                Err(FormatError::Corrupt(_))
            ),
            "accepted {bytes:?}"
        );
    }
}
