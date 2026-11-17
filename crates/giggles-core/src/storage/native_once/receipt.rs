//! Backend-independent receipt operations. Ledger v1 encoding stays in the parent.
use super::*;

pub(super) fn certify_lease(
    ledger: Option<&NativeOutcomes>,
    lease: NativeClockLease,
) -> NativeResult<NativeClockLease> {
    certify_watermark(ledger.map_or(0, |l| l.upper_watermark_unix_seconds), lease)
}

/// Certify `lease` against an upper watermark (the v1 ledger's, or the shared
/// ledger header's `once_upper_watermark_s`).
pub(super) fn certify_watermark(
    watermark: u64,
    lease: NativeClockLease,
) -> NativeResult<NativeClockLease> {
    let now = Instant::now();
    if lease.lower_unix_seconds > lease.upper_unix_seconds
        || lease.upper_unix_seconds < watermark
        || lease.valid_until <= now
        || now
            .checked_add(Duration::from_secs(MAX_NATIVE_CLOCK_LEASE_SECONDS))
            .is_none_or(|maximum| lease.valid_until > maximum)
    {
        return Err(NativeOnceError::ClockUncertain);
    }
    Ok(lease)
}

impl NativeOutcomes {
    pub(super) fn retained(&self, key: &NativeOnceKey) -> Option<&NativeOnceRecord> {
        self.entries.get(key).map(|entry| &entry.record)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn insert_outcome(
        &mut self,
        key: NativeOnceKey,
        submitted: [u8; 32],
        accepted: [u8; 32],
        result: Vec<u8>,
        retention_seconds: u64,
        lease: NativeClockLease,
    ) -> NativeResult<()> {
        if self.entries.contains_key(&key) {
            return Err(NativeOnceError::IdempotencyConflict);
        }
        if !(1..=MAX_NATIVE_RETENTION_SECONDS).contains(&retention_seconds)
            || result.len() > MAX_NATIVE_RESULT_BYTES
            || self.entries.len() >= MAX_NATIVE_RECORDS
        {
            return Err(NativeOnceError::RetentionUnavailable);
        }
        let retry_until_seconds = lease
            .upper_unix_seconds
            .checked_add(retention_seconds)
            .ok_or(NativeOnceError::ClockUncertain)?;
        let previous_watermark = self.upper_watermark_unix_seconds;
        self.upper_watermark_unix_seconds = lease.upper_unix_seconds;
        self.entries.insert(
            key.clone(),
            Entry {
                record: NativeOnceRecord {
                    submitted_fingerprint: submitted,
                    accepted_fingerprint: accepted,
                    retry_until_seconds,
                    result: Some(result),
                },
                retention_seconds,
                decided_lower_unix_seconds: lease.lower_unix_seconds,
                decided_upper_unix_seconds: lease.upper_unix_seconds,
            },
        );
        if let Err(error) = self.validate() {
            self.entries.remove(&key);
            self.upper_watermark_unix_seconds = previous_watermark;
            return Err(if error == CoreError::Limit {
                NativeOnceError::RetentionUnavailable
            } else {
                error.into()
            });
        }
        Ok(())
    }

    pub(super) fn compact(&mut self, lease: NativeClockLease) -> usize {
        let mut count = 0;
        for entry in self.entries.values_mut() {
            if entry.record.result.is_some()
                && lease.lower_unix_seconds > entry.record.retry_until_seconds
            {
                entry.record.result = None;
                count += 1;
            }
        }
        if count != 0 {
            self.upper_watermark_unix_seconds = lease.upper_unix_seconds;
        }
        count
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{clock, terminal_payload};
    use super::*;

    #[test]
    fn legacy_v1_bytes_and_receipt_operations_are_frozen() {
        let key = NativeOnceKey {
            database_domain: 9,
            identity: vec![42],
        };
        let mut ledger = NativeOutcomes::default();
        ledger
            .insert_outcome(
                key.clone(),
                [1; 32],
                [2; 32],
                terminal_payload(),
                10,
                clock(100, 110).unwrap(),
            )
            .unwrap();
        let record = ledger.retained(&key).unwrap();
        assert_eq!(
            replay(record, [3; 32]),
            Err(NativeOnceError::IdempotencyConflict)
        );
        assert_eq!(replay(record, [1; 32]).unwrap(), *record);
        assert_eq!(ledger.compact(clock(120, 120).unwrap()), 0);
        assert_eq!(ledger.compact(clock(121, 121).unwrap()), 1);
        let mut golden = 1u16.to_be_bytes().to_vec();
        golden.extend(121u64.to_be_bytes());
        golden.extend(1u64.to_be_bytes());
        golden.extend(9u128.to_be_bytes());
        golden.extend(1u64.to_be_bytes());
        golden.push(42);
        golden.extend([1; 32]);
        golden.extend([2; 32]);
        golden.extend(120u64.to_be_bytes());
        golden.push(0); // Option::None; tombstones remain permanent entries.
        for n in [10u64, 100, 110] {
            golden.extend(n.to_be_bytes());
        }
        let mut duplicate = golden.clone();
        duplicate[10..18].copy_from_slice(&2u64.to_be_bytes());
        duplicate.extend_from_slice(&golden[18..]);
        assert!(NativeOutcomes::from_bytes(&duplicate).is_err());
        let mut future = golden.clone(); future[..2].copy_from_slice(&2u16.to_be_bytes());
        assert!(matches!(NativeOutcomes::from_bytes(&future), Err(CoreError::Unsupported)));
        assert!(matches!(NativeOutcomes::from_bytes(&vec![0; MAX_NATIVE_LEDGER_BYTES + 1]), Err(CoreError::Limit)));
        assert_eq!(ledger.to_bytes().unwrap(), golden);
        assert_eq!(
            NativeOutcomes::from_bytes(&golden)
                .unwrap()
                .to_bytes()
                .unwrap(),
            golden
        );
        assert_eq!(
            ledger.insert_outcome(
                key,
                [1; 32],
                [2; 32],
                terminal_payload(),
                10,
                clock(121, 121).unwrap()
            ),
            Err(NativeOnceError::IdempotencyConflict)
        );
        assert!(certify_lease(Some(&ledger), clock(100, 120).unwrap()).is_err());
    }
}
