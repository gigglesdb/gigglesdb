//! DD68 envelope policy for independent typed scalar, native outcome and index features.
//! Presence of typed schema/receipt descriptions counts even with no typed rows.
//! Never derive native presence from the numeric ordering of version numbers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Features {
    pub typed_scalars: bool,
    pub native_outcomes: bool,
}

impl Features {
    // 7..=10 remain reserved for GD42 secondary indexes. Application namespace
    // presence has its own outer gate, rejected by all pre-application readers.
    pub fn version_application(self, applications: bool) -> u32 {
        if applications {
            self.version() + 8
        } else {
            self.version()
        }
    }
    pub fn read_application(version: u32) -> Option<(Self, bool)> {
        match version {
            11..=14 => Self::read(version - 8).map(|f| (f, true)),
            _ => Self::read(version).map(|f| (f, false)),
        }
    }
    /// Explicit opt-in for secondary-index envelopes; existing writers stay at 3..=6.
    /// GD42 must derive this flag from namespace 8 payload presence, including
    /// empty descriptor payloads, and validate flag/payload agreement on read.
    /// This policy neither inspects payloads nor enables index persistence.
    #[allow(dead_code)] // Staged API: GD42 owns persistence adoption.
    pub fn version_with(self, secondary_indexes: bool) -> u32 {
        match (self.typed_scalars, self.native_outcomes, secondary_indexes) {
            (_, _, false) => self.version(),
            (false, false, true) => 7,
            (false, true, true) => 8,
            (true, false, true) => 9,
            (true, true, true) => 10,
        }
    }

    /// Parse all three flags explicitly. The tuple is (scalar/native features,
    /// secondary indexes). GD42 must validate namespace 8 presence, including
    /// empty descriptors, against the returned index flag before accepting data.
    /// Legacy `read` deliberately continues to reject index envelopes.
    #[allow(dead_code)] // Staged API: GD42 owns persistence adoption.
    pub fn read_with(version: u32) -> Option<(Self, bool)> {
        let features = match version {
            7 => Self {
                typed_scalars: false,
                native_outcomes: false,
            },
            8 => Self {
                typed_scalars: false,
                native_outcomes: true,
            },
            9 => Self {
                typed_scalars: true,
                native_outcomes: false,
            },
            10 => Self {
                typed_scalars: true,
                native_outcomes: true,
            },
            _ => return Self::read(version).map(|features| (features, false)),
        };
        Some((features, true))
    }

    pub fn version(self) -> u32 {
        match (self.typed_scalars, self.native_outcomes) {
            (false, false) => 3,
            (false, true) => 4,
            (true, false) => 5,
            (true, true) => 6,
        }
    }
    pub fn read(version: u32) -> Option<Self> {
        match version {
            1..=3 => Some(Self {
                typed_scalars: false,
                native_outcomes: false,
            }),
            4 => Some(Self {
                typed_scalars: false,
                native_outcomes: true,
            }),
            5 => Some(Self {
                typed_scalars: true,
                native_outcomes: false,
            }),
            6 => Some(Self {
                typed_scalars: true,
                native_outcomes: true,
            }),
            _ => None,
        }
    }
}

/// Temporal envelopes wrap a legal explicit base family. Legacy entry points
/// keep their exact acceptance sets; callers must unwrap before family dispatch.
pub(super) fn temporal_version(base: u32, temporal: bool) -> Option<u32> {
    match (base, temporal) {
        (3..=14, true) => Some(base + 12),
        (1..=14, false) => Some(base),
        _ => None,
    }
}
pub(super) fn temporal_base(version: u32) -> Option<(u32, bool)> {
    match version {
        1..=14 => Some((version, false)),
        15..=26 => Some((version - 12, true)),
        _ => None,
    }
}

/// Heap is the outermost envelope, independent of the temporal and base families.
pub(super) fn heap_version(base: u32, heap: bool) -> Option<u32> {
    match (base, heap) {
        (1..=26, true) => Some(base + 26),
        (1..=26, false) => Some(base),
        _ => None,
    }
}
pub(super) fn heap_base(version: u32) -> Option<(u32, bool)> {
    match version {
        1..=26 => Some((version, false)),
        27..=52 => Some((version - 26, true)),
        _ => None,
    }
}

/// Translate through the existing family and wrapper policies; 1/2 are aliases of 3.
pub(super) fn legacy_version_to_bits(version: u32) -> Option<u64> {
    let (base, heap) = heap_base(version)?;
    let (base, temporal) = temporal_base(base)?;
    let (f, indexed, applications) = super::secondary_index::family(base).ok()?;
    Some(u64::from(f.typed_scalars) | (u64::from(f.native_outcomes) << 1)
        | (u64::from(indexed) << 2) | (u64::from(applications) << 3)
        | (u64::from(temporal) << 4) | (u64::from(heap) << 5))
}

pub(super) fn bits_to_legacy_version(bits: u64) -> Option<u32> {
    if bits & !63 != 0 || bits & 12 == 12 {
        return None;
    }
    let f = Features {
        typed_scalars: bits & 1 != 0,
        native_outcomes: bits & 2 != 0,
    };
    let base = if bits & 8 != 0 {
        f.version_application(true)
    } else {
        f.version_with(bits & 4 != 0)
    };
    heap_version(temporal_version(base, bits & 16 != 0)?, bits & 32 != 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn heap_wrapping_does_not_authorize_legacy_namespaces() {
        use super::super::*;
        for version in [1, 2, 27, 28] {
            let mut r = records(&empty(17)).unwrap();
            let mut m: Metadata = decode(&r[&vec![0]]).unwrap();
            m.format = version;
            r.insert(vec![0], encode(&m).unwrap());
            if version == 1 || version == 27 { r.remove(&vec![5]); }
            if version <= 2 {
                assert!(from_records(r.clone()).is_ok());
                // A malformed accounting record must be rejected as a namespace,
                // without reaching its payload decoder on either legacy base.
                r.insert(key(6, &[1]), vec![255]);
                assert!(matches!(from_records(r), Err(CoreError::Corrupt(message))
                    if message == "unknown record namespace or key length"));
            } else {
                assert!(matches!(from_records(r), Err(CoreError::Corrupt(message))
                    if message == "heap format mismatch"));
            }
        }
    }

    #[test]
    fn heap_metadata_presence_and_feature_families_roundtrip() {
        use super::super::*;
        use crate::relational::Prepared;
        for heap in [false, true] {
            for typed in [false, true] {
                for temporal in [false, true] {
                    for native in [false, true] {
                        for (indexed, applications) in [(false, false), (true, false), (false, true)] {
                            let mut s = empty(123);
                            let pk = if heap { "" } else { " PRIMARY KEY" };
                            let extra = if temporal { ",d DATE" } else { "" };
                            let numeric = if typed { ",n NUMERIC" } else { "" };
                            let sql = format!("CREATE TABLE t(id BIGINT{pk},tag TEXT{extra}{numeric})");
                            let p = Prepared::parse(&sql, 123, s.sql.epoch).unwrap();
                            s.sql.execute(&p, &[], None).unwrap();
                            if indexed {
                                let t = s.sql.tables["t"].clone();
                                s.sql.secondary_configure_test(t.id, "by_tag", &[t.columns[1].id], || false).unwrap();
                            }
                            if native { s.native_outcomes = Some(Default::default()); }
                            if applications { s.applications = Some(Default::default()); }
                            let r = records(&s).unwrap();
                            let mut m: Metadata = decode(&r[&vec![0]]).unwrap();
                            let f = Features { typed_scalars: typed, native_outcomes: native };
                            let base = if applications { f.version_application(true) } else { f.version_with(indexed) };
                            assert_eq!(m.format, base + if temporal { 12 } else { 0 } + if heap { 26 } else { 0 });
                            assert_eq!(envelope_family(m.format).unwrap(), (f, indexed, applications, temporal, heap));
                            let restored = from_records(r.clone()).unwrap();
                            assert_eq!(restored.sql, s.sql);
                            assert_eq!(records(&restored).unwrap(), r);
                            let mut forged = r.clone();
                            m.format = if heap { m.format - 26 } else { m.format + 26 };
                            forged.insert(vec![0], encode(&m).unwrap());
                            assert!(matches!(from_records(forged), Err(CoreError::Corrupt(message)) if message == "heap format mismatch"));
                            // Heap must not weaken the existing index/application refusal.
                            if indexed {
                                s.applications = Some(Default::default());
                                assert!(matches!(records(&s), Err(CoreError::Unsupported)));
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn heap_wrapper_is_finite_and_composes_after_temporal() {
        for base in 1..=26 {
            assert_eq!(heap_version(base, false), Some(base));
            assert_eq!(heap_version(base, true), Some(base + 26));
            assert_eq!(heap_base(base), Some((base, false)));
            assert_eq!(heap_base(base + 26), Some((base, true)));
            assert!(temporal_base(base + 26).is_none());
            assert!(Features::read(base + 26).is_none());
            assert!(Features::read_with(base + 26).is_none());
            assert!(Features::read_application(base + 26).is_none());
        }
        for invalid in [0, 27, 52, 53, u32::MAX] {
            assert_eq!(heap_version(invalid, false), None);
            assert_eq!(heap_version(invalid, true), None);
        }
        for invalid in [0, 53, u32::MAX] {
            assert_eq!(heap_base(invalid), None);
        }
        for typed_scalars in [false, true] {
            for native_outcomes in [false, true] {
                let f = Features { typed_scalars, native_outcomes };
                for (indexed, applications) in [(false, false), (true, false), (false, true)] {
                    let base = if applications { f.version_application(true) } else { f.version_with(indexed) };
                    for temporal in [false, true] {
                        for heap in [false, true] {
                            let v = heap_version(temporal_version(base, temporal).unwrap(), heap).unwrap();
                            assert_eq!(super::super::envelope_family(v).unwrap(),
                                (f, indexed, applications, temporal, heap));
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn preheap_envelope_gate_rejects_before_relational_decode() {
        use super::super::*;
        fn old_reader(r: &std::collections::BTreeMap<Vec<u8>, Vec<u8>>) -> Result<(), CoreError> {
            let m: Metadata = decode(&r[&vec![0]])?;
            let (base, _) = temporal_base(m.format).ok_or(CoreError::Unsupported)?;
            secondary_index::family(base)?;
            let _: crate::relational::Relational = decode(&r[&vec![5]])?;
            Ok(())
        }
        for version in 27..=52 {
            let mut r = records(&empty(17)).unwrap();
            let mut m: Metadata = decode(&r[&vec![0]]).unwrap();
            m.format = version;
            r.insert(vec![0], encode(&m).unwrap());
            r.insert(vec![5], vec![255]);
            assert!(matches!(old_reader(&r), Err(CoreError::Unsupported)));
        }
        for version in [53, u32::MAX] {
            let mut r = records(&empty(17)).unwrap();
            let mut m: Metadata = decode(&r[&vec![0]]).unwrap();
            m.format = version;
            r.insert(vec![0], encode(&m).unwrap());
            r.insert(vec![5], vec![255]);
            assert!(matches!(old_reader(&r), Err(CoreError::Unsupported)));
            if version == 53 {
                assert!(matches!(from_records(r), Err(CoreError::Corrupt(_))));
            } else {
                assert!(matches!(from_records(r), Err(CoreError::Unsupported)));
            }
        }
    }

    #[test]
    fn temporal_index_nonkey_typed_native_matrix_preserves_paired_records() {
        use super::super::*;
        use crate::relational::Prepared;
        for typed in [false, true] {
            for native in [false, true] {
                for rows in [false, true] {
                    let mut s = empty(123);
                    let create = if typed {
                        "CREATE TABLE t(id BIGINT PRIMARY KEY,tag TEXT,d DATE,n NUMERIC)"
                    } else {
                        "CREATE TABLE t(id BIGINT PRIMARY KEY,tag TEXT,d DATE)"
                    };
                    let p = Prepared::parse(create, 123, s.sql.epoch).unwrap();
                    s.sql.execute(&p, &[], None).unwrap();
                    if rows {
                        let insert = if typed {
                            "INSERT INTO t VALUES(1,'a','2000-01-01',1.25)"
                        } else {
                            "INSERT INTO t VALUES(1,'a','2000-01-01')"
                        };
                        let p = Prepared::parse(insert, 123, s.sql.epoch).unwrap();
                        s.sql.execute(&p, &[], None).unwrap();
                    }
                    let t = s.sql.tables["t"].clone();
                    s.sql
                        .secondary_configure_test(t.id, "by_tag", &[t.columns[1].id], || false)
                        .unwrap();
                    if native {
                        s.native_outcomes = Some(Default::default());
                    }
                    let r = records(&s).unwrap();
                    let m: Metadata = decode(&r[&vec![0]]).unwrap();
                    assert_eq!(m.format, 19 + u32::from(native) + 2 * u32::from(typed));
                    assert!(r.contains_key(&vec![5]) && r.contains_key(&vec![8]));
                    let restored = from_records(r.clone()).unwrap();
                    assert_eq!(restored.sql, s.sql);
                    assert_eq!(records(&restored).unwrap(), r);
                    s.applications = Some(Default::default());
                    assert!(matches!(records(&s), Err(CoreError::Unsupported)));
                }
            }
        }
    }
    #[test]
    fn temporal_wrapper_has_independent_finite_base_map() {
        for base in 3..=14 {
            assert_eq!(temporal_version(base, true), Some(base + 12));
            assert_eq!(temporal_base(base + 12), Some((base, true)));
            assert!(Features::read(base + 12).is_none());
            assert!(Features::read_with(base + 12).is_none());
            assert!(Features::read_application(base + 12).is_none());
        }
        for base in 1..=14 {
            assert_eq!(temporal_base(base), Some((base, false)));
            assert_eq!(temporal_version(base, false), Some(base));
        }
        for v in [0, 53, u32::MAX] {
            assert_eq!(temporal_base(v), None);
        }
        for v in [0, 1, 2, 15, u32::MAX] {
            assert_eq!(temporal_version(v, true), None);
        }
    }

    #[test]
    fn all_three_flags_have_fixed_unique_versions() {
        let cases = [
            (false, false, false, 3),
            (false, true, false, 4),
            (true, false, false, 5),
            (true, true, false, 6),
            (false, false, true, 7),
            (false, true, true, 8),
            (true, false, true, 9),
            (true, true, true, 10),
        ];
        let mut versions = std::collections::BTreeSet::new();
        for (typed_scalars, native_outcomes, indexes, version) in cases {
            let features = Features {
                typed_scalars,
                native_outcomes,
            };
            assert_eq!(features.version_with(indexes), version);
            assert!(versions.insert(version));
            assert_eq!(Features::read_with(version), Some((features, indexes)));
            if !indexes {
                assert_eq!(features.version(), version);
                assert_eq!(Features::read(version), Some(features));
            }
        }
        for version in [1, 2] {
            assert_eq!(
                Features::read_with(version),
                Some((
                    Features {
                        typed_scalars: false,
                        native_outcomes: false,
                    },
                    false
                ))
            );
        }
        for version in [0, 11, u32::MAX] {
            assert_eq!(Features::read_with(version), None);
            assert_eq!(Features::read(version), None);
        }
    }

    #[test]
    fn frozen_preindex_entrypoint_rejects_index_versions_before_payload() {
        for version in 7..=10 {
            assert_eq!(Features::read(version), None);
            let mut records = super::super::records(&super::super::empty(17)).unwrap();
            let mut metadata: super::super::Metadata =
                super::super::decode(&records[&vec![0]]).unwrap();
            metadata.format = version;
            records.insert(vec![0], super::super::encode(&metadata).unwrap());
            // Freeze the actual preindex family gate before any payload decode.
            records.insert(vec![5], vec![255]);
            records.insert(vec![7], vec![255]);
            assert!(matches!(
                frozen_preindex_reader(records),
                Err(super::super::CoreError::Unsupported)
            ));
        }
        fn frozen_preindex_reader(
            records: std::collections::BTreeMap<Vec<u8>, Vec<u8>>,
        ) -> Result<(), super::super::CoreError> {
            let metadata: super::super::Metadata = super::super::decode(&records[&vec![0]])?;
            Features::read_application(metadata.format)
                .ok_or(super::super::CoreError::Unsupported)?;
            let _: crate::relational::Relational = super::super::decode(&records[&vec![5]])?;
            Ok(())
        }
    }
    #[test]
    fn independent_features_and_legacy_reads() {
        for typed_scalars in [false, true] {
            for native_outcomes in [false, true] {
                let f = Features {
                    typed_scalars,
                    native_outcomes,
                };
                assert_eq!(Features::read(f.version()), Some(f));
            }
        }
        for version in [1, 2, 3] {
            assert_eq!(
                Features::read(version),
                Some(Features {
                    typed_scalars: false,
                    native_outcomes: false
                })
            );
        }
        for version in [0, 7, u32::MAX] {
            assert_eq!(Features::read(version), None);
        }
    }
    #[test]
    fn old_reader_rejects_typed_envelope_before_payload() {
        for native_outcomes in [false, true] {
            let version = Features {
                typed_scalars: true,
                native_outcomes,
            }
            .version();
            // Existing reader's complete acceptance set. Payload need never decode.
            assert!(![1, 2, 3, 4].contains(&version));
            // Exercise the current reader: a corrupt relational payload must
            // never be consulted for a future envelope version.
            let mut records = super::super::records(&super::super::empty(17)).unwrap();
            let mut metadata: super::super::Metadata =
                super::super::decode(records.get(&vec![0]).unwrap()).unwrap();
            metadata.format = version;
            // Frozen pre-DD68 acceptance gate, before touching either payload.
            fn legacy_reader(
                records: &std::collections::BTreeMap<Vec<u8>, Vec<u8>>,
            ) -> Result<(), super::super::CoreError> {
                let m: super::super::Metadata = super::super::decode(&records[&vec![0]])?;
                if ![1, 2, 3, 4].contains(&m.format) {
                    return Err(super::super::CoreError::Unsupported);
                }
                let _: crate::relational::Relational = super::super::decode(&records[&vec![5]])?;
                Ok(())
            }
            records.insert(vec![0], super::super::encode(&metadata).unwrap());
            records.insert(vec![5], vec![255]);
            records.insert(vec![7], vec![255]);
            assert!(matches!(
                legacy_reader(&records),
                Err(super::super::CoreError::Unsupported)
            ));
            metadata.format = version + 100;
            records.insert(vec![0], super::super::encode(&metadata).unwrap());
            records.insert(vec![5], vec![255]);
            assert!(matches!(
                super::super::from_records(records),
                Err(super::super::CoreError::Unsupported)
            ));
        }
    }
}
