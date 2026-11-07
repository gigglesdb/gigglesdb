use super::*;

fn forged(bits: u64) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut r = records(&empty(17)).unwrap();
    let mut m: Metadata = decode(&r[&vec![0]]).unwrap();
    m.format = 53;
    r.insert(vec![0], encode(&m).unwrap());
    r.insert(vec![10], bits.to_be_bytes().to_vec());
    r
}
fn normal(r: BTreeMap<Vec<u8>, Vec<u8>>) -> Result<State, CoreError> {
    from_records_mode(r, &mut || false, false)
}
#[test]
fn feature_record_precedes_payload_and_is_canonical() {
    let mut r = forged(1 << 10);
    r.insert(vec![5], vec![255]);
    assert!(matches!(normal(r), Err(CoreError::Unsupported)));
    for bits in [0, 1, 63, 64, 128, 256, 320, 512] {
        assert!(
            matches!(normal(forged(bits)), Err(CoreError::Corrupt(_))),
            "{bits}"
        );
    }
    let mut r = forged(64);
    r.remove(&vec![10]);
    assert!(matches!(normal(r), Err(CoreError::Corrupt(_))));
    let mut r = forged(64);
    r.insert(vec![10], vec![0; 7]);
    assert!(matches!(normal(r), Err(CoreError::Corrupt(_))));
    let mut r = records(&empty(17)).unwrap();
    r.insert(vec![10], vec![0; 8]);
    assert!(matches!(normal(r), Err(CoreError::Corrupt(_))));
    let mut r = records(&empty(17)).unwrap();
    r.insert(key(11, &[1, 2]), vec![255]);
    assert!(matches!(normal(r), Err(CoreError::Corrupt(_))));
    assert!(matches!(
        from_records(forged(512)),
        Err(CoreError::Unsupported)
    ));
    assert!(matches!(from_records(forged(512 | 256)), Err(CoreError::Unsupported)));
    let mut r = forged(320);
    r.insert(key(11, &[1, 2]), vec![255]);
    assert!(matches!(normal(r), Err(CoreError::Corrupt(_))));
}
#[test]
fn legacy_mapping_canonicalizes_only_aliases() {
    for version in 1..=52 {
        let bits = format_policy::legacy_version_to_bits(version).unwrap();
        let canonical = format_policy::bits_to_legacy_version(bits).unwrap();
        let expected = match version {
            1 | 2 => 3,
            27 | 28 => 29,
            _ => version,
        };
        assert_eq!(canonical, expected);
        assert_eq!(format_policy::legacy_version_to_bits(canonical), Some(bits));
    }
    for version in [0, 53, u32::MAX] {
        assert_eq!(format_policy::legacy_version_to_bits(version), None);
    }
    for bits in [12, 63, 64, u64::MAX] {
        assert_eq!(format_policy::bits_to_legacy_version(bits), None);
    }
}
#[test]
fn legacy_writers_preserve_record_maps() {
    use crate::relational::Prepared;
    for (applications, indexed, temporal, heap) in [
        (false, false, false, false),
        (true, false, false, false),
        (false, true, false, false),
        (false, false, true, false),
        (false, false, false, true),
    ] {
        let mut s = empty(17);
        if applications {
            s.applications = Some(Default::default());
        }
        let pk = if heap { "" } else { " PRIMARY KEY" };
        let date = if temporal { ",d DATE" } else { "" };
        let p = Prepared::parse(
            &format!("CREATE TABLE t(id BIGINT{pk},tag TEXT{date})"),
            17,
            s.sql.epoch,
        )
        .unwrap();
        s.sql.execute(&p, &[], None).unwrap();
        if indexed {
            let t = s.sql.tables["t"].clone();
            s.sql
                .secondary_configure_test(t.id, "by_tag", &[t.columns[1].id], || false)
                .unwrap();
        }
        let r = records(&s).unwrap();
        let f = format_policy::Features {
            typed_scalars: false,
            native_outcomes: false,
        };
        let base = if applications {
            f.version_application(true)
        } else {
            f.version_with(indexed)
        };
        let version = format_policy::heap_version(
            format_policy::temporal_version(base, temporal).unwrap(),
            heap,
        )
        .unwrap();
        let m: Metadata = decode(&r[&vec![0]]).unwrap();
        assert_eq!(m.format, version);
        let mut previous = BTreeMap::new();
        previous.insert(
            vec![0],
            encode(&Metadata {
                format: version,
                domain: s.view.domain,
                schema: s.view.schema,
                last_time: s.last_time,
                next_approval: s.next_approval,
                origin: s.origin,
            })
            .unwrap(),
        );
        if indexed {
            let (base, payload) = secondary_index::persist(&s.sql).unwrap();
            previous.insert(vec![5], base);
            previous.insert(vec![8], payload);
        } else {
            previous.insert(vec![5], encode(&s.sql).unwrap());
        }
        if let Some(a) = &s.applications {
            previous.insert(vec![9], a.bytes().unwrap());
        }
        assert_eq!(r, previous);
        assert_eq!(records(&from_records(r.clone()).unwrap()).unwrap(), r);
    }
}
#[test]
fn export_refuses_role_bootstrap_bits_without_panicking() {
    assert!(matches!(
        export_allowed(ROLE_BOOTSTRAP | 64),
        Err(CoreError::Unsupported)
    ));
    assert!(matches!(export_allowed(ROLE_BOOTSTRAP), Err(CoreError::Unsupported)));
    assert!(export_allowed(64 | 128 | 256).is_ok());
    assert!(export_allowed(0).is_ok());
}
