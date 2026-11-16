use super::*;
const A: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
const B: &str = "4e11fa47-71ca-11e1-9e33-c80aa9429562";
fn set(suffix: &str) -> GtidSet {
    format!("{A}:{suffix}").parse().unwrap()
}
fn gtid(set: GtidSet, server: &str, offset: u64) -> MysqlPosition {
    MysqlPosition::Gtid {
        set,
        server_uuid: server.into(),
        file: "mysql-bin.000004".into(),
        offset,
    }
}
fn file(server: &str, file_seq: u64, offset: u64) -> MysqlPosition {
    MysqlPosition::File {
        server_uuid: server.into(),
        file_seq,
        offset,
    }
}

#[test]
fn parsing_canonicalizes_multiline_uuids_tags_and_intervals() {
    let input = format!(
        " {B}:9:1-3:3-8,\n {} : 7 : 1-5 : 6 : Z_2 : 4-6 : a : 3 : 1-2,\n{A}:A:4 ",
        A.to_uppercase()
    );
    let parsed: GtidSet = input.parse().unwrap();
    assert_eq!(parsed.to_string(), format!("{A}:1-7:a:1-4:z_2:4-6,{B}:1-9"));
    assert_eq!(parsed.to_string().parse::<GtidSet>().unwrap(), parsed);
    assert_eq!(" \n\t".parse::<GtidSet>().unwrap(), GtidSet::default());
    assert_eq!(
        set("9223372036854775807").to_string(),
        format!("{A}:9223372036854775807")
    );
    assert!(
        set("Domain_1:1-5")
            .contains(&format!("{A}:DOMAIN_1:3"))
            .unwrap()
    );
}

#[test]
fn malformed_sets_are_rejected() {
    for suffix in [
        "",
        "0",
        "1-0",
        "5-2",
        "1--2",
        "+1",
        "1.0",
        "1:",
        "1,,",
        ":1",
        "tag",
        "tag:other:1",
        "bad-tag:1",
        "1tag:1",
        "1 2",
        "9223372036854775808",
        "18446744073709551616",
    ] {
        assert!(
            format!("{A}:{suffix}").parse::<GtidSet>().is_err(),
            "{suffix}"
        );
    }
    for text in [
        ",",
        "not-a-uuid:1",
        "3e11fa4771ca11e19e33c80aa9429562:1",
        &format!("{A}:{}:1", "a".repeat(33)),
    ] {
        assert!(text.parse::<GtidSet>().is_err(), "{text}");
    }
}

#[test]
fn exact_set_operations_and_partial_order() {
    let mut a = set("1-5:8-12:tag:1-4");
    let b = set("3-9:11:tag:2-3");
    assert_eq!(a.difference(&b), set("1-2:10:12:tag:1:4"));
    assert_eq!(a.union(&b), set("1-12:tag:1-4"));
    assert_eq!(a.compare(&a).unwrap(), Ordering::Equal);
    assert_eq!(a.compare(&a.union(&b)).unwrap(), Ordering::Less);
    assert_eq!(a.union(&b).compare(&a).unwrap(), Ordering::Greater);
    assert!(a.compare(&b).is_err());
    assert!(GtidSet::default().is_subset(&a));
    assert!(!a.is_subset(&GtidSet::default()));
    assert!(!set("1:3").is_subset(&set("1-2")));
    assert!(!a.is_subset(&format!("{B}:1-99").parse().unwrap()));
    a.add(&format!("{A}:6")).unwrap();
    assert!(a.contains(&format!("{A}:6")).unwrap());
    assert!(!a.contains(&format!("{A}:tag:6")).unwrap());
    let before = a.clone();
    for text in ["", &format!("{A}:6-7"), &format!("{A}:6,{B}:6")] {
        assert!(a.add(text).is_err());
        assert!(a.contains(text).is_err());
        assert_eq!(a, before);
    }
    let max = set("1-9223372036854775807");
    assert_eq!(
        max.difference(&set("2-9223372036854775806")),
        set("1:9223372036854775807")
    );
    assert_eq!(max.difference(&max), GtidSet::default());
    assert_eq!(max.union(&max), max);
}

#[test]
fn real_mysql_metadata_sets_parse() {
    let mut count = 0;
    for metadata in [
        include_str!("../../../tests/fixtures/mysql/mysql80-gtid-on/metadata.txt"),
        include_str!("../../../tests/fixtures/mysql/mysql84-gtid-on/metadata.txt"),
        include_str!("../../../tests/fixtures/mysql/mysql80-gtid-off/metadata.txt"),
        include_str!("../../../tests/fixtures/mysql/mysql84-gtid-off/metadata.txt"),
    ] {
        for field in metadata
            .lines()
            .flat_map(|line| line.split('\t'))
            .filter(|s| s.len() > 36 && s.as_bytes()[36] == b':')
        {
            let parsed: GtidSet = field.parse().unwrap();
            assert_eq!(parsed.to_string(), field);
            count += 1;
        }
    }
    assert_eq!(count, 4);
}

#[test]
fn versioned_encoding_round_trips_and_rejects_bad_bytes() {
    for position in [
        gtid(set("1-5:tag:7"), A, u64::MAX),
        gtid(GtidSet::default(), B, 0),
        file(A, u64::MAX, 0),
    ] {
        let bytes = position.encode().unwrap();
        assert_eq!(MysqlPosition::decode(&bytes).unwrap(), position);
        for len in 0..bytes.len() {
            assert!(MysqlPosition::decode(&bytes[..len]).is_err());
        }
        for (index, value) in [(0, b'?'), (1, 0), (1, 2), (2, b'G')] {
            let mut bad = bytes.clone();
            bad[index] = value;
            assert!(MysqlPosition::decode(&bad).is_err());
        }
        let mut trailing = bytes;
        trailing.push(0);
        assert!(MysqlPosition::decode(&trailing).is_err());
    }
    let mut bytes = gtid(set("1-5"), A, 1).encode().unwrap();
    bytes[42] = b'3'; // uppercase UUID within the canonical set
    bytes[43] = b'E';
    assert!(MysqlPosition::decode(&bytes).is_err());
    let mut bytes = gtid(set("1-5"), A, 1).encode().unwrap();
    *bytes.get_mut(42 + 39).unwrap() = b'1'; // noncanonical 1-1 interval
    assert!(MysqlPosition::decode(&bytes).is_err());
    let mut bytes = gtid(set("1"), A, 1).encode().unwrap();
    bytes[38..42].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(MysqlPosition::decode(&bytes).is_err());
    assert!(file("bad", 0, 0).encode().is_err());
    assert_eq!(
        file(&A.to_uppercase(), 1, 2).encode().unwrap(),
        file(A, 1, 2).encode().unwrap()
    );
}

#[test]
fn every_mysql_order_rule() {
    let g = MysqlOrder::gtid("source-cluster").unwrap();
    let f = MysqlOrder::file(A).unwrap();
    assert_eq!(g.domain(), "mysql:source-cluster/gtid");
    assert_eq!(f.domain(), format!("mysql:{A}/file"));
    assert!(MysqlOrder::gtid("").is_err());
    assert!(MysqlOrder::file("bad").is_err());
    let compare = |order: &MysqlOrder, a: MysqlPosition, b: MysqlPosition| {
        order.compare(&a.encode().unwrap(), &b.encode().unwrap())
    };
    for (left, right, expected) in [
        ("1", "1", Ordering::Equal),
        ("1", "1-2", Ordering::Less),
        ("1-2", "1", Ordering::Greater),
    ] {
        assert_eq!(
            compare(&g, gtid(set(left), A, 999), gtid(set(right), B, 1)).unwrap(),
            expected
        );
    }
    assert!(compare(&g, gtid(set("1"), A, 1), gtid(set("2"), A, 2)).is_err());
    for (a, b, expected) in [
        (file(A, 1, 9), file(A, 2, 0), Ordering::Less),
        (file(A, 1, 9), file(A, 1, 8), Ordering::Greater),
        (file(A, 1, 9), file(A, 1, 9), Ordering::Equal),
    ] {
        assert_eq!(compare(&f, a, b).unwrap(), expected);
    }
    for order in [&g, &f] {
        for (a, b) in [
            (file(A, 1, 1), gtid(set("1"), A, 1)),
            (gtid(set("1"), A, 1), file(A, 1, 1)),
            (file(A, 1, 1), file(B, 1, 1)),
        ] {
            assert!(compare(order, a, b).is_err());
        }
        assert!(order.compare(&[], &[]).is_err());
    }
    assert!(compare(&g, file(A, 1, 1), file(A, 1, 1)).is_err());
    assert!(compare(&f, gtid(set("1"), A, 1), gtid(set("1"), A, 1)).is_err());
    assert!(compare(&f, file(B, 1, 1), file(B, 1, 1)).is_err());
}

#[test]
fn deterministic_randomized_interval_laws() {
    let mut seed = 0x6d7953514c_u64;
    let mut random = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    for _ in 0..128 {
        let mut sets = [GtidSet::default(), GtidSet::default(), GtidSet::default()];
        let mut oracle = [[false; 96]; 3];
        for (s, bits) in sets.iter_mut().zip(&mut oracle) {
            for _ in 0..32 {
                let key = random() as usize % 3;
                let (lo, hi) = (random() as usize % 32, random() as usize % 32);
                let (lo, hi) = (lo.min(hi), lo.max(hi));
                let prefix = [A.to_string(), format!("{A}:tag"), B.to_string()][key].clone();
                *s = s.union(&format!("{prefix}:{}-{}", lo + 1, hi + 1).parse().unwrap());
                bits[key * 32 + lo..=key * 32 + hi].fill(true);
            }
        }
        let [a, b, c] = &sets;
        assert_eq!(a.union(b), b.union(a));
        assert_eq!(a.union(&b.union(c)), a.union(b).union(c));
        assert_eq!(a.union(a), *a);
        assert_eq!(a.difference(a), GtidSet::default());
        assert_eq!(a.difference(b).union(b), a.union(b));
        assert!(a.is_subset(&a.union(b)));
        assert_eq!(a.to_string().parse::<GtidSet>().unwrap(), *a);
        assert_eq!(
            a.is_subset(b),
            oracle[0].iter().zip(oracle[1]).all(|(a, b)| !a || b)
        );
        for i in 0..96 {
            let prefix = [A.to_string(), format!("{A}:tag"), B.to_string()][i / 32].clone();
            let gtid = format!("{prefix}:{}", i % 32 + 1);
            assert_eq!(
                a.union(b).contains(&gtid).unwrap(),
                oracle[0][i] || oracle[1][i]
            );
            assert_eq!(
                a.difference(b).contains(&gtid).unwrap(),
                oracle[0][i] && !oracle[1][i]
            );
        }
    }
}
