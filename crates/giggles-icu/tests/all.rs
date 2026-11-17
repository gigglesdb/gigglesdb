use giggles_icu::{Collator, Icu, pg_compare};
use serde_json::Value;
use std::{cmp::Ordering, sync::Arc};
fn runtime() -> Option<Arc<Icu>> {
    match Icu::load() {
        Ok(icu) => Some(icu),
        Err(e) if std::env::var_os("GIGGLES_ICU_DIR").is_none() => {
            eprintln!("SKIP: ICU 76.1 not installed; set GIGGLES_ICU_DIR: {e}");
            None
        }
        Err(e) => panic!("configured ICU failed to load: {e}"),
    }
}
fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/oracle.json")).unwrap()
}
fn s<'a>(v: &'a Value, field: &str) -> &'a str {
    v[field].as_str().unwrap()
}
fn verify_order(icu: &Arc<Icu>, data: &Value, case: &Value, keys: bool) {
    let coll = Collator::open_with(
        icu.clone(),
        s(case, "locale"),
        (!s(case, "rules").is_empty()).then(|| s(case, "rules")),
    )
    .unwrap();
    let strings: Vec<&[u8]> = data["strings"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().as_bytes())
        .collect();
    let compare = |a: usize, b: usize| {
        pg_compare(
            &coll,
            case["det"].as_bool().unwrap(),
            strings[a],
            strings[b],
        )
    };
    let mut order: Vec<usize> = (0..strings.len()).collect();
    order.sort_by(|&a, &b| compare(a, b).then(a.cmp(&b)));
    let expected: Vec<usize> = s(case, "order")
        .split(',')
        .map(|s| s.parse().unwrap())
        .collect();
    assert_eq!(order, expected, "ordering {}", s(case, "name"));
    assert_eq!(
        coll.version(),
        s(case, "version"),
        "version {}",
        s(case, "name")
    );
    if !case["groups"].is_null() {
        let mut groups = Vec::new();
        let mut j = 0;
        while j < order.len() {
            let mut k = j + 1;
            while k < order.len() && compare(order[j], order[k]) == Ordering::Equal {
                k += 1;
            }
            if k - j > 1 {
                let mut group = order[j..k].to_vec();
                group.sort();
                groups.push(
                    group
                        .iter()
                        .map(usize::to_string)
                        .collect::<Vec<_>>()
                        .join(","),
                );
            }
            j = k;
        }
        groups.sort();
        assert_eq!(
            groups.join(";"),
            s(case, "groups"),
            "groups {}",
            s(case, "name")
        );
    }
    if keys {
        let keys: Vec<_> = strings.iter().map(|s| coll.sort_key(s)).collect();
        for pair in order.windows(2) {
            assert_eq!(
                keys[pair[0]].cmp(&keys[pair[1]]),
                coll.compare_utf8(strings[pair[0]], strings[pair[1]]),
                "key {}",
                s(case, "name")
            );
        }
    }
}
#[test]
fn catalog_orderings() {
    let Some(icu) = runtime() else { return };
    let data = fixture();
    let cases = data["base"].as_array().unwrap();
    assert_eq!(cases.len(), 871);
    for case in cases {
        verify_order(&icu, &data, case, false);
    }
    eprintln!("ICU catalog orderings: 871/871");
}
#[test]
fn option_orderings_groups_locales_and_versions() {
    let Some(icu) = runtime() else { return };
    let data = fixture();
    let cases = data["options"].as_array().unwrap();
    assert_eq!(cases.len(), 24);
    for case in cases {
        let tag = icu.canonical_language_tag(s(case, "input_locale")).unwrap();
        assert_eq!(tag.tag, s(case, "locale"));
        assert!(tag.warnings.is_empty(), "{:?}", tag.warnings);
        verify_order(&icu, &data, case, true);
    }
    eprintln!(
        "ICU options: 24/24 orders, 24/24 groups, 24/24 locales, 24/24 versions; 16776 adjacent sort-key checks"
    );
}
#[test]
fn catalog_versions() {
    let Some(icu) = runtime() else { return };
    let data = fixture();
    let cases = data["versions"].as_array().unwrap();
    assert_eq!(cases.len(), 871);
    for case in cases {
        assert_eq!(
            Collator::open_with(icu.clone(), s(case, "locale"), None)
                .unwrap()
                .version(),
            s(case, "version"),
            "{}",
            s(case, "name")
        );
    }
    eprintln!("ICU catalog versions: 871/871");
}
#[test]
fn canonical_notices_and_validation_warnings() {
    let Some(icu) = runtime() else { return };
    let data = fixture();
    let notices = data["notices"].as_array().unwrap();
    assert_eq!(notices.len(), 3);
    for case in notices {
        let result = icu.canonical_language_tag(s(case, "input")).unwrap();
        assert_eq!(result.tag, s(case, "tag"));
        assert_eq!(result.notice.as_deref(), Some(s(case, "message")));
    }
    for locale in ["C", "POSIX"] {
        assert_eq!(
            icu.canonical_language_tag(locale).unwrap().tag,
            "en-US-u-va-posix"
        );
    }
    let bad = icu.canonical_language_tag("xx-nonsense-zzzz").unwrap();
    assert_eq!(
        bad.warnings,
        [
            "could not convert locale name \"xx-nonsense-zzzz\" to language tag: U_ILLEGAL_ARGUMENT_ERROR",
            "ICU locale \"xx-nonsense-zzzz\" has unknown language \"xx\""
        ]
    );
    assert!(icu.canonical_language_tag("en\0US").is_err());
    assert_eq!(icu.canonical_language_tag("en_US").unwrap().tag, "en-US");
    eprintln!("Canonicalisation notices: 3/3; validation warnings: 2/2");
}
#[test]
fn lifecycle_threads_and_input_boundaries() {
    let Some(icu) = runtime() else { return };
    assert!(Arc::ptr_eq(&icu, &Icu::load().unwrap()));
    assert!(Icu::load_from("/nonexistent/giggles-icu-test").is_err());
    assert!(Collator::open_with(icu.clone(), "und\0bad", None).is_err());
    assert!(Collator::open_with(icu.clone(), "und", Some("&")).is_err());
    let locales = icu.available_collation_locales();
    assert!(locales.len() > 100);
    assert!(locales.contains(&"en".to_owned()));
    let coll = Arc::new(Collator::open_with(icu, "und-u-ks-level1", None).unwrap());
    let mut workers = Vec::new();
    for _ in 0..4 {
        let c = coll.clone();
        workers.push(std::thread::spawn(move || {
            for _ in 0..100 {
                assert_eq!(c.compare_utf8(b"a", b"A"), Ordering::Equal);
                assert_eq!(pg_compare(&c, true, b"a", b"A"), Ordering::Greater);
                assert_eq!(c.sort_key(b"a"), c.sort_key(b"A"));
            }
        }));
    }
    for worker in workers {
        worker.join().unwrap();
    }
    for a in [b"".as_slice(), b"a\0b", "??".as_bytes()] {
        for b in [b"".as_slice(), b"a\0c", "??".as_bytes()] {
            assert_eq!(
                coll.sort_key(a).cmp(&coll.sort_key(b)),
                coll.compare_utf8(a, b)
            );
        }
    }
}

#[path = "casing.rs"]
mod casing;

// Each subprocess starts with a fresh DEFAULT, without mutating process-wide env.
#[test]
fn configured_directory_is_exclusive() {
    const CHILD: &str = "GIGGLES_ICU_EXCLUSIVE_TEST_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let error = Icu::load().unwrap_err();
        assert!(error.to_string().contains("not-an-icu-directory"));
        return;
    }
    let exe = std::env::current_exe().unwrap();
    let output = std::process::Command::new(&exe)
        .args(["--exact", "configured_directory_is_exclusive", "--nocapture"])
        .env(CHILD, "1")
        // An executable is a file, so this path cannot be an ICU directory.
        .env("GIGGLES_ICU_DIR", exe.join("not-an-icu-directory"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
