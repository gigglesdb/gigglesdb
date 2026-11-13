use super::*;

#[test]
fn cascade_candidates_match_scan_at_boundaries() {
    for action in ["CASCADE", "SET NULL", "RESTRICT"] {
        for n in [1, 10, 200, 201] {
            let mut results = Vec::new();
            for disabled in [false, true] {
                let f = Fixture::new();
                let mut s = f.admin().sql_session();
                for sql in ["CREATE TABLE p(id bigint PRIMARY KEY)".to_string(),
                    format!("CREATE TABLE c(id bigint PRIMARY KEY,p bigint REFERENCES p(id) ON DELETE {action})"),
                    format!("INSERT INTO p VALUES {}", (1..=220).map(|i| format!("({i})")).collect::<Vec<_>>().join(",")),
                    "INSERT INTO c VALUES(1,1)".to_string()] {
                    s.execute(&sql, &[], None).unwrap();
                }
                DISABLE_MUTATION_POINTS.set(disabled);
                MUTATION_POINT_READS.set(0);
                let sql = format!("DELETE FROM p WHERE id IN ({}) RETURNING id",
                    (1..=n).rev().map(|i| i.to_string()).collect::<Vec<_>>().join(","));
                let result = s.execute(&sql, &[], None);
                let reads = MUTATION_POINT_READS.get();
                DISABLE_MUTATION_POINTS.set(false);
                if action != "RESTRICT" {
                    assert_eq!(reads > 0, !disabled && n <= 200);
                }
                let parents = s.execute("SELECT * FROM p ORDER BY id", &[], None).unwrap();
                let children = s.execute("SELECT * FROM c ORDER BY id", &[], None).unwrap();
                results.push((format!("{result:?}"), format!("{parents:?}"), format!("{children:?}")));
            }
            assert_eq!(results[0], results[1], "{action} {n}");
        }
    }
}

#[test]
fn cascade_null_missing_deferred_and_own_child_match_legacy() {
    for deferred in [false, true] {
        let f = Fixture::new();
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let path = std::env::temp_dir().join(format!("f12-legacy-{:032x}", u128::from_be_bytes(nonce)));
        let legacy = Admin::create_engine(&path, Limits::default(), Engine::Legacy).unwrap();
        let mut sessions = [f.admin().sql_session(), legacy.sql_session()];
        let mut observations = Vec::new();
        for s in &mut sessions {
            for sql in ["CREATE TABLE p(id bigint PRIMARY KEY)".to_string(),
                format!("CREATE TABLE c(id bigint PRIMARY KEY,p bigint REFERENCES p(id) ON DELETE CASCADE {})", if deferred { "DEFERRABLE INITIALLY DEFERRED" } else { "" }),
                "INSERT INTO p VALUES(1),(2),(3)".into(), "BEGIN".into(),
                "INSERT INTO c VALUES(1,1)".into()] {
                s.execute(&sql, &[], None).unwrap();
            }
            let result = s.execute("DELETE FROM p WHERE id IN(3,NULL,999,1,1) RETURNING id", &[], None).unwrap();
            let rows = result.rows.into_iter().map(|r| r.values).collect::<Vec<_>>();
            let children = s.execute("SELECT * FROM c ORDER BY id", &[], None).unwrap().rows.into_iter().map(|r| r.values).collect::<Vec<_>>();
            s.execute("COMMIT", &[], None).unwrap();
            observations.push((rows, children));
        }
        assert_eq!(observations[0], observations[1]);
        assert!(observations[0].1.is_empty());
        drop(sessions);
        drop(legacy);
        std::fs::remove_dir_all(path).unwrap();
    }
}
