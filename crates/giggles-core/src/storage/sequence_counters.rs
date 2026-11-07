//! Namespace-11 durable sequence reservation records.
use super::*;
use sequences::SeqCounter;

const KEY_LEN: usize = 25;
const VALUE_LEN: usize = 25;

fn corrupt() -> CoreError {
    CoreError::Corrupt("sequence counter invariant".into())
}
fn key(id: u64, incarnation: u128) -> Vec<u8> {
    let mut key = Vec::with_capacity(KEY_LEN);
    key.push(11);
    key.extend(id.to_be_bytes());
    key.extend(incarnation.to_be_bytes());
    key
}
pub(super) fn validate(s: &State) -> Result<(), CoreError> {
    if s.seq.len() > relational::catalog_ext::MAX_EXT_SEQUENCES {
        return Err(corrupt());
    }
    for (&id, c) in &s.seq {
        let def = s.sql.catalog_ext().sequences.get(&id).ok_or_else(corrupt)?;
        if c.incarnation != def.incarnation
            || !(def.min..=def.max).contains(&c.last_value)
            || !(def.min..=def.max).contains(&c.log_hw)
            || (!c.is_called && c.last_value != c.log_hw)
        {
            return Err(corrupt());
        }
    }
    Ok(())
}
pub(super) fn write(s: &State, out: &mut BTreeMap<Vec<u8>, Vec<u8>>) -> Result<(), CoreError> {
    validate(s)?;
    for (&id, c) in &s.seq {
        // Recovery resumes beyond the whole reserved run. Runtime actual values
        // remain solely in Inner's matching-incarnation live overlay.
        let value = if c.is_called { c.log_hw } else { c.last_value };
        out.insert(
            key(id, c.incarnation),
            encode(&(value, c.is_called, c.log_hw, c.stamp))?,
        );
    }
    Ok(())
}
pub(super) fn read(s: &mut State, key: &[u8], value: &[u8]) -> Result<(), CoreError> {
    if key.len() != KEY_LEN || key.first() != Some(&11) || value.len() != VALUE_LEN {
        return Err(corrupt());
    }
    let id = u64::from_be_bytes(key[1..9].try_into().expect("checked key length"));
    let incarnation = u128::from_be_bytes(key[9..KEY_LEN].try_into().expect("checked key length"));
    if s.sql
        .catalog_ext()
        .sequences
        .get(&id)
        .is_none_or(|d| d.incarnation != incarnation)
        || s.seq.contains_key(&id)
    {
        return Err(corrupt());
    }
    let (last_value, is_called, log_hw, stamp) = decode(value)?;
    if last_value != log_hw {
        return Err(corrupt());
    }
    s.seq.insert(
        id,
        SeqCounter {
            incarnation,
            last_value,
            is_called,
            log_hw,
            stamp,
        },
    );
    validate(s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use relational::{Prepared, StatementClock, Value};

    fn fixture() -> (State, u64) {
        let mut s = empty(17);
        let p = Prepared::parse("CREATE TABLE counter_codec (id serial)", 17, 0).unwrap();
        s.sql.execute(&p, &[], None).unwrap();
        let id = *s.sql.catalog_ext().sequences.keys().next().unwrap();
        sequences::next(&mut s, id).unwrap();
        (s, id)
    }
    fn disk(admin: &Admin) -> State {
        let tx = admin.inner.db(LEGACY_STORAGE).unwrap().begin_read().unwrap();
        let table = tx.open_table(RECORDS).unwrap();
        let records = table
            .iter()
            .unwrap()
            .map(|item| {
                let (key, value) = item.unwrap();
                (key.value().to_vec(), value.value().to_vec())
            })
            .collect();
        from_records_mode(records, &mut || false, false).unwrap()
    }
    fn clock() -> StatementClock {
        StatementClock {
            unavailable: None,
            transaction_us: 1,
            statement_us: 1,
            zone: None,
            wall: None,
        }
    }
    fn run(s: &mut SqlSession, sql: &str) -> relational::SqlResult {
        s.execute_at(sql, &[], None, &clock()).unwrap()
    }
    fn scalar(s: &mut SqlSession, sql: &str) -> i64 {
        match &run(s, sql).rows[0].values[0] {
            Value::Int(v) => *v,
            _ => panic!("expected integer"),
        }
    }
    fn database(label: &str) -> (std::path::PathBuf, Admin) {
        let mut token = [0u8; 16];
        getrandom::fill(&mut token).unwrap();
        let path = std::env::temp_dir().join(format!(
            "sequence-{label}-{:032x}",
            u128::from_be_bytes(token)
        ));
        let admin = Admin::create(&path, Limits::default()).unwrap();
        (path, admin)
    }
    #[test]
    fn codec_roundtrip_rejects_unknown_incarnation_keys_and_trailing_bytes() {
        let (s, id) = fixture();
        let records = records(&s).unwrap();
        let restored = from_records_mode(records.clone(), &mut || false, false).unwrap();
        assert_eq!(restored.seq[&id].last_value, s.seq[&id].log_hw);
        assert_eq!(restored.seq[&id].stamp, s.seq[&id].stamp);
        let counter_key = key(id, s.seq[&id].incarnation);
        let payload = records[&counter_key].clone();
        for bad_key in [
            key(id + 1000, s.seq[&id].incarnation),
            key(id, s.seq[&id].incarnation + 1),
            vec![11; 17],
            vec![11; 24],
            vec![11; 26],
        ] {
            let mut bad = records.clone();
            bad.remove(&counter_key);
            bad.insert(bad_key, payload.clone());
            assert!(matches!(
                from_records_mode(bad, &mut || false, false),
                Err(CoreError::Corrupt(_))
            ));
        }
        for bad_payload in [
            vec![],
            {
                let mut v = payload.clone();
                v.push(0);
                v
            },
            {
                let mut v = payload.clone();
                v[8] = 2;
                v
            },
        ] {
            let mut bad = records.clone();
            bad.insert(counter_key.clone(), bad_payload);
            assert!(matches!(
                from_records_mode(bad, &mut || false, false),
                Err(CoreError::Corrupt(_))
            ));
        }
    }
    #[test]
    fn every_returned_value_has_a_safe_disk_reservation_without_generation_bumps() {
        let (path, admin) = database("disk-witness");
        let mut session = admin.sql_session();
        run(
            &mut session,
            "CREATE TABLE witness (id serial, n integer DEFAULT 4)",
        );
        let id = *disk(&admin)
            .sql
            .catalog_ext()
            .sequences
            .keys()
            .next()
            .unwrap();
        let generation = disk(&admin).sql.generation;
        for expected in 1..=75 {
            assert_eq!(
                scalar(&mut session, "SELECT nextval('witness_id_seq')"),
                expected
            );
            let mut recovered = disk(&admin);
            assert!(sequences::next(&mut recovered, id).unwrap() > expected);
            assert_eq!(recovered.sql.generation, generation);
            assert_eq!(admin.inner.read().unwrap().seq[&id].last_value, expected);
        }
        run(&mut session, "INSERT INTO witness VALUES (1000, 9)");
        assert_eq!(scalar(&mut session, "SELECT nextval('witness_id_seq')"), 76);
        assert!(sequences::next(&mut disk(&admin), id).unwrap() > 76);
        drop(session);
        drop(admin);
        let admin = Admin::open(&path, Limits::default()).unwrap();
        assert!(scalar(&mut admin.sql_session(), "SELECT nextval('witness_id_seq')") > 76);
        drop(admin);
        fs::remove_dir_all(path).unwrap();
    }
    #[test]
    fn failed_statement_refreshes_staged_counters_without_failed_row_changes() {
        let (path, admin) = database("failed-stage");
        let mut session = admin.sql_session();
        run(
            &mut session,
            "CREATE TABLE staged_counter (id serial PRIMARY KEY)",
        );
        run(&mut session, "INSERT INTO staged_counter VALUES (2)");
        run(&mut session, "BEGIN");
        assert!(
            session
                .execute_at(
                    "INSERT INTO staged_counter VALUES (DEFAULT), (2)",
                    &[],
                    None,
                    &clock()
                )
                .is_err()
        );
        let staged = session.staged.as_ref().unwrap();
        let id = *staged.sql.catalog_ext().sequences.keys().next().unwrap();
        assert_eq!(staged.seq[&id].last_value, 1);
        let mut accepted = staged.sql.clone();
        let query = Prepared::parse(
            "SELECT id FROM staged_counter WHERE id = 1",
            staged.view.domain,
            accepted.epoch,
        )
        .unwrap();
        assert!(accepted.execute(&query, &[], None).unwrap().rows.is_empty());
        assert!(sequences::next(&mut disk(&admin), id).unwrap() > 1);
        session.rollback();
        assert_eq!(
            scalar(&mut session, "SELECT nextval('staged_counter_id_seq')"),
            2
        );
        assert!(admin.drop_table("staged_counter", false).unwrap());
        assert!(disk(&admin).seq.is_empty());
        drop(session);
        drop(admin);
        fs::remove_dir_all(path).unwrap();
    }
    #[test]
    fn descending_reservations_and_lower_setval_survive_stale_commit_and_rollback() {
        let (path, admin) = database("descending-stamps");
        let mut a = admin.sql_session();
        let mut b = admin.sql_session();
        run(
            &mut a,
            "CREATE TABLE down_counter (id bigint GENERATED BY DEFAULT AS IDENTITY (START WITH -1 INCREMENT BY -1 MINVALUE -1000 MAXVALUE -1))",
        );
        let id = *disk(&admin)
            .sql
            .catalog_ext()
            .sequences
            .keys()
            .next()
            .unwrap();
        for expected in 1..=75 {
            assert_eq!(
                scalar(&mut a, "SELECT nextval('down_counter_id_seq')"),
                -expected
            );
            assert!(sequences::next(&mut disk(&admin), id).unwrap() < -expected);
        }
        run(&mut a, "CREATE TABLE lower_counter (id serial)");
        let lower = disk(&admin)
            .sql
            .catalog_ext()
            .sequences
            .iter()
            .find(|(_, d)| d.name == "lower_counter_id_seq")
            .map(|(&id, _)| id)
            .unwrap();
        assert_eq!(
            scalar(&mut a, "SELECT setval('lower_counter_id_seq', 50)"),
            50
        );
        run(&mut a, "BEGIN");
        assert_eq!(
            scalar(&mut b, "SELECT setval('lower_counter_id_seq', 7)"),
            7
        );
        run(&mut a, "COMMIT");
        assert_eq!(disk(&admin).seq[&lower].last_value, 7);
        assert_eq!(scalar(&mut a, "SELECT nextval('lower_counter_id_seq')"), 8);
        run(&mut a, "BEGIN");
        assert_eq!(
            scalar(&mut a, "SELECT setval('lower_counter_id_seq', 3, false)"),
            3
        );
        assert_eq!(scalar(&mut a, "SELECT nextval('lower_counter_id_seq')"), 3);
        run(&mut a, "ROLLBACK");
        assert!(sequences::next(&mut disk(&admin), lower).unwrap() > 3);
        drop(a);
        drop(b);
        drop(admin);
        let admin = Admin::open(&path, Limits::default()).unwrap();
        assert!(
            scalar(
                &mut admin.sql_session(),
                "SELECT nextval('lower_counter_id_seq')"
            ) > 3
        );
        drop(admin);
        fs::remove_dir_all(path).unwrap();
    }
    #[test]
    fn failed_statement_refreshes_pending_created_counters_without_durable_orphans() {
        let (path, admin) = database("pending-stage");
        let mut session = admin.sql_session();
        run(&mut session, "BEGIN");
        run(
            &mut session,
            "CREATE TABLE pending_counter (id serial PRIMARY KEY)",
        );
        assert!(
            session
                .execute_at(
                    "INSERT INTO pending_counter VALUES (DEFAULT), (1)",
                    &[],
                    None,
                    &clock()
                )
                .is_err()
        );
        let staged = session.staged.as_ref().unwrap();
        let id = *staged.sql.catalog_ext().sequences.keys().next().unwrap();
        assert_eq!(staged.seq[&id].last_value, 1);
        assert!(disk(&admin).seq.is_empty());
        session.rollback();
        assert!(disk(&admin).sql.catalog_ext().sequences.is_empty());
        drop(session);
        drop(admin);
        fs::remove_dir_all(path).unwrap();
    }
    #[test]
    fn cycling_reservations_preserve_live_order_and_restart_at_cycle_boundary() {
        let (path, admin) = database("cycle-reservation");
        let mut session = admin.sql_session();
        run(
            &mut session,
            "CREATE TABLE cycle_counter (id integer GENERATED BY DEFAULT AS IDENTITY (START WITH 1 MINVALUE 1 MAXVALUE 3 CYCLE))",
        );
        let id = *disk(&admin)
            .sql
            .catalog_ext()
            .sequences
            .keys()
            .next()
            .unwrap();
        for expected in [1, 2, 3, 1, 2, 3, 1] {
            assert_eq!(
                scalar(&mut session, "SELECT nextval('cycle_counter_id_seq')"),
                expected
            );
            assert_eq!(disk(&admin).seq[&id].last_value, 3);
        }
        drop(session);
        drop(admin);
        let admin = Admin::open(&path, Limits::default()).unwrap();
        assert_eq!(
            scalar(
                &mut admin.sql_session(),
                "SELECT nextval('cycle_counter_id_seq')"
            ),
            1
        );
        drop(admin);
        fs::remove_dir_all(path).unwrap();
    }
}
