#![cfg(feature = "engine-rocksdb")]

use giggles_core::{
    Admin, CoreError, Engine, Limits, SqlSession,
    relational::{SqlError, SqlResult, Value},
};

mod common;

const ROWS: u64 = 20_005;
const ORPHANS: &str = "SELECT count(*) FROM c WHERE pid IS NOT NULL \
    AND NOT EXISTS (SELECT 1 FROM p WHERE p.id = c.pid)";

fn with_db(test: impl FnOnce(&mut SqlSession)) {
    let path = common::fresh("fk-bulk-paged");
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    test(&mut admin.sql_session());
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

fn run(s: &mut SqlSession, sql: &str) -> SqlResult {
    s.execute(sql, &[], None)
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
}

fn fails(s: &mut SqlSession, sql: &str, error: SqlError) {
    assert_eq!(
        s.execute(sql, &[], None),
        Err(CoreError::Sql(error)),
        "{sql}"
    );
}

fn count(s: &mut SqlSession, sql: &str, expected: u64) {
    let rows = run(s, sql).rows;
    assert_eq!(rows.len(), 1, "{sql}");
    assert_eq!(rows[0].values, vec![Value::Int(expected as i64)], "{sql}");
}

fn clean(s: &mut SqlSession) {
    count(s, ORPHANS, 0);
}

fn affected(s: &mut SqlSession, sql: &str, expected: u64) {
    assert_eq!(run(s, sql).affected, expected, "{sql}");
}

#[test]
fn writable_cte_claims_a_locked_job() {
    with_db(|s| {
        run(
            s,
            "CREATE TABLE jobs(id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new')",
        );
        run(
            s,
            "INSERT INTO jobs VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),\
                (3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty')",
        );
        run(s, "BEGIN");
        let claimed = run(
            s,
            "WITH next_job AS (SELECT id FROM jobs WHERE label='root' \
            ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1), claimed AS (UPDATE jobs SET label='running' \
            FROM next_job WHERE jobs.id=next_job.id RETURNING jobs.id,jobs.label) \
            SELECT * FROM claimed ORDER BY id",
        );
        assert_eq!(claimed.columns, ["id", "label"]);
        assert_eq!(claimed.rows.len(), 1);
        assert_eq!(
            claimed.rows[0].values,
            [Value::Int(1), Value::Text("running".into())]
        );
        run(s, "COMMIT");
        count(s, "SELECT count(*) FROM jobs WHERE label='running'", 1);
    });
}

const CTE_ROWS: u64 = 3_005;

fn writable_cte_rows(s: &mut SqlSession) {
    run(s, "CREATE TABLE p(id int PRIMARY KEY)");
    run(s, "CREATE TABLE c(id int PRIMARY KEY, pid int REFERENCES p, w int)");
    affected(s, &format!("INSERT INTO p SELECT g FROM generate_series(1, {CTE_ROWS}) g"), CTE_ROWS);
    affected(s, "INSERT INTO c SELECT id, id, 0 FROM p", CTE_ROWS);
}

#[test]
fn unused_writable_cte_follows_main_update() {
    with_db(|s| {
        writable_cte_rows(s);
        affected(s, "WITH u AS (UPDATE c SET w=1 RETURNING id) \
            UPDATE c SET w=2 WHERE id IN (SELECT id FROM c)", CTE_ROWS);
        count(s, "SELECT count(*) FROM c WHERE w=2", CTE_ROWS);
        affected(s, "WITH u AS (UPDATE c SET w=1 RETURNING id), keys AS (SELECT id FROM u) \
            UPDATE c SET w=3 WHERE id IN (SELECT id FROM keys)", 0);
        count(s, "SELECT count(*) FROM c WHERE w=1", CTE_ROWS);
        clean(s);
    });
}

#[test]
fn unused_writable_cte_follows_main_delete() {
    with_db(|s| {
        writable_cte_rows(s);
        affected(s, "WITH u AS (UPDATE c SET w=1 WHERE id%2=0) DELETE FROM c", CTE_ROWS);
        count(s, "SELECT count(*) FROM c", 0);
        clean(s);
    });
}

#[test]
fn unused_writable_cte_follows_main_merge() {
    with_db(|s| {
        writable_cte_rows(s);
        affected(s, &format!("WITH u AS (UPDATE c SET w=1 WHERE id={CTE_ROWS} RETURNING id) \
            MERGE INTO c USING (SELECT g FROM generate_series(1, {CTE_ROWS}) g) src \
            ON c.id=src.g WHEN MATCHED THEN UPDATE SET w=2"), CTE_ROWS);
        count(s, "SELECT count(*) FROM c WHERE w=2", CTE_ROWS);
        clean(s);
    });
}

#[test]
fn no_action_delete_trigger_precedes_later_cascade() {
    with_db(|s| {
        run(s, "CREATE TABLE p(id int PRIMARY KEY)");
        run(s, "CREATE TABLE c(id int PRIMARY KEY, pid int, pid2 int, \
            CONSTRAINT c_a FOREIGN KEY (pid) REFERENCES p, \
            CONSTRAINT c_b FOREIGN KEY (pid2) REFERENCES p ON DELETE CASCADE)");
        run(s, "CREATE INDEX c_pid ON c(pid)");
        run(s, "CREATE INDEX c_pid2 ON c(pid2)");
        affected(s, &format!("INSERT INTO p SELECT g FROM generate_series(1, {CTE_ROWS}) g"), CTE_ROWS);
        affected(s, "INSERT INTO c SELECT id, id, id FROM p", CTE_ROWS);
        for sql in [
            "DELETE FROM p",
            "MERGE INTO p USING (SELECT id FROM p) src ON p.id=src.id WHEN MATCHED THEN DELETE",
            "WITH d AS (DELETE FROM p RETURNING id) SELECT count(*) FROM d",
        ] {
            fails(s, sql, SqlError::ForeignKeyViolation);
            count(s, "SELECT count(*) FROM p", CTE_ROWS);
            count(s, "SELECT count(*) FROM c", CTE_ROWS);
            clean(s);
            count(s, "SELECT count(*) FROM c WHERE NOT EXISTS (SELECT 1 FROM p WHERE p.id=c.pid2)", 0);
        }
    });
}

fn seed(s: &mut SqlSession, key: &str) {
    affected(
        s,
        &format!("INSERT INTO p SELECT {key} FROM generate_series(1, {ROWS}) g"),
        ROWS,
    );
    clean(s);
}

fn integer_graph(s: &mut SqlSession, delete_action: &str, update_action: &str) {
    run(s, "CREATE TABLE p(id int PRIMARY KEY)");
    run(
        s,
        &format!(
            "CREATE TABLE c(id int PRIMARY KEY, pid int REFERENCES p \
        ON DELETE {delete_action} ON UPDATE {update_action})"
        ),
    );
    run(s, "CREATE INDEX c_pid ON c(pid)");
    clean(s);
}

#[test]
fn immediate_parent_delete_and_update_across_pages() {
    for (action, error) in [
        ("NO ACTION", SqlError::ForeignKeyViolation),
        ("RESTRICT", SqlError::RestrictViolation),
    ] {
        with_db(|s| {
            integer_graph(s, action, action);
            seed(s, "g");
            affected(s, "DELETE FROM p", ROWS);
            clean(s);

            seed(s, "g");
            affected(s, &format!("INSERT INTO c VALUES (1,{ROWS})"), 1);
            clean(s);
            fails(s, "DELETE FROM p", error.clone());
            clean(s);
            count(s, "SELECT count(*) FROM p", ROWS);
            count(s, "SELECT count(*) FROM c", 1);
            fails(s, &format!("UPDATE p SET id = id + {ROWS}"), error);
            clean(s);
            count(
                s,
                &format!("SELECT count(*) FROM p WHERE id <= {ROWS}"),
                ROWS,
            );
            count(s, "SELECT count(*) FROM c", 1);

            affected(
                s,
                &format!("UPDATE p SET id = id + {ROWS} WHERE id < {ROWS}"),
                ROWS - 1,
            );
            clean(s);
            affected(s, &format!("DELETE FROM p WHERE id > {ROWS}"), ROWS - 1);
            clean(s);
            count(s, "SELECT count(*) FROM p", 1);
            affected(s, "DELETE FROM c", 1);
            clean(s);

            affected(s, "DELETE FROM p", 1);
            clean(s);
            seed(s, "g");
            affected(s, "INSERT INTO c SELECT id, id FROM p", ROWS);
            clean(s);
            run(s, "BEGIN");
            affected(s, "DELETE FROM c", ROWS);
            clean(s);
            affected(s, "DELETE FROM p", ROWS);
            clean(s);
            run(s, "COMMIT");
            clean(s);
            count(s, "SELECT count(*) FROM c", 0);
            count(s, "SELECT count(*) FROM p", 0);
        });
    }
}

#[test]
fn text_and_uuid_prisma_parent_deletes() {
    for (ty, key) in [("text", "'u' || g"), ("uuid", "md5(g::text)::uuid")] {
        with_db(|s| {
            run(s, &format!("CREATE TABLE p(id {ty} PRIMARY KEY)"));
            run(
                s,
                &format!(
                    "CREATE TABLE c(id int PRIMARY KEY, pid {ty} NOT NULL REFERENCES p \
                ON DELETE RESTRICT ON UPDATE CASCADE)"
                ),
            );
            run(s, "CREATE INDEX c_pid ON c(pid)");
            clean(s);
            seed(s, key);
            affected(
                s,
                &format!("INSERT INTO c SELECT g, {key} FROM generate_series(1, {ROWS}) g"),
                ROWS,
            );
            clean(s);
            fails(s, "DELETE FROM p", SqlError::RestrictViolation);
            clean(s);
            count(s, "SELECT count(*) FROM c", ROWS);
            count(s, "SELECT count(*) FROM p", ROWS);
            affected(s, "DELETE FROM c", ROWS);
            clean(s);
            affected(s, "DELETE FROM p", ROWS);
            clean(s);
            count(s, "SELECT count(*) FROM p", 0);
        });
    }
}

#[test]
fn deferred_no_action_checks_commit_and_set_constraints() {
    with_db(|s| {
        run(s, "CREATE TABLE p(id int PRIMARY KEY)");
        run(
            s,
            "CREATE TABLE c(id int PRIMARY KEY, pid int, \
            CONSTRAINT c_p_fk FOREIGN KEY (pid) REFERENCES p DEFERRABLE INITIALLY DEFERRED)",
        );
        run(s, "CREATE INDEX c_pid ON c(pid)");
        seed(s, "g");
        affected(s, &format!("INSERT INTO c VALUES (1, {ROWS})"), 1);
        clean(s);

        run(s, "BEGIN");
        affected(s, "DELETE FROM p", ROWS);
        count(s, ORPHANS, 1);
        fails(s, "COMMIT", SqlError::ForeignKeyViolation);
        clean(s);
        count(s, "SELECT count(*) FROM p", ROWS);
        count(s, "SELECT count(*) FROM c", 1);

        run(s, "BEGIN");
        run(s, "SET CONSTRAINTS c_p_fk IMMEDIATE");
        fails(s, "DELETE FROM p", SqlError::ForeignKeyViolation);
        run(s, "ROLLBACK");
        clean(s);
        count(s, "SELECT count(*) FROM p", ROWS);

        run(s, "BEGIN");
        run(s, "SET CONSTRAINTS c_p_fk IMMEDIATE");
        run(s, "SET CONSTRAINTS c_p_fk DEFERRED");
        affected(s, "DELETE FROM p", ROWS);
        count(s, ORPHANS, 1);
        fails(
            s,
            "SET CONSTRAINTS c_p_fk IMMEDIATE",
            SqlError::ForeignKeyViolation,
        );
        run(s, "ROLLBACK");
        clean(s);
        count(s, "SELECT count(*) FROM p", ROWS);

        run(s, "BEGIN");
        affected(s, "DELETE FROM p", ROWS);
        count(s, ORPHANS, 1);
        affected(s, "DELETE FROM c", 1);
        clean(s);
        run(s, "COMMIT");
        clean(s);
        count(s, "SELECT count(*) FROM p", 0);
    });
}

#[test]
fn no_action_uses_final_parent_key_while_restrict_checks_each_old_key() {
    for action in ["NO ACTION", "RESTRICT"] {
        with_db(|s| {
            run(s, "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE)");
            run(
                s,
                &format!(
                    "CREATE TABLE c(id int PRIMARY KEY, pid int REFERENCES p(code) ON UPDATE {action})"
                ),
            );
            run(s, "CREATE INDEX c_pid ON c(pid)");
            affected(
                s,
                &format!("INSERT INTO p SELECT g, g FROM generate_series(1, {ROWS}) g"),
                ROWS,
            );
            affected(s, "INSERT INTO c VALUES (1,1)", 1);
            let orphans =
                "SELECT count(*) FROM c WHERE NOT EXISTS (SELECT 1 FROM p WHERE p.code = c.pid)";
            count(s, orphans, 0);
            if action == "NO ACTION" {
                affected(s, "UPDATE p SET code = code - 1", ROWS);
                count(s, "SELECT count(*) FROM p WHERE id=2 AND code=1", 1);
            } else {
                fails(
                    s,
                    "UPDATE p SET code = code - 1",
                    SqlError::RestrictViolation,
                );
                count(s, "SELECT count(*) FROM p WHERE id=1 AND code=1", 1);
            }
            count(s, orphans, 0);
        });
    }
}

#[test]
fn cascaded_deletes_remove_no_action_witnesses() {
    with_db(|s| {
        run(s, "CREATE TABLE p(id int PRIMARY KEY)");
        run(
            s,
            "CREATE TABLE bridge(id int PRIMARY KEY REFERENCES p ON DELETE CASCADE)",
        );
        run(
            s,
            "CREATE TABLE c(id int PRIMARY KEY REFERENCES p ON DELETE CASCADE, \
                pid int REFERENCES p, via int REFERENCES bridge)",
        );
        run(s, "CREATE INDEX c_pid ON c(pid)");
        run(s, "CREATE INDEX c_via ON c(via)");
        seed(s, "g");
        affected(s, "INSERT INTO bridge SELECT id FROM p", ROWS);
        clean(s);
        affected(s, "INSERT INTO c SELECT id, id, id FROM bridge", ROWS);
        clean(s);
        affected(s, "DELETE FROM p", ROWS);
        clean(s);
        count(s, "SELECT count(*) FROM c", 0);
        count(s, "SELECT count(*) FROM bridge", 0);
        count(s, "SELECT count(*) FROM p", 0);
    });
}

#[test]
fn self_referencing_tree_checks_final_rows() {
    for action in ["CASCADE", "NO ACTION", "RESTRICT"] {
        with_db(|s| {
            run(
                s,
                &format!(
                    "CREATE TABLE tree(id int PRIMARY KEY, parent int REFERENCES tree ON DELETE {action})"
                ),
            );
            run(s, "CREATE INDEX tree_parent ON tree(parent)");
            let orphans = "SELECT count(*) FROM tree t WHERE parent IS NOT NULL \
                AND NOT EXISTS (SELECT 1 FROM tree p WHERE p.id = t.parent)";
            affected(
                s,
                &format!(
                    "INSERT INTO tree SELECT g, CASE WHEN g > 1 THEN g / 2 END \
                FROM generate_series(1, {ROWS}) g"
                ),
                ROWS,
            );
            count(s, orphans, 0);
            affected(s, "UPDATE tree SET parent = 1 WHERE id > 1", ROWS - 1);
            count(s, orphans, 0);
            if action == "CASCADE" {
                affected(s, "DELETE FROM tree WHERE id = 1", 1);
            } else {
                let error = if action == "RESTRICT" {
                    SqlError::RestrictViolation
                } else {
                    SqlError::ForeignKeyViolation
                };
                fails(s, "DELETE FROM tree WHERE id = 1", error);
                count(s, orphans, 0);
                count(s, "SELECT count(*) FROM tree", ROWS);
                affected(s, "DELETE FROM tree", ROWS);
            }
            count(s, orphans, 0);
            count(s, "SELECT count(*) FROM tree", 0);
        });
    }
}

#[test]
fn earlier_no_action_error_precedes_later_restrict() {
    with_db(|s| {
        integer_graph(s, "NO ACTION", "RESTRICT");
        run(
            s,
            "CREATE TABLE r(id int PRIMARY KEY, pid int REFERENCES p ON DELETE RESTRICT)",
        );
        run(s, "CREATE INDEX r_pid ON r(pid)");
        seed(s, "g");
        affected(s, "INSERT INTO c VALUES (1,1)", 1);
        clean(s);
        affected(s, &format!("INSERT INTO r VALUES (1,{ROWS})"), 1);
        fails(s, "DELETE FROM p", SqlError::ForeignKeyViolation);
        fails(s, "DELETE FROM p WHERE id > 1", SqlError::RestrictViolation);
        clean(s);
        count(
            s,
            "SELECT count(*) FROM r WHERE NOT EXISTS (SELECT 1 FROM p WHERE p.id = r.pid)",
            0,
        );
        count(s, "SELECT count(*) FROM p", ROWS);
    });
}

#[test]
fn primary_key_moves_keep_referential_trigger_order() {
    for (first, second) in [("NO ACTION", "CASCADE"), ("CASCADE", "NO ACTION")] {
        with_db(|s| {
            run(s, "CREATE TABLE p(id int PRIMARY KEY)");
            run(
                s,
                &format!(
                    "CREATE TABLE c(id int PRIMARY KEY, pid int,
                FOREIGN KEY (pid) REFERENCES p ON UPDATE {first},
                FOREIGN KEY (pid) REFERENCES p ON UPDATE {second})"
                ),
            );
            run(s, "CREATE INDEX c_pid ON c(pid)");
            affected(s, "INSERT INTO p VALUES (1)", 1);
            affected(s, "INSERT INTO c VALUES (1,1)", 1);
            clean(s);
            if first == "NO ACTION" {
                fails(s, "UPDATE p SET id = 2", SqlError::ForeignKeyViolation);
                count(s, "SELECT count(*) FROM p WHERE id=1", 1);
                count(s, "SELECT count(*) FROM c WHERE pid=1", 1);
            } else {
                affected(s, "UPDATE p SET id = 2", 1);
                count(s, "SELECT count(*) FROM p WHERE id=2", 1);
                count(s, "SELECT count(*) FROM c WHERE pid=2", 1);
            }
            clean(s);
        });
    }
}

#[test]
fn merge_child_insert_update_and_parent_cascade() {
    with_db(|s| {
        integer_graph(s, "CASCADE", "CASCADE");
        seed(s, "g");
        affected(
            s,
            &format!(
                "INSERT INTO c SELECT id, id FROM p WHERE id <= {}",
                ROWS / 2
            ),
            ROWS / 2,
        );
        affected(
            s,
            &format!(
                "MERGE INTO c USING (SELECT g FROM generate_series(1,{ROWS}) g) s
                 ON c.id = s.g WHEN MATCHED THEN UPDATE SET pid = {ROWS} + 1 - s.g
                 WHEN NOT MATCHED THEN INSERT VALUES (s.g,s.g)"
            ),
            ROWS,
        );
        clean(s);
        count(s, "SELECT count(*) FROM c", ROWS);
        count(
            s,
            &format!(
                "SELECT count(*) FROM c WHERE id <= {} AND pid = {ROWS} + 1 - id",
                ROWS / 2
            ),
            ROWS / 2,
        );
        affected(
            s,
            &format!(
                "MERGE INTO p USING (SELECT g FROM generate_series(1,{ROWS}) g) s
                 ON p.id = s.g WHEN MATCHED THEN DELETE"
            ),
            ROWS,
        );
        clean(s);
        count(s, "SELECT count(*) FROM c", 0);
        count(s, "SELECT count(*) FROM p", 0);
    });
}

#[test]
fn merge_errors_roll_back_all_pages() {
    with_db(|s| {
        integer_graph(s, "RESTRICT", "NO ACTION");
        seed(s, "g");
        affected(s, &format!("INSERT INTO c VALUES (1,{ROWS})"), 1);
        fails(
            s,
            &format!(
                "MERGE INTO p USING (SELECT g FROM generate_series(1,{ROWS}) g) s
                 ON p.id = s.g WHEN MATCHED THEN DELETE"
            ),
            SqlError::RestrictViolation,
        );
        count(s, "SELECT count(*) FROM p", ROWS);
        clean(s);
        affected(s, "DELETE FROM c", 1);
        let error = s
            .execute(
                &format!(
                    "MERGE INTO c USING (SELECT g FROM generate_series(1,{ROWS}) g) s
                 ON c.id = s.g WHEN NOT MATCHED THEN INSERT VALUES
                 (s.g,CASE WHEN s.g = {ROWS} THEN {ROWS}+1 ELSE s.g END)"
                ),
                &[],
                None,
            )
            .unwrap_err();
        assert_eq!(giggles_core::sqlstate(&error), "23503");
        count(s, "SELECT count(*) FROM c", 0);
        clean(s);
        affected(s, "INSERT INTO c SELECT id,id FROM p", ROWS);
        fails(s, &format!("MERGE INTO c USING
                 (SELECT CASE WHEN g = {ROWS} THEN 1 ELSE g END AS id FROM generate_series(1,{ROWS}) g) s
                 ON c.id = s.id WHEN MATCHED THEN UPDATE SET pid = {ROWS} + 1 - s.id"), SqlError::CardinalityViolation);
        clean(s);
        count(s, "SELECT count(*) FROM c WHERE id = pid", ROWS);
    });
}

#[test]
fn writable_cte_insert_upsert_and_delete_return_each_row() {
    with_db(|s| {
        integer_graph(s, "CASCADE", "CASCADE");
        seed(s, "g");
        count(s, &format!("WITH ins AS (INSERT INTO c SELECT g,g FROM generate_series(1,{ROWS}) g RETURNING id)
              SELECT count(*) FROM ins"), ROWS);
        clean(s);
        count(
            s,
            &format!(
                "WITH up AS (INSERT INTO c SELECT g,g FROM generate_series(1,{ROWS}) g
              ON CONFLICT (id) DO UPDATE SET pid = {ROWS} + 1 - excluded.pid RETURNING id)
              SELECT count(*) FROM up"
            ),
            ROWS,
        );
        count(
            s,
            &format!("SELECT count(*) FROM c WHERE pid = {ROWS} + 1 - id"),
            ROWS,
        );
        clean(s);
        count(
            s,
            &format!(
                "WITH d AS (DELETE FROM p WHERE id <= {} RETURNING id) SELECT count(*) FROM d",
                ROWS / 2
            ),
            ROWS / 2,
        );
        clean(s);
        count(s, "SELECT count(*) FROM c", ROWS - ROWS / 2);
    });
}

#[test]
fn writable_cte_siblings_use_the_original_base_tables() {
    with_db(|s| {
        integer_graph(s, "NO ACTION", "NO ACTION");
        let result = run(
            s,
            &format!(
                "WITH parents AS (
            INSERT INTO p SELECT g FROM generate_series(1,{ROWS}) g RETURNING id
        ), children AS (
            INSERT INTO c SELECT id,id FROM parents RETURNING id
        ) SELECT (SELECT count(*) FROM parents), (SELECT count(*) FROM children),
                 (SELECT count(*) FROM p), (SELECT count(*) FROM c)"
            ),
        );
        assert_eq!(
            result.rows[0].values,
            vec![
                Value::Int(ROWS as i64),
                Value::Int(ROWS as i64),
                Value::Int(0),
                Value::Int(0)
            ]
        );
        clean(s);
        let result = run(
            s,
            "WITH parents AS (
            DELETE FROM p RETURNING id
        ), children AS (
            DELETE FROM c RETURNING id
        ) SELECT (SELECT count(*) FROM parents), (SELECT count(*) FROM children),
                 (SELECT count(*) FROM p), (SELECT count(*) FROM c)",
        );
        assert_eq!(result.rows[0].values, vec![Value::Int(ROWS as i64); 4]);
        clean(s);
        count(s, "SELECT count(*) FROM c", 0);
        count(s, "SELECT count(*) FROM p", 0);
    });
}

#[test]
fn writable_cte_without_foreign_keys_uses_the_same_paged_path() {
    with_db(|s| {
        run(s, "CREATE TABLE plain(id int PRIMARY KEY)");
        count(
            s,
            &format!(
                "WITH ins AS (
            INSERT INTO plain SELECT g FROM generate_series(1,{ROWS}) g RETURNING id
        ) SELECT count(*) FROM ins"
            ),
            ROWS,
        );
        count(
            s,
            "WITH d AS (DELETE FROM plain RETURNING id) SELECT count(*) FROM d",
            ROWS,
        );
        count(s, "SELECT count(*) FROM plain", 0);
    });
}

#[test]
fn merge_and_writable_cte_keep_referential_trigger_order() {
    for sql in [
        "MERGE INTO p USING (VALUES (1)) s(id) ON p.id = s.id WHEN MATCHED THEN UPDATE SET id = 2",
        "WITH changed AS (UPDATE p SET id = 2 RETURNING id) SELECT id FROM changed",
    ] {
        with_db(|s| {
            run(s, "CREATE TABLE p(id int PRIMARY KEY)");
            run(
                s,
                "CREATE TABLE c(id int PRIMARY KEY, pid int,
                FOREIGN KEY (pid) REFERENCES p ON UPDATE NO ACTION,
                FOREIGN KEY (pid) REFERENCES p ON UPDATE CASCADE)",
            );
            run(s, "CREATE INDEX c_pid ON c(pid)");
            affected(s, "INSERT INTO p VALUES (1)", 1);
            affected(s, "INSERT INTO c VALUES (1,1)", 1);
            fails(s, sql, SqlError::ForeignKeyViolation);
            count(s, "SELECT count(*) FROM p WHERE id=1", 1);
            count(s, "SELECT count(*) FROM c WHERE pid=1", 1);
            clean(s);
        });
    }
}
