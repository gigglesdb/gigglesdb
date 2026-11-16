-- case: pgf_advisory
-- title: Shared locks and nonliteral advisory keys
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-admin.html
CREATE TABLE r0c_pgf_advisory (id integer PRIMARY KEY, k bigint);
INSERT INTO r0c_pgf_advisory VALUES (1,70068001);
SELECT pg_try_advisory_lock_shared(k) FROM r0c_pgf_advisory ORDER BY id;
SELECT pg_advisory_unlock_shared(70068001);
SELECT pg_advisory_lock_shared(70068002);
SELECT pg_advisory_unlock_shared(70068002);
SELECT pg_try_advisory_lock(70068003::bigint);
SELECT pg_advisory_unlock(70068003::bigint);
BEGIN;
SELECT pg_advisory_xact_lock_shared(70068004);
SELECT pg_try_advisory_xact_lock_shared(70068005);
ROLLBACK;
SELECT pg_advisory_unlock_all();
DROP TABLE r0c_pgf_advisory;
